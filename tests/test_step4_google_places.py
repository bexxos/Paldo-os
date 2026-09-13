import json
import tempfile
import unittest
from pathlib import Path

from paldo_os_outbound import Database
from step3_discovery import (
    DiscoveryMode,
    GooglePlacesAdapter,
    GooglePlacesClient,
    GooglePlacesCostProfile,
    GooglePlacesHTTPResponse,
    GooglePlacesRequestError,
    IngestionOutcome,
    LiveModeBlockedError,
    execute_google_live,
    estimate_google_request_count,
    google_places_readiness,
    migrate_step4,
    mock_google_execution,
    preview_google_query_plan,
)


class MockTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, *, url, headers, body, timeout_seconds):
        self.calls.append({"url": url, "headers": dict(headers), "body": dict(body), "timeout_seconds": timeout_seconds})
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def place(place_id="place-fictional-001", **overrides):
    value = {
        "id": place_id,
        "displayName": {"text": "Fictional Northfield Local Service Business"},
        "formattedAddress": "123 Fictional Street, Westport, Northfield, PH",
        "location": {"latitude": 15.145, "longitude": 120.588},
        "types": ["health", "doctor"],
        "primaryType": "doctor",
        "businessStatus": "OPERATIONAL",
        "googleMapsUri": "https://maps.google.test/place/fictional-001",
    }
    value.update(overrides)
    return value


class Step4GooglePlacesTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.database = Database(Path(self.tempdir.name) / "step4.sqlite3")
        migrate_step4(self.database)
        self.addCleanup(self.database.close)
        self.campaign = self.database.get_campaign_by_name("Primary region local services")

    def test_default_discovery_pro_request_uses_endpoint_headers_body_and_explicit_mask(self):
        transport = MockTransport([GooglePlacesHTTPResponse(200, {"places": [place()]})])
        client = GooglePlacesClient(env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"}, transport=transport)
        result = client.search_text("local service business in Westport, Northfield")
        call = transport.calls[0]
        self.assertEqual(call["url"], "https://places.googleapis.com/v1/places:searchText")
        self.assertEqual(call["headers"]["Content-Type"], "application/json")
        self.assertEqual(call["headers"]["X-Goog-Api-Key"], "FAKE_STEP4_KEY")
        self.assertIn("places.id", call["headers"]["X-Goog-FieldMask"])
        self.assertIn("nextPageToken", call["headers"]["X-Goog-FieldMask"])
        self.assertNotIn("*", call["headers"]["X-Goog-FieldMask"])
        self.assertEqual(call["body"]["textQuery"], "local service business in Westport, Northfield")
        self.assertEqual(call["body"]["languageCode"], "en")
        self.assertEqual(call["body"]["regionCode"], "PH")
        self.assertEqual(call["body"]["pageSize"], 20)
        self.assertEqual(result.page_count, 1)

    def test_contact_enterprise_requires_explicit_enablement(self):
        with self.assertRaises(ValueError):
            GooglePlacesClient(cost_profile=GooglePlacesCostProfile.CONTACT_ENTERPRISE)
        client = GooglePlacesClient(
            cost_profile=GooglePlacesCostProfile.CONTACT_ENTERPRISE,
            enable_contact_enterprise=True,
            env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"},
            transport=MockTransport([GooglePlacesHTTPResponse(200, {"places": []})]),
        )
        client.search_text("local business in Westport, Northfield")
        mask = client.last_request["headers"]["X-Goog-FieldMask"]
        self.assertIn("places.websiteUri", mask)
        self.assertIn("places.rating", mask)

    def test_pagination_is_bounded_and_quota_is_checked_before_each_request(self):
        transport = MockTransport([
            GooglePlacesHTTPResponse(200, {"places": [place()], "nextPageToken": "page-2"}),
            GooglePlacesHTTPResponse(200, {"places": [place("place-fictional-002")], "nextPageToken": "page-3"}),
            GooglePlacesHTTPResponse(200, {"places": [place("place-fictional-003")]})
        ])
        client = GooglePlacesClient(env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"}, transport=transport)
        result = client.search_text("local business near Harbor District, Northfield", max_pages=2, quota_remaining=2)
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(result.page_count, 2)
        self.assertEqual(result.request_count, 2)
        self.assertEqual(result.records[1]["source_record_id"], "place-fictional-002")

        limited = MockTransport([GooglePlacesHTTPResponse(200, {"places": [], "nextPageToken": "page-2"})])
        with self.assertRaisesRegex(GooglePlacesRequestError, "quota") as error:
            GooglePlacesClient(env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"}, transport=limited).search_text(
                "local business", max_pages=2, quota_remaining=1
            )
        self.assertEqual(error.exception.category, "quota")
        self.assertEqual(len(limited.calls), 1)

    def test_transient_errors_retry_with_bound_and_non_retryable_errors_do_not_retry(self):
        transient = MockTransport([
            GooglePlacesHTTPResponse(503, {"error": {"status": "UNAVAILABLE"}}),
            GooglePlacesHTTPResponse(200, {"places": [place()]}),
        ])
        client = GooglePlacesClient(
            env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"}, transport=transient, max_retries=2, retry_delay_seconds=0
        )
        client.search_text("local service business")
        self.assertEqual(len(transient.calls), 2)

        timeout = MockTransport([TimeoutError(), GooglePlacesHTTPResponse(200, {"places": [place()]})])
        GooglePlacesClient(
            env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"}, transport=timeout, max_retries=2, retry_delay_seconds=0
        ).search_text("local service business")
        self.assertEqual(len(timeout.calls), 2)

        for status, category in ((400, "invalid_request"), (401, "authentication"), (403, "permission"), (402, "billing")):
            transport = MockTransport([GooglePlacesHTTPResponse(status, {"error": {"status": "SAFE"}})])
            with self.assertRaises(GooglePlacesRequestError) as error:
                GooglePlacesClient(
                    env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"}, transport=transport, max_retries=3, retry_delay_seconds=0
                ).search_text("local service business")
            self.assertEqual(error.exception.category, category)
            self.assertEqual(len(transport.calls), 1)

    def test_response_maps_to_step3_contract_and_missing_contact_is_enrichment_hold(self):
        transport = MockTransport([GooglePlacesHTTPResponse(200, {"places": [place()]})])
        client = GooglePlacesClient(env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"}, transport=transport)
        result = client.search_text("local service business in Westport, Northfield")
        candidate = GooglePlacesAdapter().normalize_records(result.records, mode=DiscoveryMode.DRY_RUN.value)[0]
        self.assertEqual(candidate.source_record_id, "place-fictional-001")
        self.assertEqual(candidate.google_place_id, "place-fictional-001")
        self.assertEqual(candidate.business_name, "Fictional Northfield Local Service Business")
        self.assertIsNone(candidate.website_url)
        self.assertIsNone(candidate.public_business_email)
        self.assertEqual(self.database.ingest_candidate(candidate).outcome, IngestionOutcome.INSUFFICIENT_IDENTITY)

    def test_query_plan_is_campaign_backed_and_estimate_is_bounded(self):
        plan = preview_google_query_plan(self.database, self.campaign["id"])
        self.assertEqual([row["query_text"] for row in plan], [
            "local service business in Westport, Northfield",
            "local business in Westport, Northfield",
            "local service business in Riverton, Northfield",
            "local business in Riverton, Northfield",
            "local service business in Fairview, Northfield",
            "local business near Harbor District, Northfield",
        ])
        self.assertTrue(all(row["max_pages"] == 1 for row in plan))
        estimate = estimate_google_request_count(self.database, self.campaign["id"])
        self.assertEqual(estimate["estimated_requests"], 6)
        self.assertEqual(estimate["remaining_quota"], 0)
        self.assertFalse(estimate["within_quota"])

    def test_readiness_is_safe_and_never_returns_the_api_key(self):
        status = google_places_readiness(self.database, env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"})
        self.assertFalse(status["credential_value_returned"])
        self.assertTrue(status["credential_present"])
        self.assertFalse(status["ready"])
        self.assertNotIn("FAKE_STEP4_KEY", repr(status))

    def test_mock_execution_uses_injected_http_and_does_not_need_credentials_or_write_leads(self):
        transport = MockTransport([GooglePlacesHTTPResponse(200, {"places": [place()]}) for _ in range(6)])
        result = mock_google_execution(self.database, self.campaign["id"], transport=transport)
        self.assertEqual(result["mode"], "DRY_RUN")
        self.assertEqual(result["request_count"], 6)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)
        self.assertNotIn("FAKE", repr(result))

    def test_live_execution_is_blocked_by_paused_default_before_transport(self):
        transport = MockTransport([])
        with self.assertRaises(LiveModeBlockedError):
            execute_google_live(
                self.database, self.campaign["id"], env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"},
                operator_confirmation=True, transport=transport,
            )
        self.assertEqual(transport.calls, [])

    def test_live_execution_requires_live_mode_active_campaign_key_quota_and_confirmation(self):
        transport = MockTransport([])
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value='ACTIVE' WHERE key='system_state'")
            self.database.connection.execute("UPDATE system_config SET value='LIVE' WHERE key='discovery_mode'")
            self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))
            self.database.connection.execute("UPDATE system_config SET value='6' WHERE key='google_places_daily_request_cap'")
        for kwargs, text in (
            ({"env": {}, "operator_confirmation": True}, "credential"),
            ({"env": {"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"}, "operator_confirmation": False}, "confirmation"),
        ):
            with self.assertRaisesRegex(LiveModeBlockedError, text):
                execute_google_live(self.database, self.campaign["id"], transport=transport, **kwargs)
        self.assertEqual(transport.calls, [])

    def test_each_live_gate_blocks_before_transport(self):
        transport = MockTransport([])
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value='ACTIVE' WHERE key='system_state'")
            self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))
            self.database.connection.execute("UPDATE system_config SET value='6' WHERE key='google_places_daily_request_cap'")
        cases = (
            ("DRY_RUN", "ACTIVE", 6, "discovery_mode"),
            ("LIVE", "INACTIVE", 6, "campaign"),
            ("LIVE", "ACTIVE", 0, "quota"),
        )
        for mode, campaign_status, quota, expected in cases:
            with self.subTest(expected=expected):
                with self.database.connection:
                    self.database.connection.execute("UPDATE system_config SET value=? WHERE key='discovery_mode'", (mode,))
                    self.database.connection.execute("UPDATE campaigns SET status=? WHERE id=?", (campaign_status, self.campaign["id"]))
                    self.database.connection.execute("UPDATE system_config SET value=? WHERE key='google_places_daily_request_cap'", (str(quota),))
                with self.assertRaisesRegex(LiveModeBlockedError, expected):
                    execute_google_live(
                        self.database, self.campaign["id"], env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"},
                        operator_confirmation=True, transport=transport,
                    )
        self.assertEqual(transport.calls, [])

    def test_failed_request_logs_safe_category_without_secret_or_response_dump(self):
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value='ACTIVE' WHERE key='system_state'")
            self.database.connection.execute("UPDATE system_config SET value='LIVE' WHERE key='discovery_mode'")
            self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))
            self.database.connection.execute("UPDATE system_config SET value='6' WHERE key='google_places_daily_request_cap'")
        secret = "FAKE_STEP4_KEY"
        transport = MockTransport([GooglePlacesHTTPResponse(403, {"error": {"message": secret}}) for _ in range(6)])
        result = execute_google_live(
            self.database, self.campaign["id"], env={"GOOGLE_PLACES_API_KEY": secret},
            operator_confirmation=True, transport=transport, max_retries=3,
        )
        self.assertEqual(result["errors"], ["permission"])
        self.assertNotIn(secret, repr(result))
        rows = self.database.connection.execute("SELECT status, error_category FROM google_places_request_log").fetchall()
        self.assertTrue(rows)
        self.assertTrue(all(row["status"] == "FAILED" and row["error_category"] == "permission" for row in rows))
        self.assertNotIn(secret, repr([tuple(row) for row in rows]))

    def test_live_execution_with_mocked_http_reuses_ingestion_and_is_idempotent(self):
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value='ACTIVE' WHERE key='system_state'")
            self.database.connection.execute("UPDATE system_config SET value='LIVE' WHERE key='discovery_mode'")
            self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))
            self.database.connection.execute("UPDATE system_config SET value='6' WHERE key='google_places_daily_request_cap'")
        transport = MockTransport([GooglePlacesHTTPResponse(200, {"places": [place()]}) for _ in range(6)])
        result = execute_google_live(
            self.database, self.campaign["id"], env={"GOOGLE_PLACES_API_KEY": "FAKE_STEP4_KEY"},
            operator_confirmation=True, transport=transport,
        )
        self.assertEqual(result["request_count"], 6)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM discovery_candidates").fetchone()[0], 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM google_places_request_log").fetchone()[0], 6)
        self.assertNotIn("FAKE_STEP4_KEY", repr(result))

    def test_repeated_migration_is_safe_and_secret_is_absent_from_request_log(self):
        self.assertEqual(migrate_step4(self.database), 6)
        self.assertEqual(migrate_step4(self.database), 6)
        tables = {row[0] for row in self.database.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertIn("google_query_plans", tables)
        self.assertIn("google_places_request_log", tables)
        columns = {row[1] for row in self.database.connection.execute("PRAGMA table_info(google_places_request_log)")}
        self.assertNotIn("api_key", columns)
        self.assertNotIn("response", columns)


if __name__ == "__main__":
    unittest.main()
