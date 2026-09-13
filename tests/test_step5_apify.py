import json
import tempfile
import unittest
from pathlib import Path

from paldo_os_outbound import Database
from step3_discovery import (
    APIFY_SOURCE,
    ApifyInputError,
    ApifyRequestError,
    ApifyRunBlockedError,
    ApifyRunState,
    IngestionOutcome,
    migrate_step5,
    apify_readiness,
    build_apify_safe_input,
    fetch_apify_dataset,
    poll_apify_run,
    preview_apify_cost_limits,
    preview_apify_input,
    start_apify_live_run,
    summarize_apify_run,
)


class FakeApifyClient:
    def __init__(self, *, start_result=None, statuses=None, dataset=None, start_error=None):
        self.start_result = start_result or {"id": "run-fictional-001", "status": "RUNNING", "defaultDatasetId": "dataset-fictional-001"}
        self.statuses = list(statuses or [])
        self.dataset = list(dataset or [])
        self.start_error = start_error
        self.start_calls = []
        self.status_calls = []
        self.dataset_calls = []

    def start_actor(self, *, actor_id, run_input, max_items, max_total_charge_usd):
        self.start_calls.append({
            "actor_id": actor_id,
            "run_input": dict(run_input),
            "max_items": max_items,
            "max_total_charge_usd": max_total_charge_usd,
        })
        if self.start_error:
            raise self.start_error
        return dict(self.start_result)

    def get_run_status(self, run_id):
        self.status_calls.append(run_id)
        if self.statuses:
            return dict(self.statuses.pop(0))
        return {"id": run_id, "status": "RUNNING", "defaultDatasetId": "dataset-fictional-001"}

    def fetch_dataset(self, dataset_id):
        self.dataset_calls.append(dataset_id)
        return list(self.dataset)


def apify_record(record_id="apify-place-001", **overrides):
    record = {
        "placeId": record_id,
        "title": "Fictional US Local Service Business",
        "categoryName": "Local service business",
        "address": "100 Fictional Avenue, Testville, CA 90210, US",
        "city": "Testville",
        "state": "California",
        "postalCode": "90210",
        "countryCode": "US",
        "website": "https://fictional-business.test",
        "phone": "+1 555 010 2000",
        "location": {"lat": 34.0001, "lng": -118.0001},
        "url": "https://maps.google.test/?place_id=apify-place-001",
        "rating": 4.8,
        "reviewsCount": 22,
        "businessStatus": "OPERATIONAL",
        "reviews": [{"name": "Reviewer Identity", "text": "private data"}],
        "leadEnrichment": {"email": "personal@example.test"},
        "socialMediaProfiles": {"private": "discard-me"},
    }
    record.update(overrides)
    return record


class Step5ApifyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.database = Database(Path(self.tempdir.name) / "step5.sqlite3")
        migrate_step5(self.database)
        self.addCleanup(self.database.close)
        self.campaign = self.database.get_campaign_by_name("Secondary region local services")

    def activate_live(self, *, quota=1, charge=1, max_items=20):
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value='ACTIVE' WHERE key='system_state'")
            self.database.connection.execute("UPDATE system_config SET value='LIVE' WHERE key='discovery_mode'")
            self.database.connection.execute("UPDATE system_config SET value=? WHERE key='apify_daily_run_cap'", (str(quota),))
            self.database.connection.execute("UPDATE system_config SET value=? WHERE key='apify_max_total_charge_usd'", (str(charge),))
            self.database.connection.execute("UPDATE system_config SET value=? WHERE key='apify_max_items_per_run'", (str(max_items),))
            self.database.connection.execute("UPDATE campaigns SET status='INACTIVE'")
            self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))

    def test_safe_input_is_single_location_search_and_has_bounded_privacy_safe_defaults(self):
        safe = build_apify_safe_input("Fictional Testville, CA", "local service business")
        self.assertEqual(safe["locationQuery"], "Fictional Testville, CA")
        self.assertEqual(safe["searchStringsArray"], ["local service business"])
        self.assertEqual(safe["language"], "en")
        self.assertEqual(safe["countryCode"], "us")
        self.assertEqual(safe["maxCrawledPlacesPerSearch"], 20)
        self.assertTrue(safe["skipClosedPlaces"])
        self.assertEqual(safe["website"], "allPlaces")
        self.assertEqual(safe["searchMatching"], "all")
        self.assertFalse(safe["scrapePlaceDetailPage"])
        self.assertFalse(safe["scrapeContacts"])
        self.assertEqual(safe["scrapeSocialMediaProfiles"], {"facebooks": False, "instagrams": False, "youtubes": False, "tiktoks": False, "twitters": False})
        self.assertEqual(safe["maximumLeadsEnrichmentRecords"], 0)
        self.assertFalse(safe["verifyLeadsEnrichmentEmails"])
        self.assertEqual(safe["maxReviews"], 0)
        self.assertFalse(safe["scrapeReviewsPersonalData"])
        self.assertEqual(safe["maxImages"], 0)
        self.assertFalse(safe["scrapeImageAuthors"])
        self.assertFalse(safe["enableCompetitorAnalysis"])

    def test_prohibited_enrichment_and_unrestricted_counts_are_rejected(self):
        prohibited = (
            "scrapeContacts", "scrapeReviewsPersonalData", "scrapeImageAuthors",
            "enableCompetitorAnalysis", "verifyLeadsEnrichmentEmails",
        )
        for key in prohibited:
            with self.subTest(key=key):
                with self.assertRaises(ApifyInputError):
                    build_apify_safe_input("Fictional Testville, CA", "local service business", overrides={key: True})
        for key, value in (("maximumLeadsEnrichmentRecords", 1), ("maxReviews", 1), ("maxImages", 1), ("maxCrawledPlacesPerSearch", 21)):
            with self.subTest(key=key):
                with self.assertRaises(ApifyInputError):
                    build_apify_safe_input("Fictional Testville, CA", "local service business", overrides={key: value})

    def test_preview_operations_need_no_credentials_or_network(self):
        input_preview = preview_apify_input("Fictional Testville, CA", "local business")
        self.assertEqual(input_preview["mode"], "DRY_RUN")
        self.assertEqual(input_preview["provenance_type"], "FIXTURE")
        self.assertEqual(input_preview["input"]["countryCode"], "us")
        cost = preview_apify_cost_limits(self.database, self.campaign["id"])
        self.assertEqual(cost["max_items_per_run"], 25)
        self.assertEqual(cost["max_total_charge_usd"], 0)
        self.assertEqual(cost["max_concurrent_runs"], 1)
        status = apify_readiness(self.database, env={"APIFY_API_TOKEN": "FAKE_APIFY_TOKEN"})
        self.assertFalse(status["ready"])
        self.assertTrue(status["credential_present"])
        self.assertFalse(status["credential_value_returned"])
        self.assertNotIn("FAKE_APIFY_TOKEN", repr(status))

    def test_each_live_gate_blocks_before_fake_start(self):
        fake = FakeApifyClient()
        cases = (
            ({}, "PAUSED"),
            ({"system_state": "ACTIVE"}, "discovery_mode"),
            ({"system_state": "ACTIVE", "discovery_mode": "LIVE"}, "campaign"),
            ({"system_state": "ACTIVE", "discovery_mode": "LIVE", "campaign": "ACTIVE"}, "credential"),
            ({"system_state": "ACTIVE", "discovery_mode": "LIVE", "campaign": "ACTIVE", "token": True}, "quota"),
            ({"system_state": "ACTIVE", "discovery_mode": "LIVE", "campaign": "ACTIVE", "token": True, "quota": 1}, "charge"),
            ({"system_state": "ACTIVE", "discovery_mode": "LIVE", "campaign": "ACTIVE", "token": True, "quota": 1, "charge": 1, "max_items": 0}, "items"),
        )
        for settings, expected in cases:
            with self.subTest(expected=expected):
                with self.database.connection:
                    self.database.connection.execute("UPDATE system_config SET value=? WHERE key='system_state'", (settings.get("system_state", "PAUSED"),))
                    self.database.connection.execute("UPDATE system_config SET value=? WHERE key='discovery_mode'", (settings.get("discovery_mode", "DRY_RUN"),))
                    self.database.connection.execute("UPDATE system_config SET value=? WHERE key='apify_daily_run_cap'", (str(settings.get("quota", 0)),))
                    self.database.connection.execute("UPDATE system_config SET value=? WHERE key='apify_max_total_charge_usd'", (str(settings.get("charge", 0)),))
                    self.database.connection.execute("UPDATE system_config SET value=? WHERE key='apify_max_items_per_run'", (str(settings.get("max_items", 20)),))
                    self.database.connection.execute("UPDATE campaigns SET status=? WHERE id=?", (settings.get("campaign", "INACTIVE"), self.campaign["id"]))
                env = {"APIFY_API_TOKEN": "FAKE_APIFY_TOKEN"} if settings.get("token") else {}
                with self.assertRaisesRegex(ApifyRunBlockedError, expected):
                    start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env=env, operator_confirmation=True, apify_client=fake)
        self.assertEqual(fake.start_calls, [])

    def test_start_passes_limits_and_persists_safe_running_run(self):
        self.activate_live(quota=1, charge=2, max_items=17)
        fake = FakeApifyClient()
        result = start_apify_live_run(
            self.database, self.campaign["id"], "Fictional Testville, CA", "local service business",
            env={"APIFY_API_TOKEN": "FAKE_APIFY_TOKEN"}, operator_confirmation=True, apify_client=fake,
        )
        self.assertEqual(result["state"], ApifyRunState.RUNNING.value)
        self.assertEqual(len(fake.start_calls), 1)
        call = fake.start_calls[0]
        self.assertEqual(call["actor_id"], "compass~crawler-google-places")
        self.assertEqual(call["max_items"], 17)
        self.assertEqual(call["max_total_charge_usd"], 2)
        self.assertEqual(call["run_input"]["searchStringsArray"], ["local service business"])
        row = self.database.connection.execute("SELECT * FROM apify_runs WHERE remote_run_id=?", (result["remote_run_id"],)).fetchone()
        self.assertEqual(row["state"], "RUNNING")
        self.assertEqual(row["mode"], "LIVE")
        self.assertNotIn("FAKE_APIFY_TOKEN", repr(dict(row)))

    def test_only_one_concurrent_run_is_allowed(self):
        self.activate_live(quota=2, charge=1)
        fake = FakeApifyClient()
        start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env={"APIFY_API_TOKEN": "FAKE"}, operator_confirmation=True, apify_client=fake)
        with self.assertRaisesRegex(ApifyRunBlockedError, "in-progress"):
            start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local business", env={"APIFY_API_TOKEN": "FAKE"}, operator_confirmation=True, apify_client=fake)
        self.assertEqual(len(fake.start_calls), 1)

    def test_poll_resumes_persisted_run_after_simulated_restart_without_starting_again(self):
        self.activate_live(quota=1, charge=1)
        first_client = FakeApifyClient()
        started = start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env={"APIFY_API_TOKEN": "FAKE"}, operator_confirmation=True, apify_client=first_client)
        restarted_client = FakeApifyClient(statuses=[{"id": started["remote_run_id"], "status": "RUNNING", "defaultDatasetId": "dataset-fictional-001"}])
        polled = poll_apify_run(self.database, started["remote_run_id"], apify_client=restarted_client)
        self.assertEqual(polled["state"], "RUNNING")
        self.assertEqual(restarted_client.start_calls, [])
        self.assertEqual(restarted_client.status_calls, [started["remote_run_id"]])

    def test_failed_run_is_terminal_and_is_not_automatically_restarted(self):
        self.activate_live(quota=1, charge=1)
        fake = FakeApifyClient(statuses=[{"id": "run-fictional-001", "status": "FAILED"}])
        started = start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env={"APIFY_API_TOKEN": "FAKE"}, operator_confirmation=True, apify_client=fake)
        failed = poll_apify_run(self.database, started["remote_run_id"], apify_client=fake)
        self.assertEqual(failed["state"], "FAILED")
        self.assertEqual(len(fake.start_calls), 1)
        terminal = poll_apify_run(self.database, started["remote_run_id"], apify_client=fake)
        self.assertEqual(terminal["state"], "FAILED")
        self.assertEqual(len(fake.start_calls), 1)
        self.assertEqual(len(fake.status_calls), 1)

    def test_dataset_is_fetchable_only_after_succeeded_and_maps_allowlisted_fields(self):
        self.activate_live(quota=1, charge=1)
        fake = FakeApifyClient(
            statuses=[{"id": "run-fictional-001", "status": "SUCCEEDED", "defaultDatasetId": "dataset-fictional-001"}],
            dataset=[apify_record()],
        )
        started = start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env={"APIFY_API_TOKEN": "FAKE"}, operator_confirmation=True, apify_client=fake)
        with self.assertRaisesRegex(ApifyRunBlockedError, "SUCCEEDED"):
            fetch_apify_dataset(self.database, started["remote_run_id"], apify_client=fake)
        self.assertEqual(fake.dataset_calls, [])
        poll_apify_run(self.database, started["remote_run_id"], apify_client=fake)
        summary = fetch_apify_dataset(self.database, started["remote_run_id"], apify_client=fake)
        self.assertEqual(summary["item_count"], 1)
        self.assertEqual(fake.dataset_calls, ["dataset-fictional-001"])
        candidate = self.database.connection.execute("SELECT * FROM discovery_candidates").fetchone()
        self.assertEqual(candidate["business_name"], "Fictional US Local Service Business")
        self.assertEqual(candidate["country"], "US")
        self.assertEqual(candidate["google_place_id"], "apify-place-001")
        self.assertEqual(candidate["business_status"], "OPERATIONAL")
        observed = json.loads(candidate["observed_values"])
        self.assertNotIn("reviews", observed)
        self.assertNotIn("leadEnrichment", observed)
        self.assertNotIn("Reviewer Identity", repr(observed))
        self.assertNotIn("personal@example.test", repr(observed))

    def test_suppression_and_duplicate_rules_are_reused_for_apify_dataset(self):
        self.activate_live(quota=1, charge=1)
        self.database.add_suppression(domain="fictional-business.test", reason="fictional suppression")
        fake = FakeApifyClient(
            statuses=[{"id": "run-fictional-001", "status": "SUCCEEDED", "defaultDatasetId": "dataset-fictional-001"}],
            dataset=[apify_record()],
        )
        started = start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env={"APIFY_API_TOKEN": "FAKE"}, operator_confirmation=True, apify_client=fake)
        poll_apify_run(self.database, started["remote_run_id"], apify_client=fake)
        summary = fetch_apify_dataset(self.database, started["remote_run_id"], apify_client=fake)
        self.assertEqual(summary["outcomes"][IngestionOutcome.SUPPRESSED.value], 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)
        self.assertNotIn("fictional-business.test", repr(summary))

    def test_safe_summary_and_errors_never_expose_token(self):
        self.activate_live(quota=1, charge=1)
        token = "FAKE_APIFY_SECRET_TOKEN"
        fake = FakeApifyClient(start_error=RuntimeError(token))
        with self.assertRaises(ApifyRequestError) as error:
            start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env={"APIFY_API_TOKEN": token}, operator_confirmation=True, apify_client=fake)
        self.assertNotIn(token, str(error.exception))
        self.assertEqual(error.exception.category, "source_error")
        # The fake raises the secret to prove the wrapper must sanitize it.
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM apify_runs").fetchone()[0], 0)
        self.assertNotIn(token, repr(summarize_apify_run(self.database)))

    def test_apify_closure_flags_become_inactive_outcome_without_creating_lead(self):
        self.activate_live(quota=1, charge=1)
        fake = FakeApifyClient(
            statuses=[{"status": "SUCCEEDED", "defaultDatasetId": "dataset-fictional-001"}],
            dataset=[apify_record(businessStatus=None, permanentlyClosed=True)],
        )
        started = start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env={"APIFY_API_TOKEN": "FAKE"}, operator_confirmation=True, apify_client=fake)
        poll_apify_run(self.database, started["remote_run_id"], apify_client=fake)
        summary = fetch_apify_dataset(self.database, started["remote_run_id"], apify_client=fake)
        self.assertEqual(summary["outcomes"][IngestionOutcome.INACTIVE_BUSINESS.value], 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)

    def test_actor_id_is_replaceable_through_configuration(self):
        self.activate_live(quota=1, charge=1)
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value='fictional/custom-places-actor' WHERE key='apify_actor_id'")
        fake = FakeApifyClient()
        start_apify_live_run(self.database, self.campaign["id"], "Fictional Testville, CA", "local service business", env={"APIFY_API_TOKEN": "FAKE"}, operator_confirmation=True, apify_client=fake)
        self.assertEqual(fake.start_calls[0]["actor_id"], "fictional/custom-places-actor")

        self.assertEqual(migrate_step5(self.database), 7)
        self.assertEqual(migrate_step5(self.database), 7)
        self.assertEqual({state.value for state in ApifyRunState}, {"RUNNING", "SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"})
        tables = {row[0] for row in self.database.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertIn("apify_runs", tables)


if __name__ == "__main__":
    unittest.main()
