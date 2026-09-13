import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from paldo_os_outbound import DEFAULT_DB_PATH, Database, migrate_step3
from step3_discovery import (
    APIFY_SOURCE,
    GOOGLE_PLACES_SOURCE,
    ApifyAdapter,
    CandidateStatus,
    DiscoveryMode,
    GooglePlacesAdapter,
    IngestionOutcome,
    LiveModeBlockedError,
    NormalizedCandidate,
    check_live_guard,
    import_fictional_fixtures,
    ingest_candidate,
    list_run_summary,
    normalize_candidate,
    preview_dry_run_ingestion,
    run_discovery,
    show_discovery_status,
    stable_source_payload_hash,
    validate_fixture_input,
)


class Step3DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.database = Database(Path(self.temporary_directory.name) / "step3.sqlite3")
        migrate_step3(self.database)
        self.addCleanup(self.database.close)

    @staticmethod
    def google_record(record_id="google-001", **overrides):
        record = {
            "source_record_id": record_id,
            "source_url": " https://maps.example/places/google-001#details ",
            "name": "  Café   NFKC  Studio ",
            "business_category": " local service business ",
            "website": " HTTPS://WWW.Example-Business.test/booking ",
            "email": " Owner@WWW.Example-Business.test ",
            "phone": "+63 (917) 555-0101",
            "street_address": " 123   Example\nStreet ",
            "city": " Westport ",
            "region": " northfield ",
            "postal_code": "2009",
            "country": " Philippines ",
            "latitude": 15.145,
            "longitude": 120.588,
            "google_place_id": "place-fictional-001",
            "booking_url": "https://booking.example/fictional-business",
            "business_status": "OPERATIONAL",
            "ignored_raw_field": "FAKE_SECRET_DO_NOT_STORE",
        }
        record.update(overrides)
        return record

    @staticmethod
    def apify_record(record_id="apify-001", **overrides):
        record = {
            "source_record_id": record_id,
            "source_url": "https://directory.example/fictional-business",
            "name": "Fictional Apify Wellness",
            "category": "wellness",
            "website_url": "https://www.apify-wellness.test",
            "public_business_email": "hello@apify-wellness.test",
            "public_business_phone": "+1 (555) 010-2020",
            "address": "44 Fictional Avenue",
            "city": "Fictional Harbor",
            "region_state": "California",
            "postal_code": "90210",
            "country": "US",
            "booking_url": "https://apify-wellness.test/book",
            "business_status": "OPERATIONAL",
        }
        record.update(overrides)
        return record

    def _candidate(self, source=GOOGLE_PLACES_SOURCE, **overrides):
        raw = self.google_record(**overrides)
        return normalize_candidate(raw, source=source, provenance_type="FIXTURE")

    def _set_live_prerequisites(self, source=GOOGLE_PLACES_SOURCE):
        campaign = self.database.get_campaign_by_name("Primary region local services")
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value = 'ACTIVE' WHERE key = 'system_state'")
            self.database.connection.execute("UPDATE campaigns SET status = 'INACTIVE'")
            self.database.connection.execute("UPDATE campaigns SET status = 'ACTIVE' WHERE id = ?", (campaign["id"],))
            key = "google_places_daily_request_cap" if source == GOOGLE_PLACES_SOURCE else "apify_daily_run_cap"
            self.database.connection.execute("UPDATE system_config SET value = '1' WHERE key = ?", (key,))
        return campaign["id"]

    def test_default_mode_caps_and_safety_settings_are_durable(self):
        config = self.database.read_config()
        self.assertEqual(config["discovery_mode"], "DRY_RUN")
        self.assertEqual(config["google_places_daily_request_cap"], 0)
        self.assertEqual(config["apify_daily_run_cap"], 0)
        self.assertEqual(config["system_state"], "PAUSED")
        self.assertEqual(config["daily_message_cap"], 0)
        self.assertEqual(config["max_touches"], 3)
        self.assertEqual(config["followup_days"], 3)
        self.assertEqual(config["minimum_qualification_score"], 70)
        self.assertTrue(all(row["status"] == "INACTIVE" for row in self.database.get_campaigns()))

    def test_named_ingestion_outcomes_are_supported(self):
        self.assertEqual(
            {outcome.value for outcome in IngestionOutcome},
            {
                "ACCEPTED", "UPDATED", "DUPLICATE", "POSSIBLE_DUPLICATE_HOLD",
                "SUPPRESSED", "INVALID", "INACTIVE_BUSINESS", "INSUFFICIENT_IDENTITY", "SOURCE_ERROR",
            },
        )

    def test_suppression_precedes_inactive_status_and_prevents_all_downstream_records(self):
        candidate = self._candidate(record_id="suppressed-closed", business_status="CLOSED_PERMANENTLY")
        self.database.add_suppression(domain=candidate.normalized_domain, reason="fictional suppression")
        result = ingest_candidate(self.database, candidate)
        self.assertEqual(result.outcome, IngestionOutcome.SUPPRESSED)
        self.assertIsNone(result.lead_id)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM lead_scores").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)
        row = self.database.connection.execute(
            "SELECT ingestion_outcome, rejection_hold_reason FROM discovery_candidates WHERE id = ?",
            (result.candidate_id,),
        ).fetchone()
        self.assertEqual(row["ingestion_outcome"], "SUPPRESSED")
        self.assertIn("suppressed", row["rejection_hold_reason"].lower())

    def test_suppressed_domain_remains_suppressed_across_sources(self):
        self.database.add_suppression(domain="example-business.test", reason="fictional domain suppression")
        google_result = ingest_candidate(self.database, self._candidate(record_id="suppressed-google"))
        apify_candidate = normalize_candidate(
            self.apify_record(
                record_id="suppressed-apify",
                website_url="https://www.example-business.test/other",
                public_business_email="different@other.test",
            ),
            source=APIFY_SOURCE,
            provenance_type="FIXTURE",
        )
        apify_result = ingest_candidate(self.database, apify_candidate)
        self.assertEqual(google_result.outcome, IngestionOutcome.SUPPRESSED)
        self.assertEqual(apify_result.outcome, IngestionOutcome.SUPPRESSED)
        preview = preview_dry_run_ingestion(
            self.database,
            ApifyAdapter(),
            raw_records=[self.apify_record(record_id="suppressed-preview", website_url="https://example-business.test", public_business_email="different@example-business.test")],
        )
        self.assertNotIn("different@example-business.test", repr(preview))
        self.assertNotIn("example-business.test", repr(preview))
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)

    def test_named_outcomes_cover_identity_invalid_and_inactive_decisions(self):
        insufficient = ingest_candidate(
            self.database,
            self._candidate(record_id="insufficient", website=None, email=None),
        )
        invalid = ingest_candidate(
            self.database,
            self._candidate(record_id="invalid", website="javascript:alert(1)"),
        )
        inactive = ingest_candidate(
            self.database,
            self._candidate(record_id="inactive", business_status="CLOSED_PERMANENTLY"),
        )
        self.assertEqual(insufficient.outcome, IngestionOutcome.INSUFFICIENT_IDENTITY)
        self.assertEqual(invalid.outcome, IngestionOutcome.INVALID)
        self.assertEqual(inactive.outcome, IngestionOutcome.INACTIVE_BUSINESS)

    def test_newer_same_source_evidence_updates_but_older_evidence_cannot_replace_it(self):
        initial = normalize_candidate(
            self.google_record(
                record_id="google-evidence-versioned",
                website="https://old-evidence.test",
                email="old@old-evidence.test",
                google_place_id="place-versioned",
            ),
            source=GOOGLE_PLACES_SOURCE,
            collected_at="2026-09-01T10:00:00+00:00",
            provenance_type="FIXTURE",
        )
        first = ingest_candidate(self.database, initial)
        newer = normalize_candidate(
            self.google_record(
                record_id="google-evidence-versioned",
                website="https://new-evidence.test",
                email="new@new-evidence.test",
                google_place_id="place-versioned",
            ),
            source=GOOGLE_PLACES_SOURCE,
            collected_at="2026-09-02T10:00:00+00:00",
            provenance_type="FIXTURE",
        )
        updated = ingest_candidate(self.database, newer)
        older = normalize_candidate(
            self.google_record(
                record_id="google-evidence-versioned",
                website="https://older-evidence.test",
                email="older@older-evidence.test",
                google_place_id="place-versioned",
            ),
            source=GOOGLE_PLACES_SOURCE,
            collected_at="2026-08-31T10:00:00+00:00",
            provenance_type="FIXTURE",
        )
        not_replaced = ingest_candidate(self.database, older)
        row = self.database.connection.execute(
            "SELECT website_url, normalized_email, collected_at FROM discovery_candidates WHERE id = ?",
            (first.candidate_id,),
        ).fetchone()
        self.assertEqual(updated.outcome, IngestionOutcome.UPDATED)
        self.assertEqual(not_replaced.outcome, IngestionOutcome.DUPLICATE)
        self.assertEqual(row["website_url"], newer.website_url)
        self.assertEqual(row["normalized_email"], newer.normalized_email)
        self.assertEqual(row["collected_at"], newer.collected_at)

    def test_run_summary_and_safe_command_helpers_distinguish_dry_run_without_writes_for_preview(self):
        status = show_discovery_status(self.database)
        self.assertEqual(status["mode"], "DRY_RUN")
        self.assertEqual(status["system_state"], "PAUSED")
        validation = validate_fixture_input([self.google_record(record_id="validate-only")], source=GOOGLE_PLACES_SOURCE)
        self.assertTrue(validation["valid"])
        before = {
            table: self.database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("discovery_candidates", "leads", "events", "discovery_runs")
        }
        preview = preview_dry_run_ingestion(
            self.database,
            GooglePlacesAdapter(),
            raw_records=[self.google_record(record_id="preview-only")],
        )
        after = {
            table: self.database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("discovery_candidates", "leads", "events", "discovery_runs")
        }
        self.assertEqual(preview["mode"], "DRY_RUN")
        self.assertEqual(before, after)
        results = import_fictional_fixtures(
            self.database,
            GooglePlacesAdapter(),
            raw_records=[self.google_record(record_id="import-command")],
        )
        self.assertEqual(results[0].outcome, IngestionOutcome.ACCEPTED)
        summaries = list_run_summary(self.database)
        self.assertGreaterEqual(len(summaries), 1)
        self.assertEqual(summaries[-1]["mode"], "DRY_RUN")
        self.assertEqual(summaries[-1]["provenance_type"], "FIXTURE")
        self.assertEqual(summaries[-1]["accepted_count"], 1)

    def test_source_error_is_safe_and_auditable_without_secret_output(self):
        secret = "FAKE_SOURCE_SECRET"
        def failing_collector(**kwargs):
            raise RuntimeError(secret)
        results = run_discovery(self.database, ApifyAdapter(collector=failing_collector))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].outcome, IngestionOutcome.SOURCE_ERROR)
        self.assertNotIn(secret, repr(results))
        summaries = list_run_summary(self.database)
        self.assertEqual(summaries[-1]["status"], "SOURCE_ERROR")
        self.assertNotIn(secret, repr(summaries))

    def test_candidate_contract_has_all_required_fields(self):
        candidate = self._candidate()
        required = {
            "source", "source_record_id", "source_url", "collected_at", "business_name",
            "business_category", "website_url", "normalized_domain", "public_business_email",
            "normalized_email", "public_business_phone", "normalized_phone", "street_address",
            "city", "region_state", "postal_code", "country", "latitude", "longitude",
            "google_place_id", "booking_url", "business_status", "raw_payload_hash",
            "ingestion_status", "rejection_hold_reason", "provenance_mode", "provenance_type",
            "observed_values",
        }
        self.assertIsInstance(candidate, NormalizedCandidate)
        self.assertTrue(required.issubset(candidate.as_dict()))
        self.assertEqual(candidate.source, GOOGLE_PLACES_SOURCE)
        self.assertEqual(candidate.source_record_id, "google-001")
        self.assertEqual(candidate.ingestion_status, CandidateStatus.PENDING)
        self.assertEqual(candidate.provenance_mode, DiscoveryMode.DRY_RUN)
        self.assertEqual(candidate.provenance_type, "FIXTURE")

    def test_normalization_covers_urls_domain_email_names_addresses_labels_and_booking(self):
        candidate = self._candidate()
        self.assertEqual(candidate.source_url, "https://maps.example/places/google-001")
        self.assertEqual(candidate.website_url, "https://example-business.test/booking")
        self.assertEqual(candidate.booking_url, "https://booking.example/fictional-business")
        self.assertEqual(candidate.normalized_domain, "example-business.test")
        self.assertEqual(candidate.public_business_email, "Owner@WWW.Example-Business.test")
        self.assertEqual(candidate.normalized_email, "owner@example-business.test")
        self.assertEqual(candidate.business_name, "Café NFKC Studio")
        self.assertEqual(candidate.street_address, "123 Example Street")
        self.assertEqual(candidate.city, "Westport")
        self.assertEqual(candidate.region_state, "Northfield")
        self.assertEqual(candidate.country, "PH")

    def test_phone_normalization_is_safe_for_ph_and_us_and_preserves_ambiguous_original(self):
        ph = self._candidate(phone="0917 555 0101", country="PH")
        us = normalize_candidate(
            self.apify_record(public_business_phone="(555) 010-2020", country="United States"),
            source=APIFY_SOURCE,
            provenance_type="FIXTURE",
        )
        ambiguous = self._candidate(phone="0917 555 0101", country=None)
        self.assertEqual(ph.normalized_phone, "+639175550101")
        self.assertEqual(us.normalized_phone, "+15550102020")
        self.assertIsNone(ambiguous.normalized_phone)
        self.assertEqual(ambiguous.public_business_phone, "0917 555 0101")
        self.assertEqual(ambiguous.observed_values["public_business_phone"], "0917 555 0101")

    def test_google_and_apify_fixture_adapters_normalize_without_network(self):
        google = GooglePlacesAdapter()
        apify = ApifyAdapter(collector=lambda: (_ for _ in ()).throw(AssertionError("network called")))
        google_candidates = google.normalize_records([self.google_record()])
        apify_candidates = apify.normalize_records([self.apify_record()])
        self.assertEqual(google_candidates[0].source, GOOGLE_PLACES_SOURCE)
        self.assertEqual(apify_candidates[0].source, APIFY_SOURCE)
        self.assertEqual(apify_candidates[0].normalized_domain, "apify-wellness.test")
        self.assertEqual(GooglePlacesAdapter.FUTURE_HUBS, ("Westport", "Riverton", "Fairview", "Harbor area"))
        outside_hub = google.normalize_records([self.google_record(city="Eastport", record_id="google-guagua")])[0]
        self.assertEqual(outside_hub.ingestion_status, CandidateStatus.PENDING)

    def test_apify_uses_replaceable_collector_for_supplied_fixture_records(self):
        adapter = ApifyAdapter(
            collector=lambda **kwargs: [self.apify_record(record_id="apify-collected")]
        )
        records = adapter.collect_records()
        self.assertEqual(records[0]["source_record_id"], "apify-collected")

    def test_run_discovery_uses_replaceable_apify_fixture_collector_in_dry_run(self):
        adapter = ApifyAdapter(
            collector=lambda **kwargs: [self.apify_record(record_id="apify-run-collected")]
        )
        results = run_discovery(self.database, adapter)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].legacy_status, CandidateStatus.ACCEPTED)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 1)

    def test_stable_hash_is_allowlisted_and_order_independent(self):
        first = self.google_record()
        second = dict(reversed(list(first.items())))
        self.assertEqual(stable_source_payload_hash(first), stable_source_payload_hash(second))
        candidate = self._candidate()
        self.assertNotIn("ignored_raw_field", candidate.observed_values)
        self.assertNotEqual(candidate.raw_payload_hash, "")

    def test_domain_only_record_and_google_url_are_normalized(self):
        candidate = normalize_candidate(
            {
                "record_id": "domain-only",
                "url": "https://maps.example/domain-only",
                "name": "Domain Only Fixture",
                "domain": " WWW.Domain-Only.test. ",
                "country": "US",
            },
            source=GOOGLE_PLACES_SOURCE,
            provenance_type="FIXTURE",
        )
        self.assertEqual(candidate.source_url, "https://maps.example/domain-only")
        self.assertEqual(candidate.normalized_domain, "domain-only.test")
        self.assertEqual(candidate.ingestion_status, CandidateStatus.PENDING)

    def test_config_reads_for_step3_are_typed_and_fall_back_safely(self):
        with self.database.connection:
            self.database.connection.execute(
                "UPDATE system_config SET value = ?, value_type = ? WHERE key = ?",
                ("not-a-mode", "text", "discovery_mode"),
            )
            self.database.connection.execute(
                "UPDATE system_config SET value = ?, value_type = ? WHERE key = ?",
                ("not-an-integer", "integer", "google_places_daily_request_cap"),
            )
        self.assertEqual(self.database.get_config("discovery_mode"), "DRY_RUN")
        self.assertEqual(self.database.get_config("google_places_daily_request_cap"), 0)

    def test_dry_run_without_credentials_accepts_fixture_and_links_provenance(self):
        self.assertIsNone(__import__("os").environ.get("GOOGLE_PLACES_API_KEY"))
        result = run_discovery(
            self.database,
            GooglePlacesAdapter(),
            raw_records=[self.google_record()],
            mode=DiscoveryMode.DRY_RUN,
        )[0]
        self.assertEqual(result.legacy_status, CandidateStatus.ACCEPTED)
        self.assertIsNotNone(result.lead_id)
        candidate = self.database.connection.execute(
            "SELECT * FROM discovery_candidates WHERE id = ?", (result.candidate_id,)
        ).fetchone()
        self.assertEqual(candidate["source"], GOOGLE_PLACES_SOURCE)
        self.assertEqual(candidate["source_record_id"], "google-001")
        self.assertEqual(candidate["provenance_mode"], "DRY_RUN")
        self.assertEqual(candidate["provenance_type"], "FIXTURE")
        self.assertEqual(candidate["ingestion_decision"], "ACCEPT")
        self.assertEqual(self.database.get_lead(result.lead_id)["status"], "DISCOVERED")

    def test_dry_run_cannot_seed_the_durable_database_with_fixtures(self):
        self.database.path = DEFAULT_DB_PATH
        with self.assertRaises(LiveModeBlockedError) as error:
            run_discovery(
                self.database,
                GooglePlacesAdapter(),
                raw_records=[self.google_record()],
                mode=DiscoveryMode.DRY_RUN,
            )
        self.assertIn("durable database", str(error.exception))
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM discovery_candidates").fetchone()[0], 0)

    def test_direct_live_provenance_ingestion_is_blocked_until_a_future_live_step(self):
        candidate = normalize_candidate(
            self.google_record(record_id="future-live"),
            source=GOOGLE_PLACES_SOURCE,
            mode=DiscoveryMode.LIVE,
            provenance_type="LIVE",
        )
        with self.assertRaises(LiveModeBlockedError):
            ingest_candidate(self.database, candidate)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)

    def test_secret_in_ignored_raw_field_is_absent_from_database_and_event(self):
        result = ingest_candidate(self.database, self._candidate())
        self.assertEqual(result.legacy_status, CandidateStatus.ACCEPTED)
        for table in ("discovery_candidates", "events", "leads"):
            values = self.database.connection.execute(f"SELECT * FROM {table}").fetchall()
            self.assertNotIn("FAKE_SECRET_DO_NOT_STORE", repr([tuple(value) for value in values]))
        event = self.database.get_event(result.event_id)
        self.assertEqual(set(event["metadata"]), {
            "collected_at", "ingestion_status", "provenance_mode", "provenance_type",
            "raw_payload_hash", "source", "source_record_id", "source_url",
        })

    def test_rejects_unsafe_contact_urls_without_persisting_credentials(self):
        secret = "FAKE_URL_SECRET"
        candidate = self._candidate(
            record_id="unsafe-secret-url",
            source_url=f"https://fixture:{secret}@maps.example/place",
            website=f"https://fixture:{secret}@example.test",
        )
        self.assertEqual(candidate.ingestion_status, CandidateStatus.REJECTED)
        result = ingest_candidate(self.database, candidate)
        self.assertEqual(result.legacy_status, CandidateStatus.REJECTED)
        for table in ("discovery_candidates", "events"):
            rows = self.database.connection.execute(f"SELECT * FROM {table}").fetchall()
            self.assertNotIn(secret, repr([tuple(row) for row in rows]))

    def test_rejects_malformed_closed_private_and_unsafe_records(self):
        malformed = normalize_candidate({"name": "Missing identity"}, source=GOOGLE_PLACES_SOURCE)
        closed = self._candidate(record_id="closed", business_status="CLOSED_PERMANENTLY")
        private = self._candidate(record_id="private", private_contact=True)
        unsafe = self._candidate(record_id="unsafe", website="javascript:alert(1)")
        for candidate in (malformed, closed, private, unsafe):
            result = ingest_candidate(self.database, candidate)
            self.assertEqual(result.legacy_status, CandidateStatus.REJECTED)
            self.assertIsNone(result.lead_id)
            self.assertTrue(result.reason)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)

    def test_missing_identity_is_held_without_invented_match(self):
        candidate = self._candidate(
            record_id="no-identity", website=None, email=None, phone="0917 555 0101", country="PH"
        )
        self.assertIsNone(candidate.normalized_domain)
        result = ingest_candidate(self.database, candidate)
        self.assertEqual(result.legacy_status, CandidateStatus.HELD)
        self.assertIn("identity", result.reason.lower())
        self.assertIsNone(result.lead_id)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)

    def test_suppressed_email_or_domain_is_rejected(self):
        self.database.add_suppression(email="owner@example-business.test", reason="fictional suppression")
        result = ingest_candidate(self.database, self._candidate())
        self.assertEqual(result.legacy_status, CandidateStatus.REJECTED)
        self.assertIn("suppressed", result.reason.lower())
        self.assertIsNone(result.lead_id)

    def test_cross_source_email_or_domain_duplicate_is_auditable_without_merge(self):
        first = ingest_candidate(self.database, self._candidate())
        same_email = normalize_candidate(
            self.apify_record(record_id="apify-email", public_business_email="OWNER@example-business.test", website_url="https://different.test"),
            source=APIFY_SOURCE,
            provenance_type="FIXTURE",
        )
        same_domain = normalize_candidate(
            self.apify_record(record_id="apify-domain", public_business_email="other@different.test", website_url="https://WWW.Example-Business.test"),
            source=APIFY_SOURCE,
            provenance_type="FIXTURE",
        )
        email_result = ingest_candidate(self.database, same_email)
        domain_result = ingest_candidate(self.database, same_domain)
        self.assertEqual(first.legacy_status, CandidateStatus.ACCEPTED)
        self.assertEqual(email_result.legacy_status, CandidateStatus.DUPLICATE)
        self.assertEqual(domain_result.legacy_status, CandidateStatus.DUPLICATE)
        self.assertIsNone(email_result.lead_id)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM discovery_candidates").fetchone()[0], 3)

    def test_source_record_repeat_is_idempotent_for_rows_leads_and_events(self):
        candidate = self._candidate()
        first = ingest_candidate(self.database, candidate)
        counts_before = {
            table: self.database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("discovery_candidates", "leads", "events")
        }
        second = ingest_candidate(self.database, candidate)
        counts_after = {
            table: self.database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("discovery_candidates", "leads", "events")
        }
        self.assertEqual(first.candidate_id, second.candidate_id)
        self.assertTrue(second.idempotent)
        self.assertEqual(counts_before, counts_after)

    def test_google_place_repeat_is_duplicate_without_lead_merge(self):
        first = ingest_candidate(self.database, self._candidate())
        repeated_place = self._candidate(record_id="google-002", email="other@repeat-place.test", website="https://repeat-place.test")
        second = ingest_candidate(self.database, repeated_place)
        self.assertEqual(first.legacy_status, CandidateStatus.ACCEPTED)
        self.assertEqual(second.legacy_status, CandidateStatus.DUPLICATE)
        self.assertIsNone(second.lead_id)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 1)

    def test_possible_duplicate_is_held_without_merge_and_name_alone_never_deduplicates(self):
        first = ingest_candidate(self.database, self._candidate())
        possible = self._candidate(
            record_id="possible", google_place_id="place-fictional-possible", email="another@possible.test", website="https://possible.test",
            phone="+63 917 555 0101", street_address="123 Example Street", business_name="Café NFKC Studio",
        )
        held = ingest_candidate(self.database, possible)
        same_name_different_identity = self._candidate(
            record_id="same-name", google_place_id="place-fictional-same-name", email="third@different.test", website="https://different-name.test",
            phone="+63 917 555 0199", street_address="999 Other Street", business_name="Café NFKC Studio",
        )
        accepted = ingest_candidate(self.database, same_name_different_identity)
        self.assertEqual(first.legacy_status, CandidateStatus.ACCEPTED)
        self.assertEqual(held.legacy_status, CandidateStatus.HELD)
        self.assertIn("possible duplicate", held.reason.lower())
        self.assertIsNone(held.lead_id)
        self.assertEqual(accepted.legacy_status, CandidateStatus.ACCEPTED)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 2)

    def test_similar_name_with_same_address_is_possible_duplicate_hold(self):
        ingest_candidate(self.database, self._candidate())
        similar = self._candidate(
            record_id="similar-name", google_place_id="place-fictional-similar",
            email="similar@different.test", website="https://similar-name.test",
            phone="+63 917 555 0198", street_address="123 Example Street",
            business_name="Cafe NFKC Studio",
        )
        result = ingest_candidate(self.database, similar)
        self.assertEqual(result.legacy_status, CandidateStatus.HELD)
        self.assertIn("possible duplicate", result.reason.lower())
        self.assertIsNone(result.lead_id)

    def test_stronger_existing_record_is_not_overwritten(self):
        strong = self._candidate()
        first = ingest_candidate(self.database, strong)
        weaker = normalize_candidate(
            self.google_record(record_id="google-001", name="Weaker Name", website=None, email=None, phone=None),
            source=GOOGLE_PLACES_SOURCE,
            provenance_type="FIXTURE",
        )
        second = ingest_candidate(self.database, weaker)
        row = self.database.connection.execute(
            "SELECT business_name, website_url, normalized_email, raw_payload_hash FROM discovery_candidates WHERE id = ?",
            (first.candidate_id,),
        ).fetchone()
        self.assertTrue(second.idempotent)
        self.assertEqual(row["business_name"], strong.business_name)
        self.assertEqual(row["website_url"], strong.website_url)
        self.assertEqual(row["normalized_email"], strong.normalized_email)
        self.assertEqual(row["raw_payload_hash"], strong.raw_payload_hash)

    def test_live_guard_blocks_paused_inactive_zero_quota_missing_credentials_and_operator_flag(self):
        campaign = self.database.get_campaign_by_name("Primary region local services")
        blocked, reason = check_live_guard(self.database, source=GOOGLE_PLACES_SOURCE, campaign_id=campaign["id"], env={}, operator_explicitly_requested_live=True)
        self.assertFalse(blocked)
        self.assertIn("PAUSED", reason)
        self._set_live_prerequisites()
        with self.database.connection:
            self.database.connection.execute("UPDATE campaigns SET status = 'INACTIVE' WHERE id = ?", (campaign["id"],))
        blocked, reason = check_live_guard(self.database, source=GOOGLE_PLACES_SOURCE, campaign_id=campaign["id"], env={}, operator_explicitly_requested_live=True)
        self.assertFalse(blocked)
        self.assertIn("ACTIVE", reason)
        self._set_live_prerequisites()
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value = '0' WHERE key = 'google_places_daily_request_cap'")
        blocked, reason = check_live_guard(self.database, source=GOOGLE_PLACES_SOURCE, campaign_id=campaign["id"], env={}, operator_explicitly_requested_live=True)
        self.assertFalse(blocked)
        self.assertIn("quota", reason)
        self._set_live_prerequisites()
        blocked, reason = check_live_guard(self.database, source=GOOGLE_PLACES_SOURCE, campaign_id=campaign["id"], env={}, operator_explicitly_requested_live=True)
        self.assertFalse(blocked)
        self.assertIn("GOOGLE_PLACES_API_KEY", reason)
        blocked, reason = check_live_guard(self.database, source=GOOGLE_PLACES_SOURCE, campaign_id=campaign["id"], env={"GOOGLE_PLACES_API_KEY": "FAKE"}, operator_explicitly_requested_live=False)
        self.assertFalse(blocked)
        self.assertIn("operator", reason)

    def test_live_path_never_calls_injected_collector_even_when_prerequisites_are_manually_ready(self):
        campaign_id = self._set_live_prerequisites()
        adapter = ApifyAdapter(collector=lambda: (_ for _ in ()).throw(AssertionError("network called")))
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value = 'LIVE' WHERE key = 'discovery_mode'")
            self.database.connection.execute("UPDATE system_config SET value = '1' WHERE key = 'apify_daily_run_cap'")
        with self.assertRaises(LiveModeBlockedError) as error:
            run_discovery(
                self.database,
                adapter,
                mode=DiscoveryMode.LIVE,
                campaign_id=campaign_id,
                operator_explicitly_requested_live=True,
                env={"APIFY_API_TOKEN": "FAKE"},
            )
        self.assertIn("network", str(error.exception).lower())
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM discovery_candidates").fetchone()[0], 0)

    def test_migration_is_repeatable_and_has_no_raw_payload_or_credential_columns(self):
        before = self.database.connection.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 4").fetchone()[0]
        self.assertEqual(before, 1)
        migrate_step3(self.database)
        after = self.database.connection.execute("SELECT COUNT(*) FROM schema_migrations WHERE version = 4").fetchone()[0]
        self.assertEqual(after, 1)
        columns = [row[1] for row in self.database.connection.execute("PRAGMA table_info(discovery_candidates)")]
        self.assertIn("raw_payload_hash", columns)
        self.assertNotIn("raw_payload", columns)
        self.assertNotIn("credentials", columns)
        self.assertNotIn("api_key", columns)
        indexes = [row[1] for row in self.database.connection.execute("PRAGMA index_list(discovery_candidates)")]
        self.assertTrue(any("source" in index for index in indexes))
        self.assertTrue(any("email" in index for index in indexes))
        self.assertTrue(any("domain" in index for index in indexes))


if __name__ == "__main__":
    unittest.main()
