import tempfile
import unittest
from pathlib import Path

from paldo_os_outbound import Database
from step3_discovery import APIFY_SOURCE
from step6_website_enrichment import migrate_step6
from step7_pipeline import (
    PipelineBlockedError,
    PipelineInterrupted,
    PipelineState,
    SuppressedPipelineError,
    apply_operator_review,
    inspect_hold_reason,
    migrate_step7,
    pipeline_readiness,
    preview_candidate_processing,
    requalify_after_new_evidence,
    resume_pipeline,
    run_fictional_pipeline_fixture,
    run_pipeline,
    summarize_pipeline_outcome,
)
from tests.test_step6_website_enrichment import FakeDNS, FakeHTTPClient, WebsiteHTTPResponse, fixture_site, html_response


_DEFAULT = object()


class Step7PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.database = Database(Path(self.tempdir.name) / "step7.sqlite3")
        migrate_step6(self.database)
        migrate_step7(self.database)
        self.addCleanup(self.database.close)
        self.campaign = self.database.get_campaign_by_name("Secondary region local services")
        self.http = FakeHTTPClient(fixture_site())
        self.dns = FakeDNS()

    def record(self, source_record_id="fixture-1", *, website=_DEFAULT, email=_DEFAULT, phone=_DEFAULT, business_status="OPERATIONAL"):
        if website is _DEFAULT:
            website = "https://fictional-business.test"
        if email is _DEFAULT:
            email = "office@fictional-business.test"
        if phone is _DEFAULT:
            phone = "+1 555 010 2000"
        if source_record_id == "no-website-no-routes":
            website = None
            email = None
            phone = None
        return {
            "source_record_id": source_record_id,
            "business_name": "Fictional Business " + source_record_id,
            "business_category": "Local service business",
            "website_url": website,
            "public_business_email": email,
            "public_business_phone": phone,
            "business_status": business_status,
            "country": "US",
            "city": "Testville",
        }

    @staticmethod
    def evidence(*keys, source_url="https://fictional-business.test/about"):
        return [
            {
                "signal_key": key,
                "signal_value": "YES",
                "observed_or_inferred": "OBSERVED",
                "source_type": "FIXTURE_PUBLIC_WEB",
                "source_url": source_url,
                "observation": "Fictional public fixture explicitly records " + key + ".",
                "confidence": 0.95,
            }
            for key in keys
        ]

    @staticmethod
    def all_gates():
        return {
            "currently_active": "YES",
            "appointments_meaningful": "YES",
            "legitimate_public_contact_route": "YES",
            "independent_owner_led_or_accessible_local_decision_maker": "YES",
        }

    def run_fixture(self, record, *, evidence=None, gates=None, interrupt_after=None):
        return run_fictional_pipeline_fixture(
            self.database,
            campaign_id=self.campaign["id"],
            records=[record],
            evidence_by_source_record={record["source_record_id"]: evidence or []},
            gate_statuses_by_source_record={record["source_record_id"]: gates or {}},
            http_client=self.http,
            dns_client=self.dns,
            interrupt_after=interrupt_after,
        )[0]

    def test_migration_and_state_machine_are_explicit(self):
        self.assertEqual(
            {state.value for state in PipelineState},
            {
                "DISCOVERED", "INGESTION_ACCEPTED", "ENRICHMENT_PENDING", "ENRICHMENT_COMPLETE",
                "QUALIFICATION_PENDING", "QUALIFIED", "STRONG", "HOLD", "REJECTED", "SUPPRESSED",
                "ERROR_RETRYABLE", "ERROR_TERMINAL",
            },
        )
        tables = {
            row[0]
            for row in self.database.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        self.assertTrue(
            {
                "pipeline_runs", "pipeline_checkpoints", "pipeline_evidence",
                "pipeline_qualification_results", "manual_reviews",
            }.issubset(tables)
        )
        self.assertEqual(self.database.read_config()["daily_candidate_processing_cap"], 0)

    def test_readiness_blocks_live_processing_with_safe_defaults(self):
        readiness = pipeline_readiness(self.database, campaign_id=self.campaign["id"])
        self.assertFalse(readiness["ready"])
        self.assertIn("SYSTEM_PAUSED", readiness["blocking_reasons"])
        self.assertIn("DISCOVERY_NOT_LIVE", readiness["blocking_reasons"])
        self.assertIn("DAILY_CANDIDATE_CAP_ZERO", readiness["blocking_reasons"])
        with self.assertRaises(PipelineBlockedError):
            run_pipeline(
                self.database,
                [self.record()],
                campaign_id=self.campaign["id"],
                provenance_type="LIVE",
            )

    def test_preview_is_read_only_and_reports_enrichment_eligibility(self):
        before = {
            table: self.database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("discovery_candidates", "leads", "pipeline_runs")
        }
        preview = preview_candidate_processing(self.database, self.record())
        after = {
            table: self.database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("discovery_candidates", "leads", "pipeline_runs")
        }
        self.assertEqual(before, after)
        self.assertEqual(preview["outcome"], "ACCEPTED")
        self.assertTrue(preview["would_create_lead"])
        self.assertTrue(preview["website_enrichment_eligible"])

    def test_strong_clinic_reaches_strong_without_outreach(self):
        result = self.run_fixture(
            self.record(),
            evidence=self.evidence(
                "campaign_fit", "appointment_dependence", "recent_activity_demand",
                "multiple_practitioners", "multiple_services", "owner_decision_maker_reachability",
                "personalization_observation",
            ),
            gates=self.all_gates(),
        )
        self.assertEqual(result["state"], "STRONG")
        self.assertEqual(result["qualification"]["score"], 100)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)

    def test_qualifying_clinic_reaches_qualified(self):
        root = "https://qualified.test"
        self.http = FakeHTTPClient({
            root + "/robots.txt": WebsiteHTTPResponse(root + "/robots.txt", root + "/robots.txt", 404, {"content-type": "text/plain"}, b""),
            root + "/": html_response(root + "/", "<html><body><p>Appointments are available. Call the office for booking.</p></body></html>"),
        })
        result = self.run_fixture(
            self.record("qualified", website=root),
            evidence=self.evidence("campaign_fit", "appointment_dependence", "recent_activity_demand"),
            gates=self.all_gates(),
        )
        self.assertEqual(result["state"], "QUALIFIED")
        self.assertGreaterEqual(result["qualification"]["score"], 70)
        self.assertLess(result["qualification"]["score"], 80)

    def test_score_below_seventy_is_rejected_when_all_gates_pass(self):
        result = self.run_fixture(
            self.record("low-score", website=None),
            evidence=self.evidence("campaign_fit", "appointment_dependence"),
            gates=self.all_gates(),
        )
        self.assertEqual(result["state"], "REJECTED")
        self.assertLess(result["qualification"]["score"], 70)

    def test_failed_mandatory_gate_overrides_high_score(self):
        gates = self.all_gates()
        gates["appointments_meaningful"] = "NO"
        result = self.run_fixture(
            self.record("failed-gate"),
            evidence=self.evidence(
                "campaign_fit", "appointment_dependence", "recent_activity_demand",
                "multiple_practitioners", "multiple_services", "owner_decision_maker_reachability",
                "personalization_observation",
            ),
            gates=gates,
        )
        self.assertEqual(result["state"], "REJECTED")
        self.assertEqual(result["qualification"]["score"], 100)

    def test_unclear_mandatory_gate_produces_hold(self):
        result = self.run_fixture(
            self.record("unclear-gate"),
            evidence=self.evidence("campaign_fit", "appointment_dependence", "recent_activity_demand"),
            gates={
                "currently_active": "YES",
                "appointments_meaningful": "YES",
                "legitimate_public_contact_route": "YES",
            },
        )
        self.assertEqual(result["state"], "HOLD")
        hold = inspect_hold_reason(self.database, result["run_id"])
        self.assertTrue(hold["human_review_required"])
        self.assertIn("independent_owner_led_or_accessible_local_decision_maker", hold["unclear_gates"])

    def test_suppression_overrides_every_state_and_review_cannot_bypass(self):
        record = self.record("suppressed", email="suppressed@fictional-business.test")
        self.database.add_suppression(email=record["public_business_email"], reason="fictional opt-out")
        result = self.run_fixture(
            record,
            evidence=self.evidence(
                "campaign_fit", "appointment_dependence", "recent_activity_demand",
                "multiple_practitioners", "multiple_services", "owner_decision_maker_reachability",
                "personalization_observation",
            ),
            gates=self.all_gates(),
        )
        self.assertEqual(result["state"], "SUPPRESSED")
        self.assertIsNone(result["lead_id"])
        with self.assertRaises(SuppressedPipelineError):
            apply_operator_review(
                self.database,
                result["run_id"],
                exact_gate_or_signal="currently_active",
                decision="YES",
                reason="Fictional operator review cannot override suppression.",
            )
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM manual_reviews").fetchone()[0], 0)

    def test_invalid_and_inactive_candidates_do_not_create_leads_or_qualifications(self):
        results = run_fictional_pipeline_fixture(
            self.database,
            campaign_id=self.campaign["id"],
            records=[
                self.record("invalid", website="javascript:alert(1)"),
                self.record("inactive", business_status="CLOSED_PERMANENTLY"),
            ],
            http_client=self.http,
            dns_client=self.dns,
        )
        self.assertEqual([result["state"] for result in results], ["REJECTED", "REJECTED"])
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM qualification_results").fetchone()[0], 0)

    def test_duplicate_candidate_does_not_create_duplicate_lead(self):
        record = self.record("duplicate")
        results = run_fictional_pipeline_fixture(
            self.database,
            campaign_id=self.campaign["id"],
            records=[record, record],
            evidence_by_source_record={record["source_record_id"]: self.evidence("campaign_fit", "appointment_dependence")},
            gate_statuses_by_source_record={record["source_record_id"]: self.all_gates()},
            http_client=self.http,
            dns_client=self.dns,
        )
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 1)
        self.assertEqual(results[0]["run_id"], results[1]["run_id"])
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM qualification_results").fetchone()[0], 1)

    def test_missing_website_requires_alternative_evidence_but_can_continue_with_two_routes(self):
        no_identity = self.run_fixture(self.record("no-website-no-routes", website=None, email=None, phone=None))
        self.assertEqual(no_identity["state"], "HOLD")
        self.assertIsNone(no_identity["lead_id"])

        alternative = self.run_fixture(
            self.record("no-website-with-routes", website=None),
            evidence=self.evidence("campaign_fit", "appointment_dependence", "recent_activity_demand"),
            gates=self.all_gates(),
        )
        self.assertIsNotNone(alternative["lead_id"])
        self.assertNotEqual(alternative["state"], "HOLD")
        self.assertEqual(alternative["enrichment"]["skipped"], "missing_website_sufficient_alternative_evidence")

    def test_evidence_is_mapped_once_and_does_not_double_count(self):
        duplicate_evidence = self.evidence("campaign_fit", "appointment_dependence")
        duplicate_evidence.append(dict(duplicate_evidence[0]))
        result = self.run_fixture("" if False else self.record("evidence-once"), evidence=duplicate_evidence, gates=self.all_gates())
        campaign_rows = self.database.connection.execute(
            "SELECT COUNT(*) FROM pipeline_evidence WHERE lead_id = ? AND signal_key = 'campaign_fit'",
            (result["lead_id"],),
        ).fetchone()[0]
        self.assertEqual(campaign_rows, 1)
        self.assertLessEqual(result["qualification"]["score"], 100)

    def test_interrupted_run_resumes_from_checkpoint_without_repeating_enrichment(self):
        record = self.record("interrupted")
        with self.assertRaises(PipelineInterrupted) as interrupted:
            self.run_fixture(
                record,
                evidence=self.evidence(
                    "campaign_fit", "appointment_dependence", "recent_activity_demand",
                    "multiple_practitioners", "multiple_services", "owner_decision_maker_reachability",
                    "personalization_observation",
                ),
                gates=self.all_gates(),
                interrupt_after="enrichment",
            )
        run_id = interrupted.exception.run_id
        calls_after_failure = len(self.http.calls)
        resumed = resume_pipeline(self.database, run_id, http_client=self.http, dns_client=self.dns)
        self.assertEqual(len(self.http.calls), calls_after_failure)
        self.assertEqual(resumed["state"], "STRONG")
        checkpoints = {
            row["checkpoint_key"]
            for row in self.database.connection.execute(
                "SELECT checkpoint_key FROM pipeline_checkpoints WHERE pipeline_run_id = ?", (run_id,)
            )
        }
        self.assertTrue({"discovery_ingestion", "enrichment", "evidence_consolidation", "qualification"}.issubset(checkpoints))

    def test_retryable_enrichment_resumes_once_and_is_bounded(self):
        record = self.record("retryable")
        root = record["website_url"]
        self.http.routes[root + "/"] = TimeoutError("fictional timeout")
        first = run_fictional_pipeline_fixture(
            self.database,
            campaign_id=self.campaign["id"],
            records=[record],
            evidence_by_source_record={record["source_record_id"]: self.evidence("campaign_fit", "appointment_dependence", "recent_activity_demand")},
            gate_statuses_by_source_record={record["source_record_id"]: self.all_gates()},
            http_client=self.http,
            dns_client=self.dns,
        )[0]
        self.assertEqual(first["state"], "ERROR_RETRYABLE")
        self.assertEqual(first["completion_state"], "RUNNING")
        self.assertEqual(first["attempt_count"], 1)
        self.http.routes[root + "/"] = html_response(root + "/", "<html><body>Appointments are available for booking.</body></html>")
        resumed = resume_pipeline(self.database, first["run_id"], http_client=self.http, dns_client=self.dns)
        self.assertNotEqual(resumed["state"], "ERROR_RETRYABLE")
        self.assertEqual(resumed["attempt_count"], 2)
        calls = len(self.http.calls)
        terminal = resume_pipeline(self.database, first["run_id"], http_client=self.http, dns_client=self.dns)
        self.assertEqual(terminal["state"], resumed["state"])
        self.assertEqual(len(self.http.calls), calls)

    def test_terminal_rerun_is_idempotent(self):
        result = self.run_fixture(
            self.record("terminal-idempotent"),
            evidence=self.evidence("campaign_fit", "appointment_dependence", "recent_activity_demand"),
            gates=self.all_gates(),
        )
        version_count = self.database.connection.execute(
            "SELECT COUNT(*) FROM pipeline_qualification_results WHERE pipeline_run_id = ?", (result["run_id"],)
        ).fetchone()[0]
        calls = len(self.http.calls)
        rerun = resume_pipeline(self.database, result["run_id"], http_client=self.http, dns_client=self.dns)
        self.assertEqual(rerun["state"], result["state"])
        self.assertEqual(len(self.http.calls), calls)
        self.assertEqual(
            self.database.connection.execute(
                "SELECT COUNT(*) FROM pipeline_qualification_results WHERE pipeline_run_id = ?", (result["run_id"],)
            ).fetchone()[0],
            version_count,
        )

    def test_operator_review_is_audited_and_bounded(self):
        result = self.run_fixture(
            self.record("reviewable"),
            evidence=self.evidence("campaign_fit", "appointment_dependence", "recent_activity_demand"),
            gates={"currently_active": "YES", "appointments_meaningful": "YES", "legitimate_public_contact_route": "YES"},
        )
        review = apply_operator_review(
            self.database,
            result["run_id"],
            exact_gate_or_signal="independent_owner_led_or_accessible_local_decision_maker",
            decision="YES",
            reason="OPERATOR reviewed the fictional owner-led evidence.",
            source_url="https://fictional-business.test/team",
        )
        self.assertEqual(review["reviewer_identity"], "OPERATOR")
        row = self.database.connection.execute("SELECT * FROM manual_reviews WHERE id = ?", (review["review_id"],)).fetchone()
        self.assertEqual(row["decision"], "YES")
        self.assertEqual(row["reviewer_identity"], "OPERATOR")
        self.assertEqual(row["source_url"], "https://fictional-business.test/team")

    def test_new_evidence_creates_new_qualification_version(self):
        result = self.run_fixture(
            self.record("new-evidence"),
            evidence=self.evidence("campaign_fit", "appointment_dependence", "recent_activity_demand"),
            gates={"currently_active": "YES", "appointments_meaningful": "YES", "legitimate_public_contact_route": "YES"},
        )
        self.assertEqual(result["state"], "HOLD")
        updated = requalify_after_new_evidence(
            self.database,
            result["run_id"],
            new_evidence=self.evidence("owner_decision_maker_reachability", source_url="https://fictional-business.test/team"),
            gate_statuses={"independent_owner_led_or_accessible_local_decision_maker": "YES"},
        )
        self.assertIn(updated["state"], {"QUALIFIED", "STRONG"})
        versions = self.database.connection.execute(
            "SELECT version FROM pipeline_qualification_results WHERE pipeline_run_id = ? ORDER BY version",
            (result["run_id"],),
        ).fetchall()
        self.assertEqual([row[0] for row in versions], [1, 2])

    def test_summary_is_safe_and_reports_pipeline_outcome_without_contact_details(self):
        result = self.run_fixture(
            self.record("summary", website=None),
            evidence=self.evidence("campaign_fit", "appointment_dependence"),
            gates=self.all_gates(),
        )
        summary = summarize_pipeline_outcome(self.database, result["run_id"])
        self.assertEqual(summary["run_id"], result["run_id"])
        self.assertEqual(summary["state"], "REJECTED")
        self.assertNotIn("office@fictional-business.test", repr(summary))
        self.assertNotIn("public_business_email", summary)
        self.assertEqual(summary["outreach_count"], 0)


if __name__ == "__main__":
    unittest.main()
