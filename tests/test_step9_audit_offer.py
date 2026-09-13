import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from paldo_os_outbound import Database
from step9_audit_offer import (
    AUDIT_OFFER_KEY,
    AUDIT_OFFER_STATES,
    DELIVERABLE_SECTION_KEYS,
    N8N_CONTRACT_VERSION,
    AuditIntakeRejectedError,
    AuditOfferLifecycleError,
    AuditProcessingBlockedError,
    DraftingOfferNotReadyError,
    activate_audit_offer_version,
    approve_audit_offer_version,
    create_audit_offer_version,
    create_audit_request,
    get_active_offer_for_drafting,
    get_audit_offer_version,
    get_n8n_intake_contract,
    migrate_step9,
    process_audit_request,
    retire_audit_offer_version,
    score_audit_opportunity,
    transition_audit_offer,
    validate_audit_deliverable,
    validate_audit_intake,
)


class Step9AuditOfferTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.temp_dir.name) / "fictional.sqlite3")
        self.database.migrate_step2()
        migrate_step9(self.database)
        self.offer = get_audit_offer_version(self.database, version=1)

    def tearDown(self):
        self.database.close()
        self.temp_dir.cleanup()

    def valid_intake(self):
        return {
            "business_name": "Fictional Glow Business",
            "public_website": "https://fictional-glow.example",
            "requester_name": "Fictional Owner",
            "business_role": "owner",
            "business_email": "owner@fictional-glow.example",
            "business_category": "local service and local business",
            "current_inquiry_and_booking_channels": ["Instagram", "phone", "website form"],
            "workflow_to_review": "new inquiry to booked appointment follow-up",
            "current_tools": ["spreadsheet", "messaging inbox"],
            "approximate_frequency_or_volume": "fictional: about 12 inquiries weekly",
            "desired_operational_outcome": "make next follow-up actions easier to see",
            "permission_to_prepare_and_deliver_audit": True,
            "acknowledgment_no_person_or_secret_data": True,
        }

    def fact(self, fact_type, text, **extra):
        result = {"type": fact_type, "text": text}
        result.update(extra)
        return result

    def valid_deliverable(self):
        return {
            "business_and_workflow_reviewed": [self.fact("USER_PROVIDED_FACT", "Fictional Glow Business; inquiry-to-booking follow-up.")],
            "evidence_reviewed": [self.fact("OBSERVATION", "Fictional public booking page lists a contact route.", source_url="https://fictional-glow.example/contact", confidence=0.9)],
            "current_workflow_summary": [self.fact("USER_PROVIDED_FACT", "The requester described inquiry and booking channels.")],
            "observed_friction": [self.fact("OBSERVATION", "The public page shows separate inquiry and booking routes.", source_url="https://fictional-glow.example", confidence=0.8)],
            "assumptions_and_unknowns": [self.fact("UNKNOWN", "Actual staff handoff timing was not provided.")],
            "automation_opportunities": [
                {
                    "name": "follow-up visibility queue",
                    "description": [self.fact("INFERENCE", "A shared next-action queue may reduce coordination gaps; this is a hypothesis.")],
                    "ratings": {"operational_impact": 4, "frequency": 4, "implementation_feasibility": 4, "evidence_confidence": 3},
                }
            ],
            "recommended_first_automation": [self.fact("INFERENCE", "Start with a staff-controlled follow-up queue if the intake description is confirmed.")],
            "human_controls_and_operational_risks": [self.fact("OBSERVATION", "Staff approval is required before any client-facing action.")],
            "expected_benefit_as_bounded_hypothesis": [self.fact("ESTIMATE", "Follow-up visibility could improve consistency; no savings are promised.", supporting_inputs=["fictional requester description"], assumptions=["channel volume remains similar"], confidence=0.5)],
            "recommended_next_step": [self.fact("UNKNOWN", "Confirm the current handoff with an operator before implementation.")],
        }

    def test_valid_minimal_intake_is_accepted_without_sensitive_fields(self):
        result = validate_audit_intake(self.valid_intake())
        self.assertTrue(result["accepted"])
        self.assertEqual(result["normalized"]["business_name"], "Fictional Glow Business")
        self.assertNotIn("person_names", result["normalized"])
        self.assertNotIn("api_key", result["normalized"])

    def test_missing_consent_is_rejected(self):
        intake = self.valid_intake()
        intake["permission_to_prepare_and_deliver_audit"] = False
        with self.assertRaises(AuditIntakeRejectedError):
            validate_audit_intake(intake)

    def test_patient_and_health_information_is_rejected(self):
        intake = self.valid_intake()
        intake["person_names"] = ["Fictional Client"]
        with self.assertRaises(AuditIntakeRejectedError):
            validate_audit_intake(intake)

    def test_credentials_and_secret_fields_are_rejected(self):
        intake = self.valid_intake()
        intake["api_key"] = "fictional-secret"
        with self.assertRaises(AuditIntakeRejectedError):
            validate_audit_intake(intake)

    def test_duplicate_idempotency_key_does_not_create_duplicate_request(self):
        first = create_audit_request(self.database, self.valid_intake(), idempotency_key="fictional-intake-001", offer_version_id=self.offer["id"])
        second = create_audit_request(self.database, self.valid_intake(), idempotency_key="fictional-intake-001", offer_version_id=self.offer["id"])
        self.assertEqual(first["request_id"], second["request_id"])
        self.assertTrue(second["duplicate"])
        count = self.database.connection.execute("SELECT COUNT(*) FROM audit_requests").fetchone()[0]
        self.assertEqual(count, 1)

    def test_one_page_deliverable_schema_is_valid(self):
        result = validate_audit_deliverable(self.valid_deliverable())
        self.assertTrue(result["valid"])
        self.assertEqual(set(result["normalized"]), set(DELIVERABLE_SECTION_KEYS))
        self.assertEqual(result["normalized"]["automation_opportunities"][0]["priority_score"], 15)

    def test_fact_types_remain_distinct(self):
        result = validate_audit_deliverable(self.valid_deliverable())
        types = {entry["type"] for entries in result["fact_entries"] for entry in entries}
        self.assertEqual(types, {"OBSERVATION", "USER_PROVIDED_FACT", "INFERENCE", "ESTIMATE", "UNKNOWN"})

    def test_opportunity_score_is_deterministic_and_bounded_at_twenty(self):
        ratings = {"operational_impact": 5, "frequency": 4, "implementation_feasibility": 3, "evidence_confidence": 2}
        self.assertEqual(score_audit_opportunity(ratings), 14)
        with self.assertRaises(ValueError):
            score_audit_opportunity({**ratings, "frequency": 6})

    def test_estimate_without_supporting_inputs_is_rejected(self):
        deliverable = self.valid_deliverable()
        deliverable["expected_benefit_as_bounded_hypothesis"] = [self.fact("ESTIMATE", "This will save 10 hours every week.")]
        with self.assertRaises(ValueError):
            validate_audit_deliverable(deliverable)

    def test_ai_cannot_approve_an_offer(self):
        with self.assertRaises(AuditOfferLifecycleError):
            transition_audit_offer(self.database, self.offer["id"], "APPROVED", actor_identity="AI", actor_type="AI", reason="fictional AI proposal")

    def test_operator_approval_by_operator_is_audited(self):
        approved = approve_audit_offer_version(self.database, self.offer["id"], reviewer_identity="Operator", reason="fictional operator review")
        self.assertEqual(approved["status"], "APPROVED")
        event = self.database.connection.execute("SELECT actor_identity, actor_type, to_status FROM audit_offer_approval_events ORDER BY id DESC LIMIT 1").fetchone()
        self.assertEqual(tuple(event), ("OPERATOR", "OPERATOR", "APPROVED"))

    def test_active_requires_approved_status(self):
        with self.assertRaises(AuditOfferLifecycleError):
            activate_audit_offer_version(self.database, self.offer["id"], actor_identity="Operator")
        approve_audit_offer_version(self.database, self.offer["id"], reviewer_identity="Operator", reason="fictional approval")
        active = activate_audit_offer_version(self.database, self.offer["id"], actor_identity="Operator")
        self.assertEqual(active["status"], "ACTIVE")

    def test_only_one_active_offer_version_is_allowed(self):
        approve_audit_offer_version(self.database, self.offer["id"], reviewer_identity="Operator", reason="fictional approval")
        activate_audit_offer_version(self.database, self.offer["id"], actor_identity="Operator")
        version_two = create_audit_offer_version(self.database, version=2, lifecycle_state="PROPOSED", created_by="AI")
        approve_audit_offer_version(self.database, version_two["id"], reviewer_identity="Operator", reason="fictional approval")
        with self.assertRaises(AuditOfferLifecycleError):
            activate_audit_offer_version(self.database, version_two["id"], actor_identity="Operator")

    def test_retired_version_cannot_reactivate_silently(self):
        approve_audit_offer_version(self.database, self.offer["id"], reviewer_identity="Operator", reason="fictional approval")
        activate_audit_offer_version(self.database, self.offer["id"], actor_identity="Operator")
        retire_audit_offer_version(self.database, self.offer["id"], reviewer_identity="Operator", reason="fictional retirement")
        with self.assertRaises(AuditOfferLifecycleError):
            activate_audit_offer_version(self.database, self.offer["id"], actor_identity="Operator")

    def test_drafting_is_blocked_without_approved_active_offer(self):
        with self.assertRaises(DraftingOfferNotReadyError):
            get_active_offer_for_drafting(self.database)

    def test_n8n_contract_prohibits_sensitive_fields_and_credentials(self):
        contract = get_n8n_intake_contract(self.database, N8N_CONTRACT_VERSION)
        self.assertEqual(contract["version"], N8N_CONTRACT_VERSION)
        prohibited = set(contract["prohibited_fields"])
        self.assertIn("person_names", prohibited)
        self.assertIn("api_key", prohibited)
        self.assertEqual(contract["public_webhook_created"], False)

    def test_processing_is_blocked_by_zero_cap_and_safe_state(self):
        request = create_audit_request(self.database, self.valid_intake(), idempotency_key="fictional-blocked-001", offer_version_id=self.offer["id"])
        with self.assertRaises(AuditProcessingBlockedError):
            process_audit_request(self.database, request["request_id"])

    def test_migration_is_idempotent_and_no_real_audits_are_seeded(self):
        self.assertEqual(migrate_step9(self.database), 11)
        self.assertEqual(migrate_step9(self.database), 11)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM audit_requests").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM audit_deliverables").fetchone()[0], 0)
        self.assertIn("PROPOSED", AUDIT_OFFER_STATES)
        self.assertEqual(self.database.read_config()["audit_request_daily_cap"], 0)
        self.assertEqual(self.database.read_config()["audit_delivery_mode"], "HYBRID_PROPOSED")


if __name__ == "__main__":
    unittest.main()
