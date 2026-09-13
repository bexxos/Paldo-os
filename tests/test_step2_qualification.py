import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from paldo_os_outbound import (
    DEFAULT_DB_PATH,
    Database,
    MANDATORY_GATES,
    SUPPORTED_SIGNAL_KEYS,
    TRI_STATE_VALUES,
)


class Step2QualificationTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.database = Database(Path(self.temporary_directory.name) / "step2.sqlite3")
        self.database.migrate_step2()
        self.addCleanup(self.database.close)
        self.northfield_campaign = self.database.get_campaign_by_name(
            "Primary region local services"
        )

    def _lead(self, name="Fictional Paloma Business"):
        return self.database.insert_lead(
            business_name=name,
            domain=f"{name.lower().replace(' ', '-')}.example",
            industry="local service business",
            country="PH",
            location="Fictional Northfield",
            campaign_id=self.northfield_campaign["id"],
        )

    def _evidence(self, lead_id, signal_key, observation=None, value="YES"):
        return self.database.insert_structured_evidence(
            lead_id=lead_id,
            signal_key=signal_key,
            signal_value=value,
            source_type="fictional_public_fixture",
            source_url="https://source.example/fictional-business",
            observation=observation or f"Fictional public observation for {signal_key}.",
            confidence=0.9,
            observed_or_inferred="OBSERVED",
            pain_hypothesis=(
                "Hypothesis only: disconnected booking channels may create unclear next actions."
            ),
        )

    def _score(self, lead_id, statuses=None):
        return self.database.qualify_lead(
            lead_id=lead_id,
            campaign_id=self.northfield_campaign["id"],
            gate_statuses=statuses or {gate: "YES" for gate in MANDATORY_GATES},
        )

    def _strong_evidence(self, lead_id):
        for signal_key in (
            "campaign_fit",
            "appointment_dependence",
            "website_booking_page_widget",
            "recent_reviews_visible_activity",
            "multiple_practitioners",
            "multiple_services",
            "owner_decision_maker_reachability",
            "personalization_observation",
        ):
            self._evidence(lead_id, signal_key)

    def test_strong_clinic_scores_and_qualifies(self):
        lead_id = self._lead()
        self._strong_evidence(lead_id)

        result = self._score(lead_id)

        self.assertEqual(result["score"], 100)
        self.assertEqual(result["classification"], "STRONG")
        self.assertEqual(result["qualification_result"], "QUALIFY")
        self.assertTrue(result["qualifies"])
        self.assertEqual(result["mandatory_gates"]["currently_active"], "YES")
        self.assertIn("economic_value_operational_complexity", result["awarded_points"])
        self.assertTrue(result["reason"])

    def test_exactly_70_is_qualified_with_existing_minimum(self):
        lead_id = self._lead("Fictional Seventy Point Business")
        for signal_key in (
            "campaign_fit",
            "appointment_dependence",
            "website_booking_page_widget",
            "owner_decision_maker_reachability",
            "personalization_observation",
        ):
            self._evidence(lead_id, signal_key)

        result = self._score(lead_id)

        self.assertEqual(result["score"], 70)
        self.assertEqual(result["classification"], "QUALIFIED")
        self.assertEqual(result["qualification_result"], "QUALIFY")
        self.assertTrue(result["qualifies"])

    def test_below_70_cannot_qualify(self):
        lead_id = self._lead("Fictional Sixty Five Point Business")
        for signal_key in (
            "campaign_fit",
            "appointment_dependence",
            "website_booking_page_widget",
            "owner_decision_maker_reachability",
        ):
            self._evidence(lead_id, signal_key)

        result = self._score(lead_id)

        self.assertEqual(result["score"], 65)
        self.assertEqual(result["classification"], "HOLD")
        self.assertEqual(result["qualification_result"], "REJECT")
        self.assertFalse(result["qualifies"])
        self.assertIn("minimum qualification score", result["reason"].lower())

    def test_high_score_failed_mandatory_gate_is_rejected(self):
        lead_id = self._lead("Fictional Failed Gate Business")
        self._strong_evidence(lead_id)
        statuses = {gate: "YES" for gate in MANDATORY_GATES}
        statuses["currently_active"] = "NO"

        result = self._score(lead_id, statuses)

        self.assertEqual(result["score"], 100)
        self.assertEqual(result["classification"], "STRONG")
        self.assertEqual(result["qualification_result"], "REJECT")
        self.assertFalse(result["qualifies"])
        self.assertEqual(result["mandatory_gates"]["currently_active"], "NO")
        self.assertIn("failed mandatory gate", result["reason"].lower())

    def test_high_score_unclear_mandatory_gate_is_held(self):
        lead_id = self._lead("Fictional Unclear Gate Business")
        self._strong_evidence(lead_id)
        statuses = {gate: "YES" for gate in MANDATORY_GATES}
        statuses["legitimate_public_contact_route"] = "UNCLEAR"

        result = self._score(lead_id, statuses)

        self.assertEqual(result["score"], 100)
        self.assertEqual(result["classification"], "STRONG")
        self.assertEqual(result["qualification_result"], "HOLD")
        self.assertFalse(result["qualifies"])
        self.assertIn("unclear mandatory gate", result["reason"].lower())

    def test_missing_evidence_is_unclear_and_not_invented(self):
        lead_id = self._lead("Fictional Missing Evidence Business")

        result = self._score(lead_id)

        self.assertEqual(result["evidence_status"]["website_booking_page_widget"], "UNCLEAR")
        self.assertEqual(result["evidence_status"]["public_business_email"], "UNCLEAR")
        self.assertEqual(result["score"], 0)
        self.assertFalse(result["qualifies"])
        self.assertIn("missing evidence", result["reason"].lower())

    def test_website_booking_is_readiness_not_a_disqualifier(self):
        lead_id = self._lead("Fictional Online Booking Business")
        self._evidence(lead_id, "campaign_fit")
        self._evidence(lead_id, "website_booking_page_widget")
        statuses = {gate: "YES" for gate in MANDATORY_GATES}

        result = self._score(lead_id, statuses)

        self.assertEqual(result["evidence_status"]["website_booking_page_widget"], "YES")
        self.assertFalse(result["exclusion_reasons"])
        self.assertFalse(result["qualifies"])
        self.assertIn("readiness", result["reason"].lower())

    def test_duplicate_evidence_does_not_double_count(self):
        one = self._lead("Fictional Single Evidence Business")
        duplicate = self._lead("Fictional Duplicate Evidence Business")
        for lead_id in (one, duplicate):
            for signal_key in (
                "campaign_fit",
                "appointment_dependence",
                "website_booking_page_widget",
                "owner_decision_maker_reachability",
                "personalization_observation",
            ):
                self._evidence(lead_id, signal_key)
        self._evidence(duplicate, "campaign_fit")
        self._evidence(duplicate, "campaign_fit")

        first = self._score(one)
        second = self._score(duplicate)

        self.assertEqual(first["score"], 70)
        self.assertEqual(second["score"], first["score"])
        self.assertEqual(second["awarded_points"], first["awarded_points"])

    def test_suppressed_lead_cannot_qualify(self):
        lead_id = self._lead("Fictional Suppressed Business")
        self._strong_evidence(lead_id)
        self.database.add_suppression(lead_id=lead_id, reason="fictional suppression fixture")

        result = self._score(lead_id)

        self.assertEqual(result["score"], 100)
        self.assertEqual(result["qualification_result"], "REJECT")
        self.assertFalse(result["qualifies"])
        self.assertIn("suppressed", result["reason"].lower())

    def test_repeated_scoring_with_same_evidence_is_deterministic(self):
        lead_id = self._lead("Fictional Deterministic Business")
        self._strong_evidence(lead_id)

        first = self._score(lead_id)
        second = self._score(lead_id)
        stored = self.database.get_latest_qualification(lead_id)
        row_count = self.database.connection.execute(
            "SELECT COUNT(*) FROM qualification_results WHERE lead_id = ?", (lead_id,)
        ).fetchone()[0]

        self.assertEqual(first, second)
        self.assertEqual(stored, first)
        self.assertEqual(row_count, 1)

    def test_campaigns_and_future_categories_are_inactive(self):
        campaigns = self.database.get_campaigns()
        categories = self.database.get_eligible_categories()

        self.assertEqual(
            {campaign["name"] for campaign in campaigns},
            {
                "Primary region local services",
                "Secondary region local services",
            },
        )
        self.assertTrue(all(campaign["status"] == "INACTIVE" for campaign in campaigns))
        self.assertTrue(all(category["status"] == "INACTIVE" for category in categories))
        self.assertEqual(
            {category["category_key"] for category in categories},
            {
                "dental",
                "dermatology",
                "physiotherapy",
                "chiropractic",
                "optometry",
                "veterinary",
                "wellness",
                "other_legitimate_appointment_dependent_clinics",
            },
        )

    def test_campaigns_persist_buyer_and_recommendation_configuration(self):
        campaigns = {
            campaign["name"]: campaign for campaign in self.database.get_campaigns()
        }
        northfield = campaigns["Primary region local services"]
        united_states = campaigns["Secondary region local services"]
        expected_secondary_roles = [
            "owner-operator",
            "managing partner",
            "operations lead with authority",
        ]

        for campaign in (northfield, united_states):
            self.assertEqual(campaign["primary_buyer"], "business owner")
            self.assertEqual(
                json.loads(campaign["secondary_buyer_roles"]), expected_secondary_roles
            )
            self.assertEqual(campaign["unknown_role_status"], "UNCLEAR")
            self.assertTrue(campaign["recommendation_reason"])

        self.assertEqual(northfield["recommended"], 1)
        self.assertIn("direct interview evidence", northfield["recommendation_reason"])
        self.assertEqual(united_states["recommended"], 0)
        self.assertIn("not recommended", united_states["recommendation_reason"].lower())

    def test_campaigns_allow_no_more_than_one_active_campaign(self):
        campaigns = self.database.get_campaigns()
        first_id, second_id = (campaign["id"] for campaign in campaigns)

        with self.assertRaises(sqlite3.IntegrityError):
            with self.database.connection:
                self.database.connection.execute(
                    "UPDATE campaigns SET status = 'ACTIVE' WHERE id = ?", (first_id,)
                )
                self.database.connection.execute(
                    "UPDATE campaigns SET status = 'ACTIVE' WHERE id = ?", (second_id,)
                )

        self.assertEqual(
            self.database.connection.execute(
                "SELECT COUNT(*) FROM campaigns WHERE status = 'ACTIVE'"
            ).fetchone()[0],
            0,
        )

    def test_required_system_safety_defaults_remain_paused_and_zero_cap(self):
        config = self.database.read_config()

        self.assertEqual(config["system_state"], "PAUSED")
        self.assertEqual(config["daily_message_cap"], 0)
        self.assertEqual(config["minimum_qualification_score"], 70)

    def test_migration_is_idempotent_and_preserves_step1_rows(self):
        database_path = Path(self.temporary_directory.name) / "migration.sqlite3"
        database = Database(database_path)
        lead_id = database.insert_lead(
            business_name="Fictional Pre-Migration Business",
            domain="pre-migration.example",
        )
        database.migrate_step2()
        first_campaign_count = database.connection.execute(
            "SELECT COUNT(*) FROM campaigns"
        ).fetchone()[0]
        first_category_count = database.connection.execute(
            "SELECT COUNT(*) FROM eligible_categories"
        ).fetchone()[0]
        database.migrate_step2()

        self.assertEqual(database.get_lead(lead_id)["business_name"], "Fictional Pre-Migration Business")
        self.assertEqual(
            database.connection.execute("SELECT COUNT(*) FROM campaigns").fetchone()[0],
            first_campaign_count,
        )
        self.assertEqual(
            database.connection.execute("SELECT COUNT(*) FROM eligible_categories").fetchone()[0],
            first_category_count,
        )
        self.assertEqual(
            database.connection.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE version = 2"
            ).fetchone()[0],
            1,
        )
        database.close()

    def test_structured_evidence_preserves_required_semantics(self):
        lead_id = self._lead("Fictional Evidence Semantics Business")
        evidence_id = self.database.insert_structured_evidence(
            lead_id=lead_id,
            signal_key="email_explicitly_used_for_booking",
            signal_value="YES",
            source_type="fictional_public_website",
            source_url="https://source.example/booking",
            observation="The fictional contact page says email is used to request appointments.",
            confidence=0.75,
            observed_or_inferred="OBSERVED",
            pain_hypothesis="Hypothesis only: email booking may require manual staff confirmation.",
        )

        evidence = self.database.get_structured_evidence(lead_id)[0]

        self.assertEqual(evidence["id"], evidence_id)
        self.assertEqual(evidence["signal_value"], "YES")
        self.assertEqual(evidence["source_type"], "fictional_public_website")
        self.assertEqual(evidence["source_url"], "https://source.example/booking")
        self.assertEqual(evidence["observation"], "The fictional contact page says email is used to request appointments.")
        self.assertEqual(evidence["confidence"], 0.75)
        self.assertEqual(evidence["observed_or_inferred"], "OBSERVED")
        self.assertIn("Hypothesis only", evidence["pain_hypothesis"])
        self.assertTrue(evidence["collected_at"])

    def test_all_required_workflow_signal_keys_are_supported(self):
        required = {
            "appointment_channels",
            "public_business_email",
            "email_explicitly_used_for_booking",
            "website_appointment_request_form",
            "website_booking_page_widget",
            "phone_booking",
            "messenger_booking",
            "instagram_booking",
            "whatsapp_booking",
            "google_business_booking",
            "multiple_booking_channels",
            "cancellation_route",
            "rescheduling_route",
            "manual_staff_confirmation",
            "visible_reminder_capability",
            "visible_follow_up_capability",
            "public_booking_questions",
            "cancellation_deposit_no_show_policy",
            "multiple_practitioners",
            "multiple_services",
            "multiple_locations",
            "recurring_appointments",
            "post_visit_follow_up",
            "out_of_hours_inquiry_handling",
            "system_maturity",
        }
        self.assertTrue(required.issubset(SUPPORTED_SIGNAL_KEYS))

        lead_id = self._lead("Fictional Signal Vocabulary Business")
        for signal_key in required:
            self._evidence(lead_id, signal_key, value="UNCLEAR")
        self.assertEqual(len(self.database.get_structured_evidence(lead_id)), len(required))

    def test_invalid_tri_state_value_is_rejected(self):
        lead_id = self._lead("Fictional Tri State Business")

        self.assertEqual(TRI_STATE_VALUES, {"YES", "NO", "UNCLEAR"})
        with self.assertRaises(ValueError):
            self._evidence(lead_id, "campaign_fit", value="MAYBE")

    def test_durable_database_has_no_real_prospects_or_outreach(self):
        database = Database(DEFAULT_DB_PATH)
        self.addCleanup(database.close)

        self.assertEqual(database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)
        self.assertEqual(database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
