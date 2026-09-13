import inspect
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from paldo_os_outbound import Database
from step14_scheduler import migrate_step14
from step15_identity_personalization import (
    BUSINESS_FIRST,
    create_fixture_personalization_packet,
    migrate_step15,
    prepare_fixture_channel_draft_requests,
    register_fixture_decision_maker,
)
from step16_campaign_intelligence import (
    CAMPAIGN_INTELLIGENCE_VERSION,
    PRIMARY_CAMPAIGN_KEY,
    SECONDARY_CAMPAIGN_KEY,
    CampaignIntelligenceValidationError,
    campaign_intelligence_fingerprint,
    get_campaign_intelligence,
    list_campaign_intelligence_keys,
    select_campaign_concepts,
    validate_campaign_intelligence,
)

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc)


class Step16CampaignIntelligenceTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.tempdir.name) / "step16.sqlite3")
        migrate_step14(self.database)
        migrate_step15(self.database)
        rows = self.database.connection.execute(
            "SELECT id, name FROM campaigns ORDER BY id"
        ).fetchall()
        self.campaigns = {row["name"]: row["id"] for row in rows}
        self.northfield_id = self.campaigns["Primary region local services"]
        self.us_id = self.campaigns["Secondary region local services"]
        self.database.connection.execute(
            "UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.northfield_id,)
        )
        self.database.connection.execute(
            "UPDATE campaigns SET status='INACTIVE' WHERE id=?", (self.us_id,)
        )
        self.database.connection.commit()
        self.database.set_config("personalization_research_enabled", 1)
        self.database.set_config("decision_maker_enrichment_enabled", 1)
        self.database.set_config("linkedin_daily_cap", 1)

    def tearDown(self):
        self.database.close()
        self.tempdir.cleanup()

    def _lead(self, domain="northfield-business.example"):
        lead_id = self.database.insert_lead(
            business_name="Fixture Local Service Business",
            domain=domain,
            email=f"hello@{domain}",
            industry="Local service business",
            country="PH",
            source="FIXTURE",
            status="QUALIFIED",
            campaign_id=self.northfield_id,
        )
        self.database.connection.execute(
            """INSERT INTO qualification_results
               (lead_id, campaign_id, score, classification, qualification_result,
                gate_statuses, reasoning_data, evaluated_at)
               VALUES (?, ?, 85, 'QUALIFIED', 'QUALIFY', ?, ?, ?)""",
            (lead_id, self.northfield_id, "{}", "{}", NOW.isoformat()),
        )
        self.database.connection.commit()
        return lead_id

    def _research(self, with_outcome=False):
        research = {
            "business_observation": {
                "kind": "OBSERVATION",
                "text": "The business accepts inquiries through Facebook Messenger and phone.",
                "source_type": "OFFICIAL_BUSINESS_SOURCE",
                "source_url": "https://northfield-business.example/contact",
                "observed_at": NOW.isoformat(),
                "confidence": 0.95,
            },
            "operational_signal": {
                "kind": "OBSERVATION",
                "text": "Multiple public contact and booking channels are visible.",
                "source_type": "OFFICIAL_BUSINESS_SOURCE",
                "source_url": "https://northfield-business.example/contact",
                "observed_at": NOW.isoformat(),
                "confidence": 0.9,
            },
            "pain_hypothesis": {
                "kind": "HYPOTHESIS",
                "text": "Follow-up ownership may become harder when inquiries arrive through several channels.",
                "basis": ["operational_signal"],
            },
            "capability_proof_match": {
                "kind": "CAPABILITY_MATCH",
                "text": "Harborview demonstrates the Business Booking and Follow-Up System capability.",
                "source_type": "INTERNAL_OFFER_FIXTURE",
                "source_url": "https://paldo.example/proof/case-study",
                "observed_at": NOW.isoformat(),
                "confidence": 0.95,
            },
            "unknowns": ["Actual follow-up ownership is not publicly verified."],
        }
        if with_outcome:
            research["desired_outcome"] = {
                "kind": "DESIRED_OUTCOME",
                "text": "A more visible booking and follow-up pipeline could help staff track the next action.",
                "source_type": "INTERNAL_OFFER_FIXTURE",
                "source_url": "https://paldo.example/offer/business-booking",
                "observed_at": NOW.isoformat(),
                "confidence": 0.8,
            }
        return research

    def test_northfield_and_us_intelligence_are_separate(self):
        northfield = get_campaign_intelligence(PRIMARY_CAMPAIGN_KEY)
        united_states = get_campaign_intelligence(SECONDARY_CAMPAIGN_KEY)
        self.assertEqual(northfield["geography"], "Northfield")
        self.assertEqual(united_states["geography"], "United States")
        self.assertIn("PM us", northfield["local_language"])
        self.assertIn("speed-to-lead", united_states["industry_vocabulary"])
        self.assertNotIn("speed-to-lead", northfield["industry_vocabulary"])
        self.assertNotEqual(northfield["campaign_key"], united_states["campaign_key"])

    def test_intelligence_is_versioned_and_deterministic(self):
        pack = get_campaign_intelligence(PRIMARY_CAMPAIGN_KEY)
        self.assertEqual(pack["version"], CAMPAIGN_INTELLIGENCE_VERSION)
        self.assertEqual(
            campaign_intelligence_fingerprint(PRIMARY_CAMPAIGN_KEY),
            campaign_intelligence_fingerprint(PRIMARY_CAMPAIGN_KEY),
        )
        self.assertEqual(validate_campaign_intelligence(pack), pack)
        self.assertEqual(select_campaign_concepts(PRIMARY_CAMPAIGN_KEY, ["inquiry_booking_leakage"]), select_campaign_concepts(PRIMARY_CAMPAIGN_KEY, ["inquiry_booking_leakage"]))

    def test_other_verticals_are_not_automatically_included(self):
        keys = list_campaign_intelligence_keys()
        self.assertEqual(set(keys), {PRIMARY_CAMPAIGN_KEY, SECONDARY_CAMPAIGN_KEY})
        serialized = json.dumps([get_campaign_intelligence(key) for key in keys], sort_keys=True).casefold()
        self.assertNotIn("dental", serialized)
        self.assertNotIn("veterinary", serialized)
        self.assertNotIn("chiropractic", serialized)

    def test_packet_can_reference_selected_intelligence_without_collapsing_types(self):
        lead_id = self._lead()
        result = create_fixture_personalization_packet(
            self.database,
            lead_id=lead_id,
            campaign_id=self.northfield_id,
            offer_version="1",
            research=self._research(with_outcome=True),
            campaign_intelligence_keys=["inquiry_booking_leakage", "follow_up_ownership"],
            fixture_override=True,
            now=NOW,
        )
        self.assertEqual(result["status"], "READY")
        row = self.database.connection.execute(
            "SELECT packet_json FROM personalization_research_packets WHERE id=?",
            (result["packet_id"],),
        ).fetchone()
        packet = json.loads(row["packet_json"])
        self.assertEqual(packet["business_observation"]["kind"], "OBSERVATION")
        self.assertEqual(packet["operational_signal"]["kind"], "OBSERVATION")
        self.assertEqual(packet["pain_hypothesis"]["kind"], "HYPOTHESIS")
        self.assertEqual(packet["desired_outcome"]["kind"], "DESIRED_OUTCOME")
        self.assertEqual(packet["capability_proof_match"]["kind"], "CAPABILITY_MATCH")
        self.assertEqual(
            [item["concept_key"] for item in packet["campaign_intelligence_refs"]],
            ["follow_up_ownership", "inquiry_booking_leakage"],
        )

    def test_harborview_proof_is_full_pipeline_and_unquantified(self):
        proof = get_campaign_intelligence(PRIMARY_CAMPAIGN_KEY)["proof_assets"]["harborview"]
        text = json.dumps(proof, sort_keys=True).casefold()
        for term in ("client records", "booking pipeline", "appointment status", "no-show", "follow-up", "lead reactivation", "next actions"):
            self.assertIn(term, text)
        self.assertNotIn("paper records only", text)
        altered = get_campaign_intelligence(PRIMARY_CAMPAIGN_KEY)
        altered["proof_assets"]["harborview"]["description"] += " Increased bookings by 40%."
        with self.assertRaises(CampaignIntelligenceValidationError):
            validate_campaign_intelligence(altered)

    def test_local_operator_context_is_optional_and_local_only(self):
        local = get_campaign_intelligence(PRIMARY_CAMPAIGN_KEY)
        united_states = get_campaign_intelligence(SECONDARY_CAMPAIGN_KEY)
        self.assertTrue(local["trust_context"]["optional"])
        self.assertIn("independent automation specialist", local["trust_context"]["approved_context"])
        self.assertIn("builds booking, follow-up, and internal workflow systems", local["trust_context"]["approved_context"])
        self.assertEqual(united_states["trust_context"], [])
        self.assertNotIn("Routed Cloud", json.dumps([local, united_states]))

    def test_channel_requests_remain_pending_policy(self):
        lead_id = self._lead()
        identity = register_fixture_decision_maker(
            self.database,
            lead_id=lead_id,
            record={
                "public_name": "Avery Stone",
                "current_role": "Founder",
                "public_linkedin_url": "https://www.linkedin.com/in/avery-stone-fixture",
                "business_domain": "northfield-business.example",
                "source_type": "PUBLIC_SEARCH_INDEX",
                "source_url": "https://search.fixture.example/avery",
                "observed_at": NOW.isoformat(),
                "confidence": 0.95,
                "verification_status": "VERIFIED",
            },
            fixture_override=True,
            now=NOW,
        )
        packet = create_fixture_personalization_packet(
            self.database,
            lead_id=lead_id,
            campaign_id=self.northfield_id,
            decision_maker_id=identity["decision_maker_id"],
            offer_version="1",
            research={**self._research(with_outcome=True), "decision_maker_observation": {
                "kind": "OBSERVATION",
                "text": "Avery Stone is identified publicly as Founder.",
                "source_type": "PUBLIC_SEARCH_INDEX",
                "source_url": "https://search.fixture.example/avery",
                "observed_at": NOW.isoformat(),
                "confidence": 0.95,
            }},
            campaign_intelligence_keys=["inquiry_booking_leakage"],
            fixture_override=True,
            now=NOW,
        )
        requests = prepare_fixture_channel_draft_requests(
            self.database,
            lead_id=lead_id,
            campaign_id=self.northfield_id,
            packet_id=packet["packet_id"],
            offer_version="1",
            lane=BUSINESS_FIRST,
            fixture_override=True,
            now=NOW,
        )
        self.assertEqual(requests["status"], "READY")
        statuses = [row["status"] for row in self.database.connection.execute("SELECT status FROM channel_draft_requests")]
        self.assertEqual(statuses, ["PENDING_POLICY", "PENDING_POLICY"])

    def test_no_live_or_provider_action_surface(self):
        import step16_campaign_intelligence
        source = inspect.getsource(step16_campaign_intelligence).casefold()
        for forbidden in ("requests.", "urllib", "browser", "selenium", "playwright", "smtplib", "imaplib", "socket", "subprocess", "donsetch"):
            self.assertNotIn(forbidden, source)

    def test_canonical_safety_defaults_remain_unchanged(self):
        config = self.database.read_config()
        self.assertEqual(config["system_state"], "PAUSED")
        self.assertEqual(config["discovery_mode"], "DRY_RUN")
        self.assertEqual(config["gmail_send_enabled"], 0)
        self.assertEqual(config["linkedin_sending_enabled"], 0)
        self.assertEqual(config["linkedin_message_policy_status"], "UNDECIDED")
        self.assertEqual(config["scheduler_enabled"], 0)
        self.assertEqual(config["decision_maker_enrichment_enabled"], 1)
        self.assertEqual(config["personalization_research_enabled"], 1)


if __name__ == "__main__":
    unittest.main()
