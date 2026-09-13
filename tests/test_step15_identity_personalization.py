import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from paldo_os_outbound import Database
from step14_scheduler import migrate_step14
from step15_identity_personalization import (
    ALLOWED_DECISION_MAKER_ROLES,
    LINKEDIN_ONLY,
    BUSINESS_FIRST,
    create_fixture_personalization_packet,
    migrate_step15,
    prepare_fixture_channel_draft_requests,
    register_fixture_decision_maker,
    resolve_linkedin_only_fixture_seed,
)


NOW = datetime(2026, 9, 5, 8, 0, tzinfo=timezone.utc)


class Step15IdentityPersonalizationTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.tempdir.name) / "step15.sqlite3")
        migrate_step14(self.database)
        migrate_step15(self.database)
        self.addCleanup(self.database.close)
        self.addCleanup(self.tempdir.cleanup)
        self.campaign = self.database.get_campaign_by_name("Secondary region local services")
        self.database.connection.execute("UPDATE campaigns SET status='INACTIVE'")
        self.database.connection.execute(
            "UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],)
        )
        self.database.connection.commit()
        self.default_config = self.database.read_config()
        for key, value in {
            "system_state": "ACTIVE",
            "decision_maker_enrichment_enabled": 1,
            "linkedin_provider_mode": "FIXTURE_ONLY",
            "linkedin_sending_enabled": 0,
            "linkedin_daily_cap": 1,
            "linkedin_message_policy_status": "UNDECIDED",
            "personalization_research_enabled": 1,
        }.items():
            self.database.set_config(key, value)
        self.lead_id = self.database.insert_lead(
            business_name="Fictional Dual Business",
            website="https://dual.example",
            domain="dual.example",
            email="office@dual.example",
            industry="Local service business",
            country="US",
            source="FIXTURE",
            source_url="https://places.fixture/dual",
            status="QUALIFIED",
            campaign_id=self.campaign["id"],
        )
        self.qualify("QUALIFIED")

    def qualify(self, classification="QUALIFIED"):
        self.database.connection.execute(
            """INSERT INTO qualification_results
               (lead_id, campaign_id, score, classification, qualification_result,
                gate_statuses, reasoning_data, evaluated_at)
               VALUES (?, ?, ?, ?, 'QUALIFY', '{}', '{}', ?)""",
            (self.lead_id, self.campaign["id"], 85 if classification == "STRONG" else 75,
             classification, NOW.isoformat()),
        )
        self.database.connection.commit()

    def dm(self, *, linkedin=True, verification="VERIFIED", domain="dual.example", name="Avery Stone", role="Founder"):
        return {
            "public_name": name,
            "current_role": role,
            "public_linkedin_url": "https://www.linkedin.com/in/avery-stone-fixture" if linkedin else None,
            "business_domain": domain,
            "source_type": "PUBLIC_SEARCH_INDEX",
            "source_url": "https://search.fixture.example/avery-stone",
            "observed_at": NOW.isoformat(),
            "confidence": 0.96,
            "verification_status": verification,
        }

    def packet_input(self, *, changed=False, pain="The visible booking flow may leave follow-up work fragmented."):
        suffix = " Updated." if changed else ""
        return {
            "business_observation": {
                "kind": "OBSERVATION",
                "text": "The public fixture booking page lists appointments and contact options." + suffix,
                "source_type": "OFFICIAL_BUSINESS_SOURCE",
                "source_url": "https://dual.example/booking",
                "observed_at": NOW.isoformat(),
                "confidence": 0.95,
            },
            "decision_maker_observation": {
                "kind": "OBSERVATION",
                "text": "Avery Stone is listed as the business founder in the public fixture profile.",
                "source_type": "PUBLIC_SEARCH_INDEX",
                "source_url": "https://search.fixture.example/avery-stone",
                "observed_at": NOW.isoformat(),
                "confidence": 0.96,
            },
            "operational_signal": {
                "kind": "OBSERVATION",
                "text": "The public fixture presents multiple appointment services and a contact route." + suffix,
                "source_type": "OFFICIAL_BUSINESS_SOURCE",
                "source_url": "https://dual.example/services",
                "observed_at": NOW.isoformat(),
                "confidence": 0.91,
            },
            "pain_hypothesis": {
                "kind": "HYPOTHESIS",
                "text": pain,
                "basis": ["operational_signal"],
            },
            "capability_proof_match": {
                "kind": "CAPABILITY_MATCH",
                "text": "Business Booking and Follow-Up System matches the observed appointment and follow-up coordination signal.",
                "source_type": "INTERNAL_OFFER_FIXTURE",
                "source_url": "https://offer.fixture.example/booking-followup",
                "observed_at": NOW.isoformat(),
                "confidence": 0.9,
            },
            "unknowns": ["Actual inquiry volume is unknown."],
        }

    def register(self, **kwargs):
        return register_fixture_decision_maker(
            self.database,
            lead_id=self.lead_id,
            record=self.dm(**kwargs),
            fixture_override=True,
            now=NOW,
        )

    def packet(self, *, decision_maker_id=None, changed=False, pain=None):
        return create_fixture_personalization_packet(
            self.database,
            lead_id=self.lead_id,
            campaign_id=self.campaign["id"],
            decision_maker_id=decision_maker_id,
            offer_version="1",
            research=self.packet_input(changed=changed, pain=pain or "The visible booking flow may leave follow-up work fragmented."),
            fixture_override=True,
            now=NOW,
        )

    def requests(self, packet_id, *, lane=BUSINESS_FIRST):
        return prepare_fixture_channel_draft_requests(
            self.database,
            lead_id=self.lead_id,
            campaign_id=self.campaign["id"],
            packet_id=packet_id,
            offer_version="1",
            lane=lane,
            fixture_override=True,
            now=NOW,
        )

    def test_verified_business_first_creates_email_and_linkedin_requests(self):
        decision_maker = self.register()
        packet = self.packet(decision_maker_id=decision_maker["decision_maker_id"])
        result = self.requests(packet["packet_id"])
        self.assertEqual(result["status"], "READY")
        self.assertEqual(set(result["channels"]), {"EMAIL", "LINKEDIN"})
        rows = self.database.connection.execute("SELECT * FROM channel_draft_requests ORDER BY channel").fetchall()
        self.assertEqual([row["status"] for row in rows], ["PENDING_POLICY", "PENDING_POLICY"])

    def test_business_without_linkedin_remains_email_only(self):
        decision_maker = self.register(linkedin=False)
        packet = self.packet(decision_maker_id=decision_maker["decision_maker_id"])
        result = self.requests(packet["packet_id"])
        self.assertEqual(result["channels"], ["EMAIL"])

    def test_linkedin_only_seed_creates_linkedin_request_only(self):
        result = resolve_linkedin_only_fixture_seed(
            self.database,
            campaign_id=self.campaign["id"],
            seed={"lead_id": self.lead_id, **self.dm()},
            offer_version="1",
            research=self.packet_input(),
            fixture_override=True,
            now=NOW,
        )
        self.assertEqual(result["status"], "READY")
        self.assertEqual(result["channels"], ["LINKEDIN"])
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM channel_draft_requests WHERE channel='EMAIL'").fetchone()[0], 0)

    def test_linkedin_only_still_requires_normal_qualification(self):
        self.database.connection.execute("DELETE FROM qualification_results")
        self.database.connection.commit()
        result = resolve_linkedin_only_fixture_seed(
            self.database,
            campaign_id=self.campaign["id"],
            seed={"lead_id": self.lead_id, **self.dm()},
            offer_version="1",
            research=self.packet_input(),
            fixture_override=True,
            now=NOW,
        )
        self.assertEqual(result["status"], "HOLD")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM personalization_research_packets").fetchone()[0], 0)

    def test_ambiguous_identity_produces_hold(self):
        result = self.register(verification="UNCLEAR")
        self.assertEqual(result["status"], "HOLD")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM lead_decision_makers").fetchone()[0], 1)

    def test_wrong_business_association_is_rejected(self):
        result = self.register(domain="other.example")
        self.assertEqual(result["status"], "REJECTED")
        self.assertIn("BUSINESS_ASSOCIATION_MISMATCH", result["reasons"])

    def test_duplicate_profile_is_idempotent(self):
        first = self.register()
        second = self.register()
        self.assertEqual(first["decision_maker_id"], second["decision_maker_id"])
        self.assertEqual(second["status"], "DUPLICATE")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM lead_decision_makers").fetchone()[0], 1)

    def test_suppression_blocks_both_channels(self):
        decision_maker = self.register()
        packet = self.packet(decision_maker_id=decision_maker["decision_maker_id"])
        self.database.connection.execute(
            "INSERT INTO suppressions (domain, reason, created_at) VALUES (?, ?, ?)",
            ("dual.example", "fixture suppression", NOW.isoformat()),
        )
        self.database.connection.commit()
        result = self.requests(packet["packet_id"])
        self.assertEqual(result["status"], "REJECTED")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM channel_draft_requests").fetchone()[0], 0)

    def test_unqualified_candidate_receives_no_research(self):
        self.database.connection.execute("DELETE FROM qualification_results")
        self.database.connection.commit()
        result = self.packet()
        self.assertEqual(result["status"], "HOLD")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM personalization_research_packets").fetchone()[0], 0)

    def test_observations_and_pain_hypothesis_remain_distinct(self):
        packet = self.packet()
        stored = self.database.connection.execute("SELECT packet_json FROM personalization_research_packets WHERE id=?", (packet["packet_id"],)).fetchone()
        body = json.loads(stored["packet_json"])
        self.assertEqual(body["business_observation"]["kind"], "OBSERVATION")
        self.assertEqual(body["pain_hypothesis"]["kind"], "HYPOTHESIS")
        self.assertNotEqual(body["business_observation"]["kind"], body["pain_hypothesis"]["kind"])

    def test_unsupported_pain_claim_is_rejected(self):
        result = self.packet(pain="This business misses inquiries and has failed follow-ups every day.")
        self.assertEqual(result["status"], "REJECTED")
        self.assertIn("UNSUPPORTED_PAIN_CLAIM", result["reasons"])
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM personalization_research_packets").fetchone()[0], 0)

    def test_evidence_reference_must_belong_to_same_lead(self):
        other_id = self.database.insert_lead(
            business_name="Other Fixture Business",
            domain="other-evidence.example",
            email="office@other-evidence.example",
            industry="Local service business",
            country="US",
            source="FIXTURE",
            status="QUALIFIED",
            campaign_id=self.campaign["id"],
        )
        evidence_id = self.database.insert_evidence(
            lead_id=other_id,
            evidence_type="PUBLIC_BUSINESS_OBSERVATION",
            observation="Other business has a public appointment page.",
            source_url="https://other-evidence.example/booking",
            confidence=0.9,
        )
        research = self.packet_input()
        research["business_observation"]["evidence_id"] = evidence_id
        result = create_fixture_personalization_packet(
            self.database,
            lead_id=self.lead_id,
            campaign_id=self.campaign["id"],
            offer_version="1",
            research=research,
            fixture_override=True,
            now=NOW,
        )
        self.assertEqual(result["status"], "REJECTED")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM personalization_research_packets").fetchone()[0], 0)

    def test_changed_evidence_creates_new_packet_version(self):
        first = self.packet()
        second = self.packet(changed=True)
        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 2)
        self.assertNotEqual(first["packet_id"], second["packet_id"])
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM personalization_research_packets").fetchone()[0], 2)

    def test_draft_requests_contain_no_message_content(self):
        decision_maker = self.register()
        packet = self.packet(decision_maker_id=decision_maker["decision_maker_id"])
        result = self.requests(packet["packet_id"])
        self.assertEqual(result["status"], "READY")
        columns = {row["name"] for row in self.database.connection.execute("PRAGMA table_info(channel_draft_requests)")}
        self.assertNotIn("subject", columns)
        self.assertNotIn("message_body", columns)
        row = self.database.connection.execute("SELECT * FROM channel_draft_requests LIMIT 1").fetchone()
        self.assertEqual(row["status"], "PENDING_POLICY")
        self.assertNotIn("formal_proposal", json.loads(row["request_json"]))

    def test_linkedin_roles_are_allowlisted(self):
        self.assertIn("founder", ALLOWED_DECISION_MAKER_ROLES)
        self.assertIn("business director", ALLOWED_DECISION_MAKER_ROLES)

    def test_migration_is_idempotent_and_defaults_are_safe(self):
        first_version = migrate_step15(self.database)
        second_version = migrate_step15(self.database)
        self.assertEqual(first_version, 17)
        self.assertEqual(second_version, 17)
        config = self.database.read_config()
        self.assertEqual(self.default_config["decision_maker_enrichment_enabled"], 0)
        self.assertEqual(self.default_config["linkedin_provider_mode"], "FIXTURE_ONLY")
        self.assertEqual(self.default_config["linkedin_sending_enabled"], 0)
        self.assertEqual(self.default_config["linkedin_daily_cap"], 0)
        self.assertEqual(self.default_config["linkedin_message_policy_status"], "UNDECIDED")
        self.assertEqual(self.default_config["personalization_research_enabled"], 0)
        self.assertEqual(config["linkedin_provider_mode"], "FIXTURE_ONLY")
        self.assertEqual(config["linkedin_sending_enabled"], 0)
        self.assertEqual(config["linkedin_daily_cap"], 1)  # explicitly enabled only for this fixture
        self.assertEqual(config["linkedin_message_policy_status"], "UNDECIDED")
        self.assertEqual(config["personalization_research_enabled"], 1)

    def test_no_live_linkedin_or_message_surface_exists(self):
        import step15_identity_personalization as module
        source = Path(module.__file__).read_text(encoding="utf-8").lower()
        for forbidden in ("requests.", "urllib", "browser", "selenium", "playwright", "smtplib", "imaplib", "httpx"):
            self.assertNotIn(forbidden, source)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
