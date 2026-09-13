import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from paldo_os_outbound import Database
from step23_expansion import (
    ExpansionValidationError,
    LINKEDIN_COMPANY,
    LINKEDIN_PERSONAL,
    migrate_expansion,
    prepare_linkedin_draft,
    queue_linkedin_manual_cards,
    record_decision_maker_evidence,
    record_source_checkpoint,
    record_linkedin_match,
    upsert_social_opportunity,
    validate_linkedin_url,
)


NOW = datetime(2026, 9, 9, 4, 0, tzinfo=timezone.utc)


class Step23ExpansionTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "step23.sqlite3")
        self.addCleanup(self.db.close)
        self.addCleanup(self.tempdir.cleanup)
        migrate_expansion(self.db)
        self.db.connection.execute(
            """INSERT INTO discovery_candidates
               (source,source_record_id,source_url,collected_at,business_name,business_category,country,raw_payload_hash,
                ingestion_status,ingestion_decision,provenance_mode,provenance_type,observed_values,created_at,updated_at,
                ingestion_outcome,research_state,qualification_state,queue_state,proposal_state,research_packet_json,decision_maker_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "APIFY", "fixture-1", "https://example.test/maps/1", NOW.isoformat(), "Fixture Local services", "Skin care business", "PH",
                "hash-1", "ACCEPTED", "ACCEPT", "LIVE", "LIVE", "{}", NOW.isoformat(), NOW.isoformat(), "ACCEPTED",
                "RESEARCHED", "QUALIFIED", "NONE", "READY_FOR_DRAFT",
                json.dumps({"business_terminology": ["book your consultation"], "proof_outcome_match": ["made client records searchable"], "sources": [{"url": "https://fixture.test/about"}]}),
                json.dumps({"name": "Alex Business", "role": "Founder"}),
            ),
        )
        self.db.connection.commit()

    def test_linkedin_host_and_path_classification_is_strict(self):
        self.assertEqual(validate_linkedin_url("https://www.linkedin.com/in/alex-business" )["kind"], LINKEDIN_PERSONAL)
        self.assertEqual(validate_linkedin_url("https://in.linkedin.com/company/fixture" )["kind"], LINKEDIN_COMPANY)
        for bad in ("https://example.com/in/alex", "https://www.linkedin.com/pub/alex", "http://www.linkedin.com/in/alex"):
            with self.assertRaises(ExpansionValidationError):
                validate_linkedin_url(bad)

    def test_web_verified_or_operator_confirmed_personal_match_is_supported(self):
        possible = record_linkedin_match(
            self.db, candidate_id=1, person_name="Alex Business", person_role="Founder", location="Northfield, PH",
            profile_url="https://www.linkedin.com/in/alex-business", web_verification_status="POSSIBLE",
            web_evidence={"source_url": "https://fixture.test/about", "excerpt": "Founder listed on official page"}, now=NOW,
        )
        self.assertEqual(possible["match_status"], "POSSIBLE_PERSONAL_MATCH")
        web_verified = record_linkedin_match(
            self.db, candidate_id=1, person_name="Alex Business", person_role="Founder", location="Northfield, PH",
            profile_url="https://www.linkedin.com/in/alex-business", web_verification_status="VERIFIED",
            web_evidence={"source_url": "https://fixture.test/about", "excerpt": "Search result identifies Alex Business at Fixture Local services."}, now=NOW,
        )
        self.assertEqual(web_verified["operator_verification_status"], "NOT_REVIEWED")
        self.assertEqual(web_verified["match_status"], "SUPPORTED_PERSONAL")
        confirmed = record_linkedin_match(
            self.db, candidate_id=1, person_name="Alex Business", person_role="Founder", location="Northfield, PH",
            profile_url="https://www.linkedin.com/in/alex-business", web_verification_status="POSSIBLE",
            web_evidence={"source_url": "https://fixture.test/about", "excerpt": "Founder listed on official page"},
            operator_confirmed=True, operator_evidence="Operator compared the profile and exact business association.", operator_id="OPERATOR", now=NOW,
        )
        self.assertEqual(confirmed["web_verification_status"], "POSSIBLE")
        self.assertEqual(confirmed["operator_verification_status"], "CONFIRMED")
        self.assertEqual(confirmed["match_status"], "SUPPORTED_PERSONAL")

    def test_company_page_never_receives_personal_message_draft(self):
        row = record_linkedin_match(
            self.db, candidate_id=1, person_name="Fixture Local services", person_role="Company page", location="Northfield, PH",
            profile_url="https://www.linkedin.com/company/fixture-aesthetics", web_verification_status="VERIFIED",
            web_evidence={"source_url": "https://fixture.test/about"}, operator_confirmed=True,
            operator_evidence="Operator confirmed this is only the company page.", operator_id="OPERATOR", now=NOW,
        )
        self.assertEqual(row["url_kind"], LINKEDIN_COMPANY)
        with self.assertRaises(ExpansionValidationError):
            prepare_linkedin_draft(self.db, match_id=row["id"], now=NOW)

    def test_manual_card_and_confirmed_draft_are_link_free_in_connection_note(self):
        result = queue_linkedin_manual_cards(self.db, max_candidates=20, now=NOW)
        self.assertEqual(result["created"], 1)
        card = self.db.connection.execute("SELECT * FROM linkedin_queue_cards WHERE candidate_id=1").fetchone()
        self.assertIn("Candidate ID: 1", card["card_text"])
        row = record_linkedin_match(
            self.db, candidate_id=1, person_name="Alex Business", person_role="Founder", location="Northfield, PH",
            profile_url="https://www.linkedin.com/in/alex-business", web_verification_status="VERIFIED",
            web_evidence={"source_url": "https://fixture.test/about"}, operator_confirmed=True,
            operator_evidence="Operator compared the profile and exact business association.", operator_id="OPERATOR", now=NOW,
        )
        draft = prepare_linkedin_draft(self.db, match_id=row["id"], now=NOW)
        self.assertNotIn("http://", draft["connection_note"])
        self.assertNotIn("https://", draft["connection_note"])
        self.assertIn("client records searchable", draft["acceptance_message"])
        self.assertNotIn("http", draft["connection_note"])

    def test_pending_linkedin_card_refreshes_evidence_without_duplicate(self):
        first = queue_linkedin_manual_cards(self.db, max_candidates=20, now=NOW)
        self.assertEqual(first["created"], 1)
        evidence = {
            "verification_state": "NEEDS_OPERATOR_CONFIRMATION",
            "name": "Alex Business",
            "role": "Founder",
            "confidence": "MEDIUM",
            "sources": [{
                "url": "https://fixture.test/about",
                "source_type": "OFFICIAL_WEBSITE",
                "evidence_text": "Alex Business is named as founder of Fixture Local services.",
                "retrieved_at": NOW.isoformat(),
            }],
            "source_classes": ["OFFICIAL_WEBSITE", "PUBLIC_FACEBOOK"],
            "searches_attempted": ["official website cascade", "business + role variants", "person + business + LinkedIn"],
            "linkedin_profile_url": None,
        }
        record_decision_maker_evidence(self.db, candidate_id=1, evidence=evidence, now=NOW)
        second = queue_linkedin_manual_cards(self.db, max_candidates=20, now=NOW)
        self.assertEqual(second["created"], 0)
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM linkedin_queue_cards WHERE candidate_id=1").fetchone()[0], 1)
        card = self.db.connection.execute("SELECT * FROM linkedin_queue_cards WHERE candidate_id=1").fetchone()
        self.assertIn("NEEDS_OPERATOR_CONFIRMATION", card["card_text"])
        self.assertIn("OFFICIAL_WEBSITE", card["evidence_json"])
        self.assertIn("person + business + LinkedIn", card["search_terms_json"])

    def test_social_opportunity_is_distinct_and_routes_by_class(self):
        row = upsert_social_opportunity(
            self.db,
            {
                "source": "reddit",
                "community": "r/smallbusiness",
                "original_url": "https://www.reddit.com/r/smallbusiness/comments/fixture/",
                "published_at": "2026-09-08",
                "public_author": "TimelyConstruction88",
                "item_class": "HELP_SEEKING",
                "need": "Appointment scheduling and CRM that staff can operate simply",
                "supporting_excerpt": "Looking for an appointment scheduler for a small retail business...",
                "business_context": "8-10 employees using separate communications, scheduling, and CRM tools",
                "confidence": "HIGH",
                "intent_label": "ADVICE_SEEKING",
                "helpful_comment": "Map staff booking, client cancellation, calendar sync, and follow-up needs before changing tools.",
                "evidence": {"retrieval": "full public text returned"},
            },
            now=NOW,
        )
        self.assertEqual(row["route_topic"], 836)
        self.assertEqual(row["status"], "QUEUED")
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM social_opportunities").fetchone()[0], 1)

    def test_source_checkpoint_is_idempotent(self):
        record_source_checkpoint(self.db, source="reddit", cursor="cursor-1", status="COMPLETED", detail={"count": 2}, now=NOW)
        record_source_checkpoint(self.db, source="reddit", cursor="cursor-2", status="COMPLETED", detail={"count": 3}, now=NOW)
        row = self.db.connection.execute("SELECT source,cursor,status,detail_json FROM paldo_source_checkpoints WHERE source='reddit'").fetchone()
        self.assertEqual(tuple(row[:3]), ("reddit", "cursor-2", "COMPLETED"))
        self.assertIn('"count":3', row[3])


if __name__ == "__main__":
    unittest.main()
