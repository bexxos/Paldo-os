import tempfile
import unittest
from pathlib import Path

from paldo_os_outbound import (
    Database,
    DuplicateLeadError,
    REQUIRED_TABLES,
    SAFE_LEAD_STATUSES,
    LeadStatus,
    normalize_domain,
    normalize_email,
)


EXPECTED_COLUMNS = {
    "leads": [
        "id",
        "business_name",
        "website",
        "domain",
        "email",
        "phone",
        "industry",
        "country",
        "location",
        "source",
        "source_url",
        "status",
        "created_at",
        "updated_at",
    ],
    "evidence": [
        "id",
        "lead_id",
        "evidence_type",
        "observation",
        "source_url",
        "confidence",
        "captured_at",
    ],
    "lead_scores": ["lead_id", "score", "reasoning_data", "evaluated_at"],
    "offers": [
        "id",
        "name",
        "description",
        "problem_solved",
        "economic_outcome",
        "enabled",
        "created_at",
        "updated_at",
    ],
    "lead_offers": ["lead_id", "offer_id", "match_score", "reasoning", "created_at"],
    "outreach": ["id", "lead_id", "channel", "touch_number", "status", "created_at", "sent_at"],
    "suppressions": ["id", "email", "domain", "lead_id", "reason", "created_at"],
    "events": ["id", "event_type", "entity_type", "entity_id", "metadata", "created_at"],
    "system_config": ["key", "value", "value_type"],
}

EXPECTED_LIFECYCLE_VALUES = {
    "DISCOVERED",
    "ENRICHED",
    "QUALIFIED",
    "DRAFT_READY",
    "REVIEW_PENDING",
    "APPROVED",
    "SENT",
    "FOLLOWUP_DUE",
    "REPLIED",
    "INTERESTED",
    "CALL_BOOKED",
    "PROPOSAL",
    "WON",
    "LOST",
    "REJECTED",
    "DO_NOT_CONTACT",
    "BOUNCED",
    "UNSUBSCRIBED",
    "DUPLICATE",
}


class FoundationInitializationTests(unittest.TestCase):
    def test_initialization_uses_approved_schema_and_safe_defaults(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Database(Path(temporary_directory) / "foundation.sqlite3")
            self.addCleanup(database.close)

            table_names = {
                row[0]
                for row in database.connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
            self.assertEqual(table_names, REQUIRED_TABLES | {"system_config"})

            for table_name, expected_columns in EXPECTED_COLUMNS.items():
                columns = [
                    row[1]
                    for row in database.connection.execute(f"PRAGMA table_info({table_name})")
                ]
                self.assertEqual(columns, expected_columns, table_name)

            self.assertEqual(SAFE_LEAD_STATUSES, EXPECTED_LIFECYCLE_VALUES)
            self.assertEqual(LeadStatus.DISCOVERED.value, "DISCOVERED")
            self.assertEqual(database.read_config()["system_state"], "PAUSED")
            self.assertEqual(database.get_config("daily_message_cap"), 0)
            self.assertEqual(database.get_config("max_touches"), 3)
            self.assertEqual(database.get_config("followup_days"), 3)
            self.assertEqual(database.get_config("minimum_qualification_score"), 70)
            self.assertEqual(database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)

    def test_revenue_principle_is_durable_and_allowlisted(self):
        principle = (
            "We do not sell generic AI automation. The system will eventually identify "
            "business problems and match them to offers with measurable economic outcomes."
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "foundation.sqlite3"
            database = Database(database_path)
            database.close()

            reopened = Database(database_path)
            self.addCleanup(reopened.close)
            self.assertEqual(reopened.get_config("revenue_principle"), principle)
            stored = reopened.connection.execute(
                "SELECT value, value_type FROM system_config WHERE key = ?",
                ("revenue_principle",),
            ).fetchone()
            self.assertEqual(tuple(stored), (principle, "text"))
            reopened.set_config("revenue_principle", principle)
            self.assertEqual(reopened.get_config("revenue_principle"), principle)
            self.assertEqual(reopened.get_config("system_state"), "PAUSED")

    def test_config_reads_are_allowlisted_validated_and_safe(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Database(Path(temporary_directory) / "foundation.sqlite3")
            self.addCleanup(database.close)

            database.set_config("system_state", "ACTIVE")
            database.set_config("daily_message_cap", 12)
            self.assertEqual(database.read_config()["system_state"], "ACTIVE")
            self.assertEqual(database.get_config("daily_message_cap"), 12)
            self.assertIsNone(database.get_config("not_a_real_setting"))
            self.assertEqual(database.get_config("not_a_real_setting", "safe"), "safe")

            with self.assertRaises(ValueError):
                database.set_config("daily_message_cap", -1)
            with self.assertRaises(ValueError):
                database.set_config("max_touches", 0)
            with self.assertRaises(ValueError):
                database.set_config("system_state", "RUNNING")
            with self.assertRaises(KeyError):
                database.set_config("unknown_setting", 1)

            database.connection.execute(
                "UPDATE system_config SET value = ?, value_type = ? WHERE key = ?",
                ("not-an-integer", "integer", "daily_message_cap"),
            )
            self.assertEqual(database.get_config("daily_message_cap"), 0)

    def test_normalization_is_deterministic_for_email_and_domain(self):
        self.assertEqual(
            normalize_email("  Owner@WWW.Seaglass.Example.  "),
            "owner@seaglass.example",
        )
        self.assertEqual(
            normalize_domain(" HTTPS://WWW.Seaglass.Example./directory "),
            "seaglass.example",
        )
        with self.assertRaises(ValueError):
            normalize_email("not-an-email")
        with self.assertRaises(ValueError):
            normalize_domain("not a host")
        with self.assertRaises(ValueError):
            normalize_domain("owner@seaglass.example")

    def test_fictional_lead_insert_uses_requested_columns_and_safe_default(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Database(Path(temporary_directory) / "foundation.sqlite3")
            self.addCleanup(database.close)

            lead_id = database.insert_lead(
                business_name="Fictional Seaglass Books",
                website="https://www.seaglass.example/",
                domain="WWW.Seaglass.Example",
                email="Owner@Seaglass.Example",
                phone="+1-555-0100",
                industry="fictional publishing",
                country="US",
                location="Fictional Harbor",
                source="fictional fixture",
                source_url="https://source.example/fictional-seaglass",
            )

            lead = database.get_lead(lead_id)
            self.assertEqual(lead["business_name"], "Fictional Seaglass Books")
            self.assertEqual(lead["email"], "owner@seaglass.example")
            self.assertEqual(lead["domain"], "seaglass.example")
            self.assertEqual(lead["status"], "DISCOVERED")
            self.assertEqual(lead["created_at"], lead["updated_at"])

    def test_all_requested_lifecycle_values_are_accepted_and_unknown_values_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Database(Path(temporary_directory) / "foundation.sqlite3")
            self.addCleanup(database.close)

            for lifecycle_value in sorted(EXPECTED_LIFECYCLE_VALUES):
                lead_id = database.insert_lead(
                    business_name=f"Fictional {lifecycle_value}",
                    domain=f"{lifecycle_value.lower()}.example",
                    status=lifecycle_value,
                )
                self.assertEqual(database.get_lead(lead_id)["status"], lifecycle_value)

            with self.assertRaises(ValueError):
                database.insert_lead(
                    business_name="Fictional Unsafe Status",
                    domain="unsafe-status.example",
                    status="SENDING",
                )

    def test_duplicate_checks_use_normalized_email_or_domain_not_business_name(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Database(Path(temporary_directory) / "foundation.sqlite3")
            self.addCleanup(database.close)
            first_id = database.insert_lead(
                business_name="Same Fictional Name",
                email="first@seaglass.example",
                domain="seaglass.example",
            )
            second_id = database.insert_lead(
                business_name="Same Fictional Name",
                email="second@ember.example",
                domain="ember.example",
            )
            self.assertNotEqual(first_id, second_id)
            self.assertTrue(database.is_duplicate_lead(email=" FIRST@SEAGLASS.EXAMPLE "))
            self.assertTrue(database.is_duplicate_lead(domain="https://www.seaglass.example/"))
            self.assertFalse(database.is_duplicate_lead(email="new@different.example"))

            with self.assertRaises(DuplicateLeadError):
                database.insert_lead(
                    business_name="Different Fictional Name",
                    email="first@seaglass.example",
                    domain="new-domain.example",
                )
            with self.assertRaises(DuplicateLeadError):
                database.insert_lead(
                    business_name="Different Fictional Name",
                    email="new@seaglass.example",
                    domain="seaglass.example",
                )

            database.insert_lead(
                business_name="Implicit Domain Fixture",
                email="first@implicit.example",
            )
            with self.assertRaises(DuplicateLeadError):
                database.insert_lead(
                    business_name="Another Implicit Domain Fixture",
                    email="second@implicit.example",
                )

            unaddressed_id = database.insert_lead(business_name="Same Fictional Name")
            self.assertIsNotNone(unaddressed_id)

    def test_evidence_scores_and_offers_preserve_revenue_framing(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Database(Path(temporary_directory) / "foundation.sqlite3")
            self.addCleanup(database.close)
            lead_id = database.insert_lead(
                business_name="Fictional Harbor Studio",
                domain="harbor-studio.example",
            )

            evidence_id = database.insert_evidence(
                lead_id=lead_id,
                evidence_type="fictional_observation",
                observation="quote requests are answered only during business hours",
                source_url="https://source.example/harbor-studio",
                confidence=0.9,
            )
            self.assertGreater(evidence_id, 0)
            evidence = database.get_evidence(evidence_id)
            self.assertEqual(evidence["evidence_type"], "fictional_observation")
            self.assertEqual(evidence["observation"], "quote requests are answered only during business hours")
            self.assertEqual(evidence["confidence"], 0.9)

            database.insert_lead_score(
                lead_id=lead_id,
                score=82,
                reasoning_data={
                    "business_problem": "missed after-hours quote opportunities",
                    "economic_signal": "fictional estimate of 8 missed requests monthly",
                },
            )
            score = database.get_latest_lead_score(lead_id)
            self.assertEqual(score["score"], 82)
            self.assertEqual(score["reasoning_data"]["business_problem"], "missed after-hours quote opportunities")

            offer_id = database.insert_offer(
                name="Quote response recovery",
                description="A fictional response workflow for qualified quote requests.",
                problem_solved="missed after-hours quote opportunities",
                economic_outcome="recover 5 additional qualified quote requests per month",
            )
            offer = database.get_offer(offer_id)
            self.assertEqual(offer["problem_solved"], "missed after-hours quote opportunities")
            self.assertEqual(offer["economic_outcome"], "recover 5 additional qualified quote requests per month")
            self.assertEqual(offer["enabled"], 1)

            database.link_lead_offer(
                lead_id=lead_id,
                offer_id=offer_id,
                match_score=88,
                reasoning="fictional evidence points to response-time leakage",
            )
            match = database.get_lead_offer(lead_id, offer_id)
            self.assertEqual(match["match_score"], 88)
            self.assertEqual(match["reasoning"], "fictional evidence points to response-time leakage")

            with self.assertRaises(ValueError):
                database.insert_offer(
                    name="Generic automation",
                    description="",
                    problem_solved="",
                    economic_outcome="",
                )

    def test_suppressions_normalize_identifiers_and_match_safely(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Database(Path(temporary_directory) / "foundation.sqlite3")
            self.addCleanup(database.close)
            suppression_id = database.add_suppression(
                email=" OptOut@WWW.Quiet-Example.example ",
                reason="fictional opt-out",
            )
            self.assertGreater(suppression_id, 0)
            suppression = database.connection.execute(
                "SELECT email, domain, lead_id, reason FROM suppressions WHERE id = ?",
                (suppression_id,),
            ).fetchone()
            self.assertEqual(tuple(suppression), ("optout@quiet-example.example", "quiet-example.example", None, "fictional opt-out"))
            self.assertTrue(database.is_suppressed(email="optout@quiet-example.example"))
            self.assertTrue(database.is_suppressed(domain="www.quiet-example.example"))
            self.assertFalse(database.is_suppressed(email="other@different.example"))

            lead_id = database.insert_lead(
                business_name="Fictional Suppressed Shop",
                domain="suppressed-shop.example",
            )
            database.add_suppression(lead_id=lead_id, reason="fictional lead block")
            self.assertTrue(database.is_suppressed(lead_id=lead_id))
            with self.assertRaises(ValueError):
                database.add_suppression(reason="missing identifier")

    def test_event_logging_persists_structured_metadata(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "foundation.sqlite3"
            database = Database(database_path)
            lead_id = database.insert_lead(
                business_name="Fictional Signal Studio",
                domain="signal-studio.example",
            )
            event_id = database.log_event(
                event_type="fictional_lead_created",
                entity_type="lead",
                entity_id=lead_id,
                metadata={"source": "fictional fixture", "priority": 2},
            )
            database.close()

            reopened = Database(database_path)
            self.addCleanup(reopened.close)
            event = reopened.get_event(event_id)
            self.assertEqual(event["event_type"], "fictional_lead_created")
            self.assertEqual(event["entity_type"], "lead")
            self.assertEqual(event["entity_id"], str(lead_id))
            self.assertEqual(event["metadata"], {"priority": 2, "source": "fictional fixture"})
            self.assertIsNotNone(event["created_at"])


if __name__ == "__main__":
    unittest.main()
