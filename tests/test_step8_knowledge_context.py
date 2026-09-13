import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from paldo_os_outbound import Database
from step8_knowledge_context import (
    REQUIRED_GOLD_SECTIONS,
    ContextPacketNotReadyError,
    build_context_packet,
    explain_packet_readiness,
    get_current_context_packet,
    invalidate_context_packet,
    mark_packet_used_by_batch,
    migrate_step8,
    retrieve_and_build_context_packet,
    summarize_packet_provenance,
    validate_recalled_gold_records,
)


UTC = timezone.utc


def iso(value):
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


class FictionalRetrievalProvider:
    def __init__(self, records):
        self.records = records
        self.calls = []

    def kb_recall(self, *, campaign_id, sections):
        self.calls.append((campaign_id, tuple(sections)))
        return list(self.records)


def source(section, content, index, *, revision="fictional-rev-1", **extra):
    now = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
    return {
        "source_id": f"fictional-gold-{index}",
        "status": "Gold",
        "trust_tier": "Gold",
        "approval_status": "APPROVED",
        "approved_by": "OPERATOR",
        "confidence": 0.95,
        "heading": section,
        "drive_file_id": f"drive-fictional-{index}",
        "kb_path": f"gold/fictional/{index}",
        "source_url": f"https://kb-fictional.invalid/gold/{index}",
        "modified_at": iso(now - timedelta(days=1)),
        "snapshot_revision": revision,
        "manifest_revision": revision,
        "content_hash": hashlib.sha256(content.encode()).hexdigest(),
        "retrieved_at": iso(now),
        "section": section,
        "content": content,
        **extra,
    }


def complete_records(revision="fictional-rev-1"):
    contents = {
        "ICP": "Owner-led fictional local service businesses with appointment-dependent workflows.",
        "ACTIVE_OFFER_CTA": "Active offer: Business Booking and Follow-Up System. CTA: request an operator review.",
        "VOICE_GUIDE": "Use clear, calm, specific language. Avoid hype and pressure.",
        "APPROVED_PROOF_RULES": "Use only approved, attributable proof; qualify claims and preserve source context.",
        "OUTBOUND_COMPLIANCE_POLICY": "Do not claim guaranteed results. Do not use private contact data. Honor opt-outs.",
        "ACTIVE_CAMPAIGN_DECISION": "Fictional audit campaign is approved for context testing only; no sending is authorized.",
    }
    return [source(section, content, index, revision=revision) for index, (section, content) in enumerate(contents.items(), 1)]


class Step8KnowledgeContextTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.temp_dir.name) / "fixture.sqlite3")
        self.database.migrate_step2()
        migrate_step8(self.database)
        self.campaign_id = self.database.connection.execute("SELECT id FROM campaigns ORDER BY id LIMIT 1").fetchone()[0]
        self.now = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)

    def tearDown(self):
        self.database.close()
        self.temp_dir.cleanup()

    def test_complete_gold_records_produce_ready_packet(self):
        packet = build_context_packet(self.database, self.campaign_id, complete_records(), now=self.now)
        self.assertEqual(packet["readiness_state"], "READY")
        self.assertEqual(set(packet["required_section_coverage"]), set(REQUIRED_GOLD_SECTIONS))
        self.assertLessEqual(len(packet["approved_content"]), 6000)
        self.assertTrue(packet["packet_hash"])

    def test_non_gold_tiers_and_noncanonical_private_record_are_rejected(self):
        for status in ("Bronze", "Silver"):
            record = complete_records()[0].copy()
            record.update(status=status, trust_tier=status, approval_status="REVIEW")
            result = validate_recalled_gold_records([record], now=self.now)
            self.assertEqual(result["accepted"], [])
            self.assertTrue(result["rejected"])
        record = complete_records()[0].copy()
        record.update(status="Silver", trust_tier="Silver", access="private", approval_status="REVIEW")
        packet = build_context_packet(self.database, self.campaign_id, [record], now=self.now)
        self.assertEqual(packet["readiness_state"], "NOT_READY")

    def test_missing_required_section_is_not_ready(self):
        records = complete_records()[:-1]
        packet = build_context_packet(self.database, self.campaign_id, records, now=self.now)
        self.assertEqual(packet["readiness_state"], "NOT_READY")
        explanation = explain_packet_readiness(self.database, packet["packet_id"])
        self.assertIn("ACTIVE_CAMPAIGN_DECISION", explanation["missing_sections"])

    def test_missing_source_metadata_is_not_ready(self):
        record = complete_records()[0].copy()
        record.pop("drive_file_id")
        packet = build_context_packet(self.database, self.campaign_id, complete_records()[:-1] + [record], now=self.now)
        self.assertEqual(packet["readiness_state"], "NOT_READY")
        self.assertTrue(explain_packet_readiness(self.database, packet["packet_id"])["reasons"])

    def test_contradictory_gold_records_are_not_ready_and_trace_sources(self):
        records = complete_records()
        conflict = records[0].copy()
        conflict["source_id"] = "fictional-gold-conflict"
        conflict["drive_file_id"] = "drive-fictional-conflict"
        conflict["content"] = "A contradictory ICP claim for a different fictional segment."
        conflict["content_hash"] = hashlib.sha256(conflict["content"].encode()).hexdigest()
        records.append(conflict)
        packet = build_context_packet(self.database, self.campaign_id, records, now=self.now)
        self.assertEqual(packet["readiness_state"], "NOT_READY")
        self.assertEqual(packet["contradiction_status"], "CONTRADICTORY")
        explanation = explain_packet_readiness(self.database, packet["packet_id"])
        self.assertTrue(explanation["contradictions"])
        self.assertIn("fictional-gold-conflict", json.dumps(explanation))

    def test_unchanged_inputs_reuse_deterministic_packet(self):
        records = complete_records()
        first = build_context_packet(self.database, self.campaign_id, records, now=self.now)
        second = build_context_packet(self.database, self.campaign_id, records, now=self.now + timedelta(hours=1))
        self.assertEqual(first["packet_id"], second["packet_id"])
        self.assertEqual(first["packet_hash"], second["packet_hash"])
        count = self.database.connection.execute("SELECT COUNT(*) FROM kb_context_packets").fetchone()[0]
        self.assertEqual(count, 1)

    def test_changed_snapshot_invalidates_old_packet(self):
        first = build_context_packet(self.database, self.campaign_id, complete_records(), now=self.now)
        second = build_context_packet(self.database, self.campaign_id, complete_records("fictional-rev-2"), now=self.now + timedelta(hours=1))
        self.assertNotEqual(first["packet_id"], second["packet_id"])
        old = explain_packet_readiness(self.database, first["packet_id"])
        self.assertEqual(old["readiness_state"], "INVALIDATED")
        self.assertEqual(old["invalidation_reason"], "SNAPSHOT_CHANGED")

    def test_expired_packet_is_not_usable(self):
        packet = build_context_packet(self.database, self.campaign_id, complete_records(), now=self.now)
        self.assertIsNone(get_current_context_packet(self.database, self.campaign_id, now=self.now + timedelta(hours=25)))
        self.assertEqual(explain_packet_readiness(self.database, packet["packet_id"])["readiness_state"], "EXPIRED")

    def test_size_limit_is_deterministic_and_compliance_survives_truncation(self):
        records = complete_records()
        compliance = records[4]
        compliance["content"] = "COMPLIANCE RULE: Do not claim guaranteed results or imply approval.\nExamples:\n" + ("example text; " * 1200)
        compliance["content_hash"] = hashlib.sha256(compliance["content"].encode()).hexdigest()
        packet = build_context_packet(self.database, self.campaign_id, records, now=self.now)
        self.assertEqual(packet["readiness_state"], "READY")
        self.assertLessEqual(len(packet["approved_content"]), 6000)
        self.assertIn("Do not claim guaranteed results", packet["approved_content"])
        self.assertTrue(packet["truncation"]["truncated"])

    def test_retrieval_provider_is_called_once_and_no_external_binding_is_used(self):
        provider = FictionalRetrievalProvider(complete_records())
        packet = retrieve_and_build_context_packet(self.database, self.campaign_id, provider, now=self.now)
        self.assertEqual(packet["readiness_state"], "READY")
        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(provider.calls[0][0], self.campaign_id)

    def test_packet_used_by_batch_is_auditable_without_drafting(self):
        packet = build_context_packet(self.database, self.campaign_id, complete_records(), now=self.now)
        mark_packet_used_by_batch(self.database, packet["packet_id"], "fictional-batch-1")
        summary = summarize_packet_provenance(self.database, packet["packet_id"])
        self.assertTrue(summary["used_by_batch"])
        self.assertEqual(summary["batch_count"], 1)
        self.assertNotIn("Do not claim", json.dumps(summary))

    def test_drafting_fails_closed_without_ready_packet(self):
        from step8_knowledge_context import require_ready_context_packet
        with self.assertRaises(ContextPacketNotReadyError):
            require_ready_context_packet(self.database, self.campaign_id, now=self.now)

    def test_no_prospect_data_is_written_to_knowledge_context_tables(self):
        records = complete_records()
        records[0]["content"] += "\nFictional prospect: prospect@example.invalid"
        records[0]["content_hash"] = hashlib.sha256(records[0]["content"].encode()).hexdigest()
        packet = build_context_packet(self.database, self.campaign_id, records, now=self.now)
        stored = self.database.connection.execute("SELECT packet_content FROM kb_context_packets WHERE id=?", (packet["packet_id"],)).fetchone()[0]
        self.assertNotIn("prospect@example.invalid", stored)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)

    def test_migration_is_idempotent_and_defaults_are_safe(self):
        self.assertEqual(migrate_step8(self.database), 10)
        self.assertEqual(migrate_step8(self.database), 10)
        config = self.database.read_config()
        self.assertTrue(config["knowledge_context_required"])
        self.assertEqual(config["knowledge_context_packet_ttl_hours"], 24)
        self.assertEqual(config["knowledge_context_packet_max_chars"], 6000)
        self.assertEqual(config["system_state"], "PAUSED")
        self.assertEqual(config["discovery_mode"], "DRY_RUN")
        self.assertEqual(config["daily_message_cap"], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
