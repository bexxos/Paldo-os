import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from paldo_os_outbound import Database
from paldo_research_queue import (
    commit_candidate_research,
    migrate_research_queue,
    record_decision_maker_evidence,
    research_queue_plan,
    reserve_research_batch,
)
from step23_expansion import migrate_expansion


UTC = timezone.utc


class ResearchQueueTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "queue.sqlite3")
        self.addCleanup(self.db.close)
        self.addCleanup(self.tempdir.cleanup)
        migrate_expansion(self.db)
        migrate_research_queue(self.db)
        base = datetime(2026, 9, 8, 1, 0, tzinfo=UTC)
        historical_ids = set(range(1, 10)) | {12} | set(range(26, 36))
        with self.db.connection:
            for candidate_id in range(1, 51):
                state = "RESEARCHED" if candidate_id in historical_ids else "NOT_PROCESSED"
                self.db.connection.execute(
                    """INSERT INTO discovery_candidates
                       (id,source,source_record_id,source_url,collected_at,business_name,business_category,country,
                        raw_payload_hash,ingestion_status,ingestion_decision,provenance_mode,provenance_type,
                        observed_values,created_at,updated_at,ingestion_outcome,research_state,qualification_state,
                        queue_state,proposal_state,research_packet_json,decision_maker_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        candidate_id,
                        "FIXTURE",
                        f"fixture-{candidate_id}",
                        f"https://fixture.test/{candidate_id}",
                        (base + timedelta(minutes=candidate_id)).isoformat(),
                        f"Business {candidate_id}",
                        "business",
                        "PH",
                        f"hash-{candidate_id}",
                        "ACCEPTED",
                        "ACCEPT",
                        "LIVE",
                        "FIXTURE",
                        "{}",
                        (base + timedelta(minutes=candidate_id)).isoformat(),
                        (base + timedelta(minutes=candidate_id)).isoformat(),
                        "ACCEPTED",
                        state,
                        "NOT_EVALUATED",
                        "QUEUED" if state == "NOT_PROCESSED" else "NONE",
                        "NONE",
                        '{"historical":true}' if state == "RESEARCHED" else None,
                        "{}",
                    ),
                )
        self.db.connection.commit()

    def test_historical_research_does_not_consume_today_and_oldest_20_are_selected(self):
        now = datetime(2026, 9, 9, 2, 0, tzinfo=UTC)  # 10:00 PHT
        plan = research_queue_plan(self.db, now=now, daily_cap=20)
        self.assertEqual(plan["local_date"], "2026-09-09")
        self.assertEqual(plan["completed_today"], 0)
        self.assertEqual(plan["waiting_count"], 30)
        self.assertTrue(plan["discovery_skipped"])

        batch = reserve_research_batch(self.db, now=now, daily_cap=20)
        self.assertEqual(
            [row["id"] for row in batch],
            [10, 11, *range(13, 26), *range(36, 41)],
        )

    def test_commit_progress_is_per_candidate_and_same_day_rerun_has_real_remaining_capacity(self):
        now = datetime(2026, 9, 9, 2, 0, tzinfo=UTC)
        first = reserve_research_batch(self.db, now=now, daily_cap=20)
        for row in first[:5]:
            result = commit_candidate_research(
                self.db,
                candidate_id=row["id"],
                research_packet={"candidate_id": row["id"], "observations": []},
                now=now,
            )
            self.assertEqual(result["status"], "COMPLETED")

        rerun = reserve_research_batch(self.db, now=now + timedelta(hours=1), daily_cap=20)
        self.assertEqual(len(rerun), 15)
        self.assertEqual({row["id"] for row in rerun}, {row["id"] for row in first[5:]})
        self.assertEqual(len({row["id"] for row in first[:5]} & {row["id"] for row in rerun}), 0)

        plan = research_queue_plan(self.db, now=now + timedelta(hours=1), daily_cap=20)
        self.assertEqual(plan["completed_today"], 5)
        self.assertEqual(plan["remaining_today"], 15)
        for row in rerun:
            commit_candidate_research(
                self.db,
                candidate_id=row["id"],
                research_packet={"candidate_id": row["id"], "observations": []},
                now=now + timedelta(hours=1),
            )
        self.assertEqual(reserve_research_batch(self.db, now=now + timedelta(hours=2), daily_cap=20), [])
        self.assertEqual(research_queue_plan(self.db, now=now + timedelta(hours=2), daily_cap=20)["completed_today"], 20)

    def test_next_local_day_selects_remaining_ten_and_same_candidate_is_idempotent(self):
        day_one = datetime(2026, 9, 9, 2, 0, tzinfo=UTC)
        first = reserve_research_batch(self.db, now=day_one, daily_cap=20)
        for row in first:
            commit_candidate_research(
                self.db,
                candidate_id=row["id"],
                research_packet={"candidate_id": row["id"], "observations": []},
                now=day_one,
            )

        day_two = datetime(2026, 9, 10, 2, 0, tzinfo=UTC)
        second = reserve_research_batch(self.db, now=day_two, daily_cap=20)
        self.assertEqual([row["id"] for row in second], list(range(41, 51)))
        self.assertTrue(research_queue_plan(self.db, now=day_two, daily_cap=20)["discovery_skipped"])
        for row in second:
            commit_candidate_research(
                self.db,
                candidate_id=row["id"],
                research_packet={"candidate_id": row["id"], "observations": []},
                now=day_two,
            )
        self.assertFalse(research_queue_plan(self.db, now=day_two, daily_cap=20)["discovery_skipped"])

        duplicate = commit_candidate_research(
            self.db,
            candidate_id=21,
            research_packet={"candidate_id": 21, "observations": ["changed"]},
            now=day_two,
        )
        self.assertEqual(duplicate["status"], "ALREADY_COMPLETED")
        stored = self.db.connection.execute(
            "SELECT research_packet_json FROM discovery_candidates WHERE id=21"
        ).fetchone()[0]
        self.assertIn('"observations":[]', stored)

    def test_interrupted_reservation_is_resumable_without_duplicate_processing(self):
        now = datetime(2026, 9, 9, 2, 0, tzinfo=UTC)
        first = reserve_research_batch(self.db, now=now, daily_cap=20)
        self.assertEqual(len(first), 20)
        resumed = reserve_research_batch(self.db, now=now + timedelta(hours=2), daily_cap=20)
        self.assertEqual([row["id"] for row in resumed], [row["id"] for row in first])
        self.assertEqual(len({row["id"] for row in resumed}), 20)

    def test_decision_maker_evidence_states_preserve_source_quality(self):
        now = datetime(2026, 9, 9, 2, 0, tzinfo=UTC)

        def packet(state, name, role, sources, searches):
            return {
                "verification_state": state,
                "name": name,
                "role": role,
                "confidence": "HIGH" if state == "VERIFIED" else "MEDIUM",
                "sources": sources,
                "source_classes": [item["source_type"] for item in sources],
                "searches_attempted": searches,
                "linkedin_profile_url": None,
            }

        snippet = packet(
            "NEEDS_OPERATOR_CONFIRMATION", "Avery Stone", "Owner",
            [{"url": "https://search.example/result", "source_type": "SEARCH_SNIPPET", "evidence_text": "Generated summary names Avery Stone.", "retrieved_at": "2026-09-09T02:00:00+00:00"}],
            ["official website cascade", "business + role variants"],
        )
        self.assertEqual(record_decision_maker_evidence(self.db, candidate_id=2, evidence=snippet, now=now)["verification_state"], "NEEDS_OPERATOR_CONFIRMATION")

        direct = packet(
            "VERIFIED", "Avery Stone", "Founder",
            [
                {"url": "https://business.example/about", "source_type": "OFFICIAL_WEBSITE", "evidence_text": "Avery Stone is identified as founder.", "retrieved_at": "2026-09-09T02:00:00+00:00"},
                {"url": "https://www.facebook.com/business.example", "source_type": "PUBLIC_FACEBOOK", "evidence_text": "The public business page identifies Avery Stone as founder.", "retrieved_at": "2026-09-09T02:00:00+00:00"},
            ],
            ["exact name + business", "exact name + city", "site:linkedin.com/in exact full name"],
        )
        self.assertEqual(record_decision_maker_evidence(self.db, candidate_id=3, evidence=direct, now=now)["verification_state"], "VERIFIED")

        conflict = packet(
            "CONFLICTING", "Avery Stone", "Managing partner",
            [{"url": "https://business.example/team", "source_type": "OFFICIAL_WEBSITE", "evidence_text": "Avery Stone is listed as director.", "retrieved_at": "2026-09-09T02:00:00+00:00"}, {"url": "https://directory.example/business", "source_type": "PUBLIC_DIRECTORY", "evidence_text": "A different person is listed as owner.", "retrieved_at": "2026-09-09T02:00:00+00:00"}],
            ["business + owner", "business + managing partner"],
        )
        self.assertEqual(record_decision_maker_evidence(self.db, candidate_id=4, evidence=conflict, now=now)["verification_state"], "CONFLICTING")

        unknown = packet("UNKNOWN", None, None, [], ["official website", "Facebook", "Instagram", "directories", "role variants", "person-based LinkedIn searches"])
        self.assertEqual(record_decision_maker_evidence(self.db, candidate_id=5, evidence=unknown, now=now)["verification_state"], "UNKNOWN")
        stored = json.loads(self.db.connection.execute("SELECT decision_maker_json FROM discovery_candidates WHERE id=5").fetchone()[0])
        self.assertEqual(stored["searches_attempted"][-1], "person-based LinkedIn searches")


if __name__ == "__main__":
    unittest.main()
