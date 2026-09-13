import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from paldo_os_outbound import Database
from step14_scheduler import (
    FixtureSchedulerRetryableError,
    FixtureSchedulerExecutorRegistry,
    FixtureSchedulerUnknownOutcome,
    SCHEDULER_MIGRATION_VERSION,
    calculate_campaign_local_date,
    calculate_scheduler_due_date,
    detect_missed_windows,
    generate_fixture_weekly_review,
    inspect_interrupted_runs,
    inspect_scheduler_readiness,
    inspect_scheduler_run,
    migrate_step14,
    plan_shared_daily_capacity,
    resume_fixture_interrupted_run,
    run_fixture_scheduled_window,
    select_due_followup_items,
    update_fixture_reservation_state,
    verify_startup_recovery_readiness,
)

UTC = timezone.utc
NOW = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


class Step14SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db = Database(Path(self.temp_dir.name) / "step14.sqlite3")
        self.addCleanup(self.db.close)
        migrate_step14(self.db)
        self.campaign_id = self.db.connection.execute(
            "SELECT id FROM campaigns ORDER BY id LIMIT 1"
        ).fetchone()[0]

    def enable_fixture_window(self, campaign_timezone="UTC"):
        self.db.set_config("scheduler_enabled", 1)
        self.db.set_config("system_state", "ACTIVE")
        for key in (
            "daily_message_cap",
            "daily_candidate_processing_cap",
            "drafting_daily_cap",
            "gmail_draft_daily_cap",
            "followup_daily_cap",
            "notification_daily_cap",
        ):
            self.db.set_config(key, 1)
        with self.db.connection:
            self.db.connection.execute(
                "UPDATE campaigns SET status='ACTIVE',campaign_timezone=? WHERE id=?",
                (campaign_timezone, self.campaign_id),
            )

    def test_migration_defaults_are_disabled_and_idempotent(self):
        self.assertEqual(migrate_step14(self.db), SCHEDULER_MIGRATION_VERSION)
        config = self.db.read_config()
        self.assertEqual(config["scheduler_enabled"], 0)
        self.assertEqual(config["scheduler_mode"], "FIXTURE_ONLY")
        self.assertEqual(config["scheduler_trigger_mode"], "MANUAL_FIXTURE_ONLY")
        self.assertEqual(config["scheduler_install_state"], "NOT_INSTALLED")
        self.assertEqual(config["scheduler_max_concurrent_runs"], 1)
        self.assertEqual(config["scheduler_max_attempts"], 2)
        self.assertEqual(config["scheduler_catchup_enabled"], 0)
        self.assertEqual(config["scheduler_timezone_policy"], "CAMPAIGN_LOCAL")
        self.assertEqual(config["scheduler_daily_run_time"], "UNDECIDED")
        self.assertEqual(config["scheduler_weekly_review_schedule"], "UNDECIDED")
        self.assertEqual(
            self.db.connection.execute(
                "SELECT COUNT(*) FROM scheduler_runs"
            ).fetchone()[0],
            0,
        )

    def test_missing_and_invalid_timezone_are_not_ready(self):
        missing = inspect_scheduler_readiness(
            self.db,
            campaign_id=self.campaign_id,
            campaign_timezone=None,
            fixture_override=True,
            now=NOW,
        )
        self.assertFalse(missing["ready"])
        self.assertIn("CAMPAIGN_TIMEZONE_REQUIRED", missing["blocking_reasons"])

        invalid = inspect_scheduler_readiness(
            self.db,
            campaign_id=self.campaign_id,
            campaign_timezone="US/Not_A_Zone",
            fixture_override=True,
            now=NOW,
        )
        self.assertFalse(invalid["ready"])
        self.assertIn("CAMPAIGN_TIMEZONE_INVALID", invalid["blocking_reasons"])

    def test_campaign_local_date_and_dst_use_injected_iana_timezone(self):
        before_fallback = datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
        after_fallback = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
        self.assertEqual(
            calculate_campaign_local_date(before_fallback, "America/New_York"),
            date(2026, 11, 1),
        )
        self.assertEqual(
            calculate_campaign_local_date(after_fallback, "America/New_York"),
            date(2026, 11, 1),
        )
        self.assertEqual(
            calculate_campaign_local_date(NOW, "UTC"),
            date(2026, 11, 1),
        )

    def test_shared_capacity_prioritizes_followups_and_reuses_reservations(self):
        first = plan_shared_daily_capacity(
            self.db,
            campaign_id=self.campaign_id,
            campaign_timezone="UTC",
            local_date=date(2026, 11, 2),
            caps={
                "candidate_processing": 2,
                "drafting": 2,
                "gmail_draft_creation": 2,
                "followup": 1,
                "message_touch": 1,
                "notification_delivery": 1,
            },
            followup_items=[{"entity_id": "seq-1", "version": "v1"}],
            candidate_items=[{"entity_id": "candidate-1", "version": "v1"}],
            fixture_override=True,
            now=NOW,
        )
        second = plan_shared_daily_capacity(
            self.db,
            campaign_id=self.campaign_id,
            campaign_timezone="UTC",
            local_date=date(2026, 11, 2),
            caps={
                "candidate_processing": 2,
                "drafting": 2,
                "gmail_draft_creation": 2,
                "followup": 1,
                "message_touch": 1,
                "notification_delivery": 1,
            },
            followup_items=[{"entity_id": "seq-1", "version": "v1"}],
            candidate_items=[{"entity_id": "candidate-1", "version": "v1"}],
            fixture_override=True,
            now=NOW,
        )
        self.assertEqual(first["priority"], "FOLLOWUP_FIRST")
        self.assertEqual(first["followup_reserved"], 1)
        self.assertEqual(first["new_work_reserved"], 0)
        self.assertTrue(second["reused"])
        self.assertEqual(
            self.db.connection.execute(
                "SELECT COUNT(*) FROM scheduler_capacity_reservations"
            ).fetchone()[0],
            1,
        )

    def test_disabled_scheduler_blocks_window_without_creating_run(self):
        result = run_fixture_scheduled_window(
            self.db,
            campaign_id=self.campaign_id,
            campaign_timezone="UTC",
            now=NOW,
            fixture_override=True,
            executors={},
        )
        self.assertFalse(result["ok"])
        self.assertIn("SCHEDULER_DISABLED", result["blocking_reasons"])
        self.assertEqual(
            self.db.connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0],
            0,
        )

    def test_paused_inactive_and_zero_capacity_states_block(self):
        paused = inspect_scheduler_readiness(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            fixture_override=True, now=NOW,
        )
        self.assertIn("SYSTEM_PAUSED", paused["blocking_reasons"])
        self.assertIn("CAMPAIGN_INACTIVE", paused["blocking_reasons"])
        self.enable_fixture_window()
        for key in (
            "daily_message_cap", "daily_candidate_processing_cap", "drafting_daily_cap",
            "gmail_draft_daily_cap", "followup_daily_cap", "notification_daily_cap",
        ):
            self.db.set_config(key, 0)
        zero = inspect_scheduler_readiness(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            fixture_override=True, now=NOW,
        )
        self.assertIn("ALL_CAPACITIES_ZERO", zero["blocking_reasons"])

    def test_injected_business_day_holiday_is_respected(self):
        due = calculate_scheduler_due_date(
            datetime(2026, 11, 6, 12, tzinfo=UTC), "BUSINESS_DAYS", 1,
            "UTC", ["2026-11-09"],
        )
        self.assertEqual(due.date(), date(2026, 11, 10))

    def test_suppression_invalidates_unused_reservation(self):
        self.enable_fixture_window()
        caps = {key: 1 for key in (
            "candidate_processing", "drafting", "gmail_draft_creation", "followup",
            "message_touch", "notification_delivery",
        )}
        item = {"entity_id": "candidate-2", "version": "v1"}
        first = plan_shared_daily_capacity(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            local_date=date(2026, 11, 2), caps=caps, candidate_items=[item],
            fixture_override=True, now=NOW,
        )
        self.assertEqual(first["new_work_reserved"], 1)
        plan_shared_daily_capacity(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            local_date=date(2026, 11, 2), caps=caps,
            candidate_items=[{**item, "suppressed": True}], fixture_override=True, now=NOW,
        )
        state = self.db.connection.execute(
            "SELECT reservation_state,invalidation_reason FROM scheduler_capacity_reservations"
        ).fetchone()
        self.assertEqual(state["reservation_state"], "INVALIDATED")
        self.assertEqual(state["invalidation_reason"], "SUPPRESSION_OR_STALE_INPUT")

    def test_reservation_state_is_auditable_and_executor_registry_is_allowlisted(self):
        self.enable_fixture_window()
        result = plan_shared_daily_capacity(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            local_date=date(2026, 11, 2),
            caps={key: 1 for key in ("candidate_processing", "drafting", "gmail_draft_creation", "followup", "message_touch", "notification_delivery")},
            candidate_items=[{"entity_id": "candidate-state", "version": "v1"}],
            fixture_override=True, now=NOW,
        )
        reservation_id = result["reservations"][0]
        changed = update_fixture_reservation_state(
            self.db, reservation_id=reservation_id, reservation_state="CONSUMED",
            fixture_override=True, now=NOW,
        )
        self.assertTrue(changed["ok"])
        self.assertEqual(changed["reservation_state"], "CONSUMED")
        self.assertEqual(select_due_followup_items(self.db, campaign_id=self.campaign_id, now=NOW), [])
        with self.assertRaises(ValueError):
            FixtureSchedulerExecutorRegistry({"SEND_EMAIL": lambda _payload: {"ok": True}})

    def test_run_checkpoints_completed_jobs_and_clock_rollback_is_idempotent(self):
        self.enable_fixture_window()
        calls = []
        executors = {"PIPELINE_READINESS_PREVIEW": lambda payload: calls.append(payload) or {"ok": True, "category": "PREVIEW_READY"}}
        spec = {"action_type": "PIPELINE_READINESS_PREVIEW", "entity_type": "campaign", "entity_id": str(self.campaign_id), "version": "v1", "priority": "FOLLOWUP_FIRST"}
        first = run_fixture_scheduled_window(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            now=NOW, fixture_override=True, executors=executors, job_specs=[spec],
        )
        self.assertTrue(first["ok"])
        self.assertEqual(first["state"], "COMPLETED")
        self.assertEqual(len(calls), 1)
        inspected = inspect_scheduler_run(self.db, run_id=first["run_id"])
        self.assertEqual(inspected["last_completed_checkpoint"], "RUN_FINALIZED")
        self.assertGreaterEqual(len(inspected["checkpoints"]), 4)
        rolled_back = run_fixture_scheduled_window(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            now=NOW - timedelta(hours=4), fixture_override=True, executors=executors, job_specs=[spec],
        )
        self.assertTrue(rolled_back["ok"])
        self.assertTrue(rolled_back["reused"])
        self.assertEqual(len(calls), 1)

    def test_unknown_outcome_requires_recovery_and_is_not_repeated(self):
        self.enable_fixture_window()
        calls = []
        def unknown(_payload):
            calls.append(1)
            raise FixtureSchedulerUnknownOutcome("provider state unavailable")
        spec = {"action_type": "FOLLOWUP_DRAFT_REQUEST", "entity_id": "seq-unknown", "version": "v1"}
        first = run_fixture_scheduled_window(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC", now=NOW,
            fixture_override=True, executors={"FOLLOWUP_DRAFT_REQUEST": unknown}, job_specs=[spec],
        )
        self.assertFalse(first["ok"])
        self.assertEqual(first["state"], "RECOVERY_REQUIRED")
        resumed = resume_fixture_interrupted_run(
            self.db, run_id=first["run_id"], now=NOW + timedelta(minutes=1), fixture_override=True,
            executors={"FOLLOWUP_DRAFT_REQUEST": unknown},
        )
        self.assertFalse(resumed["ok"])
        self.assertIn("UNKNOWN_OUTCOME_REQUIRES_OPERATOR_RECONCILIATION", resumed["blocking_reasons"])
        self.assertEqual(len(calls), 1)

    def test_retryable_failure_has_two_attempts_and_second_success_resumes(self):
        self.enable_fixture_window()
        attempts = []
        def flaky(_payload):
            attempts.append(1)
            if len(attempts) == 1:
                raise FixtureSchedulerRetryableError("fixture transient")
            return {"ok": True, "category": "PREVIEW_READY"}
        spec = {"action_type": "CANDIDATE_PROCESSING_PREVIEW", "entity_id": "candidate-retry", "version": "v1"}
        first = run_fixture_scheduled_window(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC", now=NOW,
            fixture_override=True, executors={"CANDIDATE_PROCESSING_PREVIEW": flaky}, job_specs=[spec],
        )
        self.assertFalse(first["ok"])
        self.assertEqual(first["state"], "ERROR_RETRYABLE")
        resumed = resume_fixture_interrupted_run(
            self.db, run_id=first["run_id"], now=NOW + timedelta(minutes=1), fixture_override=True,
            executors={"CANDIDATE_PROCESSING_PREVIEW": flaky},
        )
        self.assertTrue(resumed["ok"])
        self.assertEqual(resumed["state"], "COMPLETED")
        self.assertEqual(len(attempts), 2)

    def test_stale_lease_and_missed_windows_require_inspection_without_catchup(self):
        self.enable_fixture_window()
        local_day = "2026-11-01"
        old = NOW - timedelta(hours=2)
        with self.db.connection:
            self.db.connection.execute(
                "INSERT INTO scheduler_leases(lease_key,campaign_id,local_date,owner_run_id,acquired_at_utc,expires_at_utc,heartbeat_at_utc,state,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (f"campaign:{self.campaign_id}:{local_day}", self.campaign_id, local_day, "old-run", old.isoformat(), (NOW - timedelta(minutes=1)).isoformat(), old.isoformat(), "ACTIVE", old.isoformat()),
            )
        missed = detect_missed_windows(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC", scheduled_run_time="09:00",
            offline_since=NOW - timedelta(days=3), now=NOW, fixture_override=True,
        )
        self.assertFalse(missed["auto_process"])
        self.assertFalse(missed["catchup_enabled"])
        self.assertGreaterEqual(len(missed["missed_windows"]), 1)
        recovery = inspect_interrupted_runs(self.db, now=NOW)
        self.assertEqual(recovery["recommendation"], "OPERATOR_INSPECTION_REQUIRED")

    def test_weekly_review_is_database_only_and_idempotent(self):
        self.enable_fixture_window()
        first = generate_fixture_weekly_review(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            week_start_local=date(2026, 10, 26), fixture_override=True, now=NOW,
        )
        second = generate_fixture_weekly_review(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC",
            week_start_local=date(2026, 10, 26), fixture_override=True, now=NOW,
        )
        self.assertTrue(first["ok"])
        self.assertTrue(second["reused"])
        self.assertEqual(first["snapshot"]["metrics"]["candidates_discovered"], 0)
        self.assertEqual(first["snapshot"]["conclusions"], [])
        self.assertEqual(first["snapshot"]["revenue"], "UNAVAILABLE_WITHOUT_DATABASE_FACTS")

    def test_concurrent_attempt_and_stale_lease_are_rejected(self):
        self.enable_fixture_window()
        def retry(_payload):
            raise FixtureSchedulerRetryableError("hold active lease")
        spec = {"action_type": "PIPELINE_READINESS_PREVIEW", "entity_id": "concurrent", "version": "v1"}
        first = run_fixture_scheduled_window(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC", now=NOW,
            fixture_override=True, executors={"PIPELINE_READINESS_PREVIEW": retry}, job_specs=[spec],
        )
        second = run_fixture_scheduled_window(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC", now=NOW,
            fixture_override=True, executors={}, job_specs=[spec],
        )
        self.assertFalse(second["ok"])
        self.assertIn("RUN_ALREADY_EXISTS_REQUIRES_RESUME", second["blocking_reasons"])
        self.assertIsNotNone(first["run_id"])

        stale_db = Database(Path(self.temp_dir.name) / "stale.sqlite3")
        self.addCleanup(stale_db.close)
        migrate_step14(stale_db)
        stale_campaign = stale_db.connection.execute("SELECT id FROM campaigns ORDER BY id LIMIT 1").fetchone()[0]
        stale_db.set_config("scheduler_enabled", 1)
        stale_db.set_config("system_state", "ACTIVE")
        for key in ("daily_message_cap", "daily_candidate_processing_cap", "drafting_daily_cap", "gmail_draft_daily_cap", "followup_daily_cap", "notification_daily_cap"):
            stale_db.set_config(key, 1)
        with stale_db.connection:
            stale_db.connection.execute("UPDATE campaigns SET status='ACTIVE',campaign_timezone='UTC' WHERE id=?", (stale_campaign,))
            stale_db.connection.execute(
                "INSERT INTO scheduler_leases(lease_key,campaign_id,local_date,owner_run_id,acquired_at_utc,expires_at_utc,heartbeat_at_utc,state,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (f"campaign:{stale_campaign}:2026-11-01", stale_campaign, "2026-11-01", "old", NOW.isoformat(), (NOW - timedelta(minutes=1)).isoformat(), NOW.isoformat(), "ACTIVE", NOW.isoformat()),
            )
        stale_result = run_fixture_scheduled_window(
            stale_db, campaign_id=stale_campaign, campaign_timezone="UTC", now=NOW,
            fixture_override=True, executors={}, job_specs=[],
        )
        self.assertFalse(stale_result["ok"])
        self.assertIn("STALE_LEASE_RECOVERY_REQUIRED", stale_result["blocking_reasons"])

    def test_non_retryable_failure_and_sent_result_are_terminal(self):
        self.enable_fixture_window()
        calls = []
        def bad(_payload):
            calls.append(1)
            raise ValueError("validation failed")
        spec = {"action_type": "DRAFTING_READINESS_PREVIEW", "entity_id": "draft-bad", "version": "v1"}
        first = run_fixture_scheduled_window(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC", now=NOW,
            fixture_override=True, executors={"DRAFTING_READINESS_PREVIEW": bad}, job_specs=[spec],
        )
        self.assertFalse(first["ok"])
        self.assertEqual(first["state"], "ERROR_TERMINAL")
        self.assertEqual(len(calls), 1)

        sent = run_fixture_scheduled_window(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC", now=NOW + timedelta(days=1),
            fixture_override=True, executors={"GMAIL_DRAFT_READINESS_PREVIEW": lambda _payload: {"ok": True, "category": "SENT"}},
            job_specs=[{"action_type": "GMAIL_DRAFT_READINESS_PREVIEW", "entity_id": "draft-sent", "version": "v1"}],
        )
        self.assertFalse(sent["ok"])
        self.assertIn("SCHEDULER_CANNOT_CREATE_SENT", sent["blocking_reasons"])

    def test_missed_windows_do_not_create_backfill_runs_and_startup_reports_recovery(self):
        self.enable_fixture_window()
        before = self.db.connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0]
        result = verify_startup_recovery_readiness(
            self.db, campaign_id=self.campaign_id, campaign_timezone="UTC", scheduled_run_time="09:00",
            offline_since=NOW - timedelta(days=5), now=NOW, fixture_override=True,
        )
        after = self.db.connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0]
        self.assertGreater(len(result["missed"]["missed_windows"]), 1)
        self.assertEqual(result["missed"]["auto_process"], False)
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
