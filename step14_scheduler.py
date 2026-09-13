"""Step 14: fixture-only daily scheduler and restart recovery.

This module persists deterministic scheduling decisions and recovery state only.
It never installs a scheduler, starts a loop, sleeps, contacts a provider, or
creates a real prospect, draft, send, or reply. All mutations require an
isolated fixture database and ``fixture_override=True``.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from notifier import migrate_notifier
from step12_followup import calculate_fixture_due_date

SCHEDULER_MIGRATION_VERSION = 16
SCHEDULER_POLICY_VERSION = "TEST_SCHEDULER_V0_1"
SCHEDULER_MODE = "FIXTURE_ONLY"
SCHEDULER_TRIGGER_MODE = "MANUAL_FIXTURE_ONLY"
SCHEDULER_INSTALL_STATE = "NOT_INSTALLED"
SCHEDULER_MAX_ATTEMPTS = 2
SCHEDULER_MAX_CONCURRENT_RUNS = 1
SCHEDULER_TIMEZONE_POLICY = "CAMPAIGN_LOCAL"
SCHEDULER_STATES = {
    "PLANNED", "READINESS_CHECK", "CAPACITY_PLANNED", "JOBS_QUEUED",
    "EXECUTING", "CHECKPOINTED", "COMPLETED", "BLOCKED", "PAUSED",
    "HOLD_FOR_REVIEW", "RECOVERY_REQUIRED", "MISSED_WINDOW",
    "ERROR_RETRYABLE", "ERROR_TERMINAL", "CANCELLED",
}
JOB_STATES = {
    "PLANNED", "QUEUED", "EXECUTING", "COMPLETED", "HELD",
    "BLOCKED", "RECOVERY_REQUIRED", "ERROR_RETRYABLE", "ERROR_TERMINAL",
    "CANCELLED",
}
RESERVATION_STATES = {"RESERVED", "RELEASED", "EXPIRED", "CONSUMED", "INVALIDATED"}
CHECKPOINTS = (
    "READINESS_COMPLETED",
    "DUE_FOLLOWUPS_SELECTED",
    "CAPACITY_PLANNED",
    "FOLLOWUP_JOBS_QUEUED",
    "RESUMABLE_WORK_QUEUED",
    "NEW_WORK_JOBS_QUEUED",
    "JOB_STARTED",
    "JOB_COMPLETED_OR_HELD",
    "WEEKLY_SUMMARY_PREPARED",
    "RUN_FINALIZED",
)
EXECUTOR_ACTIONS = frozenset({
    "PIPELINE_READINESS_PREVIEW",
    "DUE_FOLLOWUP_SELECTION",
    "FOLLOWUP_DRAFT_REQUEST",
    "CANDIDATE_PROCESSING_PREVIEW",
    "DRAFTING_READINESS_PREVIEW",
    "GMAIL_DRAFT_READINESS_PREVIEW",
    "NOTIFICATION_CARD_CONSTRUCTION",
    "WEEKLY_SUMMARY_CONSTRUCTION",
})
CAPACITY_TYPES = (
    "candidate_processing",
    "drafting",
    "gmail_draft_creation",
    "followup",
    "message_touch",
    "notification_delivery",
)


class SchedulerBlockedError(RuntimeError):
    """Raised when a scheduler mutation is unsafe or canonical."""


class SchedulerValidationError(ValueError):
    """Raised for malformed scheduler fixture input."""


class FixtureSchedulerUnknownOutcome(RuntimeError):
    """An injected executor reports an externally ambiguous outcome."""


class FixtureSchedulerRetryableError(RuntimeError):
    """An injected deterministic executor reports a retryable failure."""


_SCHEMA = """
CREATE TABLE IF NOT EXISTS scheduler_runs (
    id INTEGER PRIMARY KEY,
    schedule_key TEXT NOT NULL UNIQUE,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    local_date TEXT NOT NULL,
    campaign_timezone TEXT NOT NULL,
    scheduled_at_utc TEXT NOT NULL,
    state TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    owner_run_id TEXT NOT NULL,
    lease_id INTEGER,
    current_checkpoint TEXT,
    last_completed_checkpoint TEXT,
    last_error_category TEXT,
    human_review_required INTEGER NOT NULL DEFAULT 0 CHECK (human_review_required IN (0,1)),
    safe_summary_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finalized_at TEXT,
    UNIQUE (campaign_id, local_date)
);
CREATE INDEX IF NOT EXISTS scheduler_runs_state_idx ON scheduler_runs(state, updated_at);
CREATE INDEX IF NOT EXISTS scheduler_runs_campaign_day_idx ON scheduler_runs(campaign_id, local_date);

CREATE TABLE IF NOT EXISTS scheduler_jobs (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES scheduler_runs(id) ON DELETE CASCADE,
    action_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_version TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    priority TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 2 CHECK (max_attempts BETWEEN 1 AND 2),
    result_category TEXT,
    safe_result_json TEXT NOT NULL DEFAULT '{}',
    safe_error TEXT,
    human_review_required INTEGER NOT NULL DEFAULT 0 CHECK (human_review_required IN (0,1)),
    started_at TEXT,
    completed_at TEXT,
    last_checkpoint TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (run_id, action_type, entity_type, entity_id, entity_version, input_fingerprint)
);
CREATE INDEX IF NOT EXISTS scheduler_jobs_run_state_idx ON scheduler_jobs(run_id, state, priority);

CREATE TABLE IF NOT EXISTS scheduler_checkpoints (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES scheduler_runs(id) ON DELETE CASCADE,
    checkpoint_name TEXT NOT NULL,
    checkpoint_sequence INTEGER NOT NULL,
    state TEXT NOT NULL,
    safe_detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE (run_id, checkpoint_sequence)
);
CREATE INDEX IF NOT EXISTS scheduler_checkpoints_run_idx ON scheduler_checkpoints(run_id, checkpoint_sequence);

CREATE TABLE IF NOT EXISTS scheduler_leases (
    id INTEGER PRIMARY KEY,
    lease_key TEXT NOT NULL,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    local_date TEXT NOT NULL,
    owner_run_id TEXT NOT NULL,
    acquired_at_utc TEXT NOT NULL,
    expires_at_utc TEXT NOT NULL,
    heartbeat_at_utc TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ACTIVE','RELEASED','STALE','RECOVERED')),
    recovery_reason TEXT,
    released_at_utc TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (campaign_id, local_date, state)
);
CREATE INDEX IF NOT EXISTS scheduler_leases_expiry_idx ON scheduler_leases(state, expires_at_utc);

CREATE TABLE IF NOT EXISTS scheduler_capacity_reservations (
    id INTEGER PRIMARY KEY,
    reservation_fingerprint TEXT NOT NULL UNIQUE,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    local_date TEXT NOT NULL,
    action_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_version TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    capacity_claim_json TEXT NOT NULL,
    reservation_state TEXT NOT NULL,
    reserved_at_utc TEXT NOT NULL,
    expires_at_utc TEXT,
    invalidated_at_utc TEXT,
    invalidation_reason TEXT,
    consumed_at_utc TEXT,
    released_at_utc TEXT,
    UNIQUE (campaign_id, local_date, action_type, entity_type, entity_id, entity_version, input_fingerprint)
);
CREATE INDEX IF NOT EXISTS scheduler_capacity_day_idx ON scheduler_capacity_reservations(campaign_id, local_date, reservation_state);

CREATE TABLE IF NOT EXISTS scheduler_recovery_events (
    id INTEGER PRIMARY KEY,
    run_id INTEGER REFERENCES scheduler_runs(id) ON DELETE SET NULL,
    lease_id INTEGER REFERENCES scheduler_leases(id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    safe_detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS scheduler_recovery_events_run_idx ON scheduler_recovery_events(run_id, created_at);

CREATE TABLE IF NOT EXISTS scheduler_weekly_reviews (
    id INTEGER PRIMARY KEY,
    review_key TEXT NOT NULL UNIQUE,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    week_start_local TEXT NOT NULL,
    campaign_timezone TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS scheduler_weekly_reviews_campaign_idx ON scheduler_weekly_reviews(campaign_id, week_start_local);
"""


def _now(value: Optional[datetime] = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: Optional[datetime] = None) -> str:
    return _now(value).isoformat()


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _now(value)
    if not isinstance(value, str):
        raise SchedulerValidationError("timestamp is required")
    try:
        return _now(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError as error:
        raise SchedulerValidationError("timestamp is invalid") from error


def _is_durable(database: Database) -> bool:
    try:
        return database.path.resolve() == DEFAULT_DB_PATH.resolve()
    except (AttributeError, OSError):
        return False


def _require_fixture(database: Database, fixture_override: bool) -> None:
    if _is_durable(database):
        raise SchedulerBlockedError("canonical scheduler operation is blocked")
    if not fixture_override:
        raise SchedulerBlockedError("fixture_override=True is required for Step 14")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return sha256(_json(value).encode("utf-8")).hexdigest()


def _safe_text(value: Any, limit: int = 300) -> str:
    text = " ".join(str(value or "").split())
    text = text.replace("bot", "[REDACTED]") if ":" in text else text
    return text[:limit]


def _safe_detail(result: Any) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        return {"category": "EXECUTOR_RESULT_INVALID", "ok": False, "human_review_required": True}
    category = _safe_text(result.get("category") or result.get("result_category") or "EXECUTOR_COMPLETED", 80)
    safe = {
        "category": category,
        "ok": bool(result.get("ok", True)),
        "human_review_required": bool(result.get("human_review_required", False)),
    }
    if result.get("safe_error"):
        safe["safe_error"] = _safe_text(result["safe_error"])
    if isinstance(result.get("blocking_reasons"), (list, tuple)):
        safe["blocking_reasons"] = sorted({_safe_text(item, 100) for item in result["blocking_reasons"]})
    if result.get("checkpoint"):
        safe["checkpoint"] = _safe_text(result["checkpoint"], 100)
    return safe


def _valid_timezone(value: Any) -> tuple[Optional[ZoneInfo], Optional[str]]:
    if not isinstance(value, str) or not value.strip():
        return None, "CAMPAIGN_TIMEZONE_REQUIRED"
    try:
        return ZoneInfo(value.strip()), None
    except ZoneInfoNotFoundError:
        return None, "CAMPAIGN_TIMEZONE_INVALID"


def calculate_campaign_local_date(at: datetime, campaign_timezone: str) -> date:
    zone, reason = _valid_timezone(campaign_timezone)
    if reason or zone is None:
        raise SchedulerValidationError(reason or "campaign timezone is invalid")
    return _now(at).astimezone(zone).date()


def calculate_scheduler_due_date(sent_at: datetime | str, cadence_strategy: str, interval_value: int, campaign_timezone: str, holiday_calendar: Iterable[str] = ()) -> datetime:
    """Reuse Step 12's explicit business-day calculator; no holiday API is used."""
    return calculate_fixture_due_date(sent_at, cadence_strategy, interval_value, campaign_timezone, holiday_calendar)


def _campaign(database: Database, campaign_id: int):
    row = database.connection.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    return row


def _campaign_timezone(database: Database, campaign_id: int, supplied: Optional[str]) -> Optional[str]:
    if supplied is not None:
        return supplied
    row = _campaign(database, campaign_id)
    if row is None:
        return None
    try:
        return row["campaign_timezone"]
    except (IndexError, KeyError):
        return None


def _config_reasons(database: Database, fixture_override: bool) -> list[str]:
    config = database.read_config()
    reasons: list[str] = []
    if not fixture_override:
        reasons.append("FIXTURE_OVERRIDE_REQUIRED")
    if config.get("scheduler_enabled") != 1:
        reasons.append("SCHEDULER_DISABLED")
    if config.get("scheduler_mode") != SCHEDULER_MODE:
        reasons.append("SCHEDULER_MODE_UNSAFE")
    if config.get("scheduler_trigger_mode") != SCHEDULER_TRIGGER_MODE:
        reasons.append("SCHEDULER_TRIGGER_MODE_UNSAFE")
    if config.get("scheduler_install_state") != SCHEDULER_INSTALL_STATE:
        reasons.append("SCHEDULER_INSTALL_STATE_UNSAFE")
    if config.get("scheduler_max_concurrent_runs") != SCHEDULER_MAX_CONCURRENT_RUNS:
        reasons.append("CONCURRENCY_POLICY_UNSAFE")
    if config.get("scheduler_max_attempts") != SCHEDULER_MAX_ATTEMPTS:
        reasons.append("ATTEMPT_POLICY_UNSAFE")
    if config.get("scheduler_catchup_enabled") != 0:
        reasons.append("CATCHUP_MUST_REMAIN_DISABLED")
    if config.get("scheduler_timezone_policy") != SCHEDULER_TIMEZONE_POLICY:
        reasons.append("TIMEZONE_POLICY_UNSAFE")
    if config.get("system_state") != "ACTIVE":
        reasons.append("SYSTEM_PAUSED")
    capacity_keys = (
        "daily_candidate_processing_cap",
        "daily_drafting_cap",
        "gmail_draft_daily_cap",
        "followup_daily_cap",
        "daily_message_cap",
        "notification_daily_cap",
    )
    if not any(int(config.get(key, 0) or 0) > 0 for key in capacity_keys):
        reasons.append("ALL_CAPACITIES_ZERO")
    return reasons


def migrate_step14(database_or_path: Database | str | Path) -> int:
    """Apply the additive Step 14 schema and safe scheduler defaults."""
    database = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    migrate_notifier(database)
    with database.connection:
        database.connection.executescript(_SCHEMA)
        columns = {row[1] for row in database.connection.execute("PRAGMA table_info(campaigns)")}
        if "campaign_timezone" not in columns:
            database.connection.execute("ALTER TABLE campaigns ADD COLUMN campaign_timezone TEXT")
        database.connection.executemany(
            "INSERT OR IGNORE INTO system_config(key,value,value_type) VALUES (?,?,?)",
            (
                ("scheduler_enabled", "0", "integer"),
                ("scheduler_mode", SCHEDULER_MODE, "text"),
                ("scheduler_trigger_mode", SCHEDULER_TRIGGER_MODE, "text"),
                ("scheduler_install_state", SCHEDULER_INSTALL_STATE, "text"),
                ("scheduler_max_concurrent_runs", "1", "integer"),
                ("scheduler_max_attempts", "2", "integer"),
                ("scheduler_catchup_enabled", "0", "integer"),
                ("scheduler_timezone_policy", SCHEDULER_TIMEZONE_POLICY, "text"),
                ("scheduler_daily_run_time", "UNDECIDED", "text"),
                ("scheduler_weekly_review_schedule", "UNDECIDED", "text"),
            ),
        )
        database.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version,name,applied_at) VALUES (?,?,?)",
            (SCHEDULER_MIGRATION_VERSION, "step14_fixture_scheduler_restart_recovery", _iso()),
        )
    return SCHEDULER_MIGRATION_VERSION


def inspect_scheduler_readiness(database: Database, *, campaign_id: int, campaign_timezone: Optional[str] = None, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Return safe readiness facts without creating scheduler state."""
    migrate_step14(database)
    current = _now(now)
    if _is_durable(database):
        return {"ready": False, "campaign_id": campaign_id, "blocking_reasons": ["CANONICAL_SCHEDULER_OPERATION_BLOCKED"]}
    reasons = _config_reasons(database, fixture_override)
    campaign = _campaign(database, campaign_id)
    if campaign is None:
        reasons.append("CAMPAIGN_NOT_FOUND")
        return {"ready": False, "campaign_id": campaign_id, "blocking_reasons": sorted(set(reasons))}
    if campaign["status"] != "ACTIVE":
        reasons.append("CAMPAIGN_INACTIVE")
    timezone_name = _campaign_timezone(database, campaign_id, campaign_timezone)
    _, timezone_reason = _valid_timezone(timezone_name)
    if timezone_reason:
        reasons.append(timezone_reason)
    return {
        "ready": not reasons,
        "campaign_id": campaign_id,
        "campaign_timezone": timezone_name,
        "local_date": calculate_campaign_local_date(current, timezone_name).isoformat() if not timezone_reason else None,
        "policy_version": SCHEDULER_POLICY_VERSION,
        "blocking_reasons": sorted(set(reasons)),
    }


def _claim_total(database: Database, campaign_id: int, local_date: str, state: str = "RESERVED") -> dict[str, int]:
    totals = {key: 0 for key in CAPACITY_TYPES}
    rows = database.connection.execute(
        "SELECT capacity_claim_json FROM scheduler_capacity_reservations WHERE campaign_id=? AND local_date=? AND reservation_state=?",
        (campaign_id, local_date, state),
    ).fetchall()
    for row in rows:
        try:
            claim = json.loads(row["capacity_claim_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        for key in CAPACITY_TYPES:
            totals[key] += int(claim.get(key, 0) or 0)
    return totals


def _reservation_fingerprint(campaign_id: int, local_date: str, action_type: str, item: Mapping[str, Any]) -> tuple[str, str, str, str]:
    entity_type = str(item.get("entity_type") or ("sequence" if action_type == "FOLLOWUP" else "candidate"))
    entity_id = str(item.get("entity_id") or "")
    entity_version = str(item.get("version") or item.get("entity_version") or "v1")
    input_fingerprint = str(item.get("input_fingerprint") or _hash({k: item.get(k) for k in ("entity_type", "entity_id", "version", "state", "policy_version")}))
    return _hash({"campaign_id": campaign_id, "local_date": local_date, "action_type": action_type, "entity_type": entity_type, "entity_id": entity_id, "entity_version": entity_version, "policy_version": SCHEDULER_POLICY_VERSION, "input_fingerprint": input_fingerprint}), entity_type, entity_id, entity_version


def _invalidate_item_reservation(database: Database, fingerprint: str, now: datetime, reason: str) -> None:
    with database.connection:
        database.connection.execute(
            "UPDATE scheduler_capacity_reservations SET reservation_state='INVALIDATED',invalidated_at_utc=?,invalidation_reason=? WHERE reservation_fingerprint=? AND reservation_state='RESERVED'",
            (_iso(now), _safe_text(reason), fingerprint),
        )


def _can_claim(used: Mapping[str, int], claim: Mapping[str, int], caps: Mapping[str, int]) -> bool:
    return all(int(caps.get(key, 0)) > int(used.get(key, 0)) + int(claim.get(key, 0)) - 0 for key in claim)


def plan_shared_daily_capacity(database: Database, *, campaign_id: int, campaign_timezone: str, local_date: date | str, caps: Mapping[str, int], followup_items: Iterable[Mapping[str, Any]] = (), candidate_items: Iterable[Mapping[str, Any]] = (), notification_items: Iterable[Mapping[str, Any]] = (), fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Plan deterministic shared capacity, reserving due follow-ups first."""
    _require_fixture(database, fixture_override)
    migrate_step14(database)
    current = _now(now)
    zone, reason = _valid_timezone(campaign_timezone)
    if reason or zone is None:
        return {"ok": False, "blocking_reasons": [reason], "followup_reserved": 0, "new_work_reserved": 0}
    local_day = local_date.isoformat() if isinstance(local_date, date) else str(local_date)
    try:
        date.fromisoformat(local_day)
    except ValueError:
        raise SchedulerValidationError("local_date is invalid")
    normalized_caps = {key: max(0, int(caps.get(key, 0) or 0)) for key in CAPACITY_TYPES}
    used = _claim_total(database, campaign_id, local_day)
    result = {"ok": True, "priority": "FOLLOWUP_FIRST", "reused": False, "followup_reserved": 0, "new_work_reserved": 0, "notification_reserved": 0, "blocked_reasons": [], "reservations": []}

    def reserve(action_type: str, item: Mapping[str, Any], claim: Mapping[str, int], priority: str) -> bool:
        nonlocal used
        fingerprint, entity_type, entity_id, entity_version = _reservation_fingerprint(campaign_id, local_day, action_type, item)
        existing = database.connection.execute("SELECT * FROM scheduler_capacity_reservations WHERE reservation_fingerprint=?", (fingerprint,)).fetchone()
        if item.get("suppressed") or item.get("stale"):
            if existing is not None:
                _invalidate_item_reservation(database, fingerprint, current, "SUPPRESSION_OR_STALE_INPUT")
            return False
        if existing is not None:
            if existing["reservation_state"] == "RESERVED":
                result["reused"] = True
                result["reservations"].append(existing["id"])
                return True
            return False
        missing = [key for key, units in claim.items() if normalized_caps.get(key, 0) < used.get(key, 0) + int(units)]
        if missing:
            result["blocked_reasons"].append("CAP_ZERO_OR_EXHAUSTED:" + ",".join(sorted(missing)))
            return False
        input_fingerprint = str(item.get("input_fingerprint") or _hash(item))
        with database.connection:
            cursor = database.connection.execute(
                """INSERT INTO scheduler_capacity_reservations
                (reservation_fingerprint,campaign_id,local_date,action_type,entity_type,entity_id,entity_version,policy_version,input_fingerprint,capacity_claim_json,reservation_state,reserved_at_utc)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (fingerprint, campaign_id, local_day, action_type, entity_type, entity_id, entity_version, SCHEDULER_POLICY_VERSION, input_fingerprint, _json(claim), "RESERVED", _iso(current)),
            )
        result["reservations"].append(cursor.lastrowid)
        for key, units in claim.items():
            used[key] = used.get(key, 0) + int(units)
        return True

    for item in followup_items:
        if reserve("FOLLOWUP", item, {"followup": 1, "message_touch": 1}, "FOLLOWUP_FIRST"):
            result["followup_reserved"] += 1
    for item in candidate_items:
        if reserve("NEW_OUTREACH", item, {"candidate_processing": 1, "drafting": 1, "gmail_draft_creation": 1, "message_touch": 1}, "NEW_WORK"):
            result["new_work_reserved"] += 1
    for item in notification_items:
        if reserve("NOTIFICATION_NOTIFICATION", item, {"notification_delivery": 1}, "NOTIFICATION"):
            result["notification_reserved"] += 1
    if result["blocked_reasons"]:
        result["ok"] = False
    result["used"] = used
    result["remaining"] = {key: max(0, normalized_caps[key] - used.get(key, 0)) for key in CAPACITY_TYPES}
    return result


def preview_fixture_campaign_day(database: Database, *, campaign_id: int, campaign_timezone: str, local_date: date | str, caps: Mapping[str, int], followup_items: Iterable[Mapping[str, Any]] = (), candidate_items: Iterable[Mapping[str, Any]] = (), fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Preview readiness and capacity without scheduling or executor calls."""
    readiness = inspect_scheduler_readiness(database, campaign_id=campaign_id, campaign_timezone=campaign_timezone, fixture_override=fixture_override, now=now)
    if not readiness["ready"]:
        return {"ready": False, "readiness": readiness, "capacity": None}
    return {"ready": True, "readiness": readiness, "capacity": plan_shared_daily_capacity(database, campaign_id=campaign_id, campaign_timezone=campaign_timezone, local_date=local_date, caps=caps, followup_items=followup_items, candidate_items=candidate_items, fixture_override=fixture_override, now=now)}


def select_due_followup_items(database: Database, *, campaign_id: int, now: Optional[datetime] = None) -> list[dict[str, Any]]:
    """Select due, non-terminal, non-suppressed Step 12 actions without mutating them."""
    migrate_step14(database)
    current = _iso(now)
    rows = database.connection.execute(
        """SELECT a.id AS action_id, a.sequence_id, a.touch_number, a.request_fingerprint,
                  a.source_input_fingerprint, s.input_fingerprint, s.source_draft_version
           FROM followup_actions a
           JOIN followup_sequences s ON s.id=a.sequence_id
           WHERE s.campaign_id=? AND a.state='DUE' AND a.due_at<=?
             AND s.state NOT IN ('SUPPRESSED','BOUNCE_HOLD','CLOSED_POSITIVE','CLOSED_NOT_INTERESTED','CLOSED_NO_RESPONSE','HUMAN_ACTION_REQUIRED','HOLD_FOR_REVIEW','ERROR')
             AND NOT EXISTS (SELECT 1 FROM suppressions x WHERE x.lead_id=s.lead_id)
             AND NOT EXISTS (SELECT 1 FROM followup_capacity_reservations r WHERE r.action_id=a.id AND r.reservation_state='RESERVED')
           ORDER BY a.due_at,a.id""",
        (campaign_id, current),
    ).fetchall()
    return [
        {
            "entity_type": "followup_action",
            "entity_id": str(row["action_id"]),
            "entity_version": str(row["source_draft_version"]),
            "sequence_id": row["sequence_id"],
            "touch_number": row["touch_number"],
            "request_fingerprint": row["request_fingerprint"],
            "source_input_fingerprint": row["source_input_fingerprint"],
            "input_fingerprint": row["input_fingerprint"],
        }
        for row in rows
    ]


def update_fixture_reservation_state(database: Database, *, reservation_id: int, reservation_state: str, reason: str = "", fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Make release, expiry, consumption, or invalidation auditable."""
    _require_fixture(database, fixture_override)
    migrate_step14(database)
    if reservation_state not in RESERVATION_STATES:
        raise SchedulerValidationError("reservation state is invalid")
    current = _iso(now)
    row = database.connection.execute("SELECT id,reservation_state FROM scheduler_capacity_reservations WHERE id=?", (reservation_id,)).fetchone()
    if row is None:
        return {"ok": False, "blocking_reasons": ["RESERVATION_NOT_FOUND"]}
    if row["reservation_state"] != "RESERVED" and reservation_state == "RESERVED":
        return {"ok": False, "blocking_reasons": ["RESERVATION_STATE_CANNOT_REOPEN"]}
    with database.connection:
        database.connection.execute(
            """UPDATE scheduler_capacity_reservations
               SET reservation_state=?, invalidated_at_utc=CASE WHEN ?='INVALIDATED' THEN ? ELSE invalidated_at_utc END,
                   invalidation_reason=CASE WHEN ?='INVALIDATED' THEN ? ELSE invalidation_reason END,
                   consumed_at_utc=CASE WHEN ?='CONSUMED' THEN ? ELSE consumed_at_utc END,
                   released_at_utc=CASE WHEN ? IN ('RELEASED','EXPIRED') THEN ? ELSE released_at_utc END
               WHERE id=?""",
            (reservation_state, reservation_state, current, reservation_state, _safe_text(reason or reservation_state), reservation_state, current, reservation_state, current, reservation_id),
        )
    return {"ok": True, "reservation_id": reservation_id, "reservation_state": reservation_state}


class FixtureSchedulerExecutorRegistry:
    """Allowlisted registry for explicitly injected local fixture executors."""

    def __init__(self, executors: Optional[Mapping[str, Callable[[dict[str, Any]], Mapping[str, Any]]]] = None):
        self._executors: dict[str, Callable[[dict[str, Any]], Mapping[str, Any]]] = {}
        for action, executor in (executors or {}).items():
            self.register(action, executor)

    def register(self, action_type: str, executor: Callable[[dict[str, Any]], Mapping[str, Any]]) -> None:
        if action_type not in EXECUTOR_ACTIONS or not callable(executor):
            raise SchedulerValidationError("executor action is not allowlisted")
        self._executors[action_type] = executor

    def invoke(self, action_type: str, payload: dict[str, Any]) -> Mapping[str, Any]:
        executor = self._executors.get(action_type)
        if executor is None:
            return {"ok": True, "category": "NO_EXECUTOR_REGISTERED", "human_review_required": True}
        return executor(dict(payload))


def _lease_key(campaign_id: int, local_day: str) -> str:
    return f"campaign:{campaign_id}:{local_day}"


def _checkpoint(database: Database, run_id: int, name: str, state: str, now: datetime, detail: Optional[Mapping[str, Any]] = None) -> int:
    next_sequence = database.connection.execute("SELECT COALESCE(MAX(checkpoint_sequence),0)+1 FROM scheduler_checkpoints WHERE run_id=?", (run_id,)).fetchone()[0]
    with database.connection:
        database.connection.execute("INSERT INTO scheduler_checkpoints(run_id,checkpoint_name,checkpoint_sequence,state,safe_detail_json,created_at) VALUES (?,?,?,?,?,?)", (run_id, name, next_sequence, state, _json(detail or {}), _iso(now)))
        database.connection.execute("UPDATE scheduler_runs SET current_checkpoint=?,last_completed_checkpoint=?,updated_at=? WHERE id=?", (name, name, _iso(now), run_id))
    return int(next_sequence)


def _acquire_lease(database: Database, campaign_id: int, local_day: str, owner_run_id: str, now: datetime, ttl_seconds: int = 900) -> tuple[Optional[int], Optional[str]]:
    key = _lease_key(campaign_id, local_day)
    existing = database.connection.execute("SELECT * FROM scheduler_leases WHERE lease_key=? AND state='ACTIVE'", (key,)).fetchone()
    if existing is not None:
        if _parse_time(existing["expires_at_utc"]) > now:
            return None, "CONCURRENT_RUN_ACTIVE"
        with database.connection:
            database.connection.execute("UPDATE scheduler_leases SET state='STALE',recovery_reason=?,released_at_utc=? WHERE id=?", ("LEASE_EXPIRED_REQUIRES_INSPECTION", _iso(now), existing["id"]))
            database.connection.execute("INSERT INTO scheduler_recovery_events(lease_id,event_type,outcome,safe_detail_json,created_at) VALUES (?,?,?,?,?)", (existing["id"], "STALE_LEASE_DETECTED", "RECOVERY_REQUIRED", _json({"lease_key": key}), _iso(now)))
        return None, "STALE_LEASE_RECOVERY_REQUIRED"
    expires = now + timedelta(seconds=ttl_seconds)
    with database.connection:
        cursor = database.connection.execute("INSERT INTO scheduler_leases(lease_key,campaign_id,local_date,owner_run_id,acquired_at_utc,expires_at_utc,heartbeat_at_utc,state,created_at) VALUES (?,?,?,?,?,?,?,?,?)", (key, campaign_id, local_day, owner_run_id, _iso(now), _iso(expires), _iso(now), "ACTIVE", _iso(now)))
    return int(cursor.lastrowid), None


def _release_lease(database: Database, lease_id: int, now: datetime, state: str = "RELEASED") -> None:
    with database.connection:
        database.connection.execute("UPDATE scheduler_leases SET state=?,released_at_utc=?,heartbeat_at_utc=? WHERE id=?", (state, _iso(now), _iso(now), lease_id))


def _job_spec(spec: Mapping[str, Any]) -> dict[str, str]:
    action = str(spec.get("action_type") or "")
    if action not in EXECUTOR_ACTIONS:
        raise SchedulerValidationError("job action is not allowlisted")
    return {
        "action_type": action,
        "entity_type": str(spec.get("entity_type") or "scheduler"),
        "entity_id": str(spec.get("entity_id") or "0"),
        "entity_version": str(spec.get("entity_version") or spec.get("version") or "v1"),
        "input_fingerprint": str(spec.get("input_fingerprint") or _hash(spec)),
        "priority": str(spec.get("priority") or "NORMAL"),
    }


def _safe_run_result(database: Database, run_id: int, **extra: Any) -> dict[str, Any]:
    row = database.connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
    result = {"run_id": run_id, "schedule_key": row["schedule_key"], "state": row["state"], "last_completed_checkpoint": row["last_completed_checkpoint"], "human_review_required": bool(row["human_review_required"])}
    result.update(extra)
    return result


def _prepare_run(database: Database, *, campaign_id: int, campaign_timezone: str, now: datetime, fixture_override: bool) -> tuple[Optional[int], dict[str, Any]]:
    readiness = inspect_scheduler_readiness(database, campaign_id=campaign_id, campaign_timezone=campaign_timezone, fixture_override=fixture_override, now=now)
    if not readiness["ready"]:
        return None, {"ok": False, "blocking_reasons": readiness["blocking_reasons"], "readiness": readiness}
    local_day = readiness["local_date"]
    schedule_key = _lease_key(campaign_id, local_day)
    existing = database.connection.execute("SELECT * FROM scheduler_runs WHERE schedule_key=?", (schedule_key,)).fetchone()
    if existing is not None:
        if existing["state"] == "COMPLETED":
            return int(existing["id"]), {"ok": True, "reused": True}
        return int(existing["id"]), {"ok": False, "blocking_reasons": ["RUN_ALREADY_EXISTS_REQUIRES_RESUME"], "reused": True}
    owner = f"fixture-owner:{schedule_key}"
    lease_id, lease_reason = _acquire_lease(database, campaign_id, local_day, owner, now)
    if lease_reason:
        return None, {"ok": False, "blocking_reasons": [lease_reason]}
    with database.connection:
        cursor = database.connection.execute("INSERT INTO scheduler_runs(schedule_key,campaign_id,local_date,campaign_timezone,scheduled_at_utc,state,policy_version,owner_run_id,lease_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", (schedule_key, campaign_id, local_day, campaign_timezone, _iso(now), "PLANNED", SCHEDULER_POLICY_VERSION, owner, lease_id, _iso(now), _iso(now)))
    run_id = int(cursor.lastrowid)
    _checkpoint(database, run_id, "READINESS_COMPLETED", "READINESS_CHECK", now, {"campaign_id": campaign_id, "local_date": local_day})
    return run_id, {"ok": True, "reused": False}


def _create_job(database: Database, run_id: int, spec: Mapping[str, Any], now: datetime) -> int:
    safe = _job_spec(spec)
    existing = database.connection.execute("SELECT id FROM scheduler_jobs WHERE run_id=? AND action_type=? AND entity_type=? AND entity_id=? AND entity_version=? AND input_fingerprint=?", (run_id, safe["action_type"], safe["entity_type"], safe["entity_id"], safe["entity_version"], safe["input_fingerprint"])).fetchone()
    if existing is not None:
        return int(existing["id"])
    with database.connection:
        cursor = database.connection.execute("INSERT INTO scheduler_jobs(run_id,action_type,entity_type,entity_id,entity_version,input_fingerprint,priority,state,max_attempts,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", (run_id, safe["action_type"], safe["entity_type"], safe["entity_id"], safe["entity_version"], safe["input_fingerprint"], safe["priority"], "QUEUED", SCHEDULER_MAX_ATTEMPTS, _iso(now), _iso(now)))
    return int(cursor.lastrowid)


def _execute_jobs(database: Database, run_id: int, job_specs: Iterable[Mapping[str, Any]], executors: Mapping[str, Callable[[dict[str, Any]], Mapping[str, Any]]] | FixtureSchedulerExecutorRegistry, now: datetime) -> dict[str, Any]:
    registry = executors if isinstance(executors, FixtureSchedulerExecutorRegistry) else FixtureSchedulerExecutorRegistry(executors)
    jobs = [_create_job(database, run_id, spec, now) for spec in job_specs]
    job_rows = {job_id: database.connection.execute("SELECT priority FROM scheduler_jobs WHERE id=?", (job_id,)).fetchone()["priority"] for job_id in jobs}
    jobs.sort(key=lambda job_id: (0 if job_rows[job_id] == "FOLLOWUP_FIRST" else 1, job_id))
    _checkpoint(database, run_id, "FOLLOWUP_JOBS_QUEUED", "JOBS_QUEUED", now, {"job_count": len(jobs)})
    for job_id in jobs:
        job = database.connection.execute("SELECT * FROM scheduler_jobs WHERE id=?", (job_id,)).fetchone()
        if job["state"] == "COMPLETED":
            continue
        if job["state"] == "RECOVERY_REQUIRED":
            with database.connection:
                database.connection.execute("UPDATE scheduler_runs SET state='RECOVERY_REQUIRED',human_review_required=1,updated_at=? WHERE id=?", (_iso(now), run_id))
            return {"ok": False, "blocking_reasons": ["UNKNOWN_OUTCOME_REQUIRES_RECOVERY"]}
        attempt = int(job["attempt_count"]) + 1
        with database.connection:
            database.connection.execute("UPDATE scheduler_jobs SET state='EXECUTING',attempt_count=?,started_at=?,updated_at=?,last_checkpoint=? WHERE id=?", (attempt, _iso(now), _iso(now), "JOB_STARTED", job_id))
            database.connection.execute("UPDATE scheduler_runs SET state='EXECUTING',current_checkpoint=?,updated_at=? WHERE id=?", ("JOB_STARTED", _iso(now), run_id))
        try:
            output = registry.invoke(job["action_type"], {"scheduler_run_id": run_id, "scheduler_job_id": job_id, "action_type": job["action_type"], "entity_type": job["entity_type"], "entity_id": job["entity_id"], "entity_version": job["entity_version"], "input_fingerprint": job["input_fingerprint"], "attempt_number": attempt})
            safe = _safe_detail(output)
            category = safe["category"]
            if category in {"UNKNOWN_EXTERNAL_STATE", "EXTERNAL_STATE_UNKNOWN"}:
                raise FixtureSchedulerUnknownOutcome(category)
            if category == "SENT" or category == "MARKED_SENT":
                safe = {"category": "SCHEDULER_CANNOT_CREATE_SENT", "ok": False, "human_review_required": True}
                with database.connection:
                    database.connection.execute("UPDATE scheduler_jobs SET state='ERROR_TERMINAL',result_category=?,safe_result_json=?,safe_error=?,human_review_required=1,completed_at=?,updated_at=?,last_checkpoint=? WHERE id=?", (safe["category"], _json(safe), safe["category"], _iso(now), _iso(now), "JOB_COMPLETED_OR_HELD", job_id))
                    database.connection.execute("UPDATE scheduler_runs SET state='ERROR_TERMINAL',last_error_category=?,human_review_required=1,updated_at=? WHERE id=?", (safe["category"], _iso(now), run_id))
                _checkpoint(database, run_id, "JOB_COMPLETED_OR_HELD", "ERROR_TERMINAL", now, safe)
                return {"ok": False, "blocking_reasons": [safe["category"]]}
            if not safe["ok"]:
                state = "HELD" if safe["human_review_required"] else "ERROR_TERMINAL"
                with database.connection:
                    database.connection.execute("UPDATE scheduler_jobs SET state=?,result_category=?,safe_result_json=?,safe_error=?,human_review_required=?,completed_at=?,updated_at=?,last_checkpoint=? WHERE id=?", (state, category, _json(safe), safe.get("safe_error"), int(safe["human_review_required"]), _iso(now), _iso(now), "JOB_COMPLETED_OR_HELD", job_id))
                    database.connection.execute("UPDATE scheduler_runs SET state=?,human_review_required=?,updated_at=? WHERE id=?", ("HOLD_FOR_REVIEW" if state == "HELD" else "ERROR_TERMINAL", int(safe["human_review_required"]), _iso(now), run_id))
                _checkpoint(database, run_id, "JOB_COMPLETED_OR_HELD", state, now, safe)
                return {"ok": False, "blocking_reasons": [category]}
            with database.connection:
                database.connection.execute("UPDATE scheduler_jobs SET state='COMPLETED',result_category=?,safe_result_json=?,human_review_required=?,completed_at=?,updated_at=?,last_checkpoint=? WHERE id=?", (category, _json(safe), int(safe["human_review_required"]), _iso(now), _iso(now), "JOB_COMPLETED_OR_HELD", job_id))
            _checkpoint(database, run_id, "JOB_COMPLETED_OR_HELD", "COMPLETED", now, {"job_id": job_id, "category": category})
        except FixtureSchedulerUnknownOutcome as error:
            safe = {"category": "UNKNOWN_EXTERNAL_STATE", "ok": False, "human_review_required": True, "safe_error": _safe_text(error)}
            with database.connection:
                database.connection.execute("UPDATE scheduler_jobs SET state='RECOVERY_REQUIRED',result_category=?,safe_result_json=?,safe_error=?,human_review_required=1,updated_at=?,last_checkpoint=? WHERE id=?", (safe["category"], _json(safe), safe["safe_error"], _iso(now), "JOB_COMPLETED_OR_HELD", job_id))
                database.connection.execute("UPDATE scheduler_runs SET state='RECOVERY_REQUIRED',last_error_category=?,human_review_required=1,updated_at=? WHERE id=?", (safe["category"], _iso(now), run_id))
                database.connection.execute("INSERT INTO scheduler_recovery_events(run_id,event_type,outcome,safe_detail_json,created_at) VALUES (?,?,?,?,?)", (run_id, "UNKNOWN_EXTERNAL_OUTCOME", "RECOVERY_REQUIRED", _json(safe), _iso(now)))
            _checkpoint(database, run_id, "JOB_COMPLETED_OR_HELD", "RECOVERY_REQUIRED", now, safe)
            return {"ok": False, "blocking_reasons": ["UNKNOWN_EXTERNAL_STATE"]}
        except FixtureSchedulerRetryableError as error:
            safe = {"category": "ERROR_RETRYABLE", "ok": False, "human_review_required": False, "safe_error": _safe_text(error)}
            terminal = attempt >= SCHEDULER_MAX_ATTEMPTS
            state = "ERROR_TERMINAL" if terminal else "ERROR_RETRYABLE"
            with database.connection:
                database.connection.execute("UPDATE scheduler_jobs SET state=?,result_category=?,safe_result_json=?,safe_error=?,completed_at=?,updated_at=?,last_checkpoint=? WHERE id=?", (state, safe["category"], _json(safe), safe["safe_error"], _iso(now), _iso(now), "JOB_COMPLETED_OR_HELD", job_id))
                database.connection.execute("UPDATE scheduler_runs SET state=?,last_error_category=?,updated_at=? WHERE id=?", (state, state, _iso(now), run_id))
            _checkpoint(database, run_id, "JOB_COMPLETED_OR_HELD", state, now, safe)
            return {"ok": False, "blocking_reasons": [state]}
        except Exception as error:
            safe = {"category": "ERROR_TERMINAL", "ok": False, "human_review_required": True, "safe_error": _safe_text(error)}
            with database.connection:
                database.connection.execute("UPDATE scheduler_jobs SET state='ERROR_TERMINAL',result_category=?,safe_result_json=?,safe_error=?,human_review_required=1,completed_at=?,updated_at=?,last_checkpoint=? WHERE id=?", (safe["category"], _json(safe), safe["safe_error"], _iso(now), _iso(now), "JOB_COMPLETED_OR_HELD", job_id))
                database.connection.execute("UPDATE scheduler_runs SET state='ERROR_TERMINAL',last_error_category=?,human_review_required=1,updated_at=? WHERE id=?", (safe["category"], _iso(now), run_id))
            _checkpoint(database, run_id, "JOB_COMPLETED_OR_HELD", "ERROR_TERMINAL", now, safe)
            return {"ok": False, "blocking_reasons": ["ERROR_TERMINAL"]}
    with database.connection:
        database.connection.execute("UPDATE scheduler_runs SET state='COMPLETED',updated_at=?,finalized_at=? WHERE id=?", (_iso(now), _iso(now), run_id))
    _checkpoint(database, run_id, "RUN_FINALIZED", "COMPLETED", now, {})
    row = database.connection.execute("SELECT lease_id FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
    if row and row["lease_id"]:
        _release_lease(database, row["lease_id"], now)
    return {"ok": True}


def run_fixture_scheduled_window(database: Database, *, campaign_id: int, campaign_timezone: str, now: Optional[datetime] = None, fixture_override: bool = False, executors: Mapping[str, Callable[[dict[str, Any]], Mapping[str, Any]]] | FixtureSchedulerExecutorRegistry = {}, job_specs: Iterable[Mapping[str, Any]] = (), capacity_caps: Optional[Mapping[str, int]] = None, followup_items: Iterable[Mapping[str, Any]] = (), candidate_items: Iterable[Mapping[str, Any]] = (), notification_items: Iterable[Mapping[str, Any]] = ()) -> dict[str, Any]:
    """Run one explicitly requested fixture window; never installs or loops."""
    _require_fixture(database, fixture_override)
    migrate_step14(database)
    current = _now(now)
    run_id, prepared = _prepare_run(database, campaign_id=campaign_id, campaign_timezone=campaign_timezone, now=current, fixture_override=fixture_override)
    if run_id is None:
        return prepared
    if not prepared["ok"]:
        return {**_safe_run_result(database, run_id), **prepared}
    if prepared.get("reused"):
        return {**_safe_run_result(database, run_id), **prepared}
    local_day = database.connection.execute("SELECT local_date FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()["local_date"]
    followup_items = list(followup_items)
    candidate_items = list(candidate_items)
    notification_items = list(notification_items)
    _checkpoint(database, run_id, "DUE_FOLLOWUPS_SELECTED", "READINESS_CHECK", current, {"followup_count": len(followup_items)})
    config = database.read_config()
    caps = dict(capacity_caps or {
        "candidate_processing": config.get("daily_candidate_processing_cap", 0),
        "drafting": config.get("drafting_daily_cap", 0),
        "gmail_draft_creation": config.get("gmail_draft_daily_cap", 0),
        "followup": config.get("followup_daily_cap", 0),
        "message_touch": config.get("daily_message_cap", 0),
        "notification_delivery": config.get("notification_daily_cap", 0),
    })
    capacity = plan_shared_daily_capacity(database, campaign_id=campaign_id, campaign_timezone=campaign_timezone, local_date=local_day, caps=caps, followup_items=followup_items, candidate_items=candidate_items, notification_items=notification_items, fixture_override=True, now=current)
    _checkpoint(database, run_id, "CAPACITY_PLANNED", "CAPACITY_PLANNED", current, {"followup_reserved": capacity.get("followup_reserved", 0), "new_work_reserved": capacity.get("new_work_reserved", 0)})
    if not capacity.get("ok") and (followup_items or candidate_items or notification_items):
        with database.connection:
            database.connection.execute("UPDATE scheduler_runs SET state='BLOCKED',last_error_category='CAPACITY_BLOCKED',human_review_required=0,updated_at=?,finalized_at=? WHERE id=?", (_iso(current), _iso(current), run_id))
        lease = database.connection.execute("SELECT lease_id FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
        if lease and lease["lease_id"]:
            _release_lease(database, lease["lease_id"], current)
        return {**_safe_run_result(database, run_id), "ok": False, "blocking_reasons": ["CAPACITY_BLOCKED"] + sorted(set(capacity.get("blocked_reasons", [])))}
    with database.connection:
        database.connection.execute("UPDATE scheduler_runs SET state='CAPACITY_PLANNED',updated_at=? WHERE id=?", (_iso(current), run_id))
    job_specs = list(job_specs)
    _checkpoint(database, run_id, "RESUMABLE_WORK_QUEUED", "JOBS_QUEUED", current, {"job_count": len(job_specs)})
    _checkpoint(database, run_id, "NEW_WORK_JOBS_QUEUED", "JOBS_QUEUED", current, {"job_count": len(job_specs)})
    result = _execute_jobs(database, run_id, job_specs, executors, current)
    return {**_safe_run_result(database, run_id), **result}


def inspect_scheduler_run(database: Database, *, run_id: int) -> dict[str, Any]:
    migrate_step14(database)
    row = database.connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
    if row is None:
        return {"found": False, "run_id": run_id}
    jobs = [dict(item) for item in database.connection.execute("SELECT id,action_type,entity_type,entity_id,entity_version,state,attempt_count,result_category,last_checkpoint,human_review_required FROM scheduler_jobs WHERE run_id=? ORDER BY id", (run_id,)).fetchall()]
    checkpoints = [dict(item) for item in database.connection.execute("SELECT checkpoint_name,checkpoint_sequence,state,created_at FROM scheduler_checkpoints WHERE run_id=? ORDER BY checkpoint_sequence", (run_id,)).fetchall()]
    return {"found": True, "run_id": run_id, "schedule_key": row["schedule_key"], "state": row["state"], "campaign_id": row["campaign_id"], "local_date": row["local_date"], "campaign_timezone": row["campaign_timezone"], "last_completed_checkpoint": row["last_completed_checkpoint"], "jobs": jobs, "checkpoints": checkpoints}


def inspect_interrupted_runs(database: Database, *, now: Optional[datetime] = None) -> dict[str, Any]:
    migrate_step14(database)
    current = _now(now)
    runs = [dict(row) for row in database.connection.execute("SELECT id,schedule_key,campaign_id,local_date,state,last_completed_checkpoint,last_error_category,human_review_required FROM scheduler_runs WHERE state NOT IN ('COMPLETED','CANCELLED') ORDER BY id").fetchall()]
    stale = [dict(row) for row in database.connection.execute("SELECT id,lease_key,owner_run_id,expires_at_utc,state FROM scheduler_leases WHERE state='ACTIVE' AND expires_at_utc<=? ORDER BY id", (_iso(current),)).fetchall()]
    unknown = [dict(row) for row in database.connection.execute("SELECT id,run_id,action_type,entity_id,state,result_category FROM scheduler_jobs WHERE state='RECOVERY_REQUIRED' ORDER BY id").fetchall()]
    return {"interrupted_runs": runs, "stale_leases": stale, "unknown_jobs": unknown, "last_completed_checkpoint": runs[0]["last_completed_checkpoint"] if runs else None, "recommendation": "OPERATOR_INSPECTION_REQUIRED" if (runs or stale or unknown) else "NO_RECOVERY_REQUIRED"}


def detect_missed_windows(database: Database, *, campaign_id: int, campaign_timezone: str, scheduled_run_time: str, offline_since: datetime, now: datetime, fixture_override: bool = False) -> dict[str, Any]:
    _require_fixture(database, fixture_override)
    migrate_step14(database)
    zone, reason = _valid_timezone(campaign_timezone)
    if reason or zone is None:
        return {"ok": False, "blocking_reasons": [reason]}
    try:
        hour, minute = (int(piece) for piece in scheduled_run_time.split(":", 1))
        schedule = time(hour, minute)
    except (ValueError, TypeError):
        return {"ok": False, "blocking_reasons": ["SCHEDULE_TIME_INVALID"]}
    start = _now(offline_since).astimezone(zone).date()
    end = _now(now).astimezone(zone).date()
    windows: list[str] = []
    current_day = start
    while current_day <= end:
        due_local = datetime.combine(current_day, schedule, tzinfo=zone).astimezone(timezone.utc)
        if due_local <= _now(now):
            key = _lease_key(campaign_id, current_day.isoformat())
            if database.connection.execute("SELECT 1 FROM scheduler_runs WHERE schedule_key=?", (key,)).fetchone() is None:
                windows.append(current_day.isoformat())
        current_day += timedelta(days=1)
    return {"ok": True, "missed_windows": windows, "auto_process": False, "catchup_enabled": False, "recommendation": "MISSED_WINDOW_REVIEW_REQUIRED" if windows else "NO_MISSED_WINDOW"}


def verify_startup_recovery_readiness(database: Database, *, campaign_id: int, campaign_timezone: str, scheduled_run_time: str, offline_since: datetime, now: datetime, fixture_override: bool = False) -> dict[str, Any]:
    readiness = inspect_scheduler_readiness(database, campaign_id=campaign_id, campaign_timezone=campaign_timezone, fixture_override=fixture_override, now=now)
    recovery = inspect_interrupted_runs(database, now=now)
    missed = detect_missed_windows(database, campaign_id=campaign_id, campaign_timezone=campaign_timezone, scheduled_run_time=scheduled_run_time, offline_since=offline_since, now=now, fixture_override=fixture_override) if fixture_override and not _is_durable(database) else {"ok": False, "blocking_reasons": ["CANONICAL_SCHEDULER_OPERATION_BLOCKED"]}
    return {"ready": readiness["ready"] and not recovery["interrupted_runs"] and not recovery["stale_leases"] and not recovery["unknown_jobs"] and not missed.get("missed_windows"), "readiness": readiness, "recovery": recovery, "missed": missed, "recommendation": "OPERATOR_INSPECTION_REQUIRED" if recovery["recommendation"] != "NO_RECOVERY_REQUIRED" or missed.get("missed_windows") else "SAFE_TO_REVIEW_CURRENT_WINDOW"}


def resume_fixture_interrupted_run(database: Database, *, run_id: int, now: Optional[datetime] = None, fixture_override: bool = False, executors: Mapping[str, Callable[[dict[str, Any]], Mapping[str, Any]]] | FixtureSchedulerExecutorRegistry = {}) -> dict[str, Any]:
    _require_fixture(database, fixture_override)
    migrate_step14(database)
    current = _now(now)
    run = database.connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
    if run is None:
        return {"ok": False, "blocking_reasons": ["RUN_NOT_FOUND"]}
    if run["state"] in {"COMPLETED", "CANCELLED"}:
        return {"ok": False, "blocking_reasons": ["TERMINAL_RUN"]}
    if run["state"] == "RECOVERY_REQUIRED":
        return {"ok": False, "blocking_reasons": ["UNKNOWN_OUTCOME_REQUIRES_OPERATOR_RECONCILIATION"]}
    readiness = inspect_scheduler_readiness(database, campaign_id=run["campaign_id"], campaign_timezone=run["campaign_timezone"], fixture_override=True, now=current)
    if not readiness["ready"]:
        return {"ok": False, "blocking_reasons": readiness["blocking_reasons"]}
    specs = [dict(row) for row in database.connection.execute("SELECT action_type,entity_type,entity_id,entity_version,input_fingerprint,priority FROM scheduler_jobs WHERE run_id=? AND state!='COMPLETED' ORDER BY CASE priority WHEN 'FOLLOWUP_FIRST' THEN 0 ELSE 1 END,id", (run_id,)).fetchall()]
    result = _execute_jobs(database, run_id, specs, executors, current)
    return {**_safe_run_result(database, run_id), **result, "resumed": True}


def cancel_or_hold_fixture_run(database: Database, *, run_id: int, action: str, reason: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    _require_fixture(database, fixture_override)
    migrate_step14(database)
    if action not in {"CANCEL", "HOLD"} or not str(reason).strip():
        raise SchedulerValidationError("action must be CANCEL or HOLD with a reason")
    current = _now(now)
    state = "CANCELLED" if action == "CANCEL" else "HOLD_FOR_REVIEW"
    with database.connection:
        database.connection.execute("UPDATE scheduler_runs SET state=?,human_review_required=?,last_error_category=?,updated_at=?,finalized_at=? WHERE id=?", (state, int(action == "HOLD"), _safe_text(reason), _iso(current), _iso(current), run_id))
    row = database.connection.execute("SELECT lease_id FROM scheduler_runs WHERE id=?", (run_id,)).fetchone()
    if row and row["lease_id"]:
        _release_lease(database, row["lease_id"], current)
    return _safe_run_result(database, run_id, ok=True, reason=_safe_text(reason))


def _table_count(database: Database, table: str, where: str = "") -> int:
    try:
        return int(database.connection.execute(f"SELECT COUNT(*) FROM {table} {where}").fetchone()[0])
    except Exception:
        return 0


def generate_fixture_weekly_review(database: Database, *, campaign_id: int, campaign_timezone: str, week_start_local: date | str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    _require_fixture(database, fixture_override)
    migrate_step14(database)
    zone, reason = _valid_timezone(campaign_timezone)
    if reason or zone is None:
        return {"ok": False, "blocking_reasons": [reason]}
    week = week_start_local.isoformat() if isinstance(week_start_local, date) else str(week_start_local)
    date.fromisoformat(week)
    review_key = f"campaign:{campaign_id}:week:{week}:tz:{campaign_timezone}"
    counts = {
        "candidates_discovered": _table_count(database, "discovery_candidates"),
        "qualified": _table_count(database, "qualification_results", "WHERE classification='QUALIFIED'"),
        "strong": _table_count(database, "qualification_results", "WHERE classification='STRONG'"),
        "hold": _table_count(database, "qualification_results", "WHERE classification='HOLD'"),
        "rejected": _table_count(database, "qualification_results", "WHERE classification='REJECT'"),
        "suppressed": _table_count(database, "suppressions"),
        "drafts_prepared": _table_count(database, "personalized_drafts"),
        "drafts_reviewed": _table_count(database, "gmail_draft_creation_approvals"),
        "manually_recorded_sends": _table_count(database, "followup_sent_touches"),
        "followups_due": _table_count(database, "followup_actions", "WHERE state IN ('DUE','REQUESTED','READY')"),
        "followups_completed": _table_count(database, "followup_actions", "WHERE state='CONSUMED'"),
        "reply_classifications": _table_count(database, "followup_operator_classifications"),
        "human_action_cases": _table_count(database, "followup_sequences", "WHERE state='HUMAN_ACTION_REQUIRED'"),
        "bounces": _table_count(database, "followup_reply_events", "WHERE category IN ('SOFT_BOUNCE','HARD_BOUNCE')"),
        "opt_outs": _table_count(database, "followup_reply_events", "WHERE category='OPT_OUT'"),
        "calls": _table_count(database, "leads", "WHERE status='CALL_BOOKED'"),
        "proposals": _table_count(database, "leads", "WHERE status='PROPOSAL'"),
        "wins": _table_count(database, "leads", "WHERE status='WON'"),
        "losses": _table_count(database, "leads", "WHERE status='LOST'"),
        "provider_or_validation_errors": _table_count(database, "events", "WHERE event_type LIKE '%ERROR%' OR event_type LIKE '%VALIDATION%'"),
        "capacity_reserved": _table_count(database, "scheduler_capacity_reservations", "WHERE reservation_state='RESERVED'"),
        "capacity_consumed": _table_count(database, "scheduler_capacity_reservations", "WHERE reservation_state='CONSUMED'"),
        "capacity_released_or_invalidated": _table_count(database, "scheduler_capacity_reservations", "WHERE reservation_state IN ('RELEASED','INVALIDATED','EXPIRED')"),
        "interrupted_runs": _table_count(database, "scheduler_runs", "WHERE state IN ('RECOVERY_REQUIRED','ERROR_RETRYABLE','HOLD_FOR_REVIEW')"),
        "recovered_runs": _table_count(database, "scheduler_recovery_events", "WHERE outcome IN ('RECOVERED','RESUMED')"),
    }
    snapshot = {"policy_version": SCHEDULER_POLICY_VERSION, "campaign_id": campaign_id, "week_start_local": week, "campaign_timezone": campaign_timezone, "metrics": counts, "conclusions": [], "revenue": "UNAVAILABLE_WITHOUT_DATABASE_FACTS"}
    existing = database.connection.execute("SELECT id,snapshot_json FROM scheduler_weekly_reviews WHERE review_key=?", (review_key,)).fetchone()
    current = _now(now)
    with database.connection:
        if existing is None:
            cursor = database.connection.execute("INSERT INTO scheduler_weekly_reviews(review_key,campaign_id,week_start_local,campaign_timezone,policy_version,snapshot_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)", (review_key, campaign_id, week, campaign_timezone, SCHEDULER_POLICY_VERSION, _json(snapshot), _iso(current), _iso(current)))
            review_id = int(cursor.lastrowid)
            reused = False
        else:
            database.connection.execute("UPDATE scheduler_weekly_reviews SET snapshot_json=?,updated_at=? WHERE id=?", (_json(snapshot), _iso(current), existing["id"]))
            review_id = int(existing["id"])
            reused = True
    return {"ok": True, "review_id": review_id, "review_key": review_key, "reused": reused, "snapshot": snapshot}


def summarize_scheduler_provenance(database: Database) -> dict[str, Any]:
    migrate_step14(database)
    return {"policy_version": SCHEDULER_POLICY_VERSION, "mode": SCHEDULER_MODE, "trigger_mode": SCHEDULER_TRIGGER_MODE, "install_state": SCHEDULER_INSTALL_STATE, "scheduler_runs": _table_count(database, "scheduler_runs"), "scheduler_jobs": _table_count(database, "scheduler_jobs"), "capacity_reservations": _table_count(database, "scheduler_capacity_reservations"), "recovery_events": _table_count(database, "scheduler_recovery_events"), "weekly_reviews": _table_count(database, "scheduler_weekly_reviews"), "external_calls": False, "live_operations": False}


__all__ = [
    "SCHEDULER_MIGRATION_VERSION", "SCHEDULER_POLICY_VERSION", "SCHEDULER_MODE", "SCHEDULER_TRIGGER_MODE", "SCHEDULER_INSTALL_STATE", "SCHEDULER_MAX_ATTEMPTS", "SCHEDULER_MAX_CONCURRENT_RUNS", "SCHEDULER_TIMEZONE_POLICY", "EXECUTOR_ACTIONS", "SchedulerBlockedError", "SchedulerValidationError", "FixtureSchedulerUnknownOutcome", "FixtureSchedulerRetryableError", "FixtureSchedulerExecutorRegistry", "calculate_campaign_local_date", "calculate_scheduler_due_date", "migrate_step14", "inspect_scheduler_readiness", "preview_fixture_campaign_day", "select_due_followup_items", "plan_shared_daily_capacity", "update_fixture_reservation_state", "run_fixture_scheduled_window", "inspect_scheduler_run", "inspect_interrupted_runs", "detect_missed_windows", "verify_startup_recovery_readiness", "resume_fixture_interrupted_run", "cancel_or_hold_fixture_run", "generate_fixture_weekly_review", "summarize_scheduler_provenance",
]
