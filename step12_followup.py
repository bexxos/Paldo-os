"""Step 12: fixture-only follow-up and reply lifecycle.

This module records workflow state around manually performed outreach. It never
performs delivery, reads a mailbox, generates message wording, or chooses a
production cadence. All external-looking identities are fictional fixture data
and all lifecycle writes require an isolated fixture database.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Optional
from zoneinfo import ZoneInfo

from paldo_os_outbound import DEFAULT_DB_PATH, Database, normalize_domain, normalize_email
from step10_drafting import preview_bounded_drafting_input
from step11_gmail_drafts import migrate_step11, preview_gmail_draft_payload


STEP12_MIGRATION_VERSION = 14
FOLLOWUP_POLICY_VERSION = "TEST_FOLLOWUP_V0_1"
FOLLOWUP_PROVIDER_MODE = "FIXTURE_ONLY"
FOLLOWUP_MAX_TOTAL_TOUCHES = 3
FOLLOWUP_REPLY_CATEGORIES = {
    "POSITIVE_INTEREST", "QUESTION_OR_NEUTRAL", "NOT_INTERESTED", "OPT_OUT",
    "OUT_OF_OFFICE", "SOFT_BOUNCE", "HARD_BOUNCE", "UNKNOWN",
}
FOLLOWUP_CADENCE_STRATEGIES = {"CALENDAR_DAYS", "BUSINESS_DAYS"}
FOLLOWUP_STATES = {
    "AWAITING_MANUAL_SEND", "WAITING_FOR_REPLY", "FOLLOWUP_ELIGIBILITY_PENDING",
    "FOLLOWUP_DUE", "FOLLOWUP_DRAFT_REQUESTED", "FOLLOWUP_REVIEW_PENDING",
    "HUMAN_ACTION_REQUIRED", "CLOSED_POSITIVE", "CLOSED_NOT_INTERESTED",
    "CLOSED_NO_RESPONSE", "SUPPRESSED", "BOUNCE_HOLD", "OUT_OF_OFFICE_HOLD",
    "HOLD_FOR_REVIEW", "ERROR",
}
_TERMINAL_STATES = {"CLOSED_POSITIVE", "CLOSED_NOT_INTERESTED", "CLOSED_NO_RESPONSE", "SUPPRESSED"}
_REPLY_HOLDS = {"HUMAN_ACTION_REQUIRED", "BOUNCE_HOLD", "OUT_OF_OFFICE_HOLD", "HOLD_FOR_REVIEW"}
_FIXTURE_SENDER_SUFFIX = ".fictional-example.invalid"


class FollowupBlockedError(RuntimeError):
    """Raised when a canonical or unsafe follow-up mutation is attempted."""


class FollowupValidationError(ValueError):
    """Raised for malformed lifecycle, cadence, or fixture-reply input."""


class FixtureReplyProvider:
    """Injected local reply source exposing retrieval of one supplied event only."""

    def __init__(self, event: Mapping[str, Any]):
        self.event = deepcopy(dict(event))
        self.calls = 0

    def get_reply(self, provider_event_id: str) -> Optional[dict[str, Any]]:
        self.calls += 1
        if self.event.get("provider_event_id") != provider_event_id:
            return None
        return deepcopy(self.event)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS followup_sequences (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    source_gmail_run_id INTEGER NOT NULL REFERENCES gmail_draft_creation_runs(id) ON DELETE RESTRICT,
    source_draft_id INTEGER NOT NULL REFERENCES personalized_drafts(id) ON DELETE RESTRICT,
    source_draft_version INTEGER NOT NULL CHECK (source_draft_version > 0),
    recipient_email TEXT NOT NULL,
    sender_email TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    offer_version_id INTEGER NOT NULL REFERENCES audit_offer_versions(id) ON DELETE RESTRICT,
    packet_id INTEGER NOT NULL REFERENCES kb_context_packets(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN ('AWAITING_MANUAL_SEND','WAITING_FOR_REPLY','FOLLOWUP_ELIGIBILITY_PENDING','FOLLOWUP_DUE','FOLLOWUP_DRAFT_REQUESTED','FOLLOWUP_REVIEW_PENDING','HUMAN_ACTION_REQUIRED','CLOSED_POSITIVE','CLOSED_NOT_INTERESTED','CLOSED_NO_RESPONSE','SUPPRESSED','BOUNCE_HOLD','OUT_OF_OFFICE_HOLD','HOLD_FOR_REVIEW','ERROR')),
    current_touch_count INTEGER NOT NULL DEFAULT 0 CHECK (current_touch_count >= 0 AND current_touch_count <= 3),
    max_total_touches INTEGER NOT NULL DEFAULT 3 CHECK (max_total_touches = 3),
    cadence_strategy TEXT CHECK (cadence_strategy IN ('CALENDAR_DAYS','BUSINESS_DAYS') OR cadence_strategy IS NULL),
    interval_value INTEGER CHECK (interval_value IS NULL OR interval_value > 0),
    campaign_timezone TEXT NOT NULL,
    holiday_calendar_json TEXT NOT NULL DEFAULT '[]',
    next_due_at TEXT,
    out_of_office_return_at TEXT,
    hard_bounce_count INTEGER NOT NULL DEFAULT 0 CHECK (hard_bounce_count >= 0),
    close_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (source_gmail_run_id)
);
CREATE INDEX IF NOT EXISTS followup_sequences_state_idx ON followup_sequences(state, next_due_at);
CREATE INDEX IF NOT EXISTS followup_sequences_lead_idx ON followup_sequences(lead_id, created_at);

CREATE TABLE IF NOT EXISTS followup_sent_touches (
    id INTEGER PRIMARY KEY,
    sequence_id INTEGER NOT NULL REFERENCES followup_sequences(id) ON DELETE CASCADE,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    touch_number INTEGER NOT NULL CHECK (touch_number BETWEEN 1 AND 3),
    idempotency_key TEXT NOT NULL UNIQUE,
    outreach_or_draft_id TEXT NOT NULL,
    draft_version INTEGER NOT NULL CHECK (draft_version > 0),
    recipient_email TEXT NOT NULL,
    sender_email TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    reviewer_identity TEXT NOT NULL CHECK (reviewer_identity = 'OPERATOR'),
    reason TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    provider_draft_id TEXT,
    provider_message_id TEXT,
    provider_thread_id TEXT,
    due_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (sequence_id, touch_number)
);
CREATE INDEX IF NOT EXISTS followup_touches_sequence_idx ON followup_sent_touches(sequence_id, touch_number);

CREATE TABLE IF NOT EXISTS followup_reply_events (
    id INTEGER PRIMARY KEY,
    provider_event_id TEXT NOT NULL UNIQUE,
    sequence_id INTEGER NOT NULL REFERENCES followup_sequences(id) ON DELETE CASCADE,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    outreach_id TEXT NOT NULL,
    message_id TEXT,
    thread_id TEXT,
    received_at TEXT NOT NULL,
    sender_email TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('POSITIVE_INTEREST','QUESTION_OR_NEUTRAL','NOT_INTERESTED','OPT_OUT','OUT_OF_OFFICE','SOFT_BOUNCE','HARD_BOUNCE','UNKNOWN')),
    safe_text_hash TEXT NOT NULL,
    classification_source TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    operator_review_status TEXT NOT NULL CHECK (operator_review_status IN ('NOT_REQUIRED','PENDING','REVIEWED')),
    return_at TEXT,
    explicit_opt_out INTEGER NOT NULL DEFAULT 0 CHECK (explicit_opt_out IN (0,1)),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS followup_replies_sequence_idx ON followup_reply_events(sequence_id, received_at);

CREATE TABLE IF NOT EXISTS followup_actions (
    id INTEGER PRIMARY KEY,
    sequence_id INTEGER NOT NULL REFERENCES followup_sequences(id) ON DELETE CASCADE,
    action_type TEXT NOT NULL CHECK (action_type = 'FOLLOWUP_DRAFT'),
    touch_number INTEGER NOT NULL CHECK (touch_number BETWEEN 2 AND 3),
    state TEXT NOT NULL CHECK (state IN ('DUE','REQUESTED','READY','CONSUMED','CANCELLED','INVALIDATED')),
    due_at TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL UNIQUE,
    source_input_fingerprint TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    sender_email TEXT NOT NULL,
    requested_at TEXT,
    cancelled_at TEXT,
    cancellation_reason TEXT,
    created_at TEXT NOT NULL,
    UNIQUE (sequence_id, touch_number)
);
CREATE INDEX IF NOT EXISTS followup_actions_state_idx ON followup_actions(state, due_at);

CREATE TABLE IF NOT EXISTS followup_capacity_reservations (
    id INTEGER PRIMARY KEY,
    sequence_id INTEGER NOT NULL REFERENCES followup_sequences(id) ON DELETE CASCADE,
    action_id INTEGER NOT NULL REFERENCES followup_actions(id) ON DELETE CASCADE,
    touch_number INTEGER NOT NULL CHECK (touch_number BETWEEN 2 AND 3),
    reservation_key TEXT NOT NULL UNIQUE,
    reservation_state TEXT NOT NULL CHECK (reservation_state IN ('RESERVED','CONSUMED','CANCELLED')),
    policy_version TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    cancelled_at TEXT,
    UNIQUE (sequence_id, touch_number)
);
CREATE INDEX IF NOT EXISTS followup_reservations_state_idx ON followup_capacity_reservations(reservation_state, reserved_at);

CREATE TABLE IF NOT EXISTS followup_lifecycle_transitions (
    id INTEGER PRIMARY KEY,
    sequence_id INTEGER NOT NULL REFERENCES followup_sequences(id) ON DELETE CASCADE,
    from_state TEXT,
    to_state TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    reason TEXT NOT NULL,
    safe_metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS followup_transitions_sequence_idx ON followup_lifecycle_transitions(sequence_id, id);

CREATE TABLE IF NOT EXISTS followup_operator_classifications (
    id INTEGER PRIMARY KEY,
    reply_event_id INTEGER NOT NULL REFERENCES followup_reply_events(id) ON DELETE CASCADE,
    reviewer_identity TEXT NOT NULL CHECK (reviewer_identity = 'OPERATOR'),
    category TEXT NOT NULL CHECK (category IN ('POSITIVE_INTEREST','QUESTION_OR_NEUTRAL','NOT_INTERESTED','OPT_OUT','OUT_OF_OFFICE','SOFT_BOUNCE','HARD_BOUNCE')),
    policy_version TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS followup_classifications_reply_idx ON followup_operator_classifications(reply_event_id, created_at);
"""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


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
    if not isinstance(value, str) or not value.strip():
        raise FollowupValidationError("timestamp is required")
    try:
        return _now(datetime.fromisoformat(value.strip().replace("Z", "+00:00")))
    except ValueError as error:
        raise FollowupValidationError("timestamp is invalid") from error


def _is_durable_database(database: Database) -> bool:
    try:
        return database.path.resolve() == DEFAULT_DB_PATH.resolve()
    except (AttributeError, OSError):
        return False


def _require_fixture(database: Database, fixture_override: bool) -> None:
    if _is_durable_database(database):
        raise FollowupBlockedError("canonical follow-up operation is blocked")
    if not fixture_override:
        raise FollowupBlockedError("fixture_override=True is required for Step 12")


def migrate_step12(database_or_path: Database | str | Path) -> int:
    """Apply the additive Step 12 schema and safe provisional defaults."""
    database = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    migrate_step11(database)
    with database.connection:
        database.connection.executescript(_SCHEMA)
        columns = {row[1] for row in database.connection.execute("PRAGMA table_info(followup_reply_events)")}
        if "explicit_opt_out" not in columns:
            database.connection.execute("ALTER TABLE followup_reply_events ADD COLUMN explicit_opt_out INTEGER NOT NULL DEFAULT 0")
        database.connection.executemany(
            "INSERT OR IGNORE INTO system_config (key,value,value_type) VALUES (?,?,?)",
            (
                ("followup_enabled", "0", "integer"),
                ("reply_ingestion_enabled", "0", "integer"),
                ("followup_provider_mode", FOLLOWUP_PROVIDER_MODE, "text"),
                ("followup_policy_version", FOLLOWUP_POLICY_VERSION, "text"),
                ("followup_policy_status", "PROVISIONAL", "text"),
                ("followup_max_total_touches", str(FOLLOWUP_MAX_TOTAL_TOUCHES), "integer"),
                ("followup_cadence_status", "UNDECIDED", "text"),
                ("followup_daily_cap", "0", "integer"),
            ),
        )
        database.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version,name,applied_at) VALUES (?,?,?)",
            (STEP12_MIGRATION_VERSION, "step12_fixture_followup_reply_lifecycle", _iso()),
        )
    return STEP12_MIGRATION_VERSION


def _config_reasons(database: Database, *, fixture_override: bool, require_reply: bool = False, require_capacity: bool = False, now: datetime) -> list[str]:
    config = database.read_config()
    reasons: list[str] = []
    if not fixture_override:
        reasons.append("FIXTURE_OVERRIDE_REQUIRED")
    if config.get("system_state") != "ACTIVE":
        reasons.append("SYSTEM_PAUSED")
    if not config.get("followup_enabled"):
        reasons.append("FOLLOWUP_DISABLED")
    if require_reply and not config.get("reply_ingestion_enabled"):
        reasons.append("REPLY_INGESTION_DISABLED")
    if config.get("followup_provider_mode") != FOLLOWUP_PROVIDER_MODE:
        reasons.append("FOLLOWUP_PROVIDER_MODE_UNSAFE")
    if config.get("followup_policy_version") != FOLLOWUP_POLICY_VERSION:
        reasons.append("FOLLOWUP_POLICY_VERSION_UNSAFE")
    if config.get("followup_policy_status") != "PROVISIONAL":
        reasons.append("FOLLOWUP_POLICY_NOT_PROVISIONAL")
    if int(config.get("followup_max_total_touches", 0) or 0) != FOLLOWUP_MAX_TOTAL_TOUCHES:
        reasons.append("MAX_TOUCH_POLICY_UNSAFE")
    if config.get("followup_cadence_status") != "UNDECIDED":
        reasons.append("CADENCE_MUST_REMAIN_UNDECIDED")
    if require_capacity:
        cap = int(config.get("followup_daily_cap", 0) or 0)
        if cap <= 0:
            reasons.append("FOLLOWUP_DAILY_CAP_ZERO")
        else:
            day_start = _iso(now.replace(hour=0, minute=0, second=0))
            count = database.connection.execute(
                "SELECT COUNT(*) FROM followup_capacity_reservations WHERE reservation_state='RESERVED' AND reserved_at>=?", (day_start,)
            ).fetchone()[0]
            if count >= cap:
                reasons.append("FOLLOWUP_DAILY_CAP_REACHED")
    return reasons


def _sequence_row(database: Database, sequence_id: int):
    row = database.connection.execute("SELECT * FROM followup_sequences WHERE id=?", (sequence_id,)).fetchone()
    if row is None:
        raise FollowupValidationError("follow-up sequence not found")
    return row


def _lead(database: Database, lead_id: int):
    row = database.get_lead(lead_id)
    if row is None:
        raise FollowupValidationError("lead not found")
    return row


def _sender(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not value.strip() or "@" not in value:
        return None
    try:
        normalized = normalize_email(value)
    except (TypeError, ValueError):
        return None
    domain = normalized.rsplit("@", 1)[1]
    return normalized if domain == "fictional-example.invalid" or domain.endswith(_FIXTURE_SENDER_SUFFIX) else None


def _email(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        normalized = normalize_email(value)
    except (TypeError, ValueError):
        return None
    return normalized if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", normalized) else None


def _transition(database: Database, sequence_id: int, from_state: Optional[str], to_state: str, event_type: str, reason: str, *, actor: str = "SYSTEM", now: datetime, metadata: Optional[Mapping[str, Any]] = None) -> None:
    with database.connection:
        database.connection.execute(
            "INSERT INTO followup_lifecycle_transitions(sequence_id,from_state,to_state,event_type,actor,policy_version,reason,safe_metadata_json,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (sequence_id, from_state, to_state, event_type, actor, FOLLOWUP_POLICY_VERSION, " ".join(str(reason).split())[:500], _json(metadata or {}), _iso(now)),
        )


def _set_state(database: Database, sequence, state: str, reason: str, *, event_type: str, actor: str = "SYSTEM", now: datetime, metadata: Optional[Mapping[str, Any]] = None, **fields: Any) -> None:
    previous = sequence["state"]
    assignments = ["state=?", "updated_at=?"]
    values: list[Any] = [state, _iso(now)]
    for key, value in fields.items():
        assignments.append(f"{key}=?")
        values.append(value)
    values.append(sequence["id"])
    with database.connection:
        database.connection.execute(f"UPDATE followup_sequences SET {','.join(assignments)} WHERE id=?", values)
    if previous != state:
        _transition(database, sequence["id"], previous, state, event_type, reason, actor=actor, now=now, metadata=metadata)


def _current_source(database: Database, sequence, now: datetime) -> tuple[Optional[dict[str, Any]], list[str]]:
    reasons: list[str] = []
    draft = database.connection.execute("SELECT * FROM personalized_drafts WHERE id=?", (sequence["source_draft_id"],)).fetchone()
    if draft is None:
        return None, ["SOURCE_DRAFT_NOT_FOUND"]
    if draft["state"] != "REVIEW_PENDING" or draft["version"] != sequence["source_draft_version"]:
        reasons.append("STALE_SOURCE_DRAFT")
    validation = database.connection.execute("SELECT validation_state FROM draft_validation_results WHERE draft_id=? ORDER BY id DESC LIMIT 1", (draft["id"],)).fetchone()
    if validation is None or validation["validation_state"] != "PASS":
        reasons.append("STEP10_VALIDATION_REQUIRED")
    lead = _lead(database, sequence["lead_id"])
    owner = database.connection.execute(
        "SELECT public_business_email FROM lead_decision_makers WHERE lead_id=? AND verification_status='VERIFIED' AND public_business_email IS NOT NULL ORDER BY id LIMIT 1",
        (sequence["lead_id"],),
    ).fetchone() if database._table_has_column("lead_decision_makers", "public_business_email") else None
    selected_recipient = _email(owner["public_business_email"] if owner is not None else lead.get("email"))
    if selected_recipient != sequence["recipient_email"]:
        reasons.append("RECIPIENT_CHANGED")
    if selected_recipient is not None and database.is_suppressed(lead_id=lead["id"], email=selected_recipient, domain=selected_recipient.rsplit("@", 1)[1]):
        reasons.append("SUPPRESSED")
    # A due check may happen after the short-lived Gold-packet read window.
    # Revalidate against the durable approval checkpoint, while direct source
    # mutations still fail through the same Step 10 checks.
    checkpoint = now
    try:
        checkpoint = _parse_time(sequence["created_at"])
    except (KeyError, TypeError, FollowupValidationError):
        pass
    try:
        draft_run = database.connection.execute("SELECT input_json FROM drafting_runs WHERE id=?", (draft["run_id"],)).fetchone()
        draft_run_input = {} if draft_run is None else json.loads(draft_run["input_json"])
        message_policy_context = draft_run_input.get("message_policy")
        payload = preview_gmail_draft_payload(database, draft_id=draft["id"], sender_email=sequence["sender_email"], fixture_override=True, now=checkpoint)
        bounded = preview_bounded_drafting_input(database, lead_id=sequence["lead_id"], campaign_id=sequence["campaign_id"], packet_id=sequence["packet_id"], message_policy_context=message_policy_context, now=checkpoint)
    except Exception:
        payload = {"ready": False, "blocking_reasons": [], "content_hash": None}
        bounded = {"input_fingerprint": None, "blocking_reasons": ["SOURCE_DRAFT_RECHECK_FAILED"]}
    if not payload.get("ready"):
        reasons.extend(payload.get("blocking_reasons", []))
    reasons.extend(bounded.get("blocking_reasons", []))
    if bounded.get("input_fingerprint") != sequence["input_fingerprint"]:
        reasons.append("INPUT_FINGERPRINT_CHANGED")
    if payload.get("content_hash") != sequence["content_hash"]:
        reasons.append("CONTENT_HASH_CHANGED")
    return dict(draft), sorted(set(reasons))


def _result(database: Database, sequence_id: int, *, reused: bool = False, reasons: Optional[Iterable[str]] = None, **extra: Any) -> dict[str, Any]:
    sequence = _sequence_row(database, sequence_id)
    return {
        "sequence_id": sequence_id, "state": sequence["state"], "touch_count": sequence["current_touch_count"],
        "max_total_touches": sequence["max_total_touches"], "next_due_at": sequence["next_due_at"],
        "close_reason": sequence["close_reason"], "reused": reused, "blocking_reasons": sorted(set(reasons or [])), **extra,
    }


def calculate_fixture_due_date(sent_at: datetime | str, cadence_strategy: str, interval_value: int, campaign_timezone: str = "UTC", holiday_calendar: Iterable[str] = ()) -> datetime:
    """Calculate a fixture due date using an explicitly supplied strategy only."""
    sent = _parse_time(sent_at)
    if cadence_strategy not in FOLLOWUP_CADENCE_STRATEGIES:
        raise FollowupValidationError("cadence strategy must be CALENDAR_DAYS or BUSINESS_DAYS")
    if isinstance(interval_value, bool) or not isinstance(interval_value, int) or interval_value <= 0:
        raise FollowupValidationError("cadence interval must be a positive integer")
    try:
        zone = ZoneInfo(campaign_timezone)
    except Exception as error:
        raise FollowupValidationError("campaign timezone is invalid") from error
    holidays: set[date] = set()
    for value in holiday_calendar:
        try:
            holidays.add(date.fromisoformat(str(value)))
        except ValueError as error:
            raise FollowupValidationError("holiday calendar contains an invalid date") from error
    local = sent.astimezone(zone)
    if cadence_strategy == "CALENDAR_DAYS":
        local += timedelta(days=interval_value)
    else:
        remaining = interval_value
        while remaining:
            local += timedelta(days=1)
            if local.weekday() < 5 and local.date() not in holidays:
                remaining -= 1
    return local.astimezone(timezone.utc).replace(microsecond=0)


def initialize_followup_sequence(database: Database, *, gmail_run_id: int, fixture_override: bool = False, cadence_strategy: Optional[str] = None, interval_value: Optional[int] = None, campaign_timezone: str = "UTC", holiday_calendar: Iterable[str] = (), now: Optional[datetime] = None) -> dict[str, Any]:
    """Create one sequence shell from a verified Step 11 fixture run."""
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    current = _now(now)
    config_reasons = _config_reasons(database, fixture_override=True, now=current)
    run = database.connection.execute("SELECT * FROM gmail_draft_creation_runs WHERE id=?", (gmail_run_id,)).fetchone()
    if run is None:
        raise FollowupValidationError("Gmail draft run not found")
    if run["state"] != "READY_FOR_MANUAL_SEND":
        return {"state": "BLOCKED", "blocking_reasons": ["VERIFIED_EXTERNAL_DRAFT_REQUIRED"] + config_reasons}
    mapping = database.connection.execute("SELECT * FROM gmail_external_draft_mappings WHERE run_id=? ORDER BY id DESC LIMIT 1", (gmail_run_id,)).fetchone()
    if mapping is None or mapping["state"] != "READY_FOR_MANUAL_SEND":
        return {"state": "BLOCKED", "blocking_reasons": ["VERIFIED_EXTERNAL_DRAFT_REQUIRED"] + config_reasons}
    if cadence_strategy is not None and cadence_strategy not in FOLLOWUP_CADENCE_STRATEGIES:
        raise FollowupValidationError("cadence strategy is invalid")
    if (cadence_strategy is None) != (interval_value is None):
        raise FollowupValidationError("cadence strategy and interval must be supplied together")
    if cadence_strategy is not None:
        calculate_fixture_due_date(current, cadence_strategy, interval_value, campaign_timezone, holiday_calendar)
    draft = database.connection.execute("SELECT * FROM personalized_drafts WHERE id=?", (run["personalized_draft_id"],)).fetchone()
    if draft is None:
        raise FollowupValidationError("source draft not found")
    source_context = {
        "id": draft["id"], "source_draft_id": draft["id"], "source_draft_version": draft["version"],
        "lead_id": draft["lead_id"], "campaign_id": draft["campaign_id"], "packet_id": draft["packet_id"],
        "recipient_email": run["recipient_email"], "sender_email": run["sender_email"],
        "input_fingerprint": run["input_fingerprint"], "content_hash": run["content_hash"],
    }
    _, source_reasons = _current_source(database, source_context, current)
    if source_reasons:
        return {"state": "BLOCKED", "blocking_reasons": sorted(set(source_reasons + config_reasons))}
    existing = database.connection.execute("SELECT id FROM followup_sequences WHERE source_gmail_run_id=?", (gmail_run_id,)).fetchone()
    if existing is not None:
        return _result(database, existing["id"], reused=True)
    timestamp = _iso(current)
    holiday_json = _json(sorted(str(item) for item in holiday_calendar))
    with database.connection:
        cursor = database.connection.execute(
            """INSERT INTO followup_sequences
            (lead_id,campaign_id,source_gmail_run_id,source_draft_id,source_draft_version,recipient_email,sender_email,content_hash,input_fingerprint,policy_version,offer_version_id,packet_id,state,current_touch_count,max_total_touches,cadence_strategy,interval_value,campaign_timezone,holiday_calendar_json,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (draft["lead_id"], draft["campaign_id"], gmail_run_id, draft["id"], draft["version"], run["recipient_email"], run["sender_email"], run["content_hash"], run["input_fingerprint"], FOLLOWUP_POLICY_VERSION, run["offer_version_id"], run["packet_id"], "AWAITING_MANUAL_SEND", 0, FOLLOWUP_MAX_TOTAL_TOUCHES, cadence_strategy, interval_value, campaign_timezone, holiday_json, timestamp, timestamp),
        )
    sequence_id = int(cursor.lastrowid)
    _transition(database, sequence_id, None, "AWAITING_MANUAL_SEND", "SEQUENCE_INITIALIZED", "Fictional fixture sequence initialized.", now=current, metadata={"policy_version": FOLLOWUP_POLICY_VERSION})
    return _result(database, sequence_id)


def get_followup_sequence(database: Database, *, sequence_id: int) -> dict[str, Any]:
    migrate_step12(database)
    row = _sequence_row(database, sequence_id)
    return {key: row[key] for key in ("id", "lead_id", "campaign_id", "source_gmail_run_id", "source_draft_id", "source_draft_version", "recipient_email", "sender_email", "content_hash", "input_fingerprint", "policy_version", "state", "current_touch_count", "max_total_touches", "cadence_strategy", "interval_value", "campaign_timezone", "holiday_calendar_json", "next_due_at", "out_of_office_return_at", "hard_bounce_count", "close_reason", "created_at", "updated_at")}


def inspect_followup_readiness(database: Database, *, sequence_id: int, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    migrate_step12(database)
    current = _now(now)
    if _is_durable_database(database):
        return {"ready": False, "sequence_id": sequence_id, "blocking_reasons": ["CANONICAL_FOLLOWUP_OPERATION_BLOCKED", "TEST_POLICY_NONPRODUCTION"]}
    try:
        sequence = _sequence_row(database, sequence_id)
    except FollowupValidationError:
        return {"ready": False, "sequence_id": sequence_id, "blocking_reasons": ["SEQUENCE_NOT_FOUND"]}
    reasons = _config_reasons(database, fixture_override=fixture_override, now=current)
    if sequence["state"] in _TERMINAL_STATES:
        reasons.append("TERMINAL_STATE")
    if sequence["state"] == "WAITING_FOR_REPLY" and sequence["next_due_at"] is None and sequence["current_touch_count"] < sequence["max_total_touches"]:
        reasons.append("CADENCE_NOT_SUPPLIED")
    _, source_reasons = _current_source(database, sequence, current)
    reasons.extend(source_reasons)
    return {"ready": not reasons, "sequence_id": sequence_id, "state": sequence["state"], "touch_count": sequence["current_touch_count"], "next_due_at": sequence["next_due_at"], "policy_version": sequence["policy_version"], "blocking_reasons": sorted(set(reasons))}


def _existing_touch(database: Database, sequence_id: int, idempotency_key: str):
    return database.connection.execute("SELECT * FROM followup_sent_touches WHERE sequence_id=? AND idempotency_key=?", (sequence_id, idempotency_key)).fetchone()


def record_manual_send(database: Database, *, sequence_id: int, draft_id: Any, draft_version: int, recipient_email: str, sender_email: str, content_hash: str, sent_at: datetime | str, reviewer_identity: str, reason: str, idempotency_key: str, provider_draft_id: Optional[str] = None, provider_message_id: Optional[str] = None, provider_thread_id: Optional[str] = None, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Record a manual send; this function never performs the send."""
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    current = _now(now)
    sequence = _sequence_row(database, sequence_id)
    if not isinstance(idempotency_key, str) or not idempotency_key.strip():
        raise FollowupValidationError("manual-send idempotency key is required")
    existing = _existing_touch(database, sequence_id, idempotency_key)
    if existing is not None:
        return _result(database, sequence_id, reused=True, touch_number=existing["touch_number"], sent_touch_id=existing["id"])
    config_reasons = _config_reasons(database, fixture_override=True, now=current)
    if config_reasons:
        return _result(database, sequence_id, reasons=config_reasons)
    if reviewer_identity != "OPERATOR":
        raise FollowupValidationError("only OPERATOR may record a manual send")
    if not isinstance(reason, str) or not reason.strip():
        raise FollowupValidationError("manual-send reason is required")
    recipient = _email(recipient_email); sender = _sender(sender_email)
    if recipient != sequence["recipient_email"]:
        raise FollowupValidationError("recipient does not match approved sequence")
    if sender != sequence["sender_email"]:
        raise FollowupValidationError("sender does not match approved sequence")
    if str(draft_id) != str(sequence["source_draft_id"]) or draft_version != sequence["source_draft_version"]:
        raise FollowupValidationError("draft identity or version does not match approved source")
    if content_hash != sequence["content_hash"]:
        raise FollowupValidationError("content hash does not match approved source")
    sent = _parse_time(sent_at)
    if database.is_suppressed(lead_id=sequence["lead_id"], email=recipient):
        _set_state(database, sequence, "SUPPRESSED", "Suppressed lead cannot receive a newly recorded send.", event_type="SEND_BLOCKED_SUPPRESSION", now=current)
        return _result(database, sequence_id, reasons=["SUPPRESSED"])
    if sequence["state"] in _TERMINAL_STATES:
        reason_code = "MAX_TOTAL_TOUCHES_REACHED" if sequence["current_touch_count"] >= sequence["max_total_touches"] else "TERMINAL_STATE"
        return _result(database, sequence_id, reasons=[reason_code])
    if sequence["state"] != "AWAITING_MANUAL_SEND":
        return _result(database, sequence_id, reasons=["AWAITING_MANUAL_SEND_REQUIRED"])
    _, source_reasons = _current_source(database, sequence, current)
    if source_reasons:
        return _result(database, sequence_id, reasons=["STALE_SEND_RECORD"] + source_reasons)
    if sequence["current_touch_count"] >= sequence["max_total_touches"]:
        return _result(database, sequence_id, reasons=["MAX_TOTAL_TOUCHES_REACHED"])
    previous = database.connection.execute("SELECT sent_at FROM followup_sent_touches WHERE sequence_id=? ORDER BY touch_number DESC LIMIT 1", (sequence_id,)).fetchone()
    if previous is not None and sent < _parse_time(previous["sent_at"]):
        raise FollowupValidationError("send timestamp precedes the prior touch")
    touch_number = sequence["current_touch_count"] + 1
    due_at = None
    if sequence["cadence_strategy"] is not None:
        due_at = _iso(calculate_fixture_due_date(sent, sequence["cadence_strategy"], sequence["interval_value"], sequence["campaign_timezone"], json.loads(sequence["holiday_calendar_json"]))) if touch_number < sequence["max_total_touches"] else None
    timestamp = _iso(current)
    new_state = "CLOSED_NO_RESPONSE" if touch_number >= sequence["max_total_touches"] else "WAITING_FOR_REPLY"
    close_reason = "Maximum of three audited sent touches reached without a reply." if new_state == "CLOSED_NO_RESPONSE" else None
    with database.connection:
        cursor = database.connection.execute(
            """INSERT INTO followup_sent_touches(sequence_id,lead_id,touch_number,idempotency_key,outreach_or_draft_id,draft_version,recipient_email,sender_email,content_hash,sent_at,reviewer_identity,reason,policy_version,provider_draft_id,provider_message_id,provider_thread_id,due_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (sequence_id, sequence["lead_id"], touch_number, idempotency_key, str(draft_id), draft_version, recipient, sender, content_hash, _iso(sent), "OPERATOR", " ".join(reason.split())[:500], FOLLOWUP_POLICY_VERSION, provider_draft_id, provider_message_id, provider_thread_id, due_at, timestamp),
        )
        database.connection.execute("UPDATE followup_sequences SET current_touch_count=?,next_due_at=?,close_reason=?,state=?,updated_at=? WHERE id=?", (touch_number, due_at, close_reason, new_state, timestamp, sequence_id))
    _transition(database, sequence_id, sequence["state"], new_state, "MANUAL_SEND_RECORDED", reason, actor="OPERATOR", now=current, metadata={"touch_number": touch_number})
    if touch_number > 1:
        action = database.connection.execute("SELECT * FROM followup_actions WHERE sequence_id=? AND touch_number=?", (sequence_id, touch_number)).fetchone()
        if action is not None:
            with database.connection:
                database.connection.execute("UPDATE followup_actions SET state='CONSUMED' WHERE id=?", (action["id"],))
                database.connection.execute("UPDATE followup_capacity_reservations SET reservation_state='CONSUMED' WHERE action_id=?", (action["id"],))
    return _result(database, sequence_id, touch_number=touch_number, touch_count=touch_number, sent_touch_id=int(cursor.lastrowid), due_at=due_at)


def _cancel_actions(database: Database, sequence_id: int, reason: str, now: datetime) -> int:
    rows = database.connection.execute("SELECT id FROM followup_actions WHERE sequence_id=? AND state IN ('DUE','REQUESTED','READY')", (sequence_id,)).fetchall()
    if not rows:
        return 0
    with database.connection:
        for row in rows:
            database.connection.execute("UPDATE followup_actions SET state='CANCELLED',cancelled_at=?,cancellation_reason=? WHERE id=?", (_iso(now), " ".join(reason.split())[:500], row["id"]))
            database.connection.execute("UPDATE followup_capacity_reservations SET reservation_state='CANCELLED',cancelled_at=? WHERE action_id=? AND reservation_state='RESERVED'", (_iso(now), row["id"]))
    return len(rows)


def process_due_followups(database: Database, *, sequence_id: int, now: Optional[datetime] = None, fixture_override: bool = False, new_outreach_requested: int = 0) -> dict[str, Any]:
    """Reserve at most one future follow-up slot for a sequence that is due."""
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    current = _now(now); sequence = _sequence_row(database, sequence_id)
    reasons = _config_reasons(database, fixture_override=True, require_capacity=True, now=current)
    if reasons:
        return _result(database, sequence_id, reasons=reasons, priority="FOLLOWUP_FIRST", reservation_count=0, new_outreach_slots_reserved=0)
    if sequence["state"] in _TERMINAL_STATES:
        return _result(database, sequence_id, reasons=reasons + ["TERMINAL_STATE"], priority="FOLLOWUP_FIRST", reservation_count=0, new_outreach_slots_reserved=0)
    if database.is_suppressed(lead_id=sequence["lead_id"], email=sequence["recipient_email"]):
        _cancel_actions(database, sequence_id, "Suppression blocks pending follow-up actions.", current)
        _set_state(database, sequence, "SUPPRESSED", "Suppression blocks follow-up processing.", event_type="FOLLOWUP_BLOCKED_SUPPRESSION", now=current)
        return _result(database, sequence_id, reasons=reasons + ["SUPPRESSED"], priority="FOLLOWUP_FIRST", reservation_count=0, new_outreach_slots_reserved=0)
    if sequence["current_touch_count"] == 0:
        return _result(database, sequence_id, reasons=reasons + ["PRIOR_SENT_TOUCH_REQUIRED"], priority="FOLLOWUP_FIRST", reservation_count=0, new_outreach_slots_reserved=0)
    if sequence["next_due_at"] is None:
        return _result(database, sequence_id, reasons=reasons + ["CADENCE_NOT_SUPPLIED"], priority="FOLLOWUP_FIRST", reservation_count=0, new_outreach_slots_reserved=0)
    if current < _parse_time(sequence["next_due_at"]):
        return _result(database, sequence_id, reasons=reasons + ["NOT_DUE_YET"], priority="FOLLOWUP_FIRST", reservation_count=0, new_outreach_slots_reserved=0)
    _set_state(database, sequence, "FOLLOWUP_ELIGIBILITY_PENDING", "Due follow-up is entering deterministic eligibility checks.", event_type="FOLLOWUP_ELIGIBILITY_STARTED", now=current)
    sequence = _sequence_row(database, sequence_id)
    _, source_reasons = _current_source(database, sequence, current)
    if source_reasons:
        _cancel_actions(database, sequence_id, "Stale source invalidated the pending follow-up request.", current)
        _set_state(database, sequence, "HOLD_FOR_REVIEW", "Stale source invalidated follow-up eligibility.", event_type="FOLLOWUP_INVALIDATED_STALE_SOURCE", now=current)
        return _result(database, sequence_id, reasons=reasons + ["STALE_FOLLOWUP_REQUEST"] + source_reasons, priority="FOLLOWUP_FIRST", reservation_count=0, new_outreach_slots_reserved=0)
    touch_number = sequence["current_touch_count"] + 1
    existing = database.connection.execute("SELECT * FROM followup_actions WHERE sequence_id=? AND touch_number=?", (sequence_id, touch_number)).fetchone()
    if existing is not None:
        if existing["state"] in {"CANCELLED", "INVALIDATED"}:
            return _result(database, sequence_id, reasons=["STALE_FOLLOWUP_REQUEST"], priority="FOLLOWUP_FIRST", reservation_count=0, new_outreach_slots_reserved=0)
        reservation = database.connection.execute("SELECT COUNT(*) AS count FROM followup_capacity_reservations WHERE action_id=?", (existing["id"],)).fetchone()["count"]
        return _result(database, sequence_id, reused=True, action_id=existing["id"], priority="FOLLOWUP_FIRST", reservation_count=reservation, new_outreach_slots_reserved=0)
    request_fingerprint = sha256(_json({"sequence_id": sequence_id, "touch_number": touch_number, "due_at": sequence["next_due_at"], "input_fingerprint": sequence["input_fingerprint"], "policy": sequence["policy_version"]}).encode()).hexdigest()
    reservation_key = f"fixture-followup-slot:{sequence_id}:{touch_number}"
    timestamp = _iso(current)
    with database.connection:
        action_cursor = database.connection.execute(
            "INSERT INTO followup_actions(sequence_id,action_type,touch_number,state,due_at,request_fingerprint,source_input_fingerprint,policy_version,recipient_email,sender_email,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sequence_id, "FOLLOWUP_DRAFT", touch_number, "DUE", sequence["next_due_at"], request_fingerprint, sequence["input_fingerprint"], sequence["policy_version"], sequence["recipient_email"], sequence["sender_email"], timestamp),
        )
        action_id = int(action_cursor.lastrowid)
        database.connection.execute(
            "INSERT INTO followup_capacity_reservations(sequence_id,action_id,touch_number,reservation_key,reservation_state,policy_version,reserved_at) VALUES (?,?,?,?,'RESERVED',?,?)",
            (sequence_id, action_id, touch_number, reservation_key, FOLLOWUP_POLICY_VERSION, timestamp),
        )
        database.connection.execute("UPDATE followup_sequences SET state='FOLLOWUP_DUE',updated_at=? WHERE id=?", (timestamp, sequence_id))
    _transition(database, sequence_id, "FOLLOWUP_ELIGIBILITY_PENDING", "FOLLOWUP_DUE", "FOLLOWUP_BECAME_DUE", "Fixture follow-up became due and reserved one capacity slot.", now=current, metadata={"action_id": action_id, "priority": "FOLLOWUP_FIRST"})
    return _result(database, sequence_id, action_id=action_id, priority="FOLLOWUP_FIRST", reservation_count=1, new_outreach_slots_reserved=0)


def request_followup_draft(database: Database, *, sequence_id: int, action_id: int, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Request future follow-up drafting without creating wording or a draft."""
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    current = _now(now); sequence = _sequence_row(database, sequence_id)
    config_reasons = _config_reasons(database, fixture_override=True, now=current)
    if config_reasons:
        return _result(database, sequence_id, reasons=config_reasons)
    action = database.connection.execute("SELECT * FROM followup_actions WHERE id=? AND sequence_id=?", (action_id, sequence_id)).fetchone()
    if action is None or action["state"] != "DUE":
        return _result(database, sequence_id, reasons=["FOLLOWUP_DUE_ACTION_REQUIRED"])
    _, source_reasons = _current_source(database, sequence, current)
    if source_reasons or database.is_suppressed(lead_id=sequence["lead_id"], email=sequence["recipient_email"]):
        with database.connection:
            database.connection.execute("UPDATE followup_actions SET state='INVALIDATED',cancelled_at=?,cancellation_reason=? WHERE id=?", (_iso(current), "Stale or suppressed follow-up request.", action_id))
            database.connection.execute("UPDATE followup_capacity_reservations SET reservation_state='CANCELLED',cancelled_at=? WHERE action_id=?", (_iso(current), action_id))
        _set_state(database, sequence, "HOLD_FOR_REVIEW", "Follow-up request is stale or suppressed.", event_type="FOLLOWUP_REQUEST_INVALIDATED", now=current)
        return _result(database, sequence_id, reasons=["STALE_FOLLOWUP_REQUEST"] + source_reasons)
    _set_state(database, sequence, "FOLLOWUP_DRAFT_REQUESTED", "Future follow-up draft request is being recorded without message content.", event_type="FOLLOWUP_DRAFT_REQUESTED", now=current, metadata={"action_id": action_id})
    with database.connection:
        database.connection.execute("UPDATE followup_actions SET state='REQUESTED',requested_at=? WHERE id=?", (_iso(current), action_id))
        database.connection.execute("UPDATE followup_sequences SET state='FOLLOWUP_REVIEW_PENDING',updated_at=? WHERE id=?", (_iso(current), sequence_id))
    _transition(database, sequence_id, "FOLLOWUP_DRAFT_REQUESTED", "FOLLOWUP_REVIEW_PENDING", "FOLLOWUP_REVIEW_PENDING", "Future follow-up draft awaits operator review; no message content was generated.", now=current, metadata={"action_id": action_id})
    return _result(database, sequence_id, action_id=action_id, draft_requested=True)


def mark_followup_ready_for_manual_send(database: Database, *, sequence_id: int, action_id: int, reviewer_identity: str, reason: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Record bounded operator review of a content-free future-draft request."""
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    if reviewer_identity != "OPERATOR":
        raise FollowupValidationError("only OPERATOR may approve follow-up review")
    if not isinstance(reason, str) or not reason.strip():
        raise FollowupValidationError("follow-up review reason is required")
    current = _now(now); sequence = _sequence_row(database, sequence_id)
    config_reasons = _config_reasons(database, fixture_override=True, now=current)
    if config_reasons:
        return _result(database, sequence_id, reasons=config_reasons)
    action = database.connection.execute("SELECT * FROM followup_actions WHERE id=? AND sequence_id=?", (action_id, sequence_id)).fetchone()
    if action is None or action["state"] != "REQUESTED":
        return _result(database, sequence_id, reasons=["FOLLOWUP_REVIEW_REQUIRED"])
    with database.connection:
        database.connection.execute("UPDATE followup_actions SET state='READY' WHERE id=?", (action_id,))
        database.connection.execute("UPDATE followup_sequences SET state='AWAITING_MANUAL_SEND',updated_at=? WHERE id=?", (_iso(current), sequence_id))
    _transition(database, sequence_id, sequence["state"], "AWAITING_MANUAL_SEND", "FOLLOWUP_REVIEW_APPROVED", reason, actor="OPERATOR", now=current, metadata={"action_id": action_id})
    return _result(database, sequence_id, action_id=action_id, reviewer_identity="OPERATOR")


def _apply_reply_category(database: Database, sequence, reply, category: str, reason: str, now: datetime, *, actor: str = "SYSTEM") -> dict[str, Any]:
    _cancel_actions(database, sequence["id"], "Reply received; pending follow-up actions cancelled.", now)
    if sequence["state"] in _TERMINAL_STATES and category not in {"OPT_OUT", "HARD_BOUNCE"}:
        return _result(database, sequence["id"], reply_event_id=reply["id"], return_at=reply["return_at"])
    fields: dict[str, Any] = {"next_due_at": None, "close_reason": " ".join(reason.split())[:500]}
    if category == "POSITIVE_INTEREST":
        state = "HUMAN_ACTION_REQUIRED"
    elif category == "QUESTION_OR_NEUTRAL":
        state = "HUMAN_ACTION_REQUIRED"
    elif category == "NOT_INTERESTED":
        if reply["explicit_opt_out"]:
            _record_suppression(database, sequence["lead_id"], sequence["recipient_email"], "Explicit fictional opt-out in reply.", now)
            state = "SUPPRESSED"
        else:
            state = "CLOSED_NOT_INTERESTED"
    elif category == "OPT_OUT":
        _record_suppression(database, sequence["lead_id"], sequence["recipient_email"], "Explicit fictional opt-out reply.", now)
        state = "SUPPRESSED"
    elif category == "OUT_OF_OFFICE":
        state = "OUT_OF_OFFICE_HOLD"
        fields["out_of_office_return_at"] = reply["return_at"]
    elif category == "SOFT_BOUNCE":
        state = "BOUNCE_HOLD"
    elif category == "HARD_BOUNCE":
        _record_suppression(database, sequence["lead_id"], sequence["recipient_email"], "Hard bounce for fictional address.", now)
        fields["hard_bounce_count"] = sequence["hard_bounce_count"] + 1
        state = "SUPPRESSED"
    else:
        state = "HOLD_FOR_REVIEW"
    _set_state(database, sequence, state, reason, event_type="REPLY_CLASSIFIED", actor=actor, now=now, metadata={"category": category}, **fields)
    return _result(database, sequence["id"], reply_event_id=reply["id"], return_at=reply["return_at"])


def _record_suppression(database: Database, lead_id: int, email: str, reason: str, now: datetime) -> None:
    normalized = normalize_email(email); domain = normalize_domain(normalized.rsplit("@", 1)[1])
    with database.connection:
        database.connection.execute("INSERT INTO suppressions(lead_id,email,domain,reason,created_at) VALUES (?,?,?,?,?)", (lead_id, normalized, domain, reason, _iso(now)))


def ingest_fixture_reply(database: Database, *, sequence_id: int, provider: Any, provider_event_id: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Persist one supplied fictional reply event and apply its deterministic category."""
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    current = _now(now); sequence = _sequence_row(database, sequence_id)
    existing = database.connection.execute("SELECT * FROM followup_reply_events WHERE provider_event_id=?", (provider_event_id,)).fetchone()
    if existing is not None:
        return _result(database, sequence_id, reused=True, reply_event_id=existing["id"], return_at=existing["return_at"])
    config_reasons = _config_reasons(database, fixture_override=True, require_reply=True, now=current)
    if config_reasons:
        return _result(database, sequence_id, reasons=config_reasons)
    if not callable(getattr(provider, "get_reply", None)):
        raise FollowupValidationError("reply provider must expose get_reply")
    event = provider.get_reply(provider_event_id)
    if not isinstance(event, Mapping):
        raise FollowupValidationError("fixture reply event not found")
    required = {"provider_event_id", "sequence_id", "lead_id", "outreach_id", "message_id", "thread_id", "received_at", "sender", "recipient", "category", "classification_source", "operator_review_status"}
    if not required <= set(event):
        raise FollowupValidationError("fixture reply event is incomplete")
    if event["provider_event_id"] != provider_event_id or event["sequence_id"] != sequence_id or event["lead_id"] != sequence["lead_id"]:
        raise FollowupValidationError("reply identity does not match sequence")
    if event["category"] not in FOLLOWUP_REPLY_CATEGORIES:
        raise FollowupValidationError("reply category is invalid")
    sender = _email(event["sender"]); recipient = _email(event["recipient"])
    if sender != sequence["recipient_email"] or recipient != sequence["sender_email"]:
        raise FollowupValidationError("reply addresses do not match sequence")
    received = _parse_time(event["received_at"])
    return_at = None
    if event.get("return_at") is not None:
        return_at = _iso(_parse_time(event["return_at"]))
    text = event.get("text")
    if text is None:
        text = event.get("text_hash")
    if not isinstance(text, str) or not text.strip() or len(text) > 1000:
        raise FollowupValidationError("reply text or safe hash is required")
    category = str(event["category"])
    event_text = str(event.get("text", "")).lower()
    explicit_opt_out = bool(event.get("explicit_opt_out")) or any(
        phrase in event_text for phrase in ("do not contact", "don't contact", "no further contact", "stop contacting", "remove me", "unsubscribe")
    )
    review_status = "PENDING" if category == "UNKNOWN" else "NOT_REQUIRED"
    timestamp = _iso(current)
    with database.connection:
        cursor = database.connection.execute(
            "INSERT INTO followup_reply_events(provider_event_id,sequence_id,lead_id,outreach_id,message_id,thread_id,received_at,sender_email,recipient_email,category,safe_text_hash,classification_source,policy_version,operator_review_status,return_at,explicit_opt_out,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (provider_event_id, sequence_id, sequence["lead_id"], str(event["outreach_id"]), str(event["message_id"]), str(event["thread_id"]), _iso(received), sender, recipient, category, sha256(text.encode()).hexdigest(), str(event["classification_source"]), FOLLOWUP_POLICY_VERSION, review_status, return_at, int(explicit_opt_out), timestamp),
        )
    reply = database.connection.execute("SELECT * FROM followup_reply_events WHERE id=?", (cursor.lastrowid,)).fetchone()
    return _apply_reply_category(database, sequence, reply, category, "Fictional reply event applied.", current)


def record_manual_reply_event(
    database: Database,
    *,
    sequence_id: int,
    provider_event_id: str,
    category: str,
    reason: str,
    operator_identity: str,
    safe_reference: str,
    received_at: Optional[datetime | str] = None,
    return_at: Optional[datetime | str] = None,
    fixture_override: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Record operator-supplied reply metadata without reading an inbox."""
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    current = _now(now)
    if operator_identity != "OPERATOR":
        raise FollowupValidationError("only OPERATOR may record a manual reply event")
    if category not in FOLLOWUP_REPLY_CATEGORIES - {"UNKNOWN"}:
        raise FollowupValidationError("manual reply category must be a supported non-UNKNOWN category")
    if not isinstance(provider_event_id, str) or not provider_event_id.strip():
        raise FollowupValidationError("manual event reference is required")
    if not isinstance(reason, str) or not reason.strip():
        raise FollowupValidationError("manual event reason is required")
    if not isinstance(safe_reference, str) or not safe_reference.strip() or len(safe_reference) > 300:
        raise FollowupValidationError("a bounded safe reference is required; raw message text is not accepted")
    sequence = _sequence_row(database, sequence_id)
    existing = database.connection.execute("SELECT id FROM followup_reply_events WHERE provider_event_id=?", (provider_event_id.strip(),)).fetchone()
    if existing is not None:
        return _result(database, sequence_id, reused=True, reply_event_id=existing["id"])
    config_reasons = _config_reasons(database, fixture_override=True, now=current)
    if config_reasons:
        return _result(database, sequence_id, reasons=config_reasons)
    received = _iso(_parse_time(received_at)) if received_at is not None else _iso(current)
    normalized_return_at = _iso(_parse_time(return_at)) if return_at is not None else None
    safe_hash = sha256(safe_reference.strip().encode()).hexdigest()
    timestamp = _iso(current)
    with database.connection:
        cursor = database.connection.execute(
            "INSERT INTO followup_reply_events(provider_event_id,sequence_id,lead_id,outreach_id,message_id,thread_id,received_at,sender_email,recipient_email,category,safe_text_hash,classification_source,policy_version,operator_review_status,return_at,explicit_opt_out,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (provider_event_id.strip(), sequence_id, sequence["lead_id"], "MANUAL_OPERATOR", "manual-" + safe_hash[:16], "manual-" + safe_hash[16:32], received, sequence["recipient_email"], sequence["sender_email"], category, safe_hash, "MANUAL_OPERATOR", FOLLOWUP_POLICY_VERSION, "REVIEWED", normalized_return_at, int(category == "OPT_OUT"), timestamp),
        )
    reply = database.connection.execute("SELECT * FROM followup_reply_events WHERE id=?", (cursor.lastrowid,)).fetchone()
    return _apply_reply_category(database, sequence, reply, category, " ".join(reason.split())[:500], current, actor="OPERATOR")


def classify_fixture_reply(database: Database, *, reply_event_id: int, reviewer_identity: str, category: str, reason: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Apply an explicit operator classification to an UNKNOWN reply."""
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    if reviewer_identity != "OPERATOR":
        raise FollowupValidationError("only OPERATOR may classify a reply")
    if category not in FOLLOWUP_REPLY_CATEGORIES - {"UNKNOWN"}:
        raise FollowupValidationError("operator classification must be a supported non-UNKNOWN category")
    if not isinstance(reason, str) or not reason.strip():
        raise FollowupValidationError("classification reason is required")
    current = _now(now)
    config_reasons = _config_reasons(database, fixture_override=True, require_reply=True, now=current)
    if config_reasons:
        return {"reply_event_id": reply_event_id, "reused": False, "blocking_reasons": config_reasons}
    reply = database.connection.execute("SELECT * FROM followup_reply_events WHERE id=?", (reply_event_id,)).fetchone()
    if reply is None:
        raise FollowupValidationError("reply event not found")
    if reply["category"] != "UNKNOWN" or reply["operator_review_status"] == "REVIEWED":
        return _result(database, reply["sequence_id"], reused=True, reply_event_id=reply_event_id)
    with database.connection:
        database.connection.execute("UPDATE followup_reply_events SET category=?,operator_review_status='REVIEWED',classification_source='OPERATOR_REVIEW' WHERE id=?", (category, reply_event_id))
        database.connection.execute("INSERT INTO followup_operator_classifications(reply_event_id,reviewer_identity,category,policy_version,reason,created_at) VALUES (?,?,?,?,?,?)", (reply_event_id, "OPERATOR", category, FOLLOWUP_POLICY_VERSION, " ".join(reason.split())[:500], _iso(current)))
    sequence = _sequence_row(database, reply["sequence_id"])
    reply = database.connection.execute("SELECT * FROM followup_reply_events WHERE id=?", (reply_event_id,)).fetchone()
    return _apply_reply_category(database, sequence, reply, category, reason, current, actor="OPERATOR")


def cancel_pending_followup_actions(database: Database, *, sequence_id: int, reason: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    if not isinstance(reason, str) or not reason.strip():
        raise FollowupValidationError("cancellation reason is required")
    current = _now(now); sequence = _sequence_row(database, sequence_id)
    config_reasons = _config_reasons(database, fixture_override=True, now=current)
    if config_reasons:
        return _result(database, sequence_id, reasons=config_reasons)
    count = _cancel_actions(database, sequence_id, reason, current)
    if sequence["state"] not in _TERMINAL_STATES:
        _set_state(database, sequence, "HOLD_FOR_REVIEW", reason, event_type="FOLLOWUP_ACTIONS_CANCELLED", actor="OPERATOR", now=current, metadata={"cancelled_actions": count})
    return _result(database, sequence_id, cancelled_actions=count)


def close_no_response_sequence(database: Database, *, sequence_id: int, reason: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    _require_fixture(database, fixture_override)
    migrate_step12(database)
    if not isinstance(reason, str) or not reason.strip():
        raise FollowupValidationError("closure reason is required")
    current = _now(now); sequence = _sequence_row(database, sequence_id)
    config_reasons = _config_reasons(database, fixture_override=True, now=current)
    if config_reasons:
        return _result(database, sequence_id, reasons=config_reasons)
    if sequence["state"] in _TERMINAL_STATES:
        return _result(database, sequence_id, reused=True)
    _cancel_actions(database, sequence_id, reason, current)
    _set_state(database, sequence, "CLOSED_NO_RESPONSE", reason, event_type="SEQUENCE_CLOSED_NO_RESPONSE", actor="OPERATOR", now=current)
    return _result(database, sequence_id)


def summarize_followup_provenance(database: Database, *, sequence_id: int) -> dict[str, Any]:
    migrate_step12(database)
    sequence = _sequence_row(database, sequence_id)
    touches = database.connection.execute("SELECT id,touch_number,outreach_or_draft_id,draft_version,recipient_email,sender_email,content_hash,sent_at,reviewer_identity,reason,policy_version,provider_draft_id,provider_message_id,provider_thread_id,due_at FROM followup_sent_touches WHERE sequence_id=? ORDER BY touch_number", (sequence_id,)).fetchall()
    replies = database.connection.execute("SELECT id,provider_event_id,outreach_id,message_id,thread_id,received_at,sender_email,recipient_email,category,safe_text_hash,classification_source,policy_version,operator_review_status,return_at,explicit_opt_out FROM followup_reply_events WHERE sequence_id=? ORDER BY id", (sequence_id,)).fetchall()
    actions = database.connection.execute("SELECT id,action_type,touch_number,state,due_at,request_fingerprint,source_input_fingerprint,policy_version,recipient_email,sender_email,requested_at,cancelled_at,cancellation_reason FROM followup_actions WHERE sequence_id=? ORDER BY id", (sequence_id,)).fetchall()
    transitions = database.connection.execute("SELECT id,from_state,to_state,event_type,actor,policy_version,reason,safe_metadata_json,created_at FROM followup_lifecycle_transitions WHERE sequence_id=? ORDER BY id", (sequence_id,)).fetchall()
    return {
        "sequence_id": sequence_id, "lead_id": sequence["lead_id"], "campaign_id": sequence["campaign_id"],
        "source_gmail_run_id": sequence["source_gmail_run_id"], "source_draft_id": sequence["source_draft_id"], "source_draft_version": sequence["source_draft_version"],
        "policy_version": sequence["policy_version"], "content_hash": sequence["content_hash"], "input_fingerprint": sequence["input_fingerprint"],
        "state": sequence["state"], "touch_count": sequence["current_touch_count"], "max_total_touches": sequence["max_total_touches"],
        "cadence_strategy": sequence["cadence_strategy"], "interval_value": sequence["interval_value"], "campaign_timezone": sequence["campaign_timezone"],
        "next_due_at": sequence["next_due_at"], "out_of_office_return_at": sequence["out_of_office_return_at"], "hard_bounce_count": sequence["hard_bounce_count"],
        "sent_touches": [dict(row) for row in touches], "reply_events": [dict(row) for row in replies], "actions": [dict(row) for row in actions],
        "transitions": [dict(row) for row in transitions],
    }


__all__ = [
    "FOLLOWUP_CADENCE_STRATEGIES", "FOLLOWUP_MAX_TOTAL_TOUCHES", "FOLLOWUP_POLICY_VERSION", "FOLLOWUP_PROVIDER_MODE", "FOLLOWUP_REPLY_CATEGORIES", "FOLLOWUP_STATES", "STEP12_MIGRATION_VERSION",
    "FixtureReplyProvider", "FollowupBlockedError", "FollowupValidationError", "calculate_fixture_due_date", "cancel_pending_followup_actions", "classify_fixture_reply", "close_no_response_sequence", "get_followup_sequence", "ingest_fixture_reply", "initialize_followup_sequence", "inspect_followup_readiness", "mark_followup_ready_for_manual_send", "migrate_step12", "process_due_followups", "record_manual_send", "record_manual_reply_event", "request_followup_draft", "summarize_followup_provenance",
]
