"""Step 11: fixture-only Gmail draft persistence boundary.

This module intentionally has no Gmail client.  It accepts only an injected
fixture provider with two methods, ``create_draft`` and ``get_draft``.  The
canonical database is fail-closed; ``fixture_override=True`` is accepted only
for a temporary, non-canonical SQLite database used by fictional tests.

The persisted records contain identifiers, hashes, state, provenance, and
bounded audit metadata.  The validated Step 10 body is not copied into the
Step 11 tables.  No send operation is part of this adapter.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Optional

from paldo_os_outbound import DEFAULT_DB_PATH, Database, normalize_email
from step10_drafting import DRAFTING_POLICY_VERSION, get_review_pending_draft, migrate_step10


STEP11_MIGRATION_VERSION = 13
GMAIL_PROVIDER_MODE = "FIXTURE_ONLY"
GMAIL_ALLOWED_ACTIONS = frozenset({"CREATE_DRAFT", "GET_DRAFT"})
GMAIL_ALLOWED_ACTIONS_VALUE = "CREATE_DRAFT,GET_DRAFT"
GMAIL_DRAFT_STATES = frozenset({
    "APPROVED_FOR_DRAFT_CREATION",
    "DRAFT_CREATION_PENDING",
    "EXTERNAL_DRAFT_CREATED",
    "EXTERNAL_DRAFT_VERIFIED",
    "READY_FOR_MANUAL_SEND",
    "HOLD_FOR_RECONCILIATION",
    "BLOCKED",
})
FIXTURE_SENDER_SUFFIX = ".fictional-example.invalid"
HTML_RE = re.compile(r"</?[a-z][^>]*>|<!doctype|<script\b", re.IGNORECASE)
LINK_RE = re.compile(r"(?:https?://|www\.|bit\.ly/|tinyurl\.com/|t\.co/)", re.IGNORECASE)
TRACKING_RE = re.compile(r"(?:tracking|tracking-pixel|pixel|utm_[a-z_]+)", re.IGNORECASE)


class GmailDraftBlockedError(RuntimeError):
    """Raised when a canonical or otherwise unsafe Gmail-draft operation is attempted."""


class GmailDraftValidationError(ValueError):
    """Raised for malformed approval, provider, action, or payload input."""


class FixtureExternalStateUnknown(RuntimeError):
    """Fixture provider exception representing an invocation with unknown outcome."""

    def __init__(self, external_draft_id: Optional[str] = None):
        super().__init__("fixture provider outcome is unknown")
        self.external_draft_id = external_draft_id


class FixtureGmailDraftProvider:
    """Injected local provider exposing draft creation and retrieval only."""

    def __init__(self, *, mutation: Optional[Mapping[str, Any]] = None, interrupt_after_create: bool = False, interrupt_without_id: bool = False):
        self.mutation = dict(mutation or {})
        self.interrupt_after_create = interrupt_after_create
        self.interrupt_without_id = interrupt_without_id
        self.create_calls = 0
        self.get_calls = 0
        self.created: dict[str, dict[str, Any]] = {}

    def create_draft(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.create_calls += 1
        if not isinstance(payload, Mapping):
            raise GmailDraftValidationError("fixture provider payload must be an object")
        external_id = "fixture-gmail-draft-" + str(payload["content_hash"])[:16]
        record = {
            "draft_id": external_id,
            "message_id": "fixture-message-" + str(payload["content_hash"])[16:28],
            "thread_id": "fixture-thread-" + str(payload["content_hash"])[28:40],
            "to": deepcopy(payload["to"]),
            "from": payload["from"],
            "subject": payload["subject"],
            "body": payload["body"],
            "content_hash": payload["content_hash"],
            "status": "DRAFT",
            "cc": [],
            "bcc": [],
            "attachments": [],
            "html": False,
            "provider_timestamp": "2026-09-03T12:00:00+00:00",
        }
        self.created[external_id] = deepcopy(record)
        if self.interrupt_without_id:
            raise FixtureExternalStateUnknown()
        if self.interrupt_after_create:
            raise FixtureExternalStateUnknown(external_id)
        return deepcopy(record)

    def get_draft(self, external_draft_id: str) -> Optional[dict[str, Any]]:
        self.get_calls += 1
        record = self.created.get(external_draft_id)
        if record is None:
            return None
        result = deepcopy(record)
        result.update(deepcopy(self.mutation))
        return result


_SCHEMA = """
CREATE TABLE IF NOT EXISTS gmail_draft_creation_runs (
    id INTEGER PRIMARY KEY,
    personalized_draft_id INTEGER NOT NULL REFERENCES personalized_drafts(id) ON DELETE RESTRICT,
    draft_version INTEGER NOT NULL CHECK (draft_version > 0),
    state TEXT NOT NULL CHECK (state IN ('APPROVED_FOR_DRAFT_CREATION','DRAFT_CREATION_PENDING','EXTERNAL_DRAFT_CREATED','EXTERNAL_DRAFT_VERIFIED','READY_FOR_MANUAL_SEND','HOLD_FOR_RECONCILIATION','BLOCKED')),
    idempotency_key TEXT NOT NULL UNIQUE,
    content_hash TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    sender_email TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    offer_version_id INTEGER NOT NULL,
    packet_id INTEGER NOT NULL,
    approval_id INTEGER,
    external_draft_id TEXT,
    error_category TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS gmail_draft_run_draft_version_idx ON gmail_draft_creation_runs(personalized_draft_id, draft_version);
CREATE INDEX IF NOT EXISTS gmail_draft_runs_state_idx ON gmail_draft_creation_runs(state, updated_at);

CREATE TABLE IF NOT EXISTS gmail_draft_creation_approvals (
    id INTEGER PRIMARY KEY,
    personalized_draft_id INTEGER NOT NULL REFERENCES personalized_drafts(id) ON DELETE RESTRICT,
    draft_version INTEGER NOT NULL CHECK (draft_version > 0),
    reviewer_identity TEXT NOT NULL CHECK (reviewer_identity = 'OPERATOR'),
    reason TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    sender_email TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('APPROVED','INVALIDATED')),
    created_at TEXT NOT NULL,
    invalidated_at TEXT
);
CREATE INDEX IF NOT EXISTS gmail_draft_approvals_draft_idx ON gmail_draft_creation_approvals(personalized_draft_id, draft_version, status);

CREATE TABLE IF NOT EXISTS gmail_draft_provider_attempts (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES gmail_draft_creation_runs(id) ON DELETE CASCADE,
    action TEXT NOT NULL CHECK (action IN ('CREATE_DRAFT','GET_DRAFT')),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    status TEXT NOT NULL CHECK (status IN ('STARTED','SUCCEEDED','FAILED','UNKNOWN_EXTERNAL_STATE')),
    error_category TEXT,
    safe_detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (run_id, action, attempt_number)
);
CREATE INDEX IF NOT EXISTS gmail_draft_attempts_run_idx ON gmail_draft_provider_attempts(run_id, action, attempt_number);

CREATE TABLE IF NOT EXISTS gmail_external_draft_mappings (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES gmail_draft_creation_runs(id) ON DELETE RESTRICT,
    personalized_draft_id INTEGER NOT NULL REFERENCES personalized_drafts(id) ON DELETE RESTRICT,
    draft_version INTEGER NOT NULL CHECK (draft_version > 0),
    idempotency_key TEXT NOT NULL UNIQUE,
    external_draft_id TEXT NOT NULL UNIQUE,
    message_id TEXT,
    thread_id TEXT,
    provider_status TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    recipient_email TEXT NOT NULL,
    sender_email TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('EXTERNAL_DRAFT_CREATED','EXTERNAL_DRAFT_VERIFIED','READY_FOR_MANUAL_SEND','HOLD_FOR_RECONCILIATION')),
    mapped_at TEXT NOT NULL,
    verified_at TEXT
);
CREATE INDEX IF NOT EXISTS gmail_mapping_run_idx ON gmail_external_draft_mappings(run_id, mapped_at);

CREATE TABLE IF NOT EXISTS gmail_draft_verification_results (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES gmail_draft_creation_runs(id) ON DELETE CASCADE,
    mapping_id INTEGER REFERENCES gmail_external_draft_mappings(id) ON DELETE RESTRICT,
    verification_state TEXT NOT NULL CHECK (verification_state IN ('PASS','FAIL')),
    mismatch_codes_json TEXT NOT NULL,
    safe_details_json TEXT NOT NULL,
    verified_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS gmail_verification_run_idx ON gmail_draft_verification_results(run_id, verified_at);

CREATE TABLE IF NOT EXISTS gmail_draft_reconciliation_events (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES gmail_draft_creation_runs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    external_draft_id TEXT,
    safe_detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS gmail_reconciliation_run_idx ON gmail_draft_reconciliation_events(run_id, created_at);
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


def _is_durable_database(database: Database) -> bool:
    try:
        return database.path.resolve() == DEFAULT_DB_PATH.resolve()
    except (AttributeError, OSError):
        return False


def _require_fixture(database: Database, fixture_override: bool) -> None:
    if _is_durable_database(database):
        raise GmailDraftBlockedError("canonical Gmail-draft persistence is blocked")
    if not fixture_override:
        raise GmailDraftBlockedError("fixture_override=True is required for Step 11")


def migrate_step11(database_or_path: Database | str | Path) -> int:
    """Apply Step 10 prerequisites and the additive, idempotent Step 11 schema."""
    database = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    migrate_step10(database)
    with database.connection:
        database.connection.executescript(_SCHEMA)
        database.connection.executemany(
            "INSERT OR IGNORE INTO system_config (key,value,value_type) VALUES (?,?,?)",
            (
                ("gmail_draft_integration_enabled", "0", "integer"),
                ("gmail_draft_daily_cap", "0", "integer"),
                ("gmail_provider_mode", GMAIL_PROVIDER_MODE, "text"),
                ("gmail_account_configured", "0", "integer"),
                ("gmail_send_enabled", "0", "integer"),
                ("gmail_allowed_actions", GMAIL_ALLOWED_ACTIONS_VALUE, "text"),
            ),
        )
        database.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations (version,name,applied_at) VALUES (?,?,?)",
            (STEP11_MIGRATION_VERSION, "step11_fixture_gmail_draft_persistence_boundary", _iso()),
        )
    return STEP11_MIGRATION_VERSION


def _safe_email(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        normalized = normalize_email(value)
    except (TypeError, ValueError):
        return None
    return normalized if re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", normalized) else None


def _fixture_sender(value: Any) -> Optional[str]:
    email = _safe_email(value)
    if email is None:
        return None
    domain = email.rsplit("@", 1)[1]
    if domain != "fictional-example.invalid" and not domain.endswith(FIXTURE_SENDER_SUFFIX):
        return None
    return email


def _draft_row(database: Database, draft_id: int):
    row = database.connection.execute("SELECT * FROM personalized_drafts WHERE id=?", (draft_id,)).fetchone()
    if row is None:
        raise GmailDraftValidationError("Step 10 draft not found")
    return row


def _content_hash(subject: str, body: str, recipient: str, sender: str) -> str:
    canonical = _json({"body": body, "from": sender, "subject": subject, "to": [recipient]})
    return hashlib.sha256(canonical.encode()).hexdigest()


def _idempotency_key(draft_version: int, recipient: str, sender: str, content_hash: str) -> str:
    return hashlib.sha256(_json({"version": draft_version, "recipient": recipient, "sender": sender, "content_hash": content_hash}).encode()).hexdigest()


def _mime(recipient: str, sender: str, subject: str, body: str, content_hash: str) -> str:
    return f"To: {recipient}\nFrom: {sender}\nSubject: {subject}\nX-Paldo-Content-Hash: {content_hash}\n\n{body}"


def _approved_recipient(database: Database, *, lead_id: int, draft_id: Optional[int] = None) -> Optional[str]:
    if draft_id is not None:
        run = database.connection.execute(
            "SELECT recipient_email FROM gmail_draft_creation_runs WHERE personalized_draft_id=? ORDER BY id DESC LIMIT 1",
            (draft_id,),
        ).fetchone()
        if run is not None and run["recipient_email"]:
            return _safe_email(run["recipient_email"])
    if database._table_has_column("lead_decision_makers", "public_business_email"):
        owner = database.connection.execute(
            "SELECT public_business_email FROM lead_decision_makers WHERE lead_id=? AND verification_status='VERIFIED' AND public_business_email IS NOT NULL ORDER BY id LIMIT 1",
            (lead_id,),
        ).fetchone()
        if owner is not None:
            return _safe_email(owner["public_business_email"])
    lead = database.get_lead(lead_id)
    return _safe_email(lead["email"] if lead else None)


def _payload_from_row(row, recipient: str, sender: str) -> dict[str, Any]:
    content_hash = _content_hash(row["subject"], row["body"], recipient, sender)
    return {
        "to": [recipient], "from": sender, "subject": row["subject"], "body": row["body"],
        "content_hash": content_hash, "mime": _mime(recipient, sender, row["subject"], row["body"], content_hash),
    }


def validate_gmail_draft_payload(payload: Any, *, expected_to: str, expected_from: str, expected_subject: str, expected_body: str, expected_content_hash: str, allowed_links: Optional[Iterable[str]] = None) -> dict[str, Any]:
    """Validate the exact bounded fixture payload before provider invocation."""
    errors: list[str] = []
    if not isinstance(payload, Mapping):
        return {"valid": False, "error_codes": ["PAYLOAD_OBJECT_REQUIRED"]}
    allowed = {"to", "from", "subject", "body", "content_hash", "mime"}
    if set(payload) != allowed:
        errors.append("UNEXPECTED_FIELDS")
    if payload.get("to") != [expected_to]:
        errors.append("RECIPIENT_MISMATCH" if isinstance(payload.get("to"), list) and len(payload.get("to", [])) == 1 else "SINGLE_RECIPIENT_REQUIRED")
    if payload.get("from") != expected_from:
        errors.append("SENDER_MISMATCH")
    if not isinstance(payload.get("subject"), str) or not payload.get("subject", "").strip():
        errors.append("SUBJECT_REQUIRED")
    elif payload["subject"] != expected_subject:
        errors.append("SUBJECT_MISMATCH")
    if not isinstance(payload.get("body"), str) or not payload.get("body", "").strip():
        errors.append("BODY_REQUIRED")
    elif payload["body"] != expected_body:
        errors.append("BODY_MISMATCH")
    body = str(payload.get("body") or "")
    subject = str(payload.get("subject") or "")
    if HTML_RE.search(body) or HTML_RE.search(subject):
        errors.append("HTML_FORBIDDEN")
    allowed = {str(link) for link in (allowed_links or ()) if isinstance(link, str) and link}
    links = re.findall(r"(?:https?://|www\.)[^\s)\]>]+", body) + re.findall(r"(?:https?://|www\.)[^\s)\]>]+", subject)
    if links and (not allowed or any(link not in allowed for link in links)):
        errors.append("LINK_FORBIDDEN")
    if TRACKING_RE.search(body) or TRACKING_RE.search(subject):
        errors.append("TRACKING_FORBIDDEN")
    if payload.get("content_hash") != expected_content_hash:
        errors.append("CONTENT_HASH_MISMATCH")
    expected_mime = _mime(expected_to, expected_from, expected_subject, expected_body, expected_content_hash)
    if payload.get("mime") != expected_mime:
        errors.append("MIME_MISMATCH")
    if any(marker.casefold() in str(payload.get("mime") or "").casefold() for marker in ("Cc:", "Bcc:", "Content-Type: text/html", "attachment", "Reply-To:")):
        errors.append("UNSAFE_MIME_FIELDS")
    return {"valid": not errors, "error_codes": sorted(set(errors))}


def _config_reasons(database: Database, *, fixture_override: bool, now: datetime) -> list[str]:
    config = database.read_config()
    reasons: list[str] = []
    if not fixture_override:
        reasons.extend(("FIXTURE_OVERRIDE_REQUIRED", "TEST_POLICY_NONPRODUCTION"))
    if config.get("system_state") != "ACTIVE":
        reasons.append("SYSTEM_PAUSED")
    if not config.get("gmail_draft_integration_enabled"):
        reasons.append("GMAIL_INTEGRATION_DISABLED")
    if int(config.get("gmail_draft_daily_cap", 0) or 0) <= 0:
        reasons.append("GMAIL_DRAFT_DAILY_CAP_ZERO")
    if config.get("gmail_provider_mode") != GMAIL_PROVIDER_MODE:
        reasons.append("GMAIL_PROVIDER_MODE_UNSAFE")
    if not config.get("gmail_account_configured"):
        reasons.append("GMAIL_ACCOUNT_NOT_CONFIGURED")
    if config.get("gmail_send_enabled") != 0:
        reasons.append("GMAIL_SEND_MUST_REMAIN_DISABLED")
    if config.get("gmail_allowed_actions") != GMAIL_ALLOWED_ACTIONS_VALUE:
        reasons.append("GMAIL_ACTION_ALLOWLIST_UNSAFE")
    start = _iso(now.replace(hour=0, minute=0, second=0))
    cap = int(config.get("gmail_draft_daily_cap", 0) or 0)
    created_today = database.connection.execute("SELECT COUNT(*) FROM gmail_external_draft_mappings WHERE mapped_at>=?", (start,)).fetchone()[0]
    if cap > 0 and created_today >= cap:
        reasons.append("GMAIL_DRAFT_DAILY_CAP_REACHED")
    return reasons


def _step10_readiness(database: Database, draft_id: int, *, sender_email: str, fixture_override: bool, now: datetime) -> tuple[dict[str, Any], list[str]]:
    row = _draft_row(database, draft_id)
    reasons: list[str] = []
    if row["state"] != "REVIEW_PENDING":
        reasons.append("STEP10_REVIEW_PENDING_REQUIRED")
    validation = database.connection.execute(
        "SELECT validation_state FROM draft_validation_results WHERE draft_id=? ORDER BY id DESC LIMIT 1", (draft_id,)
    ).fetchone()
    if validation is None or validation["validation_state"] != "PASS":
        reasons.append("STEP10_VALIDATION_REQUIRED")
    sender = _fixture_sender(sender_email)
    if sender is None:
        reasons.append("FIXTURE_SENDER_REQUIRED")
    evidence_ids = json.loads(row["evidence_ids_json"])
    run = database.connection.execute("SELECT input_json FROM drafting_runs WHERE id=?", (row["run_id"],)).fetchone()
    run_input = {} if run is None else json.loads(run["input_json"])
    message_policy_context = run_input.get("message_policy")
    from step10_drafting import preview_bounded_drafting_input
    preview = preview_bounded_drafting_input(
        database, lead_id=row["lead_id"], campaign_id=row["campaign_id"], packet_id=row["packet_id"], evidence_ids=None, message_policy_context=message_policy_context, now=now,
    )
    reasons.extend(preview["blocking_reasons"])
    if preview.get("input_fingerprint") != row["input_fingerprint"]:
        reasons.append("DRAFT_INPUT_FINGERPRINT_CHANGED")
    active_approval = database.connection.execute(
        "SELECT * FROM gmail_draft_creation_approvals WHERE personalized_draft_id=? AND status='APPROVED' ORDER BY id DESC LIMIT 1",
        (draft_id,),
    ).fetchone()
    sender = _fixture_sender(sender_email)
    lead = database.get_lead(row["lead_id"])
    recipient = _safe_email(
        active_approval["recipient_email"] if active_approval is not None else lead["email"] if lead else None
    )
    if active_approval is not None and sender and recipient:
        if active_approval["content_hash"] != _content_hash(row["subject"], row["body"], recipient, sender):
            reasons.extend(("DRAFT_CONTENT_CHANGED", "APPROVAL_INVALIDATED"))
        if active_approval["input_fingerprint"] != row["input_fingerprint"]:
            reasons.extend(("DRAFT_INPUT_FINGERPRINT_CHANGED", "APPROVAL_INVALIDATED"))
    reasons.extend(_config_reasons(database, fixture_override=fixture_override, now=now))
    return row, sorted(set(reasons))


def inspect_gmail_draft_readiness(database: Database, *, draft_id: int, sender_email: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Inspect all fixture/canonical gates without invoking a provider."""
    migrate_step11(database)
    current = _now(now)
    if _is_durable_database(database) and not fixture_override:
        return {"ready": False, "draft_id": draft_id, "blocking_reasons": ["CANONICAL_GMAIL_DRAFT_OPERATION_BLOCKED", "FIXTURE_OVERRIDE_REQUIRED"]}
    if _is_durable_database(database):
        raise GmailDraftBlockedError("canonical Gmail-draft readiness inspection is blocked")
    try:
        row, reasons = _step10_readiness(database, draft_id, sender_email=sender_email, fixture_override=fixture_override, now=current)
    except GmailDraftValidationError as error:
        return {"ready": False, "draft_id": draft_id, "blocking_reasons": ["DRAFT_NOT_FOUND"], "detail": str(error)}
    return {
        "ready": not reasons, "draft_id": draft_id, "version": row["version"], "state": row["state"],
        "blocking_reasons": reasons, "policy_version": row["prompt_version"], "input_fingerprint": row["input_fingerprint"],
    }


def preview_gmail_draft_payload(database: Database, *, draft_id: int, sender_email: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Return a bounded deterministic payload, never a live provider object."""
    migrate_step11(database)
    if _is_durable_database(database):
        raise GmailDraftBlockedError("canonical Gmail-draft payload preview is blocked")
    current = _now(now)
    row, reasons = _step10_readiness(database, draft_id, sender_email=sender_email, fixture_override=fixture_override, now=current)
    sender = _fixture_sender(sender_email)
    recipient = _approved_recipient(database, lead_id=row["lead_id"], draft_id=draft_id)
    if recipient is None:
        reasons.append("VERIFIED_RECIPIENT_REQUIRED")
    payload = None
    run_input_row = database.connection.execute("SELECT input_json FROM drafting_runs WHERE id=?", (row["run_id"],)).fetchone()
    run_input = {} if run_input_row is None else json.loads(run_input_row["input_json"])
    message_policy = run_input.get("message_policy")
    allowed_links = [] if not isinstance(message_policy, Mapping) else [value for value in [message_policy.get("case_study_url")] if isinstance(value, str)]
    if not reasons and sender and recipient:
        payload = _payload_from_row(row, recipient, sender)
        check = validate_gmail_draft_payload(payload, expected_to=recipient, expected_from=sender, expected_subject=row["subject"], expected_body=row["body"], expected_content_hash=payload["content_hash"], allowed_links=allowed_links)
        if not check["valid"]:
            reasons.extend(check["error_codes"])
    content_hash = None if payload is None else payload["content_hash"]
    return {
        "ready": not reasons, "draft_id": draft_id, "version": row["version"], "payload": payload if not reasons else None,
        "content_hash": content_hash, "idempotency_key": None if not content_hash or not sender or not recipient else _idempotency_key(row["version"], recipient, sender, content_hash),
        "blocking_reasons": sorted(set(reasons)), "policy_version": row["prompt_version"],
    }


def _result_for_run(database: Database, run_id: int, *, reused: bool = False, reasons: Optional[list[str]] = None, error_category: Optional[str] = None) -> dict[str, Any]:
    run = database.connection.execute("SELECT * FROM gmail_draft_creation_runs WHERE id=?", (run_id,)).fetchone()
    if run is None:
        raise GmailDraftValidationError("Gmail-draft creation run not found")
    mapping = database.connection.execute("SELECT * FROM gmail_external_draft_mappings WHERE run_id=? ORDER BY id DESC LIMIT 1", (run_id,)).fetchone()
    return {
        "run_id": run_id, "approval_id": run["approval_id"], "draft_id": run["personalized_draft_id"], "version": run["draft_version"],
        "state": run["state"], "idempotency_key": run["idempotency_key"], "content_hash": run["content_hash"],
        "external_draft_id": None if mapping is None else mapping["external_draft_id"],
        "message_id": None if mapping is None else mapping["message_id"], "thread_id": None if mapping is None else mapping["thread_id"],
        "provider_status": None if mapping is None else mapping["provider_status"], "reused": reused,
        "blocking_reasons": reasons or [], "mismatch_codes": reasons or [], "error_category": error_category or run["error_category"],
    }


def approve_gmail_draft_creation(database: Database, *, draft_id: int, reviewer_identity: str, recipient_email: str, sender_email: str, reason: str, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Record the sole approval needed to create one fixture draft."""
    _require_fixture(database, fixture_override)
    migrate_step11(database)
    if reviewer_identity != "OPERATOR":
        raise GmailDraftValidationError("only reviewer identity OPERATOR may approve")
    if not isinstance(reason, str) or not reason.strip():
        raise GmailDraftValidationError("approval reason is required")
    current = _now(now)
    row, reasons = _step10_readiness(database, draft_id, sender_email=sender_email, fixture_override=True, now=current)
    expected_recipient = _safe_email(database.get_lead(row["lead_id"])["email"] if database.get_lead(row["lead_id"]) else None)
    recipient = _safe_email(recipient_email)
    sender = _fixture_sender(sender_email)
    owner_email_match = False
    if recipient is not None and database._table_has_column("lead_decision_makers", "public_business_email"):
        owner_email_match = database.connection.execute(
            "SELECT 1 FROM lead_decision_makers WHERE lead_id=? AND verification_status='VERIFIED' AND public_business_email=? LIMIT 1",
            (row["lead_id"], recipient),
        ).fetchone() is not None
    if (expected_recipient is None or recipient != expected_recipient) and not owner_email_match:
        reasons.append("RECIPIENT_MISMATCH")
    if not reasons and sender and recipient:
        content_hash = _content_hash(row["subject"], row["body"], recipient, sender)
        active_old = database.connection.execute(
            "SELECT * FROM gmail_draft_creation_approvals WHERE personalized_draft_id=? AND status='APPROVED' ORDER BY id DESC LIMIT 1", (draft_id,)
        ).fetchone()
        if active_old is not None and (active_old["content_hash"] != content_hash or active_old["recipient_email"] != recipient or active_old["sender_email"] != sender or active_old["input_fingerprint"] != row["input_fingerprint"]):
            reasons.extend(("DRAFT_CONTENT_CHANGED", "APPROVAL_INVALIDATED"))
    if reasons:
        return {"state": "BLOCKED", "draft_id": draft_id, "blocking_reasons": sorted(set(reasons)), "policy_version": row["prompt_version"]}
    content_hash = _content_hash(row["subject"], row["body"], recipient, sender)
    key = _idempotency_key(row["version"], recipient, sender, content_hash)
    existing = database.connection.execute(
        "SELECT a.*, r.id AS run_id, r.state AS run_state FROM gmail_draft_creation_approvals a JOIN gmail_draft_creation_runs r ON r.approval_id=a.id WHERE a.personalized_draft_id=? AND a.status='APPROVED' AND a.content_hash=? AND a.input_fingerprint=?",
        (draft_id, content_hash, row["input_fingerprint"]),
    ).fetchone()
    if existing is not None:
        return {"approval_id": existing["id"], "run_id": existing["run_id"], "draft_id": draft_id, "version": row["version"], "state": existing["run_state"], "reviewer_identity": "OPERATOR", "content_hash": content_hash, "idempotency_key": key, "reused": True}
    timestamp = _iso(current)
    with database.connection:
        approval_cursor = database.connection.execute(
            "INSERT INTO gmail_draft_creation_approvals (personalized_draft_id,draft_version,reviewer_identity,reason,content_hash,input_fingerprint,recipient_email,sender_email,policy_version,status,created_at) VALUES (?,?,?,?,?,?,?,?,?,'APPROVED',?)",
            (draft_id, row["version"], "OPERATOR", " ".join(reason.split())[:500], content_hash, row["input_fingerprint"], recipient, sender, row["prompt_version"], timestamp),
        )
        approval_id = int(approval_cursor.lastrowid)
        run_cursor = database.connection.execute(
            "INSERT INTO gmail_draft_creation_runs (personalized_draft_id,draft_version,state,idempotency_key,content_hash,input_fingerprint,recipient_email,sender_email,policy_version,offer_version_id,packet_id,approval_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (draft_id, row["version"], "APPROVED_FOR_DRAFT_CREATION", key, content_hash, row["input_fingerprint"], recipient, sender, row["prompt_version"], row["offer_version_id"], row["packet_id"], approval_id, timestamp, timestamp),
        )
    return {"approval_id": approval_id, "run_id": int(run_cursor.lastrowid), "draft_id": draft_id, "version": row["version"], "state": "APPROVED_FOR_DRAFT_CREATION", "reviewer_identity": "OPERATOR", "content_hash": content_hash, "idempotency_key": key, "reused": False}


def invoke_gmail_provider_action(provider: Any, action: str, payload: Any) -> Any:
    """Dispatch only the two explicit fixture actions."""
    if action not in GMAIL_ALLOWED_ACTIONS:
        raise GmailDraftValidationError("provider action is not allowlisted")
    method_name = {"CREATE_DRAFT": "create_draft", "GET_DRAFT": "get_draft"}[action]
    method = getattr(provider, method_name, None)
    if not callable(method):
        raise GmailDraftValidationError("provider is missing the allowlisted action")
    return method(payload)


def _invalidate_if_changed(database: Database, run, row, current_hash: str) -> Optional[dict[str, Any]]:
    approval = database.connection.execute("SELECT * FROM gmail_draft_creation_approvals WHERE id=?", (run["approval_id"],)).fetchone()
    changed = approval is None or approval["status"] != "APPROVED" or approval["content_hash"] != current_hash or approval["input_fingerprint"] != row["input_fingerprint"] or approval["recipient_email"] != run["recipient_email"] or approval["sender_email"] != run["sender_email"]
    if not changed:
        return None
    with database.connection:
        if approval is not None and approval["status"] == "APPROVED":
            database.connection.execute("UPDATE gmail_draft_creation_approvals SET status='INVALIDATED', invalidated_at=? WHERE id=?", (_iso(), approval["id"]))
        database.connection.execute("UPDATE gmail_draft_creation_runs SET state='BLOCKED', error_category='APPROVAL_INVALIDATED', updated_at=? WHERE id=?", (_iso(), run["id"]))
    return _result_for_run(database, run["id"], reasons=["APPROVAL_INVALIDATED", "DRAFT_CONTENT_CHANGED"])


def _start_attempt(database: Database, run_id: int, action: str, now: datetime) -> int:
    existing = database.connection.execute("SELECT COALESCE(MAX(attempt_number),0)+1 FROM gmail_draft_provider_attempts WHERE run_id=? AND action=?", (run_id, action)).fetchone()[0]
    with database.connection:
        cursor = database.connection.execute("INSERT INTO gmail_draft_provider_attempts (run_id,action,attempt_number,status,created_at) VALUES (?,?,?,'STARTED',?)", (run_id, action, existing, _iso(now)))
    return int(cursor.lastrowid)


def _finish_attempt(database: Database, attempt_id: int, status: str, now: datetime, *, error_category: Optional[str] = None, detail: Optional[Mapping[str, Any]] = None) -> None:
    with database.connection:
        database.connection.execute("UPDATE gmail_draft_provider_attempts SET status=?,error_category=?,safe_detail_json=?,completed_at=? WHERE id=?", (status, error_category, _json(detail or {}), _iso(now), attempt_id))


def _run_current(database: Database, run_id: int):
    run = database.connection.execute("SELECT * FROM gmail_draft_creation_runs WHERE id=?", (run_id,)).fetchone()
    if run is None:
        raise GmailDraftValidationError("Gmail-draft creation run not found")
    return run


def _record_mapping(database: Database, run, external: Mapping[str, Any], now: datetime) -> int:
    required = {"draft_id", "message_id", "thread_id", "status"}
    if not isinstance(external, Mapping) or not required <= set(external) or not external.get("draft_id"):
        raise GmailDraftValidationError("provider did not return a bounded draft record")
    with database.connection:
        cursor = database.connection.execute(
            "INSERT INTO gmail_external_draft_mappings (run_id,personalized_draft_id,draft_version,idempotency_key,external_draft_id,message_id,thread_id,provider_status,content_hash,recipient_email,sender_email,state,mapped_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,'EXTERNAL_DRAFT_CREATED',?)",
            (run["id"], run["personalized_draft_id"], run["draft_version"], run["idempotency_key"], external["draft_id"], external.get("message_id"), external.get("thread_id"), external["status"], run["content_hash"], run["recipient_email"], run["sender_email"], _iso(now)),
        )
        database.connection.execute("UPDATE gmail_draft_creation_runs SET state='EXTERNAL_DRAFT_CREATED',external_draft_id=?,updated_at=? WHERE id=?", (external["draft_id"], _iso(now), run["id"]))
    return int(cursor.lastrowid)


def create_fixture_gmail_draft(database: Database, *, run_id: int, provider: Any, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Create one fictional external draft after approval, never a message."""
    _require_fixture(database, fixture_override)
    migrate_step11(database)
    current = _now(now)
    run = _run_current(database, run_id)
    existing_mapping = database.connection.execute("SELECT * FROM gmail_external_draft_mappings WHERE run_id=?", (run_id,)).fetchone()
    if existing_mapping is not None:
        return _result_for_run(database, run_id, reused=True)
    if run["state"] in {"HOLD_FOR_RECONCILIATION", "BLOCKED"}:
        return _result_for_run(database, run_id)
    row = _draft_row(database, run["personalized_draft_id"])
    payload_preview = preview_gmail_draft_payload(database, draft_id=row["id"], sender_email=run["sender_email"], fixture_override=True, now=current)
    current_hash = payload_preview.get("content_hash")
    if not payload_preview["ready"] or current_hash is None:
        invalidated = _invalidate_if_changed(database, run, row, current_hash or "")
        if invalidated is not None:
            return invalidated
        with database.connection:
            database.connection.execute("UPDATE gmail_draft_creation_runs SET state='BLOCKED',error_category='READINESS_BLOCKED',updated_at=? WHERE id=?", (_iso(current), run_id))
        return _result_for_run(database, run_id, reasons=payload_preview["blocking_reasons"])
    invalidated = _invalidate_if_changed(database, run, row, current_hash)
    if invalidated is not None:
        return invalidated
    if run["state"] != "APPROVED_FOR_DRAFT_CREATION":
        return _result_for_run(database, run_id)
    attempt_id = _start_attempt(database, run_id, "CREATE_DRAFT", current)
    with database.connection:
        database.connection.execute("UPDATE gmail_draft_creation_runs SET state='DRAFT_CREATION_PENDING',updated_at=? WHERE id=?", (_iso(current), run_id))
    try:
        external = invoke_gmail_provider_action(provider, "CREATE_DRAFT", payload_preview["payload"])
    except Exception as error:
        external_id = getattr(error, "external_draft_id", None)
        _finish_attempt(database, attempt_id, "UNKNOWN_EXTERNAL_STATE", current, error_category="UNKNOWN_EXTERNAL_STATE", detail={"exception_type": type(error).__name__})
        with database.connection:
            database.connection.execute("UPDATE gmail_draft_creation_runs SET state='HOLD_FOR_RECONCILIATION',external_draft_id=?,error_category='UNKNOWN_EXTERNAL_STATE',updated_at=? WHERE id=?", (external_id, _iso(current), run_id))
            database.connection.execute("INSERT INTO gmail_draft_reconciliation_events (run_id,event_type,outcome,external_draft_id,safe_detail_json,created_at) VALUES (?,?,?,?,?,?)", (run_id, "CREATE_INVOCATION", "UNKNOWN_EXTERNAL_STATE", external_id, _json({"attempt_id": attempt_id}), _iso(current)))
        return _result_for_run(database, run_id, error_category="UNKNOWN_EXTERNAL_STATE")
    _finish_attempt(database, attempt_id, "SUCCEEDED", current, detail={"provider": "FIXTURE_ONLY"})
    try:
        _record_mapping(database, run, external, current)
    except Exception:
        with database.connection:
            database.connection.execute("UPDATE gmail_draft_creation_runs SET state='HOLD_FOR_RECONCILIATION',error_category='UNKNOWN_EXTERNAL_STATE',updated_at=? WHERE id=?", (_iso(current), run_id))
        return _result_for_run(database, run_id, error_category="UNKNOWN_EXTERNAL_STATE")
    return _result_for_run(database, run_id)


def _verify_record(database: Database, run, mapping, external: Any, now: datetime, *, attempt_id: Optional[int] = None) -> dict[str, Any]:
    expected = {"draft_id": mapping["external_draft_id"], "message_id": mapping["message_id"], "thread_id": mapping["thread_id"], "to": [mapping["recipient_email"]], "from": mapping["sender_email"], "status": "DRAFT"}
    row = _draft_row(database, run["personalized_draft_id"])
    expected_payload = _payload_from_row(row, mapping["recipient_email"], mapping["sender_email"])
    mismatch: list[str] = []
    if not isinstance(external, Mapping):
        mismatch.append("EXTERNAL_DRAFT_NOT_FOUND")
    else:
        allowed = {"draft_id", "message_id", "thread_id", "to", "from", "subject", "body", "content_hash", "status", "cc", "bcc", "attachments", "html", "provider_timestamp"}
        if set(external) - allowed:
            mismatch.append("UNEXPECTED_FIELDS")
        for key, value in {**expected, "subject": expected_payload["subject"], "body": expected_payload["body"], "content_hash": expected_payload["content_hash"]}.items():
            if external.get(key) != value:
                mismatch.append({"draft_id": "DRAFT_ID_MISMATCH", "message_id": "MESSAGE_ID_MISMATCH", "thread_id": "THREAD_ID_MISMATCH", "to": "RECIPIENT_MISMATCH", "from": "SENDER_MISMATCH", "subject": "SUBJECT_MISMATCH", "body": "BODY_MISMATCH", "content_hash": "CONTENT_HASH_MISMATCH", "status": "UNSENT_STATUS_REQUIRED"}[key])
        if external.get("cc") not in ([], None): mismatch.append("CC_FORBIDDEN")
        if external.get("bcc") not in ([], None): mismatch.append("BCC_FORBIDDEN")
        if external.get("attachments") not in ([], None): mismatch.append("ATTACHMENTS_FORBIDDEN")
        if external.get("html") not in (False, None): mismatch.append("HTML_FORBIDDEN")
    mismatch = sorted(set(mismatch))
    passed = not mismatch
    if attempt_id is not None:
        _finish_attempt(database, attempt_id, "SUCCEEDED" if passed else "FAILED", now, error_category=None if passed else "DRAFT_RETRIEVED_MISMATCH", detail={"mismatch_codes": mismatch})
    with database.connection:
        database.connection.execute("INSERT INTO gmail_draft_verification_results (run_id,mapping_id,verification_state,mismatch_codes_json,safe_details_json,verified_at) VALUES (?,?,?, ?,?,?)", (run["id"], mapping["id"], "PASS" if passed else "FAIL", _json(mismatch), _json({"fields_checked": ["draft_id", "message_id", "thread_id", "to", "from", "subject", "body", "content_hash", "status", "cc", "bcc", "attachments", "html"]}), _iso(now)))
        if passed:
            database.connection.execute("UPDATE gmail_external_draft_mappings SET state='READY_FOR_MANUAL_SEND',verified_at=? WHERE id=?", (_iso(now), mapping["id"]))
            database.connection.execute("UPDATE gmail_draft_creation_runs SET state='READY_FOR_MANUAL_SEND',error_category=NULL,updated_at=? WHERE id=?", (_iso(now), run["id"]))
        else:
            database.connection.execute("UPDATE gmail_external_draft_mappings SET state='HOLD_FOR_RECONCILIATION' WHERE id=?", (mapping["id"],))
            database.connection.execute("UPDATE gmail_draft_creation_runs SET state='HOLD_FOR_RECONCILIATION',error_category='DRAFT_VERIFICATION_MISMATCH',updated_at=? WHERE id=?", (_iso(now), run["id"]))
    return _result_for_run(database, run["id"], reasons=mismatch)


def verify_fixture_gmail_draft(database: Database, *, run_id: int, provider: Any, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Retrieve and verify exactly one fictional external draft."""
    _require_fixture(database, fixture_override)
    migrate_step11(database)
    current = _now(now)
    run = _run_current(database, run_id)
    mapping = database.connection.execute("SELECT * FROM gmail_external_draft_mappings WHERE run_id=? ORDER BY id DESC LIMIT 1", (run_id,)).fetchone()
    if mapping is None:
        return _result_for_run(database, run_id, reasons=["EXTERNAL_DRAFT_MAPPING_REQUIRED"])
    if mapping["state"] == "READY_FOR_MANUAL_SEND":
        return _result_for_run(database, run_id, reused=True)
    if mapping["state"] == "HOLD_FOR_RECONCILIATION":
        return _result_for_run(database, run_id)
    attempt_id = _start_attempt(database, run_id, "GET_DRAFT", current)
    try:
        external = invoke_gmail_provider_action(provider, "GET_DRAFT", mapping["external_draft_id"])
    except Exception as error:
        _finish_attempt(database, attempt_id, "FAILED", current, error_category="GET_DRAFT_EXCEPTION", detail={"exception_type": type(error).__name__})
        with database.connection:
            database.connection.execute("UPDATE gmail_external_draft_mappings SET state='HOLD_FOR_RECONCILIATION' WHERE id=?", (mapping["id"],))
            database.connection.execute("UPDATE gmail_draft_creation_runs SET state='HOLD_FOR_RECONCILIATION',error_category='GET_DRAFT_EXCEPTION',updated_at=? WHERE id=?", (_iso(current), run_id))
        return _result_for_run(database, run_id, error_category="GET_DRAFT_EXCEPTION")
    return _verify_record(database, run, mapping, external, current, attempt_id=attempt_id)


def reconcile_unknown_gmail_draft(database: Database, *, run_id: int, provider: Any, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Reconcile one unknown fixture outcome; never retries creation automatically."""
    _require_fixture(database, fixture_override)
    migrate_step11(database)
    current = _now(now)
    run = _run_current(database, run_id)
    mapping = database.connection.execute("SELECT * FROM gmail_external_draft_mappings WHERE run_id=? ORDER BY id DESC LIMIT 1", (run_id,)).fetchone()
    if mapping is not None:
        return _result_for_run(database, run_id, reasons=["CREATION_RETRY_BLOCKED_UNTIL_RECONCILED"])
    if run["state"] != "HOLD_FOR_RECONCILIATION":
        return _result_for_run(database, run_id)
    external_id = run["external_draft_id"]
    if not external_id:
        with database.connection:
            database.connection.execute("INSERT INTO gmail_draft_reconciliation_events (run_id,event_type,outcome,safe_detail_json,created_at) VALUES (?,?,?,?,?)", (run_id, "RECONCILIATION", "HOLD", _json({"reason": "EXTERNAL_DRAFT_ID_REQUIRED"}), _iso(current)))
        return _result_for_run(database, run_id, reasons=["EXTERNAL_DRAFT_ID_REQUIRED"])
    attempt_id = _start_attempt(database, run_id, "GET_DRAFT", current)
    try:
        external = invoke_gmail_provider_action(provider, "GET_DRAFT", external_id)
    except Exception as error:
        _finish_attempt(database, attempt_id, "FAILED", current, error_category="RECONCILIATION_GET_EXCEPTION", detail={"exception_type": type(error).__name__})
        return _result_for_run(database, run_id, reasons=["RECONCILIATION_GET_FAILED"])
    if external is None:
        _finish_attempt(database, attempt_id, "SUCCEEDED", current, detail={"external_draft": "NOT_FOUND"})
        with database.connection:
            database.connection.execute("UPDATE gmail_draft_creation_runs SET state='APPROVED_FOR_DRAFT_CREATION',external_draft_id=NULL,error_category=NULL,updated_at=? WHERE id=?", (_iso(current), run_id))
            database.connection.execute("INSERT INTO gmail_draft_reconciliation_events (run_id,event_type,outcome,external_draft_id,safe_detail_json,created_at) VALUES (?,?,?,?,?,?)", (run_id, "RECONCILIATION", "NO_EXTERNAL_DRAFT_FOUND", external_id, _json({"creation_retry": "permitted_after_reconciliation"}), _iso(current)))
        return _result_for_run(database, run_id, reasons=["NO_EXTERNAL_DRAFT_FOUND"])
    _finish_attempt(database, attempt_id, "SUCCEEDED", current, detail={"external_draft": "FOUND"})
    _record_mapping(database, run, external, current)
    mapping = database.connection.execute("SELECT * FROM gmail_external_draft_mappings WHERE run_id=? ORDER BY id DESC LIMIT 1", (run_id,)).fetchone()
    with database.connection:
        database.connection.execute("INSERT INTO gmail_draft_reconciliation_events (run_id,event_type,outcome,external_draft_id,safe_detail_json,created_at) VALUES (?,?,?,?,?,?)", (run_id, "RECONCILIATION", "EXTERNAL_DRAFT_FOUND", external_id, _json({"verified_without_creation_retry": True}), _iso(current)))
    return _verify_record(database, _run_current(database, run_id), mapping, external, current)


def get_current_external_draft_mapping(database: Database, *, run_id: int) -> dict[str, Any]:
    migrate_step11(database)
    row = database.connection.execute("SELECT * FROM gmail_external_draft_mappings WHERE run_id=? ORDER BY id DESC LIMIT 1", (run_id,)).fetchone()
    if row is None:
        raise GmailDraftValidationError("external Gmail-draft mapping not found")
    return {"mapping_id": row["id"], "run_id": row["run_id"], "draft_id": row["personalized_draft_id"], "version": row["draft_version"], "external_draft_id": row["external_draft_id"], "message_id": row["message_id"], "thread_id": row["thread_id"], "provider_status": row["provider_status"], "content_hash": row["content_hash"], "state": row["state"], "mapped_at": row["mapped_at"], "verified_at": row["verified_at"]}


def inspect_gmail_draft_mismatches(database: Database, *, run_id: int) -> dict[str, Any]:
    migrate_step11(database)
    rows = database.connection.execute("SELECT id,verification_state,mismatch_codes_json,verified_at FROM gmail_draft_verification_results WHERE run_id=? ORDER BY id", (run_id,)).fetchall()
    return {"run_id": run_id, "mismatch_codes": sorted({code for row in rows if row["verification_state"] == "FAIL" for code in json.loads(row["mismatch_codes_json"])}), "results": [{"verification_id": row["id"], "verification_state": row["verification_state"], "mismatch_codes": json.loads(row["mismatch_codes_json"]), "verified_at": row["verified_at"]} for row in rows]}


def summarize_gmail_draft_provenance(database: Database, *, run_id: int) -> dict[str, Any]:
    migrate_step11(database)
    run = _run_current(database, run_id)
    approval = database.connection.execute("SELECT id,reviewer_identity,status,policy_version,content_hash,input_fingerprint,created_at,invalidated_at FROM gmail_draft_creation_approvals WHERE id=?", (run["approval_id"],)).fetchone()
    mapping = database.connection.execute("SELECT id,external_draft_id,message_id,thread_id,provider_status,state,content_hash,mapped_at,verified_at FROM gmail_external_draft_mappings WHERE run_id=?", (run_id,)).fetchone()
    attempts = database.connection.execute("SELECT action,status,error_category,created_at,completed_at FROM gmail_draft_provider_attempts WHERE run_id=? ORDER BY id", (run_id,)).fetchall()
    reconciliations = database.connection.execute("SELECT event_type,outcome,external_draft_id,created_at FROM gmail_draft_reconciliation_events WHERE run_id=? ORDER BY id", (run_id,)).fetchall()
    return {
        "run_id": run_id, "draft_id": run["personalized_draft_id"], "version": run["draft_version"], "state": run["state"],
        "policy_version": run["policy_version"], "content_hash": run["content_hash"], "input_fingerprint": run["input_fingerprint"],
        "idempotency_key": run["idempotency_key"], "offer_version_id": run["offer_version_id"], "packet_id": run["packet_id"],
        "approval": None if approval is None else dict(approval), "mapping": None if mapping is None else dict(mapping),
        "attempts": [dict(item) for item in attempts], "reconciliation_events": [dict(item) for item in reconciliations],
    }


__all__ = [
    "GMAIL_ALLOWED_ACTIONS", "GMAIL_PROVIDER_MODE", "GmailDraftBlockedError", "GmailDraftValidationError", "FixtureExternalStateUnknown", "FixtureGmailDraftProvider",
    "STEP11_MIGRATION_VERSION", "approve_gmail_draft_creation", "create_fixture_gmail_draft", "get_current_external_draft_mapping", "inspect_gmail_draft_mismatches", "inspect_gmail_draft_readiness", "invoke_gmail_provider_action", "migrate_step11", "preview_gmail_draft_payload", "reconcile_unknown_gmail_draft", "summarize_gmail_draft_provenance", "validate_gmail_draft_payload", "verify_fixture_gmail_draft",
]
