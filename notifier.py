"""Notification delivery layer for the outbound pipeline.

The pipeline never talks to a chat platform directly. Every operator-facing
message (outbound-queue review cards, alerts, growth summaries) is handed to a
:class:`Notifier`, which is a thin, injectable dispatcher with two backends:

* ``CONSOLE`` (default) - prints a readable summary to stdout.
* ``JSON_FILE`` - appends one JSON object per notification to the JSON Lines
  path resolved from configuration.

Routing values (channel identifiers, operator identifier) come from the
environment, falling back to the placeholder table in this module, and are
persisted to the ``notification_registry`` table so a run can prove which route
it used. Nothing here sends anything unless a caller explicitly injects a
notifier and a non-canonical database, and every durable outbox write is
idempotent by message fingerprint, so a retried stage never double-delivers.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, IO, Mapping, Optional

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from step12_followup import migrate_step12


NOTIFIER_MIGRATION_VERSION = 15
NOTIFICATION_POLICY_VERSION = "PALDO_NOTIFY_V1"

CONSOLE_BACKEND = "CONSOLE"
JSON_FILE_BACKEND = "JSON_FILE"
NOTIFICATION_BACKENDS = frozenset({CONSOLE_BACKEND, JSON_FILE_BACKEND})
NOTIFICATION_PROVIDER_MODE = CONSOLE_BACKEND

NOTIFICATION_ALLOWED_ACTIONS = frozenset({"SEND_MESSAGE", "EDIT_MESSAGE"})
NOTIFICATION_ALLOWED_ACTIONS_VALUE = "SEND_MESSAGE,EDIT_MESSAGE"
NOTIFICATION_MAX_LENGTH = 4096
NOTIFICATION_JSONL_DEFAULT_PATH = "./out/notifications.jsonl"

#: Obvious placeholders. Real routes are supplied through the environment
#: (``PALDO_NOTIFICATION_CHANNELS`` / ``PALDO_NOTIFICATION_CHANNEL_ID`` /
#: ``PALDO_NOTIFICATION_OPERATOR_ID``) and mirrored by ``config.example.json``.
PLACEHOLDER_CHANNELS = {
    "control": 1001,
    "outbound_queue": 1002,
    "replies_pipeline": 1003,
    "growth_review": 1004,
    "alerts": 1005,
}
PLACEHOLDER_OPERATOR_ID = 1001


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except ValueError as error:
        raise NotifierValidationError(f"{name} must be an integer") from error


def _env_channels(name: str, default: Mapping[str, int]) -> dict[str, int]:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return dict(default)
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as error:
        raise NotifierValidationError(f"{name} must be a JSON object") from error
    if not isinstance(parsed, Mapping) or not parsed:
        raise NotifierValidationError(f"{name} must be a non-empty JSON object")
    channels: dict[str, int] = {}
    for key, value in parsed.items():
        if isinstance(value, bool) or not isinstance(value, int):
            raise NotifierValidationError(f"{name}.{key} must be an integer")
        channels[str(key)] = int(value)
    return channels


class NotifierBlockedError(RuntimeError):
    """Raised when a canonical or unsafe notification mutation is attempted."""


class NotifierValidationError(ValueError):
    """Raised for malformed notification fixtures or unsafe routing input."""


class NotifierExternalStateUnknown(RuntimeError):
    """Injected backend signal meaning the external outcome is unknown."""

    def __init__(self, external_result: Optional[Mapping[str, Any]] = None):
        super().__init__("notification backend outcome is unknown")
        self.external_result = deepcopy(dict(external_result or {}))


class ConsoleBackend:
    """Default backend: prints a readable summary of each notification."""

    name = CONSOLE_BACKEND

    def __init__(self, *, stream: Optional[IO[str]] = None, clock: Optional[Any] = None):
        self.stream = stream if stream is not None else sys.stdout
        self.clock = clock or _now

    def send_message(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._write(payload, "SENT")

    def edit_message(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._write(payload, "EDITED")

    def _write(self, payload: Mapping[str, Any], status: str) -> dict[str, Any]:
        result = _message_result(payload, status)
        lines = [
            f"[notification] {result['status']} channel={result['channel_id']} thread={result['thread_id']} id={result['message_id']}",
            *(f"    {line}" for line in str(result["text"]).splitlines()),
        ]
        print("\n".join(lines), file=self.stream)
        self.stream.flush()
        return result


class JsonFileBackend:
    """Appends one JSON object per notification to a JSON Lines file."""

    name = JSON_FILE_BACKEND

    def __init__(self, path: Any, *, clock: Optional[Any] = None):
        if not isinstance(path, (str, Path)) or not str(path).strip():
            raise NotifierValidationError("json_file backend requires a path")
        self.path = Path(path)
        self.clock = clock or _now

    def send_message(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._write(payload, "SENT")

    def edit_message(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        return self._write(payload, "EDITED")

    def _write(self, payload: Mapping[str, Any], status: str) -> dict[str, Any]:
        result = _message_result(payload, status)
        record = {
            "action": "SEND_MESSAGE" if status == "SENT" else "EDIT_MESSAGE",
            "message_id": result["message_id"],
            "channel_id": result["channel_id"],
            "thread_id": result["thread_id"],
            "status": result["status"],
            "text": result["text"],
            "written_at": _iso(self.clock()),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True, ensure_ascii=False) + "\n")
        return result


def notification_jsonl_path(database: Optional[Database] = None) -> str:
    """Resolve the JSON Lines target from the environment, then database config."""
    configured = os.environ.get("PALDO_NOTIFICATION_JSONL_PATH")
    if configured and configured.strip():
        return configured.strip()
    if database is not None:
        try:
            value = database.get_config("notification_jsonl_path")
        except Exception:
            value = None
        if isinstance(value, str) and value.strip():
            return value.strip()
    return NOTIFICATION_JSONL_DEFAULT_PATH


def resolve_backend(name: Optional[str], database: Optional[Database] = None) -> Any:
    """Return the backend object for a configured backend name."""
    selected = (name or NOTIFICATION_PROVIDER_MODE).strip().upper()
    if selected == CONSOLE_BACKEND:
        return ConsoleBackend()
    if selected == JSON_FILE_BACKEND:
        return JsonFileBackend(notification_jsonl_path(database))
    raise NotifierValidationError(f"notification backend must be one of {sorted(NOTIFICATION_BACKENDS)}")


class Notifier:
    """Notification dispatcher with the call shape the pipeline expects.

    ``send_message`` and ``edit_message`` accept one payload mapping and return
    ``message_id`` / ``channel_id`` / ``thread_id`` / ``text`` / ``status``. The
    two ``unknown_on_*`` flags let a caller exercise the unknown-outcome
    reconciliation path without touching a real backend.
    """

    def __init__(
        self,
        backend: Any = None,
        *,
        jsonl_path: Any = None,
        unknown_on_send: bool = False,
        unknown_on_edit: bool = False,
        mutation: Optional[Mapping[str, Any]] = None,
    ):
        if backend is None:
            backend = JsonFileBackend(jsonl_path) if jsonl_path is not None else ConsoleBackend()
        if not callable(getattr(backend, "send_message", None)):
            raise NotifierValidationError("backend must provide send_message")
        self.backend = backend
        self.name = str(getattr(backend, "name", type(backend).__name__))
        self.unknown_on_send = bool(unknown_on_send)
        self.unknown_on_edit = bool(unknown_on_edit)
        self.mutation = dict(mutation or {})
        self.send_calls = 0
        self.edit_calls = 0
        self.sent: list[dict[str, Any]] = []

    def send_message(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.send_calls += 1
        result = _apply_mutation(_call_backend(self.backend, "send_message", payload, "SENT"), self.mutation)
        self.sent.append(deepcopy(result))
        if self.unknown_on_send:
            raise NotifierExternalStateUnknown(result)
        return deepcopy(result)

    def edit_message(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.edit_calls += 1
        result = _apply_mutation(_call_backend(self.backend, "edit_message", payload, "EDITED"), self.mutation)
        if self.unknown_on_edit:
            raise NotifierExternalStateUnknown(result)
        return deepcopy(result)

    def notify(self, text: str, *, channel: str = "outbound_queue", thread_id: Optional[int] = None, **payload: Any) -> dict[str, Any]:
        """Convenience wrapper: send one plain-text notification to a named channel."""
        return self.send_message(
            {
                "channel_id": NOTIFICATION_CHANNEL_ID,
                "thread_id": NOTIFICATION_CHANNELS.get(channel, NOTIFICATION_CHANNEL_ID) if thread_id is None else thread_id,
                "text": text,
                **payload,
            }
        )


def _call_backend(backend: Any, method_name: str, payload: Mapping[str, Any], status: str) -> dict[str, Any]:
    method = getattr(backend, method_name, None)
    if callable(method):
        return method(payload)
    if not isinstance(payload, Mapping):
        raise NotifierValidationError("notification payload must be an object")
    return _message_result(payload, status)


def _apply_mutation(result: Any, mutation: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise NotifierValidationError("notification backend output must be an object")
    merged = dict(result)
    merged.update(deepcopy(dict(mutation)))
    return merged


def _message_result(payload: Any, status: str) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise NotifierValidationError("notification payload must be an object")
    fingerprint = str(payload.get("message_fingerprint") or "notification")
    return {
        "message_id": str(payload.get("external_message_id") or "notification-" + fingerprint[:16]),
        "channel_id": payload.get("channel_id"),
        "thread_id": payload.get("thread_id"),
        "text": payload.get("text"),
        "status": status,
    }


_CREDENTIAL_LIKE_RE = re.compile(
    r"(?:bot\d{5,}:[A-Za-z0-9_-]+|(?:api[_ -]?key|access[_ -]?token|bot[_ -]?token|webhook[_ -]?secret|password|secret)\s*[:=]|bearer\s+[A-Za-z0-9._-]{12,})",
    re.IGNORECASE,
)
_HTML_RE = re.compile(r"</?[a-z][^>]*>|<!doctype|<script\b", re.IGNORECASE)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _now(value: Optional[datetime] = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: Optional[datetime] = None) -> str:
    return _now(value).isoformat()


def _safe_text(value: Any, limit: int = 600) -> str:
    text = " ".join(str(value or "").split())
    text = _HTML_RE.sub(" ", text)
    text = _CREDENTIAL_LIKE_RE.sub("[REDACTED]", text)
    text = re.sub(r"(?i)\bstack\s*trace\b.*", "[REDACTED]", text)
    return text[:limit]


def _is_durable_database(database: Database) -> bool:
    try:
        return database.path.resolve() == DEFAULT_DB_PATH.resolve()
    except (AttributeError, OSError):
        return False


def _require_fixture(database: Database, fixture_override: bool) -> None:
    if _is_durable_database(database):
        raise NotifierBlockedError("canonical notification operation is blocked")
    if not fixture_override:
        raise NotifierBlockedError("fixture_override=True is required for the notifier")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS notification_registry (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    channel_id INTEGER NOT NULL,
    operator_id INTEGER NOT NULL,
    control_topic_id INTEGER NOT NULL,
    outbound_queue_topic_id INTEGER NOT NULL,
    replies_pipeline_topic_id INTEGER NOT NULL,
    growth_review_topic_id INTEGER NOT NULL,
    alerts_topic_id INTEGER NOT NULL,
    policy_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notification_outbox (
    id INTEGER PRIMARY KEY,
    message_fingerprint TEXT NOT NULL UNIQUE,
    message_version INTEGER NOT NULL CHECK (message_version > 0),
    action TEXT NOT NULL CHECK (action IN ('SEND_MESSAGE','EDIT_MESSAGE')),
    channel_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_version TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    text TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('QUEUED','SENT','EDITED','FAILED','UNKNOWN')),
    external_message_id TEXT,
    error_category TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS notification_outbox_state_idx ON notification_outbox(state, updated_at);

CREATE TABLE IF NOT EXISTS notification_attempts (
    id INTEGER PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES notification_outbox(id) ON DELETE CASCADE,
    action TEXT NOT NULL CHECK (action IN ('SEND_MESSAGE','EDIT_MESSAGE')),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    status TEXT NOT NULL CHECK (status IN ('STARTED','SUCCEEDED','FAILED','UNKNOWN_EXTERNAL_STATE')),
    safe_detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (outbox_id, action, attempt_number)
);
CREATE INDEX IF NOT EXISTS notification_attempts_outbox_idx ON notification_attempts(outbox_id, action, attempt_number);

CREATE TABLE IF NOT EXISTS notification_message_mappings (
    id INTEGER PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES notification_outbox(id) ON DELETE RESTRICT,
    external_message_id TEXT NOT NULL UNIQUE,
    channel_id INTEGER NOT NULL,
    topic_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    content_fingerprint TEXT NOT NULL,
    provider_status TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('SENT','EDITED','RECONCILED')),
    mapped_at TEXT NOT NULL,
    verified_at TEXT
);
CREATE INDEX IF NOT EXISTS notification_mappings_outbox_idx ON notification_message_mappings(outbox_id, mapped_at);

CREATE TABLE IF NOT EXISTS notification_reconciliation_events (
    id INTEGER PRIMARY KEY,
    outbox_id INTEGER NOT NULL REFERENCES notification_outbox(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    external_message_id TEXT,
    safe_detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS notification_reconciliation_outbox_idx ON notification_reconciliation_events(outbox_id, created_at);
"""


NOTIFICATION_CHANNELS = _env_channels("PALDO_NOTIFICATION_CHANNELS", PLACEHOLDER_CHANNELS)
NOTIFICATION_PROVIDER_MODE = os.environ.get("PALDO_NOTIFICATION_BACKEND", CONSOLE_BACKEND).strip().upper()
if NOTIFICATION_PROVIDER_MODE not in NOTIFICATION_BACKENDS:
    raise NotifierValidationError(
        "PALDO_NOTIFICATION_BACKEND must be one of " + ", ".join(sorted(NOTIFICATION_BACKENDS))
    )

NOTIFICATION_CHANNEL_ID = _env_int("PALDO_NOTIFICATION_CHANNEL_ID", PLACEHOLDER_CHANNELS["control"])
NOTIFICATION_OPERATOR_ID = _env_int("PALDO_NOTIFICATION_OPERATOR_ID", PLACEHOLDER_OPERATOR_ID)

_SOURCE_TABLES = (
    "notification_outbox",
    "notification_attempts",
    "notification_message_mappings",
    "notification_reconciliation_events",
)


def migrate_notifier(database_or_path: Database | str | Path) -> int:
    """Apply the additive notification schema and persist the resolved routes."""
    database = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    migrate_step12(database)
    timestamp = _iso()
    with database.connection:
        database.connection.executescript(_SCHEMA)
        database.connection.executemany(
            "INSERT OR IGNORE INTO system_config(key,value,value_type) VALUES (?,?,?)",
            (
                ("notifications_enabled", "0", "integer"),
                ("notification_backend", NOTIFICATION_PROVIDER_MODE, "text"),
                ("notification_target_configured", "0", "integer"),
                ("notification_daily_cap", "0", "integer"),
                ("notification_allowed_actions", NOTIFICATION_ALLOWED_ACTIONS_VALUE, "text"),
                ("notification_jsonl_path", NOTIFICATION_JSONL_DEFAULT_PATH, "text"),
            ),
        )
        database.connection.execute(
            """INSERT OR IGNORE INTO notification_registry
               (id,channel_id,operator_id,control_topic_id,outbound_queue_topic_id,
                replies_pipeline_topic_id,growth_review_topic_id,alerts_topic_id,policy_version,created_at,updated_at)
               VALUES (1,?,?,?,?,?,?,?,?,?,?)""",
            (
                NOTIFICATION_CHANNEL_ID, NOTIFICATION_OPERATOR_ID,
                NOTIFICATION_CHANNELS["control"], NOTIFICATION_CHANNELS["outbound_queue"],
                NOTIFICATION_CHANNELS["replies_pipeline"], NOTIFICATION_CHANNELS["growth_review"],
                NOTIFICATION_CHANNELS["alerts"], NOTIFICATION_POLICY_VERSION, timestamp, timestamp,
            ),
        )
        database.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version,name,applied_at) VALUES (?,?,?)",
            (NOTIFIER_MIGRATION_VERSION, "notifier_notification_delivery_surface", timestamp),
        )
    return NOTIFIER_MIGRATION_VERSION


def validate_notification_registry(database: Database) -> dict[str, Any]:
    """Confirm the persisted routes still match the configured routes."""
    migrate_notifier(database)
    row = database.connection.execute("SELECT * FROM notification_registry WHERE id=1").fetchone()
    expected = {
        "channel_id": NOTIFICATION_CHANNEL_ID,
        "operator_id": NOTIFICATION_OPERATOR_ID,
        "control_topic_id": NOTIFICATION_CHANNELS["control"],
        "outbound_queue_topic_id": NOTIFICATION_CHANNELS["outbound_queue"],
        "replies_pipeline_topic_id": NOTIFICATION_CHANNELS["replies_pipeline"],
        "growth_review_topic_id": NOTIFICATION_CHANNELS["growth_review"],
        "alerts_topic_id": NOTIFICATION_CHANNELS["alerts"],
        "policy_version": NOTIFICATION_POLICY_VERSION,
    }
    if row is None:
        return {"valid": False, "blocking_reasons": ["NOTIFICATION_REGISTRY_MISSING"]}
    reasons = [key.upper() + "_MISMATCH" for key, value in expected.items() if row[key] != value]
    return {
        "valid": not reasons,
        "blocking_reasons": reasons,
        "channel_id": row["channel_id"],
        "operator_id": row["operator_id"],
        "topics": dict(NOTIFICATION_CHANNELS),
        "policy_version": row["policy_version"],
    }


def _config_reasons(database: Database, *, fixture_override: bool, require_message_cap: bool = False) -> list[str]:
    config = database.read_config()
    reasons: list[str] = []
    if not fixture_override:
        reasons.append("FIXTURE_OVERRIDE_REQUIRED")
    if config.get("notification_backend") not in NOTIFICATION_BACKENDS:
        reasons.append("NOTIFICATION_BACKEND_UNSAFE")
    if config.get("notification_allowed_actions") != NOTIFICATION_ALLOWED_ACTIONS_VALUE:
        reasons.append("NOTIFICATION_ACTION_ALLOWLIST_UNSAFE")
    if require_message_cap:
        if config.get("notifications_enabled") != 1:
            reasons.append("NOTIFICATIONS_DISABLED")
        if int(config.get("notification_daily_cap", 0) or 0) <= 0:
            reasons.append("NOTIFICATION_DELIVERY_CAP_ZERO")
    return reasons


def inspect_notifier_readiness(database: Database, *, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Report whether the notifier could deliver without crossing the fixture boundary."""
    migrate_notifier(database)
    if _is_durable_database(database):
        return {
            "ready": False,
            "backend": NOTIFICATION_PROVIDER_MODE,
            "blocking_reasons": ["CANONICAL_NOTIFICATION_OPERATION_BLOCKED", "TEST_POLICY_NONPRODUCTION"],
        }
    registry = validate_notification_registry(database)
    reasons = _config_reasons(database, fixture_override=fixture_override)
    reasons.extend(registry["blocking_reasons"])
    return {
        "ready": not reasons,
        "backend": database.get_config("notification_backend"),
        "backends": sorted(NOTIFICATION_BACKENDS),
        "registry": {"valid": registry["valid"], "channel_id": registry.get("channel_id"), "operator_id": registry.get("operator_id")},
        "blocking_reasons": sorted(set(reasons)),
        "external_operations": False,
    }


def _result(*, action: str, ok: bool, category: str, reasons: Optional[list[str]] = None, **values: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"ok": bool(ok), "action": action, "result_category": category, "blocking_reasons": sorted(set(reasons or []))}
    payload.update(values)
    return payload


def _outbox_fingerprint(*, action: str, channel_id: int, topic_id: int, entity_type: str, entity_id: str, entity_version: str, content_fingerprint: str, text: str) -> str:
    return sha256(_json({"action": action, "channel_id": channel_id, "topic_id": topic_id, "entity_type": entity_type, "entity_id": entity_id, "entity_version": entity_version, "content_fingerprint": content_fingerprint, "text": text}).encode()).hexdigest()


def _verify_external_message(external: Any, *, outbox, expected_action: str) -> tuple[bool, list[str]]:
    if not isinstance(external, Mapping):
        return False, ["PROVIDER_OUTPUT_INVALID"]
    reasons = []
    if not external.get("message_id"):
        reasons.append("EXTERNAL_MESSAGE_ID_MISSING")
    if external.get("channel_id") != outbox["channel_id"]:
        reasons.append("EXTERNAL_CHANNEL_MISMATCH")
    if external.get("thread_id") != outbox["topic_id"]:
        reasons.append("EXTERNAL_TOPIC_MISMATCH")
    if external.get("text") != outbox["text"]:
        reasons.append("EXTERNAL_CONTENT_MISMATCH")
    if external.get("status") not in {"SENT", "EDITED"}:
        reasons.append("EXTERNAL_STATUS_INVALID")
    return not reasons, reasons


def queue_notification(database: Database, *, notifier: Notifier, topic_id: int, text: str, entity_type: str, entity_id: Any, entity_version: Any, content_fingerprint: str, action: str = "SEND_MESSAGE", channel_id: int = NOTIFICATION_CHANNEL_ID, external_message_id: Optional[str] = None, fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Persist an intended notification, then invoke one bounded delivery attempt."""
    _require_fixture(database, fixture_override)
    migrate_notifier(database)
    current = _now(now)
    if action not in NOTIFICATION_ALLOWED_ACTIONS:
        raise NotifierValidationError("notification action is not allowlisted")
    if channel_id != NOTIFICATION_CHANNEL_ID or topic_id not in NOTIFICATION_CHANNELS.values():
        raise NotifierValidationError("notification route is not allowlisted")
    if not isinstance(text, str) or not text.strip() or len(text) > NOTIFICATION_MAX_LENGTH or _CREDENTIAL_LIKE_RE.search(text):
        raise NotifierValidationError("notification text is invalid")
    config_reasons = _config_reasons(database, fixture_override=fixture_override, require_message_cap=True)
    if config_reasons:
        return _result(action=action, ok=False, category="OUTBOX_BLOCKED", reasons=config_reasons)
    fingerprint = _outbox_fingerprint(action=action, channel_id=channel_id, topic_id=topic_id, entity_type=str(entity_type), entity_id=str(entity_id), entity_version=str(entity_version), content_fingerprint=content_fingerprint, text=text)
    existing = database.connection.execute("SELECT * FROM notification_outbox WHERE message_fingerprint=?", (fingerprint,)).fetchone()
    if existing is not None:
        if existing["state"] in {"SENT", "EDITED", "UNKNOWN"}:
            return _result(action=action, ok=existing["state"] in {"SENT", "EDITED"}, category="OUTBOX_REUSED", reused=True, outbox_id=existing["id"], message_fingerprint=fingerprint, external_message_id=existing["external_message_id"])
        outbox_id = existing["id"]
        outbox = existing
    else:
        with database.connection:
            cursor = database.connection.execute(
                """INSERT INTO notification_outbox(message_fingerprint,message_version,action,channel_id,topic_id,entity_type,entity_id,entity_version,content_fingerprint,text,state,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?, ?,?)""",
                (fingerprint, 1, action, channel_id, topic_id, str(entity_type), str(entity_id), str(entity_version), content_fingerprint, text, "QUEUED", _iso(current), _iso(current)),
            )
        outbox_id = int(cursor.lastrowid)
        outbox = database.connection.execute("SELECT * FROM notification_outbox WHERE id=?", (outbox_id,)).fetchone()
    attempts = database.connection.execute("SELECT COALESCE(MAX(attempt_number),0) FROM notification_attempts WHERE outbox_id=? AND action=?", (outbox_id, action)).fetchone()[0]
    if attempts >= 2:
        return _result(action=action, ok=False, category="OUTBOX_RETRY_LIMIT", reasons=["NOTIFICATION_PROVIDER_RETRY_LIMIT"], outbox_id=outbox_id, message_fingerprint=fingerprint)
    attempt_number = attempts + 1
    with database.connection:
        attempt_cursor = database.connection.execute("INSERT INTO notification_attempts(outbox_id,action,attempt_number,status,created_at) VALUES (?,?,?,'STARTED',?)", (outbox_id, action, attempt_number, _iso(current)))
        database.connection.execute("UPDATE notification_outbox SET state='QUEUED',updated_at=? WHERE id=?", (_iso(current), outbox_id))
    payload = {"channel_id": channel_id, "thread_id": topic_id, "text": text, "message_fingerprint": fingerprint}
    if external_message_id:
        payload["external_message_id"] = external_message_id
    method = getattr(notifier, "send_message" if action == "SEND_MESSAGE" else "edit_message", None)
    if not callable(method):
        with database.connection:
            database.connection.execute("UPDATE notification_attempts SET status='FAILED',safe_detail_json=?,completed_at=? WHERE id=?", (_json({"error_category": "PROVIDER_METHOD_MISSING"}), _iso(current), attempt_cursor.lastrowid))
            database.connection.execute("UPDATE notification_outbox SET state='FAILED',error_category='PROVIDER_METHOD_MISSING',updated_at=? WHERE id=?", (_iso(current), outbox_id))
        return _result(action=action, ok=False, category="PROVIDER_METHOD_MISSING", reasons=["PROVIDER_METHOD_MISSING"], outbox_id=outbox_id)
    try:
        external = method(payload)
    except NotifierExternalStateUnknown as error:
        with database.connection:
            database.connection.execute("UPDATE notification_attempts SET status='UNKNOWN_EXTERNAL_STATE',safe_detail_json=?,completed_at=? WHERE id=?", (_json({"error_category": "UNKNOWN_EXTERNAL_STATE"}), _iso(current), attempt_cursor.lastrowid))
            database.connection.execute("UPDATE notification_outbox SET state='UNKNOWN',error_category='UNKNOWN_EXTERNAL_STATE',updated_at=? WHERE id=?", (_iso(current), outbox_id))
            database.connection.execute("INSERT INTO notification_reconciliation_events(outbox_id,event_type,outcome,external_message_id,safe_detail_json,created_at) VALUES (?,?,?,?,?,?)", (outbox_id, "PROVIDER_INVOCATION", "UNKNOWN_EXTERNAL_STATE", (error.external_result or {}).get("message_id"), _json({"attempt_number": attempt_number}), _iso(current)))
        return _result(action=action, ok=False, category="UNKNOWN_EXTERNAL_STATE", reasons=["UNKNOWN_EXTERNAL_STATE"], outbox_id=outbox_id, message_fingerprint=fingerprint)
    except Exception:
        with database.connection:
            database.connection.execute("UPDATE notification_attempts SET status='FAILED',safe_detail_json=?,completed_at=? WHERE id=?", (_json({"error_category": "PROVIDER_CALL_FAILED"}), _iso(current), attempt_cursor.lastrowid))
            database.connection.execute("UPDATE notification_outbox SET state='FAILED',error_category='PROVIDER_CALL_FAILED',updated_at=? WHERE id=?", (_iso(current), outbox_id))
        return _result(action=action, ok=False, category="PROVIDER_CALL_FAILED", reasons=["PROVIDER_CALL_FAILED"], outbox_id=outbox_id)
    valid, reasons = _verify_external_message(external, outbox=outbox, expected_action=action)
    if not valid:
        with database.connection:
            database.connection.execute("UPDATE notification_attempts SET status='UNKNOWN_EXTERNAL_STATE',safe_detail_json=?,completed_at=? WHERE id=?", (_json({"error_category": "PROVIDER_OUTPUT_INVALID", "codes": reasons}), _iso(current), attempt_cursor.lastrowid))
            database.connection.execute("UPDATE notification_outbox SET state='UNKNOWN',error_category='PROVIDER_OUTPUT_INVALID',updated_at=? WHERE id=?", (_iso(current), outbox_id))
        return _result(action=action, ok=False, category="PROVIDER_OUTPUT_INVALID", reasons=reasons, outbox_id=outbox_id)
    message_state = "SENT" if action == "SEND_MESSAGE" else "EDITED"
    with database.connection:
        database.connection.execute("UPDATE notification_attempts SET status='SUCCEEDED',safe_detail_json=?,completed_at=? WHERE id=?", (_json({"backend": NOTIFICATION_PROVIDER_MODE}), _iso(current), attempt_cursor.lastrowid))
        database.connection.execute("UPDATE notification_outbox SET state=?,external_message_id=?,updated_at=? WHERE id=?", (message_state, str(external["message_id"]), _iso(current), outbox_id))
        database.connection.execute("INSERT INTO notification_message_mappings(outbox_id,external_message_id,channel_id,topic_id,action,content_fingerprint,provider_status,state,mapped_at,verified_at) VALUES (?,?,?,?,?,?,?,?,?,?)", (outbox_id, str(external["message_id"]), channel_id, topic_id, action, content_fingerprint, str(external["status"]), message_state, _iso(current), _iso(current)))
    return _result(action=action, ok=True, category="OUTBOX_SENT" if action == "SEND_MESSAGE" else "OUTBOX_EDITED", outbox_id=outbox_id, message_fingerprint=fingerprint, external_message_id=str(external["message_id"]), message_version=outbox["message_version"], channel_id=channel_id, topic_id=topic_id)


def reconcile_notification_outcome(database: Database, *, outbox_id: int, external_result: Mapping[str, Any], fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Reconcile an unknown delivery result without polling or backend reads."""
    _require_fixture(database, fixture_override)
    migrate_notifier(database)
    current = _now(now)
    outbox = database.connection.execute("SELECT * FROM notification_outbox WHERE id=?", (outbox_id,)).fetchone()
    if outbox is None:
        return _result(action="RECONCILE", ok=False, category="OUTBOX_NOT_FOUND", reasons=["OUTBOX_NOT_FOUND"])
    if outbox["state"] != "UNKNOWN":
        return _result(action="RECONCILE", ok=outbox["state"] in {"SENT", "EDITED"}, category="RECONCILIATION_NOT_REQUIRED", reused=True, outbox_id=outbox_id)
    valid, reasons = _verify_external_message(external_result, outbox=outbox, expected_action=outbox["action"])
    if not valid:
        with database.connection:
            database.connection.execute("INSERT INTO notification_reconciliation_events(outbox_id,event_type,outcome,external_message_id,safe_detail_json,created_at) VALUES (?,?,?,?,?,?)", (outbox_id, "RECONCILIATION", "MISMATCH", external_result.get("message_id") if isinstance(external_result, Mapping) else None, _json({"codes": reasons}), _iso(current)))
        return _result(action="RECONCILE", ok=False, category="RECONCILIATION_MISMATCH", reasons=reasons, outbox_id=outbox_id)
    state = "SENT" if outbox["action"] == "SEND_MESSAGE" else "EDITED"
    with database.connection:
        database.connection.execute("UPDATE notification_outbox SET state=?,external_message_id=?,error_category=NULL,updated_at=? WHERE id=?", (state, str(external_result["message_id"]), _iso(current), outbox_id))
        database.connection.execute("INSERT INTO notification_message_mappings(outbox_id,external_message_id,channel_id,topic_id,action,content_fingerprint,provider_status,state,mapped_at,verified_at) VALUES (?,?,?,?,?,?,?,?,?,?)", (outbox_id, str(external_result["message_id"]), outbox["channel_id"], outbox["topic_id"], outbox["action"], outbox["content_fingerprint"], str(external_result["status"]), "RECONCILED", _iso(current), _iso(current)))
        database.connection.execute("INSERT INTO notification_reconciliation_events(outbox_id,event_type,outcome,external_message_id,safe_detail_json,created_at) VALUES (?,?,?,?,?,?)", (outbox_id, "RECONCILIATION", "RECONCILED", str(external_result["message_id"]), _json({"verified": True}), _iso(current)))
    return _result(action="RECONCILE", ok=True, category="RECONCILED", outbox_id=outbox_id, external_message_id=str(external_result["message_id"]))


def summarize_notifier_provenance(database: Database) -> dict[str, Any]:
    """Summarize delivery provenance without contacting any backend."""
    migrate_notifier(database)
    registry = validate_notification_registry(database)
    config = database.read_config()
    return {
        "policy_version": NOTIFICATION_POLICY_VERSION,
        "backend": NOTIFICATION_PROVIDER_MODE,
        "registry_table": "notification_registry",
        "registry": {"valid": registry["valid"], "channel_id": registry.get("channel_id"), "operator_id": registry.get("operator_id"), "topics": registry.get("topics")},
        "safe_configuration": {key: config.get(key) for key in ("notifications_enabled", "notification_backend", "notification_target_configured", "notification_daily_cap", "notification_allowed_actions", "notification_jsonl_path")},
        "operational_counts": {table: database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in _SOURCE_TABLES},
        "external_operations": False,
    }


__all__ = [
    "NOTIFIER_MIGRATION_VERSION", "NOTIFICATION_POLICY_VERSION", "NOTIFICATION_PROVIDER_MODE",
    "NOTIFICATION_BACKENDS", "CONSOLE_BACKEND", "JSON_FILE_BACKEND", "NOTIFICATION_CHANNEL_ID",
    "NOTIFICATION_OPERATOR_ID", "NOTIFICATION_CHANNELS", "NOTIFICATION_ALLOWED_ACTIONS",
    "NOTIFICATION_ALLOWED_ACTIONS_VALUE", "NOTIFICATION_MAX_LENGTH", "NOTIFICATION_JSONL_DEFAULT_PATH",
    "PLACEHOLDER_CHANNELS", "PLACEHOLDER_OPERATOR_ID", "ConsoleBackend", "JsonFileBackend",
    "Notifier", "NotifierBlockedError", "NotifierValidationError", "NotifierExternalStateUnknown",
    "inspect_notifier_readiness", "migrate_notifier", "notification_jsonl_path", "queue_notification",
    "reconcile_notification_outcome", "resolve_backend", "summarize_notifier_provenance",
    "validate_notification_registry",
]
