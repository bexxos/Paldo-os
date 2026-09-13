"""Step 7 deterministic, resumable Paldo OS pipeline orchestration.

This module composes the existing Step 3 ingestion, Step 6 bounded enrichment,
and Step 2 scorer.  The only runnable end-to-end path in this step is an
explicit fictional-fixture path against a non-durable SQLite database.  No
live collector, LLM, email draft, scheduler, or outreach operation is exposed.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urlsplit

from paldo_os_outbound import (
    DEFAULT_DB_PATH,
    Database,
    MANDATORY_GATES,
    SUPPORTED_SIGNAL_KEYS,
    TRI_STATE_VALUES,
)
from step3_discovery import (
    APIFY_SOURCE,
    DiscoveryMode,
    IngestionOutcome,
    ingest_candidate,
    normalize_candidate,
)
from step6_website_enrichment import (
    WebsiteEnrichmentBlockedError,
    migrate_step6,
    enrich_website_candidate,
)


STEP7_MIGRATION_VERSION = 9


class PipelineState(str, Enum):
    DISCOVERED = "DISCOVERED"
    INGESTION_ACCEPTED = "INGESTION_ACCEPTED"
    ENRICHMENT_PENDING = "ENRICHMENT_PENDING"
    ENRICHMENT_COMPLETE = "ENRICHMENT_COMPLETE"
    QUALIFICATION_PENDING = "QUALIFICATION_PENDING"
    QUALIFIED = "QUALIFIED"
    STRONG = "STRONG"
    HOLD = "HOLD"
    REJECTED = "REJECTED"
    SUPPRESSED = "SUPPRESSED"
    ERROR_RETRYABLE = "ERROR_RETRYABLE"
    ERROR_TERMINAL = "ERROR_TERMINAL"


class PipelineBlockedError(RuntimeError):
    """Raised when a requested operation fails a safety or provenance gate."""


class PipelineInterrupted(RuntimeError):
    """Test/fixture control signal that leaves a run resumable at a checkpoint."""

    def __init__(self, run_id: int, checkpoint: str):
        super().__init__(f"pipeline run {run_id} interrupted after {checkpoint}")
        self.run_id = run_id
        self.checkpoint = checkpoint


class SuppressedPipelineError(RuntimeError):
    """Raised when a human review is attempted for a suppressed lead."""


_FINAL_STATES = frozenset(
    {
        PipelineState.QUALIFIED.value,
        PipelineState.STRONG.value,
        PipelineState.HOLD.value,
        PipelineState.REJECTED.value,
        PipelineState.SUPPRESSED.value,
        PipelineState.ERROR_TERMINAL.value,
    }
)
_ACTIVE_BUSINESS_STATUSES = frozenset({"OPERATIONAL", "OPEN", "ACTIVE"})
_INACTIVE_BUSINESS_STATUSES = frozenset(
    {"CLOSED", "CLOSED_PERMANENTLY", "PERMANENTLY_CLOSED", "INACTIVE"}
)

# Step 6 uses deliberately descriptive extraction keys.  Step 2 has a smaller
# explicit vocabulary; each mapping below is deterministic and auditable.
_WEBSITE_SIGNAL_MAP = {
    "appointment_channels": "appointment_channels",
    "public_business_email": "public_business_email",
    "email_explicitly_used_for_booking": "email_explicitly_used_for_booking",
    "website_booking_form": "website_appointment_request_form",
    "internal_booking_page": "website_booking_page_widget",
    "external_booking_provider_link": "website_booking_page_widget",
    "phone_booking": "phone_booking",
    "messenger_booking": "messenger_booking",
    "instagram_booking": "instagram_booking",
    "whatsapp_booking": "whatsapp_booking",
    "cancellation_route": "cancellation_route",
    "rescheduling_route": "rescheduling_route",
    "deposit_policy": "cancellation_deposit_no_show_policy",
    "no_show_policy": "cancellation_deposit_no_show_policy",
    "reminders": "visible_reminder_capability",
    "post_visit_follow_up": "post_visit_follow_up",
    "recurring_appointments": "recurring_appointments",
    "multiple_practitioners": "multiple_practitioners",
    "multiple_services": "multiple_services",
    "multiple_locations": "multiple_locations",
    "out_of_hours_inquiry_option": "out_of_hours_inquiry_handling",
}

_STEP7_SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id) ON DELETE CASCADE,
    lead_id INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    current_stage TEXT NOT NULL,
    current_state TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_successful_checkpoint TEXT NOT NULL DEFAULT '',
    safe_error_category TEXT,
    human_review_required INTEGER NOT NULL DEFAULT 0 CHECK (human_review_required IN (0, 1)),
    completion_state TEXT NOT NULL DEFAULT 'RUNNING',
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 2 CHECK (max_attempts >= 1),
    ingestion_outcome TEXT NOT NULL,
    ingestion_reason TEXT,
    website_run_id INTEGER,
    fixture_evidence_json TEXT NOT NULL DEFAULT '[]',
    fixture_gate_statuses_json TEXT NOT NULL DEFAULT '{}',
    review_gate_statuses_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (candidate_id, campaign_id)
);

CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
    id INTEGER PRIMARY KEY,
    pipeline_run_id INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    checkpoint_key TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (pipeline_run_id, checkpoint_key)
);

CREATE TABLE IF NOT EXISTS pipeline_evidence (
    id INTEGER PRIMARY KEY,
    pipeline_run_id INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    evidence_id INTEGER REFERENCES evidence(id) ON DELETE SET NULL,
    signal_key TEXT NOT NULL,
    signal_value TEXT NOT NULL CHECK (signal_value IN ('YES', 'NO', 'UNCLEAR')),
    observed_or_inferred TEXT NOT NULL CHECK (observed_or_inferred IN ('OBSERVED', 'INFERRED')),
    source_type TEXT NOT NULL,
    source_url TEXT,
    observed_at TEXT NOT NULL,
    confidence REAL,
    observation TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    supersedes_id INTEGER REFERENCES pipeline_evidence(id) ON DELETE SET NULL,
    is_current INTEGER NOT NULL DEFAULT 1 CHECK (is_current IN (0, 1)),
    UNIQUE (lead_id, signal_key, evidence_hash)
);

CREATE TABLE IF NOT EXISTS pipeline_qualification_results (
    id INTEGER PRIMARY KEY,
    pipeline_run_id INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    version INTEGER NOT NULL CHECK (version >= 1),
    score INTEGER NOT NULL CHECK (score >= 0 AND score <= 100),
    classification TEXT NOT NULL,
    qualification_result TEXT NOT NULL,
    final_state TEXT NOT NULL,
    gate_statuses TEXT NOT NULL,
    evidence_fingerprint TEXT NOT NULL,
    reasoning_data TEXT NOT NULL,
    evaluated_at TEXT NOT NULL,
    UNIQUE (pipeline_run_id, version),
    UNIQUE (pipeline_run_id, evidence_fingerprint)
);

CREATE TABLE IF NOT EXISTS manual_reviews (
    id INTEGER PRIMARY KEY,
    pipeline_run_id INTEGER NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    exact_gate_or_signal TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('YES', 'NO', 'UNCLEAR')),
    reason TEXT NOT NULL,
    reviewer_identity TEXT NOT NULL CHECK (reviewer_identity = 'OPERATOR'),
    reviewed_at TEXT NOT NULL,
    source_url TEXT
);

CREATE INDEX IF NOT EXISTS pipeline_runs_state_idx ON pipeline_runs(current_state, updated_at);
CREATE INDEX IF NOT EXISTS pipeline_evidence_current_idx ON pipeline_evidence(lead_id, signal_key, is_current);
CREATE INDEX IF NOT EXISTS pipeline_qualification_latest_idx ON pipeline_qualification_results(pipeline_run_id, version DESC);
"""


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _dict_row(row):
    return None if row is None else dict(row)


def _table_exists(database: Database, table: str) -> bool:
    return database.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def _is_durable_database(database: Database) -> bool:
    return database.path.resolve() == DEFAULT_DB_PATH.resolve()


def _ensure_step7(database: Database) -> None:
    if not _table_exists(database, "pipeline_runs"):
        migrate_step7(database)


def migrate_step7(database_or_path):
    """Apply the additive Step 7 v9 schema without changing safety defaults."""
    if hasattr(database_or_path, "connection"):
        database = database_or_path
        close_after = False
    else:
        database = Database(Path(database_or_path))
        close_after = True
    try:
        migrate_step6(database)
        with database.connection:
            evidence_columns = {
                row[1] for row in database.connection.execute("PRAGMA table_info(evidence)")
            }
            if "pipeline_current" not in evidence_columns:
                database.connection.execute(
                    "ALTER TABLE evidence ADD COLUMN pipeline_current INTEGER NOT NULL DEFAULT 1"
                )
            database.connection.executescript(_STEP7_SCHEMA)
            run_columns = {
                row[1] for row in database.connection.execute("PRAGMA table_info(pipeline_runs)")
            }
            if "attempt_count" not in run_columns:
                database.connection.execute(
                    "ALTER TABLE pipeline_runs ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0"
                )
            if "max_attempts" not in run_columns:
                database.connection.execute(
                    "ALTER TABLE pipeline_runs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 2"
                )
            database.connection.execute(
                """INSERT OR IGNORE INTO system_config (key, value, value_type)
                   VALUES ('daily_candidate_processing_cap', '0', 'integer')"""
            )
            database.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
                   VALUES (?, ?, ?)""",
                (STEP7_MIGRATION_VERSION, "step7_resumable_pipeline_orchestration", _now()),
            )
        return STEP7_MIGRATION_VERSION
    finally:
        if close_after:
            database.close()


def _set_config(database: Database, key: str, value: Any) -> None:
    database.set_config(key, value)


def _prepare_fixture_environment(database: Database, campaign_id: int, record_count: int) -> None:
    """Enable only a temporary non-durable fixture database for mocked runs."""
    if _is_durable_database(database):
        raise PipelineBlockedError("fictional fixture override is forbidden for the durable database")
    _set_config(database, "system_state", "ACTIVE")
    _set_config(database, "discovery_mode", "LIVE")
    _set_config(database, "daily_candidate_processing_cap", max(1, record_count))
    _set_config(database, "website_enrichment_daily_quota", max(2 * record_count, 1))
    with database.connection:
        database.connection.execute("UPDATE campaigns SET status='INACTIVE'")
        database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (campaign_id,))


def pipeline_readiness(database: Database, *, campaign_id: Optional[int] = None) -> dict:
    """Return bounded readiness gates without returning credentials or contacts."""
    _ensure_step7(database)
    config = database.read_config()
    campaign = database.get_campaign(campaign_id) if campaign_id is not None else None
    today = _now()[:10] + "%"
    used = database.connection.execute(
        "SELECT COUNT(*) FROM pipeline_runs WHERE started_at LIKE ?", (today,)
    ).fetchone()[0]
    cap = int(config.get("daily_candidate_processing_cap", 0))
    website_quota = int(config.get("website_enrichment_daily_quota", 0))
    gates = {
        "system_not_paused": config.get("system_state") != "PAUSED",
        "discovery_live": config.get("discovery_mode") == "LIVE",
        "campaign_active": bool(campaign and campaign["status"] == "ACTIVE"),
        "positive_daily_candidate_cap": cap > 0,
        "candidate_cap_available": cap > used,
        "positive_website_enrichment_quota": website_quota > 0,
    }
    reason_map = {
        "system_not_paused": "SYSTEM_PAUSED",
        "discovery_live": "DISCOVERY_NOT_LIVE",
        "campaign_active": "CAMPAIGN_INACTIVE_OR_MISSING",
        "positive_daily_candidate_cap": "DAILY_CANDIDATE_CAP_ZERO",
        "candidate_cap_available": "DAILY_CANDIDATE_CAP_EXHAUSTED",
        "positive_website_enrichment_quota": "WEBSITE_ENRICHMENT_QUOTA_ZERO",
    }
    return {
        "ready": all(gates.values()),
        "gates": gates,
        "blocking_reasons": [reason_map[key] for key, value in gates.items() if not value],
        "campaign_id": campaign["id"] if campaign else None,
        "system_state": config.get("system_state"),
        "discovery_mode": config.get("discovery_mode"),
        "daily_candidate_processing_cap": cap,
        "candidates_processed_today": used,
        "remaining_candidate_capacity": max(0, cap - used),
        "website_enrichment_daily_quota": website_quota,
        "outreach_created_by_step7": False,
    }


def _normalise_fixture_evidence(items: Optional[Iterable[Mapping[str, Any]]]) -> list[dict]:
    normalised = []
    for item in items or ():
        if not isinstance(item, Mapping):
            raise ValueError("fixture evidence items must be mappings")
        signal_key = str(item.get("signal_key", "")).strip()
        if signal_key not in SUPPORTED_SIGNAL_KEYS:
            raise ValueError(f"unsupported evidence signal: {signal_key}")
        signal_value = item.get("signal_value", "UNCLEAR")
        if signal_value not in TRI_STATE_VALUES:
            raise ValueError("evidence signal values must be YES, NO, or UNCLEAR")
        observed_or_inferred = item.get("observed_or_inferred", "OBSERVED")
        if observed_or_inferred not in {"OBSERVED", "INFERRED"}:
            raise ValueError("observed_or_inferred must be OBSERVED or INFERRED")
        observation = " ".join(str(item.get("observation", "")).split())[:500]
        if not observation:
            raise ValueError("fixture evidence requires a concise observation")
        confidence = item.get("confidence", 1.0)
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError("evidence confidence must be between 0 and 1")
        normalised.append(
            {
                "signal_key": signal_key,
                "signal_value": signal_value,
                "observed_or_inferred": observed_or_inferred,
                "source_type": str(item.get("source_type", "FIXTURE_PUBLIC_WEB"))[:80],
                "source_url": item.get("source_url"),
                "observed_at": str(item.get("observed_at", _now())),
                "confidence": float(confidence),
                "observation": observation,
            }
        )
    return normalised


def _normalise_gate_statuses(statuses: Optional[Mapping[str, str]]) -> dict[str, str]:
    result = {}
    for key, value in (statuses or {}).items():
        if key not in MANDATORY_GATES:
            raise ValueError(f"unsupported mandatory gate: {key}")
        if value not in TRI_STATE_VALUES:
            raise ValueError("mandatory gate statuses must be YES, NO, or UNCLEAR")
        result[key] = value
    return result


def _value(record: Any, key: str, default=None):
    if isinstance(record, Mapping):
        return record.get(key, default)
    try:
        return record[key]
    except (KeyError, IndexError, TypeError):
        return getattr(record, key, default)


def _has_alternative_evidence(candidate: Mapping[str, Any]) -> bool:
    routes = sum(
        bool(_value(candidate, key))
        for key in ("normalized_email", "normalized_phone", "booking_url")
    )
    return routes >= 2


def _preview_outcome(database: Database, candidate) -> str:
    if database.is_suppressed(
        email=candidate.normalized_email,
        domain=candidate.normalized_domain,
    ):
        return IngestionOutcome.SUPPRESSED.value
    status = (candidate.business_status or "").upper()
    if status in _INACTIVE_BUSINESS_STATUSES:
        return IngestionOutcome.INACTIVE_BUSINESS.value
    if not candidate.business_name or not candidate.source_record_id:
        return IngestionOutcome.INVALID.value
    if not (candidate.normalized_domain or candidate.normalized_email or candidate.normalized_phone):
        return IngestionOutcome.INSUFFICIENT_IDENTITY.value
    existing = database.connection.execute(
        """SELECT 1 FROM discovery_candidates
           WHERE source=? AND source_record_id=?
              OR (normalized_email IS NOT NULL AND normalized_email=?)
              OR (normalized_domain IS NOT NULL AND normalized_domain=?)
           LIMIT 1""",
        (candidate.source, candidate.source_record_id, candidate.normalized_email, candidate.normalized_domain),
    ).fetchone()
    return IngestionOutcome.DUPLICATE.value if existing else IngestionOutcome.ACCEPTED.value


def preview_candidate_processing(database: Database, raw_record: Mapping[str, Any], *, source: str = APIFY_SOURCE) -> dict:
    """Preview the deterministic route with no candidate, lead, or pipeline writes."""
    _ensure_step7(database)
    candidate = normalize_candidate(
        raw_record,
        source=source,
        mode=DiscoveryMode.DRY_RUN,
        provenance_type="FIXTURE",
    )
    outcome = _preview_outcome(database, candidate)
    return {
        "outcome": outcome,
        "would_create_lead": outcome in {IngestionOutcome.ACCEPTED.value, IngestionOutcome.UPDATED.value},
        "website_enrichment_eligible": outcome in {IngestionOutcome.ACCEPTED.value, IngestionOutcome.UPDATED.value}
        and bool(candidate.website_url),
        "missing_website": not bool(candidate.website_url),
        "sufficient_alternative_evidence": _has_alternative_evidence(candidate),
        "source_record_id_present": bool(candidate.source_record_id),
        "provenance_type": "FIXTURE",
    }


def _get_run(database: Database, run_id: int):
    row = database.connection.execute("SELECT * FROM pipeline_runs WHERE id=?", (run_id,)).fetchone()
    if row is None:
        raise ValueError("pipeline run does not exist")
    return row


def _checkpoint(database: Database, run_id: int, key: str, details: Optional[Mapping[str, Any]] = None) -> None:
    timestamp = _now()
    database.connection.execute(
        """INSERT OR IGNORE INTO pipeline_checkpoints
           (pipeline_run_id, checkpoint_key, completed_at, details_json)
           VALUES (?, ?, ?, ?)""",
        (run_id, key, timestamp, _json(dict(details or {}))),
    )
    database.connection.execute(
        """UPDATE pipeline_runs
           SET last_successful_checkpoint=?, updated_at=?
           WHERE id=?""",
        (key, timestamp, run_id),
    )
    database.connection.commit()


def _has_checkpoint(database: Database, run_id: int, key: str) -> bool:
    return database.connection.execute(
        "SELECT 1 FROM pipeline_checkpoints WHERE pipeline_run_id=? AND checkpoint_key=?",
        (run_id, key),
    ).fetchone() is not None


def _checkpoint_details(database: Database, run_id: int, key: str) -> dict:
    row = database.connection.execute(
        "SELECT details_json FROM pipeline_checkpoints WHERE pipeline_run_id=? AND checkpoint_key=?",
        (run_id, key),
    ).fetchone()
    return {} if row is None else json.loads(row["details_json"])


def _update_run(database: Database, run_id: int, **values) -> None:
    values["updated_at"] = _now()
    assignments = ", ".join(f"{key}=?" for key in values)
    database.connection.execute(
        f"UPDATE pipeline_runs SET {assignments} WHERE id=?",
        (*values.values(), run_id),
    )
    database.connection.commit()


def _record_event(database: Database, event_type: str, run_id: int, metadata: Mapping[str, Any]) -> None:
    database.connection.execute(
        """INSERT INTO events (event_type, entity_type, entity_id, metadata, created_at)
           VALUES (?, 'pipeline_run', ?, ?, ?)""",
        (event_type, str(run_id), _json(dict(metadata)), _now()),
    )
    database.connection.commit()


def _run_is_suppressed(database: Database, run) -> bool:
    candidate = database.connection.execute(
        "SELECT normalized_email, normalized_domain FROM discovery_candidates WHERE id=?",
        (run["candidate_id"],),
    ).fetchone()
    if run["lead_id"] is not None and database.is_suppressed(lead_id=run["lead_id"]):
        return True
    if candidate is None or not (candidate["normalized_email"] or candidate["normalized_domain"]):
        return False
    return database.is_suppressed(
        email=candidate["normalized_email"], domain=candidate["normalized_domain"]
    )


def _finalize(
    database: Database,
    run_id: int,
    state: str,
    *,
    error_category: Optional[str] = None,
    human_review_required: bool = False,
    checkpoint: Optional[str] = None,
    details: Optional[Mapping[str, Any]] = None,
    resumable: bool = False,
) -> dict:
    run = _get_run(database, run_id)
    _update_run(
        database,
        run_id,
        current_stage="complete",
        current_state=state,
        safe_error_category=error_category,
        human_review_required=1 if human_review_required else 0,
        completion_state="RUNNING" if resumable else state,
    )
    if run["lead_id"] is not None:
        lead_status = {
            PipelineState.QUALIFIED.value: "QUALIFIED",
            PipelineState.STRONG.value: "QUALIFIED",
            PipelineState.REJECTED.value: "REJECTED",
            PipelineState.HOLD.value: "REVIEW_PENDING",
            PipelineState.SUPPRESSED.value: "DO_NOT_CONTACT",
        }.get(state)
        if lead_status:
            database.connection.execute(
                "UPDATE leads SET status=?, updated_at=? WHERE id=?",
                (lead_status, _now(), run["lead_id"]),
            )
            database.connection.commit()
    if checkpoint:
        _checkpoint(database, run_id, checkpoint, details)
    _record_event(
        database,
        "PIPELINE_COMPLETED" if state in _FINAL_STATES else "PIPELINE_ERROR",
        run_id,
        {"state": state, "safe_error_category": error_category, "human_review_required": human_review_required},
    )
    return summarize_pipeline_outcome(database, run_id)


def _create_or_find_run(database: Database, result, campaign_id: int, fixture_evidence: list[dict], fixture_gates: dict) -> tuple[int, bool]:
    existing = database.connection.execute(
        "SELECT id FROM pipeline_runs WHERE candidate_id=? AND campaign_id=?",
        (result.candidate_id, campaign_id),
    ).fetchone()
    if existing is not None:
        return existing[0], False
    timestamp = _now()
    cursor = database.connection.execute(
        """INSERT INTO pipeline_runs
           (campaign_id, candidate_id, lead_id, current_stage, current_state,
            started_at, updated_at, last_successful_checkpoint, completion_state,
            ingestion_outcome, ingestion_reason, fixture_evidence_json, fixture_gate_statuses_json)
           VALUES (?, ?, ?, 'discovery', ?, ?, ?, '', 'RUNNING', ?, ?, ?, ?)""",
        (
            campaign_id,
            result.candidate_id,
            result.lead_id,
            PipelineState.DISCOVERED.value,
            timestamp,
            timestamp,
            result.status,
            result.reason,
            _json(fixture_evidence),
            _json(fixture_gates),
        ),
    )
    database.connection.commit()
    if cursor.lastrowid is None:
        raise RuntimeError("pipeline run insert did not return an id")
    return cursor.lastrowid, True


def _evidence_hash(item: Mapping[str, Any]) -> str:
    stable = {
        "signal_key": item["signal_key"],
        "signal_value": item["signal_value"],
        "observed_or_inferred": item["observed_or_inferred"],
        "source_type": item["source_type"],
        "source_url": item.get("source_url"),
        "observation": item["observation"],
    }
    return hashlib.sha256(_json(stable).encode("utf-8")).hexdigest()


def _evidence_strength(item: Mapping[str, Any]) -> tuple:
    value = _value(item, "signal_value")
    value_rank = {"UNCLEAR": 1, "YES": 2, "NO": 2}[value]
    observed_rank = 1 if _value(item, "observed_or_inferred") == "OBSERVED" else 0
    return value_rank, float(_value(item, "confidence") or 0), observed_rank, str(_value(item, "observed_at", ""))


def _store_pipeline_evidence(database: Database, run_id: int, lead_id: int, item: Mapping[str, Any]) -> bool:
    item = dict(item)
    item["evidence_hash"] = _evidence_hash(item)
    existing = database.connection.execute(
        """SELECT id FROM pipeline_evidence
           WHERE lead_id=? AND signal_key=? AND evidence_hash=?""",
        (lead_id, item["signal_key"], item["evidence_hash"]),
    ).fetchone()
    if existing is not None:
        return False
    current = database.connection.execute(
        """SELECT * FROM pipeline_evidence
           WHERE lead_id=? AND signal_key=? AND is_current=1
           ORDER BY id DESC LIMIT 1""",
        (lead_id, item["signal_key"]),
    ).fetchone()
    is_current = current is None or _evidence_strength(item) >= _evidence_strength(current)
    evidence_id = database.insert_structured_evidence(
        lead_id=lead_id,
        signal_key=item["signal_key"],
        signal_value=item["signal_value"],
        source_type=item["source_type"],
        observation=item["observation"],
        source_url=item.get("source_url"),
        confidence=item.get("confidence"),
        observed_or_inferred=item["observed_or_inferred"],
        collected_at=item.get("observed_at"),
    )
    if not is_current:
        database.connection.execute("UPDATE evidence SET pipeline_current=0 WHERE id=?", (evidence_id,))
    elif current is not None:
        database.connection.execute("UPDATE pipeline_evidence SET is_current=0 WHERE id=?", (current["id"],))
        if current["evidence_id"] is not None:
            database.connection.execute("UPDATE evidence SET pipeline_current=0 WHERE id=?", (current["evidence_id"],))
    cursor = database.connection.execute(
        """INSERT INTO pipeline_evidence
           (pipeline_run_id, lead_id, evidence_id, signal_key, signal_value,
            observed_or_inferred, source_type, source_url, observed_at, confidence,
            observation, evidence_hash, supersedes_id, is_current)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            lead_id,
            evidence_id,
            item["signal_key"],
            item["signal_value"],
            item["observed_or_inferred"],
            item["source_type"],
            item.get("source_url"),
            item.get("observed_at", _now()),
            item.get("confidence"),
            item["observation"],
            item["evidence_hash"],
            current["id"] if is_current and current is not None else None,
            1 if is_current else 0,
        ),
    )
    database.connection.commit()
    return cursor.lastrowid is not None


def _candidate_evidence(candidate: Mapping[str, Any]) -> list[dict]:
    items = []
    if _value(candidate, "normalized_email"):
        items.append(
            {
                "signal_key": "public_business_email",
                "signal_value": "YES",
                "observed_or_inferred": "OBSERVED",
                "source_type": "DISCOVERY",
                "source_url": _value(candidate, "source_url"),
                "observed_at": _value(candidate, "collected_at") or _now(),
                "confidence": 0.8,
                "observation": "Discovery record lists a public business email contact.",
            }
        )
    return items


def _website_evidence(database: Database, website_run_id: Optional[int]) -> list[dict]:
    if website_run_id is None:
        return []
    rows = database.connection.execute(
        """SELECT signal_key, signal_value, observation, source_url, observed_at
           FROM workflow_evidence WHERE run_id=? ORDER BY id""",
        (website_run_id,),
    ).fetchall()
    items = []
    for row in rows:
        mapped = _WEBSITE_SIGNAL_MAP.get(row["signal_key"])
        if mapped not in SUPPORTED_SIGNAL_KEYS:
            continue
        items.append(
            {
                "signal_key": mapped,
                "signal_value": row["signal_value"],
                "observed_or_inferred": "OBSERVED",
                "source_type": "WEBSITE",
                "source_url": row["source_url"],
                "observed_at": row["observed_at"],
                "confidence": 0.9 if row["signal_value"] == "YES" else 0.5,
                "observation": row["observation"] or "Website observation remains UNCLEAR.",
            }
        )
    return items


def _consolidate_evidence(database: Database, run) -> int:
    candidate = database.connection.execute(
        "SELECT * FROM discovery_candidates WHERE id=?", (run["candidate_id"],)
    ).fetchone()
    if candidate is None or run["lead_id"] is None:
        return 0
    items = _candidate_evidence(candidate)
    items.extend(json.loads(run["fixture_evidence_json"]))
    items.extend(_website_evidence(database, run["website_run_id"]))
    stored = 0
    for item in items:
        if _store_pipeline_evidence(database, run["id"], run["lead_id"], item):
            stored += 1
    return stored


def _derived_gate_statuses(database: Database, run) -> dict[str, str]:
    candidate = database.connection.execute(
        "SELECT * FROM discovery_candidates WHERE id=?", (run["candidate_id"],)
    ).fetchone()
    statuses = {gate: "UNCLEAR" for gate in MANDATORY_GATES}
    if candidate is None or run["lead_id"] is None:
        return statuses
    signals = database._get_signal_statuses(run["lead_id"])
    business_status = (candidate["business_status"] or "").upper()
    if business_status in _ACTIVE_BUSINESS_STATUSES:
        statuses["currently_active"] = "YES"
    elif business_status in _INACTIVE_BUSINESS_STATUSES:
        statuses["currently_active"] = "NO"

    appointment_keys = {
        "appointment_dependence", "appointment_channels", "website_appointment_request_form",
        "website_booking_page_widget", "phone_booking", "messenger_booking", "instagram_booking",
        "whatsapp_booking", "google_business_booking", "multiple_booking_channels",
    }
    if signals.get("non_appointment_dependent") == "YES":
        statuses["appointments_meaningful"] = "NO"
    elif signals.get("appointment_dependence") == "YES" or any(signals.get(key) == "YES" for key in appointment_keys):
        statuses["appointments_meaningful"] = "YES"

    has_contact = bool(candidate["normalized_email"] or candidate["normalized_phone"] or candidate["booking_url"])
    if signals.get("no_legitimate_public_contact_route") == "YES":
        statuses["legitimate_public_contact_route"] = "NO"
    elif has_contact or any(signals.get(key) == "YES" for key in {"public_business_email", "appointment_channels", "phone_booking", "website_appointment_request_form", "website_booking_page_widget"}):
        statuses["legitimate_public_contact_route"] = "YES"

    if signals.get("owner_decision_maker_reachability") in TRI_STATE_VALUES:
        statuses["independent_owner_led_or_accessible_local_decision_maker"] = signals["owner_decision_maker_reachability"]
    return statuses


def _qualification_final_state(result: Mapping[str, Any]) -> str:
    if result["exclusion_reasons"]:
        return PipelineState.REJECTED.value
    failed = [key for key, value in result["mandatory_gates"].items() if value == "NO"]
    unclear = [key for key, value in result["mandatory_gates"].items() if value == "UNCLEAR"]
    if failed:
        return PipelineState.REJECTED.value
    if unclear:
        return PipelineState.HOLD.value
    if result["qualification_result"] == "REJECT":
        return PipelineState.REJECTED.value
    return PipelineState.STRONG.value if result["classification"] == "STRONG" else PipelineState.QUALIFIED.value


def _qualification_fingerprint(database: Database, run_id: int, gates: Mapping[str, str]) -> str:
    rows = database.connection.execute(
        """SELECT signal_key, signal_value, evidence_hash FROM pipeline_evidence
           WHERE pipeline_run_id=? AND is_current=1 ORDER BY signal_key, evidence_hash""",
        (run_id,),
    ).fetchall()
    payload = {
        "gates": dict(sorted(gates.items())),
        "evidence": [dict(row) for row in rows],
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _qualify_and_finalize(database: Database, run_id: int, *, interrupt_after: Optional[str] = None) -> dict:
    run = _get_run(database, run_id)
    if run["lead_id"] is None:
        return summarize_pipeline_outcome(database, run_id)
    if _run_is_suppressed(database, run):
        return _finalize(
            database, run_id, PipelineState.SUPPRESSED.value,
            error_category="SUPPRESSED", checkpoint="qualification",
        )
    derived = _derived_gate_statuses(database, run)
    derived.update(json.loads(run["fixture_gate_statuses_json"]))
    derived.update(json.loads(run["review_gate_statuses_json"]))
    fingerprint = _qualification_fingerprint(database, run_id, derived)
    previous = database.connection.execute(
        """SELECT * FROM pipeline_qualification_results
           WHERE pipeline_run_id=? AND evidence_fingerprint=?""",
        (run_id, fingerprint),
    ).fetchone()
    if previous is not None:
        return summarize_pipeline_outcome(database, run_id)
    _update_run(database, run_id, current_stage="qualification", current_state=PipelineState.QUALIFICATION_PENDING.value)
    result = database.qualify_lead(lead_id=run["lead_id"], campaign_id=run["campaign_id"], gate_statuses=derived)
    final_state = _qualification_final_state(result)
    version = database.connection.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM pipeline_qualification_results WHERE pipeline_run_id=?",
        (run_id,),
    ).fetchone()[0]
    database.connection.execute(
        """INSERT INTO pipeline_qualification_results
           (pipeline_run_id, lead_id, campaign_id, version, score, classification,
            qualification_result, final_state, gate_statuses, evidence_fingerprint,
            reasoning_data, evaluated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id,
            run["lead_id"],
            run["campaign_id"],
            version,
            result["score"],
            result["classification"],
            result["qualification_result"],
            final_state,
            _json(result["mandatory_gates"]),
            fingerprint,
            _json(result),
            _now(),
        ),
    )
    database.connection.commit()
    error_category = None
    if final_state == PipelineState.HOLD.value:
        error_category = "MANDATORY_GATE_UNCLEAR"
    elif final_state == PipelineState.REJECTED.value:
        if any(value == "NO" for value in result["mandatory_gates"].values()):
            error_category = "MANDATORY_GATE_FAILED"
        elif result["score"] < 70:
            error_category = "QUALIFICATION_BELOW_THRESHOLD"
        else:
            error_category = "QUALIFICATION_REJECTED"
    summary = _finalize(
        database,
        run_id,
        final_state,
        error_category=error_category,
        human_review_required=final_state == PipelineState.HOLD.value,
        checkpoint="qualification",
        details={"version": version, "score": result["score"], "final_state": final_state},
    )
    if interrupt_after == "qualification":
        raise PipelineInterrupted(run_id, "qualification")
    return summary


def _process_run(database: Database, run_id: int, *, http_client=None, dns_client=None, interrupt_after: Optional[str] = None) -> dict:
    run = _get_run(database, run_id)
    if run["completion_state"] != "RUNNING":
        return summarize_pipeline_outcome(database, run_id)
    outcome = run["ingestion_outcome"]
    if _run_is_suppressed(database, run) or outcome == IngestionOutcome.SUPPRESSED.value:
        return _finalize(database, run_id, PipelineState.SUPPRESSED.value, error_category="SUPPRESSED", checkpoint="discovery_ingestion")
    if outcome in {IngestionOutcome.INVALID.value, IngestionOutcome.INACTIVE_BUSINESS.value, IngestionOutcome.DUPLICATE.value}:
        return _finalize(database, run_id, PipelineState.REJECTED.value, error_category=f"INGESTION_{outcome}", checkpoint="discovery_ingestion")
    if outcome in {IngestionOutcome.POSSIBLE_DUPLICATE_HOLD.value, IngestionOutcome.INSUFFICIENT_IDENTITY.value}:
        return _finalize(database, run_id, PipelineState.HOLD.value, error_category=f"INGESTION_{outcome}", human_review_required=True, checkpoint="discovery_ingestion")
    if outcome not in {IngestionOutcome.ACCEPTED.value, IngestionOutcome.UPDATED.value}:
        return _finalize(
            database, run_id, PipelineState.ERROR_RETRYABLE.value,
            error_category="INGESTION_SOURCE_ERROR", human_review_required=True, resumable=True,
        )

    if not _has_checkpoint(database, run_id, "discovery_ingestion"):
        _update_run(database, run_id, current_stage="ingestion", current_state=PipelineState.INGESTION_ACCEPTED.value)
        _checkpoint(database, run_id, "discovery_ingestion", {"outcome": outcome})
        if interrupt_after == "discovery_ingestion":
            raise PipelineInterrupted(run_id, "discovery_ingestion")
    run = _get_run(database, run_id)
    candidate = database.connection.execute(
        "SELECT * FROM discovery_candidates WHERE id=?", (run["candidate_id"],)
    ).fetchone()
    if candidate is None or run["lead_id"] is None:
        return _finalize(database, run_id, PipelineState.HOLD.value, error_category="LEAD_IDENTITY_MISSING", human_review_required=True)

    enrichment = _checkpoint_details(database, run_id, "enrichment") if _has_checkpoint(database, run_id, "enrichment") else None
    if enrichment is None:
        _update_run(database, run_id, current_stage="enrichment", current_state=PipelineState.ENRICHMENT_PENDING.value)
        if not candidate["website_url"]:
            if not _has_alternative_evidence(candidate):
                return _finalize(
                    database, run_id, PipelineState.HOLD.value,
                    error_category="MISSING_WEBSITE_AND_ALTERNATIVE_EVIDENCE",
                    human_review_required=True,
                    checkpoint="enrichment",
                    details={"skipped": "missing_website_insufficient_alternative_evidence"},
                )
            enrichment = {"skipped": "missing_website_sufficient_alternative_evidence", "state": "SKIPPED"}
        else:
            if http_client is None or dns_client is None:
                raise PipelineBlockedError("fixture enrichment requires injected HTTP and DNS clients")
            attempt_count = run["attempt_count"] + 1
            _update_run(database, run_id, attempt_count=attempt_count)
            run = _get_run(database, run_id)
            try:
                enrichment_result = enrich_website_candidate(
                    database,
                    run["candidate_id"],
                    run["campaign_id"],
                    operator_confirmation=True,
                    http_client=http_client,
                    dns_client=dns_client,
                )
            except WebsiteEnrichmentBlockedError as error:
                return _finalize(database, run_id, PipelineState.ERROR_TERMINAL.value, error_category="ENRICHMENT_BLOCKED", human_review_required=True)
            enrichment = dict(enrichment_result)
            _update_run(database, run_id, website_run_id=enrichment_result["run_id"])
            if enrichment_result["state"] != "COMPLETED":
                if enrichment_result["state"] == "SOURCE_ERROR" and attempt_count < run["max_attempts"]:
                    return _finalize(
                        database, run_id, PipelineState.ERROR_RETRYABLE.value,
                        error_category=enrichment_result.get("error_category") or enrichment_result["state"],
                        human_review_required=True, resumable=True,
                    )
                error_state = PipelineState.ERROR_TERMINAL.value
                error_category = "RETRY_LIMIT_EXCEEDED" if enrichment_result["state"] == "SOURCE_ERROR" else enrichment_result.get("error_category") or enrichment_result["state"]
                return _finalize(database, run_id, error_state, error_category=error_category, human_review_required=True)
        _update_run(database, run_id, current_stage="enrichment", current_state=PipelineState.ENRICHMENT_COMPLETE.value)
        _checkpoint(database, run_id, "enrichment", enrichment)
        if interrupt_after == "enrichment":
            raise PipelineInterrupted(run_id, "enrichment")

    run = _get_run(database, run_id)
    if not _has_checkpoint(database, run_id, "evidence_consolidation"):
        _update_run(database, run_id, current_stage="evidence", current_state=PipelineState.ENRICHMENT_COMPLETE.value)
        stored = _consolidate_evidence(database, run)
        _checkpoint(database, run_id, "evidence_consolidation", {"new_evidence_count": stored})
        if interrupt_after == "evidence_consolidation":
            raise PipelineInterrupted(run_id, "evidence_consolidation")
    if not _has_checkpoint(database, run_id, "lead_creation"):
        _checkpoint(database, run_id, "lead_creation", {"lead_id": run["lead_id"], "idempotent": True})
        if interrupt_after == "lead_creation":
            raise PipelineInterrupted(run_id, "lead_creation")
    return _qualify_and_finalize(database, run_id, interrupt_after=interrupt_after)


def run_fictional_pipeline_fixture(
    database: Database,
    *,
    campaign_id: int,
    records: Iterable[Mapping[str, Any]],
    evidence_by_source_record: Optional[Mapping[str, Iterable[Mapping[str, Any]]]] = None,
    gate_statuses_by_source_record: Optional[Mapping[str, Mapping[str, str]]] = None,
    http_client=None,
    dns_client=None,
    source: str = APIFY_SOURCE,
    interrupt_after: Optional[str] = None,
) -> list[dict]:
    """Run fictional records only, with injected HTTP/DNS clients and temp-db gates."""
    if _is_durable_database(database):
        raise PipelineBlockedError("fictional fixture override cannot target the durable database")
    if http_client is None or dns_client is None:
        raise PipelineBlockedError("fictional pipeline fixtures require injected HTTP and DNS clients")
    _ensure_step7(database)
    records = list(records)
    _prepare_fixture_environment(database, campaign_id, len(records))
    evidence_by_source_record = evidence_by_source_record or {}
    gate_statuses_by_source_record = gate_statuses_by_source_record or {}
    results = []
    for raw_record in records:
        candidate = normalize_candidate(
            raw_record,
            source=source,
            mode=DiscoveryMode.DRY_RUN,
            provenance_type="FIXTURE",
        )
        fixture_evidence = _normalise_fixture_evidence(evidence_by_source_record.get(candidate.source_record_id, ()))
        fixture_gates = _normalise_gate_statuses(gate_statuses_by_source_record.get(candidate.source_record_id, {}))
        ingestion = ingest_candidate(database, candidate)
        run_id, created = _create_or_find_run(database, ingestion, campaign_id, fixture_evidence, fixture_gates)
        if not created:
            run = _get_run(database, run_id)
            if run["completion_state"] != "RUNNING":
                results.append(summarize_pipeline_outcome(database, run_id))
                continue
        results.append(_process_run(database, run_id, http_client=http_client, dns_client=dns_client, interrupt_after=interrupt_after))
    return results


def run_pipeline(database: Database, records: Iterable[Mapping[str, Any]], *, campaign_id: int, provenance_type: str = "LIVE", **kwargs) -> list[dict]:
    """Guarded public entry point; LIVE execution is intentionally not implemented."""
    _ensure_step7(database)
    if provenance_type != "FIXTURE":
        readiness = pipeline_readiness(database, campaign_id=campaign_id)
        if not readiness["ready"]:
            raise PipelineBlockedError("live pipeline blocked: " + ", ".join(readiness["blocking_reasons"]))
        raise PipelineBlockedError("LIVE pipeline execution is not implemented in Step 7")
    if not kwargs.pop("fixture_override", False):
        raise PipelineBlockedError("fixture provenance requires fixture_override=True")
    return run_fictional_pipeline_fixture(database, campaign_id=campaign_id, records=records, **kwargs)


def resume_pipeline(database: Database, run_id: int, *, http_client=None, dns_client=None) -> dict:
    """Resume one run from its last successful checkpoint."""
    _ensure_step7(database)
    run = _get_run(database, run_id)
    if run["completion_state"] != "RUNNING":
        return summarize_pipeline_outcome(database, run_id)
    return _process_run(database, run_id, http_client=http_client, dns_client=dns_client)


def inspect_hold_reason(database: Database, run_id: int) -> dict:
    _ensure_step7(database)
    run = _get_run(database, run_id)
    latest = database.connection.execute(
        """SELECT * FROM pipeline_qualification_results
           WHERE pipeline_run_id=? ORDER BY version DESC LIMIT 1""",
        (run_id,),
    ).fetchone()
    gates = {} if latest is None else json.loads(latest["gate_statuses"])
    return {
        "run_id": run_id,
        "state": run["current_state"],
        "safe_error_category": run["safe_error_category"],
        "human_review_required": bool(run["human_review_required"]),
        "unclear_gates": [key for key, value in gates.items() if value == "UNCLEAR"],
        "failed_gates": [key for key, value in gates.items() if value == "NO"],
        "reason": (latest and json.loads(latest["reasoning_data"]).get("reason")) or run["ingestion_reason"],
    }


def apply_operator_review(
    database: Database,
    run_id: int,
    *,
    exact_gate_or_signal: str,
    decision: str,
    reason: str,
    reviewer_identity: str = "OPERATOR",
    source_url: Optional[str] = None,
) -> dict:
    """Record one bounded OPERATOR review; it never activates or outreaches."""
    _ensure_step7(database)
    run = _get_run(database, run_id)
    if _run_is_suppressed(database, run):
        raise SuppressedPipelineError("suppression overrides manual review")
    if run["lead_id"] is None:
        raise ValueError("manual review requires a lead")
    if exact_gate_or_signal not in set(MANDATORY_GATES) | set(SUPPORTED_SIGNAL_KEYS):
        raise ValueError("exact_gate_or_signal must be an explicit gate or supported signal")
    if decision not in TRI_STATE_VALUES:
        raise ValueError("decision must be YES, NO, or UNCLEAR")
    if reviewer_identity != "OPERATOR":
        raise ValueError("reviewer_identity must be OPERATOR")
    reason = " ".join(str(reason).split())[:500]
    if not reason:
        raise ValueError("manual review requires a concise reason")
    if source_url is not None:
        parsed = urlsplit(source_url)
        if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password or not parsed.hostname:
            raise ValueError("source_url must be a public HTTP or HTTPS URL without credentials")
    reviewed_at = _now()
    cursor = database.connection.execute(
        """INSERT INTO manual_reviews
           (pipeline_run_id, lead_id, exact_gate_or_signal, decision, reason,
            reviewer_identity, reviewed_at, source_url)
           VALUES (?, ?, ?, ?, ?, 'OPERATOR', ?, ?)""",
        (run_id, run["lead_id"], exact_gate_or_signal, decision, reason, reviewed_at, source_url),
    )
    database.connection.commit()
    evidence_added = False
    if exact_gate_or_signal in MANDATORY_GATES:
        overrides = json.loads(run["review_gate_statuses_json"])
        overrides[exact_gate_or_signal] = decision
        _update_run(database, run_id, review_gate_statuses_json=_json(overrides), human_review_required=0)
    else:
        evidence_added = _store_pipeline_evidence(
            database,
            run_id,
            run["lead_id"],
            {
                "signal_key": exact_gate_or_signal,
                "signal_value": decision,
                "observed_or_inferred": "INFERRED",
                "source_type": "MANUAL_REVIEW",
                "source_url": source_url,
                "observed_at": reviewed_at,
                "confidence": 1.0,
                "observation": "OPERATOR review decision: " + reason,
            },
        )
    _record_event(
        database,
        "OPERATOR_REVIEW_RECORDED",
        run_id,
        {"exact_gate_or_signal": exact_gate_or_signal, "decision": decision, "reviewer_identity": "OPERATOR"},
    )
    return {
        "review_id": cursor.lastrowid,
        "run_id": run_id,
        "lead_id": run["lead_id"],
        "exact_gate_or_signal": exact_gate_or_signal,
        "decision": decision,
        "reviewer_identity": "OPERATOR",
        "reviewed_at": reviewed_at,
        "evidence_added": evidence_added,
    }


def requalify_after_new_evidence(
    database: Database,
    run_id: int,
    *,
    new_evidence: Optional[Iterable[Mapping[str, Any]]] = None,
    gate_statuses: Optional[Mapping[str, str]] = None,
) -> dict:
    """Add deterministic evidence and create a new qualification version if changed."""
    _ensure_step7(database)
    run = _get_run(database, run_id)
    if run["lead_id"] is None:
        return summarize_pipeline_outcome(database, run_id)
    if _run_is_suppressed(database, run):
        return _finalize(database, run_id, PipelineState.SUPPRESSED.value, error_category="SUPPRESSED")
    items = _normalise_fixture_evidence(new_evidence)
    for item in items:
        _store_pipeline_evidence(database, run_id, run["lead_id"], item)
    if gate_statuses:
        current = json.loads(run["fixture_gate_statuses_json"])
        current.update(_normalise_gate_statuses(gate_statuses))
        _update_run(database, run_id, fixture_gate_statuses_json=_json(current), completion_state="RUNNING")
    else:
        _update_run(database, run_id, completion_state="RUNNING")
    _update_run(database, run_id, current_stage="qualification", current_state=PipelineState.QUALIFICATION_PENDING.value)
    return _qualify_and_finalize(database, run_id)


def _qualification_summary(database: Database, run_id: int) -> Optional[dict]:
    row = database.connection.execute(
        """SELECT * FROM pipeline_qualification_results
           WHERE pipeline_run_id=? ORDER BY version DESC LIMIT 1""",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    result = json.loads(row["reasoning_data"])
    result["version"] = row["version"]
    result["final_state"] = row["final_state"]
    return result


def summarize_pipeline_outcome(database: Database, run_id: Optional[int] = None) -> dict:
    """Return a safe run or aggregate summary; contact values are deliberately omitted."""
    _ensure_step7(database)
    if run_id is None:
        rows = database.connection.execute(
            "SELECT current_state, COUNT(*) AS count FROM pipeline_runs GROUP BY current_state ORDER BY current_state"
        ).fetchall()
        return {
            "run_count": sum(row["count"] for row in rows),
            "states": {row["current_state"]: row["count"] for row in rows},
            "outreach_count": database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0],
            "active_campaign_count": database.connection.execute("SELECT COUNT(*) FROM campaigns WHERE status='ACTIVE'").fetchone()[0],
            "outreach_created_by_step7": False,
        }
    run = _get_run(database, run_id)
    enrichment = _checkpoint_details(database, run_id, "enrichment") if _has_checkpoint(database, run_id, "enrichment") else None
    return {
        "run_id": run_id,
        "campaign_id": run["campaign_id"],
        "candidate_id": run["candidate_id"],
        "lead_id": run["lead_id"],
        "state": run["current_state"],
        "current_stage": run["current_stage"],
        "started_at": run["started_at"],
        "updated_at": run["updated_at"],
        "last_successful_checkpoint": run["last_successful_checkpoint"],
        "safe_error_category": run["safe_error_category"],
        "human_review_required": bool(run["human_review_required"]),
        "completion_state": run["completion_state"],
        "attempt_count": run["attempt_count"],
        "max_attempts": run["max_attempts"],
        "ingestion_outcome": run["ingestion_outcome"],
        "enrichment": enrichment,
        "qualification": _qualification_summary(database, run_id),
        "outreach_count": database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0],
        "outreach_created_by_step7": False,
    }


__all__ = [
    "PipelineBlockedError",
    "PipelineInterrupted",
    "PipelineState",
    "STEP7_MIGRATION_VERSION",
    "SuppressedPipelineError",
    "apply_operator_review",
    "inspect_hold_reason",
    "migrate_step7",
    "pipeline_readiness",
    "preview_candidate_processing",
    "requalify_after_new_evidence",
    "resume_pipeline",
    "run_fictional_pipeline_fixture",
    "run_pipeline",
    "summarize_pipeline_outcome",
]
