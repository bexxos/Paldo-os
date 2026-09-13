"""Step 10: bounded, fixture-only personalized email drafting.

The engine assembles a small, provenance-preserving provider input from the
existing qualified-lead, evidence, active-offer, and Gold-context layers.  It
never calls a live model or messaging system.  TEST_V0_1 is intentionally an
experimental policy version and can be replaced without changing stored draft
history or the engine contract.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urlsplit

from paldo_os_outbound import DEFAULT_DB_PATH, Database, normalize_email
from step8_knowledge_context import (
    REQUIRED_GOLD_SECTIONS,
    get_current_context_packet,
    migrate_step8,
)
from step9_audit_offer import (
    IMMEDIATE_OFFER_CTA,
    IMMEDIATE_OFFER_KEY,
    get_immediate_offer_version,
    migrate_step9,
)


STEP10_MIGRATION_VERSION = 12
DRAFTING_POLICY_VERSION = "TEST_V0_1"
DRAFTING_PROVIDER_MODE = "FIXTURE_ONLY"
DRAFTING_MAX_ATTEMPTS = 2
EVIDENCE_MAX_AGE_DAYS = 90

ALLOWED_CLAIM_TYPES = {"OBSERVATION", "INFERENCE", "OFFER", "CTA"}
DRAFTING_RUN_STATES = frozenset(
    {
        "ELIGIBILITY_PENDING",
        "READY_FOR_GENERATION",
        "GENERATED",
        "VALIDATED",
        "REVIEW_PENDING",
        "BLOCKED",
        "HOLD_FOR_REVIEW",
        "ERROR_RETRYABLE",
        "ERROR_TERMINAL",
        "REJECTED",
    }
)
DRAFTING_SAFE_TERMINAL_STATES = frozenset({"REVIEW_PENDING", "BLOCKED", "HOLD_FOR_REVIEW", "ERROR_TERMINAL", "REJECTED"})

PROHIBITED_CLAIM_TERMS = (
    "guarantee",
    "guaranteed",
    "roi",
    "revenue",
    "savings",
    "save money",
    "no-show",
    "no show",
    "client",
    "diagnos",
    "treatment",
    "prescription",
    "medical advice",
    "health data",
    "free ai workflow audit",
    "workflow audit",
    "audit",
    "limited spots",
    "act now",
    "last chance",
    "today only",
    "hurry",
    "attachment",
    "tracking pixel",
    "unsubscribe",
)
INJECTION_TERMS = (
    "ignore previous",
    "ignore all previous",
    "system prompt",
    "developer message",
    "user instruction",
    "follow these instructions",
    "jailbreak",
    "reveal the prompt",
)
DECEPTIVE_LINK_RE = re.compile(r"(?:https?://|www\.|bit\.ly/|tinyurl\.com/|t\.co/)", re.IGNORECASE)
HTML_RE = re.compile(r"</?[a-z][^>]*>|<!doctype|<script\b", re.IGNORECASE)
PERSON_NAME_RE = re.compile(r"\b(?:owner|founder|director|manager|dr\.?|doctor)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b")
WORD_RE = re.compile(r"\b[\w'-]+\b", re.UNICODE)


class DraftingBlockedError(RuntimeError):
    """Raised when a live/durable drafting operation is attempted or blocked."""


class DraftingValidationError(ValueError):
    """Raised for malformed Step 10 API calls or unauthorized redrafts."""


class FixtureDraftingProvider:
    """Tiny injected provider used only by fictional tests and local fixtures."""

    def __init__(self, *, output: Any = None, outputs: Optional[Iterable[Any]] = None):
        self.output = output
        self.outputs = list(outputs or [])
        self.calls = 0
        self.inputs: list[dict[str, Any]] = []

    def generate(self, provider_input: Mapping[str, Any]) -> Any:
        self.calls += 1
        self.inputs.append(deepcopy(dict(provider_input)))
        if self.outputs:
            return self.outputs.pop(0)
        if self.output is not None:
            return deepcopy(self.output)
        raise RuntimeError("fixture provider has no configured output")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS drafting_runs (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    offer_version_id INTEGER REFERENCES audit_offer_versions(id) ON DELETE RESTRICT,
    packet_id INTEGER REFERENCES kb_context_packets(id) ON DELETE RESTRICT,
    redraft_request_id INTEGER,
    prompt_version TEXT NOT NULL,
    provider_mode TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('ELIGIBILITY_PENDING','READY_FOR_GENERATION','GENERATED','VALIDATED','REVIEW_PENDING','BLOCKED','HOLD_FOR_REVIEW','ERROR_RETRYABLE','ERROR_TERMINAL','REJECTED')),
    input_json TEXT NOT NULL,
    base_fingerprint TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL UNIQUE,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0 AND attempt_count <= 2),
    max_attempts INTEGER NOT NULL DEFAULT 2 CHECK (max_attempts BETWEEN 1 AND 2),
    last_error_category TEXT,
    blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS drafting_runs_lead_idx ON drafting_runs(lead_id, campaign_id, created_at);
CREATE INDEX IF NOT EXISTS drafting_runs_state_idx ON drafting_runs(state, created_at);

CREATE TABLE IF NOT EXISTS drafting_provider_attempts (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES drafting_runs(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL CHECK (attempt_number BETWEEN 1 AND 2),
    status TEXT NOT NULL CHECK (status IN ('STARTED','SUCCEEDED','VALIDATION_FAILED','ERROR_RETRYABLE','ERROR_TERMINAL')),
    error_category TEXT,
    safe_detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (run_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS drafting_attempts_run_idx ON drafting_provider_attempts(run_id, attempt_number);

CREATE TABLE IF NOT EXISTS personalized_drafts (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES drafting_runs(id) ON DELETE RESTRICT,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    offer_version_id INTEGER NOT NULL REFERENCES audit_offer_versions(id) ON DELETE RESTRICT,
    packet_id INTEGER NOT NULL REFERENCES kb_context_packets(id) ON DELETE RESTRICT,
    version INTEGER NOT NULL CHECK (version > 0),
    state TEXT NOT NULL CHECK (state IN ('GENERATED','VALIDATED','REVIEW_PENDING','HOLD_FOR_REVIEW','REJECTED')),
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    claims_json TEXT NOT NULL,
    evidence_ids_json TEXT NOT NULL,
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    cta_text TEXT NOT NULL,
    personalization_summary TEXT NOT NULL,
    prohibited_claim_self_check_json TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (lead_id, campaign_id, offer_version_id, version),
    UNIQUE (input_fingerprint)
);
CREATE INDEX IF NOT EXISTS personalized_drafts_review_idx ON personalized_drafts(state, updated_at);

CREATE TABLE IF NOT EXISTS draft_claim_evidence (
    id INTEGER PRIMARY KEY,
    draft_id INTEGER NOT NULL REFERENCES personalized_drafts(id) ON DELETE CASCADE,
    claim_index INTEGER NOT NULL,
    claim_text TEXT NOT NULL,
    claim_type TEXT NOT NULL CHECK (claim_type IN ('OBSERVATION','INFERENCE','OFFER','CTA')),
    evidence_id INTEGER REFERENCES evidence(id) ON DELETE RESTRICT,
    support_reason TEXT NOT NULL,
    UNIQUE (draft_id, claim_index, evidence_id)
);
CREATE INDEX IF NOT EXISTS draft_claim_evidence_draft_idx ON draft_claim_evidence(draft_id, claim_index);

CREATE TABLE IF NOT EXISTS draft_validation_results (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES drafting_runs(id) ON DELETE CASCADE,
    draft_id INTEGER REFERENCES personalized_drafts(id) ON DELETE RESTRICT,
    attempt_id INTEGER REFERENCES drafting_provider_attempts(id) ON DELETE RESTRICT,
    validation_state TEXT NOT NULL CHECK (validation_state IN ('PASS','FAIL')),
    error_codes_json TEXT NOT NULL,
    safe_details_json TEXT NOT NULL,
    validator_version TEXT NOT NULL,
    validated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS draft_validation_run_idx ON draft_validation_results(run_id, validated_at);

CREATE TABLE IF NOT EXISTS draft_redraft_requests (
    id INTEGER PRIMARY KEY,
    source_draft_id INTEGER NOT NULL REFERENCES personalized_drafts(id) ON DELETE RESTRICT,
    requested_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('REQUESTED','COMPLETED','REJECTED')),
    new_run_id INTEGER,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS draft_redraft_source_idx ON draft_redraft_requests(source_draft_id, created_at);
"""


def _now(value: Optional[datetime] = None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc, microsecond=0)
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: Optional[datetime] = None) -> str:
    return _now(value).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _parse_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("timestamp required")
    return _now(datetime.fromisoformat(value.strip().replace("Z", "+00:00")))


def _is_durable_database(database: Database) -> bool:
    try:
        return database.path.resolve() == DEFAULT_DB_PATH.resolve()
    except (AttributeError, OSError):
        return False


def _safe_url(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise DraftingValidationError("source URL must be text")
    parsed = urlsplit(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise DraftingValidationError("source URL must be public HTTP or HTTPS without credentials")
    return value.strip()


def migrate_step10(database_or_path: Database | str | Path) -> int:
    """Apply the additive Step 10 schema and disabled fixture-only defaults."""
    database = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    # The legacy lower-step migration functions are intentionally idempotent,
    # but their campaign seed reconciliation resets seeded campaign status to
    # INACTIVE.  Only call that prerequisite migration before Step 10 exists;
    # normal Step 10 reads must never mutate an operator's campaign state.
    step10_exists = database.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
    ).fetchone() is not None and database.connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (STEP10_MIGRATION_VERSION,)
    ).fetchone() is not None
    if not step10_exists:
        migrate_step9(database)
    with database.connection:
        database.connection.executescript(_SCHEMA)
        database.connection.executemany(
            "INSERT OR IGNORE INTO system_config (key, value, value_type) VALUES (?, ?, ?)",
            (
                ("drafting_enabled", "0", "integer"),
                ("drafting_daily_cap", "0", "integer"),
                ("drafting_provider_mode", DRAFTING_PROVIDER_MODE, "text"),
                ("drafting_max_attempts", str(DRAFTING_MAX_ATTEMPTS), "integer"),
            ),
        )
        database.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
            (STEP10_MIGRATION_VERSION, "step10_fixture_drafting_engine", _iso()),
        )
    return STEP10_MIGRATION_VERSION


def _lead_decision(database: Database, lead_id: int, campaign_id: int) -> str:
    if database._table_has_column("pipeline_qualification_results", "final_state"):
        row = database.connection.execute(
            """SELECT final_state, classification, qualification_result
               FROM pipeline_qualification_results
               WHERE lead_id=? AND campaign_id=? ORDER BY version DESC, id DESC LIMIT 1""",
            (lead_id, campaign_id),
        ).fetchone()
        if row is not None:
            return str(row["final_state"] or row["classification"] or row["qualification_result"] or "UNKNOWN").upper()
    row = database.connection.execute(
        """SELECT classification, qualification_result FROM qualification_results
           WHERE lead_id=? AND campaign_id=? ORDER BY evaluated_at DESC LIMIT 1""",
        (lead_id, campaign_id),
    ).fetchone()
    if row is not None:
        value = str(row["classification"] or row["qualification_result"] or "UNKNOWN").upper()
        return "QUALIFIED" if value == "QUALIFY" else value
    lead = database.get_lead(lead_id)
    return str(lead.get("status") if lead else "UNKNOWN").upper()


def _suppressed(database: Database, lead: Mapping[str, Any]) -> bool:
    try:
        return database.is_suppressed(
            lead_id=lead["id"],
            email=lead.get("email"),
            domain=lead.get("domain"),
        )
    except (TypeError, ValueError):
        # A malformed contact must still be evaluated as an ordinary eligibility
        # failure; suppression lookup must never turn it into an unsafe bypass.
        return database.is_suppressed(lead_id=lead["id"])


def _current_evidence(
    database: Database,
    lead_id: int,
    *,
    evidence_ids: Optional[Iterable[int]],
    now: datetime,
) -> tuple[list[dict[str, Any]], list[str]]:
    requested = None if evidence_ids is None else list(evidence_ids)
    stale_legacy_rows: list[Any] = []
    if requested is not None and (not requested or any(isinstance(item, bool) or not isinstance(item, int) for item in requested)):
        return [], ["CURRENT_EVIDENCE_REQUIRED"]
    if requested is None:
        rows = database.connection.execute(
            "SELECT * FROM evidence WHERE lead_id=? AND signal_key IS NOT NULL ORDER BY id", (lead_id,)
        ).fetchall()
        stale_legacy_rows = [row for row in rows if "pipeline_current" in row.keys() and row["pipeline_current"] == 0]
        legacy_current = [row for row in rows if "pipeline_current" not in row.keys() or row["pipeline_current"] != 0]
        if rows and "pipeline_current" in rows[0].keys():
            rows = legacy_current
        if not legacy_current and database._table_has_column("pipeline_evidence", "is_current"):
            rows = database.connection.execute(
                "SELECT * FROM pipeline_evidence WHERE lead_id=? AND is_current=1 ORDER BY id", (lead_id,)
            ).fetchall()
    else:
        placeholders = ",".join("?" for _ in requested)
        rows = database.connection.execute(
            f"SELECT * FROM evidence WHERE lead_id=? AND id IN ({placeholders}) ORDER BY id",
            (lead_id, *requested),
        ).fetchall()
        stale_legacy_rows = [row for row in rows if "pipeline_current" in row.keys() and row["pipeline_current"] == 0]
        legacy_current = {
            row["id"] for row in rows
            if "pipeline_current" not in row.keys() or row["pipeline_current"] != 0
        }
        if database._table_has_column("pipeline_evidence", "is_current") and legacy_current != set(requested):
            pipeline_rows = database.connection.execute(
                f"SELECT * FROM pipeline_evidence WHERE lead_id=? AND id IN ({placeholders}) AND is_current=1 ORDER BY id",
                (lead_id, *requested),
            ).fetchall()
            rows = [row for row in rows if row["id"] in legacy_current] + [
                row for row in pipeline_rows if row["id"] not in legacy_current
            ]
    by_id = {row["id"]: row for row in rows}
    reasons: list[str] = []
    current_signal_keys = {row["signal_key"] for row in rows if "signal_key" in row.keys()}
    if any(row["signal_key"] not in current_signal_keys for row in stale_legacy_rows):
        reasons.append("CURRENT_EVIDENCE_REQUIRED")
    if requested is not None and set(requested) != set(by_id):
        reasons.append("EVIDENCE_ID_NOT_FOUND")
    result: list[dict[str, Any]] = []
    for row in rows:
        if "pipeline_current" in row.keys() and row["pipeline_current"] == 0:
            reasons.append("CURRENT_EVIDENCE_REQUIRED")
            continue
        captured = (
            row["collected_at"] if "collected_at" in row.keys() and row["collected_at"]
            else row["captured_at"] if "captured_at" in row.keys() and row["captured_at"]
            else row["observed_at"]
        )
        try:
            observed_at = _parse_time(captured)
        except (TypeError, ValueError):
            reasons.append("EVIDENCE_TIMESTAMP_INVALID")
            continue
        if observed_at < now - timedelta(days=EVIDENCE_MAX_AGE_DAYS) or observed_at > now + timedelta(days=1):
            reasons.append("EVIDENCE_EXPIRED")
            continue
        observation = row["observation"]
        if not isinstance(observation, str) or not observation.strip() or len(observation) > 1000:
            reasons.append("EVIDENCE_NOT_CONCISE")
            continue
        if HTML_RE.search(observation):
            reasons.append("RAW_HTML_NOT_ALLOWED")
            continue
        try:
            source_url = _safe_url(row["source_url"])
        except DraftingValidationError:
            reasons.append("EVIDENCE_SOURCE_URL_INVALID")
            continue
        confidence = row["confidence"]
        if confidence is None or isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            reasons.append("EVIDENCE_CONFIDENCE_INVALID")
            continue
        observed_or_inferred = row["observed_or_inferred"] if "observed_or_inferred" in row.keys() else "OBSERVED"
        if observed_or_inferred not in {"OBSERVED", "INFERRED"}:
            reasons.append("EVIDENCE_CLASSIFICATION_INVALID")
            continue
        result.append(
            {
                "evidence_id": row["id"],
                "signal_key": row["signal_key"],
                "signal_value": row["signal_value"],
                "observation": observation.strip(),
                "observed_or_inferred": observed_or_inferred,
                "source_type": row["source_type"] if "source_type" in row.keys() else row["evidence_type"] if "evidence_type" in row.keys() else "UNKNOWN",
                "source_url": source_url,
                "observed_at": _iso(observed_at),
                "confidence": float(confidence),
                "status": "CURRENT",
            }
        )
    return result, sorted(set(reasons))


def _packet_sections(packet: Mapping[str, Any]) -> dict[str, str]:
    content = str(packet.get("approved_content") or "")
    sections: dict[str, str] = {}
    for index, section in enumerate(REQUIRED_GOLD_SECTIONS):
        start = content.find(f"[{section}]\n")
        if start < 0:
            continue
        start += len(section) + 3
        end = len(content)
        for next_section in REQUIRED_GOLD_SECTIONS[index + 1 :]:
            candidate = content.find(f"\n\n[{next_section}]", start)
            if candidate >= 0:
                end = min(end, candidate)
        sections[section] = content[start:end].strip()
    return sections


def _ready_packet(database: Database, campaign_id: int, packet_id: Optional[int], now: datetime) -> tuple[Optional[dict[str, Any]], list[str]]:
    if packet_id is None:
        row = database.connection.execute(
            """SELECT * FROM kb_context_packets
               WHERE campaign_id=? AND readiness_state='READY' AND invalidation_reason IS NULL
               ORDER BY id DESC LIMIT 1""",
            (campaign_id,),
        ).fetchone()
    else:
        row = database.connection.execute(
            """SELECT * FROM kb_context_packets
               WHERE id=? AND campaign_id=? AND readiness_state='READY' AND invalidation_reason IS NULL""",
            (packet_id, campaign_id),
        ).fetchone()
    if row is None:
        return None, ["READY_GOLD_CONTEXT_REQUIRED"]
    try:
        if _parse_time(row["expires_at"]) <= now:
            with database.connection:
                database.connection.execute("UPDATE kb_context_packets SET readiness_state='EXPIRED' WHERE id=?", (row["id"],))
            return None, ["READY_GOLD_CONTEXT_REQUIRED"]
    except (TypeError, ValueError):
        return None, ["GOLD_PROVENANCE_INVALID"]
    packet = dict(row)
    packet["packet_id"] = packet["id"]
    packet["approved_content"] = packet.get("packet_content")
    if packet is None or packet.get("readiness_state") != "READY" or packet.get("invalidation_reason"):
        return None, ["READY_GOLD_CONTEXT_REQUIRED"]
    source_count = database.connection.execute(
        "SELECT COUNT(*) FROM kb_context_packet_sources WHERE packet_id=?", (packet["packet_id"],)
    ).fetchone()[0]
    reasons: list[str] = []
    if source_count < len(REQUIRED_GOLD_SECTIONS):
        reasons.append("GOLD_PROVENANCE_INCOMPLETE")
    other_ready = database.connection.execute(
        """SELECT COUNT(*) FROM kb_context_packets
           WHERE campaign_id=? AND readiness_state='READY' AND invalidation_reason IS NULL AND id<>?""",
        (campaign_id, packet["packet_id"]),
    ).fetchone()[0]
    if other_ready:
        reasons.append("CONFLICTING_GOLD_CONTEXT")
    sections = _packet_sections(packet)
    missing = [section for section in REQUIRED_GOLD_SECTIONS if not sections.get(section)]
    if missing:
        reasons.append("GOLD_SECTIONS_INCOMPLETE")
    if reasons:
        return None, sorted(set(reasons))
    return packet, []


def _active_offer(database: Database, offer_key: str) -> tuple[Optional[dict[str, Any]], list[str]]:
    if offer_key != IMMEDIATE_OFFER_KEY:
        return None, ["DEFERRED_AUDIT_OFFER_FORBIDDEN"]
    row = database.connection.execute(
        "SELECT * FROM audit_offer_versions WHERE offer_key=? AND version=1", (IMMEDIATE_OFFER_KEY,)
    ).fetchone()
    if row is None:
        return None, ["ACTIVE_IMMEDIATE_OFFER_REQUIRED"]
    ctas = database.connection.execute(
        "SELECT role, cta_text FROM audit_offer_cta_variants WHERE offer_version_id=? ORDER BY role", (row["id"],)
    ).fetchall()
    offer = dict(row)
    offer["ctas"] = {cta["role"]: cta["cta_text"] for cta in ctas}
    if offer["status"] != "ACTIVE":
        return None, ["ACTIVE_IMMEDIATE_OFFER_REQUIRED"]
    if offer.get("offer_key") != IMMEDIATE_OFFER_KEY or offer.get("name") != "Business Booking and Follow-Up System":
        return None, ["ACTIVE_OFFER_MISMATCH"]
    cta = offer.get("ctas", {}).get("PRIMARY")
    if cta != IMMEDIATE_OFFER_CTA:
        return None, ["APPROVED_CTA_MISMATCH"]
    return offer, []


def _campaign(database: Database, lead: Mapping[str, Any], campaign_id: int) -> tuple[Optional[dict[str, Any]], list[str]]:
    campaign = database.get_campaign(campaign_id)
    reasons: list[str] = []
    if campaign is None:
        return None, ["CAMPAIGN_NOT_FOUND"]
    if campaign.get("status") != "ACTIVE":
        reasons.append("CAMPAIGN_INACTIVE")
    if lead.get("campaign_id") != campaign_id:
        reasons.append("BUSINESS_CAMPAIGN_MISMATCH")
    industry = str(lead.get("industry") or "").casefold()
    if campaign.get("category_key") in {"local_services_primary", "local_services_secondary"} and not ("local service" in industry or "local" in industry):
        reasons.append("BUSINESS_CATEGORY_MISMATCH")
    return campaign, reasons


def _config_reasons(database: Database) -> list[str]:
    config = database.read_config()
    reasons: list[str] = []
    if config.get("system_state") != "ACTIVE":
        reasons.append("SYSTEM_PAUSED")
    if not bool(config.get("drafting_enabled")):
        reasons.append("DRAFTING_DISABLED")
    if int(config.get("drafting_daily_cap", 0)) <= 0:
        reasons.append("DRAFTING_DAILY_CAP_ZERO")
    if config.get("drafting_provider_mode") != DRAFTING_PROVIDER_MODE:
        reasons.append("DRAFTING_PROVIDER_MODE_UNSAFE")
    max_attempts = int(config.get("drafting_max_attempts", DRAFTING_MAX_ATTEMPTS))
    if max_attempts < 1 or max_attempts > DRAFTING_MAX_ATTEMPTS:
        reasons.append("DRAFTING_ATTEMPT_LIMIT_UNSAFE")
    return reasons


def _daily_cap_reached(database: Database, now: datetime) -> bool:
    cap = int(database.get_config("drafting_daily_cap", 0) or 0)
    if cap <= 0:
        return True
    start = _iso(now.replace(hour=0, minute=0, second=0))
    count = database.connection.execute("SELECT COUNT(*) FROM drafting_runs WHERE created_at>=?", (start,)).fetchone()[0]
    return count >= cap


def _build_provider_input(
    *,
    lead: Mapping[str, Any],
    campaign: Mapping[str, Any],
    offer: Mapping[str, Any],
    evidence: list[dict[str, Any]],
    packet: Mapping[str, Any],
    message_policy_context: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    sections = _packet_sections(packet)
    rendered_cta = IMMEDIATE_OFFER_CTA.replace("[Business Name]", lead["business_name"])
    return {
        "policy_version": DRAFTING_POLICY_VERSION,
        "business_name": lead["business_name"],
        "campaign_name": campaign["name"],
        "business_category": lead.get("industry") or campaign.get("segment_name"),
        "evidence": evidence,
        "offer": {
            "name": offer["name"],
            "version": offer["version"],
            "cta": rendered_cta,
        },
        "gold_context": {
            "voice": sections["VOICE_GUIDE"],
            "proof_rules": sections["APPROVED_PROOF_RULES"],
            "outbound_policy": sections["OUTBOUND_COMPLIANCE_POLICY"],
            "campaign_decision": sections["ACTIVE_CAMPAIGN_DECISION"],
        },
        "prohibited_claims": list(PROHIBITED_CLAIM_TERMS),
        "limits": {
            "subject_characters": [3, 120],
            "body_words": [70, 120],
            "exactly_one_cta": True,
            "plain_text_only": True,
        },
        "message_policy_version": None if message_policy_context is None else message_policy_context.get("message_policy_version"),
        "message_policy": None if message_policy_context is None else deepcopy(dict(message_policy_context)),
    }


def _fingerprint(
    *,
    lead: Mapping[str, Any],
    campaign: Mapping[str, Any],
    offer: Optional[Mapping[str, Any]],
    packet: Optional[Mapping[str, Any]],
    evidence: list[Mapping[str, Any]],
    redraft_request_id: Optional[int] = None,
    message_policy_context: Optional[Mapping[str, Any]] = None,
) -> tuple[str, str]:
    evidence_part = [
        {
            "id": item["evidence_id"],
            "signal_key": item["signal_key"],
            "observation": item["observation"],
            "observed_at": item["observed_at"],
            "confidence": item["confidence"],
        }
        for item in evidence
    ]
    base = {
        "lead_id": lead["id"],
        "campaign_id": campaign["id"],
        "offer": None if offer is None else {"id": offer["id"], "version": offer["version"], "name": offer["name"], "cta": offer.get("ctas", {}).get("PRIMARY")},
        "packet_hash": None if packet is None else packet.get("packet_hash"),
        "evidence": evidence_part,
        "message_policy": None if message_policy_context is None else dict(message_policy_context),
        "prompt_version": DRAFTING_POLICY_VERSION,
    }
    base_fingerprint = hashlib.sha256(_json(base).encode()).hexdigest()
    full = dict(base)
    full["redraft_request_id"] = redraft_request_id
    input_fingerprint = hashlib.sha256(_json(full).encode()).hexdigest()
    return base_fingerprint, input_fingerprint


def _eligibility(
    database: Database,
    *,
    lead_id: int,
    campaign_id: int,
    packet_id: Optional[int],
    offer_key: str,
    evidence_ids: Optional[Iterable[int]],
    now: datetime,
    message_policy_context: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    migrate_step10(database)
    lead = database.get_lead(lead_id)
    reasons: list[str] = []
    if lead is None:
        return {"ready": False, "blocking_reasons": ["LEAD_NOT_FOUND"], "lead": None, "campaign": None, "offer": None, "packet": None, "evidence": []}
    campaign, campaign_reasons = _campaign(database, lead, campaign_id)
    reasons.extend(campaign_reasons)
    decision = _lead_decision(database, lead_id, campaign_id)
    if decision not in {"QUALIFIED", "STRONG"}:
        reasons.append("LEAD_NOT_QUALIFIED")
    if _suppressed(database, lead):
        reasons.append("SUPPRESSED")
    try:
        normalize_email(lead.get("email") or "")
        if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", lead["email"]):
            raise ValueError
    except (TypeError, ValueError):
        reasons.append("BUSINESS_EMAIL_INVALID")
    evidence, evidence_reasons = _current_evidence(database, lead_id, evidence_ids=evidence_ids, now=now)
    reasons.extend(evidence_reasons)
    verified_email = [
        item for item in evidence
        if item["signal_key"] == "public_business_email"
        and item["signal_value"] == "YES"
        and item["observed_or_inferred"] == "OBSERVED"
        and item["source_type"] in {"FIXTURE_VERIFIED_EMAIL", "PUBLIC_CONTACT_VERIFICATION"}
        and item["confidence"] >= 0.8
    ]
    if not verified_email:
        reasons.append("VERIFIED_PUBLIC_BUSINESS_EMAIL_REQUIRED")
    if not any(item["signal_key"] == "personalization_observation" and item["signal_value"] == "YES" and item["observed_or_inferred"] == "OBSERVED" for item in evidence):
        reasons.append("PERSONALIZATION_EVIDENCE_REQUIRED")
    if not any(item["signal_key"] == "campaign_fit" and item["signal_value"] == "YES" for item in evidence):
        reasons.append("CAMPAIGN_EVIDENCE_REQUIRED")
    offer, offer_reasons = _active_offer(database, offer_key)
    reasons.extend(offer_reasons)
    packet, packet_reasons = _ready_packet(database, campaign_id, packet_id, now) if campaign is not None else (None, ["READY_GOLD_CONTEXT_REQUIRED"])
    reasons.extend(packet_reasons)
    if packet is not None and offer is not None:
        sections = _packet_sections(packet)
        active_section = sections.get("ACTIVE_OFFER_CTA", "")
        campaign_section = sections.get("ACTIVE_CAMPAIGN_DECISION", "")
        if offer["name"] not in active_section or IMMEDIATE_OFFER_CTA not in active_section:
            reasons.append("GOLD_ACTIVE_OFFER_CTA_MISMATCH")
        if "audit" in active_section.casefold() or "free ai workflow audit" in campaign_section.casefold():
            reasons.append("DEFERRED_AUDIT_CONTEXT_FORBIDDEN")
        if campaign and campaign["name"] not in campaign_section:
            reasons.append("GOLD_CAMPAIGN_DECISION_MISMATCH")
    reasons.extend(_config_reasons(database))
    if _daily_cap_reached(database, now):
        reasons.append("DRAFTING_DAILY_CAP_REACHED")
    reasons = sorted(set(reasons))
    provider_input = None
    if not reasons and campaign is not None and offer is not None and packet is not None:
        provider_input = _build_provider_input(
            lead=lead,
            campaign=campaign,
            offer=offer,
            evidence=evidence,
            packet=packet,
            message_policy_context=message_policy_context,
        )
    base_fingerprint, input_fingerprint = _fingerprint(
        lead=lead,
        campaign=campaign or {"id": campaign_id},
        offer=offer,
        packet=packet,
        evidence=evidence,
        message_policy_context=message_policy_context,
    )
    return {
        "ready": not reasons,
        "blocking_reasons": reasons,
        "lead": lead,
        "campaign": campaign,
        "offer": offer,
        "packet": packet,
        "evidence": evidence,
        "provider_input": provider_input,
        "base_fingerprint": base_fingerprint,
        "input_fingerprint": input_fingerprint,
        "decision": decision,
    }


def inspect_drafting_readiness(
    database: Database,
    *,
    lead_id: int,
    campaign_id: int,
    packet_id: Optional[int] = None,
    offer_key: str = IMMEDIATE_OFFER_KEY,
    evidence_ids: Optional[Iterable[int]] = None,
    message_policy_context: Optional[Mapping[str, Any]] = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    result = _eligibility(
        database,
        lead_id=lead_id,
        campaign_id=campaign_id,
        packet_id=packet_id,
        offer_key=offer_key,
        evidence_ids=evidence_ids,
        message_policy_context=message_policy_context,
        now=_now(now),
    )
    return {
        "ready": result["ready"],
        "lead_id": lead_id,
        "campaign_id": campaign_id,
        "decision": result.get("decision"),
        "blocking_reasons": result["blocking_reasons"],
        "offer_key": None if result.get("offer") is None else result["offer"]["offer_key"],
        "offer_version": None if result.get("offer") is None else result["offer"]["version"],
        "packet_id": None if result.get("packet") is None else result["packet"]["packet_id"],
        "evidence_ids": [item["evidence_id"] for item in result.get("evidence", [])],
        "provider_input_available": result.get("provider_input") is not None,
    }


def preview_bounded_drafting_input(
    database: Database,
    *,
    lead_id: int,
    campaign_id: int,
    packet_id: Optional[int] = None,
    offer_key: str = IMMEDIATE_OFFER_KEY,
    evidence_ids: Optional[Iterable[int]] = None,
    message_policy_context: Optional[Mapping[str, Any]] = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    result = _eligibility(
        database,
        lead_id=lead_id,
        campaign_id=campaign_id,
        packet_id=packet_id,
        offer_key=offer_key,
        evidence_ids=evidence_ids,
        message_policy_context=message_policy_context,
        now=_now(now),
    )
    return {
        "ready": result["ready"],
        "blocking_reasons": result["blocking_reasons"],
        "provider_input": deepcopy(result["provider_input"]),
        "input_fingerprint": result.get("input_fingerprint"),
        "policy_version": DRAFTING_POLICY_VERSION,
    }


def _strict_output(provider_output: Any) -> tuple[Optional[dict[str, Any]], list[dict[str, str]]]:
    errors: list[dict[str, str]] = []
    if isinstance(provider_output, str):
        try:
            provider_output = json.loads(provider_output)
        except json.JSONDecodeError:
            return None, [{"code": "OUTPUT_JSON_INVALID", "message": "provider output is not valid JSON"}]
    if not isinstance(provider_output, Mapping):
        return None, [{"code": "OUTPUT_OBJECT_REQUIRED", "message": "provider output must be a JSON object"}]
    output = dict(provider_output)
    required = {
        "subject", "body", "claims", "evidence_ids_used", "confidence", "cta_text",
        "personalization_summary", "prohibited_claim_self_check",
    }
    if set(output) != required:
        errors.append({"code": "OUTPUT_KEYS_INVALID", "message": "provider output keys must match the strict contract"})
    return output, errors


def _err(errors: list[dict[str, str]], code: str, message: str) -> None:
    if code not in {item["code"] for item in errors}:
        errors.append({"code": code, "message": message})


def validate_provider_output(provider_input: Mapping[str, Any], provider_output: Any) -> dict[str, Any]:
    """Validate strict fixture-provider JSON against bounded current input."""
    if not isinstance(provider_input, Mapping):
        raise DraftingValidationError("provider input must be an object")
    output, errors = _strict_output(provider_output)
    if output is None:
        return {"valid": False, "errors": errors}
    evidence = {item["evidence_id"]: item for item in provider_input.get("evidence", []) if isinstance(item, Mapping)}
    offer_value = provider_input.get("offer")
    offer: Mapping[str, Any] = offer_value if isinstance(offer_value, Mapping) else {}
    business_name = str(provider_input.get("business_name") or "")
    expected_cta = str(offer.get("cta") or "")
    offer_name = str(offer.get("name") or "")
    message_policy_version = provider_input.get("message_policy_version")
    versioned_policy = message_policy_version is not None
    message_policy = provider_input.get("message_policy")
    if not versioned_policy and provider_input.get("policy_version") != DRAFTING_POLICY_VERSION:
        _err(errors, "POLICY_VERSION_INVALID", "experimental policy version must be TEST_V0_1")
    subject = output.get("subject")
    body = output.get("body")
    if not isinstance(subject, str) or "\n" in subject or not 3 <= len(subject.strip()) <= 120:
        _err(errors, "SUBJECT_LENGTH_INVALID", "subject must be concise and one line")
    elif re.match(r"^\s*(re|fwd|fw)\s*:", subject, re.IGNORECASE):
        _err(errors, "SUBJECT_PREFIX_FORBIDDEN", "Re: and Fwd: subjects are forbidden")
    if not isinstance(body, str):
        _err(errors, "BODY_TEXT_REQUIRED", "body must be plain text")
        body = ""
    word_count = len(WORD_RE.findall(body))
    if not 70 <= word_count <= 120:
        _err(errors, "BODY_WORD_COUNT_INVALID", "body must be approximately 70–120 words")
    if HTML_RE.search(str(subject)) or HTML_RE.search(body):
        _err(errors, "HTML_FORBIDDEN", "HTML is not allowed")
    if not versioned_policy and (DECEPTIVE_LINK_RE.search(body) or DECEPTIVE_LINK_RE.search(str(subject))):
        _err(errors, "DECEPTIVE_LINK_FORBIDDEN", "links and shorteners are not allowed")
    lowered = (str(subject) + "\n" + body).casefold()
    if not versioned_policy:
        for term in PROHIBITED_CLAIM_TERMS:
            if term in lowered:
                _err(errors, "PROHIBITED_CONTENT", "prohibited claim or messaging language detected")
                break
    for term in INJECTION_TERMS:
        if term in lowered:
            _err(errors, "UNTRUSTED_INSTRUCTION_REJECTED", "source/provider instruction text cannot become message content")
            break
    if not isinstance(output.get("claims"), list) or not output["claims"]:
        _err(errors, "CLAIMS_REQUIRED", "claims must be a non-empty list")
        claims: list[Any] = []
    else:
        claims = output["claims"]
    used_ids = output.get("evidence_ids_used")
    if not isinstance(used_ids, list) or any(isinstance(item, bool) or not isinstance(item, int) for item in used_ids) or len(set(used_ids)) != len(used_ids):
        _err(errors, "EVIDENCE_IDS_INVALID", "evidence_ids_used must be a unique integer list")
        used_ids = []
    if not isinstance(output.get("confidence"), (int, float)) or isinstance(output.get("confidence"), bool) or not 0 <= output.get("confidence", -1) <= 1:
        _err(errors, "CONFIDENCE_INVALID", "confidence must be between 0 and 1")
    self_check = output.get("prohibited_claim_self_check")
    if self_check is not True and not (isinstance(self_check, Mapping) and self_check.get("passed") is True):
        _err(errors, "PROHIBITED_SELF_CHECK_FAILED", "provider self-check must pass")
    if not isinstance(output.get("personalization_summary"), str) or not output["personalization_summary"].strip():
        _err(errors, "PERSONALIZATION_SUMMARY_REQUIRED", "personalization summary is required")
    if versioned_policy:
        cta_text = output.get("cta_text")
        if not isinstance(cta_text, str) or not cta_text.strip() or "?" not in cta_text:
            _err(errors, "CTA_INPUT_INVALID", "versioned message CTA must be one plain-text question")
        elif body.count(cta_text) != 1:
            _err(errors, "CTA_COUNT_INVALID", "body must contain exactly one versioned CTA")
    else:
        if expected_cta != IMMEDIATE_OFFER_CTA.replace("[Business Name]", business_name) or "[Business Name]" in expected_cta:
            _err(errors, "CTA_INPUT_INVALID", "input must contain the approved business-name-substituted CTA")
        if output.get("cta_text") != expected_cta:
            _err(errors, "CTA_MISMATCH", "provider CTA must exactly match the active approved CTA")
        if expected_cta and body.count(expected_cta) != 1:
            _err(errors, "CTA_COUNT_INVALID", "body must contain exactly one approved CTA")
        if business_name and expected_cta and business_name not in expected_cta:
            _err(errors, "BUSINESS_NAME_NOT_SUBSTITUTED", "verified business name must replace the CTA placeholder")
    claim_evidence_union: set[int] = set()
    cta_claim_count = 0
    if isinstance(claims, list):
        for index, claim in enumerate(claims):
            if not isinstance(claim, Mapping) or set(claim) != {"text", "claim_type", "evidence_ids"}:
                _err(errors, "CLAIM_SCHEMA_INVALID", "each claim must contain text, claim_type, and evidence_ids only")
                continue
            text = claim.get("text")
            claim_type = claim.get("claim_type")
            claim_ids = claim.get("evidence_ids")
            if not isinstance(text, str) or not text.strip() or not isinstance(claim_type, str) or claim_type not in ALLOWED_CLAIM_TYPES:
                _err(errors, "CLAIM_SCHEMA_INVALID", "claim text and allowed claim type are required")
                continue
            if not isinstance(claim_ids, list) or any(isinstance(item, bool) or not isinstance(item, int) for item in claim_ids):
                _err(errors, "CLAIM_EVIDENCE_INVALID", "claim evidence_ids must be integer IDs")
                claim_ids = []
            claim_evidence_union.update(claim_ids)
            if any(item not in evidence for item in claim_ids):
                _err(errors, "CLAIM_EVIDENCE_NOT_FOUND", "claim references missing or non-current evidence")
            if text not in body or body.count(text) != 1:
                _err(errors, "CLAIM_BODY_REFERENCE_INVALID", "each claim must appear exactly once in the plain-text body")
            if claim_type in {"OBSERVATION", "INFERENCE"} and not claim_ids:
                _err(errors, "CLAIM_EVIDENCE_REQUIRED", "factual claims require evidence IDs")
            if claim_type == "OBSERVATION":
                supported = any(text == item["observation"] or text in item["observation"] or item["observation"] in text for item_id, item in evidence.items() if item_id in claim_ids and item.get("observed_or_inferred") == "OBSERVED")
                if not supported:
                    _err(errors, "CLAIM_NOT_SUPPORTED", "observation claim is not supported by an exact current observation")
                if body.count(text) != 1:
                    _err(errors, "OBSERVATION_REFERENCE_COUNT_INVALID", "verified observation must be referenced exactly once")
            elif claim_type == "INFERENCE":
                if not any(marker in text.casefold() for marker in (" may ", " might ", " could ", " appears ", " seems ", " possibly ", " perhaps ")):
                    _err(errors, "INFERENCE_NOT_CAUTIOUS", "inference must use cautious language")
            elif claim_type == "OFFER":
                if not versioned_policy and offer_name not in text:
                    _err(errors, "OFFER_CLAIM_MISMATCH", "offer claim must match the active offer")
            elif claim_type == "CTA":
                cta_claim_count += 1
                if versioned_policy:
                    if text != output.get("cta_text"):
                        _err(errors, "CTA_MISMATCH", "versioned CTA claim must match cta_text")
                elif text != expected_cta:
                    _err(errors, "CTA_MISMATCH", "CTA claim must exactly match the active approved CTA")
    if cta_claim_count != 1:
        _err(errors, "CTA_CLAIM_COUNT_INVALID", "exactly one CTA claim is required")
    if set(used_ids) != claim_evidence_union:
        _err(errors, "EVIDENCE_ID_SET_MISMATCH", "evidence_ids_used must equal claim evidence references")
    if any(PERSON_NAME_RE.search(str(claim.get("text", ""))) for claim in claims if isinstance(claim, Mapping)):
        _err(errors, "CLAIM_NOT_SUPPORTED", "guessed owner or staff names are forbidden")
    if any(item_id not in evidence or evidence[item_id].get("status") != "CURRENT" for item_id in used_ids):
        _err(errors, "CURRENT_EVIDENCE_REQUIRED", "only current evidence may support claims")
    active_offer_name = "Business Booking and Follow-Up System"
    if not versioned_policy and active_offer_name not in body:
        _err(errors, "ACTIVE_OFFER_NOT_USED", "draft must use the active offer")
    if not versioned_policy and "audit" in lowered:
        _err(errors, "DEFERRED_AUDIT_FORBIDDEN", "deferred audit language is forbidden")
    if versioned_policy:
        if not isinstance(message_policy, Mapping):
            _err(errors, "MESSAGE_POLICY_CONTEXT_REQUIRED", "versioned drafts require a message policy context")
        else:
            from step17b_pilot_policy import validate_versioned_message_policy
            policy_result = validate_versioned_message_policy(
                {"subject": subject, "body": body, "cta_text": output.get("cta_text")},
                message_policy,
            )
            for code in policy_result["error_codes"]:
                _err(errors, code, "versioned message policy check failed")
    return {
        "valid": not errors,
        "errors": errors,
        "word_count": word_count,
        "subject_length": len(subject.strip()) if isinstance(subject, str) else 0,
        "normalized_output": output if not errors else None,
    }


def _safe_detail(errors: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return {"error_codes": sorted({str(item.get("code")) for item in errors})}


def _insert_run(database: Database, eligibility: Mapping[str, Any], *, redraft_request_id: Optional[int], now: datetime) -> int:
    lead = eligibility["lead"]
    campaign = eligibility["campaign"]
    offer = eligibility.get("offer")
    packet = eligibility.get("packet")
    input_json = eligibility.get("provider_input") or {"blocking_reasons": eligibility["blocking_reasons"]}
    base_fingerprint = eligibility.get("base_fingerprint")
    input_fingerprint = eligibility.get("input_fingerprint")
    if not base_fingerprint or not input_fingerprint:
        base_fingerprint, input_fingerprint = _fingerprint(
            lead=lead,
            campaign=campaign,
            offer=offer,
            packet=packet,
            evidence=eligibility.get("evidence", []),
            redraft_request_id=redraft_request_id,
        )
    existing = database.connection.execute("SELECT id FROM drafting_runs WHERE input_fingerprint=?", (input_fingerprint,)).fetchone()
    if existing is not None:
        return existing["id"]
    config = database.read_config()
    state = "READY_FOR_GENERATION" if eligibility["ready"] else "BLOCKED"
    with database.connection:
        cursor = database.connection.execute(
            """INSERT INTO drafting_runs
               (lead_id, campaign_id, offer_version_id, packet_id, redraft_request_id,
                prompt_version, provider_mode, state, input_json, base_fingerprint,
                input_fingerprint, attempt_count, max_attempts, last_error_category,
                blocking_reasons_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)""",
            (
                lead["id"],
                campaign["id"],
                None if offer is None else offer["id"],
                None if packet is None else packet["packet_id"],
                redraft_request_id,
                DRAFTING_POLICY_VERSION,
                config.get("drafting_provider_mode", DRAFTING_PROVIDER_MODE),
                state,
                _json(input_json),
                base_fingerprint,
                input_fingerprint,
                min(2, max(1, int(config.get("drafting_max_attempts", DRAFTING_MAX_ATTEMPTS)))),
                None,
                _json(eligibility["blocking_reasons"]),
                _iso(now),
                _iso(now),
            ),
        )
    return int(cursor.lastrowid)


def _run_row(database: Database, run_id: int):
    row = database.connection.execute("SELECT * FROM drafting_runs WHERE id=?", (run_id,)).fetchone()
    if row is None:
        raise DraftingValidationError("drafting run not found")
    return row


def _draft_dict(database: Database, row) -> dict[str, Any]:
    lead = database.get_lead(row["lead_id"])
    run_row = database.connection.execute("SELECT input_json FROM drafting_runs WHERE id=?", (row["run_id"],)).fetchone()
    run_input = {} if run_row is None else json.loads(run_row["input_json"])
    return {
        "draft_id": row["id"],
        "run_id": row["run_id"],
        "version": row["version"],
        "state": row["state"],
        "business_name": lead["business_name"] if lead else None,
        "recipient_email": lead["email"] if lead else None,
        "subject": row["subject"],
        "body": row["body"],
        "claims": json.loads(row["claims_json"]),
        "evidence_ids_used": json.loads(row["evidence_ids_json"]),
        "confidence": row["confidence"],
        "cta_text": row["cta_text"],
        "personalization_summary": row["personalization_summary"],
        "prohibited_claim_self_check": json.loads(row["prohibited_claim_self_check_json"]),
        "offer_version_id": row["offer_version_id"],
        "campaign_id": row["campaign_id"],
        "packet_id": row["packet_id"],
        "prompt_version": row["prompt_version"],
        "message_policy_version": run_input.get("message_policy_version"),
        "message_policy": run_input.get("message_policy"),
        "input_fingerprint": row["input_fingerprint"],
        "body_word_count": len(WORD_RE.findall(row["body"])),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _run_summary(database: Database, run_id: int, *, reused: bool = False) -> dict[str, Any]:
    run = _run_row(database, run_id)
    draft_row = database.connection.execute("SELECT * FROM personalized_drafts WHERE run_id=? ORDER BY version DESC LIMIT 1", (run_id,)).fetchone()
    return {
        "run_id": run_id,
        "state": run["state"],
        "lead_id": run["lead_id"],
        "campaign_id": run["campaign_id"],
        "packet_id": run["packet_id"],
        "offer_version_id": run["offer_version_id"],
        "attempt_count": run["attempt_count"],
        "max_attempts": run["max_attempts"],
        "blocking_reasons": json.loads(run["blocking_reasons_json"]),
        "last_error_category": run["last_error_category"],
        "input_fingerprint": run["input_fingerprint"],
        "reused": reused,
        "draft": None if draft_row is None else _draft_dict(database, draft_row),
    }


def _finish_attempt(database: Database, attempt_id: int, status: str, *, error_category: Optional[str] = None, detail: Optional[dict[str, Any]] = None, now: datetime) -> None:
    with database.connection:
        database.connection.execute(
            "UPDATE drafting_provider_attempts SET status=?, error_category=?, safe_detail_json=?, completed_at=? WHERE id=?",
            (status, error_category, _json(detail or {}), _iso(now), attempt_id),
        )


def _persist_success(database: Database, run_id: int, attempt_id: int, output: Mapping[str, Any], validation: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    run = _run_row(database, run_id)
    version = database.connection.execute(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM personalized_drafts WHERE lead_id=? AND campaign_id=? AND offer_version_id=?",
        (run["lead_id"], run["campaign_id"], run["offer_version_id"]),
    ).fetchone()[0]
    claims = output["claims"]
    with database.connection:
        database.connection.execute("UPDATE drafting_runs SET state='GENERATED', updated_at=? WHERE id=?", (_iso(now), run_id))
        cursor = database.connection.execute(
            """INSERT INTO personalized_drafts
               (run_id, lead_id, campaign_id, offer_version_id, packet_id, version, state,
                subject, body, claims_json, evidence_ids_json, confidence, cta_text,
                personalization_summary, prohibited_claim_self_check_json, prompt_version,
                input_fingerprint, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'GENERATED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id, run["lead_id"], run["campaign_id"], run["offer_version_id"], run["packet_id"], version,
                output["subject"], output["body"], _json(claims), _json(output["evidence_ids_used"]),
                float(output["confidence"]), output["cta_text"], output["personalization_summary"],
                _json(output["prohibited_claim_self_check"]), run["prompt_version"], run["input_fingerprint"], _iso(now), _iso(now),
            ),
        )
        draft_id = int(cursor.lastrowid)
        for index, claim in enumerate(claims):
            claim_ids = claim.get("evidence_ids", [])
            if not claim_ids:
                database.connection.execute(
                    "INSERT INTO draft_claim_evidence (draft_id, claim_index, claim_text, claim_type, evidence_id, support_reason) VALUES (?, ?, ?, ?, NULL, ?)",
                    (draft_id, index, claim["text"], claim["claim_type"], "OFFER_OR_CTA_CONTRACT"),
                )
            else:
                for evidence_id in claim_ids:
                    database.connection.execute(
                        "INSERT INTO draft_claim_evidence (draft_id, claim_index, claim_text, claim_type, evidence_id, support_reason) VALUES (?, ?, ?, ?, ?, ?)",
                        (draft_id, index, claim["text"], claim["claim_type"], evidence_id, "CURRENT_EVIDENCE_REFERENCE"),
                    )
        database.connection.execute(
            "INSERT INTO draft_validation_results (run_id, draft_id, attempt_id, validation_state, error_codes_json, safe_details_json, validator_version, validated_at) VALUES (?, ?, ?, 'PASS', '[]', ?, ?, ?)",
            (run_id, draft_id, attempt_id, _json({"word_count": validation["word_count"], "subject_length": validation["subject_length"]}), DRAFTING_POLICY_VERSION, _iso(now)),
        )
        database.connection.execute("UPDATE personalized_drafts SET state='VALIDATED', updated_at=? WHERE id=?", (_iso(now), draft_id))
        database.connection.execute("UPDATE personalized_drafts SET state='REVIEW_PENDING', updated_at=? WHERE id=?", (_iso(now), draft_id))
        database.connection.execute("UPDATE drafting_runs SET state='REVIEW_PENDING', completed_at=?, updated_at=?, last_error_category=NULL WHERE id=?", (_iso(now), _iso(now), run_id))
        if run["redraft_request_id"] is not None:
            database.connection.execute("UPDATE draft_redraft_requests SET status='COMPLETED', new_run_id=?, completed_at=? WHERE id=?", (run_id, _iso(now), run["redraft_request_id"]))
    return _run_summary(database, run_id)


def _provider_call(database: Database, run_id: int, provider: Any, now: datetime) -> dict[str, Any]:
    run = _run_row(database, run_id)
    payload = json.loads(run["input_json"])
    generate = getattr(provider, "generate", None)
    if not callable(generate):
        raise DraftingValidationError("fixture provider must expose generate")
    while run["attempt_count"] < run["max_attempts"]:
        attempt_number = int(run["attempt_count"]) + 1
        with database.connection:
            database.connection.execute("UPDATE drafting_runs SET state='GENERATED', attempt_count=?, updated_at=? WHERE id=?", (attempt_number, _iso(now), run_id))
            attempt_cursor = database.connection.execute(
                "INSERT INTO drafting_provider_attempts (run_id, attempt_number, status, safe_detail_json, created_at) VALUES (?, ?, 'STARTED', '{}', ?)",
                (run_id, attempt_number, _iso(now)),
            )
        attempt_id = int(attempt_cursor.lastrowid)
        try:
            provider_output = generate(deepcopy(payload))
        except Exception as error:
            _finish_attempt(database, attempt_id, "ERROR_RETRYABLE" if attempt_number < run["max_attempts"] else "ERROR_TERMINAL", error_category="PROVIDER_EXCEPTION", detail={"exception_type": type(error).__name__}, now=now)
            with database.connection:
                database.connection.execute("UPDATE drafting_runs SET state=?, last_error_category=?, updated_at=? WHERE id=?", ("ERROR_RETRYABLE" if attempt_number < run["max_attempts"] else "ERROR_TERMINAL", "PROVIDER_EXCEPTION", _iso(now), run_id))
            run = _run_row(database, run_id)
            if run["state"] == "ERROR_TERMINAL":
                return _run_summary(database, run_id)
            continue
        validation = validate_provider_output(payload, provider_output)
        if validation["valid"]:
            output = validation["normalized_output"]
            _finish_attempt(database, attempt_id, "SUCCEEDED", detail={"validation": "PASS"}, now=now)
            return _persist_success(database, run_id, attempt_id, output, validation, now)
        codes = [error["code"] for error in validation["errors"]]
        with database.connection:
            database.connection.execute(
                "INSERT INTO draft_validation_results (run_id, draft_id, attempt_id, validation_state, error_codes_json, safe_details_json, validator_version, validated_at) VALUES (?, NULL, ?, 'FAIL', ?, ?, ?, ?)",
                (run_id, attempt_id, _json(codes), _json(_safe_detail(validation["errors"])), DRAFTING_POLICY_VERSION, _iso(now)),
            )
        terminal = attempt_number >= run["max_attempts"]
        _finish_attempt(database, attempt_id, "ERROR_TERMINAL" if terminal else "VALIDATION_FAILED", error_category="PROVIDER_OUTPUT_INVALID", detail={"error_codes": sorted(codes)}, now=now)
        with database.connection:
            database.connection.execute("UPDATE drafting_runs SET state=?, last_error_category=?, updated_at=? WHERE id=?", ("ERROR_TERMINAL" if terminal else "ERROR_RETRYABLE", "PROVIDER_OUTPUT_INVALID", _iso(now), run_id))
        run = _run_row(database, run_id)
        if terminal:
            return _run_summary(database, run_id)
    return _run_summary(database, run_id)


def generate_fictional_draft(
    database: Database,
    *,
    lead_id: int,
    campaign_id: int,
    provider: Any,
    packet_id: Optional[int] = None,
    offer_key: str = IMMEDIATE_OFFER_KEY,
    evidence_ids: Optional[Iterable[int]] = None,
    redraft_request_id: Optional[int] = None,
    message_policy_context: Optional[Mapping[str, Any]] = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Generate only in an isolated fixture database through an injected provider."""
    if _is_durable_database(database):
        raise DraftingBlockedError("fixture drafting cannot target the canonical database")
    current = _now(now)
    eligibility = _eligibility(
        database,
        lead_id=lead_id,
        campaign_id=campaign_id,
        packet_id=packet_id,
        offer_key=offer_key,
        evidence_ids=evidence_ids,
        message_policy_context=message_policy_context,
        now=current,
    )
    if redraft_request_id is not None:
        request = database.connection.execute("SELECT * FROM draft_redraft_requests WHERE id=?", (redraft_request_id,)).fetchone()
        if request is None or request["status"] != "REQUESTED" or request["source_draft_id"] is None:
            raise DraftingValidationError("redraft request is not pending")
        base = database.connection.execute("SELECT * FROM personalized_drafts WHERE id=?", (request["source_draft_id"],)).fetchone()
        if base is None or base["lead_id"] != lead_id or base["campaign_id"] != campaign_id:
            raise DraftingValidationError("redraft request does not match the requested lead and campaign")
        base_fingerprint, input_fingerprint = _fingerprint(
            lead=eligibility["lead"], campaign=eligibility["campaign"], offer=eligibility.get("offer"), packet=eligibility.get("packet"), evidence=eligibility.get("evidence", []), redraft_request_id=redraft_request_id, message_policy_context=message_policy_context
        )
        eligibility = dict(eligibility, base_fingerprint=base_fingerprint, input_fingerprint=input_fingerprint)
    run_id = _insert_run(database, eligibility, redraft_request_id=redraft_request_id, now=current)
    existing = _run_summary(database, run_id)
    if existing["state"] == "REVIEW_PENDING" and existing["draft"] is not None:
        existing["reused"] = True
        return existing
    if existing["state"] in {"BLOCKED", "ERROR_TERMINAL", "HOLD_FOR_REVIEW"}:
        return existing
    if not eligibility["ready"]:
        return existing
    return _provider_call(database, run_id, provider, current)


def resume_drafting(database: Database, run_id: int, *, provider: Any, now: Optional[datetime] = None) -> dict[str, Any]:
    """Resume a fixture run without repeating an unresolved provider attempt."""
    if _is_durable_database(database):
        raise DraftingBlockedError("fixture drafting cannot target the canonical database")
    migrate_step10(database)
    current = _now(now)
    run = _run_row(database, run_id)
    started = database.connection.execute(
        "SELECT id FROM drafting_provider_attempts WHERE run_id=? AND status='STARTED' ORDER BY attempt_number DESC LIMIT 1", (run_id,)
    ).fetchone()
    if started is not None:
        with database.connection:
            database.connection.execute("UPDATE drafting_runs SET state='HOLD_FOR_REVIEW', last_error_category='INTERRUPTED_PROVIDER_ATTEMPT', updated_at=? WHERE id=?", (_iso(current), run_id))
        return _run_summary(database, run_id)
    if run["state"] == "REVIEW_PENDING":
        return _run_summary(database, run_id, reused=True)
    if run["state"] not in {"ERROR_RETRYABLE", "READY_FOR_GENERATION", "GENERATED"}:
        return _run_summary(database, run_id)
    return _provider_call(database, run_id, provider, current)


def generate_draft(database: Database, **kwargs: Any) -> dict[str, Any]:
    """Guarded public entry point; live drafting is intentionally unavailable."""
    if not kwargs.pop("fixture_override", False):
        raise DraftingBlockedError("live drafting is not implemented; fixture_override=True is required")
    return generate_fictional_draft(database, **kwargs)


def inspect_validation_failures(database: Database, run_id: Optional[int] = None, *, draft_id: Optional[int] = None) -> dict[str, Any]:
    migrate_step10(database)
    if draft_id is not None:
        row = database.connection.execute("SELECT run_id FROM personalized_drafts WHERE id=?", (draft_id,)).fetchone()
        if row is None:
            raise DraftingValidationError("draft not found")
        run_id = row["run_id"]
    if run_id is None:
        raise DraftingValidationError("run_id or draft_id is required")
    rows = database.connection.execute(
        "SELECT id, draft_id, attempt_id, validation_state, error_codes_json, safe_details_json, validator_version, validated_at FROM draft_validation_results WHERE run_id=? ORDER BY id",
        (run_id,),
    ).fetchall()
    return {
        "run_id": run_id,
        "failures": [
            {
                "validation_id": row["id"],
                "draft_id": row["draft_id"],
                "attempt_id": row["attempt_id"],
                "validation_state": row["validation_state"],
                "error_codes": json.loads(row["error_codes_json"]),
                "safe_details": json.loads(row["safe_details_json"]),
                "validator_version": row["validator_version"],
                "validated_at": row["validated_at"],
            }
            for row in rows if row["validation_state"] == "FAIL"
        ],
    }


def get_review_pending_draft(database: Database, draft_id: int) -> dict[str, Any]:
    migrate_step10(database)
    row = database.connection.execute("SELECT * FROM personalized_drafts WHERE id=?", (draft_id,)).fetchone()
    if row is None or row["state"] != "REVIEW_PENDING":
        raise DraftingValidationError("review-pending draft not found")
    return _draft_dict(database, row)


def request_audited_redraft(database: Database, *, draft_id: int, requested_by: str, reason: str) -> dict[str, Any]:
    migrate_step10(database)
    if str(requested_by).strip().upper() != "OPERATOR":
        raise DraftingValidationError("redrafting requires requested_by=OPERATOR")
    if not isinstance(reason, str) or not reason.strip():
        raise DraftingValidationError("an audited redraft reason is required")
    row = database.connection.execute("SELECT * FROM personalized_drafts WHERE id=?", (draft_id,)).fetchone()
    if row is None or row["state"] != "REVIEW_PENDING":
        raise DraftingValidationError("only a review-pending draft can be redrafted")
    existing = database.connection.execute(
        "SELECT * FROM draft_redraft_requests WHERE source_draft_id=? AND status='REQUESTED' ORDER BY id DESC LIMIT 1", (draft_id,)
    ).fetchone()
    if existing is not None:
        return {"request_id": existing["id"], "status": existing["status"], "requested_by": existing["requested_by"], "reason": existing["reason"]}
    timestamp = _iso()
    with database.connection:
        cursor = database.connection.execute(
            "INSERT INTO draft_redraft_requests (source_draft_id, requested_by, reason, status, created_at) VALUES (?, 'OPERATOR', ?, 'REQUESTED', ?)",
            (draft_id, " ".join(reason.split())[:500], timestamp),
        )
    return {"request_id": int(cursor.lastrowid), "status": "REQUESTED", "requested_by": "OPERATOR", "reason": " ".join(reason.split())[:500]}


def summarize_draft_provenance(database: Database, draft_id: int) -> dict[str, Any]:
    migrate_step10(database)
    row = database.connection.execute("SELECT * FROM personalized_drafts WHERE id=?", (draft_id,)).fetchone()
    if row is None:
        raise DraftingValidationError("draft not found")
    run = _run_row(database, row["run_id"])
    run_input = json.loads(run["input_json"])
    claims = database.connection.execute(
        "SELECT claim_index, claim_type, evidence_id, support_reason FROM draft_claim_evidence WHERE draft_id=? ORDER BY claim_index, id", (draft_id,)
    ).fetchall()
    validations = database.connection.execute(
        "SELECT validation_state, error_codes_json, validator_version, validated_at FROM draft_validation_results WHERE run_id=? ORDER BY id", (row["run_id"],)
    ).fetchall()
    redrafts = database.connection.execute(
        "SELECT id, requested_by, status, new_run_id, created_at, completed_at FROM draft_redraft_requests WHERE source_draft_id=? ORDER BY id", (draft_id,)
    ).fetchall()
    return {
        "draft_id": draft_id,
        "run_id": row["run_id"],
        "version": row["version"],
        "state": row["state"],
        "lead_id": row["lead_id"],
        "campaign_id": row["campaign_id"],
        "offer_version_id": row["offer_version_id"],
        "packet_id": row["packet_id"],
        "policy_version": DRAFTING_POLICY_VERSION,
        "message_policy_version": run_input.get("message_policy_version"),
        "message_policy": run_input.get("message_policy"),
        "prompt_version": row["prompt_version"],
        "input_fingerprint": row["input_fingerprint"],
        "base_fingerprint": run["base_fingerprint"],
        "evidence_ids_used": json.loads(row["evidence_ids_json"]),
        "claim_evidence": [dict(item) for item in claims],
        "validation_results": [
            {"validation_state": item["validation_state"], "error_codes": json.loads(item["error_codes_json"]), "validator_version": item["validator_version"], "validated_at": item["validated_at"]}
            for item in validations
        ],
        "redraft_requests": [dict(item) for item in redrafts],
        "provider_attempt_count": database.connection.execute("SELECT COUNT(*) FROM drafting_provider_attempts WHERE run_id=?", (row["run_id"],)).fetchone()[0],
        "provider_mode": run["provider_mode"],
    }


__all__ = [
    "ALLOWED_CLAIM_TYPES",
    "DRAFTING_MAX_ATTEMPTS",
    "DRAFTING_POLICY_VERSION",
    "DRAFTING_PROVIDER_MODE",
    "DRAFTING_RUN_STATES",
    "DraftingBlockedError",
    "DraftingValidationError",
    "FixtureDraftingProvider",
    "generate_draft",
    "generate_fictional_draft",
    "get_review_pending_draft",
    "inspect_drafting_readiness",
    "inspect_validation_failures",
    "migrate_step10",
    "preview_bounded_drafting_input",
    "request_audited_redraft",
    "resume_drafting",
    "summarize_draft_provenance",
    "validate_provider_output",
]
