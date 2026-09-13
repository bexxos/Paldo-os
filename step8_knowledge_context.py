"""Read-only Gold Knowledge Base context packets for Paldo OS Step 8.

The module accepts already-recalled records from an injected provider.  It never
searches Drive, imports a Knowledge Base implementation, calls an LLM, edits
canonical knowledge, drafts messages, or creates outreach.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urlsplit

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from step7_pipeline import migrate_step7


STEP8_MIGRATION_VERSION = 10
TRUST_TIERS = frozenset({"BRONZE", "SILVER", "GOLD", "CANONICAL"})
REQUIRED_GOLD_SECTIONS = (
    "ICP",
    "ACTIVE_OFFER_CTA",
    "VOICE_GUIDE",
    "APPROVED_PROOF_RULES",
    "OUTBOUND_COMPLIANCE_POLICY",
    "ACTIVE_CAMPAIGN_DECISION",
)

_SECTION_ALIASES = {
    "ICP": "ICP",
    "IDEAL_CUSTOMER_PROFILE": "ICP",
    "ACTIVE_OFFER_CTA": "ACTIVE_OFFER_CTA",
    "ACTIVE_OFFER_AND_CTA": "ACTIVE_OFFER_CTA",
    "OFFER_CTA": "ACTIVE_OFFER_CTA",
    "VOICE_GUIDE": "VOICE_GUIDE",
    "VOICE": "VOICE_GUIDE",
    "APPROVED_PROOF_RULES": "APPROVED_PROOF_RULES",
    "PROOF_RULES": "APPROVED_PROOF_RULES",
    "CASE_STUDY_RULES": "APPROVED_PROOF_RULES",
    "OUTBOUND_COMPLIANCE_POLICY": "OUTBOUND_COMPLIANCE_POLICY",
    "COMPLIANCE_POLICY": "OUTBOUND_COMPLIANCE_POLICY",
    "OUTBOUND_POLICY": "OUTBOUND_COMPLIANCE_POLICY",
    "ACTIVE_CAMPAIGN_DECISION": "ACTIVE_CAMPAIGN_DECISION",
    "CAMPAIGN_DECISION": "ACTIVE_CAMPAIGN_DECISION",
}
_REQUIRED_METADATA = (
    "source_id",
    "status",
    "confidence",
    "heading",
    "drive_file_id",
    "kb_path",
    "modified_at",
    "snapshot_revision",
    "content_hash",
    "retrieved_at",
)
_PROTECTED_TERMS = (
    "compliance",
    "prohibited",
    "do not",
    "must not",
    "never",
    "cannot",
    "legal",
    "opt-out",
    "opt out",
    "guarantee",
    "guaranteed",
)
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_SECTION_LIMITS = {
    "ICP": 820,
    "ACTIVE_OFFER_CTA": 900,
    "VOICE_GUIDE": 800,
    "APPROVED_PROOF_RULES": 900,
    "OUTBOUND_COMPLIANCE_POLICY": 1450,
    "ACTIVE_CAMPAIGN_DECISION": 830,
}


class ContextPacketNotReadyError(RuntimeError):
    """Raised when drafting requests context without a usable READY packet."""


class KnowledgeRecordValidationError(ValueError):
    """Raised only for malformed API calls; record trust failures are reported safely."""


_SCHEMA = """
CREATE TABLE IF NOT EXISTS kb_context_packets (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    snapshot_revision TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    readiness_state TEXT NOT NULL,
    required_section_coverage_json TEXT NOT NULL,
    source_ids_json TEXT NOT NULL,
    source_hashes_json TEXT NOT NULL,
    contradiction_status TEXT NOT NULL,
    packet_content TEXT NOT NULL,
    packet_hash TEXT NOT NULL,
    invalidation_reason TEXT,
    used_by_batch INTEGER NOT NULL DEFAULT 0 CHECK (used_by_batch IN (0, 1)),
    truncation_json TEXT NOT NULL DEFAULT '{}',
    explanation_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS kb_context_packet_sources (
    id INTEGER PRIMARY KEY,
    packet_id INTEGER NOT NULL REFERENCES kb_context_packets(id) ON DELETE CASCADE,
    source_id TEXT NOT NULL,
    kb_status TEXT NOT NULL,
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    heading TEXT NOT NULL,
    drive_file_id TEXT NOT NULL,
    kb_path TEXT NOT NULL,
    source_url TEXT,
    modified_at TEXT NOT NULL,
    snapshot_revision TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    retrieved_at TEXT NOT NULL,
    UNIQUE (packet_id, source_id, content_hash)
);

CREATE TABLE IF NOT EXISTS kb_context_packet_batches (
    id INTEGER PRIMARY KEY,
    packet_id INTEGER NOT NULL REFERENCES kb_context_packets(id) ON DELETE CASCADE,
    batch_id TEXT NOT NULL,
    used_at TEXT NOT NULL,
    UNIQUE (packet_id, batch_id)
);

CREATE INDEX IF NOT EXISTS kb_context_packets_campaign_idx
    ON kb_context_packets(campaign_id, readiness_state, expires_at);
CREATE INDEX IF NOT EXISTS kb_context_packet_sources_packet_idx
    ON kb_context_packet_sources(packet_id, source_id);
"""


def _now(value: Optional[datetime] = None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc, microsecond=0)
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return _now(value).isoformat()


def _parse_time(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"missing {field}")
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise ValueError(f"invalid {field}") from error
    return _now(parsed)


def _value(record: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in record and record[name] is not None:
            return record[name]
    return default


def _section(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing section")
    token = re.sub(r"[^A-Z0-9]+", "_", value.strip().upper()).strip("_")
    return _SECTION_ALIASES.get(token, token)


def _safe_url(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("source_url must be text")
    parsed = urlsplit(value.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise ValueError("source_url must be public HTTP or HTTPS")
    if parsed.username or parsed.password:
        raise ValueError("source_url cannot contain credentials")
    return value.strip()


def _content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _hash_from_record(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing content_hash")
    result = value.strip().lower()
    if result.startswith("sha256:"):
        result = result[7:]
    if not re.fullmatch(r"[0-9a-f]{64}", result):
        raise ValueError("content_hash is not verifiable")
    return result


def _sanitize_content(content: str) -> str:
    # Knowledge content is not a prospect store.  Email-like strings are removed
    # before the compact packet crosses the SQLite persistence boundary.
    return _EMAIL_RE.sub("[REDACTED_CONTACT]", content.strip())


def _record_source_id(record: Mapping[str, Any]) -> str:
    value = _value(record, "source_id", "id")
    return str(value).strip() if value is not None else ""


def _normalise_record(record: Mapping[str, Any], *, now: datetime, snapshot_revision: Optional[str] = None) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise ValueError("record must be an object")
    source_id = _record_source_id(record)
    if not source_id:
        raise ValueError("missing source_id")
    status = str(_value(record, "status", "kb_status", "trust_tier", "tier", default="")).strip().upper()
    trust_tier = str(_value(record, "trust_tier", "tier", "status", default="")).strip().upper()
    if status not in {"GOLD", "CANONICAL"} or trust_tier not in {"GOLD", "CANONICAL"}:
        raise ValueError("trust tier is not drafting-ready Gold/canonical")
    approval_status = str(_value(record, "approval_status", "approval", default="")).strip().upper()
    approved_by = str(_value(record, "approved_by", "approver", default="")).strip().upper()
    if approval_status not in {"APPROVED", "CANONICAL"} or approved_by != "OPERATOR":
        raise ValueError("record is not approved by Operator")
    if "canonical" in record and record["canonical"] is False:
        raise ValueError("record is not canonical")
    confidence = _value(record, "confidence")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        raise ValueError("confidence is missing or invalid")
    heading = _value(record, "heading")
    if not isinstance(heading, str) or not heading.strip():
        raise ValueError("missing heading")
    drive_file_id = _value(record, "drive_file_id", "drive_id")
    kb_path = _value(record, "kb_path", "knowledge_base_path", "path")
    if not isinstance(drive_file_id, str) or not drive_file_id.strip():
        raise ValueError("missing drive_file_id")
    if not isinstance(kb_path, str) or not kb_path.strip():
        raise ValueError("missing kb_path")
    modified_at = _parse_time(_value(record, "modified_at", "modification_date", "modified_date"), "modified_at")
    revision = _value(record, "snapshot_revision", "manifest_revision", "revision")
    if not isinstance(revision, str) or not revision.strip():
        raise ValueError("missing snapshot_revision")
    revision = revision.strip()
    if snapshot_revision is not None and revision != snapshot_revision:
        raise ValueError("record snapshot revision differs from requested revision")
    retrieved_at = _parse_time(_value(record, "retrieved_at", "retrieval_timestamp", "retrieval_time"), "retrieved_at")
    content = _value(record, "content", "approved_content", "text")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("missing approved content")
    content_hash = _hash_from_record(_value(record, "content_hash"))
    if content_hash != _content_hash(content):
        raise ValueError("content_hash does not match content")
    expires_at = _value(record, "expires_at", "expires_on")
    if expires_at is not None and _parse_time(expires_at, "expires_at") <= now:
        raise ValueError("record is expired")
    source_url = _safe_url(_value(record, "source_url", "url"))
    return {
        "source_id": source_id,
        "kb_status": status,
        "trust_tier": trust_tier,
        "confidence": float(confidence),
        "heading": heading.strip(),
        "drive_file_id": drive_file_id.strip(),
        "kb_path": kb_path.strip(),
        "source_url": source_url,
        "modified_at": _iso(modified_at),
        "snapshot_revision": revision,
        "content_hash": content_hash,
        "retrieved_at": _iso(retrieved_at),
        "section": _section(_value(record, "section", "heading")),
        "content": content.strip(),
    }


def _record_error(record: Any, error: Exception) -> dict[str, str]:
    source_id = _record_source_id(record) if isinstance(record, Mapping) else "[UNKNOWN]"
    return {"source_id": source_id or "[UNKNOWN]", "reason": str(error)}


def _contradictions(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_section: dict[str, dict[str, list[str]]] = {}
    for record in records:
        section = record["section"]
        if section not in REQUIRED_GOLD_SECTIONS:
            continue
        by_section.setdefault(section, {}).setdefault(record["content_hash"], []).append(record["source_id"])
    conflicts = []
    for section, variants in sorted(by_section.items()):
        if len(variants) > 1:
            conflicts.append(
                {
                    "section": section,
                    "source_ids": sorted(source_id for ids in variants.values() for source_id in ids),
                    "content_hashes": sorted(variants),
                }
            )
    return conflicts


def validate_recalled_gold_records(
    records: Iterable[Mapping[str, Any]], *, snapshot_revision: Optional[str] = None, now: Optional[datetime] = None
) -> dict[str, Any]:
    """Validate recalled records without searching or modifying any Knowledge Base."""
    current_time = _now(now)
    if isinstance(records, Mapping):
        records = records.get("records", [])
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []
    inferred_revision = snapshot_revision
    for record in records:
        try:
            if inferred_revision is None and isinstance(record, Mapping):
                inferred_revision = _value(record, "snapshot_revision", "manifest_revision", "revision")
            accepted.append(_normalise_record(record, now=current_time, snapshot_revision=inferred_revision))
        except (TypeError, ValueError) as error:
            rejected.append(_record_error(record, error))
    if inferred_revision is None:
        inferred_revision = ""
    if any(record["snapshot_revision"] != inferred_revision for record in accepted):
        rejected.extend(
            {"source_id": record["source_id"], "reason": "mixed snapshot revisions"}
            for record in accepted
            if record["snapshot_revision"] != inferred_revision
        )
        accepted = [record for record in accepted if record["snapshot_revision"] == inferred_revision]
    coverage = sorted({record["section"] for record in accepted if record["section"] in REQUIRED_GOLD_SECTIONS})
    missing = [section for section in REQUIRED_GOLD_SECTIONS if section not in coverage]
    contradictions = _contradictions(accepted)
    return {
        "accepted": accepted,
        "rejected": rejected,
        "snapshot_revision": str(inferred_revision),
        "required_sections": list(REQUIRED_GOLD_SECTIONS),
        "coverage": coverage,
        "missing_sections": missing,
        "contradictions": contradictions,
        "ready_candidate": not missing and not contradictions,
    }


def migrate_step8(database_or_path: Database | str | Path) -> int:
    """Apply additive Step 8 storage and safe configuration defaults."""
    database = database_or_path if hasattr(database_or_path, "connection") else Database(database_or_path)
    migrate_step7(database)
    with database.connection:
        database.connection.executescript(_SCHEMA)
        database.connection.executemany(
            "INSERT OR IGNORE INTO system_config (key, value, value_type) VALUES (?, ?, 'integer')",
            (("knowledge_context_required", "1"), ("knowledge_context_packet_ttl_hours", "24"), ("knowledge_context_packet_max_chars", "6000")),
        )
        database.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
            (STEP8_MIGRATION_VERSION, "step8_knowledge_context_packets", _iso(_now())),
        )
    return STEP8_MIGRATION_VERSION


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _split_optional_examples(text: str) -> tuple[list[str], list[str]]:
    lines = text.splitlines() or [text]
    for index, line in enumerate(lines):
        if re.match(r"^\s*(optional\s+)?examples?\s*[:\-]", line, re.IGNORECASE):
            return lines[:index], lines[index:]
    return lines, []


def _protected_lines(lines: Iterable[str]) -> list[str]:
    result = []
    for line in lines:
        lowered = line.casefold()
        if any(term in lowered for term in _PROTECTED_TERMS):
            result.append(line.strip())
    return result


def _join_lines(lines: Iterable[str], budget: int) -> str:
    result = ""
    for line in lines:
        line = line.strip()
        if not line:
            continue
        candidate = line if not result else result + "\n" + line
        if len(candidate) > budget:
            break
        result = candidate
    return result


def _compact_section(text: str, budget: int, section: str) -> tuple[str, bool, Optional[str]]:
    text = _sanitize_content(text)
    if len(text) <= budget:
        return text, False, None
    rules, examples = _split_optional_examples(text)
    protected = _protected_lines(rules + examples)
    protected_text = _join_lines(protected, budget)
    if len(protected_text) < sum(len(line) for line in protected) + max(0, len(protected) - 1):
        return protected_text, True, f"protected {section} rules exceed deterministic section limit"
    chosen = _join_lines(rules, budget)
    if len(chosen) < len(protected_text):
        chosen = protected_text
    if len(chosen) < len(protected_text):
        return protected_text, True, f"protected {section} rules exceed deterministic section limit"
    if examples and len(chosen) < budget:
        remaining = budget - len(chosen) - 1
        if remaining > 0:
            example_text = _join_lines(examples, remaining)
            if example_text:
                chosen = chosen + "\n" + example_text
    return chosen[:budget], True, None


def _compact_packet(records: list[dict[str, Any]], max_chars: int) -> tuple[str, dict[str, Any], Optional[str]]:
    selected: dict[str, dict[str, Any]] = {}
    for record in records:
        section = record["section"]
        if section not in REQUIRED_GOLD_SECTIONS:
            continue
        prior = selected.get(section)
        if prior is None or (record["confidence"], record["modified_at"], record["source_id"]) > (prior["confidence"], prior["modified_at"], prior["source_id"]):
            selected[section] = record
    total_section_limit = sum(_SECTION_LIMITS.values())
    scale = min(1.0, max(1, max_chars - 120) / total_section_limit)
    parts = []
    truncated_sections = []
    unsafe_reasons = []
    for section in REQUIRED_GOLD_SECTIONS:
        record = selected.get(section)
        if record is None:
            continue
        budget = max(40, int(_SECTION_LIMITS[section] * scale))
        compacted, truncated, unsafe = _compact_section(record["content"], budget, section)
        if truncated:
            truncated_sections.append(section)
        if unsafe:
            unsafe_reasons.append(unsafe)
        parts.append(f"[{section}]\n{compacted}")
    content = "\n\n".join(parts)
    if len(content) > max_chars:
        # This only removes optional tail material; the required section budgets
        # and protected compliance-line check remain authoritative.
        content = content[:max_chars]
        if "OUTBOUND_COMPLIANCE_POLICY" not in content:
            unsafe_reasons.append("packet limit would remove compliance policy")
    report = {
        "truncated": bool(truncated_sections),
        "truncated_sections": truncated_sections,
        "max_chars": max_chars,
        "unsafe": bool(unsafe_reasons),
        "unsafe_reasons": unsafe_reasons,
    }
    return content, report, "; ".join(unsafe_reasons) if unsafe_reasons else None


def _packet_row(database: Database, packet_id: int):
    return database.connection.execute("SELECT * FROM kb_context_packets WHERE id=?", (packet_id,)).fetchone()


def _packet_dict(database: Database, row) -> dict[str, Any]:
    if row is None:
        raise KeyError("context packet not found")
    return {
        "packet_id": row["id"],
        "campaign_id": row["campaign_id"],
        "snapshot_revision": row["snapshot_revision"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "readiness_state": row["readiness_state"],
        "required_section_coverage": json.loads(row["required_section_coverage_json"]),
        "source_ids": json.loads(row["source_ids_json"]),
        "source_hashes": json.loads(row["source_hashes_json"]),
        "contradiction_status": row["contradiction_status"],
        "approved_content": row["packet_content"],
        "packet_hash": row["packet_hash"],
        "invalidation_reason": row["invalidation_reason"],
        "used_by_batch": bool(row["used_by_batch"]),
        "truncation": json.loads(row["truncation_json"]),
        "explanation": json.loads(row["explanation_json"]),
    }


def _mark_expired(database: Database, row, now: datetime):
    if row and row["readiness_state"] == "READY" and _parse_time(row["expires_at"], "expires_at") <= now:
        with database.connection:
            database.connection.execute(
                "UPDATE kb_context_packets SET readiness_state='EXPIRED', invalidation_reason='TTL_EXPIRED' WHERE id=?",
                (row["id"],),
            )
        return _packet_row(database, row["id"])
    return row


def build_context_packet(
    database: Database,
    campaign_id: int,
    recalled_records: Iterable[Mapping[str, Any]],
    *,
    snapshot_revision: Optional[str] = None,
    now: Optional[datetime] = None,
    ttl_hours: Optional[int] = None,
    max_chars: Optional[int] = None,
) -> dict[str, Any]:
    """Validate recalled records and build or reuse one deterministic packet."""
    migrate_step8(database)
    current_time = _now(now)
    config = database.read_config()
    ttl = int(config.get("knowledge_context_packet_ttl_hours", 24) if ttl_hours is None else ttl_hours)
    limit = int(config.get("knowledge_context_packet_max_chars", 6000) if max_chars is None else max_chars)
    if ttl <= 0 or limit <= 0:
        raise KnowledgeRecordValidationError("packet TTL and max_chars must be positive")
    validation = validate_recalled_gold_records(recalled_records, snapshot_revision=snapshot_revision, now=current_time)
    accepted = validation["accepted"]
    revision = validation["snapshot_revision"]
    approved_content, truncation, unsafe = _compact_packet(accepted, limit)
    source_ids = sorted(record["source_id"] for record in accepted)
    source_hashes = {record["source_id"]: record["content_hash"] for record in sorted(accepted, key=lambda item: item["source_id"])}
    packet_hash = hashlib.sha256(
        _json(
            {
                "campaign_id": campaign_id,
                "snapshot_revision": revision,
                "source_hashes": source_hashes,
                "coverage": validation["coverage"],
                "contradictions": validation["contradictions"],
                "approved_content": approved_content,
                "truncation": truncation,
            }
        ).encode("utf-8")
    ).hexdigest()
    missing = list(validation["missing_sections"])
    reasons = list(item["reason"] for item in validation["rejected"])
    if missing:
        reasons.append("missing required sections: " + ", ".join(missing))
    if validation["contradictions"]:
        reasons.append("contradictory Gold sources require operator review")
    if unsafe:
        reasons.append(unsafe)
    readiness = "READY" if not missing and not validation["contradictions"] and not unsafe else "NOT_READY"
    explanation = {
        "missing_sections": missing,
        "rejected_sources": validation["rejected"],
        "contradictions": validation["contradictions"],
        "reasons": reasons,
    }
    expires_at = _iso(current_time + timedelta(hours=ttl))
    with database.connection:
        if revision:
            database.connection.execute(
                """UPDATE kb_context_packets SET readiness_state='INVALIDATED', invalidation_reason='SNAPSHOT_CHANGED'
                   WHERE campaign_id=? AND snapshot_revision<>? AND readiness_state IN ('READY','NOT_READY')""",
                (campaign_id, revision),
            )
        reusable = database.connection.execute(
            """SELECT * FROM kb_context_packets
               WHERE campaign_id=? AND snapshot_revision=? AND packet_hash=?
                 AND readiness_state IN ('READY','NOT_READY') AND expires_at>? AND invalidation_reason IS NULL
               ORDER BY id DESC LIMIT 1""",
            (campaign_id, revision, packet_hash, _iso(current_time)),
        ).fetchone()
        if reusable is not None:
            return _packet_dict(database, reusable)
        database.connection.execute(
            """UPDATE kb_context_packets SET readiness_state='INVALIDATED', invalidation_reason='INPUTS_CHANGED'
               WHERE campaign_id=? AND snapshot_revision=? AND readiness_state IN ('READY','NOT_READY')""",
            (campaign_id, revision),
        )
        cursor = database.connection.execute(
            """INSERT INTO kb_context_packets
               (campaign_id, snapshot_revision, created_at, expires_at, readiness_state,
                required_section_coverage_json, source_ids_json, source_hashes_json,
                contradiction_status, packet_content, packet_hash, invalidation_reason,
                used_by_batch, truncation_json, explanation_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, ?, ?)""",
            (
                campaign_id,
                revision,
                _iso(current_time),
                expires_at,
                readiness,
                _json(validation["coverage"]),
                _json(source_ids),
                _json(source_hashes),
                "CONTRADICTORY" if validation["contradictions"] else "NONE",
                approved_content,
                packet_hash,
                _json(truncation),
                _json(explanation),
            ),
        )
        packet_id = cursor.lastrowid
        if packet_id is None:
            raise RuntimeError("context packet insert did not return an id")
        for record in accepted:
            database.connection.execute(
                """INSERT INTO kb_context_packet_sources
                   (packet_id, source_id, kb_status, confidence, heading, drive_file_id,
                    kb_path, source_url, modified_at, snapshot_revision, content_hash, retrieved_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    packet_id,
                    record["source_id"],
                    record["kb_status"],
                    record["confidence"],
                    record["heading"],
                    record["drive_file_id"],
                    record["kb_path"],
                    record["source_url"],
                    record["modified_at"],
                    record["snapshot_revision"],
                    record["content_hash"],
                    record["retrieved_at"],
                ),
            )
    return _packet_dict(database, _packet_row(database, int(packet_id)))


def get_current_context_packet(
    database: Database, campaign_id: int, *, snapshot_revision: Optional[str] = None, now: Optional[datetime] = None
) -> Optional[dict[str, Any]]:
    """Return only a non-expired, non-invalidated READY packet."""
    migrate_step8(database)
    current_time = _now(now)
    row = database.connection.execute(
        """SELECT * FROM kb_context_packets
           WHERE campaign_id=? AND readiness_state='READY' AND invalidation_reason IS NULL
           ORDER BY id DESC LIMIT 1""",
        (campaign_id,),
    ).fetchone()
    row = _mark_expired(database, row, current_time)
    if row is None:
        return None
    if snapshot_revision is not None and row["snapshot_revision"] != snapshot_revision:
        invalidate_context_packet(database, row["id"], "SNAPSHOT_CHANGED")
        return None
    if _parse_time(row["expires_at"], "expires_at") <= current_time:
        return None
    return _packet_dict(database, row)


def require_ready_context_packet(database: Database, campaign_id: int, *, snapshot_revision: Optional[str] = None, now: Optional[datetime] = None) -> dict[str, Any]:
    packet = get_current_context_packet(database, campaign_id, snapshot_revision=snapshot_revision, now=now)
    if packet is None:
        raise ContextPacketNotReadyError(f"no READY context packet for campaign {campaign_id}")
    return packet


def drafting_readiness(database: Database, campaign_id: int, *, snapshot_revision: Optional[str] = None, now: Optional[datetime] = None) -> dict[str, Any]:
    packet = get_current_context_packet(database, campaign_id, snapshot_revision=snapshot_revision, now=now)
    return {"ready": packet is not None, "packet_id": packet["packet_id"] if packet else None, "reason": None if packet else "READY_CONTEXT_PACKET_REQUIRED"}


def inspect_packet_readiness(database: Database, packet_id: int, *, now: Optional[datetime] = None) -> dict[str, Any]:
    migrate_step8(database)
    row = _mark_expired(database, _packet_row(database, packet_id), _now(now))
    packet = _packet_dict(database, row)
    return {
        "packet_id": packet["packet_id"],
        "campaign_id": packet["campaign_id"],
        "snapshot_revision": packet["snapshot_revision"],
        "readiness_state": packet["readiness_state"],
        "required_section_coverage": packet["required_section_coverage"],
        "missing_sections": packet["explanation"].get("missing_sections", []),
        "contradictions": packet["explanation"].get("contradictions", []),
        "reasons": packet["explanation"].get("reasons", []),
        "invalidation_reason": packet["invalidation_reason"],
        "expires_at": packet["expires_at"],
        "truncation": packet["truncation"],
    }


def explain_packet_readiness(database: Database, packet_id: int) -> dict[str, Any]:
    return inspect_packet_readiness(database, packet_id)


def invalidate_context_packet(database: Database, packet_id: int, reason: str) -> dict[str, Any]:
    if not isinstance(reason, str) or not reason.strip():
        raise KnowledgeRecordValidationError("invalidation reason is required")
    migrate_step8(database)
    with database.connection:
        database.connection.execute(
            "UPDATE kb_context_packets SET readiness_state='INVALIDATED', invalidation_reason=? WHERE id=?",
            (reason.strip()[:200], packet_id),
        )
    return inspect_packet_readiness(database, packet_id)


def mark_packet_used_by_batch(database: Database, packet_id: int, batch_id: str, *, now: Optional[datetime] = None) -> dict[str, Any]:
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise KnowledgeRecordValidationError("batch_id is required")
    migrate_step8(database)
    with database.connection:
        database.connection.execute("UPDATE kb_context_packets SET used_by_batch=1 WHERE id=?", (packet_id,))
        database.connection.execute(
            "INSERT OR IGNORE INTO kb_context_packet_batches (packet_id, batch_id, used_at) VALUES (?, ?, ?)",
            (packet_id, batch_id.strip(), _iso(_now(now))),
        )
    return _packet_dict(database, _packet_row(database, packet_id))


def summarize_packet_provenance(database: Database, packet_id: int) -> dict[str, Any]:
    migrate_step8(database)
    packet = _packet_dict(database, _packet_row(database, packet_id))
    sources = database.connection.execute(
        """SELECT source_id, kb_status, confidence, heading, drive_file_id, kb_path,
                  source_url, modified_at, snapshot_revision, content_hash, retrieved_at
           FROM kb_context_packet_sources WHERE packet_id=? ORDER BY source_id""",
        (packet_id,),
    ).fetchall()
    batch_count = database.connection.execute(
        "SELECT COUNT(*) FROM kb_context_packet_batches WHERE packet_id=?", (packet_id,)
    ).fetchone()[0]
    return {
        "packet_id": packet["packet_id"],
        "campaign_id": packet["campaign_id"],
        "snapshot_revision": packet["snapshot_revision"],
        "readiness_state": packet["readiness_state"],
        "packet_hash": packet["packet_hash"],
        "source_count": len(sources),
        "source_ids": [row["source_id"] for row in sources],
        "source_hashes": [row["content_hash"] for row in sources],
        "source_metadata": [dict(row) for row in sources],
        "used_by_batch": packet["used_by_batch"],
        "batch_count": batch_count,
        "truncation": packet["truncation"],
    }


def retrieve_and_build_context_packet(
    database: Database,
    campaign_id: int,
    retrieval_provider: Any,
    *,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Call the injected kb_recall-compatible provider once, then validate locally."""
    recall = getattr(retrieval_provider, "kb_recall", None) or getattr(retrieval_provider, "recall", None)
    if not callable(recall):
        raise KnowledgeRecordValidationError("retrieval provider must expose kb_recall or recall")
    response = recall(campaign_id=campaign_id, sections=REQUIRED_GOLD_SECTIONS)
    snapshot_revision = None
    records = response
    if isinstance(response, Mapping):
        snapshot_revision = _value(response, "snapshot_revision", "manifest_revision", "revision")
        records = response.get("records", [])
    return build_context_packet(database, campaign_id, records, snapshot_revision=snapshot_revision, now=now)


# Explicit aliases make the callable operations easy to bind without creating a
# second retrieval implementation.
validate_gold_records = validate_recalled_gold_records
inspect_context_packet = inspect_packet_readiness
retrieve_current_packet = get_current_context_packet
invalidate_stale_packet = invalidate_context_packet
explain_missing_section_or_contradiction = explain_packet_readiness


__all__ = [
    "STEP8_MIGRATION_VERSION",
    "TRUST_TIERS",
    "REQUIRED_GOLD_SECTIONS",
    "ContextPacketNotReadyError",
    "KnowledgeRecordValidationError",
    "migrate_step8",
    "validate_recalled_gold_records",
    "validate_gold_records",
    "build_context_packet",
    "inspect_packet_readiness",
    "inspect_context_packet",
    "get_current_context_packet",
    "retrieve_current_packet",
    "require_ready_context_packet",
    "drafting_readiness",
    "invalidate_context_packet",
    "invalidate_stale_packet",
    "explain_packet_readiness",
    "explain_missing_section_or_contradiction",
    "mark_packet_used_by_batch",
    "summarize_packet_provenance",
    "retrieve_and_build_context_packet",
]
