"""Step 15 fixture-only decision-maker identity and personalization research.

This module stores bounded identity observations, deterministic personalization
packets, and channel draft *requests*. It never creates message content,
contacts LinkedIn, invokes a provider, or sends anything.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from paldo_os_outbound import DEFAULT_DB_PATH, Database, normalize_domain, normalize_email
from step14_scheduler import migrate_step14
from step16_campaign_intelligence import campaign_intelligence_for_campaign, select_campaign_concepts


STEP15_MIGRATION_VERSION = 17
BUSINESS_FIRST = "BUSINESS_FIRST"
LINKEDIN_ONLY = "LINKEDIN_ONLY"
ALLOWED_LANES = frozenset({BUSINESS_FIRST, LINKEDIN_ONLY})
ALLOWED_SOURCE_TYPES = frozenset(
    {"MANUAL_OPERATOR", "OFFICIAL_BUSINESS_SOURCE", "PUBLIC_SEARCH_INDEX", "AUTHORIZED_PROVIDER"}
)
ALLOWED_PACKET_SOURCE_TYPES = ALLOWED_SOURCE_TYPES | {"INTERNAL_OFFER_FIXTURE"}
ALLOWED_VERIFICATION_STATUSES = frozenset({"VERIFIED", "UNCLEAR", "REJECTED"})
ALLOWED_REQUEST_STATUSES = frozenset({"PENDING_POLICY", "READY", "HOLD", "REJECTED"})
ALLOWED_DECISION_MAKER_ROLES = frozenset(
    {
        "founder",
        "co-founder",
        "owner",
        "ceo",
        "chief executive officer",
        "managing partner",
        "managing director",
        "business director",
        "practice director",
        "operations director",
        "operations manager",
        "practice manager",
        "general manager",
        "operating partner",
        "president",
    }
)

_STEP15_SCHEMA = """
CREATE TABLE IF NOT EXISTS lead_decision_makers (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    public_name TEXT NOT NULL,
    current_role TEXT NOT NULL,
    public_linkedin_url TEXT,
    public_business_email TEXT,
    business_domain TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_url TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    verification_status TEXT NOT NULL CHECK (verification_status IN ('VERIFIED','UNCLEAR','REJECTED')),
    verification_reason TEXT NOT NULL,
    record_fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (lead_id, record_fingerprint)
);
CREATE UNIQUE INDEX IF NOT EXISTS lead_decision_makers_linkedin_idx
    ON lead_decision_makers(public_linkedin_url)
    WHERE public_linkedin_url IS NOT NULL;
CREATE INDEX IF NOT EXISTS lead_decision_makers_lead_idx
    ON lead_decision_makers(lead_id, verification_status, updated_at);

CREATE TABLE IF NOT EXISTS personalization_research_packets (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    decision_maker_id INTEGER REFERENCES lead_decision_makers(id) ON DELETE RESTRICT,
    offer_version TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    packet_state TEXT NOT NULL CHECK (packet_state IN ('READY','HOLD','REJECTED')),
    packet_json TEXT NOT NULL,
    evidence_fingerprint TEXT NOT NULL,
    supersedes_id INTEGER REFERENCES personalization_research_packets(id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    UNIQUE (lead_id, campaign_id, evidence_fingerprint)
);
CREATE INDEX IF NOT EXISTS personalization_packets_current_idx
    ON personalization_research_packets(lead_id, campaign_id, version DESC, packet_state);

CREATE TABLE IF NOT EXISTS channel_draft_requests (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE RESTRICT,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    decision_maker_id INTEGER REFERENCES lead_decision_makers(id) ON DELETE RESTRICT,
    personalization_packet_id INTEGER NOT NULL REFERENCES personalization_research_packets(id) ON DELETE RESTRICT,
    personalization_packet_version INTEGER NOT NULL CHECK (personalization_packet_version >= 1),
    offer_version TEXT NOT NULL,
    lane TEXT NOT NULL CHECK (lane IN ('BUSINESS_FIRST','LINKEDIN_ONLY')),
    channel TEXT NOT NULL CHECK (channel IN ('EMAIL','LINKEDIN')),
    request_fingerprint TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('PENDING_POLICY','READY','HOLD','REJECTED')),
    request_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS channel_draft_requests_lead_idx
    ON channel_draft_requests(lead_id, campaign_id, channel, status);
"""


class Step15BlockedError(RuntimeError):
    """Raised when a fixture-only mutation is aimed at the canonical database."""


class Step15ValidationError(ValueError):
    """Raised for malformed or unsafe fixture input."""


def _now(value: Optional[datetime]) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise Step15ValidationError("timestamp must include timezone")
    return current.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: datetime) -> str:
    return _now(value).isoformat()


def _parse_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise Step15ValidationError("observed_at is required")
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise Step15ValidationError("observed_at is invalid") from exc
    return _now(parsed)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _safe_text(value: Any, field: str, *, maximum: int = 500) -> str:
    if not isinstance(value, str) or not value.strip():
        raise Step15ValidationError(f"{field} is required")
    text = " ".join(value.split())
    if len(text) > maximum or "<" in text or ">" in text:
        raise Step15ValidationError(f"{field} is not bounded")
    if re.search(r"(?:password|credential|api[_ -]?key|access[_ -]?token|private key|client information)", text, re.I):
        raise Step15ValidationError(f"{field} contains restricted data")
    return text


def _safe_url(value: Any, field: str, *, required: bool = True) -> Optional[str]:
    if value is None or value == "":
        if required:
            raise Step15ValidationError(f"{field} is required")
        return None
    if not isinstance(value, str) or len(value) > 500 or "@" in value:
        raise Step15ValidationError(f"{field} is invalid")
    value = value.strip()
    if not re.fullmatch(r"https?://[A-Za-z0-9.-]+(?:/[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*)?", value):
        raise Step15ValidationError(f"{field} must be an HTTP(S) URL")
    return value


def _linkedin_url(value: Any) -> Optional[str]:
    value = _safe_url(value, "public_linkedin_url", required=False)
    if value is None:
        return None
    if not re.fullmatch(r"https://(?:www\.)?linkedin\.com/in/[A-Za-z0-9][A-Za-z0-9_-]*/?", value):
        raise Step15ValidationError("public_linkedin_url must be a public profile URL")
    return value.rstrip("/")


def _is_durable(database: Database) -> bool:
    return database.path.resolve() == DEFAULT_DB_PATH.resolve()


def _require_fixture(database: Database, fixture_override: bool) -> None:
    if not fixture_override or _is_durable(database):
        raise Step15BlockedError("Step 15 mutations require an isolated fixture database")


def _record_event(database: Database, event_type: str, entity_type: str, entity_id: Any, details: Mapping[str, Any], now: datetime) -> None:
    with database.connection:
        database.connection.execute(
            "INSERT INTO events (event_type, entity_type, entity_id, metadata, created_at) VALUES (?, ?, ?, ?, ?)",
            (event_type, entity_type, str(entity_id), _json(details), _iso(now)),
        )


def migrate_step15(database_or_path: Database | str | Path) -> int:
    """Apply the additive Step 15 schema and safe defaults idempotently."""
    database = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    close_after = not isinstance(database_or_path, Database)
    try:
        step15_exists = database.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
        ).fetchone() is not None and database.connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version=?", (STEP15_MIGRATION_VERSION,)
        ).fetchone() is not None
        if not step15_exists:
            migrate_step14(database)
        with database.connection:
            database.connection.executescript(_STEP15_SCHEMA)
            decision_maker_columns = {row[1] for row in database.connection.execute("PRAGMA table_info(lead_decision_makers)")}
            if "public_business_email" not in decision_maker_columns:
                database.connection.execute("ALTER TABLE lead_decision_makers ADD COLUMN public_business_email TEXT")
            defaults = (
                ("decision_maker_enrichment_enabled", "0", "integer"),
                ("linkedin_provider_mode", "FIXTURE_ONLY", "text"),
                ("linkedin_sending_enabled", "0", "integer"),
                ("linkedin_daily_cap", "0", "integer"),
                ("linkedin_message_policy_status", "UNDECIDED", "text"),
                ("personalization_research_enabled", "0", "integer"),
            )
            database.connection.executemany(
                "INSERT OR IGNORE INTO system_config (key, value, value_type) VALUES (?, ?, ?)",
                defaults,
            )
            database.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations(version, name, applied_at)
                   VALUES (?, ?, ?)""",
                (STEP15_MIGRATION_VERSION, "step15_decision_maker_personalization", _iso(datetime.now(timezone.utc))),
            )
        return STEP15_MIGRATION_VERSION
    finally:
        if close_after:
            database.close()


def _lead(database: Database, lead_id: int) -> dict[str, Any]:
    row = database.get_lead(lead_id)
    if row is None:
        raise Step15ValidationError("lead does not exist")
    return row


def _qualified(database: Database, lead_id: int, campaign_id: int) -> bool:
    if database._table_has_column("pipeline_qualification_results", "final_state"):
        row = database.connection.execute(
            """SELECT final_state, classification, qualification_result
               FROM pipeline_qualification_results
               WHERE lead_id=? AND campaign_id=? ORDER BY version DESC, id DESC LIMIT 1""",
            (lead_id, campaign_id),
        ).fetchone()
        if row is not None:
            return str(row["final_state"] or row["classification"] or row["qualification_result"]).upper() in {"QUALIFIED", "STRONG", "QUALIFY"}
    row = database.connection.execute(
        """SELECT classification, qualification_result FROM qualification_results
           WHERE lead_id=? AND campaign_id=? ORDER BY evaluated_at DESC LIMIT 1""",
        (lead_id, campaign_id),
    ).fetchone()
    if row is not None:
        return str(row["classification"] or row["qualification_result"]).upper() in {"QUALIFIED", "STRONG", "QUALIFY"}
    lead = _lead(database, lead_id)
    return False


def _campaign_ready(database: Database, lead: Mapping[str, Any], campaign_id: int) -> tuple[bool, list[str]]:
    campaign = database.get_campaign(campaign_id)
    reasons: list[str] = []
    if campaign is None:
        return False, ["CAMPAIGN_NOT_FOUND"]
    if campaign.get("status") != "ACTIVE":
        reasons.append("CAMPAIGN_INACTIVE")
    if "campaign_id" in lead and lead.get("campaign_id") not in (None, campaign_id):
        reasons.append("LEAD_CAMPAIGN_MISMATCH")
    if not _qualified(database, int(lead["id"]), campaign_id):
        reasons.append("LEAD_NOT_QUALIFIED")
    if database.is_suppressed(lead_id=lead["id"], email=lead.get("email"), domain=lead.get("domain")):
        reasons.append("LEAD_SUPPRESSED")
    return not reasons, reasons


def _role_matches(role: str) -> bool:
    normalized = " ".join(role.casefold().replace("/", " ").split())
    return normalized in ALLOWED_DECISION_MAKER_ROLES or any(
        phrase in normalized for phrase in ("founder", "co-founder", "business director", "practice manager", "operations director")
    )


def register_fixture_decision_maker(
    database: Database,
    *,
    lead_id: int,
    record: Mapping[str, Any],
    fixture_override: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Accept one bounded, fictional public decision-maker record."""
    _require_fixture(database, fixture_override)
    migrate_step15(database)
    current = _now(now)
    lead = _lead(database, lead_id)
    config = database.read_config()
    if not int(config.get("decision_maker_enrichment_enabled", 0)):
        return {"status": "HOLD", "reasons": ["DECISION_MAKER_ENRICHMENT_DISABLED"]}
    try:
        name = _safe_text(record.get("public_name"), "public_name", maximum=160)
        role = _safe_text(record.get("current_role"), "current_role", maximum=120)
        business_domain = normalize_domain(record.get("business_domain"))
        public_business_email = record.get("public_business_email")
        if public_business_email is not None:
            public_business_email = normalize_email(public_business_email)
            if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", public_business_email):
                raise Step15ValidationError("public_business_email is invalid")
        source_type = record.get("source_type")
        if source_type not in ALLOWED_SOURCE_TYPES:
            raise Step15ValidationError("source_type is unsupported")
        source_url = _safe_url(record.get("source_url"), "source_url")
        linkedin_url = _linkedin_url(record.get("public_linkedin_url"))
        observed_at = _parse_time(record.get("observed_at"))
        confidence = record.get("confidence")
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise Step15ValidationError("confidence must be between 0 and 1")
        verification = str(record.get("verification_status") or "").upper()
        if verification not in ALLOWED_VERIFICATION_STATUSES:
            raise Step15ValidationError("verification_status is unsupported")
    except (TypeError, ValueError, Step15ValidationError) as exc:
        return {"status": "REJECTED", "reasons": ["DECISION_MAKER_RECORD_INVALID", str(exc)]}

    if business_domain != normalize_domain(lead.get("domain")):
        return {"status": "REJECTED", "reasons": ["BUSINESS_ASSOCIATION_MISMATCH"]}
    if not _role_matches(role):
        return {"status": "REJECTED", "reasons": ["ROLE_NOT_OPERATIONAL_DECISION_MAKER"]}
    if verification == "REJECTED":
        return {"status": "REJECTED", "reasons": ["DECISION_MAKER_VERIFICATION_REJECTED"]}

    fingerprint_data = {
        "lead_id": lead_id,
        "public_name": name,
        "current_role": role,
        "public_linkedin_url": linkedin_url,
        "public_business_email": public_business_email,
        "business_domain": business_domain,
        "source_type": source_type,
        "source_url": source_url,
        "observed_at": _iso(observed_at),
        "confidence": float(confidence),
        "verification_status": verification,
    }
    fingerprint = _fingerprint(fingerprint_data)
    duplicate = database.connection.execute(
        "SELECT * FROM lead_decision_makers WHERE lead_id=? AND record_fingerprint=?",
        (lead_id, fingerprint),
    ).fetchone()
    if duplicate is not None:
        return {"status": "DUPLICATE", "decision_maker_id": duplicate["id"], "verification_status": duplicate["verification_status"]}
    if linkedin_url is not None:
        other = database.connection.execute(
            "SELECT * FROM lead_decision_makers WHERE public_linkedin_url=?", (linkedin_url,)
        ).fetchone()
        if other is not None:
            if int(other["lead_id"]) != lead_id:
                return {"status": "REJECTED", "reasons": ["LINKEDIN_PROFILE_ASSOCIATED_WITH_OTHER_BUSINESS"]}
            return {"status": "DUPLICATE", "decision_maker_id": other["id"], "verification_status": other["verification_status"]}

    status = "HOLD" if verification == "UNCLEAR" else "ACCEPTED"
    reason = "AMBIGUOUS_IDENTITY" if status == "HOLD" else "VERIFIED_FIXTURE_RECORD"
    with database.connection:
        cursor = database.connection.execute(
            """INSERT INTO lead_decision_makers
               (lead_id, public_name, current_role, public_linkedin_url, public_business_email, business_domain,
                source_type, source_url, observed_at, confidence, verification_status,
                verification_reason, record_fingerprint, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (lead_id, name, role, linkedin_url, public_business_email, business_domain, source_type, source_url,
             _iso(observed_at), float(confidence), verification, reason, fingerprint,
             _iso(current), _iso(current)),
        )
    decision_maker_id = int(cursor.lastrowid)
    _record_event(database, "DECISION_MAKER_FIXTURE_ACCEPTED", "lead_decision_maker", decision_maker_id,
                  {"lead_id": lead_id, "verification_status": verification, "source_type": source_type}, current)
    return {"status": status, "decision_maker_id": decision_maker_id, "verification_status": verification, "reasons": [] if status == "ACCEPTED" else [reason]}


def _evidence_exists(database: Database, lead_id: int, evidence_id: int) -> bool:
    row = database.connection.execute(
        "SELECT id FROM evidence WHERE id=? AND lead_id=?", (evidence_id, lead_id)
    ).fetchone()
    if row is not None:
        return True
    if database._table_has_column("pipeline_evidence", "is_current"):
        row = database.connection.execute(
            "SELECT id FROM pipeline_evidence WHERE id=? AND lead_id=? AND is_current=1",
            (evidence_id, lead_id),
        ).fetchone()
        return row is not None
    return False


def _research_item(database: Database, lead_id: int, item: Any, field: str, *, source_required: bool = True) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise Step15ValidationError(f"{field} must be a bounded object")
    kind = item.get("kind")
    if field == "pain_hypothesis":
        if kind != "HYPOTHESIS":
            raise Step15ValidationError("pain hypothesis must remain labeled HYPOTHESIS")
        text = _safe_text(item.get("text"), field)
        if re.search(r"\b(missed|misses|no[- ]shows|failed follow[- ]?ups?|lost revenue|revenue loss|ignored inquiries)\b", text, re.I):
            raise Step15ValidationError("UNSUPPORTED_PAIN_CLAIM")
        basis = item.get("basis")
        if not isinstance(basis, list) or not basis or any(not isinstance(value, str) for value in basis):
            raise Step15ValidationError("pain hypothesis needs explicit evidence basis")
        return {"kind": kind, "text": text, "basis": sorted(set(basis))}
    if field == "desired_outcome":
        expected_kind = "DESIRED_OUTCOME"
    elif field == "capability_proof_match":
        expected_kind = "CAPABILITY_MATCH"
    else:
        expected_kind = "OBSERVATION"
    if kind != expected_kind:
        raise Step15ValidationError(f"{field} has the wrong evidence label")
    text = _safe_text(item.get("text"), field)
    source_type = item.get("source_type")
    allowed = ALLOWED_PACKET_SOURCE_TYPES
    if source_type not in allowed:
        raise Step15ValidationError(f"{field} source type is unsupported")
    source_url = _safe_url(item.get("source_url"), f"{field}.source_url", required=source_required)
    observed_at = _parse_time(item.get("observed_at"))
    confidence = item.get("confidence")
    if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        raise Step15ValidationError(f"{field}.confidence is invalid")
    result = {
        "kind": kind,
        "text": text,
        "source_type": source_type,
        "source_url": source_url,
        "observed_at": _iso(observed_at),
        "confidence": float(confidence),
    }
    if item.get("evidence_id") is not None:
        evidence_id = item.get("evidence_id")
        if isinstance(evidence_id, bool) or not isinstance(evidence_id, int):
            raise Step15ValidationError(f"{field}.evidence_id is invalid")
        if not _evidence_exists(database, lead_id, evidence_id):
            raise Step15ValidationError(f"{field}.evidence_id is not current evidence for this lead")
        result["evidence_id"] = evidence_id
    return result


def _packet_failure(database: Database, lead_id: int, campaign_id: int, state: str, reasons: list[str], now: datetime) -> dict[str, Any]:
    _record_event(database, "PERSONALIZATION_PACKET_NOT_CREATED", "lead", lead_id,
                  {"campaign_id": campaign_id, "state": state, "reason_codes": sorted(set(reasons))}, now)
    return {"status": state, "reasons": sorted(set(reasons))}


def create_fixture_personalization_packet(
    database: Database,
    *,
    lead_id: int,
    campaign_id: int,
    offer_version: str,
    research: Mapping[str, Any],
    decision_maker_id: Optional[int] = None,
    campaign_intelligence_keys: Optional[list[str]] = None,
    fixture_override: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Create or reuse a bounded packet for a qualified fictional lead."""
    _require_fixture(database, fixture_override)
    migrate_step15(database)
    current = _now(now)
    lead = _lead(database, lead_id)
    config = database.read_config()
    if not int(config.get("personalization_research_enabled", 0)):
        return _packet_failure(database, lead_id, campaign_id, "HOLD", ["PERSONALIZATION_RESEARCH_DISABLED"], current)
    ready, reasons = _campaign_ready(database, lead, campaign_id)
    if not ready:
        return _packet_failure(database, lead_id, campaign_id, "HOLD", reasons, current)
    if not isinstance(offer_version, str) or not offer_version.strip():
        return _packet_failure(database, lead_id, campaign_id, "REJECTED", ["OFFER_VERSION_REQUIRED"], current)

    decision_maker = None
    if decision_maker_id is not None:
        decision_maker = database.connection.execute(
            "SELECT * FROM lead_decision_makers WHERE id=? AND lead_id=?", (decision_maker_id, lead_id)
        ).fetchone()
        if decision_maker is None:
            return _packet_failure(database, lead_id, campaign_id, "REJECTED", ["DECISION_MAKER_NOT_FOUND"], current)
        if decision_maker["verification_status"] != "VERIFIED":
            return _packet_failure(database, lead_id, campaign_id, "HOLD", ["DECISION_MAKER_NOT_VERIFIED"], current)

    try:
        business = _research_item(database, lead_id, research.get("business_observation"), "business_observation")
        operational = _research_item(database, lead_id, research.get("operational_signal"), "operational_signal")
        hypothesis = _research_item(database, lead_id, research.get("pain_hypothesis"), "pain_hypothesis", source_required=False)
        desired_outcome = None
        if research.get("desired_outcome") is not None:
            desired_outcome = _research_item(database, lead_id, research.get("desired_outcome"), "desired_outcome")
        capability = _research_item(database, lead_id, research.get("capability_proof_match"), "capability_proof_match")
        if "Business Booking and Follow-Up System" not in capability["text"]:
            raise Step15ValidationError("CAPABILITY_PROOF_MISMATCH")
        selected_intelligence = (
            select_campaign_concepts(
                campaign_intelligence_for_campaign(database, campaign_id)["campaign_key"],
                campaign_intelligence_keys,
            )
            if campaign_intelligence_keys is not None
            else []
        )
        dm_observation = None
        if decision_maker is not None:
            dm_observation = _research_item(database, lead_id, research.get("decision_maker_observation"), "decision_maker_observation")
            if decision_maker["public_name"] not in dm_observation["text"] or decision_maker["current_role"].casefold() not in dm_observation["text"].casefold():
                raise Step15ValidationError("DECISION_MAKER_OBSERVATION_MISMATCH")
        unknowns = research.get("unknowns", [])
        if not isinstance(unknowns, list) or any(not isinstance(item, str) for item in unknowns) or len(unknowns) > 12:
            raise Step15ValidationError("unknowns must be a bounded list")
        unknowns = [_safe_text(item, "unknown", maximum=240) for item in unknowns]
    except (TypeError, ValueError, Step15ValidationError) as exc:
        reason = str(exc)
        code = reason if reason.isupper() and " " not in reason else "PERSONALIZATION_INPUT_INVALID"
        return _packet_failure(database, lead_id, campaign_id, "REJECTED", [code], current)

    packet_body: dict[str, Any] = {
        "lead_id": lead_id,
        "campaign_id": campaign_id,
        "offer_version": offer_version.strip(),
        "business_observation": business,
        "decision_maker_observation": dm_observation,
        "operational_signal": operational,
        "pain_hypothesis": hypothesis,
        "desired_outcome": desired_outcome,
        "capability_proof_match": capability,
        "campaign_intelligence_refs": selected_intelligence,
        "unknowns": unknowns,
        "sources": [item for item in (business, dm_observation, operational, desired_outcome, capability) if item is not None],
    }
    evidence_fingerprint = _fingerprint({"decision_maker_id": decision_maker_id, "packet": packet_body})
    existing = database.connection.execute(
        """SELECT * FROM personalization_research_packets
           WHERE lead_id=? AND campaign_id=? AND evidence_fingerprint=?""",
        (lead_id, campaign_id, evidence_fingerprint),
    ).fetchone()
    if existing is not None:
        return {"status": "REUSED", "packet_id": existing["id"], "version": existing["version"], "reasons": []}
    previous = database.connection.execute(
        """SELECT * FROM personalization_research_packets
           WHERE lead_id=? AND campaign_id=? ORDER BY version DESC, id DESC LIMIT 1""",
        (lead_id, campaign_id),
    ).fetchone()
    version = 1 if previous is None else int(previous["version"]) + 1
    with database.connection:
        cursor = database.connection.execute(
            """INSERT INTO personalization_research_packets
               (lead_id, campaign_id, decision_maker_id, offer_version, version,
                packet_state, packet_json, evidence_fingerprint, supersedes_id, created_at)
               VALUES (?, ?, ?, ?, ?, 'READY', ?, ?, ?, ?)""",
            (lead_id, campaign_id, decision_maker_id, offer_version.strip(), version,
             _json(packet_body), evidence_fingerprint, None if previous is None else previous["id"], _iso(current)),
        )
    packet_id = int(cursor.lastrowid)
    _record_event(database, "PERSONALIZATION_PACKET_CREATED", "personalization_packet", packet_id,
                  {"lead_id": lead_id, "campaign_id": campaign_id, "version": version}, current)
    return {"status": "READY", "packet_id": packet_id, "version": version, "reasons": []}


def _verified_dm(database: Database, decision_maker_id: Optional[int], lead_id: int) -> Optional[sqlite3.Row]:
    if decision_maker_id is not None:
        return database.connection.execute(
            "SELECT * FROM lead_decision_makers WHERE id=? AND lead_id=? AND verification_status='VERIFIED'",
            (decision_maker_id, lead_id),
        ).fetchone()
    return database.connection.execute(
        """SELECT * FROM lead_decision_makers
           WHERE lead_id=? AND verification_status='VERIFIED' AND public_linkedin_url IS NOT NULL
           ORDER BY id LIMIT 1""",
        (lead_id,),
    ).fetchone()


def _contact_route(lead: Mapping[str, Any], dm: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    if dm is not None and dm.get("public_business_email"):
        return {
            "type": "OWNER_BUSINESS_EMAIL",
            "value": dm["public_business_email"],
            "source_url": dm.get("source_url"),
            "decision_maker_id": dm.get("id"),
            "public_name": dm.get("public_name"),
            "current_role": dm.get("current_role"),
        }
    if lead.get("email"):
        return {
            "type": "GENERAL_BUSINESS_EMAIL",
            "value": lead["email"],
            "source_url": lead.get("source_url") or lead.get("website"),
            "decision_maker_id": None,
            "public_name": None,
            "current_role": None,
        }
    if dm is not None and dm.get("public_linkedin_url"):
        return {
            "type": "LINKEDIN",
            "value": dm["public_linkedin_url"],
            "source_url": dm.get("source_url"),
            "decision_maker_id": dm.get("id"),
            "public_name": dm.get("public_name"),
            "current_role": dm.get("current_role"),
        }
    return None


def prepare_fixture_channel_draft_requests(
    database: Database,
    *,
    lead_id: int,
    campaign_id: int,
    packet_id: int,
    offer_version: str,
    lane: str = BUSINESS_FIRST,
    fixture_override: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Prepare identity-linked channel requests without subject or message text."""
    _require_fixture(database, fixture_override)
    migrate_step15(database)
    current = _now(now)
    lead = _lead(database, lead_id)
    if lane not in ALLOWED_LANES:
        return {"status": "REJECTED", "reasons": ["LANE_UNSUPPORTED"], "channels": [], "request_ids": []}
    ready, reasons = _campaign_ready(database, lead, campaign_id)
    if not ready:
        return {"status": "REJECTED" if "LEAD_SUPPRESSED" in reasons else "HOLD", "reasons": reasons, "channels": [], "request_ids": []}
    config = database.read_config()
    if not int(config.get("personalization_research_enabled", 0)):
        return {"status": "HOLD", "reasons": ["PERSONALIZATION_RESEARCH_DISABLED"], "channels": [], "request_ids": []}
    if config.get("linkedin_provider_mode") != "FIXTURE_ONLY" or int(config.get("linkedin_sending_enabled", 0)) != 0:
        return {"status": "HOLD", "reasons": ["LINKEDIN_BOUNDARY_UNSAFE"], "channels": [], "request_ids": []}
    if config.get("linkedin_message_policy_status") != "UNDECIDED":
        return {"status": "HOLD", "reasons": ["LINKEDIN_MESSAGE_POLICY_UNDECIDED_REQUIRED"], "channels": [], "request_ids": []}
    packet = database.connection.execute(
        """SELECT * FROM personalization_research_packets
           WHERE id=? AND lead_id=? AND campaign_id=? AND packet_state='READY'""",
        (packet_id, lead_id, campaign_id),
    ).fetchone()
    if packet is None:
        return {"status": "HOLD", "reasons": ["READY_PERSONALIZATION_PACKET_REQUIRED"], "channels": [], "request_ids": []}
    try:
        offer_version = _safe_text(offer_version, "offer_version", maximum=80)
    except Step15ValidationError:
        return {"status": "REJECTED", "reasons": ["OFFER_VERSION_REQUIRED"], "channels": [], "request_ids": []}

    decision_maker_id = packet["decision_maker_id"]
    dm = _verified_dm(database, decision_maker_id, lead_id)
    dm_data = None if dm is None else dict(dm)
    email_route = _contact_route(lead, dm_data)
    linkedin_route = None if dm_data is None or not dm_data.get("public_linkedin_url") else {
        "type": "LINKEDIN",
        "value": dm_data["public_linkedin_url"],
        "source_url": dm_data.get("source_url"),
        "decision_maker_id": dm_data.get("id"),
        "public_name": dm_data.get("public_name"),
        "current_role": dm_data.get("current_role"),
    }
    channels: list[str] = []
    if lane == BUSINESS_FIRST and email_route is not None and email_route["type"] in {"OWNER_BUSINESS_EMAIL", "GENERAL_BUSINESS_EMAIL"}:
        channels.append("EMAIL")
    if linkedin_route is not None:
        channels.append("LINKEDIN")
    if lane == LINKEDIN_ONLY:
        if linkedin_route is None:
            return {"status": "HOLD", "reasons": ["VERIFIED_LINKEDIN_PROFILE_REQUIRED"], "channels": [], "request_ids": []}
        if int(config.get("linkedin_daily_cap", 0)) <= 0:
            return {"status": "HOLD", "reasons": ["LINKEDIN_DAILY_CAP_ZERO"], "channels": [], "request_ids": []}
        channels = ["LINKEDIN"]
    if not channels:
        return {"status": "HOLD", "reasons": ["NO_ELIGIBLE_CHANNEL"], "channels": [], "request_ids": []}

    request_ids: list[int] = []
    for channel in channels:
        contact_route = email_route if channel == "EMAIL" else linkedin_route
        request_fingerprint = _fingerprint({
            "lead_id": lead_id,
            "campaign_id": campaign_id,
            "decision_maker_id": None if dm is None else dm["id"],
            "packet_id": packet["id"],
            "packet_version": packet["version"],
            "offer_version": offer_version,
            "lane": lane,
            "channel": channel,
            "contact_route": contact_route,
        })
        existing = database.connection.execute(
            "SELECT id FROM channel_draft_requests WHERE request_fingerprint=?", (request_fingerprint,)
        ).fetchone()
        if existing is not None:
            request_ids.append(int(existing["id"]))
            continue
        request_body = {
            "lead_id": lead_id,
            "decision_maker_id": None if dm is None else dm["id"],
            "public_name": None if dm is None else dm["public_name"],
            "current_role": None if dm is None else dm["current_role"],
            "public_linkedin_url": None if dm is None else dm["public_linkedin_url"],
            "business_domain": lead.get("domain"),
            "campaign_id": campaign_id,
            "offer_version": offer_version,
            "personalization_packet_id": packet["id"],
            "personalization_packet_version": packet["version"],
            "lane": lane,
            "channel": channel,
            "policy_status": "PENDING_POLICY",
            "contact_route": contact_route,
        }
        with database.connection:
            cursor = database.connection.execute(
                """INSERT INTO channel_draft_requests
                   (lead_id, campaign_id, decision_maker_id, personalization_packet_id,
                    personalization_packet_version, offer_version, lane, channel,
                    request_fingerprint, status, request_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING_POLICY', ?, ?, ?)""",
                (lead_id, campaign_id, None if dm is None else dm["id"], packet["id"], packet["version"],
                 offer_version, lane, channel, request_fingerprint, _json(request_body), _iso(current), _iso(current)),
            )
        request_ids.append(int(cursor.lastrowid))
    for request_id in request_ids:
        _record_event(database, "CHANNEL_DRAFT_REQUEST_CREATED", "channel_draft_request", request_id,
                      {"lead_id": lead_id, "campaign_id": campaign_id, "channel": "LINKEDIN" if len(channels) == 1 and channels[0] == "LINKEDIN" else "MULTI", "status": "PENDING_POLICY"}, current)
    return {
        "status": "READY",
        "channels": channels,
        "request_ids": request_ids,
        "contact_routes": {
            "EMAIL": email_route,
            "LINKEDIN": linkedin_route,
        },
        "reasons": [],
    }


def resolve_linkedin_only_fixture_seed(
    database: Database,
    *,
    campaign_id: int,
    seed: Mapping[str, Any],
    offer_version: str,
    research: Mapping[str, Any],
    fixture_override: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Resolve an existing fictional business and prepare one LinkedIn request."""
    _require_fixture(database, fixture_override)
    migrate_step15(database)
    lead_id = seed.get("lead_id")
    if lead_id is None and seed.get("business_domain"):
        domain = normalize_domain(seed["business_domain"])
        rows = database.connection.execute("SELECT id FROM leads WHERE domain=? ORDER BY id", (domain,)).fetchall()
        if len(rows) != 1:
            return {"status": "HOLD", "reasons": ["BUSINESS_NOT_RESOLVED"], "channels": [], "request_ids": []}
        lead_id = rows[0]["id"]
    if isinstance(lead_id, bool) or not isinstance(lead_id, int):
        return {"status": "HOLD", "reasons": ["BUSINESS_NOT_RESOLVED"], "channels": [], "request_ids": []}
    record = dict(seed)
    record.pop("lead_id", None)
    identity = register_fixture_decision_maker(database, lead_id=lead_id, record=record, fixture_override=True, now=now)
    if identity.get("status") not in {"ACCEPTED", "DUPLICATE"}:
        return {"status": identity.get("status", "HOLD"), "reasons": identity.get("reasons", []), "channels": [], "request_ids": []}
    packet = create_fixture_personalization_packet(
        database,
        lead_id=lead_id,
        campaign_id=campaign_id,
        decision_maker_id=identity["decision_maker_id"],
        offer_version=offer_version,
        research=research,
        fixture_override=True,
        now=now,
    )
    if packet.get("status") not in {"READY", "REUSED"}:
        return {"status": packet.get("status", "HOLD"), "reasons": packet.get("reasons", []), "channels": [], "request_ids": []}
    return prepare_fixture_channel_draft_requests(
        database,
        lead_id=lead_id,
        campaign_id=campaign_id,
        packet_id=packet["packet_id"],
        offer_version=offer_version,
        lane=LINKEDIN_ONLY,
        fixture_override=True,
        now=now,
    )


__all__ = [
    "ALLOWED_DECISION_MAKER_ROLES",
    "ALLOWED_SOURCE_TYPES",
    "BUSINESS_FIRST",
    "LINKEDIN_ONLY",
    "STEP15_MIGRATION_VERSION",
    "Step15BlockedError",
    "Step15ValidationError",
    "create_fixture_personalization_packet",
    "migrate_step15",
    "prepare_fixture_channel_draft_requests",
    "register_fixture_decision_maker",
    "resolve_linkedin_only_fixture_seed",
]
