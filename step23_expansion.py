"""Paldo OS expansion boundaries for manual LinkedIn and social opportunities.

This module is intentionally additive.  It records public evidence and operator
verification separately, prepares LinkedIn copy without sending it, and keeps
Reddit/Facebook opportunity cards separate from the job-monitor database.
External collection is owned by the orchestrator; this module accepts bounded,
secret-free handoffs and commits them idempotently.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

from paldo_os_outbound import Database
from paldo_recovery import migrate_recovery_schema


EXPANSION_MIGRATION_VERSION = 25
LINKEDIN_PERSONAL = "PERSONAL_PROFILE"
LINKEDIN_COMPANY = "COMPANY_PAGE"
ALLOWED_LINKEDIN_HOSTS = frozenset({"linkedin.com", "www.linkedin.com", "in.linkedin.com"})
ALLOWED_OUTCOMES = (
    "reduced no-shows",
    "reduced manual data entry",
    "made client records searchable",
)

#: Lane inference for handoff records: primary-lane countries (ISO codes) and
#: neutral research markers, both supplied through the environment.
PRIMARY_LANE_COUNTRIES = frozenset(
    code.strip().upper()
    for code in (os.environ.get("PALDO_PRIMARY_LANE_COUNTRIES") or "").split(",")
    if code.strip()
)
PRIMARY_LANE_MARKERS = tuple(
    marker.strip().casefold()
    for marker in (os.environ.get("PALDO_PRIMARY_LANE_MARKERS") or "primary region").split(",")
    if marker.strip()
)


class ExpansionValidationError(ValueError):
    pass


_SCHEMA = """
CREATE TABLE IF NOT EXISTS paldo_source_checkpoints (
    source TEXT PRIMARY KEY,
    cursor TEXT,
    last_observed_at TEXT,
    status TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS linkedin_match_records (
    id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id) ON DELETE RESTRICT,
    business_name TEXT NOT NULL,
    person_name TEXT NOT NULL,
    person_role TEXT,
    location TEXT,
    profile_url TEXT NOT NULL,
    url_host TEXT NOT NULL,
    url_kind TEXT NOT NULL CHECK (url_kind IN ('PERSONAL_PROFILE','COMPANY_PAGE')),
    web_verification_status TEXT NOT NULL CHECK (web_verification_status IN ('NOT_CHECKED','POSSIBLE','VERIFIED','REJECTED')),
    web_evidence_json TEXT NOT NULL DEFAULT '{}',
    operator_verification_status TEXT NOT NULL CHECK (operator_verification_status IN ('NOT_REVIEWED','CONFIRMED','REJECTED')),
    operator_evidence TEXT,
    operator_id TEXT,
    operator_verified_at TEXT,
    match_status TEXT NOT NULL CHECK (match_status IN ('SUPPORTED_PERSONAL','POSSIBLE_PERSONAL_MATCH','COMPANY_PAGE_ONLY','MANUAL_SEARCH_NEEDED','REJECTED')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(candidate_id, profile_url)
);
CREATE INDEX IF NOT EXISTS linkedin_match_candidate_idx ON linkedin_match_records(candidate_id, match_status, updated_at);

CREATE TABLE IF NOT EXISTS linkedin_message_drafts (
    id INTEGER PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES linkedin_match_records(id) ON DELETE RESTRICT,
    candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id) ON DELETE RESTRICT,
    campaign_lane TEXT NOT NULL,
    connection_note TEXT NOT NULL,
    acceptance_message TEXT NOT NULL,
    outcome_used TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('DRAFTED','QUEUED','REJECTED')),
    delivery_state TEXT NOT NULL DEFAULT 'PENDING' CHECK (delivery_state IN ('PENDING','SENT','UNKNOWN')),
    notification_message_id TEXT,
    content_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(match_id)
);

CREATE TABLE IF NOT EXISTS linkedin_queue_cards (
    id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id) ON DELETE RESTRICT,
    business_name TEXT NOT NULL,
    person_name TEXT NOT NULL,
    person_role TEXT,
    location TEXT,
    evidence_json TEXT NOT NULL,
    search_terms_json TEXT NOT NULL,
    compare_json TEXT NOT NULL,
    card_text TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('MANUAL_SEARCH_NEEDED','DRAFT_READY','DELIVERED','CLOSED')),
    delivery_state TEXT NOT NULL CHECK (delivery_state IN ('PENDING','SENT','UNKNOWN')),
    notification_message_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(candidate_id)
);

CREATE TABLE IF NOT EXISTS social_opportunities (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    community TEXT NOT NULL,
    original_url TEXT NOT NULL,
    published_at TEXT,
    public_author TEXT,
    item_class TEXT NOT NULL CHECK (item_class IN ('BUSINESS_OPPORTUNITY','HIRING_REQUEST','HELP_SEEKING','CONTEXT_ONLY')),
    need TEXT NOT NULL,
    supporting_excerpt TEXT NOT NULL,
    business_context TEXT NOT NULL,
    confidence TEXT NOT NULL CHECK (confidence IN ('HIGH','MEDIUM','LOW')),
    intent_label TEXT NOT NULL CHECK (intent_label IN ('ADVICE_SEEKING','BUYING_SIGNAL','EXPLICIT_HIRING','CONTEXT_ONLY')),
    helpful_comment TEXT,
    route_topic INTEGER,
    job_source_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('NEW','QUEUED','DELIVERED','RETAINED_CONTEXT','BLOCKED')),
    notification_message_id TEXT,
    freshness_status TEXT NOT NULL DEFAULT 'ELIGIBLE',
    evidence_json TEXT NOT NULL DEFAULT '{}',
    content_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source, original_url)
);
CREATE INDEX IF NOT EXISTS social_opportunities_route_idx ON social_opportunities(item_class, status, created_at);
"""


def _now(value: Optional[datetime] = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ExpansionValidationError("timestamp must include timezone")
    return current.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: Optional[datetime] = None) -> str:
    return _now(value).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _text(value: Any, field: str, maximum: int = 1200) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExpansionValidationError(f"{field} is required")
    value = " ".join(value.split())
    if len(value) > maximum or "<script" in value.casefold() or "api_key" in value.casefold():
        raise ExpansionValidationError(f"{field} is not bounded")
    return value


def _optional_text(value: Any, maximum: int = 1200) -> str | None:
    if value is None or value == "":
        return None
    return _text(value, "optional text", maximum)


def _candidate(database: Database, candidate_id: int) -> dict[str, Any]:
    row = database.connection.execute("SELECT * FROM discovery_candidates WHERE id=?", (candidate_id,)).fetchone()
    if row is None:
        raise ExpansionValidationError("candidate does not exist")
    return dict(row)


def _validate_http_source_url(value: Any) -> str:
    url = _text(value, "source_url", 800)
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise ExpansionValidationError("source_url must be a public HTTP(S) URL")
    return url


def validate_linkedin_url(value: Any) -> dict[str, str]:
    """Validate host and classify only /in/ personal or /company/ pages."""
    url = _text(value, "linkedin_url", 500)
    parsed = urlsplit(url)
    host = (parsed.hostname or "").casefold().rstrip(".")
    if parsed.scheme != "https" or host not in ALLOWED_LINKEDIN_HOSTS:
        raise ExpansionValidationError("LinkedIn URL host or scheme is not allowed")
    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) != 2 or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", segments[1]):
        raise ExpansionValidationError("LinkedIn URL must contain one /in/<slug> or /company/<slug> path")
    kind = LINKEDIN_PERSONAL if segments[0].casefold() == "in" else LINKEDIN_COMPANY if segments[0].casefold() == "company" else None
    if kind is None:
        raise ExpansionValidationError("LinkedIn URL must use /in/ or /company/")
    return {"url": f"https://{host}/{segments[0]}/{segments[1]}", "host": host, "kind": kind}


def migrate_expansion(database_or_path: Database | str | Path) -> int:
    db = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    close_after = not isinstance(database_or_path, Database)
    try:
        migrate_recovery_schema(db)
        with db.connection:
            db.connection.executescript(_SCHEMA)
            social_columns = {row[1] for row in db.connection.execute("PRAGMA table_info(social_opportunities)")}
            if "notification_message_id" not in social_columns:
                db.connection.execute("ALTER TABLE social_opportunities ADD COLUMN notification_message_id TEXT")
            if "freshness_status" not in social_columns:
                db.connection.execute("ALTER TABLE social_opportunities ADD COLUMN freshness_status TEXT NOT NULL DEFAULT 'ELIGIBLE'")
            draft_columns = {row[1] for row in db.connection.execute("PRAGMA table_info(linkedin_message_drafts)")}
            if "delivery_state" not in draft_columns:
                db.connection.execute("ALTER TABLE linkedin_message_drafts ADD COLUMN delivery_state TEXT NOT NULL DEFAULT 'PENDING'")
            if "notification_message_id" not in draft_columns:
                db.connection.execute("ALTER TABLE linkedin_message_drafts ADD COLUMN notification_message_id TEXT")
            db.connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version,name,applied_at) VALUES (?,?,?)",
                (EXPANSION_MIGRATION_VERSION, "step23_manual_linkedin_social_expansion", _iso()),
            )
        return EXPANSION_MIGRATION_VERSION
    finally:
        if close_after:
            db.close()


def record_decision_maker_evidence(
    database: Database,
    *,
    candidate_id: int,
    evidence: Mapping[str, Any],
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Expose the existing queue-boundary owner enrichment commit to expansion callers."""
    from paldo_research_queue import record_decision_maker_evidence as _record

    return _record(database, candidate_id=candidate_id, evidence=evidence, now=now)


def record_source_checkpoint(
    database: Database,
    *,
    source: str,
    cursor: str | None,
    status: str,
    detail: Mapping[str, Any] | None = None,
    now: Optional[datetime] = None,
) -> None:
    migrate_expansion(database)
    current = _iso(now)
    with database.connection:
        database.connection.execute(
            """INSERT INTO paldo_source_checkpoints(source,cursor,last_observed_at,status,detail_json,updated_at)
               VALUES (?,?,?,?,?,?)
               ON CONFLICT(source) DO UPDATE SET cursor=excluded.cursor,last_observed_at=excluded.last_observed_at,
                 status=excluded.status,detail_json=excluded.detail_json,updated_at=excluded.updated_at""",
            (source, _optional_text(cursor, 500), current, _text(status, "checkpoint status", 80), _json(detail or {}), current),
        )


def _research(candidate: Mapping[str, Any]) -> dict[str, Any]:
    raw = candidate.get("research_packet_json")
    if not raw:
        return {}
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return dict(data) if isinstance(data, Mapping) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def _decision_maker(candidate: Mapping[str, Any]) -> dict[str, Any]:
    raw = candidate.get("decision_maker_json")
    if not raw:
        research = _research(candidate)
        raw = research.get("decision_maker_evidence")
    if not raw:
        return {}
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return dict(data) if isinstance(data, Mapping) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def _lane(candidate: Mapping[str, Any]) -> str:
    research = _research(candidate)
    text = _json(research).casefold()
    primary = str(candidate.get("country") or "").upper() in PRIMARY_LANE_COUNTRIES
    primary = primary or any(marker in text for marker in PRIMARY_LANE_MARKERS)
    return "PRIMARY" if primary else "SECONDARY"


def _location(candidate: Mapping[str, Any]) -> str:
    return ", ".join(str(candidate.get(k)).strip() for k in ("city", "region_state", "country") if candidate.get(k)) or "UNKNOWN"


def _evidence_for_card(candidate: Mapping[str, Any]) -> dict[str, Any]:
    research = _research(candidate)
    decision = _decision_maker(candidate)
    sources = research.get("sources") or []
    decision_sources = decision.get("sources") or []
    source_urls = [str(item.get("url")) for item in sources if isinstance(item, Mapping) and item.get("url")]
    source_urls.extend(str(item.get("url")) for item in decision_sources if isinstance(item, Mapping) and item.get("url"))
    if candidate.get("source_url"):
        source_urls.insert(0, str(candidate["source_url"]))
    return {
        "candidate_id": candidate["id"],
        "business": candidate.get("business_name"),
        "decision_maker_evidence": decision,
        "research_observations": (research.get("observations") or [])[:4],
        "source_urls": list(dict.fromkeys(source_urls))[:12],
        "source_classes": decision.get("source_classes") or [],
        "searches_attempted": decision.get("searches_attempted") or [],
        "verification_state": decision.get("verification_state") or "UNKNOWN",
        "confidence": decision.get("confidence") or "UNKNOWN",
        "linkedin_profile_url": decision.get("linkedin_profile_url"),
        "operator_verification": "PENDING_OPERATOR" if decision.get("verification_state") != "VERIFIED" else "NOT_REQUIRED_YET",
    }


def _search_terms(candidate: Mapping[str, Any]) -> list[str]:
    decision = _decision_maker(candidate)
    name = str(decision.get("name") or "").strip()
    role = str(decision.get("role") or decision.get("current_role") or "").strip()
    business = str(candidate.get("business_name") or "").strip()
    location = _location(candidate)
    terms = list(decision.get("searches_attempted") or [])
    if name and "general business contact" not in name.casefold() and "management" not in name.casefold():
        terms.extend([f'"{name}" LinkedIn', f'"{name}" "{business}"', f'"{name}" {location}'])
    terms.extend([
        f'"{business}" LinkedIn',
        f'"{business}" owner founder co-founder medical director business director managing partner practice manager doctor operations lead {location}',
        f'site:linkedin.com/in "{business}"',
        f'{business} official website About Team Providers Contact Facebook Instagram owner founder',
    ])
    if role:
        terms.append(f'"{business}" "{role}" {location}')
    return list(dict.fromkeys(terms))


def _compare_for_card(candidate: Mapping[str, Any]) -> list[str]:
    return [
        "Profile URL host is linkedin.com or an allowed localized LinkedIn host.",
        "Personal profile uses /in/<slug>, not /company/<slug>.",
        "Name, role, location, and business association match the evidence shown here.",
        "Do not treat a business doctor or a same-name person as owner without supporting evidence.",
    ]


def _card_text(candidate: Mapping[str, Any]) -> str:
    decision = _decision_maker(candidate)
    name = str(decision.get("name") or "UNKNOWN").strip()
    role = str(decision.get("role") or decision.get("current_role") or "UNKNOWN").strip()
    state = str(decision.get("verification_state") or "UNKNOWN").strip()
    confidence = str(decision.get("confidence") or "UNKNOWN").strip()
    evidence = _evidence_for_card(candidate)
    urls = ", ".join(evidence["source_urls"][:5]) or "none recorded"
    source_classes = ", ".join(evidence["source_classes"][:8]) or "none recorded"
    searches = "; ".join(evidence["searches_attempted"][:8]) or "not recorded"
    state_line = "NEEDS_OPERATOR_CONFIRMATION" if state == "NEEDS_OPERATOR_CONFIRMATION" else state
    return "\n".join([
        "PALDO OS · LINKEDIN MANUAL SEARCH",
        f"Candidate ID: {candidate['id']}",
        f"Business: {candidate.get('business_name') or 'UNKNOWN'}",
        f"Person: {name}",
        f"Role: {role}",
        f"Confidence: {confidence}",
        f"Verification state: {state_line}",
        f"Location: {_location(candidate)}",
        "A personal LinkedIn profile is not accepted without evidence tying it to this business and person.",
        f"Supporting evidence: {urls}",
        f"Source classes: {source_classes}",
        f"Searches attempted: {searches}",
        "Compare: name, role, location, exact business association, /in/ personal path, and current profile details.",
        "No connection request, follow, reaction, or DM was sent.",
    ])[:3900]


def queue_linkedin_manual_cards(
    database: Database,
    *,
    max_candidates: int = 20,
    now: Optional[datetime] = None,
    refresh_delivered: bool = False,
) -> dict[str, int]:
    migrate_expansion(database)
    current = _iso(now)
    proposals = {
        int(row["candidate_id"])
        for row in database.connection.execute("SELECT candidate_id FROM production_message_proposals ORDER BY id")
    }
    rows = database.connection.execute(
        """SELECT * FROM discovery_candidates
           WHERE research_state='RESEARCHED'
           ORDER BY CASE WHEN id IN ({}) THEN 0 ELSE 1 END, id""".format(",".join("?" for _ in proposals) or "NULL"),
        tuple(proposals),
    ).fetchall()
    created = 0
    skipped = 0
    with database.connection:
        for row in rows[:max_candidates]:
            candidate = dict(row)
            supported = database.connection.execute("SELECT id FROM linkedin_match_records WHERE candidate_id=? AND match_status='SUPPORTED_PERSONAL' LIMIT 1", (candidate["id"],)).fetchone()
            if supported and not refresh_delivered:
                skipped += 1
                continue
            existing = database.connection.execute("SELECT id,status,delivery_state FROM linkedin_queue_cards WHERE candidate_id=?", (candidate["id"],)).fetchone()
            if existing:
                should_refresh = existing["delivery_state"] == "PENDING" and existing["status"] in {"MANUAL_SEARCH_NEEDED", "DRAFT_READY"}
                should_refresh = should_refresh or (refresh_delivered and existing["delivery_state"] in {"SENT", "UNKNOWN"})
                if should_refresh:
                    decision = _decision_maker(candidate)
                    next_status = "MANUAL_SEARCH_NEEDED" if existing["delivery_state"] == "PENDING" else existing["status"]
                    database.connection.execute(
                        """UPDATE linkedin_queue_cards
                           SET business_name=?,person_name=?,person_role=?,location=?,evidence_json=?,search_terms_json=?,compare_json=?,card_text=?,status=?,updated_at=?
                           WHERE candidate_id=?""",
                        (
                            candidate.get("business_name") or "UNKNOWN",
                            str(decision.get("name") or "UNKNOWN"),
                            str(decision.get("role") or decision.get("current_role") or "UNKNOWN"),
                            _location(candidate), _json(_evidence_for_card(candidate)), _json(_search_terms(candidate)), _json(_compare_for_card(candidate)), _card_text(candidate), next_status, current, candidate["id"],
                        ),
                    )
                skipped += 1
                continue
            card = _card_text(candidate)
            decision = _decision_maker(candidate)
            cursor = database.connection.execute(
                """INSERT INTO linkedin_queue_cards
                   (candidate_id,business_name,person_name,person_role,location,evidence_json,search_terms_json,compare_json,card_text,status,delivery_state,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,'MANUAL_SEARCH_NEEDED','PENDING',?,?)""",
                (
                    candidate["id"], candidate.get("business_name") or "UNKNOWN",
                    str(decision.get("name") or "UNKNOWN"),
                    str(decision.get("role") or decision.get("current_role") or "UNKNOWN"),
                    _location(candidate), _json(_evidence_for_card(candidate)), _json(_search_terms(candidate)), _json(_compare_for_card(candidate)), card, current, current,
                ),
            )
            created += int(cursor.rowcount == 1)
    return {"created": created, "skipped_supported": skipped, "considered": min(len(rows), max_candidates)}


def record_linkedin_match(
    database: Database,
    *,
    candidate_id: int,
    person_name: str,
    person_role: str,
    location: str,
    profile_url: str,
    web_verification_status: str,
    web_evidence: Mapping[str, Any],
    operator_confirmed: bool = False,
    operator_evidence: str | None = None,
    operator_id: str | None = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    migrate_expansion(database)
    candidate = _candidate(database, candidate_id)
    parsed = validate_linkedin_url(profile_url)
    web_status = str(web_verification_status).upper()
    if web_status not in {"NOT_CHECKED", "POSSIBLE", "VERIFIED", "REJECTED"}:
        raise ExpansionValidationError("unsupported web_verification_status")
    if not isinstance(web_evidence, Mapping):
        raise ExpansionValidationError("web_evidence must be an object")
    current = _iso(now)
    person = _text(person_name, "person_name", 200)
    role = _text(person_role, "person_role", 160)
    loc = _text(location, "location", 160)
    operator_status = "CONFIRMED" if operator_confirmed else "NOT_REVIEWED"
    if operator_confirmed and not _optional_text(operator_evidence, 800):
        raise ExpansionValidationError("operator_evidence is required when confirmed")
    if parsed["kind"] == LINKEDIN_COMPANY:
        match_status = "COMPANY_PAGE_ONLY"
    elif web_status == "REJECTED":
        match_status = "REJECTED"
    elif web_status == "VERIFIED" or operator_confirmed:
        match_status = "SUPPORTED_PERSONAL"
    else:
        match_status = "POSSIBLE_PERSONAL_MATCH"
    with database.connection:
        database.connection.execute(
            """INSERT INTO linkedin_match_records
               (candidate_id,business_name,person_name,person_role,location,profile_url,url_host,url_kind,web_verification_status,web_evidence_json,operator_verification_status,operator_evidence,operator_id,operator_verified_at,match_status,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(candidate_id,profile_url) DO UPDATE SET
                 person_name=excluded.person_name, person_role=excluded.person_role, location=excluded.location,
                 web_verification_status=excluded.web_verification_status, web_evidence_json=excluded.web_evidence_json,
                 operator_verification_status=excluded.operator_verification_status, operator_evidence=excluded.operator_evidence,
                 operator_id=excluded.operator_id, operator_verified_at=excluded.operator_verified_at,
                 match_status=excluded.match_status, updated_at=excluded.updated_at""",
            (candidate_id, candidate.get("business_name") or "UNKNOWN", person, role, loc, parsed["url"], parsed["host"], parsed["kind"], web_status, _json(dict(web_evidence)), operator_status, _optional_text(operator_evidence, 800), _optional_text(operator_id, 100), current if operator_confirmed else None, match_status, current, current),
        )
        row = database.connection.execute("SELECT * FROM linkedin_match_records WHERE candidate_id=? AND profile_url=?", (candidate_id, parsed["url"])).fetchone()
        database.connection.execute("UPDATE linkedin_queue_cards SET status=?,updated_at=? WHERE candidate_id=?", ("DRAFT_READY" if match_status == "SUPPORTED_PERSONAL" else "CLOSED" if match_status == "COMPANY_PAGE_ONLY" else "MANUAL_SEARCH_NEEDED", current, candidate_id))
    return dict(row)


def _outcome(candidate: Mapping[str, Any]) -> str:
    research = _research(candidate)
    raw = " ".join(str(x) for x in (research.get("proof_outcome_match") or []))
    text = (raw + " " + _json(research)).casefold()
    if "no-show" in text or "no show" in text or "attendance" in text:
        return "reduced no-shows"
    if "manual data" in text or "manual entry" in text:
        return "reduced manual data entry"
    return "made client records searchable"


def _observation(candidate: Mapping[str, Any]) -> str:
    research = _research(candidate)
    terms = research.get("business_terminology") or []
    if terms:
        return str(terms[0])
    route = research.get("booking_route")
    if isinstance(route, Mapping):
        return str(route.get("evidence") or route.get("type") or "booking information")
    if isinstance(route, list) and route:
        return str(route[0])
    return "your public booking information"


def prepare_linkedin_draft(database: Database, *, match_id: int, now: Optional[datetime] = None) -> dict[str, Any]:
    migrate_expansion(database)
    match = database.connection.execute("SELECT * FROM linkedin_match_records WHERE id=?", (match_id,)).fetchone()
    if match is None or match["match_status"] != "SUPPORTED_PERSONAL" or match["url_kind"] != LINKEDIN_PERSONAL:
        raise ExpansionValidationError("only web-verified or operator-confirmed personal matches can receive LinkedIn drafts")
    candidate = _candidate(database, int(match["candidate_id"]))
    lane = _lane(candidate)
    outcome = _outcome(candidate)
    observation = _observation(candidate)
    name_parts = str(match["person_name"]).split()
    first = name_parts[1] if name_parts and name_parts[0].rstrip(".").casefold() in {"dr", "doctor"} and len(name_parts) > 1 else (name_parts[0] if name_parts else "there")
    business = str(candidate.get("business_name") or "the business")
    location = _location(candidate)
    if lane == "PRIMARY":
        connection = f"Hi {first}, I came across {business} while looking at local service businesses in {location}. I work on booking and follow-up workflows for businesses. Open to connecting?"
        acceptance = f"Thanks for connecting. I noticed {observation}. If keeping that part of the client journey visible is ever a priority, I can share how I helped a local service business {outcome}. Would that be useful?"
    else:
        connection = f"Hi {first}, I came across {business} while researching local service businesses. I work on booking and follow-up workflows for businesses. Open to connecting?"
        acceptance = f"Thanks for connecting. I noticed {observation}. If that workflow ever becomes a priority, I can share how, at a local service business I worked with, I {outcome}. Would that be useful?"
    for label, value in (("connection_note", connection), ("acceptance_message", acceptance)):
        if len(value) > 900:
            raise ExpansionValidationError(f"{label} is too long")
    content_hash = sha256((connection + "\n" + acceptance).encode("utf-8")).hexdigest()
    current = _iso(now)
    with database.connection:
        database.connection.execute(
            """INSERT INTO linkedin_message_drafts(match_id,candidate_id,campaign_lane,connection_note,acceptance_message,outcome_used,status,content_hash,created_at,updated_at)
               VALUES (?,?,?,?,?,?, 'DRAFTED',?,?,?)
               ON CONFLICT(match_id) DO UPDATE SET connection_note=excluded.connection_note, acceptance_message=excluded.acceptance_message, outcome_used=excluded.outcome_used, content_hash=excluded.content_hash, updated_at=excluded.updated_at""",
            (match_id, candidate["id"], lane, connection, acceptance, outcome, content_hash, current, current),
        )
        database.connection.execute("UPDATE linkedin_queue_cards SET status='DRAFT_READY',updated_at=? WHERE candidate_id=?", (current, candidate["id"]))
        row = database.connection.execute("SELECT * FROM linkedin_message_drafts WHERE match_id=?", (match_id,)).fetchone()
    return dict(row)


def upsert_social_opportunity(database: Database, record: Mapping[str, Any], *, now: Optional[datetime] = None) -> dict[str, Any]:
    migrate_expansion(database)
    source = _text(record.get("source"), "source", 80).casefold()
    community = _text(record.get("community"), "community", 160)
    url = _validate_http_source_url(record.get("original_url"))
    item_class = str(record.get("item_class") or "").upper()
    intent = str(record.get("intent_label") or "").upper()
    if item_class not in {"BUSINESS_OPPORTUNITY", "HIRING_REQUEST", "HELP_SEEKING", "CONTEXT_ONLY"}:
        raise ExpansionValidationError("unsupported social item_class")
    if intent not in {"ADVICE_SEEKING", "BUYING_SIGNAL", "EXPLICIT_HIRING", "CONTEXT_ONLY"}:
        raise ExpansionValidationError("unsupported social intent_label")
    excerpt = _text(record.get("supporting_excerpt"), "supporting_excerpt", 1000)
    need = _text(record.get("need"), "need", 500)
    business_context = _text(record.get("business_context"), "business_context", 700)
    confidence = str(record.get("confidence") or "").upper()
    if confidence not in {"HIGH", "MEDIUM", "LOW"}:
        raise ExpansionValidationError("unsupported confidence")
    current = _iso(now)
    fingerprint = sha256(_json({"source": source, "url": url, "excerpt": excerpt, "item_class": item_class}).encode("utf-8")).hexdigest()
    published = str(record.get("published_at") or "").strip()
    freshness = str(record.get("freshness_status") or "").upper()
    if freshness not in {"ELIGIBLE", "STALE", "DATE_UNVERIFIED"}:
        freshness = "DATE_UNVERIFIED" if not published or published.upper() == "UNKNOWN" else "ELIGIBLE"
    if freshness == "ELIGIBLE" and published and published.upper() != "UNKNOWN":
        try:
            observed = datetime.fromisoformat(published.replace("Z", "+00:00"))
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            age_days = max(0.0, (_now(now) - _now(observed)).total_seconds() / 86400)
            freshness = "ELIGIBLE" if age_days <= 7 else "STALE"
        except ValueError:
            freshness = "DATE_UNVERIFIED"
    if freshness != "ELIGIBLE":
        status = "RETAINED_CONTEXT" if item_class == "CONTEXT_ONLY" else "BLOCKED"
    else:
        status = "QUEUED" if item_class in {"BUSINESS_OPPORTUNITY", "HIRING_REQUEST", "HELP_SEEKING"} else "RETAINED_CONTEXT"
    topic = 15 if item_class == "HIRING_REQUEST" else 836 if item_class in {"BUSINESS_OPPORTUNITY", "HELP_SEEKING"} else None
    with database.connection:
        database.connection.execute(
            """INSERT INTO social_opportunities
               (source,community,original_url,published_at,public_author,item_class,need,supporting_excerpt,business_context,confidence,intent_label,helpful_comment,route_topic,job_source_id,status,freshness_status,evidence_json,content_hash,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(source,original_url) DO UPDATE SET
                 published_at=excluded.published_at, public_author=excluded.public_author, item_class=excluded.item_class,
                 need=excluded.need, supporting_excerpt=excluded.supporting_excerpt, business_context=excluded.business_context,
                 confidence=excluded.confidence, intent_label=excluded.intent_label, helpful_comment=excluded.helpful_comment,
                 route_topic=excluded.route_topic, job_source_id=excluded.job_source_id, status=excluded.status, freshness_status=excluded.freshness_status, evidence_json=excluded.evidence_json,
                 updated_at=excluded.updated_at""",
            (source, community, url, _optional_text(record.get("published_at"), 80), _optional_text(record.get("public_author"), 160), item_class, need, excerpt, business_context, confidence, intent, _optional_text(record.get("helpful_comment"), 1800), topic, _optional_text(record.get("job_source_id"), 200), status, freshness, _json(record.get("evidence") or {}), fingerprint, current, current),
        )
        row = database.connection.execute("SELECT * FROM social_opportunities WHERE source=? AND original_url=?", (source, url)).fetchone()
    return dict(row)


def render_linkedin_cards(database: Database, *, limit: int = 20) -> list[dict[str, Any]]:
    migrate_expansion(database)
    rows = database.connection.execute("SELECT * FROM linkedin_queue_cards WHERE delivery_state='PENDING' ORDER BY id LIMIT ?", (limit,)).fetchall()
    return [dict(row) for row in rows]


def render_social_cards(database: Database, *, limit: int = 50) -> list[dict[str, Any]]:
    migrate_expansion(database)
    rows = database.connection.execute("SELECT * FROM social_opportunities WHERE status='QUEUED' AND freshness_status='ELIGIBLE' ORDER BY id LIMIT ?", (limit,)).fetchall()
    return [dict(row) for row in rows]


def mark_linkedin_delivery(database: Database, candidate_ids: list[int], message_id: str, *, now: Optional[datetime] = None) -> int:
    current = _iso(now)
    changed = 0
    with database.connection:
        for candidate_id in dict.fromkeys(candidate_ids):
            cur = database.connection.execute("UPDATE linkedin_queue_cards SET delivery_state='SENT',status='DELIVERED',notification_message_id=?,updated_at=? WHERE candidate_id=? AND delivery_state='PENDING'", (message_id, current, candidate_id))
            changed += cur.rowcount
    return changed


def mark_linkedin_draft_delivery(database: Database, draft_ids: list[int], message_id: str, *, now: Optional[datetime] = None) -> int:
    migrate_expansion(database)
    current = _iso(now)
    changed = 0
    with database.connection:
        for draft_id in dict.fromkeys(draft_ids):
            cur = database.connection.execute(
                "UPDATE linkedin_message_drafts SET status='QUEUED',delivery_state='SENT',notification_message_id=?,updated_at=? WHERE id=? AND delivery_state='PENDING'",
                (message_id, current, draft_id),
            )
            changed += cur.rowcount
    return changed


def mark_social_delivery(database: Database, ids: list[int], message_id: str, *, now: Optional[datetime] = None) -> int:
    current = _iso(now)
    changed = 0
    with database.connection:
        for row_id in dict.fromkeys(ids):
            cur = database.connection.execute("UPDATE social_opportunities SET status='DELIVERED',notification_message_id=?,updated_at=? WHERE id=? AND status='QUEUED'", (message_id, current, row_id))
            changed += cur.rowcount
    return changed
