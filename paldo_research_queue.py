"""Crash-safe daily research accounting for the Paldo weekday orchestrator.

This is an additive queue boundary, not a replacement research framework.  The
Hermes cron agent performs the external research; this module selects saved
candidates and commits one completed research packet at a time.
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from paldo_os_outbound import Database
from step23_expansion import migrate_expansion, validate_linkedin_url


LOCAL_TZ = ZoneInfo(os.environ.get("PALDO_LOCAL_TIMEZONE") or "UTC")
RESEARCH_QUEUE_MIGRATION_VERSION = 26
DEFAULT_DAILY_CAP = 20

_SCHEMA = """
CREATE TABLE IF NOT EXISTS candidate_research_progress (
    candidate_id INTEGER PRIMARY KEY REFERENCES discovery_candidates(id) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status IN ('IN_PROGRESS','COMPLETED')),
    reserved_for_local_date TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    completed_at TEXT,
    packet_hash TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS candidate_research_progress_status_idx
    ON candidate_research_progress(status, reserved_for_local_date, candidate_id);
"""


def _utc(value: Optional[datetime] = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return current.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: Optional[datetime] = None) -> str:
    return _utc(value).isoformat()


def _local_date(value: Optional[datetime] = None) -> str:
    return _utc(value).astimezone(LOCAL_TZ).date().isoformat()


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def migrate_research_queue(database_or_path: Database | str | Path) -> int:
    """Apply only the additive research-progress schema migration."""
    db = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    close_after = not isinstance(database_or_path, Database)
    try:
        migrate_expansion(db)
        with db.connection:
            db.connection.executescript(_SCHEMA)
            db.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations(version,name,applied_at)
                   VALUES (?,?,?)""",
                (
                    RESEARCH_QUEUE_MIGRATION_VERSION,
                    "paldo_daily_research_queue_accounting",
                    _iso(),
                ),
            )
        return RESEARCH_QUEUE_MIGRATION_VERSION
    finally:
        if close_after:
            db.close()


def _waiting_rows(database: Database) -> list[dict[str, Any]]:
    rows = database.connection.execute(
        """SELECT c.*, p.status AS progress_status
           FROM discovery_candidates AS c
           LEFT JOIN candidate_research_progress AS p ON p.candidate_id = c.id
           WHERE c.research_state='NOT_PROCESSED'
             AND c.ingestion_status='ACCEPTED'
             AND (p.status IS NULL OR p.status='IN_PROGRESS')
           ORDER BY CASE WHEN p.status='IN_PROGRESS' THEN 0 ELSE 1 END,
                    c.created_at ASC, c.id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _completed_today(database: Database, local_date: str) -> int:
    count = 0
    rows = database.connection.execute(
        """SELECT completed_at FROM candidate_research_progress
           WHERE status='COMPLETED' AND completed_at IS NOT NULL"""
    ).fetchall()
    for row in rows:
        timestamp = _parse_timestamp(row["completed_at"])
        if timestamp is not None and timestamp.astimezone(LOCAL_TZ).date().isoformat() == local_date:
            count += 1
    return count


def research_queue_plan(
    database: Database,
    *,
    now: Optional[datetime] = None,
    daily_cap: int = DEFAULT_DAILY_CAP,
) -> dict[str, Any]:
    """Return the current local-day quota and whether discovery must stay skipped."""
    if daily_cap < 0:
        raise ValueError("daily_cap must be non-negative")
    migrate_research_queue(database)
    local_date = _local_date(now)
    waiting = _waiting_rows(database)
    completed_today = _completed_today(database, local_date)
    remaining = max(0, daily_cap - completed_today)
    return {
        "local_date": local_date,
        "daily_cap": daily_cap,
        "completed_today": completed_today,
        "remaining_today": remaining,
        "waiting_count": len(waiting),
        "discovery_skipped": bool(waiting),
        "eligible_waiting_ids": [int(row["id"]) for row in waiting],
    }


def reserve_research_batch(
    database: Database,
    *,
    now: Optional[datetime] = None,
    daily_cap: int = DEFAULT_DAILY_CAP,
) -> list[dict[str, Any]]:
    """Reserve the oldest resumable/waiting candidates within today's local-day cap."""
    plan = research_queue_plan(database, now=now, daily_cap=daily_cap)
    if plan["remaining_today"] <= 0:
        return []
    rows = _waiting_rows(database)[: plan["remaining_today"]]
    timestamp = _iso(now)
    local_date = plan["local_date"]
    with database.connection:
        for row in rows:
            database.connection.execute(
                """INSERT INTO candidate_research_progress
                   (candidate_id,status,reserved_for_local_date,reserved_at,updated_at)
                   VALUES (?,'IN_PROGRESS',?,?,?)
                   ON CONFLICT(candidate_id) DO UPDATE SET
                     status=CASE WHEN candidate_research_progress.status='COMPLETED'
                                 THEN candidate_research_progress.status ELSE 'IN_PROGRESS' END,
                     updated_at=excluded.updated_at""",
                (int(row["id"]), local_date, timestamp, timestamp),
            )
    return [
        {
            "id": int(row["id"]),
            "business_name": row["business_name"],
            "created_at": row["created_at"],
            "research_state": row["research_state"],
            "reserved_for_local_date": local_date,
        }
        for row in rows
    ]


def commit_candidate_research(
    database: Database,
    *,
    candidate_id: int,
    research_packet: Mapping[str, Any],
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Atomically commit one candidate's research and its completion ledger."""
    if not isinstance(research_packet, Mapping):
        raise ValueError("research_packet must be a mapping")
    migrate_research_queue(database)
    candidate = database.connection.execute(
        "SELECT id,research_state FROM discovery_candidates WHERE id=?", (candidate_id,)
    ).fetchone()
    if candidate is None:
        raise ValueError("candidate does not exist")
    progress = database.connection.execute(
        "SELECT status FROM candidate_research_progress WHERE candidate_id=?", (candidate_id,)
    ).fetchone()
    if candidate["research_state"] == "RESEARCHED" or (progress and progress["status"] == "COMPLETED"):
        return {"status": "ALREADY_COMPLETED", "candidate_id": candidate_id}
    if candidate["research_state"] != "NOT_PROCESSED":
        raise ValueError("candidate is not in the eligible waiting state")

    completed_at = _iso(now)
    packet_json = json.dumps(dict(research_packet), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    packet_hash = sha256(packet_json.encode("utf-8")).hexdigest()
    with database.connection:
        database.connection.execute(
            """INSERT INTO candidate_research_progress
               (candidate_id,status,reserved_for_local_date,reserved_at,completed_at,packet_hash,updated_at)
               VALUES (?,'COMPLETED',?,?,?,?,?)
               ON CONFLICT(candidate_id) DO UPDATE SET
                 status='COMPLETED', completed_at=excluded.completed_at,
                 packet_hash=excluded.packet_hash, updated_at=excluded.updated_at""",
            (candidate_id, _local_date(now), completed_at, completed_at, packet_hash, completed_at),
        )
        updated = database.connection.execute(
            """UPDATE discovery_candidates
               SET research_state='RESEARCHED', research_packet_json=?, updated_at=?
               WHERE id=? AND research_state='NOT_PROCESSED'""",
            (packet_json, completed_at, candidate_id),
        ).rowcount
        if updated != 1:
            raise RuntimeError("candidate research state changed during commit")
    return {
        "status": "COMPLETED",
        "candidate_id": candidate_id,
        "local_date": _local_date(now),
        "completed_at": completed_at,
        "packet_hash": packet_hash,
    }


def record_decision_maker_evidence(
    database: Database,
    *,
    candidate_id: int,
    evidence: Mapping[str, Any],
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Persist an idempotent owner/decision-maker enrichment packet.

    This is deliberately separate from the business-research quota: it can update
    an already RESEARCHED candidate without re-researching or changing its
    research_state. Evidence is a bounded, secret-free handoff from the existing
    orchestrator; this function performs validation and the canonical commit only.
    """
    if not isinstance(evidence, Mapping):
        raise ValueError("decision-maker evidence must be an object")
    migrate_research_queue(database)
    candidate = database.connection.execute(
        "SELECT id,research_state,decision_maker_json FROM discovery_candidates WHERE id=?",
        (candidate_id,),
    ).fetchone()
    if candidate is None:
        raise ValueError("candidate does not exist")
    if candidate["research_state"] != "RESEARCHED":
        raise ValueError("decision-maker enrichment requires a RESEARCHED candidate")

    allowed_states = {"VERIFIED", "NEEDS_OPERATOR_CONFIRMATION", "CONFLICTING", "UNKNOWN"}
    state = str(evidence.get("verification_state") or "").strip().upper()
    if state not in allowed_states:
        raise ValueError("unsupported verification_state")
    confidence = str(evidence.get("confidence") or "").strip().upper()
    if confidence not in {"HIGH", "MEDIUM", "LOW"}:
        raise ValueError("unsupported confidence")

    def bounded_list(value: Any, field: str, maximum: int, item_max: int) -> list[str]:
        if not isinstance(value, list) or len(value) > maximum:
            raise ValueError(f"{field} must be a bounded list")
        result=[]
        for item in value:
            if not isinstance(item, str) or not item.strip() or len(item.strip()) > item_max:
                raise ValueError(f"{field} contains invalid text")
            result.append(" ".join(item.split()))
        return list(dict.fromkeys(result))

    name = evidence.get("name")
    role = evidence.get("role")
    if name is not None and (not isinstance(name, str) or len(name.strip()) > 200):
        raise ValueError("name is invalid")
    if role is not None and (not isinstance(role, str) or len(role.strip()) > 160):
        raise ValueError("role is invalid")
    name = " ".join(name.split()) if isinstance(name, str) and name.strip() else None
    role = " ".join(role.split()) if isinstance(role, str) and role.strip() else None
    if state in {"VERIFIED", "NEEDS_OPERATOR_CONFIRMATION", "CONFLICTING"} and not name:
        raise ValueError("name is required for a non-UNKNOWN state")

    raw_sources = evidence.get("sources")
    if not isinstance(raw_sources, list) or len(raw_sources) > 20:
        raise ValueError("sources must be a bounded list")
    sources=[]
    for source in raw_sources:
        if not isinstance(source, Mapping):
            raise ValueError("each source must be an object")
        url = source.get("url")
        if not isinstance(url, str) or len(url.strip()) > 800:
            raise ValueError("source url is invalid")
        parsed = urlsplit(url.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("source url must be a public HTTP(S) URL")
        source_type = source.get("source_type")
        text = source.get("evidence_text")
        if not isinstance(source_type, str) or not source_type.strip() or len(source_type.strip()) > 100:
            raise ValueError("source_type is invalid")
        if not isinstance(text, str) or not text.strip() or len(text.strip()) > 1200:
            raise ValueError("evidence_text is invalid")
        sources.append({
            "url": url.strip(),
            "source_type": " ".join(source_type.split()),
            "evidence_text": " ".join(text.split()),
            "retrieved_at": str(source.get("retrieved_at") or ""),
        })
    source_key=lambda item: (item["url"], item["source_type"], item["evidence_text"])
    sources=sorted({source_key(item): item for item in sources}.values(), key=source_key)
    source_classes=bounded_list(evidence.get("source_classes") or [], "source_classes", 20, 100)
    searches=bounded_list(evidence.get("searches_attempted") or [], "searches_attempted", 40, 300)
    if not sources and not searches:
        raise ValueError("sources or searches_attempted is required")

    linkedin_profile_url = evidence.get("linkedin_profile_url")
    if linkedin_profile_url:
        parsed_linkedin = validate_linkedin_url(linkedin_profile_url)
        if parsed_linkedin["kind"] != "PERSONAL_PROFILE":
            raise ValueError("linkedin_profile_url must be a personal /in/ profile")
        linkedin_profile_url = parsed_linkedin["url"]
    else:
        linkedin_profile_url = None

    current = {}
    if candidate["decision_maker_json"]:
        try:
            parsed_current = json.loads(candidate["decision_maker_json"])
            if isinstance(parsed_current, Mapping):
                current = dict(parsed_current)
        except (TypeError, json.JSONDecodeError):
            current = {}
    old_sources = [] if evidence.get("_replace_sources_attempted") else (current.get("sources") if isinstance(current.get("sources"), list) else [])
    old_classes = [] if evidence.get("_replace_sources_attempted") else (list(current.get("source_classes") or []) if isinstance(current.get("source_classes"), list) else [])
    old_searches = [] if evidence.get("_replace_searches_attempted") else (list(current.get("searches_attempted") or []) if isinstance(current.get("searches_attempted"), list) else [])
    merged_sources = []
    for item in [*old_sources, *sources]:
        if isinstance(item, Mapping) and item.get("url") and item.get("evidence_text"):
            merged_sources.append({
                "url": str(item["url"]),
                "source_type": str(item.get("source_type") or "UNKNOWN"),
                "evidence_text": str(item["evidence_text"]),
                "retrieved_at": str(item.get("retrieved_at") or ""),
            })
    merged_sources = sorted({source_key(item): item for item in merged_sources}.values(), key=source_key)
    base = {
        "verification_state": state,
        "name": None if evidence.get("_clear_person_fields") else (name or current.get("name")),
        "role": None if evidence.get("_clear_person_fields") else (role or current.get("role")),
        "confidence": confidence,
        "sources": merged_sources,
        "source_classes": list(dict.fromkeys([*old_classes, *source_classes])),
        "searches_attempted": list(dict.fromkeys([*old_searches, *searches])),
        "linkedin_profile_url": linkedin_profile_url or current.get("linkedin_profile_url"),
    }
    verified_match = database.connection.execute(
        "SELECT profile_url FROM linkedin_match_records WHERE candidate_id=? AND match_status='SUPPORTED_PERSONAL' AND url_kind='PERSONAL_PROFILE' ORDER BY id DESC LIMIT 1",
        (candidate_id,),
    ).fetchone()
    if verified_match and verified_match["profile_url"]:
        base["linkedin_profile_url"] = verified_match["profile_url"]
        if "VERIFIED_LINKEDIN" not in base["source_classes"]:
            base["source_classes"].append("VERIFIED_LINKEDIN")
    canonical = json.dumps(base, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    previous = json.dumps({k: current.get(k) for k in base}, ensure_ascii=False, sort_keys=True, separators=(",", ":")) if current else ""
    if canonical == previous:
        return {"status": "REUSED", "candidate_id": candidate_id, "verification_state": state}
    fingerprint = sha256(canonical.encode("utf-8")).hexdigest()
    stored = dict(base)
    stored["evidence_fingerprint"] = fingerprint
    stored["last_checked_at"] = _iso(now)
    with database.connection:
        database.connection.execute(
            "UPDATE discovery_candidates SET decision_maker_json=?, updated_at=? WHERE id=? AND research_state='RESEARCHED'",
            (json.dumps(stored, ensure_ascii=False, sort_keys=True, separators=(",", ":")), _iso(now), candidate_id),
        )
        database.connection.execute(
            "INSERT INTO events(event_type,entity_type,entity_id,metadata,created_at) VALUES (?,?,?,?,?)",
            (
                "DECISION_MAKER_ENRICHMENT_COMMITTED",
                "discovery_candidate",
                str(candidate_id),
                json.dumps({"verification_state": state, "confidence": confidence, "evidence_fingerprint": fingerprint}, sort_keys=True),
                _iso(now),
            ),
        )
    return {"status": "UPDATED", "candidate_id": candidate_id, "verification_state": state, "evidence_fingerprint": fingerprint}


def _main() -> int:
    parser = argparse.ArgumentParser(description="Paldo daily research queue boundary")
    parser.add_argument("command", choices=("plan", "reserve", "commit", "decision-maker"))
    parser.add_argument("--database", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--daily-cap", type=int, default=DEFAULT_DAILY_CAP)
    parser.add_argument("--candidate-id", type=int)
    parser.add_argument("--input", type=Path)
    args = parser.parse_args()
    db = Database(args.database)
    try:
        if args.command == "plan":
            result: Any = research_queue_plan(db, daily_cap=args.daily_cap)
        elif args.command == "reserve":
            result = reserve_research_batch(db, daily_cap=args.daily_cap)
        elif args.command == "commit":
            if args.candidate_id is None or args.input is None:
                parser.error("commit requires --candidate-id and --input")
            result = commit_candidate_research(
                db,
                candidate_id=args.candidate_id,
                research_packet=json.loads(args.input.read_text(encoding="utf-8")),
            )
        else:
            if args.candidate_id is None or args.input is None:
                parser.error("decision-maker requires --candidate-id and --input")
            result = record_decision_maker_evidence(
                db,
                candidate_id=args.candidate_id,
                evidence=json.loads(args.input.read_text(encoding="utf-8")),
            )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(_main())
