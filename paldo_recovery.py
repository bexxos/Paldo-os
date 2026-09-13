#!/usr/bin/env python3
"""Recover one saved Paldo production handoff without rediscovery.

This module is deliberately split from the external-provider clients.  It
commits saved discovery, research/qualification, and approved message
proposals to canonical SQLite before any Gmail or Notification operation.  The
external operations table is populated as PENDING work with independent
idempotency keys; another orchestrator may advance those rows after upstream
commit.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo
import unicodedata

from notifier import (
    NOTIFICATION_CHANNEL_ID,
    NOTIFICATION_CHANNELS,
    Notifier,
    NotifierExternalStateUnknown,
)
from paldo_os_outbound import DEFAULT_DB_PATH, Database
from step22_integration import migrate_step22


RECOVERY_SCHEMA_VERSION = 19
RECOVERY_RUN_ID = os.environ.get("PALDO_RECOVERY_RUN_ID") or "recovered-run-0000-00-00"
EXPECTED_ACTOR_ID = "compass~crawler-google-places"
EXPECTED_RUNS = {
    "PRIMARY": {
        "remote_run_id": os.environ.get("PALDO_PRIMARY_REMOTE_RUN_ID") or "primary-remote-run-id",
        "dataset_id": os.environ.get("PALDO_PRIMARY_DATASET_ID") or "primary-dataset-id",
        "item_count": 25,
        "cost_usd": 0.1252,
    },
    "SECONDARY": {
        "remote_run_id": os.environ.get("PALDO_SECONDARY_REMOTE_RUN_ID") or "secondary-remote-run-id",
        "dataset_id": os.environ.get("PALDO_SECONDARY_DATASET_ID") or "secondary-dataset-id",
        "item_count": 25,
        "cost_usd": 0.1252,
    },
}
RESEARCHED_COUNT = 20
QUALIFIED_COUNT = 8
HELD_COUNT = 12
QUEUED_COUNT = 30
PROPOSAL_COUNT = 7


class RecoveryBlockedError(RuntimeError):
    """Raised when saved inputs or canonical state fail closed validation."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _pht_now() -> str:
    return datetime.now(ZoneInfo("UTC")).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise RecoveryBlockedError(f"saved artifact missing: {path}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise RecoveryBlockedError(f"saved artifact unreadable: {path}") from error


def _reject_secret_keys(value: Any, *, path: str = "artifact") -> None:
    if isinstance(value, Mapping):
        forbidden = {
            "token", "api_key", "apikey", "authorization", "cookie", "secret",
            "password", "access_token", "refresh_token",
        }
        for key, nested in value.items():
            key_text = str(key).casefold()
            if key_text in forbidden or any(part in key_text for part in ("token", "api_key", "authorization", "secret")):
                raise RecoveryBlockedError(f"saved artifact contains prohibited secret field: {path}.{key}")
            _reject_secret_keys(nested, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _reject_secret_keys(nested, path=f"{path}[{index}]")


def _text(value: Any) -> str:
    raw = unicodedata.normalize("NFKD", str(value or "")).casefold()
    raw = "".join(char for char in raw if not unicodedata.combining(char))
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    raw = re.sub(r"\baesthetics\b", "local service", raw)
    raw = re.sub(r"\bclinics\b", "business", raw)
    return " ".join(raw.split())


def _candidate_name(candidate: Mapping[str, Any]) -> str:
    return str(candidate.get("business_name") or candidate.get("name") or "").strip()


def _research_name(research: Mapping[str, Any], fallback: str = "") -> str:
    identity = research.get("business_identity")
    if isinstance(identity, Mapping):
        return str(identity.get("name") or fallback).strip()
    return fallback.strip()


def _identity_key(lane: str, candidate: Mapping[str, Any], *, fallback_name: str = "") -> str:
    lane = str(lane).upper().strip()
    source_id = str(candidate.get("source_record_id") or "").strip()
    if source_id:
        return f"{lane}:source:{source_id}"
    parts = [
        lane,
        _candidate_name(candidate) or fallback_name,
        candidate.get("street_address"),
        candidate.get("city"),
        candidate.get("region_state"),
        candidate.get("website_url"),
        candidate.get("normalized_domain"),
        candidate.get("normalized_phone"),
    ]
    basis = "|".join(_text(item) for item in parts)
    if not basis.strip("|"):
        raise RecoveryBlockedError("candidate has no stable identity")
    return f"{lane}:derived:{hashlib.sha256(basis.encode('utf-8')).hexdigest()}"


def _source_id_from_idempotency(value: Any) -> str | None:
    text = str(value or "")
    parts = text.split(":")
    if len(parts) >= 6 and parts[0] == "paldo-os":
        return parts[2] or None
    return None


def _add_column(connection: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
    if column not in columns:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def migrate_recovery_schema(database: Database | str | Path) -> int:
    """Apply the additive recovery schema exactly once."""
    db = database if isinstance(database, Database) else Database(database)
    close_after = not isinstance(database, Database)
    try:
        migrate_step22(db)
        with db.connection:
            for column, definition in (
                ("recovery_key", "TEXT"),
                ("production_run_id", "TEXT"),
                ("discovery_state", "TEXT"),
                ("research_state", "TEXT"),
                ("qualification_state", "TEXT"),
                ("queue_state", "TEXT"),
                ("proposal_state", "TEXT"),
                ("hold_reason", "TEXT"),
                ("proposal_block_reason", "TEXT"),
                ("research_packet_json", "TEXT"),
                ("decision_maker_json", "TEXT"),
            ):
                _add_column(db.connection, "discovery_candidates", column, definition)
            db.connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS discovery_candidates_recovery_key_unique "
                "ON discovery_candidates(recovery_key) WHERE recovery_key IS NOT NULL"
            )
            db.connection.execute(
                "CREATE INDEX IF NOT EXISTS discovery_candidates_production_state_idx "
                "ON discovery_candidates(production_run_id, discovery_state, research_state, queue_state)"
            )
            db.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS production_recovery_runs (
                    run_id TEXT PRIMARY KEY,
                    discovery_run_id INTEGER REFERENCES discovery_runs(id) ON DELETE RESTRICT,
                    actor_runs_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    upstream_state TEXT NOT NULL,
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS production_recovery_stages (
                    production_run_id TEXT NOT NULL REFERENCES production_recovery_runs(run_id) ON DELETE CASCADE,
                    stage_name TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (production_run_id, stage_name)
                );
                CREATE TABLE IF NOT EXISTS production_message_proposals (
                    id INTEGER PRIMARY KEY,
                    production_run_id TEXT NOT NULL REFERENCES production_recovery_runs(run_id) ON DELETE RESTRICT,
                    candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id) ON DELETE RESTRICT,
                    source_record_id TEXT,
                    business_name TEXT NOT NULL,
                    lane TEXT NOT NULL CHECK (lane IN ('PRIMARY', 'SECONDARY')),
                    channel TEXT NOT NULL CHECK (channel IN ('EMAIL', 'LINKEDIN')),
                    message_stage TEXT NOT NULL,
                    touch_number INTEGER NOT NULL CHECK (touch_number BETWEEN 1 AND 3),
                    state TEXT NOT NULL,
                    review_status TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    subject TEXT,
                    body TEXT NOT NULL,
                    policy_context_json TEXT NOT NULL,
                    proposal_json TEXT NOT NULL,
                    gmail_state TEXT NOT NULL DEFAULT 'NOT_STARTED',
                    notification_state TEXT NOT NULL DEFAULT 'NOT_STARTED',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS production_message_proposals_state_idx
                    ON production_message_proposals(production_run_id, state, gmail_state, notification_state);
                CREATE TABLE IF NOT EXISTS production_external_operations (
                    id INTEGER PRIMARY KEY,
                    production_run_id TEXT NOT NULL REFERENCES production_recovery_runs(run_id) ON DELETE RESTRICT,
                    proposal_id INTEGER REFERENCES production_message_proposals(id) ON DELETE RESTRICT,
                    operation_name TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    provider TEXT NOT NULL,
                    account_scope TEXT,
                    state TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    external_id TEXT,
                    safe_result_json TEXT NOT NULL DEFAULT '{}',
                    error_category TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS production_external_operations_state_idx
                    ON production_external_operations(production_run_id, operation_name, state);
                """
            )
            db.connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version,name,applied_at) VALUES (?,?,?)",
                (RECOVERY_SCHEMA_VERSION, "step23_staged_production_recovery_boundary", _utc_now()),
            )
        return RECOVERY_SCHEMA_VERSION
    finally:
        if close_after:
            db.close()


@contextmanager
def _production_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            handle.close()
            raise RecoveryBlockedError("PRODUCTION_RUN_ALREADY_ACTIVE") from error
        yield handle
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _load_inputs(
    northfield_path: Path,
    us_path: Path,
    primary_path: Path,
    secondary_path: Path,
    research_path: Path,
    writer_path: Path,
    valid_path: Path,
) -> dict[str, Any]:
    payloads = {
        "PRIMARY": _read_json(primary_path),
        "SECONDARY": _read_json(secondary_path),
        "research": _read_json(research_path),
        "writer": _read_json(writer_path),
        "valid": _read_json(valid_path),
    }
    _reject_secret_keys(payloads)
    actor_runs = []
    candidates: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    by_source_id: dict[str, str] = {}
    by_lane_name: dict[tuple[str, str], str] = {}
    for lane in ("PRIMARY", "SECONDARY"):
        payload = payloads[lane]
        if not isinstance(payload, Mapping) or payload.get("source") != "APIFY":
            raise RecoveryBlockedError(f"{lane} saved dataset is not an APIFY handoff")
        actor = payload.get("actor_run")
        expected = EXPECTED_RUNS[lane]
        if not isinstance(actor, Mapping):
            raise RecoveryBlockedError(f"{lane} Actor metadata missing")
        for field in ("actor_id", "state", "remote_run_id", "dataset_id", "item_count", "cost_usd"):
            if field not in actor:
                raise RecoveryBlockedError(f"{lane} Actor metadata missing {field}")
        if (
            actor["actor_id"] != EXPECTED_ACTOR_ID
            or actor["state"] != "SUCCEEDED"
            or actor["remote_run_id"] != expected["remote_run_id"]
            or actor["dataset_id"] != expected["dataset_id"]
            or actor["item_count"] != expected["item_count"]
            or float(actor["cost_usd"]) != expected["cost_usd"]
        ):
            raise RecoveryBlockedError(f"{lane} saved Actor metadata does not match the verified run")
        actor_runs.append(dict(actor))
        lane_candidates = payload.get("candidates")
        if not isinstance(lane_candidates, list) or len(lane_candidates) != 25:
            raise RecoveryBlockedError(f"{lane} saved dataset must contain exactly 25 candidates")
        for raw in lane_candidates:
            if not isinstance(raw, Mapping):
                raise RecoveryBlockedError(f"{lane} candidate is not an object")
            candidate = dict(raw)
            candidate["lane"] = lane
            key = _identity_key(lane, candidate)
            if key in by_key:
                raise RecoveryBlockedError(f"duplicate saved candidate identity: {key}")
            by_key[key] = candidate
            candidates.append(candidate)
            source_id = str(candidate.get("source_record_id") or "").strip()
            if source_id:
                by_source_id[source_id] = key
            name = _text(_candidate_name(candidate))
            if name:
                by_lane_name[(lane, name)] = key
    if len(candidates) != 50 or len(by_key) != 50:
        raise RecoveryBlockedError("saved discovery handoff is not exactly 50 unique candidates")

    combined = payloads["research"]
    if not isinstance(combined, list) or len(combined) != RESEARCHED_COUNT:
        raise RecoveryBlockedError("saved research handoff is not exactly 20 records")
    research_by_key: dict[str, dict[str, Any]] = {}
    for item in combined:
        if not isinstance(item, Mapping) or not isinstance(item.get("research"), Mapping):
            raise RecoveryBlockedError("saved research record is incomplete")
        lane = str(item.get("lane") or "").upper()
        candidate_payload = item.get("candidate")
        candidate = dict(candidate_payload) if isinstance(candidate_payload, Mapping) else {
            "business_name": str(item.get("research_name") or "").strip()
        }
        candidate["lane"] = lane
        source_id = str(candidate.get("source_record_id") or "").strip()
        key = by_source_id.get(source_id) if source_id else None
        if key is None:
            key = by_lane_name.get((lane, _text(_candidate_name(candidate) or item.get("research_name"))))
        if key is None:
            key = _identity_key(lane, candidate, fallback_name=str(item.get("research_name") or ""))
        if key not in by_key or key in research_by_key:
            raise RecoveryBlockedError("saved research record cannot be mapped uniquely to discovery")
        research_by_key[key] = {
            "lane": lane,
            "candidate": by_key[key],
            "research": dict(item["research"]),
            "research_name": item.get("research_name"),
        }
    if len(research_by_key) != RESEARCHED_COUNT:
        raise RecoveryBlockedError("saved research mapping is not exactly 20 records")

    writer_payload = payloads["writer"]
    writer_results = writer_payload.get("results") if isinstance(writer_payload, Mapping) else None
    if not isinstance(writer_results, list) or len(writer_results) != RESEARCHED_COUNT:
        raise RecoveryBlockedError("saved Writer handoff is not exactly 20 records")
    writer_by_key: dict[str, dict[str, Any]] = {}
    ordered_research_keys = list(research_by_key)
    for writer_index, result in enumerate(writer_results):
        if not isinstance(result, Mapping):
            raise RecoveryBlockedError("saved Writer record is not an object")
        lane = str(result.get("lane") or "").upper()
        source_id = str(result.get("source_record_id") or "").strip()
        key = by_source_id.get(source_id) if source_id else None
        if key is None:
            key = by_lane_name.get((lane, _text(result.get("business_name"))))
        if key is None and not str(result.get("business_name") or "").strip() and writer_index < len(ordered_research_keys):
            positional_key = ordered_research_keys[writer_index]
            if research_by_key[positional_key]["lane"] == lane:
                key = positional_key
        if key is None or key not in research_by_key or key in writer_by_key:
            raise RecoveryBlockedError("saved Writer record cannot be mapped uniquely")
        writer_by_key[key] = dict(result)
    if len(writer_by_key) != RESEARCHED_COUNT:
        raise RecoveryBlockedError("saved Writer mapping is not exactly 20 records")

    valid = payloads["valid"]
    if not isinstance(valid, list) or len(valid) != PROPOSAL_COUNT:
        raise RecoveryBlockedError("saved validated message handoff is not exactly 7 records")
    proposals: list[dict[str, Any]] = []
    proposal_keys: set[str] = set()
    for item in valid:
        if not isinstance(item, Mapping) or not isinstance(item.get("message"), Mapping):
            raise RecoveryBlockedError("validated message record is incomplete")
        message = dict(item["message"])
        lane = str(item.get("lane") or "").upper()
        source_id = str(message.get("source_record_id") or item.get("source_record_id") or "").strip()
        if not source_id:
            source_id = _source_id_from_idempotency(message.get("idempotency_key")) or ""
        key = by_source_id.get(source_id) if source_id else None
        if key is None:
            key = by_lane_name.get((lane, _text(item.get("business_name"))))
        if key is None or key not in research_by_key or key in proposal_keys:
            raise RecoveryBlockedError("validated message cannot be mapped uniquely")
        if message.get("channel") != "EMAIL":
            raise RecoveryBlockedError("the saved approved proposal set contains a non-email message")
        if (message.get("message_stage") or message.get("stage")) != "INITIAL" or message.get("touch_number") != 1:
            raise RecoveryBlockedError("saved proposal is not an initial touch")
        context = message.get("policy_context")
        if not isinstance(context, Mapping) or context.get("message_policy_version") != "PALDO_OS_V1_0":
            raise RecoveryBlockedError("saved proposal lacks PALDO_OS_V1_0 context")
        proposal_keys.add(key)
        proposals.append({
            "key": key,
            "lane": lane,
            "source_record_id": source_id or None,
            "business_name": str(item.get("business_name") or _candidate_name(by_key[key])),
            "message": message,
        })
    if len(proposals) != PROPOSAL_COUNT or len(proposal_keys) != PROPOSAL_COUNT:
        raise RecoveryBlockedError("saved proposal mapping is not exactly 7 records")

    qualified_keys = {
        key
        for key, record in research_by_key.items()
        if isinstance(record["research"].get("business_identity"), Mapping)
        and record["research"]["business_identity"].get("disposition") == "QUALIFIED"
    }
    qualified_keys.update(proposal_keys)
    if len(qualified_keys) != QUALIFIED_COUNT:
        raise RecoveryBlockedError(
            f"saved qualified set is {len(qualified_keys)}, expected {QUALIFIED_COUNT}; refusing to infer another prospect"
        )
    held_keys = set(research_by_key) - qualified_keys
    if len(held_keys) != HELD_COUNT:
        raise RecoveryBlockedError("saved held set is not exactly 12 records")
    for key in held_keys:
        reason = str((writer_by_key[key].get("held_reason") or "")).strip()
        if not reason:
            raise RecoveryBlockedError(f"saved Writer hold reason missing for {key}")
    blocked_keys = qualified_keys - proposal_keys
    if len(blocked_keys) != 1:
        raise RecoveryBlockedError("saved qualified-without-message set is not exactly one record")
    blocked_key = next(iter(blocked_keys))
    blocked_reason = str((writer_by_key[blocked_key].get("held_reason") or "")).strip()
    if not blocked_reason:
        raise RecoveryBlockedError("the eighth qualified prospect has no saved draft-block reason")
    queued_keys = set(by_key) - set(research_by_key)
    if len(queued_keys) != QUEUED_COUNT:
        raise RecoveryBlockedError("saved queued set is not exactly 30 candidates")

    return {
        "actor_runs": actor_runs,
        "candidates": candidates,
        "by_key": by_key,
        "research_by_key": research_by_key,
        "writer_by_key": writer_by_key,
        "proposals": proposals,
        "qualified_keys": qualified_keys,
        "held_keys": held_keys,
        "blocked_key": blocked_key,
        "queued_keys": queued_keys,
        "pht_retrieved_at": _pht_now(),
    }


def _event_once(db: Database, event_type: str, entity_id: str, metadata: Mapping[str, Any]) -> None:
    exists = db.connection.execute(
        "SELECT 1 FROM events WHERE event_type=? AND entity_type='production' AND entity_id=? LIMIT 1",
        (event_type, entity_id),
    ).fetchone()
    if exists is None:
        db.connection.execute(
            "INSERT INTO events(event_type,entity_type,entity_id,metadata,created_at) VALUES (?,?,?,?,?)",
            (event_type, "production", entity_id, _json(metadata), _utc_now()),
        )


def _stage_done(db: Database, stage: str) -> bool:
    row = db.connection.execute(
        "SELECT state FROM production_recovery_stages WHERE production_run_id=? AND stage_name=?",
        (RECOVERY_RUN_ID, stage),
    ).fetchone()
    return row is not None and row["state"] == "COMPLETED"


def _mark_stage(db: Database, stage: str, metadata: Mapping[str, Any]) -> None:
    now = _utc_now()
    key = f"{RECOVERY_RUN_ID}:{stage}"
    db.connection.execute(
        """INSERT INTO production_recovery_stages
           (production_run_id,stage_name,idempotency_key,state,metadata_json,created_at,updated_at)
           VALUES (?,?,?,'COMPLETED',?,?,?)
           ON CONFLICT(production_run_id,stage_name) DO UPDATE SET
             state='COMPLETED', metadata_json=excluded.metadata_json, updated_at=excluded.updated_at""",
        (RECOVERY_RUN_ID, stage, key, _json(metadata), now, now),
    )
    _event_once(db, "PRODUCTION_RECOVERY_STAGE_COMPLETED", key, {"stage": stage, **dict(metadata)})


def _candidate_row(db: Database, recovery_key: str):
    return db.connection.execute(
        "SELECT * FROM discovery_candidates WHERE recovery_key=?", (recovery_key,)
    ).fetchone()


def _candidate_insert_values(candidate: Mapping[str, Any], recovery_key: str, discovery_run_id: int, now: str) -> tuple[Any, ...]:
    observed = candidate.get("observed_values")
    if not isinstance(observed, Mapping):
        observed = {
            key: candidate.get(key)
            for key in (
                "source_record_id", "source_url", "business_name", "business_category", "website_url",
                "normalized_domain", "public_business_phone", "normalized_phone", "street_address",
                "city", "region_state", "postal_code", "country", "google_place_id", "booking_url",
                "business_status",
            )
            if candidate.get(key) is not None
        }
    raw_hash = str(candidate.get("raw_payload_hash") or "").strip()
    if not raw_hash:
        raw_hash = hashlib.sha256(_json(dict(candidate)).encode("utf-8")).hexdigest()
    return (
        "APIFY", candidate.get("source_record_id"), candidate.get("source_url"), candidate.get("collected_at") or now,
        _candidate_name(candidate), candidate.get("business_category"), candidate.get("website_url"),
        candidate.get("normalized_domain"), candidate.get("public_business_email"), candidate.get("normalized_email"),
        candidate.get("public_business_phone"), candidate.get("normalized_phone"), candidate.get("street_address"),
        candidate.get("city"), candidate.get("region_state"), candidate.get("postal_code"), candidate.get("country"),
        candidate.get("latitude"), candidate.get("longitude"), candidate.get("google_place_id"), candidate.get("booking_url"),
        candidate.get("business_status"), raw_hash, "ACCEPTED", "ACCEPT", None, "LIVE", "LIVE", _json(observed),
        None, now, now, discovery_run_id, "ACCEPTED", recovery_key, RECOVERY_RUN_ID, "DISCOVERED", "NOT_PROCESSED",
        "NOT_EVALUATED", "NONE", "NONE", None, None, None, None,
    )


def _commit_discovery(db: Database, data: Mapping[str, Any]) -> int:
    for actor in data["actor_runs"]:
        row = db.connection.execute(
            """SELECT actor_id,state,remote_run_id,dataset_id,item_count,error_category
               FROM apify_runs WHERE remote_run_id=?""",
            (actor["remote_run_id"],),
        ).fetchone()
        if row is None:
            raise RecoveryBlockedError(f"verified Apify run is missing from canonical ledger: {actor['remote_run_id']}")
        if (
            row["actor_id"] != EXPECTED_ACTOR_ID
            or row["state"] != "SUCCEEDED"
            or row["remote_run_id"] != actor["remote_run_id"]
            or row["dataset_id"] != actor["dataset_id"]
            or row["item_count"] != actor["item_count"]
            or row["error_category"] is not None
        ):
            raise RecoveryBlockedError(f"canonical Apify ledger does not match saved run: {actor['remote_run_id']}")
    row = db.connection.execute(
        "SELECT discovery_run_id FROM production_recovery_runs WHERE run_id=?", (RECOVERY_RUN_ID,)
    ).fetchone()
    if row is not None and row["discovery_run_id"] is not None:
        discovery_run_id = int(row["discovery_run_id"])
    else:
        now = _utc_now()
        cursor = db.connection.execute(
            """INSERT INTO discovery_runs
               (source,mode,provenance_type,status,started_at,completed_at,input_count,
                accepted_count,updated_count,duplicate_count,possible_duplicate_hold_count,
                suppressed_count,invalid_count,inactive_business_count,insufficient_identity_count,
                source_error_count,metadata)
               VALUES ('APIFY','LIVE','LIVE','COMPLETED',?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (now, now, 50, 50, 0, 0, 0, 0, 0, 0, 0, 0, _json({"production_run_id": RECOVERY_RUN_ID, "recovered": True})),
        )
        discovery_run_id = int(cursor.lastrowid)
        db.connection.execute(
            "UPDATE production_recovery_runs SET discovery_run_id=?, updated_at=? WHERE run_id=?",
            (discovery_run_id, now, RECOVERY_RUN_ID),
        )
    insert_sql = """INSERT INTO discovery_candidates
       (source,source_record_id,source_url,collected_at,business_name,business_category,website_url,
        normalized_domain,public_business_email,normalized_email,public_business_phone,normalized_phone,
        street_address,city,region_state,postal_code,country,latitude,longitude,google_place_id,booking_url,
        business_status,raw_payload_hash,ingestion_status,ingestion_decision,rejection_hold_reason,
        provenance_mode,provenance_type,observed_values,lead_id,created_at,updated_at,run_id,ingestion_outcome,
        recovery_key,production_run_id,discovery_state,research_state,qualification_state,queue_state,
        proposal_state,hold_reason,proposal_block_reason,research_packet_json,decision_maker_json)
       VALUES (""" + ",".join("?" for _ in range(45)) + ")"""
    for key, candidate in data["by_key"].items():
        existing = _candidate_row(db, key)
        values = _candidate_insert_values(candidate, key, discovery_run_id, _utc_now())
        if existing is None:
            db.connection.execute(insert_sql, values)
        elif existing["production_run_id"] != RECOVERY_RUN_ID:
            raise RecoveryBlockedError(f"candidate recovery key belongs to another production run: {key}")
        else:
            db.connection.execute(
                "UPDATE discovery_candidates SET run_id=?,ingestion_outcome='ACCEPTED',updated_at=? WHERE recovery_key=?",
                (discovery_run_id, _utc_now(), key),
            )
    _mark_stage(db, "discovery_commit", {"discovered": 50, "discovery_run_id": discovery_run_id, "actor_runs": data["actor_runs"]})
    return discovery_run_id


def _commit_research_and_qualification(db: Database, data: Mapping[str, Any]) -> None:
    for key, record in data["research_by_key"].items():
        research = record["research"]
        qualified = key in data["qualified_keys"]
        writer = data["writer_by_key"][key]
        hold_reason = str(writer.get("held_reason") or "").strip() or None
        blocked_reason = hold_reason if key == data["blocked_key"] else None
        db.connection.execute(
            """UPDATE discovery_candidates
               SET research_state='RESEARCHED', qualification_state=?, queue_state='NONE',
                   proposal_state=?, hold_reason=?, proposal_block_reason=?, research_packet_json=?,
                   decision_maker_json=?, updated_at=?
               WHERE recovery_key=? AND production_run_id=?""",
            (
                "QUALIFIED" if qualified else "HELD",
                "DRAFT_BLOCKED" if key == data["blocked_key"] else "PENDING_PROPOSAL" if qualified else "NONE",
                hold_reason if not qualified else None,
                blocked_reason,
                _json(research),
                _json(research.get("decision_maker") or {}),
                _utc_now(), key, RECOVERY_RUN_ID,
            ),
        )
        if db.connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise RecoveryBlockedError(f"researched candidate not found during commit: {key}")
    for key in data["queued_keys"]:
        db.connection.execute(
            "UPDATE discovery_candidates SET discovery_state='DISCOVERED',research_state='NOT_PROCESSED',qualification_state='NOT_EVALUATED',queue_state='QUEUED',proposal_state='NONE',updated_at=? WHERE recovery_key=? AND production_run_id=?",
            (_utc_now(), key, RECOVERY_RUN_ID),
        )
    _mark_stage(db, "research_qualification_commit", {"researched": 20, "qualified": 8, "held": 12, "queued": 30})


def _commit_message_proposals(db: Database, data: Mapping[str, Any]) -> None:
    for item in data["proposals"]:
        key = item["key"]
        candidate = _candidate_row(db, key)
        if candidate is None:
            raise RecoveryBlockedError(f"proposal candidate missing: {key}")
        message = item["message"]
        proposal_key = str(message.get("idempotency_key") or "").strip()
        if not proposal_key:
            raise RecoveryBlockedError(f"proposal idempotency key missing: {key}")
        existing = db.connection.execute(
            "SELECT * FROM production_message_proposals WHERE idempotency_key=?", (proposal_key,)
        ).fetchone()
        policy_context = message.get("policy_context")
        if not isinstance(policy_context, Mapping):
            raise RecoveryBlockedError(f"proposal policy context missing: {proposal_key}")
        if existing is None:
            db.connection.execute(
                """INSERT INTO production_message_proposals
                   (production_run_id,candidate_id,source_record_id,business_name,lane,channel,message_stage,
                    touch_number,state,review_status,idempotency_key,subject,body,policy_context_json,proposal_json,
                    gmail_state,notification_state,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    RECOVERY_RUN_ID, candidate["id"], item["source_record_id"], item["business_name"], item["lane"],
                    message["channel"], message.get("message_stage") or message.get("stage") or "INITIAL",
                    int(message.get("touch_number", 1)), "READY_FOR_DRAFT", "PASS", proposal_key,
                    message.get("subject"), str(message.get("body") or ""), _json(policy_context), _json(message),
                    "PENDING", "PENDING", _utc_now(), _utc_now(),
                ),
            )
        else:
            if existing["production_run_id"] != RECOVERY_RUN_ID or existing["body"] != str(message.get("body") or ""):
                raise RecoveryBlockedError(f"proposal idempotency collision or content mismatch: {proposal_key}")
        db.connection.execute(
            "UPDATE discovery_candidates SET proposal_state='READY_FOR_DRAFT',updated_at=? WHERE recovery_key=? AND production_run_id=?",
            (_utc_now(), key, RECOVERY_RUN_ID),
        )
        proposal = db.connection.execute(
            "SELECT id FROM production_message_proposals WHERE idempotency_key=?", (proposal_key,)
        ).fetchone()
        proposal_id = int(proposal["id"])
        for operation_name, suffix in (
            ("GMAIL_CREATE_EMAIL_DRAFT", "gmail:create"),
            ("GMAIL_GET_DRAFT", "gmail:get"),
            ("NOTIFICATION_SEND_OUTBOUND_QUEUE", "notification:outbound-queue"),
        ):
            db.connection.execute(
                """INSERT OR IGNORE INTO production_external_operations
                   (production_run_id,proposal_id,operation_name,idempotency_key,provider,account_scope,state,created_at,updated_at)
                   VALUES (?,?,?,?,?,?, 'PENDING',?,?)""",
                (
                    RECOVERY_RUN_ID, proposal_id, operation_name, f"{proposal_key}:{suffix}",
                    "COMPOSIO" if operation_name.startswith("GMAIL") else "NOTIFIER",
                    "owner@fictional-example.invalid" if operation_name.startswith("GMAIL") else f"{NOTIFICATION_CHANNEL_ID}:{NOTIFICATION_CHANNELS['outbound_queue']}",
                    _utc_now(), _utc_now(),
                ),
            )
    _mark_stage(db, "message_proposal_commit", {"approved_proposals": 7, "eighth_qualified": data["blocked_key"]})


def commit_upstream(
    *,
    northfield_path: Path,
    us_path: Path,
    primary_path: Path,
    secondary_path: Path,
    research_path: Path,
    writer_path: Path,
    valid_path: Path,
    database_path: Path = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    """Commit all saved upstream stages; never calls an external provider."""
    data = _load_inputs(northfield_path, us_path, research_path, writer_path, valid_path)
    db = Database(database_path)
    lock = None
    try:
        migrate_recovery_schema(db)
        lock_path = Path(database_path).parent / ".paldo-os-production.lock"
        with _production_lock(lock_path):
            now = _utc_now()
            db.connection.execute(
                """INSERT OR IGNORE INTO production_recovery_runs
                   (run_id,actor_runs_json,state,upstream_state,summary_json,created_at,updated_at)
                   VALUES (?,?, 'UPSTREAM_IN_PROGRESS','PENDING','{}',?,?)""",
                (RECOVERY_RUN_ID, _json(data["actor_runs"]), now, now),
            )
            db.connection.commit()
            discovery_run_id = _commit_discovery(db, data) if not _stage_done(db, "discovery_commit") else int(
                db.connection.execute("SELECT discovery_run_id FROM production_recovery_runs WHERE run_id=?", (RECOVERY_RUN_ID,)).fetchone()[0]
            )
            db.connection.commit()
            if not _stage_done(db, "research_qualification_commit"):
                _commit_research_and_qualification(db, data)
                db.connection.commit()
            if not _stage_done(db, "message_proposal_commit"):
                _commit_message_proposals(db, data)
                db.connection.commit()
            summary = {
                "run_id": RECOVERY_RUN_ID,
                "recovered_at_pht": _pht_now(),
                "discovery_run_id": discovery_run_id,
                "actor_runs": data["actor_runs"],
                "discovered": 50,
                "researched": 20,
                "qualified": 8,
                "held": 12,
                "queued": 30,
                "approved_proposals": 7,
                "eighth_qualified_draft_state": "DRAFT_BLOCKED",
                "eighth_qualified_recovery_key": data["blocked_key"],
                "gmail_drafts": 0,
                "notification_deliveries": 0,
                "linkedin_automated": False,
            }
            db.connection.execute(
                "UPDATE production_recovery_runs SET state='READY_FOR_EXTERNAL',upstream_state='COMMITTED',summary_json=?,updated_at=? WHERE run_id=?",
                (_json(summary), _utc_now(), RECOVERY_RUN_ID),
            )
            _event_once(db, "PRODUCTION_UPSTREAM_STAGES_COMMITTED", RECOVERY_RUN_ID, summary)
            db.connection.commit()
            return summary
    except Exception:
        db.connection.rollback()
        raise
    finally:
        db.close()


def summarize_canonical(database_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    """Return a safe readback summary, including canonical IDs and states."""
    db = Database(database_path)
    try:
        migrate_recovery_schema(db)
        run = db.connection.execute("SELECT * FROM production_recovery_runs WHERE run_id=?", (RECOVERY_RUN_ID,)).fetchone()
        candidates = [
            dict(row)
            for row in db.connection.execute(
                """SELECT id,recovery_key,production_run_id,business_name,source_record_id,discovery_state,
                          research_state,qualification_state,queue_state,proposal_state,hold_reason,proposal_block_reason
                   FROM discovery_candidates WHERE production_run_id=? ORDER BY id""",
                (RECOVERY_RUN_ID,),
            )
        ]
        proposals = [
            dict(row)
            for row in db.connection.execute(
                """SELECT id,candidate_id,business_name,lane,channel,state,review_status,idempotency_key,
                          gmail_state,notification_state FROM production_message_proposals
                   WHERE production_run_id=? ORDER BY id""",
                (RECOVERY_RUN_ID,),
            )
        ]
        ops = [
            dict(row)
            for row in db.connection.execute(
                """SELECT id,proposal_id,operation_name,idempotency_key,provider,account_scope,state,
                          attempt_count,external_id,error_category FROM production_external_operations
                   WHERE production_run_id=? ORDER BY id""",
                (RECOVERY_RUN_ID,),
            )
        ]
        return {
            "run": None if run is None else dict(run),
            "candidates": candidates,
            "proposals": proposals,
            "external_operations": ops,
            "counts": {
                "discovered": len(candidates),
                "researched": sum(row["research_state"] == "RESEARCHED" for row in candidates),
                "qualified": sum(row["qualification_state"] == "QUALIFIED" for row in candidates),
                "held": sum(row["qualification_state"] == "HELD" for row in candidates),
                "queued": sum(row["queue_state"] == "QUEUED" for row in candidates),
                "ready_for_draft": sum(row["proposal_state"] == "READY_FOR_DRAFT" for row in candidates),
                "draft_blocked": sum(row["proposal_state"] == "DRAFT_BLOCKED" for row in candidates),
            },
            "integrity": db.connection.execute("PRAGMA integrity_check").fetchone()[0],
        }
    finally:
        db.close()


def _queue_card(proposal: Mapping[str, Any]) -> str:
    """Render a bounded Operator-review card without performing a provider call."""
    body = str(proposal.get("body") or "")[:2600]
    return "\n".join(
        [
            "PALDO OS · OUTBOUND QUEUE REVIEW",
            f"Recovery run: {RECOVERY_RUN_ID}",
            f"Proposal ID: {proposal['id']}",
            f"Business: {proposal['business_name']}",
            f"Lane: {proposal['lane']}",
            f"Channel: {proposal['channel']}",
            f"Reviewer status: {proposal['review_status']}",
            "Message policy: PALDO_OS_V1_0",
            f"Subject: {proposal.get('subject') or ''}",
            "Draft body:",
            body,
            "Gmail state: not created; persistent scheduled consent is unavailable on the current Composio consumer surface.",
            f"Next action: Operator reviews this proposal, then uses /approve {proposal['id']} for the bounded draft-only interaction.",
            "Safety: no Gmail SEND and no automated LinkedIn activity.",
        ]
    )[:4096]


def finish_external_operation(
    operation_id: int,
    *,
    state: str,
    external_id: str | None = None,
    safe_result: Mapping[str, Any] | None = None,
    error_category: str | None = None,
    database_path: Path = DEFAULT_DB_PATH,
) -> dict[str, Any]:
    """Finalize one persisted external operation after provider readback."""
    if state not in {"SUCCEEDED", "FAILED", "UNKNOWN"}:
        raise RecoveryBlockedError("invalid external operation terminal state")
    db = Database(database_path)
    try:
        migrate_recovery_schema(db)
        with _production_lock(Path(database_path).parent / ".paldo-os-production.lock"):
            row = db.connection.execute("SELECT * FROM production_external_operations WHERE id=?", (operation_id,)).fetchone()
            if row is None:
                raise RecoveryBlockedError(f"external operation not found: {operation_id}")
            if row["state"] == "SUCCEEDED":
                return {"ok": True, "reused": True, "operation_id": operation_id, "state": "SUCCEEDED", "external_id": row["external_id"]}
            if row["state"] not in {"RUNNING", "PENDING"}:
                raise RecoveryBlockedError(f"external operation already terminal: {row['state']}")
            now = _utc_now()
            db.connection.execute(
                "UPDATE production_external_operations SET state=?,external_id=?,safe_result_json=?,error_category=?,updated_at=? WHERE id=?",
                (state, external_id, _json(safe_result or {}), error_category, now, operation_id),
            )
            if row["operation_name"] == "NOTIFICATION_SEND_OUTBOUND_QUEUE" and state == "SUCCEEDED":
                db.connection.execute(
                    "UPDATE production_message_proposals SET notification_state='DELIVERED',updated_at=? WHERE id=?",
                    (now, row["proposal_id"]),
                )
            db.connection.commit()
            return {"ok": True, "reused": False, "operation_id": operation_id, "state": state, "external_id": external_id}
    finally:
        db.close()


def deliver_notification_review(proposal_id: int, database_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    """Send one committed review card through the existing Hermes gateway."""
    db = Database(database_path)
    try:
        migrate_recovery_schema(db)
        with _production_lock(Path(database_path).parent / ".paldo-os-production.lock"):
            proposal = db.connection.execute(
                """SELECT id,business_name,lane,channel,state,review_status,subject,body,notification_state
                   FROM production_message_proposals
                   WHERE id=? AND production_run_id=?""",
                (proposal_id, RECOVERY_RUN_ID),
            ).fetchone()
            if proposal is None:
                raise RecoveryBlockedError(f"proposal not found: {proposal_id}")
            operation = db.connection.execute(
                """SELECT * FROM production_external_operations
                   WHERE proposal_id=? AND operation_name='NOTIFICATION_SEND_OUTBOUND_QUEUE'""",
                (proposal_id,),
            ).fetchone()
            if operation is None:
                raise RecoveryBlockedError(f"Notification operation not found for proposal: {proposal_id}")
            if operation["state"] == "SUCCEEDED":
                return {"ok": True, "reused": True, "proposal_id": proposal_id, "message_id": operation["external_id"], "state": "SUCCEEDED"}
            if operation["state"] != "PENDING":
                raise RecoveryBlockedError(f"Notification operation is not safely runnable: {operation['state']}")
            now = _utc_now()
            db.connection.execute(
                "UPDATE production_external_operations SET state='RUNNING',attempt_count=attempt_count+1,updated_at=? WHERE id=?",
                (now, operation["id"]),
            )
            db.connection.commit()
            card = _queue_card(dict(proposal))
            operation_id = int(operation["id"])
    finally:
        db.close()

    notifier = Notifier()
    payload = {
        "channel_id": NOTIFICATION_CHANNEL_ID,
        "thread_id": NOTIFICATION_CHANNELS["outbound_queue"],
        "text": card,
        "message_fingerprint": hashlib.sha256(card.encode("utf-8")).hexdigest()[:16],
    }
    try:
        external = notifier.send_message(payload)
        message_id = str(external.get("message_id") or "")
        if not message_id:
            raise RuntimeError("notifier returned no message id")
        finish_external_operation(
            operation_id,
            state="SUCCEEDED",
            external_id=message_id,
            safe_result={"backend": notifier.name, "message_id": message_id, "channel_id": NOTIFICATION_CHANNEL_ID, "topic_id": NOTIFICATION_CHANNELS["outbound_queue"]},
            database_path=database_path,
        )
        return {"ok": True, "reused": False, "proposal_id": proposal_id, "message_id": message_id, "state": "SUCCEEDED"}
    except NotifierExternalStateUnknown:
        finish_external_operation(operation_id, state="UNKNOWN", safe_result={"error_category": "NOTIFICATION_DELIVERY_OUTCOME_UNKNOWN"}, error_category="NOTIFICATION_DELIVERY_OUTCOME_UNKNOWN", database_path=database_path)
        return {"ok": False, "proposal_id": proposal_id, "state": "UNKNOWN", "error_category": "NOTIFICATION_DELIVERY_OUTCOME_UNKNOWN"}
    except Exception:
        finish_external_operation(operation_id, state="FAILED", safe_result={"error_category": "NOTIFICATION_DELIVERY_ERROR"}, error_category="NOTIFICATION_DELIVERY_ERROR", database_path=database_path)
        return {"ok": False, "proposal_id": proposal_id, "state": "FAILED", "error_category": "NOTIFICATION_DELIVERY_ERROR"}


def mark_awaiting_operator_approval(database_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    """Close the Notification fallback stage without falsely completing the cycle."""
    db = Database(database_path)
    try:
        migrate_recovery_schema(db)
        with _production_lock(Path(database_path).parent / ".paldo-os-production.lock"):
            total = db.connection.execute("SELECT COUNT(*) FROM production_message_proposals WHERE production_run_id=?", (RECOVERY_RUN_ID,)).fetchone()[0]
            delivered = db.connection.execute(
                """SELECT COUNT(*) FROM production_external_operations
                   WHERE production_run_id=? AND operation_name='NOTIFICATION_SEND_OUTBOUND_QUEUE' AND state='SUCCEEDED'""",
                (RECOVERY_RUN_ID,),
            ).fetchone()[0]
            if total != PROPOSAL_COUNT or delivered != PROPOSAL_COUNT:
                raise RecoveryBlockedError(f"Notification fallback incomplete: delivered={delivered}, expected={PROPOSAL_COUNT}")
            run = db.connection.execute("SELECT summary_json FROM production_recovery_runs WHERE run_id=?", (RECOVERY_RUN_ID,)).fetchone()
            summary = json.loads(run["summary_json"] or "{}") if run else {}
            summary.update({"notification_deliveries": delivered, "gmail_drafts": 0, "state": "AWAITING_OPERATOR_APPROVAL", "automatic_linkedin": False, "cycle_success": False})
            db.connection.execute(
                "UPDATE production_recovery_runs SET state='AWAITING_OPERATOR_APPROVAL',summary_json=?,updated_at=? WHERE run_id=?",
                (_json(summary), _utc_now(), RECOVERY_RUN_ID),
            )
            _mark_stage(db, "notification_delivery", {"delivered": delivered, "topic_id": NOTIFICATION_CHANNELS["outbound_queue"], "channel_id": NOTIFICATION_CHANNEL_ID})
            _mark_stage(db, "final_cycle_summary", {"state": "AWAITING_OPERATOR_APPROVAL", "cycle_success": False, "gmail_drafts": 0, "notification_deliveries": delivered})
            _event_once(db, "PRODUCTION_RECOVERY_AWAITING_OPERATOR_APPROVAL", RECOVERY_RUN_ID, summary)
            db.connection.commit()
            return summary
    finally:
        db.close()


def _default_paths() -> dict[str, Path]:
    root = Path(os.environ.get("PALDO_RECOVERY_DIR") or "/tmp")
    return {
        "primary_path": root / "recovery-primary-handoff.json",
        "secondary_path": root / "recovery-secondary-handoff.json",
        "research_path": root / "recovery-research-combined.json",
        "writer_path": root / "recovery-writer.json",
        "valid_path": root / "recovery-valid-messages.json",
    }


def main(argv: list[str] | None = None) -> int:
    defaults = _default_paths()
    parser = argparse.ArgumentParser(description="Paldo saved-result recovery boundary")
    sub = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--primary", type=Path, default=defaults["primary_path"])
    common.add_argument("--secondary", type=Path, default=defaults["secondary_path"])
    common.add_argument("--research", type=Path, default=defaults["research_path"])
    common.add_argument("--writer", type=Path, default=defaults["writer_path"])
    common.add_argument("--valid", type=Path, default=defaults["valid_path"])
    common.add_argument("--database", type=Path, default=DEFAULT_DB_PATH)
    sub.add_parser("validate", parents=[common])
    sub.add_parser("commit-upstream", parents=[common])
    sub.add_parser("summary", parents=[common])
    deliver = sub.add_parser("deliver-notification", parents=[common])
    deliver.add_argument("--proposal-id", type=int, required=True)
    sub.add_parser("mark-awaiting-approval", parents=[common])
    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            data = _load_inputs(args.northfield, args.us, args.research, args.writer, args.valid)
            print(_json({
                "ok": True,
                "run_id": RECOVERY_RUN_ID,
                "actor_runs": data["actor_runs"],
                "discovered": len(data["by_key"]),
                "researched": len(data["research_by_key"]),
                "qualified": len(data["qualified_keys"]),
                "held": len(data["held_keys"]),
                "queued": len(data["queued_keys"]),
                "approved_proposals": len(data["proposals"]),
                "eighth_qualified_draft_state": "DRAFT_BLOCKED",
            }))
        elif args.command == "commit-upstream":
            print(_json(commit_upstream(
                northfield_path=args.northfield, us_path=args.us, research_path=args.research,
                writer_path=args.writer, valid_path=args.valid, database_path=args.database,
            )))
        elif args.command == "deliver-notification":
            print(_json(deliver_notification_review(args.proposal_id, args.database)))
        elif args.command == "mark-awaiting-approval":
            print(_json(mark_awaiting_operator_approval(args.database)))
        else:
            print(_json(summarize_canonical(args.database)))
        return 0
    except (RecoveryBlockedError, OSError, sqlite3.Error, ValueError) as error:
        print(_json({"ok": False, "status": "FAILED", "error_category": str(error)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
