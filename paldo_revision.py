#!/usr/bin/env python3
"""Store and review one experimental revision without changing an operational proposal."""
from __future__ import annotations

from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from paldo_recovery import RECOVERY_RUN_ID, migrate_recovery_schema
from step17b_pilot_policy import validate_versioned_message_policy

PROPOSAL_ID = 7
REVISION_STATE = "AWAITING_OPERATOR_REVIEW"
REVISION_KEY_PREFIX = os.environ.get("PALDO_REVISION_KEY_PREFIX") or "paldo-os:experimental-variant:consultation-attendance"
RESEARCH_PATH = Path(os.environ.get("PALDO_REVISION_RESEARCH") or "recovery-research-combined.json")
REVISED_SUBJECT = "consultation attendance"
REVISED_BODY = """Hi Alex,

I’m an automation specialist working with appointment-based service businesses.
I saw that the fictional wellness group’s downtown location offers free consultations and online booking. Would reducing missed consultations be a priority for your team?
At a local service business I worked with, I built a system that helped reduce no-shows.
Here’s the case study: https://example.com/work/case-study
Operator"""


class RevisionBlockedError(RuntimeError):
    pass


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _pht_now() -> str:
    return datetime.now(ZoneInfo("UTC")).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _read_prospect_research() -> dict[str, Any]:
    try:
        records = json.loads(RESEARCH_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RevisionBlockedError(f"saved prospect research is unavailable: {RESEARCH_PATH}") from error
    for item in records:
        if item.get("research_name") == "Fictional Wellness Downtown":
            return item
    raise RevisionBlockedError("saved prospect research record not found")


def _factual_review(research_record: Mapping[str, Any], proposal: Mapping[str, Any]) -> dict[str, Any]:
    research = research_record.get("research") or {}
    decision = research.get("decision_maker") or {}
    booking = research.get("booking_route") or {}
    sources = research.get("sources") or []
    source_urls = {str(source.get("url")) for source in sources if isinstance(source, Mapping)}
    body = REVISED_BODY
    findings: list[str] = []
    checks: dict[str, Any] = {}

    checks["edward_identity_supported"] = (
        "Alex" in str(decision.get("name") or "")
        and str(decision.get("role") or "").casefold() == "founder and medical director"
        and str(proposal.get("policy_context_json") or "").find('"recipient_first_name":"Alex"') >= 0
        and "https://example.com/fictional-wellness/about" in source_urls
    )
    if not checks["edward_identity_supported"]:
        findings.append("Alex’s identity/role or the saved evidence linkage is not sufficient.")

    booking_evidence = str(booking.get("evidence") or "").casefold()
    checks["free_consultation_and_online_booking_supported"] = (
        "free consultation" in booking_evidence
        and "book now" in booking_evidence
        and booking.get("url") == "https://example.com/fictional-wellness/downtown"
        and booking.get("url") in source_urls
    )
    if not checks["free_consultation_and_online_booking_supported"]:
        findings.append("The saved evidence does not support the free-consultation and booking observation.")

    checks["no_current_prospect_no_show_claim"] = not bool(
        re.search(r"fictional wellness[^.?!]*(?:no[- ]shows?|missed (?:appointments?|consultations?))", body, re.IGNORECASE)
    )
    if not checks["no_current_prospect_no_show_claim"]:
        findings.append("The revision could be read as claiming the prospect currently has a no-show problem.")

    checks["one_question_cta"] = body.count("?") == 1
    checks["no_metrics_guarantees_deadlines"] = not bool(
        re.search(r"%|guarantee|guaranteed|within \d+|by (?:tomorrow|next week)|save you \d+", body, re.IGNORECASE)
    )
    checks["no_existing_software_claim"] = not bool(re.search(r"your (?:software|platform|crm|system)", body, re.IGNORECASE))
    checks["international_body_exclusions"] = not bool(re.search(r"harborview business|westport|northfield", body, re.IGNORECASE))
    checks["one_case_study_link"] = re.findall(r"https?://[^\s]+", body) == ["https://example.com/work/case-study"]
    checks["outcome_is_no_show_reduction"] = "reduce missed appointments" in body.casefold() and "reduce no-shows" in body.casefold()
    checks["concise_no_tool_list"] = "tools" not in body.casefold() and "services:" not in body.casefold()
    for name, passed in checks.items():
        if not passed and name not in {"edward_identity_supported", "free_consultation_and_online_booking_supported", "no_current_prospect_no_show_claim"}:
            findings.append(f"Factual/wording check failed: {name}.")
    return {"passed": not findings, "checks": checks, "findings": findings, "source_urls": sorted(source_urls)}


def _schema(db: Database) -> None:
    with db.connection:
        db.connection.execute(
            """CREATE TABLE IF NOT EXISTS production_message_revisions (
                id INTEGER PRIMARY KEY,
                production_run_id TEXT NOT NULL,
                proposal_id INTEGER NOT NULL REFERENCES production_message_proposals(id) ON DELETE RESTRICT,
                revision_key TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                variant_type TEXT NOT NULL,
                original_snapshot_json TEXT NOT NULL,
                revised_subject TEXT NOT NULL,
                revised_body TEXT NOT NULL,
                factual_review_json TEXT NOT NULL,
                deterministic_valid INTEGER NOT NULL CHECK (deterministic_valid IN (0,1)),
                deterministic_error_codes_json TEXT NOT NULL,
                reviewer_status TEXT NOT NULL,
                reviewer_qa_json TEXT NOT NULL,
                policy_scope TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )
        db.connection.execute(
            "CREATE INDEX IF NOT EXISTS production_message_revisions_state_idx ON production_message_revisions(proposal_id,state,reviewer_status)"
        )


def store_revision(database_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    research_record = _read_prospect_research()
    db = Database(database_path)
    lock_path = Path(database_path).parent / ".paldo-os-production.lock"
    try:
        migrate_recovery_schema(db)
        _schema(db)
        with lock_path.open("a+", encoding="utf-8") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                raise RevisionBlockedError("PRODUCTION_RUN_ALREADY_ACTIVE") from error
            try:
                proposal = db.connection.execute(
                    """SELECT id,candidate_id,business_name,lane,channel,message_stage,touch_number,state,review_status,
                              idempotency_key,subject,body,policy_context_json,proposal_json
                       FROM production_message_proposals WHERE id=? AND production_run_id=?""",
                    (PROPOSAL_ID, RECOVERY_RUN_ID),
                ).fetchone()
                if proposal is None:
                    raise RevisionBlockedError("proposal 7 not found in the recovered run")
                if proposal["business_name"] != "Fictional Wellness Downtown" or proposal["lane"] != "SECONDARY" or proposal["channel"] != "EMAIL":
                    raise RevisionBlockedError("proposal 7 does not match the saved prospect record")
                original = dict(proposal)
                original.pop("proposal_json", None)
                factual = _factual_review(research_record, original)
                context = json.loads(proposal["policy_context_json"])
                revised = {
                    "channel": "EMAIL",
                    "message_stage": "INITIAL",
                    "touch_number": 1,
                    "subject": REVISED_SUBJECT,
                    "body": REVISED_BODY,
                }
                deterministic = validate_versioned_message_policy(revised, context)
                reviewer_qa = {
                    "status": "BLOCK" if not deterministic.get("valid") or not factual.get("passed") else "PASS",
                    "review_type": "SELECTIVE_REVIEWER_QA",
                    "summary": (
                        "Factual observation and Alex identity are supported; the experimental copy avoids a current prospect no-show claim, metrics, guarantees, software claims, and extra links. "
                        "The existing international identity/proof contract blocks this variant and requires Operator’s policy decision before operational use."
                    ),
                    "blocking_reasons": list(deterministic.get("error_codes", [])) + list(factual.get("findings", [])),
                    "policy_exception_requested": True,
                    "global_policy_changed": False,
                }
                revision_digest = hashlib.sha256((REVISED_SUBJECT + "\n" + REVISED_BODY).encode("utf-8")).hexdigest()[:16]
                revision_key = f"{REVISION_KEY_PREFIX}:{revision_digest}"
                existing = db.connection.execute("SELECT * FROM production_message_revisions WHERE revision_key=?", (revision_key,)).fetchone()
                if existing is None:
                    db.connection.execute(
                        """INSERT INTO production_message_revisions
                           (production_run_id,proposal_id,revision_key,state,variant_type,original_snapshot_json,
                            revised_subject,revised_body,factual_review_json,deterministic_valid,deterministic_error_codes_json,
                            reviewer_status,reviewer_qa_json,policy_scope,created_at,updated_at)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            RECOVERY_RUN_ID, PROPOSAL_ID, revision_key, REVISION_STATE, "EXPERIMENTAL_VARIANT",
                            _json(original), REVISED_SUBJECT, REVISED_BODY, _json(factual), int(bool(deterministic.get("valid"))),
                            _json(deterministic.get("error_codes", [])), reviewer_qa["status"], _json(reviewer_qa),
                            "PROPOSAL_7_ONLY__PALDO_OS_V1_0_UNCHANGED", _utc_now(), _utc_now(),
                        ),
                    )
                    revision_id = int(db.connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                else:
                    revision_id = int(existing["id"])
                    if existing["revised_body"] != REVISED_BODY or existing["original_snapshot_json"] != _json(original):
                        raise RevisionBlockedError("revision key collision or original snapshot mismatch")
                after = db.connection.execute(
                    "SELECT id,subject,body,policy_context_json,state,review_status FROM production_message_proposals WHERE id=?",
                    (PROPOSAL_ID,),
                ).fetchone()
                if dict(after) != {k: original[k] for k in ("id", "subject", "body", "policy_context_json", "state", "review_status")}:
                    raise RevisionBlockedError("proposal 7 changed while storing the experimental revision")
                for preserved_id in range(1, 7):
                    count = db.connection.execute("SELECT COUNT(*) FROM production_message_proposals WHERE id=?", (preserved_id,)).fetchone()[0]
                    if count != 1:
                        raise RevisionBlockedError(f"proposal {preserved_id} was not preserved")
                event_exists = db.connection.execute(
                    "SELECT 1 FROM events WHERE event_type='EXPERIMENTAL_MESSAGE_REVISION_STORED' AND entity_type='production_message_revision' AND entity_id=? LIMIT 1",
                    (str(revision_id),),
                ).fetchone()
                if event_exists is None:
                    db.connection.execute(
                        "INSERT INTO events(event_type,entity_type,entity_id,metadata,created_at) VALUES (?,?,?,?,?)",
                        (
                            "EXPERIMENTAL_MESSAGE_REVISION_STORED", "production_message_revision", str(revision_id),
                            _json({"proposal_id": PROPOSAL_ID, "state": REVISION_STATE, "reviewer_status": reviewer_qa["status"], "global_policy_changed": False}), _utc_now(),
                        ),
                    )
                db.connection.commit()
                return {
                    "revision_id": revision_id,
                    "proposal_id": PROPOSAL_ID,
                    "state": REVISION_STATE,
                    "reviewer_status": reviewer_qa["status"],
                    "deterministic_valid": bool(deterministic.get("valid")),
                    "deterministic_error_codes": deterministic.get("error_codes", []),
                    "factual_review": factual,
                    "reviewer_qa": reviewer_qa,
                    "original_subject": original["subject"],
                    "original_body": original["body"],
                    "revised_subject": REVISED_SUBJECT,
                    "revised_body": REVISED_BODY,
                    "global_policy_changed": False,
                    "proposals_1_to_6_touched": False,
                }
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    finally:
        db.close()


def read_revision(revision_id: int, database_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    db = Database(database_path)
    try:
        migrate_recovery_schema(db)
        _schema(db)
        row = db.connection.execute("SELECT * FROM production_message_revisions WHERE id=?", (revision_id,)).fetchone()
        if row is None:
            raise RevisionBlockedError(f"revision not found: {revision_id}")
        return dict(row)
    finally:
        db.close()


def main() -> int:
    try:
        result = store_revision()
        print(_json(result))
        return 0
    except (RevisionBlockedError, OSError, sqlite3.Error, ValueError, json.JSONDecodeError) as error:
        print(_json({"ok": False, "status": "FAILED", "error": str(error)}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
