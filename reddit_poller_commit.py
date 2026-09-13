#!/usr/bin/env python3
"""Commit and deliver a bounded, triaged Reddit poll result.

Input is a records file produced by the poller's triage step (canonical URLs,
verified publication times, returned public text, routed classes). This step:

* acquires the poller lock around the whole commit/delivery operation;
* upserts every record through the canonical Paldo social-opportunity schema;
* writes the secret-free social and job handoff files into the evidence run dir;
* delivers ONLY newly upserted canonical QUEUED records with
  freshness_status=ELIGIBLE, to the outbound-queue topic (business/help) or the
  Job Scrape topic (explicit hiring), saving the returned message ids;
* records the canonical source checkpoint.

It never posts, comments, votes, follows, messages or applies anywhere, and
never performs a Gmail, LinkedIn, Apify or Google Places call.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from hashlib import sha256
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WORKSPACE = Path(os.environ.get("PALDO_WORKSPACE") or Path(__file__).resolve().parent)
LOCK_PATH = Path(os.environ.get("PALDO_REDDIT_LOCK") or (WORKSPACE / ".reddit-poll.lock"))
PALDO_DB = Path(os.environ.get("PALDO_DB_PATH") or (WORKSPACE / "data" / "paldo_os_outbound.sqlite3"))
MONITOR_PROJECT = Path(os.environ["PALDO_JOB_MONITOR_DIR"]) if os.environ.get("PALDO_JOB_MONITOR_DIR") else None
MONITOR_PYTHON = MONITOR_PROJECT / ".venv" / "bin" / "python" if MONITOR_PROJECT else None
CHANNEL_BUSINESS = os.environ.get("PALDO_CHANNEL_BUSINESS") or "business"
CHANNEL_JOB = os.environ.get("PALDO_CHANNEL_JOBS") or "jobs"

sys.path.insert(0, str(WORKSPACE))

from notifier import (  # noqa: E402
    NOTIFICATION_CHANNEL_ID,
    Notifier,
    NotifierExternalStateUnknown,
)


def card_text(row: dict[str, Any]) -> str:
    lines = [
        "PALDO OS · REDDIT OPPORTUNITY",
        f"Community: {row.get('community')}",
        f"Class: {row.get('item_class')} · intent: {row.get('intent_label')}",
        f"Author: {row.get('public_author')}",
        f"Published: {row.get('published_at')} · freshness: {row.get('freshness_status')}",
        f"URL: {row.get('original_url')}",
        f"Need: {row.get('need')}",
        f"Excerpt: {str(row.get('supporting_excerpt') or '')[:700]}",
        f"Context: {str(row.get('business_context') or '')[:500]}",
        f"Confidence: {row.get('confidence')}",
    ]
    comment = row.get("helpful_comment")
    if comment:
        body = str(comment).strip()
        lines += [
            "Draft to copy and paste:",
            "```",
            body,
            "```",
        ]
    lines += [
        f"Record id: {row.get('id')}",
        "Next action: Operator reviews this card and replies manually on Reddit if he chooses.",
        "Safety: no comment, post, DM, vote, follow or application was made by Paldo OS.",
    ]
    return "\n".join(lines)[:4000]


def send(target: str, text: str, *, notifier: Any = None) -> dict[str, Any]:
    """Deliver one card through the configured notifier backend."""
    dispatcher = notifier or Notifier()
    try:
        result = dispatcher.send_message({
            "channel_id": NOTIFICATION_CHANNEL_ID,
            "thread_id": target,
            "text": text,
            "message_fingerprint": sha256(text.encode("utf-8")).hexdigest()[:16],
        })
    except NotifierExternalStateUnknown:
        return {"ok": False, "state": "UNKNOWN", "error": "unknown_external_state"}
    except Exception as error:
        return {"ok": False, "state": "FAILED", "error": str(error)[:200]}
    if not result.get("message_id"):
        return {"ok": False, "state": "UNKNOWN", "error": "missing_message_id"}
    return {"ok": True, "message_id": str(result["message_id"])}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--records", required=True)
    args = parser.parse_args(argv)
    run_dir = Path(args.run_dir)

    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[SILENT]")
            return 0

        now = datetime.now(timezone.utc).replace(microsecond=0)
        retrieved_at = now.isoformat().replace("+00:00", "Z")
        payload = json.loads(Path(args.records).read_text(encoding="utf-8"))
        records = payload.get("records") if isinstance(payload, dict) else payload
        if not isinstance(records, list):
            raise SystemExit("records must be a list")

        from paldo_os_outbound import Database  # type: ignore
        from step23_expansion import (  # type: ignore
            mark_social_delivery,
            record_source_checkpoint,
            upsert_social_opportunity,
        )

        # Duplicate suppression: a canonical URL that the poller has already
        # seen (and that is no longer sitting in the QUEUED state) must never be
        # re-alerted, even when a later sweep rediscovers the same thread.
        known: set[str] = set()
        with Database(PALDO_DB) as db:  # type: ignore[attr-defined]
            for row in db.connection.execute(
                "SELECT original_url,status FROM social_opportunities WHERE source='reddit'"
            ).fetchall():
                if str(row["status"]) != "QUEUED":
                    known.add(str(row["original_url"]))
        held_duplicates = [r.get("original_url") for r in records if r.get("original_url") in known]
        records = [r for r in records if r.get("original_url") not in known]

        upserted: list[dict[str, Any]] = []
        with Database(PALDO_DB) as db:  # type: ignore[attr-defined]
            for record in records:
                upserted.append(upsert_social_opportunity(db, record, now=now))

        business_rows = [r for r in upserted if r.get("item_class") in {"BUSINESS_OPPORTUNITY", "HELP_SEEKING"}]
        job_rows = [r for r in upserted if r.get("item_class") == "HIRING_REQUEST"]

        social_handoff = {
            "schema_version": 2,
            "source": "reddit",
            "retrieved_at": retrieved_at,
            "cursor": retrieved_at,
            "records": [r for r in records if str(r.get("item_class", "")).upper() != "HIRING_REQUEST"],
            "triage": {
                "jobs_routed": len(job_rows),
                "business_items_routed": len(business_rows),
                "business_record_ids": [r["id"] for r in business_rows],
                "job_record_ids": [r["id"] for r in job_rows],
            },
        }
        job_handoff = {
            "schema_version": 2,
            "source": "reddit",
            "retrieved_at": retrieved_at,
            "cursor": retrieved_at,
            "records": [r for r in records if str(r.get("item_class", "")).upper() == "HIRING_REQUEST"],
        }
        (run_dir / "reddit_social_handoff.json").write_text(json.dumps(social_handoff, indent=2, ensure_ascii=False), encoding="utf-8")
        (run_dir / "reddit_job_handoff.json").write_text(json.dumps(job_handoff, indent=2, ensure_ascii=False), encoding="utf-8")

        # Keep the existing job monitor reconciled even when there is no new job card.
        if MONITOR_PROJECT is None:
            job_returncode = None
            job_summary = {"skipped": "PALDO_JOB_MONITOR_DIR is not configured"}
        else:
            try:
                job_result = subprocess.run(
                    [str(MONITOR_PYTHON), str(MONITOR_PROJECT / "scripts" / "process_external_handoff.py"),
                     "--source", "reddit", "--input", str(run_dir / "reddit_job_handoff.json")],
                    cwd=str(MONITOR_PROJECT), capture_output=True, text=True, timeout=120,
                )
                job_returncode = job_result.returncode
                try:
                    job_summary = json.loads(job_result.stdout)
                except Exception:
                    job_summary = {"raw": (job_result.stdout or "")[:300]}
            except subprocess.TimeoutExpired:
                job_returncode = 124
                job_summary = {"error": "timeout"}

        deliveries: list[dict[str, Any]] = []
        for row in business_rows + job_rows:
            if row.get("status") != "QUEUED" or row.get("freshness_status") != "ELIGIBLE":
                deliveries.append({"id": row.get("id"), "state": "HELD", "status": row.get("status"), "freshness": row.get("freshness_status")})
                continue
            target = CHANNEL_JOB if row.get("item_class") == "HIRING_REQUEST" else CHANNEL_BUSINESS
            outcome = send(target, card_text(row))
            if outcome.get("ok"):
                with Database(PALDO_DB) as db:  # type: ignore[attr-defined]
                    mark_social_delivery(db, [int(row["id"])], outcome["message_id"], now=now)
                row["status"] = "DELIVERED"
                row["notification_message_id"] = outcome["message_id"]
                deliveries.append({"id": row["id"], "state": "DELIVERED", "message_id": outcome["message_id"], "target": target})
            else:
                deliveries.append({"id": row["id"], "state": outcome.get("state"), "target": target, "error": outcome.get("stderr") or outcome.get("stdout")})

        detail = {
            "access_route": "DonSeTch only",
            "discovery_route": "official /new/.json listings via public Reddit JSON representation, then canonical thread + comment JSON deep fetch",
            "collection_run_dirs": [str(run_dir)],
            "candidates_triaged": len(records) + len(held_duplicates),
            "duplicates_held": held_duplicates,
            "upserted": len(upserted),
            "jobs_routed": len(job_rows),
            "business_items_routed": len(business_rows),
            "deliveries": deliveries,
            "job_handoff_returncode": job_returncode,
            "job_handoff": job_summary,
            "delivery_attempted": bool(deliveries),
        }
        with Database(PALDO_DB) as db:  # type: ignore[attr-defined]
            record_source_checkpoint(db, source="reddit", cursor=retrieved_at, status="COMPLETED", detail=detail, now=now)

        result = {
            "status": "complete",
            "run_dir": str(run_dir),
            "retrieved_at": retrieved_at,
            "upserted": [
                {"id": r["id"], "community": r["community"], "item_class": r["item_class"],
                 "intent_label": r["intent_label"], "status": r["status"], "freshness": r["freshness_status"],
                 "url": r["original_url"]}
                for r in upserted
            ],
            "deliveries": deliveries,
        }
        (run_dir / "commit_result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False))
        return 0
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


if __name__ == "__main__":
    raise SystemExit(main())
