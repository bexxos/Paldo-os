#!/usr/bin/env python3
"""Finalize a bounded Reddit poll after DonSeTch collection.

The collector has already saved raw feeds/searches/thread attempts. This step
commits empty secret-free handoffs when no canonical, fresh, evidence-backed
record is routable, updates the canonical checkpoints, and never sends when
there are no newly discovered eligible records.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import sys
from typing import Any

WORKSPACE = Path(os.environ.get("PALDO_WORKSPACE") or Path(__file__).resolve().parent)
LOCK_PATH = Path(os.environ.get("PALDO_REDDIT_LOCK") or (WORKSPACE / ".reddit-poll.lock"))
PALDO_DB = Path(os.environ.get("PALDO_DB_PATH") or (WORKSPACE / "data" / "paldo_os_outbound.sqlite3"))
MONITOR_PROJECT = Path(os.environ["PALDO_JOB_MONITOR_DIR"]) if os.environ.get("PALDO_JOB_MONITOR_DIR") else None
MONITOR_PYTHON = MONITOR_PROJECT / ".venv" / "bin" / "python" if MONITOR_PROJECT else None


def parse_dt(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip() or value.upper() in {"UNKNOWN", "DATE_UNVERIFIED"}:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def run(args: list[str], cwd: Path | None = None, timeout: int = 120) -> dict[str, Any]:
    try:
        p = subprocess.run(args, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        try:
            payload = json.loads(p.stdout)
        except Exception:
            payload = None
        return {"argv": args, "returncode": p.returncode, "stdout": p.stdout, "stderr": p.stderr, "json": payload}
    except subprocess.TimeoutExpired as exc:
        return {"argv": args, "returncode": None, "stdout": exc.stdout or "", "stderr": exc.stderr or "", "json": None, "error": "timeout"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    run_dir = Path(args.run_dir)
    checkpoint_path = run_dir / "collection_checkpoint.json"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))

    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[SILENT]")
            return 0
        now = datetime.now(timezone.utc).replace(microsecond=0)
        retrieved_at = now.isoformat().replace("+00:00", "Z")
        cursor = checkpoint.get("retrieved_at") or retrieved_at

        # The empty records are intentional: the source evidence had no newly
        # discovered record with a recoverable canonical URL and verified fresh
        # publication/comment timestamp. Raw evidence remains in run_dir.
        handoff = {
            "schema_version": 2,
            "source": "reddit",
            "retrieved_at": retrieved_at,
            "cursor": cursor,
            "records": [],
            "collection_run_dir": str(run_dir),
            "triage": {
                "jobs_routed": 0,
                "business_items_routed": 0,
                "reason": "No newly discovered canonical thread/comment met the verified seven-day recency gate with returned public evidence.",
            },
        }
        social_path = run_dir / "reddit_social_handoff.json"
        job_path = run_dir / "reddit_job_handoff.json"
        social_path.write_text(json.dumps(handoff, indent=2, ensure_ascii=False), encoding="utf-8")
        job_path.write_text(json.dumps(handoff, indent=2, ensure_ascii=False), encoding="utf-8")

        job_result = run([
            str(MONITOR_PYTHON), str(MONITOR_PROJECT / "scripts" / "process_external_handoff.py"),
            "--source", "reddit", "--input", str(job_path),
        ], cwd=MONITOR_PROJECT, timeout=120)
        social_result = run([
            sys.executable, str(WORKSPACE / "paldo_social_handoff.py"),
            "--input", str(social_path),
        ], cwd=WORKSPACE, timeout=120)

        # Import the canonical Paldo modules only after the handoff files have
        # been durably written and processed.
        sys.path.insert(0, str(WORKSPACE))
        from paldo_os_outbound import Database  # type: ignore
        from step23_expansion import record_source_checkpoint  # type: ignore

        stale_social = 0
        queued_social = 0
        with Database(PALDO_DB) as db:  # type: ignore[attr-defined]
            cutoff = now - timedelta(days=7)
            rows = db.connection.execute("SELECT id,published_at,status,freshness_status FROM social_opportunities WHERE source='reddit'").fetchall()
            for row in rows:
                published = parse_dt(row["published_at"])
                if published is not None and published < cutoff and row["status"] == "QUEUED":
                    cur = db.connection.execute(
                        "UPDATE social_opportunities SET status='RETAINED_CONTEXT', freshness_status='STALE', updated_at=? WHERE id=? AND status='QUEUED'",
                        (now.isoformat(), row["id"]),
                    )
                    stale_social += cur.rowcount
            queued_social = int(db.connection.execute("SELECT COUNT(*) FROM social_opportunities WHERE source='reddit' AND status='QUEUED' AND freshness_status='ELIGIBLE'").fetchone()[0])
            detail = {
                "access_route": "DonSeTch only",
                "agent_reach": checkpoint.get("agent_reach"),
                "collection_run_dir": str(run_dir),
                "fresh_feeds": checkpoint.get("fresh_feeds", []),
                "search_count": checkpoint.get("search_count", 0),
                "canonical_candidates": checkpoint.get("candidate_count", 0),
                "selected_thread_attempts": checkpoint.get("selected_thread_count", 0),
                "jobs_routed": 0,
                "business_items_routed": 0,
                "queued_eligible_after_handoff": queued_social,
                "stale_queued_marked": stale_social,
                "reason": "Fresh listings were returned, but the supported DonSeTch Reddit listing/search route did not expose recoverable canonical URLs for current feed items; canonical fetches recovered only old material. No snippet-only or stale item was alerted.",
                "job_handoff_returncode": job_result.get("returncode"),
                "social_handoff_returncode": social_result.get("returncode"),
                "delivery_attempted": False,
                "delivery": "No notification sent: no newly discovered QUEUED + ELIGIBLE record.",
            }
            record_source_checkpoint(
                db,
                source="reddit",
                cursor=cursor,
                status="COMPLETED",
                detail=detail,
                now=now,
            )
            monitor_summary = {}
            if isinstance(job_result.get("json"), dict):
                monitor_summary = job_result["json"]
            social_summary = social_result.get("json") if isinstance(social_result.get("json"), dict) else {}
            result = {
                "status": "complete",
                "run_dir": str(run_dir),
                "retrieved_at": retrieved_at,
                "jobs": monitor_summary,
                "social": social_summary,
                "stale_social_marked": stale_social,
                "queued_social_eligible": queued_social,
                "delivery_attempted": False,
                "delivery_reason": "no newly discovered eligible routed records",
            }
            (run_dir / "finalization.json").write_text(json.dumps({"checkpoint_detail": detail, "result": result, "job_result": job_result, "social_result": social_result}, indent=2, ensure_ascii=False), encoding="utf-8")
            print(json.dumps(result, ensure_ascii=False))
            return 0 if job_result.get("returncode") == 0 and social_result.get("returncode") == 0 else 1
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


if __name__ == "__main__":
    raise SystemExit(main())
