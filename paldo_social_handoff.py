#!/usr/bin/env python3
"""Commit a bounded, secret-free Reddit/Facebook opportunity handoff."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys

WORKSPACE = Path(os.environ.get("PALDO_WORKSPACE") or Path(__file__).resolve().parent)
sys.path.insert(0, str(WORKSPACE))

from paldo_os_outbound import Database  # noqa: E402
from step23_expansion import migrate_expansion, record_source_checkpoint, render_social_cards, upsert_social_opportunity  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--database", default=str(WORKSPACE / "data" / "paldo_os_outbound.sqlite3"))
    args = parser.parse_args(argv)
    payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
    records = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise SystemExit("handoff records must be a list")
    now = datetime.fromisoformat(str(payload.get("retrieved_at"))) if isinstance(payload, dict) and payload.get("retrieved_at") else datetime.now(timezone.utc)
    db = Database(args.database)
    try:
        migrate_expansion(db)
        rows = [upsert_social_opportunity(db, record, now=now) for record in records]
        record_source_checkpoint(db, source=str(payload.get("source") or "social"), cursor=str(payload.get("cursor") or payload.get("retrieved_at") or ""), status="COMPLETED", detail={"upserted": len(rows)}, now=now)
        cards = render_social_cards(db)
        print(json.dumps({"status": "ok", "upserted": [{"id": r["id"], "class": r["item_class"], "route_topic": r["route_topic"], "status": r["status"]} for r in rows], "queued_cards": cards}, ensure_ascii=False))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
