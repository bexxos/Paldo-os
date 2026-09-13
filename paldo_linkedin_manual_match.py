#!/usr/bin/env python3
"""Operator's manual LinkedIn URL/match input boundary; never sends anything."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

WORKSPACE = Path(os.environ.get("PALDO_WORKSPACE") or Path(__file__).resolve().parent)
sys.path.insert(0, str(WORKSPACE))

from paldo_os_outbound import Database  # noqa: E402
from step23_expansion import prepare_linkedin_draft, record_linkedin_match  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Record one operator-reviewed LinkedIn match")
    parser.add_argument("--candidate-id", type=int, required=True)
    parser.add_argument("--person-name", required=True)
    parser.add_argument("--person-role", required=True)
    parser.add_argument("--location", required=True)
    parser.add_argument("--profile-url", required=True)
    parser.add_argument("--web-status", choices=["NOT_CHECKED", "POSSIBLE", "VERIFIED", "REJECTED"], default="POSSIBLE")
    parser.add_argument("--web-source-url", required=True)
    parser.add_argument("--web-excerpt", required=True)
    parser.add_argument("--confirm-business", action="store_true")
    parser.add_argument("--operator-evidence")
    parser.add_argument("--operator-id", default="OPERATOR")
    parser.add_argument("--database", default=str(WORKSPACE / "data" / "paldo_os_outbound.sqlite3"))
    args = parser.parse_args(argv)
    db = Database(args.database)
    try:
        match = record_linkedin_match(
            db,
            candidate_id=args.candidate_id,
            person_name=args.person_name,
            person_role=args.person_role,
            location=args.location,
            profile_url=args.profile_url,
            web_verification_status=args.web_status,
            web_evidence={"source_url": args.web_source_url, "excerpt": args.web_excerpt},
            operator_confirmed=args.confirm_business,
            operator_evidence=args.operator_evidence,
            operator_id=args.operator_id,
        )
        result = {"match": match, "message_draft": None}
        if match["match_status"] == "SUPPORTED_PERSONAL":
            result["message_draft"] = prepare_linkedin_draft(db, match_id=int(match["id"]))
        print(json.dumps(result, ensure_ascii=False))
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
