#!/usr/bin/env python3
"""Bounded Apify discovery helper for Hermes cron.

This helper is called by the Hermes cron agent. It reads only APIFY_API_TOKEN
from the protected Hermes environment file, uses the existing Step 5 Apify
client/lifecycle, and writes only normalized, allowlisted candidate data.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from step3_discovery import (
    APIFY_SOURCE,
    ApifyAdapter,
    ApifyClient,
    ApifyRequestError,
    ApifyRunState,
    DiscoveryMode,
    migrate_step5,
    poll_apify_run,
    start_apify_live_run,
)

ENV_PATH = Path(os.environ.get("PALDO_ENV_PATH") or (Path.home() / ".hermes" / ".env"))
LANES = {
    "PRIMARY": {"location": "Primary Region", "search_term": "local service business", "campaign": "Primary region local services"},
    "SECONDARY": {"location": "Secondary Region", "search_term": "appointment based local service business", "campaign": "Secondary region local services"},
}


def _read_protected_env(path: Path = ENV_PATH) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key != "APIFY_API_TOKEN":
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if value:
            values[key] = value
    return values


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _safe_cost(status: dict) -> float:
    for key in ("usageTotalUsd", "costUsd", "cost"):
        value = status.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
            return float(value)
    usage = status.get("usage")
    if isinstance(usage, dict):
        value = usage.get("totalUsd")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
            return float(value)
    return 0.0


def _select_lane(database: Database, lane: str) -> dict:
    campaign = database.get_campaign_by_name(LANES[lane]["campaign"])
    if campaign is None:
        raise RuntimeError("CAMPAIGN_NOT_FOUND")
    with database.connection:
        database.connection.execute("UPDATE campaigns SET status='INACTIVE'")
        database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (campaign["id"],))
    return campaign


def run_lane(lane: str, output_path: Path, *, timeout_seconds: int = 600, max_items: int | None = None) -> dict:
    lane = lane.upper()
    if lane not in LANES:
        raise ValueError("lane must be PRIMARY or US")
    env = _read_protected_env()
    if not env.get("APIFY_API_TOKEN"):
        raise RuntimeError("APIFY_API_TOKEN_UNAVAILABLE_IN_PROTECTED_ENV")
    database = Database(DEFAULT_DB_PATH)
    client = ApifyClient(env=env, timeout_seconds=15)
    campaign = None
    run = None
    try:
        migrate_step5(database)
        config = database.read_config()
        if config.get("system_state") != "ACTIVE" or config.get("discovery_mode") != "LIVE":
            raise RuntimeError("PRODUCTION_DISCOVERY_NOT_ACTIVE")
        if config.get("apify_actor_id") != "compass~crawler-google-places":
            raise RuntimeError("ALLOWLISTED_ACTOR_MISMATCH")
        if max_items is not None and (isinstance(max_items, bool) or not isinstance(max_items, int) or not 1 <= max_items <= 25):
            raise ValueError("max_items must be between 1 and 25")
        campaign = _select_lane(database, lane)
        run = start_apify_live_run(
            database,
            campaign["id"],
            LANES[lane]["location"],
            LANES[lane]["search_term"],
            country_code="ph" if lane == "PRIMARY" else "us",
            env=env,
            operator_confirmation=True,
            apify_client=client,
            overrides={"maxCrawledPlacesPerSearch": max_items} if max_items is not None else None,
        )
        if not isinstance(run, dict) or not run.get("remote_run_id"):
            raise RuntimeError("APIFY_RUN_RESPONSE_INVALID")
        deadline = time.monotonic() + timeout_seconds
        while run["state"] == ApifyRunState.RUNNING.value:
            if time.monotonic() >= deadline:
                raise RuntimeError("APIFY_POLL_TIMEOUT")
            time.sleep(5)
            run = poll_apify_run(database, run["remote_run_id"], apify_client=client)
        if run["state"] != ApifyRunState.SUCCEEDED.value or not run.get("dataset_id"):
            raise RuntimeError("APIFY_RUN_NOT_SUCCEEDED")
        records = client.fetch_dataset(run["dataset_id"])
        if not isinstance(records, list) or len(records) > 25:
            raise RuntimeError("APIFY_DATASET_BOUNDS_INVALID")
        normalized = ApifyAdapter().normalize_records(records, mode=DiscoveryMode.DRY_RUN, collected_at=_iso_now())
        candidates = []
        for candidate in normalized:
            safe = asdict(candidate)
            safe["lane"] = lane
            safe["source"] = APIFY_SOURCE
            safe["provenance_mode"] = DiscoveryMode.LIVE.value
            safe["provenance_type"] = "LIVE"
            candidates.append(safe)
        status = client.get_run_status(run["remote_run_id"])
        cost = _safe_cost(status if isinstance(status, dict) else {})
        with database.connection:
            database.connection.execute(
                "UPDATE apify_runs SET dataset_fetched_at=?, item_count=?, updated_at=? WHERE id=?",
                (_iso_now(), len(candidates), _iso_now(), run["run_id"]),
            )
        payload = {
            "lane": lane,
            "source": "APIFY",
            "actor_run": {
                "lane": lane,
                "actor_id": run["actor_id"],
                "state": "SUCCEEDED",
                "remote_run_id": run["remote_run_id"],
                "dataset_id": run["dataset_id"],
                "item_count": len(candidates),
                "cost_usd": cost,
            },
            "candidates": candidates,
            "retrieved_at": _iso_now(),
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = output_path.with_suffix(output_path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(temporary, output_path)
        return {"ok": True, "lane": lane, "remote_run_id": run["remote_run_id"], "dataset_id": run["dataset_id"], "item_count": len(candidates), "cost_usd": cost, "output_path": str(output_path)}
    finally:
        with database.connection:
            database.connection.execute("UPDATE campaigns SET status='INACTIVE'")
        database.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("lane", choices=sorted(LANES))
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--max-items", type=int, default=None)
    args = parser.parse_args()
    try:
        result = run_lane(args.lane, args.output, timeout_seconds=args.timeout_seconds, max_items=args.max_items)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except ApifyRequestError as error:
        print(json.dumps({"ok": False, "status": "FAILED", "lane": args.lane, "error_category": error.category, "error_type": error.provider_type, "error_message": error.provider_message, "http_status": error.status_code}, sort_keys=True, separators=(",", ":")))
        return 1
    except (OSError, RuntimeError, ValueError) as error:
        print(json.dumps({"ok": False, "status": "FAILED", "lane": args.lane, "error_category": str(error)}, sort_keys=True, separators=(",", ":")))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
