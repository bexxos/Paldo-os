#!/usr/bin/env python3
"""Paldo OS production boundary for Hermes-native cron orchestration.

Hermes cron performs all external work and writes a bounded, secret-free JSON
handoff. This entrypoint only configures the canonical database or commits that
handoff through step22_integration.run_production_cycle().
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from step22_integration import Step22BlockedError, configure_production_runtime, run_production_cycle


def _set_state(*, paused: bool) -> dict:
    db = Database(DEFAULT_DB_PATH)
    try:
        with db.connection:
            db.connection.execute("UPDATE campaigns SET status='INACTIVE'")
        db.set_config("system_state", "PAUSED" if paused else "ACTIVE")
        if not paused:
            db.set_config("discovery_mode", "LIVE")
        if paused:
            db.set_config("scheduler_enabled", 0)
        db.log_event(
            event_type="PRODUCTION_RUNTIME_PAUSED" if paused else "PRODUCTION_RUNTIME_RESUMED",
            entity_type="production",
            entity_id="paldo-os-runtime",
            metadata={"system_state": "PAUSED" if paused else "ACTIVE", "scheduler_enabled": db.get_config("scheduler_enabled")},
        )
        return {"ok": True, "status": "PAUSED" if paused else "ACTIVE", "scheduler_enabled": db.get_config("scheduler_enabled")}
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Paldo OS production boundary")
    subparsers = parser.add_subparsers(dest="command", required=True)
    configure = subparsers.add_parser("configure")
    configure.add_argument("--scheduler-installed", action="store_true")
    run = subparsers.add_parser("run")
    run.add_argument("--handoff", required=True, type=Path)
    subparsers.add_parser("pause")
    subparsers.add_parser("resume")
    subparsers.add_parser("stop")
    args = parser.parse_args(argv)
    try:
        if args.command == "configure":
            result = configure_production_runtime(scheduler_installed=args.scheduler_installed)
        elif args.command == "run":
            with args.handoff.open("r", encoding="utf-8") as handle:
                handoff = json.load(handle)
            result = run_production_cycle(handoff=handoff)
        elif args.command in {"pause", "stop"}:
            result = _set_state(paused=True)
        else:
            result = _set_state(paused=False)
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        return 0
    except (Step22BlockedError, OSError, ValueError, json.JSONDecodeError) as error:
        print(json.dumps({"ok": False, "status": "FAILED", "error_category": str(error)}, sort_keys=True, separators=(",", ":")))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
