#!/usr/bin/env python3
"""Bounded Reddit collection for the scheduled Paldo OS poller.

This collector deliberately uses only the installed DonSeTch CLI for Reddit
search/fetch. It writes raw envelopes before any triage or handoff is done.
"""
from __future__ import annotations

import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
from typing import Any

WORKSPACE = Path(os.environ.get("PALDO_WORKSPACE") or Path(__file__).resolve().parent)
LOCK_PATH = Path(os.environ.get("PALDO_REDDIT_LOCK") or (WORKSPACE / ".reddit-poll.lock"))
DONSETCH = os.environ.get("PALDO_DONSETCH_BIN") or shutil.which("donsetch") or "donsetch"
LOCAL_UTC_OFFSET = os.environ.get("PALDO_LOCAL_UTC_OFFSET") or "+0 hours"
COMMUNITIES = [
    ("n8n", "https://www.reddit.com/r/n8n/new/.json?limit=100"),
    ("CRM", "https://www.reddit.com/r/CRM/new/.json?limit=100"),
    ("smallbusiness", "https://www.reddit.com/r/smallbusiness/new/.json?limit=100"),
    ("Entrepreneur", "https://www.reddit.com/r/Entrepreneur/new/.json?limit=100"),
    ("EntrepreneurRideAlong", "https://www.reddit.com/r/EntrepreneurRideAlong/new/.json?limit=100"),
    ("agency", "https://www.reddit.com/r/agency/new/.json?limit=100"),
    ("marketingagency", "https://www.reddit.com/r/marketingagency/new/.json?limit=100"),
    ("DigitalMarketing", "https://www.reddit.com/r/DigitalMarketing/new/.json?limit=100"),
    ("GoHighLevelCRM", "https://www.reddit.com/r/GoHighLevelCRM/new/.json?limit=100"),
    ("gohighlevel", "https://www.reddit.com/r/gohighlevel/new/.json?limit=100"),
    ("nocode", "https://www.reddit.com/r/nocode/new/.json?limit=100"),
    ("automation", "https://www.reddit.com/r/automation/new/.json?limit=100"),
    ("LocalServices", "https://www.reddit.com/r/LocalServices/new/.json?limit=100"),
    ("Esthetics", "https://www.reddit.com/r/Esthetics/new/.json?limit=100"),
]
SEARCHES = [
    'site:reddit.com/r/n8n (hiring OR "looking for" OR "need help" OR recommendation OR integration) after:2026-09-02',
    'site:reddit.com/r/CRM (manual OR sync OR "follow-up" OR recommendation OR integration OR automation) after:2026-09-02',
    'site:reddit.com/r/smallbusiness (automation OR CRM OR booking OR leads OR "follow-up" OR no-shows) after:2026-09-02',
    'site:reddit.com/r/Entrepreneur (owner OR founder OR business OR hiring OR automation OR CRM OR booking) after:2026-09-02',
    'site:reddit.com/r/EntrepreneurRideAlong (leads OR repetitive OR operations OR automation OR booking OR CRM) after:2026-09-02',
    'site:reddit.com/r/agency (client acquisition OR CRM OR lead generation OR automation OR operations) after:2026-09-02',
    'site:reddit.com/r/marketingagency (client portal OR lead follow-up OR CRM OR automation OR operations) after:2026-09-02',
    'site:reddit.com/r/DigitalMarketing (CRM OR leads OR booking OR automation OR campaign operations) after:2026-09-02',
    'site:reddit.com/r/GoHighLevelCRM (shared inbox OR follow-up OR pipeline OR reports OR integration) after:2026-09-02',
    'site:reddit.com/r/gohighlevel (missed calls OR lead tracking OR booking OR pipeline OR n8n) after:2026-09-02',
    'site:reddit.com/r/nocode (integration OR webhook OR CRM OR internal tool OR automation) after:2026-09-02',
    'site:reddit.com/r/automation (SMB OR business OR workflow OR repetitive OR AI OR integration) after:2026-09-02',
    'site:reddit.com/r/LocalServices (owner OR booking OR marketing OR no-shows OR operations OR hiring) after:2026-09-02',
    'site:reddit.com/r/Esthetics (booking OR clients OR salon OR owner OR marketing OR operations) after:2026-09-02',
]


def run_cmd(args: list[str], timeout: int = 100) -> dict[str, Any]:
    started = time.monotonic()
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        stdout = p.stdout
        stderr = p.stderr
        try:
            parsed = json.loads(stdout)
        except Exception:
            parsed = None
        return {
            "argv": args,
            "returncode": p.returncode,
            "elapsed_s": round(time.monotonic() - started, 3),
            "stdout": stdout,
            "stderr": stderr,
            "json": parsed,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "argv": args,
            "returncode": None,
            "elapsed_s": round(time.monotonic() - started, 3),
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
            "json": None,
            "error": "timeout",
        }


def walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def reddit_urls(value: Any) -> list[str]:
    found: list[str] = []
    for item in walk(value):
        if isinstance(item, dict):
            for key in ("url", "permalink", "source_url", "original_url"):
                val = item.get(key)
                if isinstance(val, str) and "reddit.com/r/" in val and "/comments/" in val:
                    u = val.split("?")[0].rstrip("/") + "/"
                    if u not in found:
                        found.append(u)
    text = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
    for match in re.findall(r"https?://(?:www\.|old\.|amp\.)?reddit\.com/r/[^\s\"<>]+/comments/[^\s\"<>]+", text):
        u = match.rstrip(".,;)]")
        u = u.split("?")[0].rstrip("/") + "/"
        if u not in found:
            found.append(u)
    return found


def result_rows(envelope: Any) -> list[dict[str, Any]]:
    if not isinstance(envelope, dict):
        return []
    for key in ("results",):
        rows = envelope.get(key)
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]
    structured = envelope.get("structuredContent")
    if isinstance(structured, dict) and isinstance(structured.get("results"), list):
        return [x for x in structured["results"] if isinstance(x, dict)]
    meta = envelope.get("meta")
    if isinstance(meta, dict) and isinstance(meta.get("results"), list):
        return [x for x in meta["results"] if isinstance(x, dict)]
    return []


def feed_titles(envelope: Any) -> list[dict[str, str]]:
    if not isinstance(envelope, dict):
        return []
    content = envelope.get("content")
    if not isinstance(content, str):
        return []
    rows: list[dict[str, str]] = []
    pattern = re.compile(r"^\d+\. \*\*(.*?)\*\* \((?:self\.)?([^)]*)\) · ([^·]+) · (u/[^·]+) · ([^·]+) · ([^·]+)", re.M)
    for match in pattern.finditer(content):
        rows.append({
            "title": match.group(1).strip(),
            "community": match.group(2).strip(),
            "score": match.group(3).strip(),
            "author": match.group(4).strip(),
            "age": match.group(5).strip(),
            "comments": match.group(6).strip(),
        })
    return rows


def title_of(row: dict[str, Any]) -> str:
    return str(row.get("title") or row.get("name") or "")


def relevance(text: str) -> int:
    low = text.casefold()
    terms = {
        "hiring": 7, "looking for": 6, "need help": 5, "recommend": 4,
        "manual": 4, "sync": 5, "integration": 5, "automation": 4,
        "crm": 4, "lead": 3, "booking": 3, "follow-up": 3, "workflow": 3,
        "business": 2, "data": 2, "no-show": 4, "quote": 3,
        "owner": 3, "founder": 2, "business": 2, "salon": 2, "agency": 2,
        "appointment": 3, "missed call": 4, "shared inbox": 4, "pipeline": 3,
    }
    score = sum(weight for term, weight in terms.items() if term in low)
    if "for hire" in low or "job seeker" in low or "portfolio" in low:
        score -= 5
    if "free workflow" in low or "built a" in low or "here's the exact" in low:
        score -= 3
    return score


def envelope_meta(env: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(env, dict):
        return {"content_ok": False, "thin": True, "title": None, "via": None, "status": None, "content_chars": 0}
    sc = env.get("structuredContent") if isinstance(env.get("structuredContent"), dict) else env.get("meta") if isinstance(env.get("meta"), dict) else {}
    content = env.get("content")
    if isinstance(content, list):
        text = "\n".join(str(x.get("text", "")) if isinstance(x, dict) else str(x) for x in content)
    else:
        text = str(content or "")
    return {
        "content_ok": sc.get("content_ok", bool(text.strip())),
        "thin": sc.get("thin", len(text.strip()) < 500),
        "title": sc.get("title") or env.get("title"),
        "via": sc.get("via"),
        "status": sc.get("status") or env.get("status"),
        "content_kind": sc.get("content_kind"),
        "quality": sc.get("quality"),
        "content_chars": len(text),
        "text_preview": text[:700].replace("\n", " "),
    }


def main() -> int:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[SILENT]")
            return 0
        now = datetime.now(timezone.utc).replace(microsecond=0)
        local_cmd = ["date", "-u", "-d", LOCAL_UTC_OFFSET, "+%Y-%m-%d %H:%M:%S %z"]
        local_time = subprocess.run(local_cmd, capture_output=True, text=True).stdout.strip()
        run_dir = WORKSPACE / "reddit_evidence" / now.strftime("%Y-%m-%dT%H%M%SZ")
        run_dir.mkdir(parents=True, exist_ok=False)
        meta = {
            "schema_version": 2,
            "source": "reddit",
            "retrieved_at": now.isoformat().replace("+00:00", "Z"),
            "local_time_command_output": local_time,
            "agent_reach": {
                "path_lookup": shutil.which("agent-reach") or shutil.which("agent_reach") or shutil.which("reach"),
                "explicit_path_exists": os.path.exists("/opt/data/bin/agent-reach"),
                "route": "DonSeTch only",
            },
            "lock": str(LOCK_PATH),
            "fresh_feeds": [url for _, url in COMMUNITIES],
            "search_queries": SEARCHES,
        }
        (run_dir / "run_metadata.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

        feeds: dict[str, dict[str, Any]] = {}
        for community, url in COMMUNITIES:
            result = run_cmd([DONSETCH, "fetch", url, "--json", "--max-chars", "30000", "--deadline-ms", "90000"], 105)
            feeds[community] = result
            (run_dir / f"feed_{community}.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

        dynamic_searches: list[str] = []
        for community, result in feeds.items():
            if len(dynamic_searches) >= 24:
                break
            env = result.get("json")
            titles = sorted(feed_titles(env), key=lambda row: -relevance(row["title"]))
            for row in titles:
                if len(dynamic_searches) >= 24:
                    break
                if relevance(row["title"]) < 4:
                    continue
                for query in (
                    f'site:reddit.com/r/{community} "{row["title"]}"',
                    f'Reddit "{row["title"]}"',
                ):
                    if query not in dynamic_searches:
                        dynamic_searches.append(query)
                    if len(dynamic_searches) >= 24:
                        break
                # Keep the pass bounded while giving each supported community
                # a chance to contribute at most two dynamic queries.
                if len([q for q in dynamic_searches if f"/{community} " in q or (community == "CRM" and "/CRM" in q) or (community == "smallbusiness" and "/smallbusiness" in q)]) >= 2:
                    break
        actual_searches = SEARCHES + dynamic_searches
        meta["search_queries"] = actual_searches
        (run_dir / "run_metadata.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

        searches: list[dict[str, Any]] = []
        for idx, query in enumerate(actual_searches, 1):
            result = run_cmd([DONSETCH, "search", query, "--json", "--max-results", "10", "--deadline-ms", "90000"], 105)
            result["query"] = query
            searches.append(result)
            (run_dir / f"search_{idx:02d}.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

        candidates: dict[str, dict[str, Any]] = {}
        for community, result in feeds.items():
            env = result.get("json")
            for url in reddit_urls(env):
                candidates.setdefault(url, {"url": url, "community": community, "discovery": [], "rank": 0})
            raw = ""
            if isinstance(env, dict):
                content = env.get("content")
                raw = json.dumps(content, ensure_ascii=False) if content is not None else ""
            for line in raw.splitlines():
                if "**" in line:
                    score = relevance(line)
                    if score > 0:
                        for url in reddit_urls(line):
                            candidates.setdefault(url, {"url": url, "community": community, "discovery": [], "rank": 0})
                            candidates[url]["rank"] += score
                            candidates[url]["discovery"].append({"feed_line": line[:700]})
        for result in searches:
            env = result.get("json")
            rows = result_rows(env)
            for row in rows:
                url = next(iter(reddit_urls(row)), None)
                if not url:
                    continue
                text = " ".join(str(row.get(k) or "") for k in ("title", "snippet", "description"))
                score = relevance(text)
                candidates.setdefault(url, {"url": url, "community": "search", "discovery": [], "rank": 0})
                candidates[url]["rank"] += score
                candidates[url]["discovery"].append({"query": result.get("query"), "title": title_of(row), "snippet": row.get("snippet"), "score": row.get("score"), "date": row.get("date")})
        ordered = sorted(candidates.values(), key=lambda x: (-int(x.get("rank", 0)), x["url"]))
        # Keep the bounded fetch set focused; all raw discovery remains saved.
        selected = ordered[:16]
        thread_index: list[dict[str, Any]] = []
        for idx, item in enumerate(selected, 1):
            url = item["url"]
            human = run_cmd([DONSETCH, "fetch", url, "--json", "--max-chars", "30000", "--deadline-ms", "90000"], 105)
            (run_dir / f"thread_{idx:02d}_human.json").write_text(json.dumps(human, indent=2, ensure_ascii=False), encoding="utf-8")
            hm = envelope_meta(human.get("json"))
            fallback = None
            # Reddit JSON is the permitted recovery path for thin/blocked human pages.
            if hm.get("thin") or not hm.get("content_ok") or hm.get("content_kind") not in {"Forum", "Article"} or hm.get("content_chars", 0) < 900:
                json_url = url.rstrip("/") + "/.json"
                fallback = run_cmd([DONSETCH, "fetch", json_url, "--json", "--max-chars", "40000", "--deadline-ms", "90000"], 105)
                (run_dir / f"thread_{idx:02d}_json.json").write_text(json.dumps(fallback, indent=2, ensure_ascii=False), encoding="utf-8")
            thread_index.append({
                "url": url,
                "community": item.get("community"),
                "rank": item.get("rank"),
                "discovery": item.get("discovery", [])[:10],
                "human": envelope_meta(human.get("json")),
                "json_fallback": envelope_meta(fallback.get("json")) if fallback else None,
            })
        checkpoint = {
            **meta,
            "feed_meta": {community: envelope_meta(result.get("json")) for community, result in feeds.items()},
            "search_count": len(searches),
            "candidate_count": len(ordered),
            "selected_thread_count": len(selected),
            "threads": thread_index,
            "collection_status": "COMPLETE",
        }
        (run_dir / "collection_checkpoint.json").write_text(json.dumps(checkpoint, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps({"status": "collected", "run_dir": str(run_dir), "local_time": local_time, "candidate_count": len(ordered), "selected_thread_count": len(selected), "threads": thread_index}, ensure_ascii=False))
        return 0
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


if __name__ == "__main__":
    raise SystemExit(main())
