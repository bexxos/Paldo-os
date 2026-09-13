#!/usr/bin/env python3
"""Bounded in-subreddit Reddit search pass via the public JSON representation.

Route: DonSeTch only. Uses Reddit's own search listing
``/r/SUB/search.json?q=...&restrict_sr=on&sort=new&t=week`` wrapped in a JSONP
callback so the raw listing (permalink, created_utc, score, num_comments,
selftext) is returned instead of a link-less digest.

The purpose is coverage, not volume: the /new/ sweep only sees each
community's 25 newest posts, so a capped set of buyer-intent queries recovers
older-but-still-fresh items in the high-volume communities.

Read-only. No cookies, no login, no mirrors, no other scraper.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

DONSETCH = os.environ.get("PALDO_DONSETCH_BIN") or shutil.which("donsetch") or "donsetch"
JSONP = "paldosearchcb"

# (community, query) pairs. Buyer/help-seeking language for the primary
# appointment-based lane plus the broader qualified lane.
QUERIES: list[tuple[str, str]] = [
    ("LocalServices", '"no show" OR "no-shows" OR reminder OR booking'),
    ("LocalServices", "front desk OR operations OR hiring"),
    ("Esthetics", '"no show" OR reminder OR rebooking OR booking'),
    ("Estheticians", '"no show" OR reminder OR rebooking OR booking'),
    ("gohighlevel", '"missed call" OR "follow up" OR setup OR "looking for"'),
    ("GoHighLevelCRM", "help OR setup OR integration OR booking"),
    ("CRM", 'help OR setup OR migration OR "follow up"'),
    ("smallbusiness", '"no-show" OR reminder OR booking OR "lead follow"'),
    ("agency", "CRM OR automation OR operations OR hiring"),
    ("marketingagency", '"client portal" OR "lead follow-up" OR CRM OR automation'),
    ("forhire", "n8n OR GoHighLevel OR CRM OR automation"),
    ("Dentistry", '"front desk" OR recall OR reminder OR "no show"'),
    ("Chiropractic", 'booking OR reminder OR "no show" OR front desk'),
    ("local", 'booking OR "no show" OR reminder OR clients'),
]


def unwrap(content: str):
    if not isinstance(content, str):
        return None
    m = re.match(r"^\s*/\*\*/\s*[A-Za-z0-9_]*\((.*)\)\s*$", content, re.S)
    body = m.group(1) if m else content
    try:
        return json.loads(body)
    except Exception:
        return None


def run(url: str) -> dict:
    started = time.monotonic()
    try:
        p = subprocess.run(
            [DONSETCH, "fetch", url, "--json", "--max-chars", "400000", "--deadline-ms", "60000"],
            capture_output=True, text=True, timeout=90,
        )
        try:
            parsed = json.loads(p.stdout)
        except Exception:
            parsed = None
        return {"returncode": p.returncode, "json": parsed, "elapsed_s": round(time.monotonic() - started, 2)}
    except subprocess.TimeoutExpired:
        return {"returncode": None, "json": None, "error": "timeout", "elapsed_s": round(time.monotonic() - started, 2)}


def main() -> int:
    run_dir = Path(sys.argv[1])
    now = datetime.now(timezone.utc).replace(microsecond=0)
    posts: dict[str, dict] = {}
    status: list[dict] = []
    for idx, (community, query) in enumerate(QUERIES, 1):
        url = (
            f"https://www.reddit.com/r/{community}/search.json?q={quote_plus(query)}"
            f"&restrict_sr=on&sort=new&t=week&limit=25&jsonp={JSONP}"
        )
        result = run(url)
        (run_dir / f"search_{idx:02d}_{community}.json").write_text(
            json.dumps(result, ensure_ascii=False), encoding="utf-8")
        env = result.get("json") if isinstance(result.get("json"), dict) else {}
        content = env.get("content") if isinstance(env, dict) else None
        data = unwrap(content) if isinstance(content, str) else None
        found = 0
        if isinstance(data, dict):
            for child in ((data.get("data") or {}).get("children") or []):
                d = child.get("data") if isinstance(child, dict) else None
                if not isinstance(d, dict):
                    continue
                permalink = d.get("permalink")
                created = d.get("created_utc")
                if not isinstance(permalink, str) or "/comments/" not in permalink:
                    continue
                if not isinstance(created, (int, float)) or created <= 0:
                    continue
                url_c = "https://www.reddit.com" + permalink
                found += 1
                posts[url_c.rstrip("/")] = {
                    "community": d.get("subreddit") or community,
                    "query_community": community,
                    "query": query,
                    "canonical_url": url_c,
                    "title": d.get("title") or "",
                    "author": d.get("author") or "",
                    "created_utc": created,
                    "published_at": datetime.fromtimestamp(float(created), tz=timezone.utc)
                        .replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    "score": d.get("score"),
                    "num_comments": d.get("num_comments"),
                    "selftext": (d.get("selftext") or "")[:6000],
                    "link_flair": d.get("link_flair_text"),
                    "is_self": d.get("is_self"),
                }
        status.append({"community": community, "query": query, "returncode": result.get("returncode"),
                       "ok": bool(env.get("ok")) if isinstance(env, dict) else False, "found": found,
                       "error": (env.get("error") or {}).get("message") if isinstance(env, dict) and isinstance(env.get("error"), dict) else None})
    out = {
        "retrieved_at": now.isoformat().replace("+00:00", "Z"),
        "route": "DonSeTch only, public Reddit search JSON representation (jsonp-wrapped)",
        "queries": len(QUERIES),
        "unique_posts": len(posts),
        "status": status,
        "posts": list(posts.values()),
    }
    (run_dir / "search_pass.json").write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"queries": len(QUERIES), "unique_posts": len(posts),
                      "failed": [s for s in status if not s["ok"]]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
