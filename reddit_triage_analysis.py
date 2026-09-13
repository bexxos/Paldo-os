#!/usr/bin/env python3
"""Bounded read-only triage over the raw feed evidence of one collection run.

Reads feed_*.json (official /new/.json listings already retrieved through
DonSeTch), rebuilds the full post set, applies the seven-day recency gate plus a
72-hour priority flag, suppresses canonical URLs already present in the canonical
Paldo DB, and ranks buyer/owner/help signals.

Read-only: no network, no writes to the canonical DB.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

WORKSPACE = Path(os.environ.get("PALDO_WORKSPACE") or Path(__file__).resolve().parent)
PALDO_DB = Path(os.environ.get("PALDO_DB_PATH") or (WORKSPACE / "data" / "paldo_os_outbound.sqlite3"))
RUN = Path(sys.argv[1]) if len(sys.argv) > 1 else None

HIRING = (
    "hiring", "[hiring]", "looking to hire", "need to hire", "we're hiring",
    "want to hire", "paid project", "paid work", "my budget", "budget of",
    "contractor", "freelancer", "per project", "$/hr", "per hour", "willing to pay",
    "looking for someone", "need someone", "looking for a",
)
BUYER_OWNER = (
    "i own", "my business", "my salon", "my agency",
    "my business", "our business", "our business", "our agency", "my practice",
    "we run", "i run", "founder", "owner", "operator", "practice manager",
    "my team", "our team", "my staff", "front desk", "my clients", "our clients",
    "my clients", "our clients", "my customers", "our customers",
)
PROBLEM = (
    "no-show", "no show", "noshow", "missed call", "missed inquiry", "missed lead",
    "missed appointment", "follow up", "follow-up", "followups", "booking",
    "rebook", "rebooking", "appointment", "pipeline", "manual data entry",
    "double entry", "duplicate data", "duplicate entry", "shared inbox",
    "client record", "customer history", "crm", "gohighlevel", "go high level",
    "n8n", "zapier", "make.com", "automation", "automate", "integrat", "webhook",
    "reminder", "confirm", "intake form", "client portal", "lead capture",
    "lead gen", "lost lead", "churn", "retention", "reschedul",
    "booking software", "calendly", "acuity", "mindbody", "vagaro", "jane app",
    "spreadsheet", "excel", "google sheet", "airtable", "notion",
)
ASK = (
    "recommend", "anyone know", "need help", "looking for", "how do i", "how can i",
    "advice", "suggestions", "any tips", "what do you", "should i", "which ",
    "anyone else", "help me", "struggling", "frustrat", "overwhelm",
)
PROMO = (
    "dm me", "my agency", "we offer", "our services", "link in bio", "i specialize",
    "book a call", "free audit", "portfolio", "we help businesses", "i run an agency",
    "taking on a few new", "i'm currently taking on", "shameless plug",
    "for hire", "[for hire]", "hire me", "my services", "i am a ", "i'm a developer",
    "i build", "i can help", "available for", "open for work", "looking for work",
    "here's my", "check out my", "worked with", "my clients", "case study",
)
CONSUMER = (
    "am i being ripped off", "should i tip", "my botox", "my filler", "is it worth it",
    "got my lips", "before and after", "should i get",
)
INDUSTRY = (
    "local_services", "local service business", "local service", "esthetic", "salon", "spa", "business",
    "wellness", "local", "cosmetic", "dental", "dentist", "chiropract",
    "massage", "therapy", "therapist", "practice", "client", "client",
    "agency", "consult", "service business", "local business",
)


def unwrap(content: str):
    if not isinstance(content, str):
        return None
    m = re.match(r"^\s*/\*\*/\s*[A-Za-z0-9_]*\((.*)\)\s*$", content, re.S)
    body = m.group(1) if m else content
    try:
        return json.loads(body)
    except Exception:
        return None


def load_posts(run: Path) -> list[dict]:
    posts: list[dict] = []
    for path in sorted(run.glob("feed_*.json")):
        community = path.stem[len("feed_"):]
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        env = payload.get("json") if isinstance(payload, dict) else None
        content = env.get("content") if isinstance(env, dict) else None
        data = unwrap(content)
        if not isinstance(data, dict):
            continue
        for child in ((data.get("data") or {}).get("children") or []):
            d = child.get("data") if isinstance(child, dict) else None
            if not isinstance(d, dict):
                continue
            permalink = d.get("permalink")
            if not isinstance(permalink, str) or "/comments/" not in permalink:
                continue
            created = d.get("created_utc")
            if not isinstance(created, (int, float)) or created <= 0:
                continue
            posts.append({
                "community": community,
                "id": d.get("id"),
                "canonical_url": "https://www.reddit.com" + permalink,
                "url": "https://www.reddit.com" + permalink.rstrip("/") + "/",
                "title": d.get("title") or "",
                "author": d.get("author") or "",
                "created_utc": created,
                "published_at": datetime.fromtimestamp(float(created), tz=timezone.utc)
                    .replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "score": d.get("score"),
                "num_comments": d.get("num_comments"),
                "selftext": d.get("selftext") or "",
                "link_flair": d.get("link_flair_text"),
                "is_self": d.get("is_self"),
                "over_18": d.get("over_18"),
            })
    return posts


def hits(text: str, terms) -> list[str]:
    low = text.casefold()
    return [t for t in terms if t in low]


def main() -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    posts = load_posts(RUN)
    db = sqlite3.connect(PALDO_DB)
    db.row_factory = sqlite3.Row
    known = {str(r["original_url"]).rstrip("/") for r in
             db.execute("SELECT original_url FROM social_opportunities WHERE source='reddit'")}
    db.close()

    rows = []
    for p in posts:
        age = (now - datetime.fromtimestamp(p["created_utc"], tz=timezone.utc)).total_seconds() / 3600.0
        p["age_hours"] = round(age, 2)
        if age > 168:
            continue
        blob = f"{p['title']}\n{p['selftext']}".casefold()
        h_hiring = hits(blob, HIRING)
        h_buyer = hits(blob, BUYER_OWNER)
        h_problem = hits(blob, PROBLEM)
        h_ask = hits(blob, ASK)
        h_promo = hits(blob, PROMO)
        h_consumer = hits(blob, CONSUMER)
        h_industry = hits(blob, INDUSTRY)
        p.update(hiring=h_hiring, buyer=h_buyer, problem=h_problem, ask=h_ask,
                 promo=h_promo, consumer=h_consumer, industry=h_industry)
        # rank: interest - promo/consumer
        p["interest"] = (3 * len(h_hiring) + 3 * len(h_buyer) + 2 * len(h_problem)
                         + 2 * len(h_ask) + 2 * len(h_industry))
        p["penalty"] = 6 * len(h_promo) + 5 * len(h_consumer)
        p["rank_score"] = p["interest"] - p["penalty"]
        p["duplicate"] = p["canonical_url"].rstrip("/") in known
        p["within_72h"] = age <= 72
        rows.append(p)

    rows.sort(key=lambda r: (-r["rank_score"], r["age_hours"]))
    out = {
        "run": str(RUN),
        "now_utc": now.isoformat().replace("+00:00", "Z"),
        "total_posts": len(posts),
        "fresh_posts": len(rows),
        "already_known": sum(1 for r in rows if r["duplicate"]),
        "within_72h": sum(1 for r in rows if r["within_72h"]),
        "top": rows[:80],
    }
    (RUN / "triage_analysis.json").write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"total={out['total_posts']} fresh={out['fresh_posts']} known={out['already_known']} within72={out['within_72h']}")
    for r in rows[:60]:
        flag = "D" if r["duplicate"] else " "
        print(f"{flag} rank={r['rank_score']:>3} age={r['age_hours']:>6}h {r['community']:<24} c={r['num_comments']:<4} {r['title'][:95]}")
        print(f"      promo={r['promo']} consumer={r['consumer']} hiring={r['hiring'][:3]} industry={r['industry'][:4]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
