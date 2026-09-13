#!/usr/bin/env python3
"""Bounded Reddit collection for the scheduled Paldo OS poller.

Route: DonSeTch only.

Chronological discovery uses each supported community's official public
``/new/.json`` listing. The listing is requested through Reddit's public JSON
representation wrapped in a JSONP callback so the raw listing (canonical
``permalink``, ``created_utc``, ``author``, ``score``, ``num_comments``,
``selftext``) is returned instead of the adaptor's link-less title digest.

Selected threads are then deep-fetched: the canonical human thread URL plus the
thread's public JSON representation for comment-level text and comment times.

Raw DonSeTch envelopes are written before any triage, so every later decision
stays auditable. No cookies, no login, no mirrors, no other scraper.
"""
from __future__ import annotations

import fcntl
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WORKSPACE = Path(os.environ.get("PALDO_WORKSPACE") or Path(__file__).resolve().parent)
LOCK_PATH = Path(os.environ.get("PALDO_REDDIT_LOCK") or (WORKSPACE / ".reddit-poll.lock"))
DONSETCH = os.environ.get("PALDO_DONSETCH_BIN") or shutil.which("donsetch") or "donsetch"
LOCAL_UTC_OFFSET = os.environ.get("PALDO_LOCAL_UTC_OFFSET") or "+0 hours"

# Broad, durable seed catalog. Owner/operator, agency/consultant,
# appointment-based (local/business/practice/front-desk), CRM/GHL/automation
# and hiring lanes. Not an exclusive boundary; relevance evidence per community
# is retained in the evidence directory.
COMMUNITIES = [
    # automation / CRM / no-code
    "n8n", "CRM", "gohighlevel", "GoHighLevelCRM", "nocode", "automation", "Zapier",
    # agencies / consultants / SMB owners
    "agency", "marketingagency", "DigitalMarketing", "Entrepreneur",
    "EntrepreneurRideAlong", "smallbusiness", "consulting", "msp", "sales",
    # appointment-based / local / business / practice / front desk
    "LocalServices", "Esthetics", "Estheticians", "Barber", "therapists",
    "Dentistry", "Chiropractic", "lashextensions", "massage", "Nailtechs", "spa",
    "SalonOwners",
    # local-service / trades owner-operators (verified live 2026-09-12)
    "cleaningbusiness", "HVAC", "Plumbing", "roofing", "lawncare", "pestcontrol",
    # hiring / freelance
    "forhire", "freelance", "Upwork",
]

FEED_LIMIT = 25
MAX_DEEP = 12
JSONP = "paldopollcb"

HIRING_TERMS = (
    "hiring", "[hiring]", "looking to hire", "need to hire", "we're hiring",
    "want to hire", "paid project", "paid work", "budget of", "my budget",
    "contractor", "freelancer", "freelance", "per project", "offer pay",
    "$/hr", "per hour", "job post", "commission-based", "willing to pay",
)
BUSINESS_TERMS = (
    "no-show", "no show", "noshow", "missed call", "missed inquiry", "missed lead",
    "follow up", "follow-up", "booking", "rebook", "appointment", "pipeline",
    "manual data entry", "double entry", "duplicate data", "shared inbox",
    "client record", "customer history", "crm", "gohighlevel", "go high level",
    "n8n", "zapier", "automation", "automate", "integrat", "webhook",
    "recommend", "anyone know", "need help", "looking for", "how do i",
    "front desk", "receptionist", "client portal", "intake form", "reminder",
)
SELF_PROMO_TERMS = (
    "i build", "i can help", "dm me", "my agency", "we offer", "our services",
    "link in bio", "i specialize", "i'm a developer", "i am a developer",
    "book a call", "free audit", "portfolio", "i made a", "i created",
    "here's how i", "we help businesses", "i run an agency", "i work with businesses",
    "taking on a few new", "i'm currently taking on", "shameless plug",
)
CONSUMER_TERMS = (
    "am i being ripped off", "should i tip", "my botox", "my filler",
    "is it worth it", "experience with a top-rated",
)


def run_cmd(args: list[str], timeout: int = 120) -> dict[str, Any]:
    started = time.monotonic()
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        try:
            parsed = json.loads(p.stdout)
        except Exception:
            parsed = None
        return {
            "argv": args,
            "returncode": p.returncode,
            "elapsed_s": round(time.monotonic() - started, 3),
            "stdout": p.stdout,
            "stderr": p.stderr,
            "json": parsed,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "argv": args, "returncode": None,
            "elapsed_s": round(time.monotonic() - started, 3),
            "stdout": exc.stdout or "", "stderr": exc.stderr or "",
            "json": None, "error": "timeout",
        }


def unwrap_jsonp(content: str) -> Any:
    """Return the JSON object embedded in a JSONP response, else None."""
    if not isinstance(content, str):
        return None
    m = re.match(r"^\s*/\*\*/\s*[A-Za-z0-9_]*\((.*)\)\s*$", content, re.S)
    body = m.group(1) if m else content
    try:
        return json.loads(body)
    except Exception:
        return None


def extract_posts(content: str, community: str) -> list[dict[str, Any]]:
    data = unwrap_jsonp(content)
    if not isinstance(data, dict):
        return []
    children = (((data.get("data") or {}).get("children")) or [])
    posts: list[dict[str, Any]] = []
    for child in children:
        if not isinstance(child, dict):
            continue
        d = child.get("data")
        if not isinstance(d, dict):
            continue
        permalink = d.get("permalink")
        if not isinstance(permalink, str) or "/comments/" not in permalink:
            continue
        created = d.get("created_utc")
        created_iso = None
        if isinstance(created, (int, float)) and created > 0:
            created_iso = datetime.fromtimestamp(float(created), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        posts.append({
            "community": community,
            "id": d.get("id"),
            "url": "https://www.reddit.com" + permalink.rstrip("/") + "/",
            "canonical_url": "https://www.reddit.com" + permalink,
            "title": d.get("title") or "",
            "author": d.get("author") or "",
            "created_utc": created,
            "published_at": created_iso,
            "score": d.get("score"),
            "num_comments": d.get("num_comments"),
            "upvote_ratio": d.get("upvote_ratio"),
            "over_18": d.get("over_18"),
            "link_flair": d.get("link_flair_text"),
            "selftext": (d.get("selftext") or "")[:6000],
            "external_url": d.get("url") if not str(d.get("url") or "").startswith("/r/") else None,
            "is_self": d.get("is_self"),
        })
    return posts


def extract_comments(content: str, limit: int = 60) -> list[dict[str, Any]]:
    data = unwrap_jsonp(content)
    out: list[dict[str, Any]] = []
    if not isinstance(data, list):
        return out
    for listing in data:
        if not isinstance(listing, dict) or listing.get("kind") != "Listing":
            continue
        for child in ((listing.get("data") or {}).get("children") or []):
            if not isinstance(child, dict) or child.get("kind") != "t1":
                continue
            d = child.get("data") or {}
            created = d.get("created_utc")
            created_iso = None
            if isinstance(created, (int, float)) and created > 0:
                created_iso = datetime.fromtimestamp(float(created), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            out.append({
                "id": d.get("id"),
                "author": d.get("author"),
                "body": (d.get("body") or "")[:2500],
                "created_utc": created,
                "published_at": created_iso,
                "score": d.get("score"),
                "permalink": ("https://www.reddit.com" + str(d.get("permalink"))) if d.get("permalink") else None,
            })
            if len(out) >= limit:
                return out
    return out


def age_hours(published_at: str | None, now: datetime) -> float | None:
    if not published_at:
        return None
    try:
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((now - dt).total_seconds() / 3600.0, 2)


def term_hits(text: str, terms: tuple[str, ...]) -> list[str]:
    low = text.casefold()
    return [t for t in terms if t in low]


def pre_score(post: dict[str, Any]) -> dict[str, Any]:
    blob = f"{post.get('title','')} \n {post.get('selftext','')}".casefold()
    hiring = term_hits(blob, HIRING_TERMS)
    business = term_hits(blob, BUSINESS_TERMS)
    promo = term_hits(blob, SELF_PROMO_TERMS)
    consumer = term_hits(blob, CONSUMER_TERMS)
    score = 3 * len(hiring) + 2 * len(business) - 5 * len(promo) - 4 * len(consumer)
    is_link_post = not post.get("is_self")
    if is_link_post and not post.get("selftext"):
        score -= 2  # an image/link drop with no text is rarely actionable
    return {
        "score": score,
        "hiring_terms": hiring,
        "business_terms": business,
        "self_promo_terms": promo,
        "consumer_terms": consumer,
    }


def extract_thread_post(content: str, community_hint: str = "") -> dict[str, Any] | None:
    """Return the post object from a thread's public JSON representation."""
    data = unwrap_jsonp(content)
    if not isinstance(data, list):
        return None
    for listing in data:
        if not isinstance(listing, dict) or listing.get("kind") != "Listing":
            continue
        for child in ((listing.get("data") or {}).get("children") or []):
            if not isinstance(child, dict) or child.get("kind") != "t3":
                continue
            d = child.get("data") or {}
            created = d.get("created_utc")
            created_iso = None
            if isinstance(created, (int, float)) and created > 0:
                created_iso = datetime.fromtimestamp(float(created), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            permalink = d.get("permalink") or ""
            return {
                "community": d.get("subreddit") or community_hint,
                "id": d.get("id"),
                "url": ("https://www.reddit.com" + permalink.rstrip("/") + "/") if permalink else None,
                "canonical_url": ("https://www.reddit.com" + permalink) if permalink else None,
                "title": d.get("title") or "",
                "author": d.get("author") or "",
                "created_utc": created,
                "published_at": created_iso,
                "score": d.get("score"),
                "num_comments": d.get("num_comments"),
                "link_flair": d.get("link_flair_text"),
                "selftext": (d.get("selftext") or "")[:8000],
                "external_url": d.get("url") if not str(d.get("url") or "").startswith("/r/") else None,
                "is_self": d.get("is_self"),
            }
    return None


def deep_fetch_mode(urls_file: str, run_dir: Path, now: datetime, local_time: str) -> int:
    """Bounded retrieval of an explicit set of canonical thread URLs (no feed sweep)."""
    urls = [ln.strip() for ln in Path(urls_file).read_text(encoding="utf-8").splitlines() if ln.strip().startswith("http")]
    urls = list(dict.fromkeys(urls))[:16]
    meta: dict[str, Any] = {
        "schema_version": 2,
        "source": "reddit",
        "route": "DonSeTch only",
        "mode": "bounded-deep-fetch",
        "discovery_route": "explicit canonical thread URLs selected from the prior bounded feed collection",
        "retrieved_at": now.isoformat().replace("+00:00", "Z"),
        "local_time_command_output": local_time,
        "lock": str(LOCK_PATH),
        "requested_urls": urls,
    }
    threads: list[dict[str, Any]] = []
    for idx, url in enumerate(urls, 1):
        canonical = url.split("?")[0].rstrip("/")
        human = run_cmd([DONSETCH, "fetch", canonical, "--json", "--max-chars", "30000", "--deadline-ms", "60000"], 90)
        (run_dir / f"thread_{idx:02d}_human.json").write_text(json.dumps(human, ensure_ascii=False), encoding="utf-8")
        json_url = canonical + f".json?limit=100&sort=new&jsonp={JSONP}"
        comments = run_cmd([DONSETCH, "fetch", json_url, "--json", "--max-chars", "300000", "--deadline-ms", "60000"], 90)
        (run_dir / f"thread_{idx:02d}_comments.json").write_text(json.dumps(comments, ensure_ascii=False), encoding="utf-8")
        cenv = comments.get("json") if isinstance(comments.get("json"), dict) else {}
        ccontent = cenv.get("content") if isinstance(cenv, dict) else None
        post = extract_thread_post(ccontent) if isinstance(ccontent, str) else None
        if post is None:
            post = {"url": canonical, "canonical_url": canonical, "community": None, "title": None,
                    "author": None, "published_at": None, "selftext": "", "note": "canonical JSON representation not recovered"}
        parsed = extract_comments(ccontent) if isinstance(ccontent, str) else []
        post["comments"] = parsed
        post["comments_returned"] = len(parsed)
        post["age_hours"] = age_hours(post.get("published_at"), now)
        post["human_meta"] = {
            "returncode": human.get("returncode"),
            "ok": bool((human.get("json") or {}).get("ok")) if isinstance(human.get("json"), dict) else False,
            "content_kind": ((human.get("json") or {}).get("meta") or {}).get("content_kind") if isinstance(human.get("json"), dict) else None,
        }
        post["json_meta"] = {
            "returncode": comments.get("returncode"),
            "ok": bool(cenv.get("ok")) if isinstance(cenv, dict) else False,
            "error": (cenv.get("error") or {}).get("message") if isinstance(cenv, dict) and isinstance(cenv.get("error"), dict) else None,
        }
        threads.append(post)

    checkpoint = {**meta, "threads": threads, "collection_status": "COMPLETE"}
    (run_dir / "collection_checkpoint.json").write_text(json.dumps(checkpoint, indent=2, ensure_ascii=False), encoding="utf-8")
    summary = {
        "status": "collected-deep",
        "run_dir": str(run_dir),
        "local_time": local_time,
        "threads": [
            {
                "url": t.get("url"), "community": t.get("community"), "title": t.get("title"),
                "author": t.get("author"), "published_at": t.get("published_at"), "age_hours": t.get("age_hours"),
                "num_comments": t.get("num_comments"), "comments_returned": t.get("comments_returned"),
                "selftext": (t.get("selftext") or "")[:1500],
                "comments": t.get("comments", [])[:25],
            }
            for t in threads
        ],
    }
    (run_dir / "candidates.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "threads"}, ensure_ascii=False))
    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--urls-file", default=None,
                        help="Bounded deep-fetch mode: read newline-delimited canonical thread URLs and fetch only those.")
    args = parser.parse_args(argv)

    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[SILENT]")
            return 0

        now = datetime.now(timezone.utc).replace(microsecond=0)
        local_time = subprocess.run(
            ["date", "-u", "-d", LOCAL_UTC_OFFSET, "+%Y-%m-%d %H:%M:%S %z"],
            capture_output=True, text=True,
        ).stdout.strip()
        run_dir = WORKSPACE / "reddit_evidence" / now.strftime("%Y-%m-%dT%H%M%SZ")
        run_dir.mkdir(parents=True, exist_ok=False)

        if args.urls_file:
            return deep_fetch_mode(args.urls_file, run_dir, now, local_time)

        meta: dict[str, Any] = {
            "schema_version": 2,
            "source": "reddit",
            "route": "DonSeTch only",
            "discovery_route": "official /new/.json listing via public Reddit JSON representation (jsonp-wrapped)",
            "retrieved_at": now.isoformat().replace("+00:00", "Z"),
            "local_time_command_output": local_time,
            "lock": str(LOCK_PATH),
            "communities": COMMUNITIES,
            "feed_limit": FEED_LIMIT,
            "max_deep": MAX_DEEP,
        }

        feeds: dict[str, dict[str, Any]] = {}
        all_posts: list[dict[str, Any]] = []
        feed_status: dict[str, Any] = {}
        for community in COMMUNITIES:
            url = f"https://www.reddit.com/r/{community}/new/.json?limit={FEED_LIMIT}&jsonp={JSONP}"
            result = run_cmd([DONSETCH, "fetch", url, "--json", "--max-chars", "400000", "--deadline-ms", "60000"], 90)
            feeds[community] = result
            (run_dir / f"feed_{community}.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
            env = result.get("json") if isinstance(result.get("json"), dict) else {}
            content = env.get("content") if isinstance(env, dict) else None
            posts = extract_posts(content, community) if isinstance(content, str) else []
            all_posts.extend(posts)
            feed_status[community] = {
                "returncode": result.get("returncode"),
                "ok": bool(env.get("ok")) if isinstance(env, dict) else False,
                "content_kind": ((env.get("meta") or {}).get("content_kind") if isinstance(env, dict) else None),
                "posts": len(posts),
                "error": ((env.get("error") or {}).get("message") if isinstance(env, dict) and isinstance(env.get("error"), dict) else None),
            }
        meta["feed_status"] = feed_status

        # Recency gate: verified publication time inside seven days.
        fresh: list[dict[str, Any]] = []
        for post in all_posts:
            age = age_hours(post.get("published_at"), now)
            post["age_hours"] = age
            if age is None:
                post["freshness"] = "DATE_UNVERIFIED"
                continue
            post["freshness"] = "ELIGIBLE" if age <= 168 else "STALE"
            if post["freshness"] == "ELIGIBLE":
                post.update(pre_score(post))
                fresh.append(post)

        ordered = sorted(fresh, key=lambda p: (-int(p.get("score", 0)), float(p.get("age_hours") or 999)))
        selected = [p for p in ordered if p.get("score", 0) >= 4][:MAX_DEEP]
        meta["post_count"] = len(all_posts)
        meta["fresh_count"] = len(fresh)
        meta["selected_count"] = len(selected)

        threads: list[dict[str, Any]] = []
        for idx, post in enumerate(selected, 1):
            human = run_cmd([DONSETCH, "fetch", post["url"], "--json", "--max-chars", "30000", "--deadline-ms", "60000"], 90)
            (run_dir / f"thread_{idx:02d}_human.json").write_text(json.dumps(human, ensure_ascii=False), encoding="utf-8")
            json_url = post["canonical_url"] + f".json?limit=100&sort=new&jsonp={JSONP}"
            comments = run_cmd([DONSETCH, "fetch", json_url, "--json", "--max-chars", "300000", "--deadline-ms", "60000"], 90)
            (run_dir / f"thread_{idx:02d}_comments.json").write_text(json.dumps(comments, ensure_ascii=False), encoding="utf-8")
            cenv = comments.get("json") if isinstance(comments.get("json"), dict) else {}
            ccontent = cenv.get("content") if isinstance(cenv, dict) else None
            parsed = extract_comments(ccontent) if isinstance(ccontent, str) else []
            post["comments"] = parsed
            post["comments_returned"] = len(parsed)
            post["human_meta"] = {
                "returncode": human.get("returncode"),
                "content_kind": ((human.get("json") or {}).get("meta") or {}).get("content_kind") if isinstance(human.get("json"), dict) else None,
                "ok": bool((human.get("json") or {}).get("ok")) if isinstance(human.get("json"), dict) else False,
            }
            threads.append(post)

        checkpoint = {
            **meta,
            "threads": threads,
            "collection_status": "COMPLETE",
        }
        (run_dir / "collection_checkpoint.json").write_text(json.dumps(checkpoint, indent=2, ensure_ascii=False), encoding="utf-8")

        summary = {
            "status": "collected",
            "run_dir": str(run_dir),
            "local_time": local_time,
            "post_count": len(all_posts),
            "fresh_count": len(fresh),
            "selected": [
                {
                    "url": p["url"], "community": p["community"], "author": p["author"],
                    "published_at": p["published_at"], "age_hours": p["age_hours"],
                    "score": p.get("score"), "pre_score": p.get("score"),
                    "num_comments": p.get("num_comments"), "comments_returned": p.get("comments_returned"),
                    "title": p["title"],
                    "hiring_terms": p.get("hiring_terms"), "business_terms": p.get("business_terms"),
                    "self_promo_terms": p.get("self_promo_terms"), "consumer_terms": p.get("consumer_terms"),
                    "selftext": p.get("selftext", "")[:1200],
                }
                for p in selected
            ],
        }
        (run_dir / "candidates.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False))
        return 0
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


if __name__ == "__main__":
    raise SystemExit(main())
