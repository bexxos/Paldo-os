#!/usr/bin/env python3
"""Offline, credential-free demo of the bounded outbound pipeline.

This script exists so that a newcomer can watch the pipeline work in seconds
without any credential, network access, or canonical database. It runs one
fictional candidate through discovery, dedupe, enrichment, qualification,
drafting, and console notification, and prints what each stage received and
decided.

Everything is injected fixture code:

* ``FixtureHTTPClient``  - canned fixture pages, no sockets.
* ``FixtureDNSResolver`` - fixed answers, no resolver.
* ``FixtureDraftingProvider`` (from ``step10_drafting``) - returns one
  pre-written fictional draft, no language model.
* ``notifier.ConsoleBackend`` - prints the operator card to stdout.

The run writes only inside a temporary directory that is deleted on exit, and
passes an explicit non-canonical database path. Nothing is sent, queued, or
posted anywhere: the pipeline stops at human review by design.

The production handoff format used by
``python3 paldo_production_entrypoint.py run --handoff FILE`` is a different
input contract; see ``examples/handoff.example.json``.
"""

from __future__ import annotations

import hashlib
import socket
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from notifier import (  # noqa: E402
    NOTIFICATION_CHANNELS,
    ConsoleBackend,
    Notifier,
    migrate_notifier,
    queue_notification,
)
from paldo_os_outbound import DEFAULT_DB_PATH, Database  # noqa: E402
from step6_website_enrichment import WebsiteHTTPResponse, migrate_step6  # noqa: E402
from step7_pipeline import migrate_step7, run_fictional_pipeline_fixture  # noqa: E402
from step8_knowledge_context import build_context_packet  # noqa: E402
from step9_audit_offer import (  # noqa: E402
    IMMEDIATE_OFFER_CTA,
    activate_immediate_offer,
    approve_immediate_offer,
    get_immediate_offer_version,
)
from step10_drafting import (  # noqa: E402
    FixtureDraftingProvider,
    generate_fictional_draft,
    migrate_step10,
    preview_bounded_drafting_input,
)

FIXTURE_WEBSITE = "https://fictional-business.test"
FIXTURE_CAMPAIGN = "Secondary region local services"
DRAFTING_DAILY_CAP = 5
NOTIFICATION_DAILY_CAP = 5


# --------------------------------------------------------------------------
# Injected fixture clients. None of them can reach the network.
# --------------------------------------------------------------------------
class FixtureHTTPClient:
    """Serves canned responses from a fixed route table."""

    def __init__(self, routes):
        self.routes = dict(routes)
        self.calls = []

    def fetch(self, url, *, timeout_seconds, max_redirects):
        self.calls.append(url)
        response = self.routes.get(url)
        if response is None:
            return WebsiteHTTPResponse(
                requested_url=url,
                final_url=url,
                status_code=404,
                headers={"content-type": "text/plain"},
                body=b"not found",
            )
        return response


class FixtureDNSResolver:
    """Answers from a fixed table and never contacts a resolver."""

    def __init__(self, address="93.184.216.34"):
        self.address = address
        self.calls = []

    def resolve(self, host):
        self.calls.append(host)
        return [self.address]

    def lookup_mx(self, domain):
        return ["mail." + domain]


class NetworkGuard:
    """Fails loudly if any code tries to open a socket during the demo."""

    def __enter__(self):
        self._socket = socket.socket
        self._create_connection = socket.create_connection

        def blocked(*args, **kwargs):
            raise RuntimeError("network access is blocked inside the offline demo")

        socket.socket = blocked
        socket.create_connection = blocked
        return self

    def __exit__(self, *exc_info):
        socket.socket = self._socket
        socket.create_connection = self._create_connection
        return False


def html_response(url, body):
    return WebsiteHTTPResponse(
        requested_url=url,
        final_url=url,
        status_code=200,
        headers={
            "content-type": "text/html; charset=utf-8",
            "content-length": str(len(body.encode("utf-8"))),
        },
        body=body.encode("utf-8"),
    )


def fixture_site():
    """Six canned pages for a fictional appointment business."""
    root = FIXTURE_WEBSITE
    return {
        root + "/robots.txt": WebsiteHTTPResponse(
            requested_url=root + "/robots.txt",
            final_url=root + "/robots.txt",
            status_code=404,
            headers={"content-type": "text/plain"},
            body=b"not found",
        ),
        root + "/": html_response(
            root + "/",
            "<html><body><h1>Fictional Business</h1>"
            "<p>Appointments: email "
            "<a href='mailto:appointments@fictional-business.test'>"
            "appointments@fictional-business.test</a>.</p>"
            "<p>Call 555-010-2000 for booking.</p>"
            "<a href='/contact'>Contact</a>"
            "<a href='/book'>Book an appointment</a>"
            "<a href='/services'>Services</a>"
            "<a href='/team'>Our team</a>"
            "</body></html>",
        ),
        root + "/book": html_response(
            root + "/book",
            "<html><body><h1>Book an appointment</h1>"
            "<form action='/book' method='post'><input name='preferred_time'></form>"
            "<p>Cancellation and rescheduling are handled by reception.</p>"
            "<p>A deposit is required. No-show appointments may be charged.</p>"
            "<p>Appointment reminders are sent and post-visit follow-up is offered.</p>"
            "</body></html>",
        ),
        root + "/contact": html_response(
            root + "/contact",
            "<html><body><h1>Contact</h1>"
            "<p>General office: info@fictional-business.test</p>"
            "<p>Phone booking is available.</p></body></html>",
        ),
        root + "/services": html_response(
            root + "/services",
            "<html><body><h1>Services</h1>"
            "<p>Facials, laser treatments, and injectables are offered.</p>"
            "</body></html>",
        ),
        root + "/team": html_response(
            root + "/team",
            "<html><body><h1>Meet our practitioners</h1>"
            "<p>Two practitioners welcome clients.</p></body></html>",
        ),
    }


# --------------------------------------------------------------------------
# Fictional input records and evidence
# --------------------------------------------------------------------------
def source_record(source_record_id, *, business_status="OPERATIONAL"):
    return {
        "source_record_id": source_record_id,
        "business_name": "Fictional Business " + source_record_id,
        "business_category": "Local service business",
        "website_url": FIXTURE_WEBSITE,
        "public_business_email": "office@fictional-business.test",
        "public_business_phone": "+1 555 010 2000",
        "business_status": business_status,
        "country": "US",
        "city": "Testville",
    }


def now_iso(value):
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def fixture_evidence(observed_at):
    """Fictional public evidence for the accepted candidate."""
    items = (
        ("campaign_fit", "FIXTURE_PUBLIC_WEB", 0.95, "The fictional public listing is an appointment-based local service business."),
        ("appointment_dependence", "FIXTURE_PUBLIC_WEB", 0.92, "The fictional site asks visitors to book an appointment."),
        ("recent_activity_demand", "FIXTURE_PUBLIC_WEB", 0.88, "The fictional site states that appointments are available this week."),
        ("multiple_practitioners", "FIXTURE_PUBLIC_WEB", 0.90, "The fictional team page lists two practitioners."),
        ("multiple_services", "FIXTURE_PUBLIC_WEB", 0.90, "The fictional services page lists several treatments."),
        ("owner_decision_maker_reachability", "FIXTURE_PUBLIC_WEB", 0.85, "The fictional site names an on-site owner-operator."),
        ("personalization_observation", "FIXTURE_PUBLIC_WEB", 0.94, "The fictional contact page says booking inquiries arrive by phone and email."),
        ("public_business_email", "FIXTURE_VERIFIED_EMAIL", 0.99, "The fictional public business contact route is verified for this fixture."),
    )
    return [
        {
            "signal_key": key,
            "signal_value": "YES",
            "observed_or_inferred": "OBSERVED",
            "source_type": source_type,
            "source_url": FIXTURE_WEBSITE + "/contact",
            "observation": observation,
            "confidence": confidence,
            "observed_at": now_iso(observed_at),
        }
        for key, source_type, confidence, observation in items
    ]


def all_gates():
    return {
        "currently_active": "YES",
        "appointments_meaningful": "YES",
        "legitimate_public_contact_route": "YES",
        "independent_owner_led_or_accessible_local_decision_maker": "YES",
    }


def gold_record(section, content, index, revision, retrieved_at):
    return {
        "source_id": "demo-gold-%d" % index,
        "status": "Gold",
        "trust_tier": "Gold",
        "approval_status": "APPROVED",
        "approved_by": "OPERATOR",
        "confidence": 0.96,
        "heading": section,
        "drive_file_id": "demo-drive-%d" % index,
        "kb_path": "gold/demo/%d" % index,
        "source_url": "https://fictional-kb.invalid/demo/%d" % index,
        "modified_at": now_iso(retrieved_at - timedelta(days=1)),
        "snapshot_revision": revision,
        "manifest_revision": revision,
        "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "retrieved_at": now_iso(retrieved_at),
        "section": section,
        "content": content,
    }


def gold_records(campaign_name, revision, retrieved_at):
    """Six fictional Gold sections, shaped like recalled Knowledge Base files."""
    contents = (
        ("ICP", "Owner-led appointment businesses, starting with fictional regional local services."),
        ("ACTIVE_OFFER_CTA", "Active offer: Business Booking and Follow-Up System. Approved CTA: %s" % IMMEDIATE_OFFER_CTA),
        ("VOICE_GUIDE", "Use clear, calm, specific language. Be professional, natural, and non-pressuring."),
        ("APPROVED_PROOF_RULES", "Use only current, attributable observations, and separate them from cautious inference."),
        (
            "OUTBOUND_COMPLIANCE_POLICY",
            "Do not claim guaranteed results, savings, revenue, or reduced no-shows. "
            "Do not use confidential data, fake familiarity, urgency, or deceptive links. Honor suppression.",
        ),
        (
            "ACTIVE_CAMPAIGN_DECISION",
            "The active fictional campaign is %s. Drafting is fixture-only and requires human review; no sending is authorized." % campaign_name,
        ),
    )
    return [
        gold_record(section, content, index, revision, retrieved_at)
        for index, (section, content) in enumerate(contents, 1)
    ]


def drafting_output(bounded_input):
    """One pre-written fictional draft, the way the fixture provider returns it."""
    observation = next(
        item for item in bounded_input["evidence"] if item["signal_key"] == "personalization_observation"
    )
    business = bounded_input["business_name"]
    offer_name = bounded_input["offer"]["name"]
    cta = bounded_input["offer"]["cta"]
    inference = "A clearer staff-controlled next-action process could make those conversations easier to track."
    body = (
        "Hello,\n\n"
        "I noticed this public observation: %s "
        "That is a concrete signal about a channel your team already monitors. "
        "%s I build the %s to help business staff organize inquiries, bookings, reminders, "
        "follow-ups, and communication history in one visible process. "
        "It is designed to support staff control without assuming a missing workflow or changing your current tools. "
        "%s\n\nBest,\nOperator"
    ) % (observation["observation"], inference, offer_name, cta)
    return {
        "subject": "A clearer booking follow-up process for %s" % business,
        "body": body,
        "claims": [
            {"text": observation["observation"], "claim_type": "OBSERVATION", "evidence_ids": [observation["evidence_id"]]},
            {"text": inference, "claim_type": "INFERENCE", "evidence_ids": [observation["evidence_id"]]},
            {"text": offer_name, "claim_type": "OFFER", "evidence_ids": []},
            {"text": cta, "claim_type": "CTA", "evidence_ids": []},
        ],
        "evidence_ids_used": [observation["evidence_id"]],
        "confidence": 0.91,
        "cta_text": cta,
        "personalization_summary": "Uses one verified public observation about %s and its inquiry channel." % business,
        "prohibited_claim_self_check": True,
    }


def rule(title):
    print("")
    print("-" * 72)
    print(title)
    print("-" * 72)


def indent(text, prefix="  "):
    for line in str(text).splitlines():
        print(prefix + line)


# --------------------------------------------------------------------------
# Stages
# --------------------------------------------------------------------------
def run_demo():
    now = datetime.now(timezone.utc).replace(microsecond=0)

    print("=" * 72)
    print("paldo-outbound-agent: offline pipeline demo")
    print("=" * 72)
    print("")
    print("One fictional candidate is driven through the bounded pipeline:")
    print("discovery -> dedupe -> enrichment -> qualification -> drafting -> notification.")
    print("")
    print("Injected providers (never the real ones):")
    print("  HTTP      FixtureHTTPClient (canned pages, no sockets)")
    print("  DNS       FixtureDNSResolver (fixed answers, no resolver)")
    print("  drafting  FixtureDraftingProvider (pre-written draft, no model)")
    print("  notifier  notifier.ConsoleBackend (stdout)")
    print("")
    print("A network guard is installed for the whole run: if anything tried to")
    print("open a socket, this demo would fail instead of quietly calling out.")

    with tempfile.TemporaryDirectory(prefix="paldo-demo-") as temp_dir:
        database_path = Path(temp_dir) / "demo.sqlite3"
        if database_path.resolve() == DEFAULT_DB_PATH.resolve():
            raise RuntimeError("refusing to run the demo against the canonical database")
        database = Database(database_path)
        try:
            print("")
            print("Temporary database (non-canonical, deleted on exit):")
            print("  " + str(database_path))
            try:
                canonical_label = str(DEFAULT_DB_PATH.relative_to(REPO_ROOT))
            except ValueError:
                canonical_label = str(DEFAULT_DB_PATH)
            print("Canonical database this demo never opens:")
            print("  " + canonical_label + " (repo-relative)")

            migrate_step6(database)
            migrate_step7(database)
            campaign = database.get_campaign_by_name(FIXTURE_CAMPAIGN)
            if campaign is None:
                raise RuntimeError("fixture campaign is missing from the seed data")
            campaign_id = campaign["id"]

            records = [
                source_record("demo-accepted"),
                source_record("demo-accepted"),
                source_record("demo-closed", business_status="CLOSED_PERMANENTLY"),
            ]
            evidence_by_source = {"demo-accepted": fixture_evidence(now)}
            gates_by_source = {"demo-accepted": all_gates()}

            rule("STAGE 1: DISCOVERY (fixture records, no discovery provider)")
            print("Campaign: %s (id %d)" % (campaign["name"], campaign_id))
            print("")
            print("Submitted records:")
            for index, record in enumerate(records, 1):
                print(
                    "  %d. source_record_id=%-14s name=%-28s status=%s"
                    % (index, record["source_record_id"], record["business_name"], record["business_status"])
                )
            print("")
            print("Records 1 and 2 are the same source record, on purpose: the pipeline")
            print("must dedupe them instead of contacting the business twice.")

            http_client = FixtureHTTPClient(fixture_site())
            dns_client = FixtureDNSResolver()
            results = run_fictional_pipeline_fixture(
                database,
                campaign_id=campaign_id,
                records=records,
                evidence_by_source_record=evidence_by_source,
                gate_statuses_by_source_record=gates_by_source,
                http_client=http_client,
                dns_client=dns_client,
            )

            rule("STAGE 2: DEDUPE AND INGESTION (per-record outcome)")
            seen_runs = {}
            accepted = None
            for index, (record, summary) in enumerate(zip(records, results), 1):
                run_id = summary["run_id"]
                if run_id in seen_runs:
                    print(
                        "  %d. %-14s -> duplicate source record: routed to existing run %s"
                        % (index, record["source_record_id"], run_id)
                    )
                    continue
                seen_runs[run_id] = summary
                print(
                    "  %d. %-14s -> run %s, ingestion=%s, lead=%s, state=%s"
                    % (
                        index,
                        record["source_record_id"],
                        run_id,
                        summary["ingestion_outcome"],
                        summary["lead_id"],
                        summary["state"],
                    )
                )
                if summary["lead_id"] is not None and summary["state"] == "STRONG":
                    accepted = summary
            lead_count = database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
            run_count = database.connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0]
            print("")
            print("  leads created: %d (three submitted records)" % lead_count)
            print("  pipeline runs: %d" % run_count)
            print("  the closed business is rejected at ingestion and never gets a lead")
            if accepted is None:
                raise RuntimeError("the accepted fixture candidate did not reach STRONG")

            rule("STAGE 3: ENRICHMENT (fixture website, injected HTTP and DNS)")
            lead = database.get_lead(accepted["lead_id"])
            enrichment = accepted["enrichment"] or {}
            print("  lead %s: %s" % (lead["id"], lead["business_name"]))
            print("  website: %s" % lead["website"])
            print("  crawl state: %s" % enrichment.get("state"))
            print("  pages fetched: %s" % enrichment.get("page_count"))
            print("  public contacts extracted: %s" % enrichment.get("contact_count"))
            print("  workflow signals recorded: %s" % enrichment.get("evidence_count"))
            print("")
            print("  HTTP requests made by the fake client: %d, all to one host" % len(http_client.calls))
            print("  pages considered are same-site HTML only, robots.txt is respected")
            print("  no page body is stored in the database, only bounded observations")

            rule("STAGE 4: QUALIFICATION (deterministic rubric, no provider)")
            qualification = accepted["qualification"]
            print("  score: %s/100 -> %s" % (qualification["score"], qualification["classification"]))
            print("  qualification result: %s" % qualification["qualification_result"])
            print("")
            print("  points awarded per rubric component:")
            for component, points in sorted(qualification["awarded_points"].items()):
                print("    %-38s %s" % (component, points))
            print("")
            print("  mandatory gates:")
            for gate, value in sorted(qualification["mandatory_gates"].items()):
                print("    %-58s %s" % (gate, value))
            print("")
            print("  why:")
            indent(qualification["reason"], "    ")
            outreach_count = database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0]
            print("")
            print("  outreach rows created by this stage: %d" % outreach_count)

            rule("STAGE 5: DRAFTING (FixtureDraftingProvider, no live model)")
            migrate_step10(database)
            database.set_config("system_state", "ACTIVE")
            database.set_config("drafting_enabled", 1)
            database.set_config("drafting_daily_cap", DRAFTING_DAILY_CAP)
            database.set_config("drafting_provider_mode", "FIXTURE_ONLY")
            approve_immediate_offer(
                database,
                reason="Fictional demo approval of the fixture offer version.",
            )
            activate_immediate_offer(database)
            offer = get_immediate_offer_version(database)

            migrated = database.get_campaign_by_name(FIXTURE_CAMPAIGN)
            if migrated is None:
                raise RuntimeError("fixture campaign disappeared after migrations")
            packet = build_context_packet(
                database,
                migrated["id"],
                gold_records(migrated["name"], "demo-gold-revision-1", now),
                now=now,
            )
            # Lower-step migrations reconcile the seeded campaign list back to
            # INACTIVE, so the fixture activates its campaign once, last.
            with database.connection:
                database.connection.execute("UPDATE campaigns SET status='INACTIVE'")
                database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (migrated["id"],))

            print("  context packet %s: %s from %d fictional Gold sources"
                  % (packet["packet_id"], packet["readiness_state"], len(packet["source_ids"])))
            print("  active offer: %s (version %s, status %s)"
                  % (offer["name"], offer["version"], offer["status"]))

            preview = preview_bounded_drafting_input(
                database,
                lead_id=lead["id"],
                campaign_id=migrated["id"],
                packet_id=packet["packet_id"],
                now=now,
            )
            print("  drafting readiness: ready=%s" % preview["ready"])
            if not preview["ready"]:
                for reason in preview["blocking_reasons"]:
                    print("    blocked by: %s" % reason)
                raise RuntimeError("drafting eligibility failed for the fixture input")
            print("  evidence items handed to the provider: %d" % len(preview["provider_input"]["evidence"]))
            print("  keys handed to the provider: %s"
                  % ", ".join(sorted(preview["provider_input"])))
            print("  no recipient email is ever passed to the provider")

            provider = FixtureDraftingProvider(output=drafting_output(preview["provider_input"]))
            draft_result = generate_fictional_draft(
                database,
                lead_id=lead["id"],
                campaign_id=migrated["id"],
                provider=provider,
                packet_id=packet["packet_id"],
                now=now,
            )
            draft = draft_result["draft"]
            if draft is None:
                raise RuntimeError(
                    "drafting was blocked: %s" % ", ".join(draft_result["blocking_reasons"])
                )
            print("")
            print("  provider calls: %d" % provider.calls)
            attempt_states = [
                row["status"]
                for row in database.connection.execute(
                    "SELECT status FROM drafting_provider_attempts WHERE run_id=? ORDER BY attempt_number",
                    (draft_result["run_id"],),
                ).fetchall()
            ]
            print("  provider attempts recorded: %s" % ", ".join(attempt_states))
            print("  a draft only reaches REVIEW_PENDING after the drafting policy validator")
            print("  accepts every claim, so this state means the validator passed")
            print("  draft %s version %s state: %s"
                  % (draft["draft_id"], draft["version"], draft["state"]))
            print("  subject: %s" % draft["subject"])
            print("  body words: %s (policy limit is 70 to 120)" % draft["body_word_count"])
            print("")
            print("  draft body as generated:")
            indent(draft["body"], "    ")

            rule("STAGE 6: NOTIFICATION (console backend, nothing is sent)")
            print("The console backend below prints the operator card to stdout. The word")
            print("SENT in its output means the card was handed to the console backend, not")
            print("that a person received it: this demo has no transport that can reach one.")
            migrate_notifier(database)
            database.set_config("notifications_enabled", 1)
            database.set_config("notification_daily_cap", NOTIFICATION_DAILY_CAP)
            card = "\n".join(
                (
                    "REVIEW CARD (offline fixture demo)",
                    "draft %s version %s for %s" % (draft["draft_id"], draft["version"], lead["business_name"]),
                    "campaign: %s" % migrated["name"],
                    "qualification: %s, score %s/100" % (qualification["classification"], qualification["score"]),
                    "offer: %s (version %s)" % (offer["name"], offer["version"]),
                    "draft state: %s" % draft["state"],
                    "subject: %s" % draft["subject"],
                    "operator action required: review this draft before anything is sent.",
                )
            )
            notification = queue_notification(
                database,
                notifier=Notifier(ConsoleBackend()),
                topic_id=NOTIFICATION_CHANNELS["outbound_queue"],
                text=card,
                entity_type="draft",
                entity_id=draft["draft_id"],
                entity_version=draft["version"],
                content_fingerprint=draft_result["input_fingerprint"],
                fixture_override=True,
                now=now,
            )
            print("")
            print("  notification outbox: ok=%s, result=%s, outbox_id=%s"
                  % (notification["ok"], notification["result_category"], notification.get("outbox_id")))
            print("  the console backend above printed the operator card to stdout")
            attempts = database.connection.execute(
                "SELECT COUNT(*) FROM notification_attempts"
            ).fetchone()[0]
            print("  delivery attempts recorded: %s" % attempts)
            print("  outbound email, chat, and social sends: 0 (no such code path is reachable)")

            final_draft_state = database.connection.execute(
                "SELECT state FROM personalized_drafts WHERE id=?", (draft["draft_id"],)
            ).fetchone()[0]
            final_run_state = accepted["state"]

            print("")
            print("=" * 72)
            print("PIPELINE STOPPED FOR HUMAN REVIEW")
            print("=" * 72)
            print("run %s ended in state %s" % (accepted["run_id"], final_run_state))
            print("draft %s is in state %s and waiting for an operator" % (draft["draft_id"], final_draft_state))
            print("no email, message, or social contact was sent, and none will be")
            print("the temporary database below is deleted on exit")
            print("  " + str(database_path))
            print("")
            print("next: examples/handoff.example.json documents the production handoff the")
            print("      external cron agent must produce; it is not runnable as-is")
        finally:
            database.close()
    return 0


def main():
    with NetworkGuard():
        return run_demo()


if __name__ == "__main__":
    raise SystemExit(main())
