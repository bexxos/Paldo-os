from dataclasses import replace
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from paldo_os_outbound import Database
from step8_knowledge_context import build_context_packet
from step9_audit_offer import activate_immediate_offer, approve_immediate_offer
from step10_drafting import migrate_step10
from step11_gmail_drafts import FixtureGmailDraftProvider
from step12_followup import process_due_followups
from notifier import NOTIFICATION_CHANNEL_ID, NOTIFICATION_CHANNELS, NOTIFICATION_OPERATOR_ID, Notifier
from step14_scheduler import migrate_step14
from step15_identity_personalization import migrate_step15
from step16_campaign_intelligence import PRIMARY_CAMPAIGN_KEY, SECONDARY_CAMPAIGN_KEY
from step17b_pilot_policy import (
    ACTIVE_MESSAGE_POLICY_VERSION,
    APPROVED_PROOF_OUTCOMES,
    PilotPolicyValidationError,
    evaluate_touch_limit,
    INTERNATIONAL_PROOF_PHRASE,
    OPERATOR_INTERNATIONAL_IDENTITY,
    OPERATOR_LOCAL_IDENTITY,
    OPERATOR_LOCAL_REQUIRED_INTRO,
    build_message_policy_context,
    select_relevant_outcome,
    validate_versioned_message_policy,
)
from step22_integration import (
    GMAIL_ALLOWED_ACCOUNT,
    STEP22_MIGRATION_VERSION,
    Step22BlockedError,
    collect_basic_reporting,
    migrate_step22,
    record_manual_event,
    run_bounded_pilot,
    run_production_cycle,
)
from tests.test_step10_drafting import complete_gold_records
from tests.test_step6_website_enrichment import FakeDNS, FakeHTTPClient, fixture_site

NOW = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)


class Step22IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "step22.sqlite3")
        self.addCleanup(self.db.close)
        self.addCleanup(self.tempdir.cleanup)
        migrate_step14(self.db)
        migrate_step15(self.db)
        migrate_step10(self.db)
        migrate_step22(self.db)
        self.northfield = self.db.get_campaign_by_name("Primary region local services")
        self.us = self.db.get_campaign_by_name("Secondary region local services")
        for campaign in (self.northfield, self.us):
            build_context_packet(self.db, campaign["id"], complete_gold_records(campaign["name"]), now=NOW)
        approve_immediate_offer(self.db, reason="Step 22 fictional fixture approval.")
        activate_immediate_offer(self.db)
        for key, value in {
            "drafting_enabled": 1,
            "drafting_daily_cap": 10,
            "gmail_draft_integration_enabled": 1,
            "gmail_draft_daily_cap": 10,
            "gmail_account_configured": 1,
            "notifications_enabled": 1,
            "notification_daily_cap": 10,
            "personalization_research_enabled": 1,
            "decision_maker_enrichment_enabled": 1,
            "followup_enabled": 1,
            "reply_ingestion_enabled": 1,
            "followup_daily_cap": 10,
            "followup_provider_mode": "FIXTURE_ONLY",
            "linkedin_daily_cap": 10,
        }.items():
            self.db.set_config(key, value)

    def record(self, source_id, *, country, category):
        root = f"https://{source_id}.fictional-business.test"
        return {
            "source_record_id": source_id,
            "business_name": f"Fixture {source_id}",
            "business_category": category,
            "website_url": root,
            "public_business_email": f"hello@{source_id}.fictional-business.test",
            "public_business_phone": "+1 555 010 " + ("2001" if source_id == "northfield" else "2002"),
            "business_status": "OPERATIONAL",
            "country": country,
            "city": "Fixture City",
        }

    @staticmethod
    def http(source_id):
        old_root = "https://fictional-business.test"
        new_root = f"https://{source_id}.fictional-business.test"
        routes = {}
        for url, response in fixture_site().items():
            routes[url.replace(old_root, new_root)] = replace(
                response,
                requested_url=response.requested_url.replace(old_root, new_root),
                final_url=response.final_url.replace(old_root, new_root),
            )
        return FakeHTTPClient(routes)

    def evidence(self, source_id, *, local=False):
        return [
            {
                "signal_key": "campaign_fit",
                "signal_value": "YES",
                "observed_or_inferred": "OBSERVED",
                "source_type": "FIXTURE_PUBLIC_WEB",
                "source_url": "https://fictional-business.test/about",
                "observation": "The fictional public listing identifies an active local service appointment business.",
                "confidence": 0.95,
            },
            {
                "signal_key": "personalization_observation",
                "signal_value": "YES",
                "observed_or_inferred": "OBSERVED",
                "source_type": "FIXTURE_PUBLIC_WEB",
                "source_url": "https://fictional-business.test/contact",
                "observation": "The fictional public listing shows a booking and consultation route.",
                "confidence": 0.95,
            },
            {
                "signal_key": "public_business_email",
                "signal_value": "YES",
                "observed_or_inferred": "OBSERVED",
                "source_type": "FIXTURE_VERIFIED_EMAIL",
                "source_url": "https://fictional-business.test/contact",
                "observation": "The fictional business email is verified on the public contact route.",
                "confidence": 0.99,
            },
            {
                "signal_key": "owner_decision_maker_reachability",
                "signal_value": "YES",
                "observed_or_inferred": "OBSERVED",
                "source_type": "FIXTURE_PUBLIC_WEB",
                "source_url": "https://fictional-business.test/about",
                "observation": "The fictional public source identifies an accessible owner route.",
                "confidence": 0.95,
            },
        ]

    @staticmethod
    def gates():
        return {
            "currently_active": "YES",
            "appointments_meaningful": "YES",
            "legitimate_public_contact_route": "YES",
            "independent_owner_led_or_accessible_local_decision_maker": "YES",
        }

    @staticmethod
    def research(owner_name=None):
        decision = None if owner_name is None else {
            "kind": "OBSERVATION",
            "text": f"{owner_name} is identified as the owner on the fictional public source.",
            "source_type": "OFFICIAL_BUSINESS_SOURCE",
            "source_url": "https://fictional-business.test/about",
            "observed_at": NOW.isoformat(),
            "confidence": 0.95,
        }
        result = {
            "business_observation": {
                "kind": "OBSERVATION", "text": "The fictional public listing shows a booking and consultation route.",
                "source_type": "OFFICIAL_BUSINESS_SOURCE", "source_url": "https://fictional-business.test/contact",
                "observed_at": NOW.isoformat(), "confidence": 0.95,
            },
            "operational_signal": {
                "kind": "OBSERVATION", "text": "The fictional public business uses an appointment-based inquiry process.",
                "source_type": "OFFICIAL_BUSINESS_SOURCE", "source_url": "https://fictional-business.test/services",
                "observed_at": NOW.isoformat(), "confidence": 0.95,
            },
            "pain_hypothesis": {
                "kind": "HYPOTHESIS", "text": "The current process may require staff to review details in more than one place.",
                "basis": ["business_observation", "operational_signal"],
            },
            "desired_outcome": {
                "kind": "DESIRED_OUTCOME", "text": "Relevant records may be easier to find before a returning-client conversation.",
                "source_type": "OFFICIAL_BUSINESS_SOURCE", "source_url": "https://fictional-business.test/contact",
                "observed_at": NOW.isoformat(), "confidence": 0.8,
            },
            "capability_proof_match": {
                "kind": "CAPABILITY_MATCH", "text": "Business Booking and Follow-Up System can support visible booking, reminder, and follow-up records.",
                "source_type": "INTERNAL_OFFER_FIXTURE", "source_url": "https://fictional-business.test/services",
                "observed_at": NOW.isoformat(), "confidence": 0.9,
            },
            "unknowns": ["The current internal process is not established by public evidence."],
        }
        if decision is not None:
            result["decision_maker_observation"] = decision
        return result

    def test_approved_palado_policy_accepts_final_sequences_and_marks_ready_for_pilot(self):
        config = self.db.read_config()
        self.assertEqual(config["message_policy_status"], "APPROVED_FOR_DRAFT_CREATION")
        self.assertEqual(config["message_policy_automatic_sending"], 0)
        self.assertEqual(config["pilot_readiness_status"], "READY_FOR_PILOT")
        self.assertEqual(config["system_state"], "PAUSED")
        self.assertEqual(APPROVED_PROOF_OUTCOMES, ("Reduced no-shows", "Reduced manual data entry", "Made client records searchable"))

        local = build_message_policy_context(
            campaign_key=PRIMARY_CAMPAIGN_KEY,
            channel="EMAIL",
            proposed_angle="consultation cards, searchable client history, and repeated manual encoding",
        )
        self.assertEqual(local["selected_outcomes"], ["Reduced manual data entry", "Made client records searchable"])
        self.assertEqual(local["policy_status"], "APPROVED_FOR_DRAFT_CREATION")
        self.assertFalse(local["automatic_sending_approved"])
        local["original_subject"] = "consultation cards"
        local["thread_id"] = "thread-northfield"
        local_initial = (
            "Hi, I’m a local automation specialist working with owner-led service businesses.\n"
            "I saw that Tala asks returning clients to bring a consultation card. A searchable client history could help staff find previous details without having to enter the same information again.\n"
            "At Harborview Services in Westport, I built a system that reduced manual data entry and made client records searchable.\n"
            "Here’s the case study: https://example.com/work/case-study\n"
            "Would something like this be useful for Tala?\nOperator Joven Operator"
        )
        initial_check = validate_versioned_message_policy({"message_stage": "INITIAL", "subject": "consultation cards", "body": local_initial}, local)
        self.assertTrue(initial_check["valid"], initial_check)
        followup_one = validate_versioned_message_policy({"message_stage": "FOLLOWUP_1", "thread_id": "thread-northfield", "subject": "consultation cards", "body": "Hi,\nA simple version could let staff search a returning client’s previous details before the appointment while keeping the consultation-card process you already use.\nWould that be useful for your team?\nOperator Joven Operator"}, local)
        self.assertTrue(followup_one["valid"], followup_one)
        final_followup = validate_versioned_message_policy({"message_stage": "FINAL_FOLLOWUP", "thread_id": "thread-northfield", "subject": "consultation cards", "body": "Hi,\nI’ll leave this here for now. If reducing manual encoding or making returning-client history easier to find becomes a priority, feel free to message me.\nOperator Joven Operator"}, local)
        self.assertTrue(final_followup["valid"], final_followup)

        international = build_message_policy_context(
            campaign_key=SECONDARY_CAMPAIGN_KEY,
            channel="EMAIL",
            proposed_angle="treatment history and manual data entry during existing-client rebooking",
        )
        self.assertEqual(international["selected_outcomes"], ["Reduced manual data entry", "Made client records searchable"])
        international["original_subject"] = "treatment history"
        international["thread_id"] = "thread-us"
        international_initial = (
            "Hi Northline team,\n"
            "I saw that your booking information separates new consultations from existing-client rebooking, and your FAQ asks returning clients to bring their previous treatment plan.\n"
            "A searchable treatment history could make those details easier for staff to access when a client returns.\n"
            "I’m an automation specialist working with small service businesses. I built a similar system for an local service business that reduced manual data entry and made client records searchable.\n"
            "Here’s the case study: https://example.com/work/case-study\n"
            "Would something like this be useful for Northline?\nOperator Joven Operator"
        )
        international_check = validate_versioned_message_policy({"message_stage": "INITIAL", "subject": "treatment history", "body": international_initial}, international)
        self.assertTrue(international_check["valid"], international_check)
        self.assertNotIn("Harborview Services", international_initial)
        self.assertNotIn("Westport", international_initial)

        owner_context = build_message_policy_context(
            campaign_key=SECONDARY_CAMPAIGN_KEY,
            channel="EMAIL",
            proposed_angle="treatment history",
            recipient_first_name="Maya",
        )
        owner_body = f"Hi Maya,\n{OPERATOR_INTERNATIONAL_IDENTITY} {owner_context['proof']}\n{owner_context['case_study_url']}\nWould something like this be useful?"
        self.assertTrue(validate_versioned_message_policy({"message_stage": "INITIAL", "subject": "treatment history", "body": owner_body}, owner_context)["valid"])
        neutral_body = f"Hi Northline team,\n{OPERATOR_INTERNATIONAL_IDENTITY} {international['proof']}\n{international['case_study_url']}\nWould something like this be useful?"
        neutral_check = validate_versioned_message_policy({"message_stage": "INITIAL", "subject": "treatment history", "body": neutral_body}, international)
        self.assertTrue(neutral_check["valid"], neutral_check)

        defensive = validate_versioned_message_policy({"message_stage": "FINAL_FOLLOWUP", "thread_id": "thread-us", "subject": "treatment history", "body": "I haven’t confirmed this is happening, not because I’m assuming there’s a problem."}, international)
        self.assertFalse(defensive["valid"])
        self.assertIn("DEFENSIVE_UNCERTAINTY_FORBIDDEN", defensive["error_codes"])
        self.assertIn("FINAL_FOLLOWUP_MUST_HAVE_NO_QUESTION", validate_versioned_message_policy({"message_stage": "FINAL_FOLLOWUP", "thread_id": "thread-us", "subject": "treatment history", "body": "Would this help?"}, international)["error_codes"])
        with self.assertRaises(PilotPolicyValidationError):
            build_message_policy_context(campaign_key=SECONDARY_CAMPAIGN_KEY, channel="EMAIL", proposed_angle="treatment history, manual entry, and missed appointments")
        with self.assertRaises(PilotPolicyValidationError):
            build_message_policy_context(campaign_key=SECONDARY_CAMPAIGN_KEY, channel="EMAIL", proposed_angle="membership retention")

    def test_policy_selects_relevant_outcome_and_market_rules(self):
        local = build_message_policy_context(
            campaign_key=PRIMARY_CAMPAIGN_KEY,
            channel="EMAIL",
            proposed_angle="Returning-client consultation details may require less manual entry.",
        )
        self.assertEqual(local["message_policy_version"], ACTIVE_MESSAGE_POLICY_VERSION)
        self.assertEqual(local["selected_outcome"], "Reduced manual data entry")
        self.assertTrue(local["proof"].startswith("At Harborview Services in Westport"))
        self.assertTrue(validate_versioned_message_policy({
            "subject": "consultation details",
            "body": f"{OPERATOR_LOCAL_REQUIRED_INTRO}\n\nThe consultation details may be easier to review in one place. {local['proof']}\n\n{local['case_study_url']}\n\nWould something like this be useful?",
            "cta_text": "Would something like this be useful?",
        }, local)["valid"])

        international = build_message_policy_context(
            campaign_key=SECONDARY_CAMPAIGN_KEY,
            channel="EMAIL",
            proposed_angle="Existing-client treatment history may be easier to find before rebooking.",
        )
        self.assertEqual(international["selected_outcome"], "Made client records searchable")
        self.assertIn(INTERNATIONAL_PROOF_PHRASE, international["proof"])
        body = f"The prior treatment history may be easier to review before rebooking. {OPERATOR_INTERNATIONAL_IDENTITY} {international['proof']}\n\n{international['case_study_url']}\n\nWould something like this be useful?"
        check = validate_versioned_message_policy({"subject": "treatment history", "body": body, "cta_text": "Would something like this be useful?"}, international)
        self.assertTrue(check["valid"], check)
        self.assertNotIn("Harborview", body)
        self.assertNotIn("Westport", body)

    def test_bounded_local_and_us_paths_wire_writer_reviewer_gmail_and_notification_idempotently(self):
        writer_calls = []
        reviewer_calls = []

        def writer(payload):
            writer_calls.append(payload)
            context = payload["message_policy"]
            local = context["market"] == "PRIMARY"
            proof = context["proof"]
            question = "Would something like this be useful for Tala?" if local else "Would something like this be useful for Northline?"
            if local:
                subject = "consultation cards"
                body = (
                    f"{OPERATOR_LOCAL_REQUIRED_INTRO}\n"
                    "I saw that Tala asks returning clients to bring a consultation card. A searchable client history could help staff find previous details without having to enter the same information again.\n"
                    "At Harborview Services in Westport, I built a system that reduced manual data entry and made client records searchable.\n"
                    f"Here’s the case study: {context['case_study_url']}\n"
                    f"{question}\nOperator Joven Operator"
                )
            else:
                subject = "treatment history"
                body = (
                    "Hi Northline team,\n"
                    "I saw that your booking information separates new consultations from existing-client rebooking, and your FAQ asks returning clients to bring their previous treatment plan.\n"
                    "A searchable treatment history could make those details easier for staff to access when a client returns.\n"
                    f"{OPERATOR_INTERNATIONAL_IDENTITY} I built a similar system for an local service business that reduced manual data entry and made client records searchable.\n"
                    f"Here’s the case study: {context['case_study_url']}\n"
                    f"{question}\nOperator Joven Operator"
                )
            claims = [
                {"text": proof, "claim_type": "OFFER", "evidence_ids": []},
                {"text": question, "claim_type": "CTA", "evidence_ids": []},
            ]
            return {
                "subject": subject,
                "body": body,
                "claims": claims,
                "evidence_ids_used": [],
                "confidence": 0.9,
                "cta_text": question,
                "personalization_summary": "Fixture Writer used the current research packet and selected outcome.",
                "prohibited_claim_self_check": True,
            }

        def reviewer(message):
            reviewer_calls.append(message)
            return {"passed": True, "summary": "Fixture Reviewer passed factual and policy QA."}

        local_context = build_message_policy_context(
            campaign_key=PRIMARY_CAMPAIGN_KEY,
            channel="EMAIL",
            proposed_angle="Consultation cards, searchable client history, and repeated manual entry.",
        )
        local_record = self.record("northfield", country="PH", category="Local service business")
        local = run_bounded_pilot(
            self.db,
            campaign_id=self.northfield["id"],
            campaign_key=PRIMARY_CAMPAIGN_KEY,
            records=[local_record],
            evidence_by_source_record={"northfield": self.evidence("northfield", local=True)},
            gate_statuses_by_source_record={"northfield": self.gates()},
            decision_maker={
                "public_name": "Fictional Owner",
                "current_role": "owner",
                "business_domain": "northfield.fictional-business.test",
                "verification_status": "VERIFIED",
                "public_business_email": "owner@northfield.fictional-business.test",
                "public_linkedin_url": "https://www.linkedin.com/in/fictional-owner",
                "source_type": "OFFICIAL_BUSINESS_SOURCE",
                "source_url": "https://fictional-business.test/about",
                "observed_at": NOW.isoformat(),
                "confidence": 0.95,
            },
            research=self.research("Fictional Owner"),
            message_context=local_context,
            writer_provider=writer,
            reviewer=reviewer,
            gmail_provider=FixtureGmailDraftProvider(),
            notification_provider=Notifier(),
            http_client=self.http("northfield"),
            dns_client=FakeDNS(),
            lane="BUSINESS_FIRST",
            fixture_override=True,
            now=NOW,
        )
        self.assertTrue(local["ok"], local)
        self.assertEqual(local["contact_route"]["type"], "OWNER_BUSINESS_EMAIL")
        self.assertEqual(local["draft"]["state"], "REVIEW_PENDING")
        self.assertEqual(local["review"]["status"], "PASS")
        self.assertEqual(local["gmail"]["state"], "READY_FOR_MANUAL_SEND")
        self.assertEqual(local["notification"]["topic_id"], NOTIFICATION_CHANNELS["outbound_queue"])
        self.assertEqual(len(writer_calls), 1)
        self.assertEqual(len(reviewer_calls), 1)

        local_again = run_bounded_pilot(
            self.db,
            campaign_id=self.northfield["id"], campaign_key=PRIMARY_CAMPAIGN_KEY,
            records=[local_record], evidence_by_source_record={"northfield": self.evidence("northfield", local=True)},
            gate_statuses_by_source_record={"northfield": self.gates()}, decision_maker={
                "public_name": "Fictional Owner", "current_role": "owner", "business_domain": "northfield.fictional-business.test",
                "verification_status": "VERIFIED", "public_business_email": "owner@northfield.fictional-business.test",
                "public_linkedin_url": "https://www.linkedin.com/in/fictional-owner", "source_type": "OFFICIAL_BUSINESS_SOURCE",
                "source_url": "https://fictional-business.test/about", "observed_at": NOW.isoformat(), "confidence": 0.95,
            },
            research=self.research("Fictional Owner"),
            message_context=local_context, writer_provider=writer, reviewer=reviewer,
            gmail_provider=local["providers"]["gmail"], notification_provider=local["providers"]["notification"],
            http_client=self.http("northfield"), dns_client=FakeDNS(), lane="BUSINESS_FIRST",
            fixture_override=True, now=NOW,
        )
        self.assertTrue(local_again["reused"])
        self.assertEqual(len(writer_calls), 1)
        self.assertEqual(len(reviewer_calls), 1)

        us_context = build_message_policy_context(
            campaign_key=SECONDARY_CAMPAIGN_KEY, channel="EMAIL",
            proposed_angle="Treatment history and repeated manual data entry during existing-client rebooking.",
        )
        us_record = self.record("us", country="US", category="Local service business")
        us = run_bounded_pilot(
            self.db, campaign_id=self.us["id"], campaign_key=SECONDARY_CAMPAIGN_KEY,
            records=[us_record], evidence_by_source_record={"us": self.evidence("us")},
            gate_statuses_by_source_record={"us": self.gates()}, decision_maker=None,
            research=self.research(),
            message_context=us_context, writer_provider=writer, reviewer=reviewer,
            gmail_provider=FixtureGmailDraftProvider(), notification_provider=Notifier(),
            http_client=self.http("us"), dns_client=FakeDNS(), lane="BUSINESS_FIRST",
            fixture_override=True, now=NOW,
        )
        self.assertTrue(us["ok"], us)
        self.assertEqual(us["contact_route"]["type"], "GENERAL_BUSINESS_EMAIL")
        self.assertIn("an local service business", us["draft"]["body"])
        self.assertNotIn("Harborview Services", us["draft"]["body"])
        self.assertNotIn("Westport", us["draft"]["body"])
        self.assertEqual(len(writer_calls), 2)
        self.assertEqual(len(reviewer_calls), 2)
        queue_text = self.db.connection.execute("SELECT text FROM notification_outbox WHERE topic_id=? ORDER BY id DESC LIMIT 1", (NOTIFICATION_CHANNELS["outbound_queue"],)).fetchone()["text"]
        for label in ("Business:", "Campaign:", "Decision-maker/contact route:", "Relevant research:", "Proposed angle:", "Draft text:", "Gmail draft reference:", "Review status:"):
            self.assertIn(label, queue_text)
        self.assertIn("Gmail draft reference:", queue_text)

    def test_production_cycle_commits_live_handoff_idempotently_and_cleans_campaigns(self):
        context = build_message_policy_context(
            campaign_key=PRIMARY_CAMPAIGN_KEY,
            channel="EMAIL",
            proposed_angle="consultation cards, searchable client history, and repeated manual encoding",
        )
        body = (
            "Hi, I’m a local automation specialist working with owner-led service businesses.\n"
            "I saw that Tala asks returning clients to bring a consultation card. A searchable client history could help staff find previous details without having to enter the same information again.\n"
            "At Harborview Services in Westport, I built a system that reduced manual data entry and made client records searchable.\n"
            "Here’s the case study: https://example.com/work/case-study\n"
            "Would something like this be useful for Tala?\nOperator Joven Operator"
        )
        message = {
            "channel": "EMAIL", "message_stage": "INITIAL", "subject": "consultation cards", "body": body,
            "policy_context": context, "review_status": "PASS", "touch_number": 1,
            "idempotency_key": "prod-northfield-message-1",
            "gmail_reference": {
                "provider": "COMPOSIO", "actions": ["CREATE_DRAFT", "GET_DRAFT"], "draft_id": "r-prod-draft-1",
                "message_id": "r-prod-message-1", "thread_id": "r-prod-thread-1", "sender": GMAIL_ALLOWED_ACCOUNT,
                "recipient": "owner@prod-business.example", "subject": "consultation cards", "body": body,
                "content_hash": __import__("hashlib").sha256(__import__("json").dumps({"to": "owner@prod-business.example", "from": GMAIL_ALLOWED_ACCOUNT, "subject": "consultation cards", "body": body}, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                "labels": ["DRAFT"], "sent": False,
            },
            "notification_reference": {"provider": "JSON_FILE", "channel_id": NOTIFICATION_CHANNEL_ID, "topic_id": NOTIFICATION_CHANNELS["outbound_queue"], "message_id": "prod-queue-1"},
        }
        evidence = []
        for key in ("campaign_fit", "appointment_dependence", "website_booking_page_widget", "recent_activity_demand", "multiple_services", "multiple_practitioners", "owner_decision_maker_reachability", "personalization_observation"):
            evidence.append({"signal_key": key, "signal_value": "YES", "observed_or_inferred": "OBSERVED", "source_type": "APIFY_PUBLIC_WEB", "source_url": "https://prod-business.example/about", "observation": f"Public source supports {key}.", "confidence": 0.9})
        raw_candidate = {
            "lane": "PRIMARY", "source_record_id": "apify-prod-northfield-1", "business_name": "Tala Local Service Business",
            "business_category": "Local service business", "website_url": "https://prod-business.example", "public_business_email": "hello@prod-business.example",
            "public_business_phone": "+1 555 0101", "business_status": "OPERATIONAL", "country": "PH", "city": "Westport",
            "source_url": "https://www.google.com/maps?cid=prod-northfield-1", "idempotency_key": "prod-northfield-candidate-1",
            "decision_maker": {"route_type": "OWNER_PROFESSIONAL_EMAIL", "route_value": "owner@prod-business.example", "person_name": "Tala Owner", "verified_role": "Owner", "verification_status": "VERIFIED", "public_business_email": "owner@prod-business.example", "public_linkedin_url": "https://www.linkedin.com/in/tala-owner", "business_domain": "prod-business.example", "source_type": "OFFICIAL_BUSINESS_SOURCE", "source_url": "https://prod-business.example/about", "observed_at": NOW.isoformat(), "confidence": 0.95},
            "research": {"business_identity": "Tala Local Service Business", "location": "Westport, Northfield", "services": ["local service consultations"], "official_website": "https://prod-business.example", "booking_route": "website consultation route", "inquiry_channels": ["email", "phone"], "business_terminology": ["consultation", "returning client"], "operational_signals": ["appointment-based service"], "observations": ["The public site describes consultations."], "pain_hypotheses": ["Staff may need to re-enter returning-client details."], "sources": [{"url": "https://prod-business.example/about", "retrieved_at": NOW.isoformat()}], "retrieved_at": NOW.isoformat(), "uncertainty": [], "contradictions": [], "proof_outcome_match": ["Reduced manual data entry", "Made client records searchable"], "decision_maker_evidence": {"status": "verified", "source_url": "https://prod-business.example/about"}, "evidence": evidence, "gate_statuses": self.gates()},
            "messages": [message],
        }
        handoff = {"mode": "LIVE", "external_orchestrator": "HERMES_CRON_AGENT", "run_id": "prod-cycle-test-1", "actor_runs": [{"lane": "PRIMARY", "actor_id": "compass~crawler-google-places", "state": "SUCCEEDED", "remote_run_id": "apify-run-northfield-1", "dataset_id": "apify-dataset-northfield-1", "item_count": 1, "cost_usd": 0.05}, {"lane": "SECONDARY", "actor_id": "compass~crawler-google-places", "state": "SUCCEEDED", "remote_run_id": "apify-run-secondary-1", "dataset_id": "apify-dataset-us-1", "item_count": 0, "cost_usd": 0.05}], "candidates": [raw_candidate]}
        self.db.set_config("system_state", "ACTIVE")
        self.db.set_config("discovery_mode", "LIVE")
        with patch("step22_integration.DEFAULT_DB_PATH", self.db.path):
            result = run_production_cycle(self.db, handoff=handoff, now=NOW)
            again = run_production_cycle(self.db, handoff=handoff, now=NOW)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["counts"]["qualified"], 1)
        self.assertEqual(result["counts"]["gmail_drafts"], 1)
        self.assertTrue(again["reused"])
        self.assertEqual({row["status"] for row in self.db.get_campaigns()}, {"INACTIVE"})
        self.assertEqual(self.db.connection.execute("SELECT COUNT(*) FROM outreach WHERE status='DRAFT_REVIEW_PENDING'").fetchone()[0], 1)

    def test_production_cycle_rejects_fixture_and_temporary_database(self):
        with self.assertRaises(Exception):
            run_production_cycle(self.db, handoff={}, fixture_override=True)
        with self.assertRaises(Exception):
            run_production_cycle(self.db, handoff={}, fixture_override=False)

        provider = FixtureGmailDraftProvider()
        sequence_id = 1
        with self.assertRaises(Exception):
            record_manual_event(self.db, event_type="MANUAL_SEND", sequence_id=sequence_id, operator_user_id=999, fixture_override=True)
        # The full lifecycle is exercised by the existing Step 12 tests; Step 22
        # must expose the same audited commands without counting drafts as sends.
        summary = collect_basic_reporting(self.db)
        self.assertEqual(summary["drafts_awaiting_review"], 0)
        self.assertEqual(summary["manual_sends_recorded"], 0)
        self.assertEqual(summary["drafts_counted_as_sends"], 0)
        self.assertFalse(provider.__class__.__dict__.get("send"))
        blocked = run_bounded_pilot(self.db, campaign_id=self.northfield["id"], campaign_key=PRIMARY_CAMPAIGN_KEY, fixture_override=False)
        self.assertFalse(blocked["ok"])
        self.assertIn("FIXTURE_OVERRIDE_REQUIRED", blocked["blocking_reasons"])
        self.assertEqual(self.db.get_config("system_state"), "PAUSED")
        self.assertEqual(self.db.get_config("gmail_send_enabled"), 0)
        self.assertEqual(self.db.get_config("scheduler_enabled"), 0)
        self.assertEqual(STEP22_MIGRATION_VERSION, 18)
    def test_manual_send_reply_stop_opt_out_hard_bounce_and_authorized_notification_recording(self):
        self.test_bounded_local_and_us_paths_wire_writer_reviewer_gmail_and_notification_idempotently()
        sequences = self.db.connection.execute("SELECT id FROM followup_sequences ORDER BY id").fetchall()
        self.assertEqual(len(sequences), 2)
        local_sequence = sequences[0]["id"]
        us_sequence = sequences[1]["id"]

        sent = record_manual_event(
            self.db, event_type="MANUAL_SEND", sequence_id=us_sequence, operator_user_id=NOTIFICATION_OPERATOR_ID,
            fixture_override=True, reason="Fictional manual send recorded after Operator review.",
            idempotency_key="step22-manual-send-1", sent_at=NOW, now=NOW,
        )
        self.assertEqual(sent["touch_number"], 1)
        sent_again = record_manual_event(
            self.db, event_type="MANUAL_SEND", sequence_id=us_sequence, operator_user_id=NOTIFICATION_OPERATOR_ID,
            fixture_override=True, reason="Same fictional manual send retry.",
            idempotency_key="step22-manual-send-1", sent_at=NOW, now=NOW,
        )
        self.assertTrue(sent_again["reused"])

        positive = record_manual_event(
            self.db, event_type="REPLY", sequence_id=us_sequence, operator_user_id=NOTIFICATION_OPERATOR_ID,
            fixture_override=True, category="POSITIVE_INTEREST", event_reference="manual-positive-1",
            safe_reference="operator-note-positive-interest", reason="Positive reply recorded manually.", now=NOW,
        )
        self.assertEqual(positive["state"], "HUMAN_ACTION_REQUIRED")
        blocked_followup = process_due_followups(self.db, sequence_id=us_sequence, fixture_override=True, now=NOW)
        self.assertIn("CADENCE_NOT_SUPPLIED", blocked_followup["blocking_reasons"])

        opt_out = record_manual_event(
            self.db, event_type="OPT_OUT", sequence_id=us_sequence, operator_user_id=NOTIFICATION_OPERATOR_ID,
            fixture_override=True, event_reference="manual-optout-1", safe_reference="operator-note-opt-out",
            reason="Opt-out recorded manually.", now=NOW,
        )
        self.assertEqual(opt_out["state"], "SUPPRESSED")

        with self.assertRaises(Step22BlockedError) as unauthorized:
            record_manual_event(
                self.db, event_type="HARD_BOUNCE", sequence_id=local_sequence, operator_user_id=999,
                fixture_override=True, event_reference="manual-bounce-1", safe_reference="operator-note-bounce",
                reason="Hard bounce recorded.", now=NOW,
            )
        self.assertIn("OPERATOR_UNAUTHORIZED", str(unauthorized.exception))

        bounce = record_manual_event(
            self.db, event_type="HARD_BOUNCE", sequence_id=us_sequence, operator_user_id=NOTIFICATION_OPERATOR_ID,
            fixture_override=True, event_reference="manual-bounce-1", safe_reference="operator-note-bounce",
            reason="Hard bounce recorded.", now=NOW,
        )
        self.assertEqual(bounce["state"], "SUPPRESSED")

        report = collect_basic_reporting(self.db)
        self.assertEqual(report["manual_sends_recorded"], 1)
        self.assertEqual(report["replies_received"], 3)
        self.assertEqual(report["positive_replies"], 1)
        self.assertEqual(report["opt_outs"], 1)
        self.assertEqual(report["bounces"], 1)
        self.assertEqual(report["drafts_counted_as_sends"], 0)
        self.assertFalse(evaluate_touch_limit(3, replied=False)["allowed"])
        self.assertFalse(evaluate_touch_limit(1, replied=True)["allowed"])


if __name__ == "__main__":
    unittest.main()
