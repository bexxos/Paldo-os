import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from step8_knowledge_context import build_context_packet
from step9_audit_offer import IMMEDIATE_OFFER_CTA, activate_immediate_offer, approve_immediate_offer, get_immediate_offer_version
from step10_drafting import FixtureDraftingProvider, generate_fictional_draft, preview_bounded_drafting_input
from step11_gmail_drafts import FixtureGmailDraftProvider, approve_gmail_draft_creation, create_fixture_gmail_draft, migrate_step11, verify_fixture_gmail_draft
from step12_followup import (
    FOLLOWUP_REPLY_CATEGORIES,
    FixtureReplyProvider,
    classify_fixture_reply,
    calculate_fixture_due_date,
    cancel_pending_followup_actions,
    close_no_response_sequence,
    get_followup_sequence,
    ingest_fixture_reply,
    initialize_followup_sequence,
    inspect_followup_readiness,
    migrate_step12,
    process_due_followups,
    record_manual_send,
    request_followup_draft,
    summarize_followup_provenance,
    mark_followup_ready_for_manual_send,
    FollowupBlockedError,
    FollowupValidationError,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
SENDER = "operator@fictional-example.invalid"


def iso(value):
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def fixture_gold_records(campaign_name):
    contents = {
        "ICP": "Owner-led appointment businesses, beginning with fictional Primary region local services.",
        "ACTIVE_OFFER_CTA": f"Active offer: Business Booking and Follow-Up System. Approved CTA: {IMMEDIATE_OFFER_CTA}",
        "VOICE_GUIDE": "Use clear, calm, specific language. Be professional, natural, and non-pressuring.",
        "APPROVED_PROOF_RULES": "Use only current, attributable observations. Keep hypotheses cautious and distinguish them from observations.",
        "OUTBOUND_COMPLIANCE_POLICY": "Do not claim guaranteed results, savings, revenue, ROI, or reduced no-shows. Do not use client data, medical advice, fake familiarity, urgency, deceptive links, or audit language. Honor suppression.",
        "ACTIVE_CAMPAIGN_DECISION": f"The active fictional campaign is {campaign_name}. Drafting is fixture-only and requires human review; no sending is authorized.",
    }
    return [
        {
            "source_id": f"step12-gold-{index}", "status": "Gold", "trust_tier": "Gold", "approval_status": "APPROVED",
            "approved_by": "OPERATOR", "confidence": 0.96, "heading": section,
            "drive_file_id": f"drive-step12-{index}", "kb_path": f"gold/step12/{index}",
            "source_url": f"https://fictional-kb.invalid/step12/{index}", "modified_at": iso(NOW - timedelta(days=1)),
            "snapshot_revision": "fictional-step12-gold-rev-1", "manifest_revision": "fictional-step12-gold-rev-1",
            "content_hash": hashlib.sha256(content.encode()).hexdigest(), "retrieved_at": iso(NOW),
            "section": section, "content": content,
        }
        for index, (section, content) in enumerate(contents.items(), 1)
    ]


class Step12FollowupTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.database = Database(Path(self.temp_dir.name) / "step12.sqlite3")
        self.addCleanup(self.database.close)
        migrate_step12(self.database)
        self.campaign = self.database.get_campaign_by_name("Primary region local services")
        self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))
        self.database.connection.commit()
        for key, value in {
            "system_state": "ACTIVE", "drafting_enabled": 1, "drafting_daily_cap": 20,
            "gmail_draft_integration_enabled": 1, "gmail_draft_daily_cap": 5,
            "gmail_account_configured": 1, "gmail_send_enabled": 0,
            "followup_enabled": 1, "reply_ingestion_enabled": 1, "followup_daily_cap": 5,
        }.items():
            self.database.set_config(key, value)
        get_immediate_offer_version(self.database)
        approve_immediate_offer(self.database, reason="Fictional Step 12 fixture approval.")
        activate_immediate_offer(self.database)
        self.packet = build_context_packet(self.database, self.campaign["id"], fixture_gold_records(self.campaign["name"]), now=NOW)
        self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))
        self.database.connection.commit()
        self.lead_id, self.draft, self.gmail_run_id = self._make_verified_step11_draft()
        self.recipient = self.database.get_lead(self.lead_id)["email"]

    def _make_verified_step11_draft(self):
        recipient = "bookings@fictional-glow.fictional-business.invalid"
        lead_id = self.database.insert_lead(
            business_name="Fictional Glow Business", website=None, email=recipient,
            industry="local service local business", country="Philippines", location="Northfield",
            source="FIXTURE", source_url="https://fictional-business.invalid/listing",
            status="QUALIFIED", campaign_id=self.campaign["id"],
        )
        for signal, observation, source_type, source_url, confidence in (
            ("campaign_fit", "The fictional public business listing identifies an local service appointment business in Northfield.", "FIXTURE_PUBLIC_WEB", "https://fictional-business.invalid/listing", .95),
            ("personalization_observation", "The fictional public listing says appointment inquiries are accepted through Facebook Messenger.", "FIXTURE_PUBLIC_WEB", "https://fictional-business.invalid/contact", .95),
            ("public_business_email", "The fictional public business contact route is verified for this fixture.", "FIXTURE_VERIFIED_EMAIL", "https://fictional-business.invalid/contact", .99),
        ):
            self.database.insert_structured_evidence(
                lead_id=lead_id, signal_key=signal, signal_value="YES", source_type=source_type,
                observation=observation, source_url=source_url, confidence=confidence, collected_at=iso(NOW),
            )
        self.database.connection.execute(
            "INSERT INTO qualification_results (lead_id,campaign_id,score,classification,qualification_result,gate_statuses,reasoning_data,evaluated_at) VALUES (?,?,90,'STRONG','QUALIFY',?,?,?)",
            (lead_id, self.campaign["id"], json.dumps({"fixture": "YES"}), json.dumps({"classification": "STRONG"}), iso(NOW)),
        )
        self.database.connection.commit()
        bounded = preview_bounded_drafting_input(self.database, lead_id=lead_id, campaign_id=self.campaign["id"], packet_id=self.packet["packet_id"], now=NOW)["provider_input"]
        observation = next(item for item in bounded["evidence"] if item["signal_key"] == "personalization_observation")
        business, offer_name, cta = bounded["business_name"], bounded["offer"]["name"], bounded["offer"]["cta"]
        inference = "A clearer staff-controlled next-action process could make those conversations easier to track."
        body = (
            f"Hello,\n\nI noticed this public observation: {observation['observation']} That is a concrete signal about a channel your team already monitors. "
            f"{inference} I build the {offer_name} to help business staff organize inquiries, bookings, reminders, follow-ups, and communication history in one visible process. "
            "It is designed to support staff control without assuming a missing workflow or changing your current tools. "
            f"{cta}\n\nBest,\nOperator"
        )
        output = {
            "subject": f"A clearer booking follow-up process for {business}", "body": body,
            "claims": [
                {"text": observation["observation"], "claim_type": "OBSERVATION", "evidence_ids": [observation["evidence_id"]]},
                {"text": inference, "claim_type": "INFERENCE", "evidence_ids": [observation["evidence_id"]]},
                {"text": offer_name, "claim_type": "OFFER", "evidence_ids": []},
                {"text": cta, "claim_type": "CTA", "evidence_ids": []},
            ],
            "evidence_ids_used": [observation["evidence_id"]], "confidence": .91, "cta_text": cta,
            "personalization_summary": f"Uses one verified public observation about {business}'s inquiry channel.",
            "prohibited_claim_self_check": True,
        }
        draft_result = generate_fictional_draft(
            self.database, lead_id=lead_id, campaign_id=self.campaign["id"], provider=FixtureDraftingProvider(output=output),
            packet_id=self.packet["packet_id"], now=NOW,
        )
        self.assertEqual(draft_result["state"], "REVIEW_PENDING")
        draft = draft_result["draft"]
        approval = approve_gmail_draft_creation(
            self.database, draft_id=draft["draft_id"], reviewer_identity="OPERATOR", recipient_email=recipient,
            sender_email=SENDER, reason="Fictional operator approval for Step 12 fixture.", fixture_override=True, now=NOW,
        )
        provider = FixtureGmailDraftProvider()
        created = create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(created["state"], "EXTERNAL_DRAFT_CREATED")
        verified = verify_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(verified["state"], "READY_FOR_MANUAL_SEND")
        return lead_id, draft, approval["run_id"]

    def _sequence(self, **kwargs):
        args = {
            "gmail_run_id": self.gmail_run_id, "fixture_override": True, "now": NOW,
            "cadence_strategy": "CALENDAR_DAYS", "interval_value": 3,
            "campaign_timezone": "UTC", "holiday_calendar": (),
        }
        args.update(kwargs)
        return initialize_followup_sequence(self.database, **args)

    def _send(self, sequence_id, **kwargs):
        sequence = get_followup_sequence(self.database, sequence_id=sequence_id)
        args = {
            "sequence_id": sequence_id, "draft_id": self.draft["draft_id"], "draft_version": self.draft["version"],
            "recipient_email": self.recipient, "sender_email": SENDER, "content_hash": sequence["content_hash"],
            "sent_at": iso(NOW), "reviewer_identity": "OPERATOR", "reason": "Fictional manual send confirmed by operator.",
            "idempotency_key": kwargs.pop("idempotency_key", f"fixture-send-{sequence_id}-{sequence['current_touch_count'] + 1}"),
            "fixture_override": True, "now": NOW,
        }
        args.update(kwargs)
        return record_manual_send(self.database, **args)

    def _due_and_requested(self, sequence_id):
        due = process_due_followups(self.database, sequence_id=sequence_id, now=NOW + timedelta(days=3), fixture_override=True)
        self.assertEqual(due["state"], "FOLLOWUP_DUE")
        requested = request_followup_draft(self.database, sequence_id=sequence_id, action_id=due["action_id"], fixture_override=True, now=NOW + timedelta(days=3))
        return due, requested

    def test_migration_is_idempotent_defaults_and_policy_are_safe(self):
        self.assertEqual(migrate_step12(self.database), 14)
        self.assertEqual(migrate_step12(self.database), 14)
        fresh = Database(Path(self.temp_dir.name) / "defaults.sqlite3")
        self.addCleanup(fresh.close)
        migrate_step12(fresh)
        config = fresh.read_config()
        self.assertFalse(config["followup_enabled"])
        self.assertFalse(config["reply_ingestion_enabled"])
        self.assertEqual(config["followup_provider_mode"], "FIXTURE_ONLY")
        self.assertEqual(config["followup_policy_version"], "TEST_FOLLOWUP_V0_1")
        self.assertEqual(config["followup_policy_status"], "PROVISIONAL")
        self.assertEqual(config["followup_max_total_touches"], 3)
        self.assertEqual(config["followup_cadence_status"], "UNDECIDED")
        self.assertEqual(config["followup_daily_cap"], 0)

    def test_sequence_initializes_awaiting_manual_send_without_content_generation(self):
        result = self._sequence(cadence_strategy=None, interval_value=None)
        self.assertEqual(result["state"], "AWAITING_MANUAL_SEND")
        readiness = inspect_followup_readiness(self.database, sequence_id=result["sequence_id"], fixture_override=True, now=NOW)
        self.assertTrue(readiness["ready"])
        self.assertIsNone(readiness["next_due_at"])

    def test_first_manual_send_creates_touch_one(self):
        sequence = self._sequence()
        result = self._send(sequence["sequence_id"])
        self.assertEqual(result["state"], "WAITING_FOR_REPLY")
        self.assertEqual(result["touch_number"], 1)
        self.assertEqual(result["touch_count"], 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)

    def test_duplicate_manual_send_event_does_not_increment_touch_count(self):
        sequence = self._sequence(); first = self._send(sequence["sequence_id"], idempotency_key="same-send-key")
        duplicate = self._send(sequence["sequence_id"], idempotency_key="same-send-key")
        self.assertTrue(duplicate["reused"]); self.assertEqual(duplicate["touch_count"], 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM followup_sent_touches").fetchone()[0], 1)

    def test_manual_send_mismatch_suppression_and_non_operator_are_blocked(self):
        sequence = self._sequence()
        with self.assertRaises(FollowupValidationError):
            self._send(sequence["sequence_id"], recipient_email="other@fictional-business.invalid")
        with self.assertRaises(FollowupValidationError):
            self._send(sequence["sequence_id"], reviewer_identity="AI")
        self.database.add_suppression(lead_id=self.lead_id, reason="fictional opt-out")
        result = self._send(sequence["sequence_id"], idempotency_key="suppressed-send")
        self.assertIn("SUPPRESSED", result["blocking_reasons"])

    def test_calendar_day_due_date_is_injected_and_cannot_precede_send(self):
        sequence = self._sequence()
        before = process_due_followups(self.database, sequence_id=sequence["sequence_id"], now=NOW, fixture_override=True)
        self.assertIn("PRIOR_SENT_TOUCH_REQUIRED", before["blocking_reasons"])
        self._send(sequence["sequence_id"])
        due_at = calculate_fixture_due_date(NOW, "CALENDAR_DAYS", 3, "UTC", ())
        self.assertEqual(due_at, NOW + timedelta(days=3))

    def test_business_day_due_date_honors_timezone_and_holidays(self):
        friday = datetime(2026, 9, 4, 9, 0, tzinfo=UTC)
        due = calculate_fixture_due_date(friday, "BUSINESS_DAYS", 2, "UTC", ("2026-09-08",))
        self.assertEqual(due, datetime(2026, 9, 9, 9, 0, tzinfo=UTC))

    def test_no_canonical_cadence_is_selected_and_canonical_operation_blocks(self):
        self.assertEqual(self.database.read_config()["followup_cadence_status"], "UNDECIDED")
        canonical = Database(Path(self.temp_dir.name) / "canonical-guard.sqlite3"); self.addCleanup(canonical.close)
        canonical.path = DEFAULT_DB_PATH
        migrate_step12(canonical)
        readiness = inspect_followup_readiness(canonical, sequence_id=999999, fixture_override=False, now=NOW)
        self.assertFalse(readiness["ready"]); self.assertIn("CANONICAL_FOLLOWUP_OPERATION_BLOCKED", readiness["blocking_reasons"])

    def test_due_processing_is_idempotent_reserves_one_slot_and_prioritizes_followup(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"])
        due = process_due_followups(self.database, sequence_id=sequence["sequence_id"], now=NOW + timedelta(days=3), fixture_override=True, new_outreach_requested=2)
        again = process_due_followups(self.database, sequence_id=sequence["sequence_id"], now=NOW + timedelta(days=3), fixture_override=True, new_outreach_requested=2)
        self.assertEqual(due["state"], "FOLLOWUP_DUE"); self.assertEqual(due["priority"], "FOLLOWUP_FIRST")
        self.assertTrue(again["reused"]); self.assertEqual(due["reservation_count"], 1); self.assertEqual(again["reservation_count"], 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM followup_capacity_reservations").fetchone()[0], 1)
        self.assertEqual(due["new_outreach_slots_reserved"], 0)

    def test_followup_request_contains_no_subject_or_body_and_requires_review(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"]); due, requested = self._due_and_requested(sequence["sequence_id"])
        self.assertEqual(requested["state"], "FOLLOWUP_REVIEW_PENDING")
        self.assertNotIn("subject", requested); self.assertNotIn("body", requested)
        row = self.database.connection.execute("SELECT * FROM followup_actions WHERE id=?", (due["action_id"],)).fetchone()
        self.assertNotIn("subject", row.keys()); self.assertNotIn("body", row.keys())

    def test_maximum_total_touches_is_three_and_third_closes_no_response(self):
        sequence = self._sequence(); first = self._send(sequence["sequence_id"])
        _, _ = self._due_and_requested(sequence["sequence_id"])
        mark_followup_ready_for_manual_send(self.database, sequence_id=sequence["sequence_id"], action_id=1, reviewer_identity="OPERATOR", reason="Fictional review complete.", fixture_override=True, now=NOW + timedelta(days=3))
        second = self._send(sequence["sequence_id"], sent_at=iso(NOW + timedelta(days=3)))
        self.assertEqual(second["touch_number"], 2)
        process_due_followups(self.database, sequence_id=sequence["sequence_id"], now=NOW + timedelta(days=6), fixture_override=True)
        request_followup_draft(self.database, sequence_id=sequence["sequence_id"], action_id=2, fixture_override=True, now=NOW + timedelta(days=6))
        mark_followup_ready_for_manual_send(self.database, sequence_id=sequence["sequence_id"], action_id=2, reviewer_identity="OPERATOR", reason="Fictional third-touch review.", fixture_override=True, now=NOW + timedelta(days=6))
        third = self._send(sequence["sequence_id"], sent_at=iso(NOW + timedelta(days=6)))
        self.assertEqual(third["touch_number"], 3); self.assertEqual(third["state"], "CLOSED_NO_RESPONSE")
        blocked = self._send(sequence["sequence_id"], sent_at=iso(NOW + timedelta(days=9)), idempotency_key="fourth-touch")
        self.assertIn("MAX_TOTAL_TOUCHES_REACHED", blocked["blocking_reasons"])

    def test_positive_and_neutral_replies_cancel_actions_and_require_human_action(self):
        for index, category in enumerate(("POSITIVE_INTEREST", "QUESTION_OR_NEUTRAL")):
            target = self
            if index:
                target = type(self)(self._testMethodName)
                target.setUp()
                self.addCleanup(target.tearDown)
            sequence = target._sequence(); target._send(sequence["sequence_id"]); due, _ = target._due_and_requested(sequence["sequence_id"])
            reply = FixtureReplyProvider({
                "provider_event_id": f"reply-{category}", "sequence_id": sequence["sequence_id"], "lead_id": target.lead_id,
                "outreach_id": f"fixture-outreach-{sequence['sequence_id']}", "message_id": "fixture-reply-message", "thread_id": "fixture-thread",
                "received_at": iso(NOW + timedelta(days=4)), "sender": target.recipient, "recipient": SENDER,
                "category": category, "text": "Fictional reply text.", "classification_source": "FIXTURE_CATEGORY", "operator_review_status": "NOT_REQUIRED",
            })
            result = ingest_fixture_reply(target.database, sequence_id=sequence["sequence_id"], provider=reply, provider_event_id=f"reply-{category}", fixture_override=True, now=NOW + timedelta(days=4))
            self.assertEqual(result["state"], "HUMAN_ACTION_REQUIRED")
            self.assertEqual(target.database.connection.execute("SELECT state FROM followup_actions WHERE id=?", (due["action_id"],)).fetchone()[0], "CANCELLED")

    def test_not_interested_closes_without_global_suppression(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"])
        provider = FixtureReplyProvider(self._reply(sequence["sequence_id"], "NOT_INTERESTED", "not-interested"))
        result = ingest_fixture_reply(self.database, sequence_id=sequence["sequence_id"], provider=provider, provider_event_id="not-interested", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(result["state"], "CLOSED_NOT_INTERESTED"); self.assertFalse(self.database.is_suppressed(lead_id=self.lead_id))

    def test_explicit_opt_out_suppresses_and_stops_sequence(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"])
        provider = FixtureReplyProvider(self._reply(sequence["sequence_id"], "OPT_OUT", "opt-out"))
        result = ingest_fixture_reply(self.database, sequence_id=sequence["sequence_id"], provider=provider, provider_event_id="opt-out", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(result["state"], "SUPPRESSED"); self.assertTrue(self.database.is_suppressed(lead_id=self.lead_id))

    def test_out_of_office_preserves_only_explicit_return_date(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"])
        without_date = ingest_fixture_reply(self.database, sequence_id=sequence["sequence_id"], provider=FixtureReplyProvider(self._reply(sequence["sequence_id"], "OUT_OF_OFFICE", "ooo-1")), provider_event_id="ooo-1", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(without_date["state"], "OUT_OF_OFFICE_HOLD"); self.assertIsNone(without_date["return_at"])
        sequence2 = self._sequence(); self._send(sequence2["sequence_id"])
        event = self._reply(sequence2["sequence_id"], "OUT_OF_OFFICE", "ooo-2"); event["return_at"] = "2026-09-20T12:00:00+00:00"
        with_date = ingest_fixture_reply(self.database, sequence_id=sequence2["sequence_id"], provider=FixtureReplyProvider(event), provider_event_id="ooo-2", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(with_date["state"], "OUT_OF_OFFICE_HOLD"); self.assertEqual(with_date["return_at"], "2026-09-20T12:00:00+00:00")

    def test_soft_and_hard_bounce_have_different_safe_behaviors(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"])
        soft = ingest_fixture_reply(self.database, sequence_id=sequence["sequence_id"], provider=FixtureReplyProvider(self._reply(sequence["sequence_id"], "SOFT_BOUNCE", "soft")), provider_event_id="soft", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(soft["state"], "BOUNCE_HOLD"); self.assertFalse(self.database.is_suppressed(lead_id=self.lead_id))
        sequence2 = self._sequence(); self._send(sequence2["sequence_id"])
        hard = ingest_fixture_reply(self.database, sequence_id=sequence2["sequence_id"], provider=FixtureReplyProvider(self._reply(sequence2["sequence_id"], "HARD_BOUNCE", "hard")), provider_event_id="hard", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(hard["state"], "SUPPRESSED"); self.assertTrue(self.database.is_suppressed(email=self.recipient))
        self.assertEqual(self.database.connection.execute("SELECT hard_bounce_count FROM followup_sequences WHERE id=?", (sequence2["sequence_id"],)).fetchone()[0], 1)

    def test_unknown_reply_stays_hold_until_operator_classification(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"])
        result = ingest_fixture_reply(self.database, sequence_id=sequence["sequence_id"], provider=FixtureReplyProvider(self._reply(sequence["sequence_id"], "UNKNOWN", "unknown")), provider_event_id="unknown", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(result["state"], "HOLD_FOR_REVIEW")
        classified = classify_fixture_reply(self.database, reply_event_id=result["reply_event_id"], reviewer_identity="OPERATOR", category="QUESTION_OR_NEUTRAL", reason="Fictional operator classification.", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(classified["state"], "HUMAN_ACTION_REQUIRED")

    def test_reply_event_id_is_immutable_and_duplicate_ingestion_is_idempotent(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"]); provider = FixtureReplyProvider(self._reply(sequence["sequence_id"], "QUESTION_OR_NEUTRAL", "duplicate-reply"))
        first = ingest_fixture_reply(self.database, sequence_id=sequence["sequence_id"], provider=provider, provider_event_id="duplicate-reply", fixture_override=True, now=NOW + timedelta(days=1))
        second = ingest_fixture_reply(self.database, sequence_id=sequence["sequence_id"], provider=provider, provider_event_id="duplicate-reply", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertTrue(second["reused"]); self.assertEqual(first["reply_event_id"], second["reply_event_id"]); self.assertEqual(provider.calls, 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM followup_reply_events").fetchone()[0], 1)

    def test_reply_after_due_cancels_pending_action(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"]); due, _ = self._due_and_requested(sequence["sequence_id"])
        result = ingest_fixture_reply(self.database, sequence_id=sequence["sequence_id"], provider=FixtureReplyProvider(self._reply(sequence["sequence_id"], "POSITIVE_INTEREST", "after-due")), provider_event_id="after-due", fixture_override=True, now=NOW + timedelta(days=3, hours=1))
        self.assertEqual(result["state"], "HUMAN_ACTION_REQUIRED"); self.assertEqual(self.database.connection.execute("SELECT state FROM followup_actions WHERE id=?", (due["action_id"],)).fetchone()[0], "CANCELLED")

    def test_stale_evidence_invalidates_pending_followup_request(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"]); due, _ = self._due_and_requested(sequence["sequence_id"])
        evidence_id = self.database.connection.execute("SELECT id FROM evidence WHERE lead_id=? AND signal_key='personalization_observation'", (self.lead_id,)).fetchone()[0]
        self.database.connection.execute("UPDATE evidence SET pipeline_current=0 WHERE id=?", (evidence_id,)); self.database.connection.commit()
        result = process_due_followups(self.database, sequence_id=sequence["sequence_id"], now=NOW + timedelta(days=4), fixture_override=True)
        self.assertIn("STALE_FOLLOWUP_REQUEST", result["blocking_reasons"]); self.assertEqual(self.database.connection.execute("SELECT state FROM followup_actions WHERE id=?", (due["action_id"],)).fetchone()[0], "CANCELLED")

    def test_cancel_and_close_are_bounded_and_terminal_states_do_not_reopen(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"])
        cancelled = cancel_pending_followup_actions(self.database, sequence_id=sequence["sequence_id"], reason="Fictional operator hold.", fixture_override=True, now=NOW + timedelta(days=1))
        self.assertEqual(cancelled["state"], "HOLD_FOR_REVIEW")
        closed = close_no_response_sequence(self.database, sequence_id=sequence["sequence_id"], reason="Fictional close.", fixture_override=True, now=NOW + timedelta(days=2))
        self.assertEqual(closed["state"], "CLOSED_NO_RESPONSE")
        again = process_due_followups(self.database, sequence_id=sequence["sequence_id"], now=NOW + timedelta(days=3), fixture_override=True)
        self.assertIn("TERMINAL_STATE", again["blocking_reasons"])

    def test_reply_provider_has_no_outbound_capability_and_policy_is_fixture_only(self):
        provider = FixtureReplyProvider(self._reply(1, "UNKNOWN", "surface"))
        self.assertTrue(callable(provider.get_reply)); self.assertEqual(FOLLOWUP_REPLY_CATEGORIES, {"POSITIVE_INTEREST", "QUESTION_OR_NEUTRAL", "NOT_INTERESTED", "OPT_OUT", "OUT_OF_OFFICE", "SOFT_BOUNCE", "HARD_BOUNCE", "UNKNOWN"})
        for name in ("send", "send_reply", "create_draft", "schedule", "forward"):
            self.assertFalse(hasattr(provider, name), name)
        self.assertEqual(self.database.read_config()["followup_policy_version"], "TEST_FOLLOWUP_V0_1")

    def test_provenance_is_safe_and_has_no_message_body(self):
        sequence = self._sequence(); self._send(sequence["sequence_id"])
        provenance = summarize_followup_provenance(self.database, sequence_id=sequence["sequence_id"])
        self.assertEqual(provenance["policy_version"], "TEST_FOLLOWUP_V0_1")
        self.assertNotIn("subject", provenance); self.assertNotIn("body", provenance); self.assertNotIn("text", provenance)
        self.assertIn("transitions", provenance); self.assertIn("sent_touches", provenance)

    def _reply(self, sequence_id, category, provider_event_id):
        return {
            "provider_event_id": provider_event_id, "sequence_id": sequence_id, "lead_id": self.lead_id,
            "outreach_id": f"fixture-outreach-{sequence_id}", "message_id": f"fixture-reply-message-{provider_event_id}", "thread_id": f"fixture-thread-{sequence_id}",
            "received_at": iso(NOW + timedelta(days=1)), "sender": self.recipient, "recipient": SENDER,
            "category": category, "text": "Fictional reply text.", "classification_source": "FIXTURE_CATEGORY", "operator_review_status": "NOT_REQUIRED" if category != "UNKNOWN" else "PENDING",
        }


if __name__ == "__main__":
    unittest.main()
