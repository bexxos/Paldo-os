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
from step11_gmail_drafts import (
    GMAIL_ALLOWED_ACTIONS,
    GMAIL_PROVIDER_MODE,
    GmailDraftBlockedError,
    GmailDraftValidationError,
    FixtureGmailDraftProvider,
    approve_gmail_draft_creation,
    create_fixture_gmail_draft,
    get_current_external_draft_mapping,
    inspect_gmail_draft_mismatches,
    inspect_gmail_draft_readiness,
    invoke_gmail_provider_action,
    migrate_step11,
    preview_gmail_draft_payload,
    reconcile_unknown_gmail_draft,
    summarize_gmail_draft_provenance,
    validate_gmail_draft_payload,
    verify_fixture_gmail_draft,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
SENDER = "operator@fictional-example.invalid"


def iso(value):
    return value.astimezone(UTC).replace(microsecond=0).isoformat()


def gold_records(campaign_name):
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
            "source_id": f"step11-gold-{i}", "status": "Gold", "trust_tier": "Gold", "approval_status": "APPROVED",
            "approved_by": "OPERATOR", "confidence": 0.96, "heading": section,
            "drive_file_id": f"drive-step11-{i}", "kb_path": f"gold/step11/{i}",
            "source_url": f"https://fictional-kb.invalid/step11/{i}", "modified_at": iso(NOW - timedelta(days=1)),
            "snapshot_revision": "fictional-step11-gold-rev-1", "manifest_revision": "fictional-step11-gold-rev-1",
            "content_hash": hashlib.sha256(content.encode()).hexdigest(), "retrieved_at": iso(NOW),
            "section": section, "content": content,
        }
        for i, (section, content) in enumerate(contents.items(), 1)
    ]


class Step11GmailDraftTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.database = Database(Path(self.temp_dir.name) / "step11.sqlite3")
        self.addCleanup(self.database.close)
        migrate_step11(self.database)
        self.campaign = self.database.get_campaign_by_name("Primary region local services")
        self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))
        self.database.connection.commit()
        for key, value in {
            "system_state": "ACTIVE", "drafting_enabled": 1, "drafting_daily_cap": 20,
            "drafting_provider_mode": "FIXTURE_ONLY", "gmail_draft_integration_enabled": 1,
            "gmail_draft_daily_cap": 5, "gmail_provider_mode": "FIXTURE_ONLY",
            "gmail_account_configured": 1, "gmail_send_enabled": 0,
            "gmail_allowed_actions": "CREATE_DRAFT,GET_DRAFT",
        }.items():
            self.database.set_config(key, value)
        self.offer = get_immediate_offer_version(self.database)
        approve_immediate_offer(self.database, reason="Fictional Step 11 fixture approval.")
        activate_immediate_offer(self.database)
        self.packet = build_context_packet(self.database, self.campaign["id"], gold_records(self.campaign["name"]), now=NOW)
        self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))
        self.database.connection.commit()
        self.lead_id, self.step10_draft = self._make_step10_draft()

    def _make_step10_draft(self):
        recipient = "bookings@fictional-glow.fictional-business.invalid"
        lead_id = self.database.insert_lead(
            business_name="Fictional Glow Business", website=None, email=recipient,
            industry="local service local business", country="Philippines", location="Northfield",
            source="FIXTURE", source_url="https://fictional-business.invalid/listing",
            status="QUALIFIED", campaign_id=self.campaign["id"],
        )
        evidence = [
            ("campaign_fit", "The fictional public business listing identifies an local service appointment business in Northfield.", "FIXTURE_PUBLIC_WEB", "https://fictional-business.invalid/listing", .95),
            ("personalization_observation", "The fictional public listing says appointment inquiries are accepted through Facebook Messenger.", "FIXTURE_PUBLIC_WEB", "https://fictional-business.invalid/contact", .95),
            ("public_business_email", "The fictional public business contact route is verified for this fixture.", "FIXTURE_VERIFIED_EMAIL", "https://fictional-business.invalid/contact", .99),
        ]
        for signal, observation, source_type, source_url, confidence in evidence:
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
        result = generate_fictional_draft(
            self.database, lead_id=lead_id, campaign_id=self.campaign["id"], provider=FixtureDraftingProvider(output=output),
            packet_id=self.packet["packet_id"], now=NOW,
        )
        self.assertEqual(result["state"], "REVIEW_PENDING")
        return lead_id, result["draft"]

    def approve(self, **overrides):
        args = {
            "draft_id": self.step10_draft["draft_id"], "reviewer_identity": "OPERATOR",
            "recipient_email": self.database.get_lead(self.lead_id)["email"], "sender_email": SENDER,
            "reason": "Fictional operator approved fixture Gmail draft creation for boundary testing.",
            "fixture_override": True, "now": NOW,
        }
        args.update(overrides)
        return approve_gmail_draft_creation(self.database, **args)

    def test_migration_is_idempotent_and_defaults_are_safe(self):
        self.assertEqual(migrate_step11(self.database), 13)
        self.assertEqual(migrate_step11(self.database), 13)
        tables = {row[0] for row in self.database.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"gmail_draft_creation_runs", "gmail_draft_creation_approvals", "gmail_draft_provider_attempts", "gmail_external_draft_mappings", "gmail_draft_verification_results", "gmail_draft_reconciliation_events"} <= tables)
        fresh = Database(Path(self.temp_dir.name) / "fresh.sqlite3")
        self.addCleanup(fresh.close)
        migrate_step11(fresh)
        config = fresh.read_config()
        self.assertFalse(config["gmail_draft_integration_enabled"])
        self.assertEqual(config["gmail_draft_daily_cap"], 0)
        self.assertEqual(config["gmail_provider_mode"], GMAIL_PROVIDER_MODE)
        self.assertFalse(config["gmail_account_configured"])
        self.assertFalse(config["gmail_send_enabled"])
        self.assertEqual(config["gmail_allowed_actions"], "CREATE_DRAFT,GET_DRAFT")

    def test_valid_review_pending_draft_can_be_approved_by_operator(self):
        result = self.approve()
        self.assertEqual(result["state"], "APPROVED_FOR_DRAFT_CREATION")
        self.assertEqual(result["reviewer_identity"], "OPERATOR")
        self.assertEqual(result["draft_id"], self.step10_draft["draft_id"])
        self.assertTrue(result["content_hash"])

    def test_non_operator_approval_is_rejected(self):
        with self.assertRaises(GmailDraftValidationError):
            self.approve(reviewer_identity="AI")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM gmail_draft_creation_approvals").fetchone()[0], 0)

    def test_unvalidated_suppressed_and_recipient_mismatch_are_blocked(self):
        self.database.connection.execute("UPDATE draft_validation_results SET validation_state='FAIL' WHERE draft_id=?", (self.step10_draft["draft_id"],))
        self.database.connection.commit()
        self.assertIn("STEP10_VALIDATION_REQUIRED", self.approve()["blocking_reasons"])
        self.database.connection.execute("UPDATE draft_validation_results SET validation_state='PASS' WHERE draft_id=?", (self.step10_draft["draft_id"],))
        self.database.add_suppression(lead_id=self.lead_id, reason="fictional opt-out")
        self.assertIn("SUPPRESSED", self.approve()["blocking_reasons"])
        self.database.connection.execute("DELETE FROM suppressions WHERE lead_id=?", (self.lead_id,))
        self.database.connection.commit()
        self.assertIn("RECIPIENT_MISMATCH", self.approve(recipient_email="other@fictional-business.invalid")["blocking_reasons"])

    def test_changed_evidence_invalidates_approval(self):
        approved = self.approve()
        evidence_id = self.database.connection.execute("SELECT id FROM evidence WHERE lead_id=? AND signal_key='personalization_observation'", (self.lead_id,)).fetchone()[0]
        self.database.connection.execute("UPDATE evidence SET pipeline_current=0 WHERE id=?", (evidence_id,))
        self.database.connection.commit()
        result = create_fixture_gmail_draft(self.database, run_id=approved["run_id"], provider=FixtureGmailDraftProvider(), fixture_override=True, now=NOW)
        self.assertEqual(result["state"], "BLOCKED")
        self.assertIn("APPROVAL_INVALIDATED", result["blocking_reasons"])
        self.assertEqual(self.database.connection.execute("SELECT status FROM gmail_draft_creation_approvals WHERE id=?", (approved["approval_id"],)).fetchone()[0], "INVALIDATED")

    def test_preview_payload_is_deterministic_and_bounded(self):
        preview = preview_gmail_draft_payload(self.database, draft_id=self.step10_draft["draft_id"], sender_email=SENDER, fixture_override=True, now=NOW)
        self.assertTrue(preview["ready"])
        payload = preview["payload"]
        self.assertEqual(set(payload), {"to", "from", "subject", "body", "content_hash", "mime"})
        self.assertEqual(payload["to"], [self.database.get_lead(self.lead_id)["email"]])
        self.assertEqual(payload["from"], SENDER)
        self.assertNotIn("Cc:", payload["mime"])
        self.assertNotIn("Bcc:", payload["mime"])
        self.assertIn(f"X-Paldo-Content-Hash: {payload['content_hash']}", payload["mime"])

    def test_payload_validator_rejects_multiple_recipients_cc_bcc_html_attachment_tracking_and_links(self):
        payload = preview_gmail_draft_payload(self.database, draft_id=self.step10_draft["draft_id"], sender_email=SENDER, fixture_override=True, now=NOW)["payload"]
        for mutation in (
            {"to": [payload["to"][0], "second@fictional-business.invalid"]}, {"cc": ["copy@fictional.invalid"]},
            {"bcc": ["blind@fictional.invalid"]}, {"body": "<html>unsafe</html>"},
            {"attachments": ["https://fictional-business.invalid/file"]}, {"body": payload["body"] + " https://fictional-business.invalid/track"},
        ):
            candidate = dict(payload); candidate.update(mutation)
            check = validate_gmail_draft_payload(candidate, expected_to=payload["to"][0], expected_from=SENDER, expected_subject=payload["subject"], expected_body=payload["body"], expected_content_hash=payload["content_hash"])
            self.assertFalse(check["valid"], mutation)

    def test_canonical_test_policy_and_fixture_storage_guards_block(self):
        readiness = inspect_gmail_draft_readiness(self.database, draft_id=self.step10_draft["draft_id"], sender_email=SENDER, fixture_override=False, now=NOW)
        self.assertFalse(readiness["ready"])
        self.assertIn("FIXTURE_OVERRIDE_REQUIRED", readiness["blocking_reasons"])
        with self.assertRaises(GmailDraftBlockedError):
            self.approve(fixture_override=False)
        canonical = Database(DEFAULT_DB_PATH)
        self.addCleanup(canonical.close)
        migrate_step11(canonical)
        with self.assertRaises(GmailDraftBlockedError):
            preview_gmail_draft_payload(canonical, draft_id=self.step10_draft["draft_id"], sender_email=SENDER, fixture_override=True, now=NOW)

    def test_integration_cap_and_account_gates_block(self):
        for key, value, reason in (("gmail_draft_integration_enabled", 0, "GMAIL_INTEGRATION_DISABLED"), ("gmail_draft_daily_cap", 0, "GMAIL_DRAFT_DAILY_CAP_ZERO"), ("gmail_account_configured", 0, "GMAIL_ACCOUNT_NOT_CONFIGURED")):
            self.database.set_config(key, value)
            result = inspect_gmail_draft_readiness(self.database, draft_id=self.step10_draft["draft_id"], sender_email=SENDER, fixture_override=True, now=NOW)
            self.assertIn(reason, result["blocking_reasons"])
            self.database.set_config(key, 5 if key == "gmail_draft_daily_cap" else 1)

    def test_provider_surface_allowlist_and_config_reject_sending(self):
        provider = FixtureGmailDraftProvider()
        self.assertEqual(GMAIL_ALLOWED_ACTIONS, {"CREATE_DRAFT", "GET_DRAFT"})
        self.assertTrue(callable(provider.create_draft)); self.assertTrue(callable(provider.get_draft))
        for name in ("send", "send_draft", "forward", "reply_and_send", "schedule_send", "bulk_send"):
            self.assertFalse(hasattr(provider, name), name)
        for action in ("SEND", "UNKNOWN", "MESSAGES.SEND"):
            with self.assertRaises(GmailDraftValidationError):
                invoke_gmail_provider_action(provider, action, {})
        with self.assertRaises(ValueError):
            self.database.set_config("gmail_send_enabled", 1)

    def test_create_is_idempotent_and_verified_fake_draft_reaches_ready_for_manual_send(self):
        approval = self.approve(); provider = FixtureGmailDraftProvider()
        created = create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(created["state"], "EXTERNAL_DRAFT_CREATED"); self.assertEqual(provider.create_calls, 1)
        repeated = create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertTrue(repeated["reused"]); self.assertEqual(provider.create_calls, 1)
        verified = verify_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(verified["state"], "READY_FOR_MANUAL_SEND"); self.assertEqual(verified["provider_status"], "DRAFT")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM gmail_external_draft_mappings").fetchone()[0], 1)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)

    def test_mismatch_is_hold_and_no_automatic_retry(self):
        approval = self.approve(); provider = FixtureGmailDraftProvider(mutation={"subject": "mutated fixture subject"})
        create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        result = verify_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(result["state"], "HOLD_FOR_RECONCILIATION"); self.assertIn("SUBJECT_MISMATCH", result["mismatch_codes"])
        again = create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(again["state"], "HOLD_FOR_RECONCILIATION"); self.assertEqual(provider.create_calls, 1)

    def test_interrupted_unknown_outcome_reconciles_without_second_create(self):
        approval = self.approve(); provider = FixtureGmailDraftProvider(interrupt_after_create=True)
        result = create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(result["state"], "HOLD_FOR_RECONCILIATION"); self.assertEqual(result["error_category"], "UNKNOWN_EXTERNAL_STATE")
        self.assertEqual(create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)["state"], "HOLD_FOR_RECONCILIATION")
        reconciled = reconcile_unknown_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(reconciled["state"], "READY_FOR_MANUAL_SEND"); self.assertEqual(provider.create_calls, 1); self.assertEqual(provider.get_calls, 1)

    def test_unknown_without_id_stays_hold_and_redraft_approval_is_not_implicit(self):
        approval = self.approve(); provider = FixtureGmailDraftProvider(interrupt_without_id=True)
        result = create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(result["state"], "HOLD_FOR_RECONCILIATION")
        reconciled = reconcile_unknown_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertIn("EXTERNAL_DRAFT_ID_REQUIRED", reconciled["blocking_reasons"]); self.assertEqual(provider.create_calls, 1)
        self.database.connection.execute("UPDATE personalized_drafts SET body=body || ' revised' WHERE id=?", (self.step10_draft["draft_id"],)); self.database.connection.commit()
        self.assertIn("DRAFT_CONTENT_CHANGED", inspect_gmail_draft_readiness(self.database, draft_id=self.step10_draft["draft_id"], sender_email=SENDER, fixture_override=True, now=NOW)["blocking_reasons"])

    def test_mapping_mismatch_and_provenance_are_safe(self):
        approval = self.approve(); provider = FixtureGmailDraftProvider(mutation={"body": "mutated body"})
        create_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        verify_fixture_gmail_draft(self.database, run_id=approval["run_id"], provider=provider, fixture_override=True, now=NOW)
        self.assertEqual(get_current_external_draft_mapping(self.database, run_id=approval["run_id"])["state"], "HOLD_FOR_RECONCILIATION")
        self.assertIn("BODY_MISMATCH", inspect_gmail_draft_mismatches(self.database, run_id=approval["run_id"])["mismatch_codes"])
        provenance = summarize_gmail_draft_provenance(self.database, run_id=approval["run_id"])
        self.assertEqual(provenance["policy_version"], "TEST_V0_1")
        for secret_field in ("body", "subject", "recipient_email"):
            self.assertNotIn(secret_field, provenance)

    def test_approval_does_not_create_sent_or_followup_records(self):
        result = self.approve()
        self.assertNotIn(result["state"], {"SENT", "FOLLOW_UP_SCHEDULED"})
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM gmail_external_draft_mappings").fetchone()[0], 0)

    def test_no_live_integration_symbols_or_real_data_are_used(self):
        source = Path(__file__).resolve().parents[1].joinpath("step11_gmail_drafts.py").read_text().lower()
        for forbidden in ("googleapiclient", "composio", "smtplib", "requests", "urllib.request", "send_draft", "messages.send", "oauth"):
            self.assertNotIn(forbidden, source)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM gmail_external_draft_mappings").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
