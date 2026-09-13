import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from paldo_os_outbound import Database
from step10_drafting import DRAFTING_POLICY_VERSION, IMMEDIATE_OFFER_CTA
from step17b_pilot_policy import (
    APPROVED_CASE_STUDY_URL,
    CASE_STUDY_PLACEHOLDER,
    DEFAULT_PILOT_POLICY,
    MAX_OUTBOUND_TOUCHES,
    PilotPolicyValidationError,
    build_pilot_policy_contract,
    evaluate_case_study_readiness,
    evaluate_pilot_runtime_readiness,
    evaluate_touch_limit,
    validate_pilot_message,
)


IDENTITY = "I’m a local automation specialist working with owner-led service businesses."
PROOF = "I helped move Harborview Services’s paper-based client records into a searchable digital system."


class Step17BPilotPolicyTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.tempdir.name) / "step17b.sqlite3")

    def tearDown(self):
        self.database.close()
        self.tempdir.cleanup()

    def _message(self, channel="EMAIL", link=CASE_STUDY_PLACEHOLDER, extra="", fixture=True):
        return {
            "channel": channel,
            "subject": "client records",
            "body": f"{IDENTITY}\n\n{PROOF}\n\n{link}\n\nIf this is relevant, I’d be interested in your take.{extra}",
            "case_study_url": link,
            "fixture": fixture,
        }

    def test_policy_isolated_from_existing_drafting_policy_and_proposed(self):
        contract = build_pilot_policy_contract()
        self.assertEqual(contract["policy_version"], "PILOT_V0_1")
        self.assertNotEqual(contract["policy_version"], DRAFTING_POLICY_VERSION)
        self.assertEqual(contract["status"], "PROPOSED")
        self.assertFalse(contract["production_approved"])
        self.assertEqual(contract["existing_drafting_policy_version"], "TEST_V0_1")
        self.assertEqual(contract["channel_request_status"], "PENDING_POLICY")
        self.assertEqual(contract["case_study_url"], APPROVED_CASE_STUDY_URL)
        self.assertEqual(contract["max_outbound_touches"], MAX_OUTBOUND_TOUCHES)

    def test_missing_or_unapproved_url_blocks_real_drafting(self):
        missing = evaluate_case_study_readiness(DEFAULT_PILOT_POLICY, fixture=False)
        self.assertEqual(missing["state"], "NOT_READY")
        self.assertIn("CASE_STUDY_URL_UNSET", missing["reasons"])
        unapproved = evaluate_case_study_readiness(
            DEFAULT_PILOT_POLICY,
            fixture=False,
            requested_url="https://case.example/harborview",
        )
        self.assertEqual(unapproved["state"], "NOT_READY")
        self.assertIn("URL_NOT_EXACT_APPROVED_EXCEPTION", unapproved["reasons"])

    def test_placeholder_is_fixture_only(self):
        fixture = evaluate_case_study_readiness(DEFAULT_PILOT_POLICY, fixture=True, requested_url=CASE_STUDY_PLACEHOLDER)
        self.assertEqual(fixture["state"], "READY_FIXTURE")
        real = validate_pilot_message(self._message(fixture=False), DEFAULT_PILOT_POLICY)
        self.assertFalse(real["valid"])
        self.assertIn("PLACEHOLDER_NOT_ALLOWED_FOR_REAL", real["error_codes"])

    def test_exact_operator_approved_https_url_is_the_only_real_exception(self):
        policy = replace(DEFAULT_PILOT_POLICY, approved_case_study_url="https://case.example/harborview")
        accepted = validate_pilot_message(
            {
                "channel": "EMAIL",
                "subject": "client records",
                "body": f"{IDENTITY}\n\n{PROOF}\n\nhttps://case.example/harborview\n\nIf relevant, I’d be interested in your take.",
                "case_study_url": "https://case.example/harborview",
                "fixture": False,
            },
            policy,
        )
        self.assertTrue(accepted["valid"], accepted)
        other = validate_pilot_message(
            {
                "channel": "EMAIL",
                "subject": "client records",
                "body": f"{IDENTITY}\n\n{PROOF}\n\nhttps://other.example/case\n\nIf relevant, I’d be interested in your take.",
                "case_study_url": "https://other.example/case",
                "fixture": False,
            },
            policy,
        )
        self.assertFalse(other["valid"])
        self.assertIn("URL_NOT_EXACT_APPROVED_EXCEPTION", other["error_codes"])
        tracking = validate_pilot_message(
            {
                "channel": "EMAIL",
                "subject": "client records",
                "body": f"{IDENTITY}\n\n{PROOF}\n\nhttps://case.example/harborview?utm_source=test\n\nIf relevant, I’d be interested in your take.",
                "case_study_url": "https://case.example/harborview?utm_source=test",
                "fixture": False,
            },
            policy,
        )
        self.assertFalse(tracking["valid"])
        self.assertIn("URL_NOT_EXACT_APPROVED_EXCEPTION", tracking["error_codes"])

    def test_identity_and_harborview_naming_are_exact(self):
        result = validate_pilot_message(self._message(), DEFAULT_PILOT_POLICY)
        self.assertTrue(result["valid"], result)
        self.assertIn(IDENTITY, result["normalized_body"])
        self.assertIn("Harborview Services", result["normalized_body"])
        self.assertNotIn("I built Harborview", result["normalized_body"])
        self.assertNotIn("Operator builds", result["normalized_body"])
        no_year = self._message(extra=" I’m a third-year student.")
        invalid = validate_pilot_message(no_year, DEFAULT_PILOT_POLICY)
        self.assertIn("UNAPPROVED_IDENTITY_DETAIL", invalid["error_codes"])

    def test_unsupported_proof_and_invented_pain_are_rejected(self):
        proof = self._message(extra=" This increased bookings and reduced no-shows.")
        proof_result = validate_pilot_message(proof, DEFAULT_PILOT_POLICY)
        self.assertIn("UNSUPPORTED_PROOF_CLAIM", proof_result["error_codes"])
        pain = self._message(extra=" Your staff is losing leads and forgetting follow-ups.")
        pain_result = validate_pilot_message(pain, DEFAULT_PILOT_POLICY)
        self.assertIn("UNSUPPORTED_PAIN_ASSERTION", pain_result["error_codes"])

    def test_channel_specific_cta_and_link_behavior(self):
        email = validate_pilot_message(self._message("EMAIL"), DEFAULT_PILOT_POLICY)
        self.assertTrue(email["valid"], email)
        connection = validate_pilot_message(self._message("LINKEDIN_CONNECTION"), DEFAULT_PILOT_POLICY)
        self.assertIn("LINKEDIN_CONNECTION_LINK_FORBIDDEN", connection["error_codes"])
        post_acceptance = validate_pilot_message(self._message("LINKEDIN_MESSAGE"), DEFAULT_PILOT_POLICY)
        self.assertTrue(post_acceptance["valid"], post_acceptance)
        call_cta = self._message(extra=f" {IMMEDIATE_OFFER_CTA.replace('[Business Name]', 'Luna Skin Studio')}")
        call_result = validate_pilot_message(call_cta, DEFAULT_PILOT_POLICY)
        self.assertIn("CALL_CTA_NOT_COMBINED_WITH_CASE_STUDY", call_result["error_codes"])

    def test_shared_touch_limit_and_reply_stop(self):
        self.assertTrue(evaluate_touch_limit(0, replied=False)["allowed"])
        self.assertTrue(evaluate_touch_limit(2, replied=False)["allowed"])
        exhausted = evaluate_touch_limit(3, replied=False)
        self.assertFalse(exhausted["allowed"])
        self.assertIn("SHARED_TOUCH_LIMIT_REACHED", exhausted["reasons"])
        replied = evaluate_touch_limit(0, replied=True)
        self.assertFalse(replied["allowed"])
        self.assertIn("REPLY_STOPS_COLD_SEQUENCE", replied["reasons"])

    def test_runtime_readiness_is_fixture_only_and_preserves_safe_state(self):
        canonical = evaluate_pilot_runtime_readiness(self.database, fixture_override=False)
        self.assertEqual(canonical["state"], "BLOCKED")
        self.assertIn("FIXTURE_ONLY_REQUIRED", canonical["reasons"])
        config = self.database.read_config()
        self.assertEqual(config["system_state"], "PAUSED")
        self.assertEqual(config["daily_message_cap"], 0)
        self.assertEqual(config["gmail_send_enabled"], 0)
        self.assertEqual(config["linkedin_sending_enabled"], 0)
        self.assertEqual(config["linkedin_message_policy_status"], "UNDECIDED")

    def test_no_attachments_shorteners_tracking_or_extra_links(self):
        extra_link = self._message(extra=" See https://other.example too.")
        result = validate_pilot_message(extra_link, DEFAULT_PILOT_POLICY)
        self.assertIn("ONLY_CASE_STUDY_LINK_ALLOWED", result["error_codes"])
        attachment = self._message(extra=" attachment: proposal.pdf")
        attachment_result = validate_pilot_message(attachment, DEFAULT_PILOT_POLICY)
        self.assertIn("ATTACHMENTS_FORBIDDEN", attachment_result["error_codes"])


if __name__ == "__main__":
    unittest.main()
