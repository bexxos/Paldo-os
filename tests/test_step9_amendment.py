import tempfile
import unittest
from pathlib import Path

from paldo_os_outbound import Database
from step9_audit_offer import (
    AUDIT_DEFERRED_REASON,
    AUDIT_OFFER_KEY,
    IMMEDIATE_OFFER_CTA,
    IMMEDIATE_OFFER_KEY,
    IMMEDIATE_OFFER_POSITIONING,
    PORTFOLIO_CONTACT_DECISION,
    AuditOfferLifecycleError,
    DraftingOfferNotReadyError,
    activate_immediate_offer,
    activate_audit_offer_version,
    approve_immediate_offer,
    approve_audit_offer_version,
    get_active_offer_for_drafting,
    get_audit_offer_version,
    get_immediate_offer_version,
    migrate_step9,
)


class Step9AmendmentTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Database(Path(self.temp_dir.name) / "fictional-amendment.sqlite3")
        self.database.migrate_step2()
        migrate_step9(self.database)

    def tearDown(self):
        self.database.close()
        self.temp_dir.cleanup()

    def test_deferred_audit_remains_proposed_inactive_and_unavailable_to_drafting(self):
        audit = get_audit_offer_version(self.database)
        self.assertEqual(audit["offer_key"], AUDIT_OFFER_KEY)
        self.assertEqual(audit["status"], "PROPOSED")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM audit_offer_versions WHERE offer_key=? AND status='ACTIVE'", (AUDIT_OFFER_KEY,)).fetchone()[0], 0)
        with self.assertRaises(DraftingOfferNotReadyError):
            get_active_offer_for_drafting(self.database)
        self.assertEqual(self.database.read_config()["audit_offer_state"], "PROPOSED")
        self.assertEqual(self.database.read_config()["audit_deferred_reason"], AUDIT_DEFERRED_REASON)

    def test_deferred_audit_has_no_gold_promotion(self):
        audit = get_audit_offer_version(self.database)
        self.assertEqual(audit["status"], "PROPOSED")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM kb_context_packets").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM audit_deliverables").fetchone()[0], 0)

    def test_immediate_offer_has_approved_name_target_positioning_and_cta(self):
        offer = get_immediate_offer_version(self.database)
        self.assertEqual(offer["offer_key"], IMMEDIATE_OFFER_KEY)
        self.assertEqual(offer["name"], "Business Booking and Follow-Up System")
        self.assertIn("Owner-led appointment businesses", offer["target_icp"])
        self.assertIn("Primary region local services", offer["target_icp"])
        self.assertEqual(offer["positioning"], IMMEDIATE_OFFER_POSITIONING)
        self.assertEqual(offer["ctas"]["PRIMARY"], IMMEDIATE_OFFER_CTA)

    def test_only_audited_operator_can_approve_immediate_offer(self):
        offer = get_immediate_offer_version(self.database)
        with self.assertRaises(AuditOfferLifecycleError):
            approve_audit_offer_version(self.database, offer["id"], reviewer_identity="AI", reason="fictional AI approval")
        approved = approve_immediate_offer(self.database, reviewer_identity="OPERATOR", reason="Operator explicitly approved the immediate service offer")
        self.assertEqual(approved["status"], "APPROVED")
        event = self.database.connection.execute("SELECT actor_identity, actor_type, to_status FROM audit_offer_approval_events WHERE offer_version_id=? ORDER BY id DESC LIMIT 1", (offer["id"],)).fetchone()
        self.assertEqual(tuple(event), ("OPERATOR", "OPERATOR", "APPROVED"))

    def test_active_requires_prior_approved_for_immediate_offer(self):
        offer = get_immediate_offer_version(self.database)
        with self.assertRaises(AuditOfferLifecycleError):
            activate_immediate_offer(self.database, actor_identity="OPERATOR")
        approve_immediate_offer(self.database, reviewer_identity="OPERATOR", reason="Operator explicitly approved the immediate service offer")
        active = activate_immediate_offer(self.database, actor_identity="OPERATOR")
        self.assertEqual(active["status"], "ACTIVE")

    def test_only_one_offer_can_be_active_globally(self):
        immediate = get_immediate_offer_version(self.database)
        approve_immediate_offer(self.database, reviewer_identity="OPERATOR", reason="Operator explicitly approved the immediate service offer")
        activate_immediate_offer(self.database, actor_identity="OPERATOR")
        audit = get_audit_offer_version(self.database)
        approve_audit_offer_version(self.database, audit["id"], reviewer_identity="OPERATOR", reason="fictional approval attempt")
        with self.assertRaises(AuditOfferLifecycleError):
            activate_audit_offer_version(self.database, audit["id"], actor_identity="OPERATOR")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM audit_offer_versions WHERE status='ACTIVE'").fetchone()[0], 1)
        self.assertEqual(self.database.connection.execute("SELECT offer_key FROM audit_offer_versions WHERE status='ACTIVE'").fetchone()[0], immediate["offer_key"])

    def test_activation_does_not_activate_campaign_or_system(self):
        approve_immediate_offer(self.database, reviewer_identity="OPERATOR", reason="Operator explicitly approved the immediate service offer")
        activate_immediate_offer(self.database, actor_identity="OPERATOR")
        config = self.database.read_config()
        self.assertEqual(config["system_state"], "PAUSED")
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM campaigns WHERE status='ACTIVE'").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], 0)

    def test_immediate_cta_requires_no_audit_website_form_n8n_or_scheduler(self):
        offer = get_immediate_offer_version(self.database)
        self.assertEqual(offer["cta_requirements"], {
            "reply_by_email": True,
            "requires_automated_audit": False,
            "requires_n8n_form": False,
            "requires_website": False,
            "requires_scheduling_platform": False,
            "requires_live_booking_server": False,
            "manual_email_scheduling_allowed": True,
        })

    def test_no_automatic_email_or_follow_up_is_created(self):
        approve_immediate_offer(self.database, reviewer_identity="OPERATOR", reason="Operator explicitly approved the immediate service offer")
        activate_immediate_offer(self.database, actor_identity="OPERATOR")
        for table in ("audit_requests", "audit_deliverables", "outreach"):
            self.assertEqual(self.database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0)

    def test_portfolio_contact_decision_is_direct_email_and_audit_cta_deferred(self):
        self.assertEqual(PORTFOLIO_CONTACT_DECISION["main_ctas"], ["View My Work", "Email Me"])
        self.assertEqual(PORTFOLIO_CONTACT_DECISION["client_cta"], "Discuss an Automation")
        self.assertEqual(PORTFOLIO_CONTACT_DECISION["employer_cta"], "Email Me About a Role")
        self.assertTrue(PORTFOLIO_CONTACT_DECISION["direct_email_visible_and_copyable"])
        self.assertFalse(PORTFOLIO_CONTACT_DECISION["depends_on_local_n8n"])
        self.assertEqual(PORTFOLIO_CONTACT_DECISION["public_audit_cta"], "DEFERRED")

    def test_amendment_preserves_zero_caps_and_inactive_campaigns(self):
        config = self.database.read_config()
        self.assertEqual(config["system_state"], "PAUSED")
        self.assertEqual(config["discovery_mode"], "DRY_RUN")
        self.assertEqual(config["daily_candidate_processing_cap"], 0)
        self.assertEqual(config["daily_message_cap"], 0)
        self.assertEqual(config["audit_request_daily_cap"], 0)
        self.assertEqual(config["google_places_daily_request_cap"], 0)
        self.assertEqual(config["apify_daily_run_cap"], 0)
        self.assertEqual(config["website_enrichment_daily_quota"], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM campaigns WHERE status='ACTIVE'").fetchone()[0], 0)

    def test_step9_amendment_does_not_add_a_schema_migration(self):
        self.assertEqual(migrate_step9(self.database), 11)
        self.assertEqual(self.database.connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0], 11)


if __name__ == "__main__":
    unittest.main()
