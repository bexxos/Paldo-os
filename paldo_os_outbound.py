"""Step 1 storage foundation for Paldo OS Outbound.

This module deliberately provides storage and validation primitives only. It does
not research leads, compose outreach, send messages, or integrate with external
systems.
"""

from enum import Enum
from pathlib import Path
import json
import math
import sqlite3
from datetime import datetime, timezone
from urllib.parse import urlsplit


DEFAULT_DB_PATH = Path(__file__).resolve().parent / "data" / "paldo_os_outbound.sqlite3"
REQUIRED_TABLES = {
    "leads",
    "evidence",
    "lead_scores",
    "offers",
    "lead_offers",
    "outreach",
    "suppressions",
    "events",
}


class LeadStatus(str, Enum):
    """Safe, finite lifecycle values for a lead."""

    DISCOVERED = "DISCOVERED"
    ENRICHED = "ENRICHED"
    QUALIFIED = "QUALIFIED"
    DRAFT_READY = "DRAFT_READY"
    REVIEW_PENDING = "REVIEW_PENDING"
    APPROVED = "APPROVED"
    SENT = "SENT"
    FOLLOWUP_DUE = "FOLLOWUP_DUE"
    REPLIED = "REPLIED"
    INTERESTED = "INTERESTED"
    CALL_BOOKED = "CALL_BOOKED"
    PROPOSAL = "PROPOSAL"
    WON = "WON"
    LOST = "LOST"
    REJECTED = "REJECTED"
    DO_NOT_CONTACT = "DO_NOT_CONTACT"
    BOUNCED = "BOUNCED"
    UNSUBSCRIBED = "UNSUBSCRIBED"
    DUPLICATE = "DUPLICATE"


SAFE_LEAD_STATUSES = frozenset(status.value for status in LeadStatus)


class DuplicateLeadError(ValueError):
    """Raised when a normalized email or domain already identifies a lead."""


class SystemState(str, Enum):
    """Finite operational states; the foundation starts safely paused."""

    PAUSED = "PAUSED"
    ACTIVE = "ACTIVE"


SAFE_SYSTEM_STATES = frozenset(state.value for state in SystemState)


_DEFAULT_CONFIG = {
    "system_state": ("PAUSED", "text"),
    "revenue_principle": (
        "We do not sell generic AI automation. The system will eventually identify "
        "business problems and match them to offers with measurable economic outcomes.",
        "text",
    ),
    "daily_message_cap": ("0", "integer"),
    "daily_candidate_processing_cap": ("0", "integer"),
    "max_touches": ("3", "integer"),
    "followup_days": ("3", "integer"),
    "minimum_qualification_score": ("70", "integer"),
    "discovery_mode": ("DRY_RUN", "text"),
    "google_places_daily_request_cap": ("0", "integer"),
    "apify_daily_run_cap": ("0", "integer"),
    "apify_actor_id": ("compass~crawler-google-places", "text"),
    "apify_max_items_per_run": ("25", "integer"),
    "apify_max_total_charge_usd": ("0", "text"),
    "apify_max_concurrent_runs": ("1", "integer"),
    "website_enrichment_daily_quota": ("0", "integer"),
    "knowledge_context_required": ("1", "integer"),
    "knowledge_context_packet_ttl_hours": ("24", "integer"),
    "knowledge_context_packet_max_chars": ("6000", "integer"),
    "audit_offer_state": ("PROPOSED", "text"),
    "audit_request_daily_cap": ("0", "integer"),
    "audit_delivery_mode": ("HYBRID_PROPOSED", "text"),
    "audit_deferred_reason": ("Deferred until stable n8n hosting is available.", "text"),
    "portfolio_contact_mode": ("DIRECT_EMAIL_VISIBLE_COPYABLE_MANUAL", "text"),
    "portfolio_public_audit_cta": ("DEFERRED", "text"),
    "portfolio_main_ctas": ("View My Work | Email Me", "text"),
    "portfolio_client_cta": ("Discuss an Automation", "text"),
    "portfolio_employer_cta": ("Email Me About a Role", "text"),
    "drafting_enabled": ("0", "integer"),
    "drafting_daily_cap": ("0", "integer"),
    "drafting_provider_mode": ("FIXTURE_ONLY", "text"),
    "drafting_max_attempts": ("2", "integer"),
    "message_policy_version": ("PALDO_OS_V1_0", "text"),
    "message_policy_status": ("APPROVED_FOR_DRAFT_CREATION", "text"),
    "message_policy_automatic_sending": ("0", "integer"),
    "pilot_readiness_status": ("READY_FOR_PILOT", "text"),
    "gmail_allowed_account": ("owner@fictional-example.invalid", "text"),
    "gmail_portfolio_connected": ("0", "integer"),
    "gmail_draft_integration_enabled": ("0", "integer"),
    "gmail_draft_daily_cap": ("0", "integer"),
    "gmail_provider_mode": ("FIXTURE_ONLY", "text"),
    "gmail_account_configured": ("0", "integer"),
    "gmail_send_enabled": ("0", "integer"),
    "gmail_allowed_actions": ("CREATE_DRAFT,GET_DRAFT", "text"),
    "followup_enabled": ("0", "integer"),
    "reply_ingestion_enabled": ("0", "integer"),
    "followup_provider_mode": ("FIXTURE_ONLY", "text"),
    "followup_policy_version": ("TEST_FOLLOWUP_V0_1", "text"),
    "followup_policy_status": ("PROVISIONAL", "text"),
    "followup_max_total_touches": ("3", "integer"),
    "followup_cadence_status": ("UNDECIDED", "text"),
    "followup_daily_cap": ("0", "integer"),
    "notifications_enabled": ("0", "integer"),
    "notification_backend": ("CONSOLE", "text"),
    "notification_target_configured": ("0", "integer"),
    "notification_daily_cap": ("0", "integer"),
    "notification_allowed_actions": ("SEND_MESSAGE,EDIT_MESSAGE", "text"),
    "notification_jsonl_path": ("./out/notifications.jsonl", "text"),
    "scheduler_enabled": ("0", "integer"),
    "scheduler_mode": ("FIXTURE_ONLY", "text"),
    "scheduler_trigger_mode": ("MANUAL_FIXTURE_ONLY", "text"),
    "scheduler_install_state": ("NOT_INSTALLED", "text"),
    "scheduler_max_concurrent_runs": ("1", "integer"),
    "scheduler_max_attempts": ("2", "integer"),
    "scheduler_catchup_enabled": ("0", "integer"),
    "scheduler_timezone_policy": ("CAMPAIGN_LOCAL", "text"),
    "scheduler_daily_run_time": ("UNDECIDED", "text"),
    "scheduler_weekly_review_schedule": ("UNDECIDED", "text"),
    "decision_maker_enrichment_enabled": ("0", "integer"),
    "linkedin_provider_mode": ("FIXTURE_ONLY", "text"),
    "linkedin_sending_enabled": ("0", "integer"),
    "linkedin_daily_cap": ("0", "integer"),
    "linkedin_message_policy_status": ("UNDECIDED", "text"),
    "personalization_research_enabled": ("0", "integer"),
}

DEFAULT_CONFIG = {
    key: int(value) if value_type == "integer" else float(value) if value_type == "number" else value
    for key, (value, value_type) in _DEFAULT_CONFIG.items()
}


_CONFIG_KEYS = frozenset(_DEFAULT_CONFIG)

STEP2_MIGRATION_VERSION = 2
STEP2_CONFIGURATION_MIGRATION_VERSION = 3
TRI_STATE_VALUES = frozenset({"YES", "NO", "UNCLEAR"})
CAMPAIGN_PRIMARY_BUYER = "business owner"
CAMPAIGN_SECONDARY_BUYER_ROLES = (
    "owner-operator",
    "managing partner",
    "operations lead with authority",
)
CAMPAIGN_UNKNOWN_ROLE_STATUS = "UNCLEAR"
PRIMARY_RECOMMENDATION_REASON = (
    "Recommended because the current direct interview evidence came from the pilot region."
)
SECONDARY_RECOMMENDATION_REASON = (
    "Not recommended because the current direct interview evidence came from the pilot "
    "region, not from the secondary region."
)
CAMPAIGN_SECONDARY_BUYER_ROLES_JSON = json.dumps(
    CAMPAIGN_SECONDARY_BUYER_ROLES, separators=(",", ":")
)
MANDATORY_GATES = (
    "currently_active",
    "appointments_meaningful",
    "legitimate_public_contact_route",
    "independent_owner_led_or_accessible_local_decision_maker",
)

# These are the only signal keys accepted by structured Step 2 evidence.  The
# vocabulary is intentionally explicit so missing evidence can remain UNCLEAR
# instead of being inferred from a business name or industry label.
SUPPORTED_SIGNAL_KEYS = frozenset(
    {
        "campaign_fit",
        "appointment_dependence",
        "appointment_channels",
        "public_business_email",
        "email_explicitly_used_for_booking",
        "website_appointment_request_form",
        "website_booking_page_widget",
        "phone_booking",
        "messenger_booking",
        "instagram_booking",
        "whatsapp_booking",
        "google_business_booking",
        "multiple_booking_channels",
        "cancellation_route",
        "rescheduling_route",
        "manual_staff_confirmation",
        "visible_reminder_capability",
        "visible_follow_up_capability",
        "public_booking_questions",
        "cancellation_deposit_no_show_policy",
        "multiple_practitioners",
        "multiple_services",
        "multiple_locations",
        "recurring_appointments",
        "post_visit_follow_up",
        "out_of_hours_inquiry_handling",
        "system_maturity",
        "recent_activity_demand",
        "recent_reviews_visible_activity",
        "active_promotions",
        "extended_hours",
        "higher_value_services",
        "owner_decision_maker_reachability",
        "personalization_observation",
        "hospital_or_emergency_care",
        "government_facility",
        "large_corporate_chain_without_local_decision_maker",
        "permanently_closed_or_stale",
        "no_legitimate_public_contact_route",
        "non_appointment_dependent",
        "medical_diagnosis_or_treatment_workflow",
        "private_or_sensitive_contact",
    }
)

_WORKFLOW_SIGNAL_KEYS = frozenset(
    {
        "appointment_channels",
        "public_business_email",
        "email_explicitly_used_for_booking",
        "website_appointment_request_form",
        "website_booking_page_widget",
        "phone_booking",
        "messenger_booking",
        "instagram_booking",
        "whatsapp_booking",
        "google_business_booking",
        "multiple_booking_channels",
        "cancellation_route",
        "rescheduling_route",
        "manual_staff_confirmation",
        "visible_reminder_capability",
        "visible_follow_up_capability",
        "public_booking_questions",
        "cancellation_deposit_no_show_policy",
        "post_visit_follow_up",
        "out_of_hours_inquiry_handling",
    }
)
_RECENT_ACTIVITY_SIGNAL_KEYS = frozenset(
    {
        "recent_activity_demand",
        "recent_reviews_visible_activity",
        "active_promotions",
        "extended_hours",
    }
)
VALUE_COMPLEXITY_SIGNAL_KEYS = frozenset(
    {
        "multiple_practitioners",
        "multiple_services",
        "higher_value_services",
        "recurring_appointments",
        "multiple_locations",
        "system_maturity",
    }
)
_EXCLUSION_SIGNAL_KEYS = {
    "hospital_or_emergency_care": "hospital or emergency care",
    "government_facility": "government facility",
    "large_corporate_chain_without_local_decision_maker": "large corporate chain without accessible local decision-maker",
    "permanently_closed_or_stale": "permanently closed or stale business",
    "no_legitimate_public_contact_route": "no legitimate public contact route",
    "non_appointment_dependent": "non-appointment-dependent business",
    "medical_diagnosis_or_treatment_workflow": "workflow requires medical diagnosis or treatment decisions",
    "private_or_sensitive_contact": "private, leaked, or sensitive contact",
}

_FUTURE_CATEGORIES = (
    ("dental", "Dental"),
    ("dermatology", "Dermatology"),
    ("physiotherapy", "Physiotherapy"),
    ("chiropractic", "Chiropractic"),
    ("optometry", "Optometry"),
    ("veterinary", "Veterinary"),
    ("wellness", "Wellness"),
    (
        "other_legitimate_appointment_dependent_clinics",
        "Other legitimate appointment-dependent businesses",
    ),
)

INITIAL_OFFER = "Booking and Follow-Up System"
POSITIONING = (
    "I help owner-led appointment-based businesses track bookings, reminders, and "
    "client follow-ups in one staff-controlled workflow, so staff can see who needs "
    "attention and what happens next."
)


_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY,
    business_name TEXT NOT NULL,
    website TEXT,
    domain TEXT,
    email TEXT,
    phone TEXT,
    industry TEXT,
    country TEXT,
    location TEXT,
    source TEXT,
    source_url TEXT,
    status TEXT NOT NULL DEFAULT 'DISCOVERED' CHECK (status IN (
        'DISCOVERED', 'ENRICHED', 'QUALIFIED', 'DRAFT_READY', 'REVIEW_PENDING',
        'APPROVED', 'SENT', 'FOLLOWUP_DUE', 'REPLIED', 'INTERESTED', 'CALL_BOOKED',
        'PROPOSAL', 'WON', 'LOST', 'REJECTED', 'DO_NOT_CONTACT', 'BOUNCED',
        'UNSUBSCRIBED', 'DUPLICATE'
    )),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS leads_email_unique
    ON leads(email) WHERE email IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS leads_domain_unique
    ON leads(domain) WHERE domain IS NOT NULL;

CREATE TABLE IF NOT EXISTS evidence (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    evidence_type TEXT NOT NULL,
    observation TEXT NOT NULL,
    source_url TEXT,
    confidence REAL,
    captured_at TEXT NOT NULL,
    CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1))
);

CREATE TABLE IF NOT EXISTS lead_scores (
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    score INTEGER NOT NULL CHECK (score >= 0 AND score <= 100),
    reasoning_data TEXT NOT NULL,
    evaluated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS offers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    problem_solved TEXT NOT NULL,
    economic_outcome TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lead_offers (
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    offer_id INTEGER NOT NULL REFERENCES offers(id) ON DELETE CASCADE,
    match_score INTEGER NOT NULL CHECK (match_score >= 0 AND match_score <= 100),
    reasoning TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (lead_id, offer_id)
);

CREATE TABLE IF NOT EXISTS outreach (
    id INTEGER PRIMARY KEY,
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    touch_number INTEGER NOT NULL CHECK (touch_number >= 1),
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    sent_at TEXT
);

CREATE TABLE IF NOT EXISTS suppressions (
    id INTEGER PRIMARY KEY,
    email TEXT,
    domain TEXT,
    lead_id INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (email IS NOT NULL OR domain IS NOT NULL OR lead_id IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    entity_type TEXT,
    entity_id TEXT,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS system_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    value_type TEXT NOT NULL CHECK (value_type IN ('text', 'integer'))
);
"""


_STEP2_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS campaigns (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    segment_name TEXT NOT NULL,
    geography TEXT NOT NULL,
    category_key TEXT NOT NULL,
    offer_id INTEGER REFERENCES offers(id) ON DELETE RESTRICT,
    positioning TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'INACTIVE' CHECK (status IN ('INACTIVE', 'ACTIVE')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (geography, segment_name)
);

CREATE TABLE IF NOT EXISTS eligible_categories (
    category_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'INACTIVE' CHECK (status IN ('INACTIVE', 'ACTIVE'))
);

CREATE TABLE IF NOT EXISTS qualification_results (
    lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    score INTEGER NOT NULL CHECK (score >= 0 AND score <= 100),
    classification TEXT NOT NULL CHECK (classification IN ('STRONG', 'QUALIFIED', 'HOLD', 'REJECT')),
    qualification_result TEXT NOT NULL CHECK (qualification_result IN ('QUALIFY', 'HOLD', 'REJECT')),
    gate_statuses TEXT NOT NULL,
    reasoning_data TEXT NOT NULL,
    evaluated_at TEXT NOT NULL,
    PRIMARY KEY (lead_id, campaign_id)
);
"""


def normalize_domain(value):
    """Return a lowercase host suitable for deterministic matching."""
    if not isinstance(value, str):
        raise ValueError("domain must be text")
    raw = value.strip().casefold()
    if not raw or "@" in raw or any(character.isspace() for character in raw):
        raise ValueError("domain must be a non-empty host")

    parsed = urlsplit(raw if "://" in raw else f"//{raw}")
    host = parsed.hostname
    if not host or "@" in host:
        raise ValueError("domain must contain a host")
    host = host.rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    if not host:
        raise ValueError("domain must contain a host")
    try:
        return host.encode("idna").decode("ascii")
    except UnicodeError as error:
        raise ValueError("domain contains invalid characters") from error


def normalize_email(value):
    """Return a lowercase, trimmed email address for matching."""
    if not isinstance(value, str):
        raise ValueError("email must be text")
    raw = value.strip().casefold()
    if raw.count("@") != 1 or any(character.isspace() for character in raw):
        raise ValueError("email must contain one valid separator")
    local_part, domain_part = raw.split("@")
    if not local_part:
        raise ValueError("email local part is required")
    return f"{local_part}@{normalize_domain(domain_part)}"


def _utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class Database:
    """Small transactional SQLite wrapper for the Step 1 foundation."""

    def __init__(self, path=None):
        self.path = Path(path) if path is not None else DEFAULT_DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.initialize()

    def initialize(self):
        with self.connection:
            self.connection.executescript(_SCHEMA)
            self.connection.executemany(
                "INSERT OR IGNORE INTO system_config (key, value, value_type) VALUES (?, ?, ?)",
                ((key, value, value_type) for key, (value, value_type) in _DEFAULT_CONFIG.items()),
            )

    def migrate_step2(self):
        """Apply the idempotent Step 2 schema and seed only fictional setup data."""
        with self.connection:
            self.connection.executescript(_STEP2_SCHEMA)
            columns = {
                row[1]
                for row in self.connection.execute("PRAGMA table_info(leads)")
            }
            if "campaign_id" not in columns:
                self.connection.execute(
                    "ALTER TABLE leads ADD COLUMN campaign_id INTEGER REFERENCES campaigns(id) ON DELETE SET NULL"
                )

            evidence_columns = {
                row[1]
                for row in self.connection.execute("PRAGMA table_info(evidence)")
            }
            additional_evidence_columns = (
                ("signal_key", "TEXT"),
                (
                    "signal_value",
                    "TEXT CHECK (signal_value IS NULL OR signal_value IN ('YES', 'NO', 'UNCLEAR'))",
                ),
                ("source_type", "TEXT"),
                (
                    "observed_or_inferred",
                    "TEXT CHECK (observed_or_inferred IS NULL OR observed_or_inferred IN ('OBSERVED', 'INFERRED'))",
                ),
                ("pain_hypothesis", "TEXT"),
                ("collected_at", "TEXT"),
            )
            for column_name, definition in additional_evidence_columns:
                if column_name not in evidence_columns:
                    self.connection.execute(
                        f"ALTER TABLE evidence ADD COLUMN {column_name} {definition}"
                    )

            offer = self.connection.execute(
                "SELECT id FROM offers WHERE name = ? ORDER BY id LIMIT 1", (INITIAL_OFFER,)
            ).fetchone()
            if offer is None:
                timestamp = _utc_now()
                offer_id = self.connection.execute(
                    """INSERT INTO offers
                       (name, description, problem_solved, economic_outcome, enabled, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        INITIAL_OFFER,
                        POSITIONING,
                        "Clients and inquiries can become difficult to track across multiple channels, causing missed reminders, no-shows, forgotten follow-ups, and unclear next actions.",
                        "Make staff next actions visible in one controlled workflow so appointment follow-up leakage can be reduced and operational capacity can be protected.",
                        1,
                        timestamp,
                        timestamp,
                    ),
                ).lastrowid
            else:
                offer_id = offer[0]

            self.connection.executemany(
                "INSERT OR IGNORE INTO eligible_categories (category_key, display_name, status) VALUES (?, ?, 'INACTIVE')",
                _FUTURE_CATEGORIES,
            )
            self.connection.execute(
                "UPDATE eligible_categories SET status = 'INACTIVE' WHERE category_key IN ({})".format(
                    ",".join("?" for _ in _FUTURE_CATEGORIES)
                ),
                tuple(category_key for category_key, _ in _FUTURE_CATEGORIES),
            )

            campaign_columns = {
                row[1]
                for row in self.connection.execute("PRAGMA table_info(campaigns)")
            }
            additional_campaign_columns = (
                ("primary_buyer", "TEXT NOT NULL DEFAULT 'business owner'"),
                (
                    "secondary_buyer_roles",
                    "TEXT NOT NULL DEFAULT '[\\\"owner-operator\\\",\\\"managing partner\\\",\\\"operations lead with authority\\\"]'",
                ),
                ("unknown_role_status", "TEXT NOT NULL DEFAULT 'UNCLEAR' CHECK (unknown_role_status = 'UNCLEAR')"),
                ("recommended", "INTEGER NOT NULL DEFAULT 0 CHECK (recommended IN (0, 1))"),
                ("recommendation_reason", "TEXT NOT NULL DEFAULT ''"),
            )
            for column_name, definition in additional_campaign_columns:
                if column_name not in campaign_columns:
                    self.connection.execute(
                        f"ALTER TABLE campaigns ADD COLUMN {column_name} {definition}"
                    )

            self.connection.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS campaigns_one_active_idx
                   ON campaigns(status) WHERE status = 'ACTIVE'"""
            )

            timestamp = _utc_now()
            prepared_campaigns = (
                (
                    "Primary region local services",
                    "local service businesses",
                    "Primary Region",
                    "local_services_primary",
                    1,
                    PRIMARY_RECOMMENDATION_REASON,
                ),
                (
                    "Secondary region local services",
                    "local service businesses",
                    "Secondary Region",
                    "local_services_secondary",
                    0,
                    SECONDARY_RECOMMENDATION_REASON,
                ),
            )
            self.connection.executemany(
                """INSERT OR IGNORE INTO campaigns
                   (name, segment_name, geography, category_key, offer_id, positioning, status,
                    primary_buyer, secondary_buyer_roles, unknown_role_status, recommended,
                    recommendation_reason, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'INACTIVE', ?, ?, ?, ?, ?, ?, ?)""",
                (
                    (
                        name,
                        segment_name,
                        geography,
                        category_key,
                        offer_id,
                        POSITIONING,
                        CAMPAIGN_PRIMARY_BUYER,
                        CAMPAIGN_SECONDARY_BUYER_ROLES_JSON,
                        CAMPAIGN_UNKNOWN_ROLE_STATUS,
                        recommended,
                        recommendation_reason,
                        timestamp,
                        timestamp,
                    )
                    for (
                        name,
                        segment_name,
                        geography,
                        category_key,
                        recommended,
                        recommendation_reason,
                    ) in prepared_campaigns
                ),
            )
            for (
                name,
                _segment_name,
                _geography,
                _category_key,
                recommended,
                recommendation_reason,
            ) in prepared_campaigns:
                self.connection.execute(
                    """UPDATE campaigns
                       SET primary_buyer = ?, secondary_buyer_roles = ?, unknown_role_status = ?,
                           recommended = ?, recommendation_reason = ?, status = 'INACTIVE', updated_at = ?
                       WHERE name = ?
                         AND (primary_buyer <> ? OR secondary_buyer_roles <> ?
                              OR unknown_role_status <> ? OR recommended <> ?
                              OR recommendation_reason <> ? OR status <> 'INACTIVE')""",
                    (
                        CAMPAIGN_PRIMARY_BUYER,
                        CAMPAIGN_SECONDARY_BUYER_ROLES_JSON,
                        CAMPAIGN_UNKNOWN_ROLE_STATUS,
                        recommended,
                        recommendation_reason,
                        timestamp,
                        name,
                        CAMPAIGN_PRIMARY_BUYER,
                        CAMPAIGN_SECONDARY_BUYER_ROLES_JSON,
                        CAMPAIGN_UNKNOWN_ROLE_STATUS,
                        recommended,
                        recommendation_reason,
                    ),
                )

            self.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
                   VALUES (?, ?, ?)""",
                (
                    STEP2_MIGRATION_VERSION,
                    "step2_campaigns_structured_evidence_qualification",
                    timestamp,
                ),
            )
            self.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
                   VALUES (?, ?, ?)""",
                (
                    STEP2_CONFIGURATION_MIGRATION_VERSION,
                    "step2_durable_icp_recommendations_and_single_active_campaign",
                    timestamp,
                ),
            )
        return STEP2_CONFIGURATION_MIGRATION_VERSION

    def _step2_is_applied(self):
        row = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
        ).fetchone()
        if row is None:
            return False
        return self.connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version = ?", (STEP2_MIGRATION_VERSION,)
        ).fetchone() is not None

    def _require_step2(self):
        if not self._step2_is_applied():
            raise RuntimeError("Step 2 migration has not been applied")

    def _table_has_column(self, table_name, column_name):
        return any(
            row[1] == column_name
            for row in self.connection.execute(f"PRAGMA table_info({table_name})")
        )

    def _validate_config_value(self, key, value):
        if key not in _CONFIG_KEYS:
            raise KeyError(key)
        if key == "system_state":
            if isinstance(value, SystemState):
                value = value.value
            if not isinstance(value, str) or value not in SAFE_SYSTEM_STATES:
                raise ValueError("system_state must be PAUSED or ACTIVE")
            return value
        if key == "revenue_principle":
            if not isinstance(value, str):
                raise ValueError("revenue_principle must be text")
            return value
        if key == "message_policy_status":
            if value != "APPROVED_FOR_DRAFT_CREATION":
                raise ValueError("message_policy_status must remain APPROVED_FOR_DRAFT_CREATION")
            return value
        if key == "pilot_readiness_status":
            if value != "READY_FOR_PILOT":
                raise ValueError("pilot_readiness_status must remain READY_FOR_PILOT")
            return value
        if key == "message_policy_automatic_sending":
            if isinstance(value, bool) or not isinstance(value, int) or value != 0:
                raise ValueError("message_policy_automatic_sending must remain 0")
            return value
        if key == "discovery_mode":
            if not isinstance(value, str) or value not in {"DRY_RUN", "LIVE"}:
                raise ValueError("discovery_mode must be DRY_RUN or LIVE")
            return value
        if key == "apify_actor_id":
            if not isinstance(value, str) or not value.strip():
                raise ValueError("apify_actor_id must be non-empty text")
            return value.strip()
        if key == "apify_max_total_charge_usd":
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise ValueError(f"{key} must be a non-negative number")
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{key} must be a non-negative number") from None
            if not math.isfinite(parsed) or parsed < 0:
                raise ValueError(f"{key} must be a non-negative number")
            return parsed
        if key == "drafting_provider_mode":
            if not isinstance(value, str) or value not in {"FIXTURE_ONLY", "HERMES_AGENT"}:
                raise ValueError("drafting_provider_mode must be FIXTURE_ONLY or HERMES_AGENT")
            return value
        if key == "drafting_enabled":
            if isinstance(value, bool) or not isinstance(value, int) or value not in {0, 1}:
                raise ValueError("drafting_enabled must be 0 or 1")
            return value
        if key == "drafting_max_attempts":
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 2:
                raise ValueError("drafting_max_attempts must be between 1 and 2")
            return value
        if key == "gmail_provider_mode":
            if not isinstance(value, str) or value not in {"FIXTURE_ONLY", "COMPOSIO"}:
                raise ValueError("gmail_provider_mode must be FIXTURE_ONLY or COMPOSIO")
            return value
        if key == "gmail_allowed_actions":
            if not isinstance(value, str) or value != "CREATE_DRAFT,GET_DRAFT":
                raise ValueError("gmail_allowed_actions must be CREATE_DRAFT,GET_DRAFT")
            return value
        if key == "gmail_send_enabled":
            if isinstance(value, bool) or not isinstance(value, int) or value != 0:
                raise ValueError("gmail_send_enabled must remain 0 during Step 11")
            return value
        if key in {"gmail_draft_integration_enabled", "gmail_account_configured"}:
            if isinstance(value, bool) or not isinstance(value, int) or value not in {0, 1}:
                raise ValueError(f"{key} must be 0 or 1")
            return value
        if key in {"followup_provider_mode"}:
            if value not in {"FIXTURE_ONLY", "HERMES_AGENT"}:
                raise ValueError("followup_provider_mode must be FIXTURE_ONLY or HERMES_AGENT")
            return value
        if key == "followup_policy_version":
            if value != "TEST_FOLLOWUP_V0_1":
                raise ValueError("followup_policy_version must remain TEST_FOLLOWUP_V0_1")
            return value
        if key == "followup_policy_status":
            if value not in {"PROVISIONAL", "APPROVED_FOR_PREPARATION"}:
                raise ValueError("followup_policy_status must be PROVISIONAL or APPROVED_FOR_PREPARATION")
            return value
        if key == "followup_cadence_status":
            if value not in {"UNDECIDED", "APPROVED"}:
                raise ValueError("followup_cadence_status must be UNDECIDED or APPROVED")
            return value
        if key in {"followup_enabled", "reply_ingestion_enabled"}:
            if isinstance(value, bool) or not isinstance(value, int) or value not in {0, 1}:
                raise ValueError(f"{key} must be 0 or 1")
            return value
        if key == "notification_backend":
            if not isinstance(value, str) or value.upper() not in {"CONSOLE", "JSON_FILE"}:
                raise ValueError("notification_backend must be CONSOLE or JSON_FILE")
            return value
        if key == "notification_allowed_actions":
            if value != "SEND_MESSAGE,EDIT_MESSAGE":
                raise ValueError("notification_allowed_actions is not allowlisted")
            return value
        if key in {"notifications_enabled", "notification_target_configured"}:
            if isinstance(value, bool) or not isinstance(value, int) or value not in {0, 1}:
                raise ValueError(f"{key} must be 0 or 1")
            return value
        if key == "notification_jsonl_path":
            if not isinstance(value, str) or not value.strip():
                raise ValueError("notification_jsonl_path must be a non-empty string")
            return value
        if key == "scheduler_mode":
            if value not in {"FIXTURE_ONLY", "HERMES_NATIVE_CRON"}:
                raise ValueError("scheduler_mode must be FIXTURE_ONLY or HERMES_NATIVE_CRON")
            return value
        if key == "scheduler_trigger_mode":
            if value not in {"MANUAL_FIXTURE_ONLY", "HERMES_NATIVE"}:
                raise ValueError("scheduler_trigger_mode must be MANUAL_FIXTURE_ONLY or HERMES_NATIVE")
            return value
        if key == "scheduler_install_state":
            if value not in {"NOT_INSTALLED", "INSTALLED"}:
                raise ValueError("scheduler_install_state must be NOT_INSTALLED or INSTALLED")
            return value
        if key == "scheduler_timezone_policy":
            if value not in {"CAMPAIGN_LOCAL", "UTC"}:
                raise ValueError("scheduler_timezone_policy must be CAMPAIGN_LOCAL or UTC")
            return value
        if key in {"scheduler_daily_run_time", "scheduler_weekly_review_schedule"}:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{key} must be a non-empty schedule")
            return value.strip()
        if key in {"scheduler_enabled", "scheduler_catchup_enabled"}:
            if isinstance(value, bool) or not isinstance(value, int) or value not in {0, 1}:
                raise ValueError(f"{key} must be 0 or 1")
            return value
        if key == "scheduler_max_concurrent_runs":
            if isinstance(value, bool) or not isinstance(value, int) or value != 1:
                raise ValueError("scheduler_max_concurrent_runs must remain 1")
            return value
        if key == "scheduler_max_attempts":
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 2:
                raise ValueError("scheduler_max_attempts must be between 1 and 2")
            return value
        if key == "linkedin_provider_mode":
            if value not in {"FIXTURE_ONLY", "HERMES_AGENT"}:
                raise ValueError("linkedin_provider_mode must be FIXTURE_ONLY or HERMES_AGENT")
            return value
        if key == "linkedin_message_policy_status":
            if value not in {"UNDECIDED", "APPROVED_FOR_PREPARATION"}:
                raise ValueError("linkedin_message_policy_status must be UNDECIDED or APPROVED_FOR_PREPARATION")
            return value
        if key in {"decision_maker_enrichment_enabled", "linkedin_sending_enabled", "personalization_research_enabled"}:
            if isinstance(value, bool) or not isinstance(value, int) or value not in {0, 1}:
                raise ValueError(f"{key} must be 0 or 1")
            return value
        if key == "linkedin_daily_cap":
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("linkedin_daily_cap must be non-negative")
            return value
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{key} must be an integer")
        if key == "minimum_qualification_score" and not 0 <= value <= 100:
            raise ValueError(f"{key} must be between 0 and 100")
        if key in {"max_touches", "followup_days"} and value < 1:
            raise ValueError(f"{key} must be at least 1")
        if key == "followup_max_total_touches" and value != 3:
            raise ValueError("followup_max_total_touches must remain 3")
        if key in {"daily_message_cap", "daily_candidate_processing_cap", "google_places_daily_request_cap", "apify_daily_run_cap", "website_enrichment_daily_quota", "audit_request_daily_cap", "drafting_daily_cap", "gmail_draft_daily_cap", "followup_daily_cap", "notification_daily_cap"} and value < 0:
            raise ValueError(f"{key} must be non-negative")
        return value

    def _decode_config_row(self, row):
        key = row["key"]
        if key not in _CONFIG_KEYS:
            return None
        if row["value_type"] == "integer":
            value = int(row["value"])
        elif row["value_type"] == "number":
            value = float(row["value"])
        elif row["value_type"] == "text":
            value = row["value"]
        else:
            raise ValueError("unknown config value type")
        return self._validate_config_value(key, value)

    def read_config(self):
        """Read validated known settings, falling back safely on bad rows."""
        config = dict(DEFAULT_CONFIG)
        for row in self.connection.execute(
            "SELECT key, value, value_type FROM system_config ORDER BY key"
        ):
            try:
                value = self._decode_config_row(row)
            except (TypeError, ValueError):
                continue
            if value is not None:
                config[row["key"]] = value
        return config

    def get_config(self, key, default=None):
        """Read one allowlisted setting without evaluating arbitrary values."""
        if key not in _CONFIG_KEYS:
            return default
        row = self.connection.execute(
            "SELECT key, value, value_type FROM system_config WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return DEFAULT_CONFIG[key] if default is None else default
        try:
            return self._decode_config_row(row)
        except (TypeError, ValueError):
            return DEFAULT_CONFIG[key] if default is None else default

    def set_config(self, key, value):
        """Set one validated setting; unknown keys are rejected."""
        validated = self._validate_config_value(key, value)
        value_type = _DEFAULT_CONFIG[key][1]
        with self.connection:
            self.connection.execute(
                """INSERT INTO system_config (key, value, value_type)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value,
                                                 value_type = excluded.value_type""",
                (key, str(validated), value_type),
            )

    def find_duplicate_leads(self, *, email=None, domain=None):
        """Return leads matching a normalized email or domain identifier."""
        conditions = []
        parameters = []
        if email is not None:
            conditions.append("email = ?")
            parameters.append(normalize_email(email))
        if domain is not None:
            conditions.append("domain = ?")
            parameters.append(normalize_domain(domain))
        if not conditions:
            raise ValueError("email or domain is required for duplicate checking")
        rows = self.connection.execute(
            f"SELECT * FROM leads WHERE {' OR '.join(conditions)} ORDER BY id",
            parameters,
        ).fetchall()
        return [dict(row) for row in rows]

    def is_duplicate_lead(self, *, email=None, domain=None):
        return bool(self.find_duplicate_leads(email=email, domain=domain))

    def insert_lead(
        self,
        *,
        business_name,
        website=None,
        domain=None,
        email=None,
        phone=None,
        industry=None,
        country=None,
        location=None,
        source=None,
        source_url=None,
        status=LeadStatus.DISCOVERED,
        campaign_id=None,
    ):
        """Insert a lead after identifier normalization and duplicate checking."""
        business_name = self._require_text(business_name, "business_name")
        optional_fields = {
            field_name: self._optional_text(value, field_name)
            for field_name, value in {
                "website": website,
                "phone": phone,
                "industry": industry,
                "country": country,
                "location": location,
                "source": source,
                "source_url": source_url,
            }.items()
        }
        normalized_email = normalize_email(email) if email is not None else None
        normalized_domain = normalize_domain(domain) if domain is not None else None
        if normalized_domain is None and normalized_email is not None:
            normalized_domain = normalized_email.rsplit("@", 1)[1]
        if isinstance(status, LeadStatus):
            normalized_status = status.value
        elif isinstance(status, str) and status in SAFE_LEAD_STATUSES:
            normalized_status = status
        else:
            raise ValueError(f"unsupported lead status: {status!r}")
        if campaign_id is not None:
            if not self._table_has_column("leads", "campaign_id"):
                raise RuntimeError("Step 2 migration has not been applied")
            if self.get_campaign(campaign_id) is None:
                raise ValueError("campaign does not exist")
        if (normalized_email is not None or normalized_domain is not None) and self.is_duplicate_lead(
            email=normalized_email, domain=normalized_domain
        ):
            raise DuplicateLeadError("a lead with this email or domain already exists")

        timestamp = _utc_now()
        try:
            with self.connection:
                lead_values = (
                    business_name,
                    optional_fields["website"],
                    normalized_domain,
                    normalized_email,
                    optional_fields["phone"],
                    optional_fields["industry"],
                    optional_fields["country"],
                    optional_fields["location"],
                    optional_fields["source"],
                    optional_fields["source_url"],
                    normalized_status,
                    timestamp,
                    timestamp,
                )
                if self._table_has_column("leads", "campaign_id"):
                    cursor = self.connection.execute(
                        """INSERT INTO leads
                           (business_name, website, domain, email, phone, industry, country,
                            location, source, source_url, status, created_at, updated_at, campaign_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (*lead_values, campaign_id),
                    )
                else:
                    cursor = self.connection.execute(
                        """INSERT INTO leads
                           (business_name, website, domain, email, phone, industry, country,
                            location, source, source_url, status, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        lead_values,
                    )
        except sqlite3.IntegrityError as error:
            if "leads_email_unique" in str(error) or "leads_domain_unique" in str(error):
                raise DuplicateLeadError("a lead with this email or domain already exists") from error
            raise
        return cursor.lastrowid

    def get_lead(self, lead_id):
        row = self.connection.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
        return dict(row) if row is not None else None

    def get_campaign(self, campaign_id):
        if not self._table_has_column("leads", "campaign_id"):
            return None
        row = self.connection.execute(
            "SELECT * FROM campaigns WHERE id = ?", (campaign_id,)
        ).fetchone()
        return dict(row) if row is not None else None

    def get_campaign_by_name(self, name):
        if not self._step2_is_applied():
            return None
        row = self.connection.execute(
            "SELECT * FROM campaigns WHERE name = ?", (name,)
        ).fetchone()
        return dict(row) if row is not None else None

    def get_campaigns(self):
        self._require_step2()
        return [
            dict(row)
            for row in self.connection.execute("SELECT * FROM campaigns ORDER BY id")
        ]

    def get_eligible_categories(self):
        self._require_step2()
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM eligible_categories ORDER BY category_key"
            )
        ]

    def insert_structured_evidence(
        self,
        *,
        lead_id,
        signal_key,
        signal_value,
        source_type,
        observation,
        source_url=None,
        confidence=None,
        observed_or_inferred="OBSERVED",
        pain_hypothesis=None,
        collected_at=None,
    ):
        """Store a traceable observation without turning a hypothesis into a fact."""
        self._require_step2()
        self._require_existing_lead(lead_id)
        signal_key = self._require_text(signal_key, "signal_key")
        if signal_key not in SUPPORTED_SIGNAL_KEYS:
            raise ValueError(f"unsupported signal_key: {signal_key}")
        if signal_value not in TRI_STATE_VALUES:
            raise ValueError("signal_value must be YES, NO, or UNCLEAR")
        source_type = self._require_text(source_type, "source_type")
        observation = self._require_text(observation, "observation")
        source_url = self._optional_text(source_url, "source_url")
        pain_hypothesis = self._optional_text(pain_hypothesis, "pain_hypothesis")
        if observed_or_inferred not in {"OBSERVED", "INFERRED"}:
            raise ValueError("observed_or_inferred must be OBSERVED or INFERRED")
        if confidence is not None and (
            isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not 0 <= confidence <= 1
        ):
            raise ValueError("confidence must be between 0 and 1")
        collected_at = _utc_now() if collected_at is None else self._require_text(
            collected_at, "collected_at"
        )
        with self.connection:
            cursor = self.connection.execute(
                """INSERT INTO evidence
                   (lead_id, evidence_type, observation, source_url, confidence, captured_at,
                    signal_key, signal_value, source_type, observed_or_inferred,
                    pain_hypothesis, collected_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    lead_id,
                    "structured_signal",
                    observation,
                    source_url,
                    confidence,
                    collected_at,
                    signal_key,
                    signal_value,
                    source_type,
                    observed_or_inferred,
                    pain_hypothesis,
                    collected_at,
                ),
            )
        return cursor.lastrowid

    def get_structured_evidence(self, lead_id):
        self._require_step2()
        return [
            dict(row)
            for row in self.connection.execute(
                """SELECT * FROM evidence
                   WHERE lead_id = ? AND signal_key IS NOT NULL
                   ORDER BY id""",
                (lead_id,),
            )
        ]

    @staticmethod
    def _combine_signal_values(values):
        values = set(values)
        if not values:
            return "UNCLEAR"
        if "YES" in values and "NO" in values:
            return "UNCLEAR"
        if "YES" in values:
            return "YES"
        return "NO" if "NO" in values else "UNCLEAR"

    def _get_signal_statuses(self, lead_id):
        current_filter = ""
        if self._table_has_column("evidence", "pipeline_current"):
            current_filter = " AND (pipeline_current IS NULL OR pipeline_current = 1)"
        rows = self.connection.execute(
            f"""SELECT signal_key, signal_value FROM evidence
               WHERE lead_id = ? AND signal_key IS NOT NULL{current_filter}
               ORDER BY id""",
            (lead_id,),
        ).fetchall()
        values_by_key = {key: [] for key in SUPPORTED_SIGNAL_KEYS}
        for row in rows:
            if row["signal_key"] in values_by_key:
                values_by_key[row["signal_key"]].append(row["signal_value"])
        return {
            key: self._combine_signal_values(values_by_key[key])
            for key in sorted(SUPPORTED_SIGNAL_KEYS)
        }

    @staticmethod
    def _classification(score):
        if score >= 80:
            return "STRONG"
        if score >= 70:
            return "QUALIFIED"
        if score >= 60:
            return "HOLD"
        return "REJECT"

    def qualify_lead(self, *, lead_id, campaign_id=None, gate_statuses=None):
        """Deterministically score evidence and store a qualification decision."""
        self._require_step2()
        lead = self.get_lead(lead_id)
        if lead is None:
            raise ValueError("lead does not exist")
        campaign_id = campaign_id if campaign_id is not None else lead.get("campaign_id")
        if campaign_id is None:
            raise ValueError("campaign_id is required for Step 2 qualification")
        campaign = self.get_campaign(campaign_id)
        if campaign is None:
            raise ValueError("campaign does not exist")
        if lead.get("campaign_id") not in (None, campaign_id):
            raise ValueError("lead is assigned to a different campaign")
        if lead.get("campaign_id") is None:
            with self.connection:
                self.connection.execute(
                    "UPDATE leads SET campaign_id = ?, updated_at = ? WHERE id = ?",
                    (campaign_id, _utc_now(), lead_id),
                )

        supplied_gates = {} if gate_statuses is None else dict(gate_statuses)
        unknown_gates = set(supplied_gates) - set(MANDATORY_GATES)
        if unknown_gates:
            raise ValueError(f"unsupported mandatory gate: {sorted(unknown_gates)[0]}")
        mandatory = {gate: "UNCLEAR" for gate in MANDATORY_GATES}
        for gate, value in supplied_gates.items():
            if value not in TRI_STATE_VALUES:
                raise ValueError("mandatory gate statuses must be YES, NO, or UNCLEAR")
            mandatory[gate] = value

        evidence_status = self._get_signal_statuses(lead_id)
        points = {}
        signals_used = {}

        def award(component, maximum, signal_keys, points_awarded=None):
            matching = sorted(key for key in signal_keys if evidence_status[key] == "YES")
            amount = points_awarded if points_awarded is not None else (maximum if matching else 0)
            points[component] = amount
            signals_used[component] = matching

        award("active_campaign_fit", 20, ("campaign_fit",))
        award("appointment_dependence", 15, ("appointment_dependence",))
        award("visible_workflow_evidence_friction", 20, _WORKFLOW_SIGNAL_KEYS)
        award("recent_activity_demand", 15, _RECENT_ACTIVITY_SIGNAL_KEYS)
        complexity_signals = sorted(
            key for key in VALUE_COMPLEXITY_SIGNAL_KEYS if evidence_status[key] == "YES"
        )
        complexity_points = 15 if len(complexity_signals) >= 2 else 7 if complexity_signals else 0
        award(
            "economic_value_operational_complexity",
            15,
            VALUE_COMPLEXITY_SIGNAL_KEYS,
            points_awarded=complexity_points,
        )
        award("owner_decision_maker_reachability", 10, ("owner_decision_maker_reachability",))
        award("personalization_evidence", 5, ("personalization_observation",))
        score = sum(points.values())
        classification = self._classification(score)

        exclusion_reasons = [
            reason
            for signal_key, reason in _EXCLUSION_SIGNAL_KEYS.items()
            if evidence_status[signal_key] == "YES"
        ]
        if self.is_suppressed(lead_id=lead_id):
            exclusion_reasons.append("suppressed lead")

        failed_gates = [gate for gate in MANDATORY_GATES if mandatory[gate] == "NO"]
        unclear_gates = [gate for gate in MANDATORY_GATES if mandatory[gate] == "UNCLEAR"]
        minimum_score_value = self.get_config("minimum_qualification_score", 70)
        minimum_score = minimum_score_value if isinstance(minimum_score_value, int) else 70
        if exclusion_reasons or failed_gates:
            qualification_result = "REJECT"
        elif unclear_gates:
            qualification_result = "HOLD"
        elif score < minimum_score:
            qualification_result = "REJECT"
        else:
            qualification_result = "QUALIFY"

        missing_evidence = [key for key, value in evidence_status.items() if value == "UNCLEAR"]
        reason_parts = [
            f"Deterministic Step 2 score: {score}/100 ({classification}).",
            "Awarded points are capped per rubric component and traceable to YES evidence signals.",
        ]
        if exclusion_reasons:
            reason_parts.append("Rejected because: " + "; ".join(exclusion_reasons) + ".")
        if failed_gates:
            reason_parts.append("Rejected because a failed mandatory gate was recorded: " + ", ".join(failed_gates) + ".")
        if unclear_gates:
            reason_parts.append("Held because an unclear mandatory gate was recorded: " + ", ".join(unclear_gates) + ".")
        if not exclusion_reasons and not failed_gates and not unclear_gates and score < minimum_score:
            reason_parts.append(
                f"Not qualified because the minimum qualification score is {minimum_score}."
            )
        if len(complexity_signals) < 2:
            reason_parts.append(
                "Fewer than two value/complexity signals were evidenced, so the full complexity component was not awarded."
            )
        if missing_evidence:
            reason_parts.append(
                "Missing evidence remains UNCLEAR; no unsupported business pain, role, contact, technology, or failure was invented."
            )
        if evidence_status["website_booking_page_widget"] == "YES":
            reason_parts.append(
                "Website booking is treated as readiness, not a disqualifier."
            )
        reason = " ".join(reason_parts)
        reasoning = {
            "campaign_name": campaign["name"],
            "mandatory_gates": mandatory,
            "evidence_status": evidence_status,
            "awarded_points": points,
            "signals_used": signals_used,
            "value_complexity_signal_count": len(complexity_signals),
            "minimum_qualification_score": minimum_score,
            "exclusion_reasons": exclusion_reasons,
            "business_pain_hypothesis": (
                "Hypothesis only: clients and inquiries may become difficult to track across channels, "
                "which may create unclear next actions."
            ),
        }
        result = {
            "lead_id": lead_id,
            "campaign_id": campaign_id,
            "score": score,
            "classification": classification,
            "qualification_result": qualification_result,
            "qualifies": qualification_result == "QUALIFY",
            "mandatory_gates": mandatory,
            "evidence_status": evidence_status,
            "awarded_points": points,
            "exclusion_reasons": exclusion_reasons,
            "reason": reason,
            "reasoning_data": reasoning,
        }
        with self.connection:
            self.connection.execute(
                """INSERT INTO qualification_results
                   (lead_id, campaign_id, score, classification, qualification_result,
                    gate_statuses, reasoning_data, evaluated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(lead_id, campaign_id) DO UPDATE SET
                       score = excluded.score,
                       classification = excluded.classification,
                       qualification_result = excluded.qualification_result,
                       gate_statuses = excluded.gate_statuses,
                       reasoning_data = excluded.reasoning_data,
                       evaluated_at = excluded.evaluated_at""",
                (
                    lead_id,
                    campaign_id,
                    score,
                    classification,
                    qualification_result,
                    json.dumps(mandatory, sort_keys=True, separators=(",", ":")),
                    json.dumps(result, sort_keys=True, separators=(",", ":")),
                    _utc_now(),
                ),
            )
        return result

    def score_lead(self, *, lead_id, campaign_id=None, gate_statuses=None):
        """Compatibility spelling for the deterministic qualification operation."""
        return self.qualify_lead(
            lead_id=lead_id,
            campaign_id=campaign_id,
            gate_statuses=gate_statuses,
        )

    def get_latest_qualification(self, lead_id, campaign_id=None):
        self._require_step2()
        if campaign_id is None:
            row = self.connection.execute(
                """SELECT reasoning_data FROM qualification_results
                   WHERE lead_id = ? ORDER BY evaluated_at DESC LIMIT 1""",
                (lead_id,),
            ).fetchone()
        else:
            row = self.connection.execute(
                """SELECT reasoning_data FROM qualification_results
                   WHERE lead_id = ? AND campaign_id = ?""",
                (lead_id, campaign_id),
            ).fetchone()
        return None if row is None else json.loads(row["reasoning_data"])

    def _require_existing_lead(self, lead_id):
        if self.get_lead(lead_id) is None:
            raise ValueError("lead does not exist")

    @staticmethod
    def _require_text(value, field_name):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} is required")
        return value.strip()

    @staticmethod
    def _optional_text(value, field_name):
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{field_name} must be text")
        value = value.strip()
        return value or None

    def insert_evidence(
        self,
        *,
        lead_id,
        evidence_type,
        observation,
        source_url=None,
        confidence=None,
    ):
        """Store a factual observation for an existing lead."""
        self._require_existing_lead(lead_id)
        evidence_type = self._require_text(evidence_type, "evidence_type")
        observation = self._require_text(observation, "observation")
        source_url = self._optional_text(source_url, "source_url")
        if confidence is not None and (
            isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not 0 <= confidence <= 1
        ):
            raise ValueError("confidence must be between 0 and 1")
        with self.connection:
            cursor = self.connection.execute(
                """INSERT INTO evidence
                   (lead_id, evidence_type, observation, source_url, confidence, captured_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (lead_id, evidence_type, observation, source_url, confidence, _utc_now()),
            )
        return cursor.lastrowid

    def get_evidence(self, evidence_id):
        row = self.connection.execute("SELECT * FROM evidence WHERE id = ?", (evidence_id,)).fetchone()
        return dict(row) if row is not None else None

    def insert_lead_score(self, *, lead_id, score, reasoning_data):
        """Store a bounded score and structured reasoning for an existing lead."""
        self._require_existing_lead(lead_id)
        if isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 100:
            raise ValueError("score must be an integer between 0 and 100")
        if not isinstance(reasoning_data, dict):
            raise ValueError("reasoning_data must be a dictionary")
        reasoning_json = json.dumps(reasoning_data, sort_keys=True, separators=(",", ":"))
        with self.connection:
            cursor = self.connection.execute(
                """INSERT INTO lead_scores (lead_id, score, reasoning_data, evaluated_at)
                   VALUES (?, ?, ?, ?)""",
                (lead_id, score, reasoning_json, _utc_now()),
            )
        return cursor.lastrowid

    def get_latest_lead_score(self, lead_id):
        row = self.connection.execute(
            "SELECT * FROM lead_scores WHERE lead_id = ? ORDER BY rowid DESC LIMIT 1", (lead_id,)
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["reasoning_data"] = json.loads(result["reasoning_data"])
        return result

    def insert_offer(self, *, name, description, problem_solved, economic_outcome, enabled=True):
        """Store an offer tied to a concrete problem and economic outcome."""
        values = (
            self._require_text(name, "name"),
            self._require_text(description, "description"),
            self._require_text(problem_solved, "problem_solved"),
            self._require_text(economic_outcome, "economic_outcome"),
        )
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be boolean")
        timestamp = _utc_now()
        with self.connection:
            cursor = self.connection.execute(
                """INSERT INTO offers
                   (name, description, problem_solved, economic_outcome, enabled, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (*values, int(enabled), timestamp, timestamp),
            )
        return cursor.lastrowid

    def get_offer(self, offer_id):
        row = self.connection.execute("SELECT * FROM offers WHERE id = ?", (offer_id,)).fetchone()
        return dict(row) if row is not None else None

    def link_lead_offer(self, *, lead_id, offer_id, match_score, reasoning):
        """Record why a measured offer fits a lead; this does not initiate outreach."""
        self._require_existing_lead(lead_id)
        if self.get_offer(offer_id) is None:
            raise ValueError("offer does not exist")
        if isinstance(match_score, bool) or not isinstance(match_score, int) or not 0 <= match_score <= 100:
            raise ValueError("match_score must be an integer between 0 and 100")
        reasoning = self._require_text(reasoning, "reasoning")
        with self.connection:
            cursor = self.connection.execute(
                """INSERT INTO lead_offers
                   (lead_id, offer_id, match_score, reasoning, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (lead_id, offer_id, match_score, reasoning, _utc_now()),
            )
        return cursor.lastrowid

    def get_lead_offer(self, lead_id, offer_id):
        row = self.connection.execute(
            "SELECT * FROM lead_offers WHERE lead_id = ? AND offer_id = ?", (lead_id, offer_id)
        ).fetchone()
        return dict(row) if row is not None else None

    def add_suppression(self, *, lead_id=None, email=None, domain=None, reason):
        """Record an opt-out or blocked identifier using normalized values."""
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("suppression reason is required")
        lead = self.get_lead(lead_id) if lead_id is not None else None
        if lead_id is not None and lead is None:
            raise ValueError("lead does not exist")
        normalized_email = normalize_email(email) if email is not None else (lead["email"] if lead else None)
        normalized_domain = normalize_domain(domain) if domain is not None else (lead["domain"] if lead else None)
        if normalized_email and not normalized_domain:
            normalized_domain = normalized_email.rsplit("@", 1)[1]
        if normalized_email is None and normalized_domain is None:
            raise ValueError("email, domain, or lead_id is required")

        with self.connection:
            cursor = self.connection.execute(
                """INSERT INTO suppressions
                   (lead_id, email, domain, reason, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (lead_id, normalized_email, normalized_domain, reason.strip(), _utc_now()),
            )
        return cursor.lastrowid

    def is_suppressed(self, *, lead_id=None, email=None, domain=None):
        """Return whether a lead, email, or domain is suppressed."""
        conditions = []
        parameters = []
        if lead_id is not None:
            conditions.append("lead_id = ?")
            parameters.append(lead_id)
        normalized_email = normalize_email(email) if email is not None else None
        if normalized_email is not None:
            conditions.extend(["email = ?", "domain = ?"])
            parameters.extend([normalized_email, normalized_email.rsplit("@", 1)[1]])
        if domain is not None:
            conditions.append("domain = ?")
            parameters.append(normalize_domain(domain))
        if not conditions:
            raise ValueError("lead_id, email, or domain is required")
        query = f"SELECT 1 FROM suppressions WHERE {' OR '.join(conditions)} LIMIT 1"
        return self.connection.execute(query, parameters).fetchone() is not None

    def log_event(self, *, event_type, entity_type=None, entity_id=None, metadata=None):
        """Append a structured event with deterministic JSON serialization."""
        if not isinstance(event_type, str) or not event_type.strip():
            raise ValueError("event_type is required")
        entity_type = self._optional_text(entity_type, "entity_type")
        if entity_id is not None:
            entity_id = str(entity_id)
        event_metadata = {} if metadata is None else metadata
        if not isinstance(event_metadata, dict):
            raise ValueError("metadata must be a dictionary")
        metadata_json = json.dumps(event_metadata, sort_keys=True, separators=(",", ":"))
        with self.connection:
            cursor = self.connection.execute(
                """INSERT INTO events (event_type, entity_type, entity_id, metadata, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (event_type.strip(), entity_type, entity_id, metadata_json, _utc_now()),
            )
        return cursor.lastrowid

    def get_event(self, event_id):
        row = self.connection.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
        if row is None:
            return None
        event = dict(row)
        event["metadata"] = json.loads(event["metadata"])
        return event

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def migrate_step2(database_or_path=DEFAULT_DB_PATH):
    """Apply Step 2 to an existing Database or database path."""
    if isinstance(database_or_path, Database):
        return database_or_path.migrate_step2()
    database = Database(database_or_path)
    try:
        return database.migrate_step2()
    finally:
        database.close()


from step3_discovery import (
    APIFY_SOURCE,
    GOOGLE_PLACES_SOURCE,
    STEP3_MIGRATION_VERSION,
    ApifyAdapter,
    CandidateStatus,
    DiscoveryMode,
    GooglePlacesAdapter,
    IngestionResult,
    LiveModeBlockedError,
    NormalizedCandidate,
    check_live_guard,
    ingest_candidate,
    migrate_step3,
    normalize_candidate,
    normalize_url,
    run_discovery,
    stable_source_payload_hash,
)


def _database_migrate_step3(self):
    return migrate_step3(self)


def _database_ingest_candidate(self, candidate):
    return ingest_candidate(self, candidate)


def _database_run_discovery(self, adapter, **kwargs):
    return run_discovery(self, adapter, **kwargs)


Database.migrate_step3 = _database_migrate_step3
Database.ingest_candidate = _database_ingest_candidate
Database.run_discovery = _database_run_discovery


if __name__ == "__main__":
    with Database() as database:
        print(database.path)
