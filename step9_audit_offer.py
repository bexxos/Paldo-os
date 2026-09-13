"""Step 9: versioned, approval-controlled Free AI Workflow Audit offer.

This module defines an offer and future integration contracts only.  It does not
retrieve or write Knowledge Base content, call Writer, call an LLM, create
personalized messages, activate campaigns, or perform live audit processing.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

from paldo_os_outbound import Database, normalize_email
from step8_knowledge_context import migrate_step8


STEP9_MIGRATION_VERSION = 11
AUDIT_OFFER_KEY = "free_ai_workflow_audit"
IMMEDIATE_OFFER_KEY = "booking_follow_up_system"
AUDIT_OFFER_STATES = frozenset({"DRAFT", "PROPOSED", "APPROVED", "ACTIVE", "RETIRED"})
N8N_CONTRACT_VERSION = "1"
MAX_DELIVERABLE_CHARS = 6000

AUDIT_DEFERRED_REASON = "Deferred until stable n8n hosting is available."
IMMEDIATE_OFFER_NAME = "Business Booking and Follow-Up System"
IMMEDIATE_OFFER_TARGET_ICP = (
    "Owner-led appointment businesses, beginning with Primary region local services."
)
IMMEDIATE_OFFER_POSITIONING = (
    "Help business staff maintain a visible, staff-controlled process for inquiries, bookings, reminders, "
    "follow-ups, communication history, and the next required action."
)
IMMEDIATE_OFFER_CTA = (
    "Would you be open to a quick 15-minute conversation about whether this could simplify booking or "
    "follow-up at [Business Name]?"
)
IMMEDIATE_OFFER_CTA_REQUIREMENTS = {
    "reply_by_email": True,
    "requires_automated_audit": False,
    "requires_n8n_form": False,
    "requires_website": False,
    "requires_scheduling_platform": False,
    "requires_live_booking_server": False,
    "manual_email_scheduling_allowed": True,
}
PORTFOLIO_CONTACT_DECISION = {
    "main_ctas": ["View My Work", "Email Me"],
    "client_cta": "Discuss an Automation",
    "employer_cta": "Email Me About a Role",
    "public_audit_cta": "DEFERRED",
    "direct_email_visible_and_copyable": True,
    "depends_on_local_n8n": False,
}
FOLLOW_UP_POLICY = {
    "status": "MANUAL_ONLY",
    "prepared_as": "DRAFTS",
    "reviewed_by": "OPERATOR",
    "sent_by": "OPERATOR",
    "stop_on": ["REPLY", "REJECTION", "OPT_OUT", "BOUNCE", "SUPPRESSION"],
    "automatic_sending": False,
    "automatic_follow_ups": False,
}

OFFER_NAME = "Free AI Workflow Audit"
OFFER_PURPOSE = (
    "Identify one high-value repetitive workflow, show where coordination or follow-up may be breaking down, "
    "and recommend a practical first automation."
)
OFFER_TARGET_ICP = (
    "Owner-led appointment businesses; initial specialization is Primary region local services; "
    "later expansion may include Secondary region local services."
)
OFFER_PROBLEM_SCOPE = (
    "Booking, reminders, no-shows, follow-ups, inquiry tracking, and unclear next actions. "
    "The first automation may differ when evidence supports another workflow."
)
OFFER_INCLUSIONS = (
    "one business",
    "one primary operational workflow",
    "public business evidence",
    "optional operator-approved intake answers",
    "current-process summary",
    "observed friction and clearly labeled hypotheses",
    "up to three automation opportunities",
    "one recommended first automation",
    "risks, dependencies, and human-approval points",
    "suggested next step",
)
OFFER_EXCLUSIONS = (
    "implementation",
    "access to production systems",
    "passwords, API keys, or credentials",
    "processing client records",
    "medical advice",
    "diagnosis or treatment decisions",
    "guaranteed financial outcomes",
    "unlimited consulting",
    "a complete digital-transformation strategy",
)
OFFER_CTA_ROLES = {
    "PORTFOLIO_PRIMARY": "Get a free AI workflow audit",
    "OUTBOUND_PERMISSION": "Would it be useful if I sent you a free one-page workflow audit showing where booking or follow-up could be simplified?",
    "SECONDARY_AFTER_INTEREST": "Would you be open to a 15-minute call to review it?",
}
OFFER_INTAKE_RESTRICTIONS = (
    "Do not request or submit client names, treatment histories, medical records, or sensitive health data.",
    "Do not submit passwords, API keys, credentials, tokens, cookies, or secrets.",
    "Permission to prepare and deliver the audit is required.",
    "The requester must acknowledge that client data and secrets are prohibited.",
)
OFFER_PROOF_RULES = (
    "Use public evidence and operator-approved intake facts only.",
    "Keep observations, user-provided facts, inferences, estimates, and unknowns distinct.",
    "Do not claim guaranteed time savings, revenue, ROI, no-show reduction, or financial outcomes.",
    "Numerical estimates require explicit inputs, assumptions, and confidence.",
)
OFFER_HUMAN_REVIEW = (
    "Final audit delivery requires human review.",
    "Human approval is required before any client-facing or operational action.",
    "Offer approval is an audited OPERATOR action by Operator.",
)
OFFER_DISCOVERY_CALL = (
    "A 15-minute discovery call is optional after interest or audit delivery.",
    "The hybrid mode prepares value asynchronously and does not require a call before delivery.",
)

DELIVERABLE_SECTION_KEYS = (
    "business_and_workflow_reviewed",
    "evidence_reviewed",
    "current_workflow_summary",
    "observed_friction",
    "assumptions_and_unknowns",
    "automation_opportunities",
    "recommended_first_automation",
    "human_controls_and_operational_risks",
    "expected_benefit_as_bounded_hypothesis",
    "recommended_next_step",
)
FACT_TYPES = frozenset({"OBSERVATION", "USER_PROVIDED_FACT", "INFERENCE", "ESTIMATE", "UNKNOWN"})
RATING_KEYS = (
    "operational_impact",
    "frequency",
    "implementation_feasibility",
    "evidence_confidence",
)

INTAKE_SCHEMA_VERSION = "1"
INTAKE_REQUIRED_FIELDS = (
    "business_name",
    "public_website",
    "requester_name",
    "business_role",
    "business_email",
    "business_category",
    "current_inquiry_and_booking_channels",
    "workflow_to_review",
    "desired_operational_outcome",
    "permission_to_prepare_and_deliver_audit",
    "acknowledgment_no_person_or_secret_data",
)
INTAKE_OPTIONAL_FIELDS = ("current_tools", "approximate_frequency_or_volume")
INTAKE_ALLOWED_FIELDS = frozenset(INTAKE_REQUIRED_FIELDS + INTAKE_OPTIONAL_FIELDS)
PROHIBITED_INTAKE_FIELDS = frozenset(
    {
        "person_names",
        "person_name",
        "health_information",
        "medical_records",
        "treatment_histories",
        "treatment_history",
        "diagnosis",
        "password",
        "passwords",
        "api_key",
        "api_keys",
        "credential",
        "credentials",
        "secret",
        "secrets",
        "token",
        "tokens",
        "access_token",
        "authorization",
        "cookie",
        "private_key",
    }
)


class AuditOfferLifecycleError(RuntimeError):
    """Raised when an offer status transition violates approval controls."""


class AuditIntakeRejectedError(ValueError):
    """Raised for unsafe, incomplete, or invalid audit intake."""


class DraftingOfferNotReadyError(RuntimeError):
    """Raised when no approved ACTIVE offer is available for future drafting."""


class AuditProcessingBlockedError(RuntimeError):
    """Raised while safe state or the zero processing cap blocks audit work."""


_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_offer_versions (
    id INTEGER PRIMARY KEY,
    offer_key TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    name TEXT NOT NULL,
    purpose TEXT NOT NULL,
    target_icp TEXT NOT NULL,
    problem_scope TEXT NOT NULL,
    inclusions_json TEXT NOT NULL,
    exclusions_json TEXT NOT NULL,
    intake_restrictions_json TEXT NOT NULL,
    proof_rules_json TEXT NOT NULL,
    human_review_json TEXT NOT NULL,
    discovery_call_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'PROPOSED', 'APPROVED', 'ACTIVE', 'RETIRED')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (offer_key, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS audit_offer_one_active_idx
    ON audit_offer_versions(offer_key) WHERE status = 'ACTIVE';

CREATE UNIQUE INDEX IF NOT EXISTS audit_offer_only_one_active_idx
    ON audit_offer_versions(status) WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS audit_offer_cta_variants (
    id INTEGER PRIMARY KEY,
    offer_version_id INTEGER NOT NULL REFERENCES audit_offer_versions(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    cta_text TEXT NOT NULL,
    UNIQUE (offer_version_id, role)
);

CREATE TABLE IF NOT EXISTS audit_request_definitions (
    id INTEGER PRIMARY KEY,
    offer_version_id INTEGER NOT NULL REFERENCES audit_offer_versions(id) ON DELETE CASCADE,
    schema_version TEXT NOT NULL,
    required_fields_json TEXT NOT NULL,
    optional_fields_json TEXT NOT NULL,
    prohibited_fields_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (offer_version_id, schema_version)
);

CREATE TABLE IF NOT EXISTS audit_deliverable_definitions (
    id INTEGER PRIMARY KEY,
    offer_version_id INTEGER NOT NULL REFERENCES audit_offer_versions(id) ON DELETE CASCADE,
    schema_version TEXT NOT NULL,
    sections_json TEXT NOT NULL,
    rubric_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (offer_version_id, schema_version)
);

CREATE TABLE IF NOT EXISTS audit_offer_approval_events (
    id INTEGER PRIMARY KEY,
    offer_version_id INTEGER NOT NULL REFERENCES audit_offer_versions(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    actor_identity TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_requests (
    id INTEGER PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    offer_version_id INTEGER NOT NULL REFERENCES audit_offer_versions(id) ON DELETE RESTRICT,
    source TEXT NOT NULL CHECK (source IN ('PORTFOLIO', 'INBOUND', 'QUALIFIED_OUTBOUND')),
    status TEXT NOT NULL CHECK (status IN ('RECEIVED', 'QUEUED', 'HOLD', 'REJECTED', 'COMPLETED')),
    intake_schema_version TEXT NOT NULL,
    intake_json TEXT NOT NULL,
    intake_hash TEXT NOT NULL,
    audit_event_id TEXT NOT NULL UNIQUE,
    human_review_required INTEGER NOT NULL DEFAULT 1 CHECK (human_review_required IN (0, 1)),
    received_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_request_events (
    id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    request_id INTEGER NOT NULL REFERENCES audit_requests(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_deliverables (
    id INTEGER PRIMARY KEY,
    request_id INTEGER NOT NULL UNIQUE REFERENCES audit_requests(id) ON DELETE RESTRICT,
    definition_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('DRAFT', 'REVIEW', 'APPROVED', 'DELIVERED')),
    content_json TEXT NOT NULL,
    human_review_required INTEGER NOT NULL DEFAULT 1 CHECK (human_review_required IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_n8n_contracts (
    version TEXT PRIMARY KEY,
    contract_json TEXT NOT NULL,
    public_webhook_created INTEGER NOT NULL DEFAULT 0 CHECK (public_webhook_created IN (0, 1)),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS audit_requests_status_idx ON audit_requests(status, received_at);
CREATE INDEX IF NOT EXISTS audit_offer_events_version_idx ON audit_offer_approval_events(offer_version_id, created_at);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _loads(value: str) -> Any:
    return json.loads(value)


def _safe_http_url(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AuditIntakeRejectedError("public_website is required")
    parsed = urlsplit(value.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise AuditIntakeRejectedError("public_website must be a public HTTP or HTTPS URL")
    return value.strip()


def _field_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).casefold()).strip("_")


def _safe_actor(identity: Any) -> str:
    if not isinstance(identity, str) or not identity.strip():
        return ""
    return identity.strip().upper()


def _is_operator(identity: Any) -> bool:
    return _safe_actor(identity) in {"OPERATOR", "OPERATOR", "OPERATOR:OPERATOR"}


def _offer_payload(
    version: int,
    status: str,
    created_by: str,
    timestamp: str,
    *,
    offer_key: str = AUDIT_OFFER_KEY,
    name: str = OFFER_NAME,
    purpose: str = OFFER_PURPOSE,
    target_icp: str = OFFER_TARGET_ICP,
    problem_scope: str = OFFER_PROBLEM_SCOPE,
    inclusions: tuple[str, ...] = OFFER_INCLUSIONS,
    exclusions: tuple[str, ...] = OFFER_EXCLUSIONS,
    intake_restrictions: tuple[str, ...] = OFFER_INTAKE_RESTRICTIONS,
    proof_rules: tuple[str, ...] = OFFER_PROOF_RULES,
    human_review: tuple[str, ...] = OFFER_HUMAN_REVIEW,
    discovery_call: tuple[str, ...] = OFFER_DISCOVERY_CALL,
) -> tuple[Any, ...]:
    return (
        offer_key,
        version,
        name,
        purpose,
        target_icp,
        problem_scope,
        _json(inclusions),
        _json(exclusions),
        _json(intake_restrictions),
        _json(proof_rules),
        _json(human_review),
        _json(discovery_call),
        status,
        created_by,
        timestamp,
        timestamp,
    )


def _n8n_contract() -> dict[str, Any]:
    return {
        "version": N8N_CONTRACT_VERSION,
        "purpose": "Deferred future portfolio/inbound Free AI Workflow Audit intake; no live endpoint is created in Step 9.",
        "execution_state": "DEFERRED_UNTIL_STABLE_N8N_HOSTING",
        "planned_flow": [
            "portfolio form",
            "n8n webhook",
            "schema and consent validation",
            "duplicate and suppression check",
            "Paldo OS audit-request queue",
            "evidence collection",
            "draft audit",
            "human review",
            "approved delivery",
            "optional 15-minute discovery call",
        ],
        "authentication_expectation": "Authenticated, signed, secret-managed n8n-to-Paldo transport; never an unauthenticated public queue writer.",
        "idempotency": {"required": True, "field": "idempotency_key", "scope": "one request identity across retries"},
        "request_schema": {
            "required": list(INTAKE_REQUIRED_FIELDS),
            "optional": list(INTAKE_OPTIONAL_FIELDS),
            "source": ["PORTFOLIO", "INBOUND", "QUALIFIED_OUTBOUND"],
        },
        "safe_error_responses": {
            "VALIDATION_REJECTED": "400 with safe error code and no payload echo",
            "UNAUTHENTICATED": "401 with no credential detail",
            "DUPLICATE": "200 or 409 with existing audit event ID only",
            "RATE_LIMITED": "429 with bounded retry guidance and no secret detail",
            "TEMPORARY_QUEUE_FAILURE": "503 with bounded retry guidance",
            "PROCESSING_BLOCKED": "409 with safe state/cap category",
        },
        "retry_boundary": {
            "validation_and_duplicate": "do not retry automatically",
            "temporary_transport_or_queue_failure": "bounded retry at n8n boundary only",
            "human_review_and_delivery": "never auto-retry past review boundary",
        },
        "prohibited_fields": sorted(PROHIBITED_INTAKE_FIELDS),
        "prohibited_operations": ["client data", "health information", "credentials", "secrets", "live webhook creation", "automatic outreach"],
        "audit_event_ids": "Every accepted request receives a stable audit event ID; status changes append events without payload echo.",
        "logging": "No credentials, secrets, client data, raw sensitive fields, or unrestricted provider payloads in payloads or logs.",
        "public_webhook_created": False,
    }


def _offer_spec(offer_key: str) -> dict[str, Any]:
    if offer_key == IMMEDIATE_OFFER_KEY:
        return {
            "name": IMMEDIATE_OFFER_NAME,
            "purpose": IMMEDIATE_OFFER_POSITIONING,
            "target_icp": IMMEDIATE_OFFER_TARGET_ICP,
            "problem_scope": "Inquiries, bookings, reminders, follow-ups, communication history, and the next required action.",
            "inclusions": (
                "visible staff-controlled inquiry and booking process",
                "reminder and follow-up tracking",
                "communication history",
                "next-action visibility",
                "manual email coordination when appropriate",
            ),
            "exclusions": (
                "guaranteed savings, revenue, ROI, or reduced no-shows",
                "automatic email sending or follow-ups",
                "required website, n8n form, scheduler, or live booking server",
                "client data, health information, passwords, API keys, or credentials",
            ),
            "intake_restrictions": (
                "The offer is introduced by reply-to-email conversation; no automated audit is required.",
                "Do not request or submit client data, health information, passwords, API keys, or credentials.",
            ),
            "proof_rules": (
                "Use factual, approved business information only.",
                "Do not promise guaranteed savings, revenue, reduced no-shows, or ROI.",
            ),
            "human_review": (
                "Outreach drafts are reviewed by Operator and manually sent by Operator.",
                "Stop after reply, rejection, opt-out, bounce, or suppression.",
            ),
            "discovery_call": (
                "The approved CTA invites a manual email conversation of approximately 15 minutes.",
                "Scheduling may be coordinated manually through email.",
            ),
            "cta_roles": {"PRIMARY": IMMEDIATE_OFFER_CTA},
        }
    return {
        "name": OFFER_NAME,
        "purpose": OFFER_PURPOSE,
        "target_icp": OFFER_TARGET_ICP,
        "problem_scope": OFFER_PROBLEM_SCOPE,
        "inclusions": OFFER_INCLUSIONS,
        "exclusions": OFFER_EXCLUSIONS,
        "intake_restrictions": OFFER_INTAKE_RESTRICTIONS,
        "proof_rules": OFFER_PROOF_RULES,
        "human_review": OFFER_HUMAN_REVIEW,
        "discovery_call": OFFER_DISCOVERY_CALL,
        "cta_roles": OFFER_CTA_ROLES,
    }


def _offer_dict(database: Database, row) -> dict[str, Any]:
    if row is None:
        raise KeyError("audit offer version not found")
    ctas = database.connection.execute(
        "SELECT role, cta_text FROM audit_offer_cta_variants WHERE offer_version_id=? ORDER BY role", (row["id"],)
    ).fetchall()
    request_definition = database.connection.execute(
        """SELECT schema_version, required_fields_json, optional_fields_json, prohibited_fields_json
           FROM audit_request_definitions WHERE offer_version_id=? ORDER BY id LIMIT 1""", (row["id"],)
    ).fetchone()
    deliverable_definition = database.connection.execute(
        """SELECT schema_version, sections_json, rubric_json
           FROM audit_deliverable_definitions WHERE offer_version_id=? ORDER BY id LIMIT 1""", (row["id"],)
    ).fetchone()
    result = {
        "id": row["id"],
        "offer_key": row["offer_key"],
        "version": row["version"],
        "name": row["name"],
        "purpose": row["purpose"],
        "target_icp": row["target_icp"],
        "problem_scope": row["problem_scope"],
        "inclusions": _loads(row["inclusions_json"]),
        "exclusions": _loads(row["exclusions_json"]),
        "intake_restrictions": _loads(row["intake_restrictions_json"]),
        "proof_rules": _loads(row["proof_rules_json"]),
        "human_review": _loads(row["human_review_json"]),
        "discovery_call": _loads(row["discovery_call_json"]),
        "status": row["status"],
        "created_by": row["created_by"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "ctas": {cta["role"]: cta["cta_text"] for cta in ctas},
        "intake_schema_version": request_definition["schema_version"] if request_definition else None,
        "deliverable_schema_version": deliverable_definition["schema_version"] if deliverable_definition else None,
    }
    if row["offer_key"] == IMMEDIATE_OFFER_KEY:
        result.update({
            "positioning": row["purpose"],
            "cta_requirements": dict(IMMEDIATE_OFFER_CTA_REQUIREMENTS),
            "follow_up_policy": dict(FOLLOW_UP_POLICY),
            "portfolio_contact_decision": dict(PORTFOLIO_CONTACT_DECISION),
        })
    return result


def migrate_step9(database_or_path: Database | str | Path) -> int:
    """Apply additive Step 9 tables/defaults and seed only a PROPOSED definition."""
    database = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    step9_exists = database.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
    ).fetchone() is not None and database.connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (STEP9_MIGRATION_VERSION,)
    ).fetchone() is not None
    if not step9_exists:
        migrate_step8(database)
    with database.connection:
        database.connection.executescript(_SCHEMA)
        database.connection.executemany(
            "INSERT OR IGNORE INTO system_config (key, value, value_type) VALUES (?, ?, ?)",
            (
                ("audit_offer_state", "PROPOSED", "text"),
                ("audit_request_daily_cap", "0", "integer"),
                ("audit_delivery_mode", "HYBRID_PROPOSED", "text"),
                ("audit_deferred_reason", AUDIT_DEFERRED_REASON, "text"),
                ("portfolio_contact_mode", "DIRECT_EMAIL_VISIBLE_COPYABLE_MANUAL", "text"),
                ("portfolio_public_audit_cta", "DEFERRED", "text"),
                ("portfolio_main_ctas", "View My Work | Email Me", "text"),
                ("portfolio_client_cta", "Discuss an Automation", "text"),
                ("portfolio_employer_cta", "Email Me About a Role", "text"),
            ),
        )
        timestamp = _utc_now()
        offer_ids = {}
        for offer_key, created_by in ((AUDIT_OFFER_KEY, "AI"), (IMMEDIATE_OFFER_KEY, "OPERATOR")):
            spec = _offer_spec(offer_key)
            existing = database.connection.execute(
                "SELECT id FROM audit_offer_versions WHERE offer_key=? AND version=1", (offer_key,)
            ).fetchone()
            if existing is None:
                cursor = database.connection.execute(
                    """INSERT INTO audit_offer_versions
                       (offer_key, version, name, purpose, target_icp, problem_scope,
                        inclusions_json, exclusions_json, intake_restrictions_json, proof_rules_json,
                        human_review_json, discovery_call_json, status, created_by, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    _offer_payload(
                        1,
                        "PROPOSED",
                        created_by,
                        timestamp,
                        offer_key=offer_key,
                        name=spec["name"],
                        purpose=spec["purpose"],
                        target_icp=spec["target_icp"],
                        problem_scope=spec["problem_scope"],
                        inclusions=spec["inclusions"],
                        exclusions=spec["exclusions"],
                        intake_restrictions=spec["intake_restrictions"],
                        proof_rules=spec["proof_rules"],
                        human_review=spec["human_review"],
                        discovery_call=spec["discovery_call"],
                    ),
                )
                offer_ids[offer_key] = cursor.lastrowid
            else:
                offer_ids[offer_key] = existing["id"]
            cta_rows = [(offer_ids[offer_key], role, text) for role, text in sorted(spec["cta_roles"].items())]
            database.connection.executemany(
                "INSERT OR IGNORE INTO audit_offer_cta_variants (offer_version_id, role, cta_text) VALUES (?, ?, ?)", cta_rows
            )

        offer_id = offer_ids[AUDIT_OFFER_KEY]
        database.connection.execute(
            """INSERT OR IGNORE INTO audit_request_definitions
               (offer_version_id, schema_version, required_fields_json, optional_fields_json, prohibited_fields_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (offer_id, INTAKE_SCHEMA_VERSION, _json(INTAKE_REQUIRED_FIELDS), _json(INTAKE_OPTIONAL_FIELDS), _json(sorted(PROHIBITED_INTAKE_FIELDS)), timestamp),
        )
        database.connection.execute(
            """INSERT OR IGNORE INTO audit_deliverable_definitions
               (offer_version_id, schema_version, sections_json, rubric_json, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (offer_id, "1", _json(DELIVERABLE_SECTION_KEYS), _json({"ratings": list(RATING_KEYS), "range": [1, 5], "maximum": 20}), timestamp),
        )

        database.connection.execute(
            "INSERT OR IGNORE INTO audit_n8n_contracts (version, contract_json, public_webhook_created, created_at) VALUES (?, ?, 0, ?)",
            (N8N_CONTRACT_VERSION, _json(_n8n_contract()), timestamp),
        )
        database.connection.execute(
            "UPDATE audit_n8n_contracts SET contract_json=?, public_webhook_created=0 WHERE version=?",
            (_json(_n8n_contract()), N8N_CONTRACT_VERSION),
        )
        database.connection.execute(
            "INSERT OR IGNORE INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
            (STEP9_MIGRATION_VERSION, "step9_audit_offer", timestamp),
        )
    return STEP9_MIGRATION_VERSION


def get_audit_offer_version(database: Database, *, version: int = 1, offer_key: str = AUDIT_OFFER_KEY) -> dict[str, Any]:
    migrate_step9(database)
    row = database.connection.execute(
        "SELECT * FROM audit_offer_versions WHERE offer_key=? AND version=?", (offer_key, version)
    ).fetchone()
    return _offer_dict(database, row)


def get_immediate_offer_version(database: Database, *, version: int = 1) -> dict[str, Any]:
    migrate_step9(database)
    row = database.connection.execute(
        "SELECT * FROM audit_offer_versions WHERE offer_key=? AND version=?", (IMMEDIATE_OFFER_KEY, version)
    ).fetchone()
    return _offer_dict(database, row)


def approve_immediate_offer(database: Database, *, reviewer_identity: str = "OPERATOR", reason: str) -> dict[str, Any]:
    offer = get_immediate_offer_version(database)
    return transition_audit_offer(database, offer["id"], "APPROVED", actor_identity=reviewer_identity, actor_type="OPERATOR", reason=reason)


def activate_immediate_offer(database: Database, *, actor_identity: str = "OPERATOR") -> dict[str, Any]:
    offer = get_immediate_offer_version(database)
    return transition_audit_offer(database, offer["id"], "ACTIVE", actor_identity=actor_identity, actor_type="OPERATOR", reason="operator activation")



def create_audit_offer_version(
    database: Database,
    *,
    version: Optional[int] = None,
    lifecycle_state: str = "DRAFT",
    created_by: str = "AI",
) -> dict[str, Any]:
    migrate_step9(database)
    state = str(lifecycle_state).upper()
    if state not in {"DRAFT", "PROPOSED"}:
        raise AuditOfferLifecycleError("AI-created offer versions may only be DRAFT or PROPOSED")
    actor = _safe_actor(created_by) or "AI"
    if version is None:
        row = database.connection.execute("SELECT COALESCE(MAX(version), 0) + 1 FROM audit_offer_versions WHERE offer_key=?", (AUDIT_OFFER_KEY,)).fetchone()
        version = int(row[0])
    if not isinstance(version, int) or version <= 0:
        raise AuditOfferLifecycleError("offer version must be a positive integer")
    timestamp = _utc_now()
    with database.connection:
        try:
            cursor = database.connection.execute(
                """INSERT INTO audit_offer_versions
                   (offer_key, version, name, purpose, target_icp, problem_scope,
                    inclusions_json, exclusions_json, intake_restrictions_json, proof_rules_json,
                    human_review_json, discovery_call_json, status, created_by, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                _offer_payload(version, state, actor, timestamp),
            )
        except Exception as error:
            raise AuditOfferLifecycleError("offer version already exists or violates lifecycle constraints") from error
        offer_id = cursor.lastrowid
        for role, text in sorted(OFFER_CTA_ROLES.items()):
            database.connection.execute("INSERT INTO audit_offer_cta_variants (offer_version_id, role, cta_text) VALUES (?, ?, ?)", (offer_id, role, text))
        database.connection.execute(
            """INSERT INTO audit_request_definitions
               (offer_version_id, schema_version, required_fields_json, optional_fields_json, prohibited_fields_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (offer_id, INTAKE_SCHEMA_VERSION, _json(INTAKE_REQUIRED_FIELDS), _json(INTAKE_OPTIONAL_FIELDS), _json(sorted(PROHIBITED_INTAKE_FIELDS)), timestamp),
        )
        database.connection.execute(
            """INSERT INTO audit_deliverable_definitions
               (offer_version_id, schema_version, sections_json, rubric_json, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (offer_id, "1", _json(DELIVERABLE_SECTION_KEYS), _json({"ratings": list(RATING_KEYS), "range": [1, 5], "maximum": 20}), timestamp),
        )
    return _offer_dict(database, database.connection.execute("SELECT * FROM audit_offer_versions WHERE id=?", (offer_id,)).fetchone())


def _transition_allowed(current: str, target: str) -> bool:
    return (current, target) in {
        ("DRAFT", "PROPOSED"),
        ("DRAFT", "RETIRED"),
        ("PROPOSED", "APPROVED"),
        ("PROPOSED", "RETIRED"),
        ("APPROVED", "ACTIVE"),
        ("APPROVED", "RETIRED"),
        ("ACTIVE", "RETIRED"),
    }


def transition_audit_offer(
    database: Database,
    offer_version_id: int,
    target_status: str,
    *,
    actor_identity: str = "AI",
    actor_type: str = "AI",
    reason: str = "",
) -> dict[str, Any]:
    migrate_step9(database)
    target = str(target_status).upper()
    if target not in AUDIT_OFFER_STATES:
        raise AuditOfferLifecycleError("unknown audit offer lifecycle state")
    row = database.connection.execute("SELECT * FROM audit_offer_versions WHERE id=?", (offer_version_id,)).fetchone()
    if row is None:
        raise AuditOfferLifecycleError("audit offer version not found")
    current = row["status"]
    if target == current:
        return _offer_dict(database, row)
    if not _transition_allowed(current, target):
        raise AuditOfferLifecycleError(f"invalid offer transition {current} to {target}")
    normalized_actor = _safe_actor(actor_identity)
    normalized_type = _safe_actor(actor_type)
    if not normalized_actor or not normalized_type or not reason.strip():
        raise AuditOfferLifecycleError("actor identity, actor type, and reason are required")
    if target == "APPROVED" and (normalized_type != "OPERATOR" or not _is_operator(normalized_actor)):
        raise AuditOfferLifecycleError("only an audited OPERATOR action by Operator may approve an offer")
    if target in {"ACTIVE", "RETIRED"} and (normalized_type != "OPERATOR" or not _is_operator(normalized_actor)):
        raise AuditOfferLifecycleError("activation and retirement require an audited OPERATOR action by Operator")
    if target == "ACTIVE":
        active = database.connection.execute("SELECT id FROM audit_offer_versions WHERE status='ACTIVE' AND id<>?", (offer_version_id,)).fetchone()
        if active is not None:
            raise AuditOfferLifecycleError("only one active offer is allowed")
    timestamp = _utc_now()
    with database.connection:
        database.connection.execute("UPDATE audit_offer_versions SET status=?, updated_at=? WHERE id=?", (target, timestamp, offer_version_id))
        database.connection.execute(
            """INSERT INTO audit_offer_approval_events
               (offer_version_id, event_type, from_status, to_status, actor_identity, actor_type, reason, created_at)
               VALUES (?, 'STATUS_CHANGE', ?, ?, ?, ?, ?, ?)""",
            (offer_version_id, current, target, normalized_actor, normalized_type, reason.strip()[:500], timestamp),
        )
    return _offer_dict(database, database.connection.execute("SELECT * FROM audit_offer_versions WHERE id=?", (offer_version_id,)).fetchone())


def approve_audit_offer_version(database: Database, offer_version_id: int, *, reviewer_identity: str, reason: str) -> dict[str, Any]:
    return transition_audit_offer(database, offer_version_id, "APPROVED", actor_identity=reviewer_identity, actor_type="OPERATOR", reason=reason)


def activate_audit_offer_version(database: Database, offer_version_id: int, *, actor_identity: str) -> dict[str, Any]:
    return transition_audit_offer(database, offer_version_id, "ACTIVE", actor_identity=actor_identity, actor_type="OPERATOR", reason="operator activation")


def retire_audit_offer_version(database: Database, offer_version_id: int, *, reviewer_identity: str, reason: str) -> dict[str, Any]:
    return transition_audit_offer(database, offer_version_id, "RETIRED", actor_identity=reviewer_identity, actor_type="OPERATOR", reason=reason)


def get_active_offer_for_drafting(database: Database, *, offer_key: str = AUDIT_OFFER_KEY) -> dict[str, Any]:
    migrate_step9(database)
    row = database.connection.execute("SELECT * FROM audit_offer_versions WHERE offer_key=? AND status='ACTIVE' ORDER BY version DESC LIMIT 1", (offer_key,)).fetchone()
    if row is None:
        raise DraftingOfferNotReadyError("no approved ACTIVE audit offer version exists")
    return _offer_dict(database, row)


def validate_audit_intake(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise AuditIntakeRejectedError("intake payload must be an object")
    for key in payload:
        token = _field_token(key)
        if key not in INTAKE_ALLOWED_FIELDS:
            if token in PROHIBITED_INTAKE_FIELDS or any(part in token for part in ("client", "medical", "health", "password", "secret", "credential", "token", "api_key")):
                raise AuditIntakeRejectedError("prohibited sensitive field")
            raise AuditIntakeRejectedError("unknown intake field")
    missing = [field for field in INTAKE_REQUIRED_FIELDS if field not in payload]
    if missing:
        raise AuditIntakeRejectedError("missing required intake field: " + missing[0])
    if payload["permission_to_prepare_and_deliver_audit"] is not True:
        raise AuditIntakeRejectedError("explicit permission is required")
    if payload["acknowledgment_no_person_or_secret_data"] is not True:
        raise AuditIntakeRejectedError("sensitive-data acknowledgment is required")
    normalized: dict[str, Any] = {}
    for field in ("business_name", "requester_name", "business_role", "business_category", "workflow_to_review", "desired_operational_outcome"):
        value = payload[field]
        if not isinstance(value, str) or not value.strip() or len(value.strip()) > 500:
            raise AuditIntakeRejectedError("invalid " + field)
        normalized[field] = value.strip()
    normalized["public_website"] = _safe_http_url(payload["public_website"])
    try:
        normalized["business_email"] = normalize_email(payload["business_email"])
    except ValueError as error:
        raise AuditIntakeRejectedError("invalid business_email") from error
    channels = payload["current_inquiry_and_booking_channels"]
    if isinstance(channels, str):
        channels = [channels]
    if not isinstance(channels, (list, tuple)) or not channels or any(not isinstance(item, str) or not item.strip() for item in channels):
        raise AuditIntakeRejectedError("current inquiry and booking channels are required")
    normalized["current_inquiry_and_booking_channels"] = [item.strip() for item in channels]
    tools = payload.get("current_tools", [])
    if isinstance(tools, str):
        tools = [tools]
    if not isinstance(tools, (list, tuple)) or any(not isinstance(item, str) or not item.strip() for item in tools):
        raise AuditIntakeRejectedError("invalid current_tools")
    normalized["current_tools"] = [item.strip() for item in tools]
    if "approximate_frequency_or_volume" in payload and payload["approximate_frequency_or_volume"] is not None:
        value = payload["approximate_frequency_or_volume"]
        if not isinstance(value, (str, int, float)) or isinstance(value, bool):
            raise AuditIntakeRejectedError("invalid approximate_frequency_or_volume")
        normalized["approximate_frequency_or_volume"] = str(value).strip()
    return {"accepted": True, "schema_version": INTAKE_SCHEMA_VERSION, "normalized": normalized}


def _request_dict(database: Database, row, *, duplicate: bool = False) -> dict[str, Any]:
    return {
        "request_id": row["id"],
        "idempotency_key": row["idempotency_key"],
        "offer_version_id": row["offer_version_id"],
        "source": row["source"],
        "status": row["status"],
        "intake_schema_version": row["intake_schema_version"],
        "intake": json.loads(row["intake_json"]),
        "intake_hash": row["intake_hash"],
        "audit_event_id": row["audit_event_id"],
        "human_review_required": bool(row["human_review_required"]),
        "received_at": row["received_at"],
        "updated_at": row["updated_at"],
        "duplicate": duplicate,
    }


def create_audit_request(
    database: Database,
    payload: Mapping[str, Any],
    *,
    idempotency_key: str,
    offer_version_id: Optional[int] = None,
    source: str = "PORTFOLIO",
) -> dict[str, Any]:
    migrate_step9(database)
    if not isinstance(idempotency_key, str) or not idempotency_key.strip() or len(idempotency_key.strip()) > 200:
        raise AuditIntakeRejectedError("valid idempotency_key is required")
    source_value = str(source).upper()
    if source_value not in {"PORTFOLIO", "INBOUND", "QUALIFIED_OUTBOUND"}:
        raise AuditIntakeRejectedError("invalid intake source")
    validated = validate_audit_intake(payload)
    normalized = validated["normalized"]
    intake_json = _json(normalized)
    intake_hash = hashlib.sha256(intake_json.encode("utf-8")).hexdigest()
    existing = database.connection.execute("SELECT * FROM audit_requests WHERE idempotency_key=?", (idempotency_key.strip(),)).fetchone()
    if existing is not None:
        if existing["intake_hash"] != intake_hash:
            raise AuditIntakeRejectedError("idempotency key is already bound to another intake")
        return _request_dict(database, existing, duplicate=True)
    if offer_version_id is None:
        offer_version_id = get_audit_offer_version(database, version=1)["id"]
    offer = database.connection.execute("SELECT id FROM audit_offer_versions WHERE id=?", (offer_version_id,)).fetchone()
    if offer is None:
        raise AuditIntakeRejectedError("audit offer version not found")
    timestamp = _utc_now()
    with database.connection:
        cursor = database.connection.execute(
            """INSERT INTO audit_requests
               (idempotency_key, offer_version_id, source, status, intake_schema_version,
                intake_json, intake_hash, audit_event_id, human_review_required, received_at, updated_at)
               VALUES (?, ?, ?, 'RECEIVED', ?, ?, ?, '', 1, ?, ?)""",
            (idempotency_key.strip(), offer_version_id, source_value, INTAKE_SCHEMA_VERSION, intake_json, intake_hash, timestamp, timestamp),
        )
        request_id = cursor.lastrowid
        if request_id is None:
            raise AuditIntakeRejectedError("request was not created")
        event_id = f"AUDIT-REQUEST-{request_id:08d}-RECEIVED"
        database.connection.execute("UPDATE audit_requests SET audit_event_id=? WHERE id=?", (event_id, request_id))
        database.connection.execute(
            "INSERT INTO audit_request_events (event_id, request_id, event_type, detail_json, created_at) VALUES (?, ?, 'RECEIVED', ?, ?)",
            (event_id, request_id, _json({"source": source_value, "schema_version": INTAKE_SCHEMA_VERSION}), timestamp),
        )
    return _request_dict(database, database.connection.execute("SELECT * FROM audit_requests WHERE id=?", (request_id,)).fetchone())


def process_audit_request(database: Database, request_id: int) -> dict[str, Any]:
    """Future processing gate; Step 9 never runs audit/evidence generation."""
    migrate_step9(database)
    row = database.connection.execute("SELECT * FROM audit_requests WHERE id=?", (request_id,)).fetchone()
    if row is None:
        raise AuditProcessingBlockedError("audit request not found")
    config = database.read_config()
    cap = int(config.get("audit_request_daily_cap", 0))
    system_state = str(config.get("system_state", "PAUSED")).upper()
    if cap <= 0 or system_state != "RUNNING":
        raise AuditProcessingBlockedError("audit processing blocked by safe state or zero daily cap")
    raise AuditProcessingBlockedError("audit processing is not implemented in Step 9")


def score_audit_opportunity(ratings: Mapping[str, Any]) -> int:
    if not isinstance(ratings, Mapping):
        raise ValueError("ratings must be an object")
    values = []
    for key in RATING_KEYS:
        value = ratings.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 5:
            raise ValueError(f"{key} must be an integer from 1 to 5")
        values.append(value)
    return sum(values)


def _validate_fact(entry: Any) -> dict[str, Any]:
    if not isinstance(entry, Mapping):
        raise ValueError("fact entry must be an object")
    fact_type = str(entry.get("type", "")).upper()
    text = entry.get("text")
    if fact_type not in FACT_TYPES or not isinstance(text, str) or not text.strip():
        raise ValueError("fact type and text are required")
    result = {"type": fact_type, "text": text.strip()}
    if "source_url" in entry:
        result["source_url"] = _safe_http_url(entry["source_url"])
    if "confidence" in entry:
        confidence = entry["confidence"]
        if isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        result["confidence"] = float(confidence)
    if fact_type == "ESTIMATE":
        inputs = entry.get("supporting_inputs")
        assumptions = entry.get("assumptions")
        if not isinstance(inputs, (list, tuple)) or not inputs or not isinstance(assumptions, (list, tuple)) or not assumptions:
            raise ValueError("estimates require supporting_inputs and assumptions")
        if "confidence" not in result:
            raise ValueError("estimates require confidence")
        result["supporting_inputs"] = [str(item) for item in inputs]
        result["assumptions"] = [str(item) for item in assumptions]
    return result


def validate_audit_deliverable(deliverable: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(deliverable, Mapping):
        raise ValueError("deliverable must be an object")
    missing = [key for key in DELIVERABLE_SECTION_KEYS if key not in deliverable]
    if missing:
        raise ValueError("missing deliverable section: " + missing[0])
    normalized: dict[str, Any] = {}
    all_facts: list[list[dict[str, Any]]] = []
    for key in DELIVERABLE_SECTION_KEYS:
        if key == "automation_opportunities":
            opportunities = deliverable[key]
            if not isinstance(opportunities, (list, tuple)) or len(opportunities) > 3:
                raise ValueError("up to three automation opportunities are allowed")
            normalized_opportunities = []
            for opportunity in opportunities:
                if not isinstance(opportunity, Mapping) or not isinstance(opportunity.get("name"), str) or not opportunity["name"].strip():
                    raise ValueError("opportunity name is required")
                descriptions = opportunity.get("description", [])
                if not isinstance(descriptions, (list, tuple)) or not descriptions:
                    raise ValueError("opportunity description facts are required")
                facts = [_validate_fact(entry) for entry in descriptions]
                ratings = opportunity.get("ratings")
                score = score_audit_opportunity(ratings)
                if "priority_score" in opportunity and opportunity["priority_score"] != score:
                    raise ValueError("priority_score must equal the four ratings")
                normalized_opportunities.append({"name": opportunity["name"].strip(), "description": facts, "ratings": dict(ratings), "priority_score": score})
                all_facts.append(facts)
            normalized[key] = normalized_opportunities
        else:
            entries = deliverable[key]
            if not isinstance(entries, (list, tuple)) or not entries:
                raise ValueError("deliverable sections must contain fact entries")
            facts = [_validate_fact(entry) for entry in entries]
            normalized[key] = facts
            all_facts.append(facts)
    encoded = _json(normalized)
    if len(encoded) > MAX_DELIVERABLE_CHARS:
        raise ValueError("one-page deliverable exceeds deterministic size limit")
    return {"valid": True, "normalized": normalized, "fact_entries": all_facts, "serialized_chars": len(encoded)}


def get_n8n_intake_contract(database: Database, version: str = N8N_CONTRACT_VERSION) -> dict[str, Any]:
    migrate_step9(database)
    row = database.connection.execute("SELECT contract_json, public_webhook_created FROM audit_n8n_contracts WHERE version=?", (version,)).fetchone()
    if row is None:
        raise KeyError("n8n intake contract not found")
    result = _loads(row["contract_json"])
    result["public_webhook_created"] = bool(row["public_webhook_created"])
    return result


__all__ = [
    "STEP9_MIGRATION_VERSION",
    "AUDIT_OFFER_KEY",
    "IMMEDIATE_OFFER_KEY",
    "AUDIT_DEFERRED_REASON",
    "IMMEDIATE_OFFER_NAME",
    "IMMEDIATE_OFFER_TARGET_ICP",
    "IMMEDIATE_OFFER_POSITIONING",
    "IMMEDIATE_OFFER_CTA",
    "IMMEDIATE_OFFER_CTA_REQUIREMENTS",
    "PORTFOLIO_CONTACT_DECISION",
    "FOLLOW_UP_POLICY",
    "AUDIT_OFFER_STATES",
    "N8N_CONTRACT_VERSION",
    "DELIVERABLE_SECTION_KEYS",
    "FACT_TYPES",
    "AuditOfferLifecycleError",
    "AuditIntakeRejectedError",
    "DraftingOfferNotReadyError",
    "AuditProcessingBlockedError",
    "migrate_step9",
    "get_audit_offer_version",
    "get_immediate_offer_version",
    "create_audit_offer_version",
    "approve_immediate_offer",
    "activate_immediate_offer",
    "transition_audit_offer",
    "approve_audit_offer_version",
    "activate_audit_offer_version",
    "retire_audit_offer_version",
    "get_active_offer_for_drafting",
    "validate_audit_intake",
    "create_audit_request",
    "process_audit_request",
    "score_audit_opportunity",
    "validate_audit_deliverable",
    "get_n8n_intake_contract",
]
