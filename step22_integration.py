"""Step 22: bounded Paldo OS v1 integration and readiness path.

This module composes the existing discovery, qualification, personalization,
drafting, Gmail-draft, follow-up, and Notification adapters. It does not create a
second fixture subsystem, read a mailbox, send email, send LinkedIn messages,
install a scheduler, or run live discovery. External-looking work is possible
only through explicitly injected providers against a non-canonical database.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional
from urllib.parse import urlsplit

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from step3_discovery import DiscoveryMode, ingest_candidate, normalize_candidate
from step7_pipeline import run_fictional_pipeline_fixture
from step9_audit_offer import get_immediate_offer_version
from step10_drafting import generate_fictional_draft, get_review_pending_draft
from step11_gmail_drafts import (
    approve_gmail_draft_creation,
    create_fixture_gmail_draft,
    verify_fixture_gmail_draft,
)
from step12_followup import (
    FOLLOWUP_REPLY_CATEGORIES,
    get_followup_sequence,
    initialize_followup_sequence,
    record_manual_reply_event,
    record_manual_send,
    migrate_step12,
)
from notifier import (
    NOTIFICATION_BACKENDS,
    NOTIFICATION_CHANNEL_ID,
    NOTIFICATION_OPERATOR_ID,
    NOTIFICATION_CHANNELS,
    Notifier,
    migrate_notifier,
    queue_notification,
)
from step15_identity_personalization import (
    BUSINESS_FIRST,
    LINKEDIN_ONLY,
    create_fixture_personalization_packet,
    migrate_step15,
    prepare_fixture_channel_draft_requests,
    register_fixture_decision_maker,
)
from step17b_pilot_policy import (
    ACTIVE_MESSAGE_AUTOMATIC_SENDING_APPROVED,
    ACTIVE_MESSAGE_POLICY_STATUS,
    ACTIVE_MESSAGE_POLICY_VERSION,
    APPROVED_CASE_STUDY_URL,
    APPROVED_PROOF_OUTCOMES,
    PILOT_READINESS_STATUS,
    build_message_policy_context,
    validate_versioned_message_policy,
)


STEP22_MIGRATION_VERSION = 18
GMAIL_ALLOWED_ACCOUNT = os.environ.get("PALDO_GMAIL_ACCOUNT") or "owner@fictional-example.invalid"
#: Business names excluded from a live production lane, supplied by the operator
#: (comma-separated in PALDO_EXCLUDED_BUSINESSES). Nothing is excluded by default.
EXCLUDED_BUSINESS_NAMES = frozenset(
    name.strip().casefold()
    for name in (os.environ.get("PALDO_EXCLUDED_BUSINESSES") or "").split(",")
    if name.strip()
)
GMAIL_ALLOWED_ACTIONS = ("CREATE_DRAFT", "GET_DRAFT")
BOUNDED_RUN_MODE = "FIXTURE_INJECTED_ONLY"


class Step22BlockedError(RuntimeError):
    """Raised when a bounded run would cross a live or unsafe boundary."""


def _now(value: Optional[datetime] = None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc, microsecond=0)
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _iso(value: Optional[datetime] = None) -> str:
    return _now(value).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _is_canonical(database: Database) -> bool:
    return database.path.resolve() == DEFAULT_DB_PATH.resolve()


def _provider_with_generate(provider: Any) -> Any:
    if callable(getattr(provider, "generate", None)):
        return provider
    if callable(provider):
        class CallableWriter:
            def __init__(self, function: Callable[[Mapping[str, Any]], Any]):
                self.function = function

            def generate(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
                return self.function(payload)

        return CallableWriter(provider)
    raise Step22BlockedError("writer provider must expose generate")


def migrate_step22(database_or_path: Database | str | Path) -> int:
    """Apply only additive Step 22 migration/config defaults."""
    database = database_or_path if isinstance(database_or_path, Database) else Database(database_or_path)
    close_after = not isinstance(database_or_path, Database)
    try:
        migrate_notifier(database)
        with database.connection:
            database.connection.executemany(
                "INSERT OR IGNORE INTO system_config(key,value,value_type) VALUES (?,?,?)",
                (
                    ("message_policy_version", ACTIVE_MESSAGE_POLICY_VERSION, "text"),
                    ("message_policy_status", ACTIVE_MESSAGE_POLICY_STATUS, "text"),
                    ("message_policy_automatic_sending", "1" if ACTIVE_MESSAGE_AUTOMATIC_SENDING_APPROVED else "0", "integer"),
                    ("pilot_readiness_status", PILOT_READINESS_STATUS, "text"),
                    ("gmail_allowed_account", GMAIL_ALLOWED_ACCOUNT, "text"),
                    ("gmail_portfolio_connected", "0", "integer"),
                ),
            )
            database.connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version,name,applied_at) VALUES (?,?,?)",
                (STEP22_MIGRATION_VERSION, "step22_bounded_v1_integration", _iso()),
            )
        return STEP22_MIGRATION_VERSION
    finally:
        if close_after:
            database.close()


def _pipeline_lead_id(result: Mapping[str, Any]) -> Optional[int]:
    value = result.get("lead_id")
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else None


def _request(database: Database, request_id: int) -> dict[str, Any]:
    row = database.connection.execute("SELECT * FROM channel_draft_requests WHERE id=?", (request_id,)).fetchone()
    if row is None:
        raise Step22BlockedError("channel draft request not found")
    body = json.loads(row["request_json"])
    body["request_id"] = row["id"]
    body["request_fingerprint"] = row["request_fingerprint"]
    return body


def _safe_research(research: Mapping[str, Any]) -> str:
    return _json(deepcopy(dict(research)))[:1400]


def build_outbound_queue_card(
    *,
    business_name: str,
    campaign_name: str,
    contact_route: Mapping[str, Any],
    relevant_research: Mapping[str, Any],
    proposed_angle: str,
    draft_text: str,
    gmail_draft_reference: Optional[str],
    review_status: str,
    policy_version: str,
    lane: str,
) -> str:
    """Render the one Notification Outbound Queue record shape."""
    gmail_reference = gmail_draft_reference or "not applicable (LinkedIn-only or not created)"
    return "\n".join(
        [
            "PALDO OS · OUTBOUND QUEUE",
            f"Business: {business_name}",
            f"Campaign: {campaign_name}",
            f"Lane: {lane}",
            f"Decision-maker/contact route: {contact_route.get('type')} · {contact_route.get('value')}",
            f"Relevant research: {_safe_research(relevant_research)}",
            f"Proposed angle: {' '.join(str(proposed_angle).split())[:900]}",
            f"Draft text: {draft_text[:2400]}",
            f"Gmail draft reference: {gmail_reference}",
            f"Review status: {review_status}",
            f"Message policy: {policy_version}",
            "Next action: Operator reviews; manual sending remains outside Paldo OS.",
        ]
    )[:4096]


def collect_basic_reporting(database: Database) -> dict[str, Any]:
    """Return read-only counts from existing SQLite records; drafts are not sends."""
    migrate_step22(database)
    qualified = database.connection.execute(
        """SELECT COUNT(DISTINCT lead_id) FROM (
             SELECT lead_id FROM qualification_results WHERE UPPER(classification) IN ('QUALIFIED','STRONG')
             UNION SELECT lead_id FROM pipeline_qualification_results WHERE UPPER(COALESCE(final_state, classification, qualification_result)) IN ('QUALIFIED','STRONG','QUALIFY')
        )"""
    ).fetchone()[0]
    drafts = database.connection.execute("SELECT COUNT(*) FROM personalized_drafts WHERE state='REVIEW_PENDING'").fetchone()[0]
    sends = database.connection.execute("SELECT COUNT(*) FROM followup_sent_touches").fetchone()[0]
    replies = database.connection.execute("SELECT COUNT(*) FROM followup_reply_events").fetchone()[0]
    positive = database.connection.execute("SELECT COUNT(*) FROM followup_reply_events WHERE category='POSITIVE_INTEREST'").fetchone()[0]
    bounces = database.connection.execute("SELECT COUNT(*) FROM followup_reply_events WHERE category IN ('SOFT_BOUNCE','HARD_BOUNCE')").fetchone()[0]
    opt_outs = database.connection.execute("SELECT COUNT(*) FROM followup_reply_events WHERE category='OPT_OUT' OR explicit_opt_out=1").fetchone()[0]
    queue_entries = database.connection.execute(
        "SELECT COUNT(*) FROM notification_outbox WHERE topic_id=?", (NOTIFICATION_CHANNELS["outbound_queue"],)
    ).fetchone()[0]
    return {
        "qualified_businesses": int(qualified or 0),
        "drafts_awaiting_review": int(drafts or 0),
        "manual_sends_recorded": int(sends or 0),
        "replies_received": int(replies or 0),
        "positive_replies": int(positive or 0),
        "bounces": int(bounces or 0),
        "opt_outs": int(opt_outs or 0),
        "outbound_queue_entries": int(queue_entries or 0),
        "drafts_counted_as_sends": 0,
        "gmail_send_enabled": int(database.get_config("gmail_send_enabled", 0)),
        "scheduler_enabled": int(database.get_config("scheduler_enabled", 0)),
        "followup_processing_enabled": int(database.get_config("followup_enabled", 0)),
        "system_state": database.get_config("system_state"),
        "pilot_readiness_status": database.get_config("pilot_readiness_status"),
        "message_policy_version": database.get_config("message_policy_version"),
        "message_policy_status": database.get_config("message_policy_status"),
        "message_policy_automatic_sending": int(database.get_config("message_policy_automatic_sending", 0) or 0),
    }


def _manual_send_from_sequence(database: Database, sequence_id: int, *, reason: str, idempotency_key: str, sent_at: Any, fixture_override: bool, now: datetime) -> dict[str, Any]:
    sequence = get_followup_sequence(database, sequence_id=sequence_id)
    run = database.connection.execute("SELECT * FROM gmail_draft_creation_runs WHERE id=?", (sequence["source_gmail_run_id"],)).fetchone()
    mapping = database.connection.execute("SELECT * FROM gmail_external_draft_mappings WHERE run_id=? ORDER BY id DESC LIMIT 1", (sequence["source_gmail_run_id"],)).fetchone()
    return record_manual_send(
        database,
        sequence_id=sequence_id,
        draft_id=sequence["source_draft_id"],
        draft_version=sequence["source_draft_version"],
        recipient_email=sequence["recipient_email"],
        sender_email=sequence["sender_email"],
        content_hash=sequence["content_hash"],
        sent_at=sent_at or _iso(now),
        reviewer_identity="OPERATOR",
        reason=reason,
        idempotency_key=idempotency_key,
        provider_draft_id=None if mapping is None else mapping["external_draft_id"],
        provider_message_id=None if mapping is None else mapping["message_id"],
        provider_thread_id=None if mapping is None else mapping["thread_id"],
        fixture_override=fixture_override,
        now=now,
    )


def record_manual_event(
    database: Database,
    *,
    event_type: str,
    sequence_id: int,
    operator_user_id: int,
    fixture_override: bool = False,
    reason: str = "Operator recorded a manual lifecycle event.",
    idempotency_key: str = "",
    sent_at: Any = None,
    category: Optional[str] = None,
    event_reference: Optional[str] = None,
    safe_reference: Optional[str] = None,
    received_at: Any = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Minimal authenticated operator bridge for send/reply/opt-out/bounce records."""
    if operator_user_id != NOTIFICATION_OPERATOR_ID:
        raise Step22BlockedError("OPERATOR_UNAUTHORIZED")
    current = _now(now)
    kind = event_type.upper()
    if kind == "MANUAL_SEND":
        if not idempotency_key:
            raise Step22BlockedError("MANUAL_SEND_IDEMPOTENCY_KEY_REQUIRED")
        return _manual_send_from_sequence(database, sequence_id, reason=reason, idempotency_key=idempotency_key, sent_at=sent_at, fixture_override=fixture_override, now=current)
    if kind not in {"REPLY", "OPT_OUT", "HARD_BOUNCE"}:
        raise Step22BlockedError("MANUAL_EVENT_UNSUPPORTED")
    selected_category = "OPT_OUT" if kind == "OPT_OUT" else "HARD_BOUNCE" if kind == "HARD_BOUNCE" else category
    if selected_category not in FOLLOWUP_REPLY_CATEGORIES - {"UNKNOWN"}:
        raise Step22BlockedError("MANUAL_REPLY_CATEGORY_REQUIRED")
    reference = event_reference or f"manual-event:{sequence_id}:{kind.lower()}"
    safe = safe_reference or reference
    return record_manual_reply_event(
        database,
        sequence_id=sequence_id,
        provider_event_id=reference,
        category=selected_category,
        reason=reason,
        operator_identity="OPERATOR",
        safe_reference=safe,
        received_at=received_at,
        fixture_override=fixture_override,
        now=current,
    )


def run_bounded_pilot(
    database: Database,
    *,
    campaign_id: int,
    campaign_key: str,
    records: Optional[Iterable[Mapping[str, Any]]] = None,
    evidence_by_source_record: Optional[Mapping[str, Iterable[Mapping[str, Any]]]] = None,
    gate_statuses_by_source_record: Optional[Mapping[str, Mapping[str, str]]] = None,
    decision_maker: Optional[Mapping[str, Any]] = None,
    research: Optional[Mapping[str, Any]] = None,
    message_context: Optional[Mapping[str, Any]] = None,
    writer_provider: Any = None,
    reviewer: Any = None,
    gmail_provider: Any = None,
    notification_provider: Optional[Notifier] = None,
    http_client: Any = None,
    dns_client: Any = None,
    lane: str = BUSINESS_FIRST,
    fixture_override: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Run one bounded, injected end-to-end path; duplicate input reuses records."""
    migrate_step22(database)
    if not fixture_override or _is_canonical(database):
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["FIXTURE_OVERRIDE_REQUIRED", "CANONICAL_DATABASE_BLOCKED"], "reused": False}
    if records is None or http_client is None or dns_client is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["FIXTURE_RECORDS_AND_HTTP_DNS_PROVIDERS_REQUIRED"], "reused": False}
    if message_context is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["VERSIONED_MESSAGE_POLICY_CONTEXT_REQUIRED"], "reused": False}
    if writer_provider is None or reviewer is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["WRITER_AND_REVIEWER_PROVIDERS_REQUIRED"], "reused": False}
    current = _now(now)
    pipeline_results = run_fictional_pipeline_fixture(
        database,
        campaign_id=campaign_id,
        records=list(records),
        evidence_by_source_record=evidence_by_source_record,
        gate_statuses_by_source_record=gate_statuses_by_source_record,
        http_client=http_client,
        dns_client=dns_client,
    )
    if len(pipeline_results) != 1 or _pipeline_lead_id(pipeline_results[0]) is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["QUALIFIED_PIPELINE_RESULT_REQUIRED"], "pipeline": pipeline_results, "reused": False}
    pipeline = pipeline_results[0]
    lead_id_value = _pipeline_lead_id(pipeline)
    if lead_id_value is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["PIPELINE_LEAD_ID_REQUIRED"], "pipeline": pipeline, "reused": False}
    lead_id = int(lead_id_value)
    lead = database.get_lead(lead_id)
    campaign = database.get_campaign(campaign_id)
    if lead is None or campaign is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["PIPELINE_LEAD_OR_CAMPAIGN_NOT_FOUND"], "pipeline": pipeline, "reused": False}

    def activate_selected_campaign() -> None:
        with database.connection:
            database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (campaign_id,))

    activate_selected_campaign()
    campaign = database.get_campaign(campaign_id)

    decision_maker_result = None
    decision_maker_id = None
    if decision_maker is not None:
        decision_maker_result = register_fixture_decision_maker(database, lead_id=lead_id, record=decision_maker, fixture_override=True, now=current)
        if decision_maker_result.get("status") not in {"ACCEPTED", "DUPLICATE"}:
            return {"ok": False, "status": "BLOCKED", "blocking_reasons": decision_maker_result.get("reasons", ["DECISION_MAKER_NOT_ACCEPTED"]), "pipeline": pipeline, "reused": False}
        decision_maker_id = decision_maker_result.get("decision_maker_id")

    activate_selected_campaign()
    offer = get_immediate_offer_version(database)
    if offer is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["ACTIVE_OFFER_REQUIRED"], "pipeline": pipeline, "reused": False}
    packet_result = create_fixture_personalization_packet(
        database,
        lead_id=lead_id,
        campaign_id=campaign_id,
        offer_version=str(offer["version"]),
        research=research or {},
        decision_maker_id=decision_maker_id,
        fixture_override=True,
        now=current,
    )
    if packet_result.get("status") not in {"READY", "REUSED"}:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": packet_result.get("reasons", ["PERSONALIZATION_PACKET_REQUIRED"]), "pipeline": pipeline, "packet": packet_result, "reused": False}
    packet_id = int(packet_result["packet_id"])
    activate_selected_campaign()
    channel_result = prepare_fixture_channel_draft_requests(
        database,
        lead_id=lead_id,
        campaign_id=campaign_id,
        packet_id=packet_id,
        offer_version=str(offer["version"]),
        lane=lane,
        fixture_override=True,
        now=current,
    )
    if channel_result.get("status") != "READY":
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": channel_result.get("reasons", ["CHANNEL_REQUESTS_NOT_READY"]), "pipeline": pipeline, "packet": packet_result, "channels": channel_result, "reused": False}
    email_request = next((_request(database, request_id) for request_id in channel_result["request_ids"] if _request(database, request_id).get("channel") == "EMAIL"), None)
    linkedin_request = next((_request(database, request_id) for request_id in channel_result["request_ids"] if _request(database, request_id).get("channel") == "LINKEDIN"), None)
    selected_request = email_request or linkedin_request
    selected_channel = "EMAIL" if email_request is not None else "LINKEDIN"
    if selected_request is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["NO_CHANNEL_REQUEST"], "pipeline": pipeline, "packet": packet_result, "channels": channel_result, "reused": False}
    context = dict(message_context)
    context.setdefault("channel", selected_channel)
    draft_result = generate_fictional_draft(
        database,
        lead_id=lead_id,
        campaign_id=campaign_id,
        provider=_provider_with_generate(writer_provider),
        packet_id=packet_id,
        message_policy_context=context,
        now=current,
    )
    if draft_result.get("state") != "REVIEW_PENDING" or draft_result.get("draft") is None:
        return {"ok": False, "status": "BLOCKED", "blocking_reasons": [draft_result.get("last_error_category", "VERSIONED_DRAFT_NOT_READY")], "pipeline": pipeline, "packet": packet_result, "channels": channel_result, "draft": draft_result, "reused": False}
    draft = draft_result["draft"]
    review_event = None
    if draft_result.get("reused"):
        review_event = database.connection.execute(
            "SELECT metadata FROM events WHERE event_type='STEP22_REVIEW_COMPLETED' AND entity_type='personalized_draft' AND entity_id=? ORDER BY id DESC LIMIT 1",
            (str(draft["draft_id"]),),
        ).fetchone()
    if review_event is not None:
        review_metadata = json.loads(review_event["metadata"])
        review = {"status": str(review_metadata.get("status", "NEEDS_REVISION")), "summary": "Reused prior Step 22 Reviewer result."}
        review_passed = review["status"] == "PASS"
    else:
        review_result = reviewer(deepcopy(draft)) if callable(reviewer) else reviewer.review(deepcopy(draft))
        review_passed = bool(review_result.get("passed")) if isinstance(review_result, Mapping) else False
        review = {"status": "PASS" if review_passed else "NEEDS_REVISION", "summary": str(review_result.get("summary", ""))[:500] if isinstance(review_result, Mapping) else "Reviewer result was not an object."}
        database.log_event(event_type="STEP22_REVIEW_COMPLETED", entity_type="personalized_draft", entity_id=draft["draft_id"], metadata={"status": review["status"], "policy_version": ACTIVE_MESSAGE_POLICY_VERSION})

    gmail_result: dict[str, Any] = {"state": "NOT_APPLICABLE", "reference": None}
    followup_result: dict[str, Any] = {"state": "NOT_APPLICABLE"}
    provider_gmail = gmail_provider
    provider_notification = notification_provider or Notifier()
    contact_route = channel_result.get("contact_routes", {}).get(selected_channel) or {"type": "UNRESOLVED", "value": "not available"}
    if selected_channel == "EMAIL" and review_passed:
        if provider_gmail is None:
            return {"ok": False, "status": "BLOCKED", "blocking_reasons": ["GMAIL_DRAFT_PROVIDER_REQUIRED"], "pipeline": pipeline, "packet": packet_result, "channels": channel_result, "draft": draft_result, "review": review, "reused": False}
        sender = "operator@fixture.fictional-example.invalid"
        approval = approve_gmail_draft_creation(database, draft_id=draft["draft_id"], reviewer_identity="OPERATOR", recipient_email=contact_route["value"], sender_email=sender, reason="Step 22 internal QA passed; Gmail draft only.", fixture_override=True, now=current)
        if approval.get("state") != "APPROVED_FOR_DRAFT_CREATION":
            gmail_result = {"state": "BLOCKED", "blocking_reasons": approval.get("blocking_reasons", ["GMAIL_APPROVAL_BLOCKED"])}
        else:
            created = create_fixture_gmail_draft(database, run_id=approval["run_id"], provider=provider_gmail, fixture_override=True, now=current)
            verified = verify_fixture_gmail_draft(database, run_id=approval["run_id"], provider=provider_gmail, fixture_override=True, now=current)
            gmail_result = {"state": verified.get("state"), "run_id": approval["run_id"], "external_draft_id": verified.get("external_draft_id"), "created": created, "verified": verified, "reference": verified.get("external_draft_id")}
            if verified.get("state") == "READY_FOR_MANUAL_SEND":
                followup_result = initialize_followup_sequence(database, gmail_run_id=approval["run_id"], fixture_override=True, now=current)

    queue_text = build_outbound_queue_card(
        business_name=lead["business_name"],
        campaign_name=campaign["name"],
        contact_route=contact_route,
        relevant_research=research or {},
        proposed_angle=context.get("proposed_angle", ""),
        draft_text=draft["body"],
        gmail_draft_reference=gmail_result.get("reference"),
        review_status=draft["state"] if review_passed else review["status"],
        policy_version=context.get("message_policy_version", ACTIVE_MESSAGE_POLICY_VERSION),
        lane=lane,
    )
    queued = queue_notification(
        database,
        provider=provider_notification,
        topic_id=NOTIFICATION_CHANNELS["outbound_queue"],
        text=queue_text,
        entity_type="personalized_draft",
        entity_id=draft["draft_id"],
        entity_version=draft["version"],
        content_fingerprint=sha256(queue_text.encode()).hexdigest(),
        channel_id=NOTIFICATION_CHANNEL_ID,
        fixture_override=True,
        now=current,
    )
    reused = bool(draft_result.get("reused")) or bool(packet_result.get("status") == "REUSED")
    return {
        "ok": True,
        "status": "READY_FOR_OPERATOR_REVIEW",
        "reused": reused,
        "pipeline": pipeline,
        "lead_id": lead_id,
        "campaign": campaign,
        "decision_maker": decision_maker_result,
        "contact_route": contact_route,
        "packet": packet_result,
        "channels": channel_result,
        "draft": draft,
        "review": review,
        "gmail": gmail_result,
        "followup": followup_result,
        "notification": queued,
        "providers": {"gmail": provider_gmail, "notification": provider_notification},
    }
def _production_is_public_url(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    parsed = urlsplit(value.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.hostname) and not parsed.username and not parsed.password


def _production_reject_secret_keys(value: Any, *, path: str = "handoff") -> None:
    if isinstance(value, Mapping):
        forbidden = {"token", "api_key", "apikey", "authorization", "cookie", "secret", "password", "access_token", "refresh_token"}
        for key, nested in value.items():
            if str(key).casefold() in forbidden or any(part in str(key).casefold() for part in ("token", "api_key", "authorization", "secret")):
                raise Step22BlockedError(f"production handoff contains prohibited secret field: {path}.{key}")
            _production_reject_secret_keys(nested, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            _production_reject_secret_keys(nested, path=f"{path}[{index}]")


def _production_event_exists(database: Database, event_type: str, entity_id: str) -> bool:
    return database.connection.execute(
        "SELECT 1 FROM events WHERE event_type=? AND entity_type='production' AND entity_id=? LIMIT 1",
        (event_type, entity_id),
    ).fetchone() is not None


def _production_event_once(database: Database, *, event_type: str, entity_id: str, metadata: Mapping[str, Any]) -> bool:
    if _production_event_exists(database, event_type, entity_id):
        return False
    database.log_event(event_type=event_type, entity_type="production", entity_id=entity_id, metadata=dict(metadata))
    return True


def _production_valid_research(research: Any) -> tuple[bool, str]:
    if not isinstance(research, Mapping):
        return False, "RESEARCH_PACKET_REQUIRED"
    required = {
        "business_identity", "location", "services", "official_website", "booking_route",
        "inquiry_channels", "business_terminology", "operational_signals", "observations",
        "pain_hypotheses", "sources", "retrieved_at", "uncertainty", "contradictions",
        "proof_outcome_match", "decision_maker_evidence", "evidence", "gate_statuses",
    }
    missing = sorted(key for key in required if key not in research)
    if missing:
        return False, "RESEARCH_FIELDS_MISSING:" + ",".join(missing)
    if not isinstance(research["sources"], list) or not research["sources"]:
        return False, "RESEARCH_SOURCES_REQUIRED"
    for source in research["sources"]:
        if not isinstance(source, Mapping) or not _production_is_public_url(source.get("url")) or not isinstance(source.get("retrieved_at"), str) or not source["retrieved_at"].strip():
            return False, "RESEARCH_SOURCE_URL_AND_TIMESTAMP_REQUIRED"
    if not isinstance(research["evidence"], list) or not research["evidence"]:
        return False, "RESEARCH_EVIDENCE_REQUIRED"
    outcomes = research.get("proof_outcome_match")
    if not isinstance(outcomes, list) or not 1 <= len(outcomes) <= 2 or any(outcome not in APPROVED_PROOF_OUTCOMES for outcome in outcomes):
        return False, "PROOF_OUTCOME_MATCH_REQUIRED"
    return True, ""


def _production_record_decision_maker(database: Database, *, lead_id: int, record: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    route_type = str(record.get("route_type") or record.get("contact_route_type") or "").strip()
    route_value = record.get("route_value") or record.get("public_business_email") or record.get("public_linkedin_url")
    verification = str(record.get("verification_status") or "").upper()
    source_url = record.get("source_url")
    if route_type not in {"OWNER_PROFESSIONAL_EMAIL", "OWNER_LINKEDIN", "OWNER_PUBLIC_SOCIAL", "MANAGER_PROFESSIONAL_EMAIL", "GENERAL_BUSINESS_EMAIL"}:
        raise Step22BlockedError("DECISION_MAKER_ROUTE_INVALID")
    if not isinstance(route_value, str) or not route_value.strip() or not _production_is_public_url(source_url):
        raise Step22BlockedError("DECISION_MAKER_SOURCE_AND_ROUTE_REQUIRED")
    if route_type != "GENERAL_BUSINESS_EMAIL":
        if verification != "VERIFIED" or not isinstance(record.get("person_name"), str) or not record["person_name"].strip() or not isinstance(record.get("verified_role"), str) or not record["verified_role"].strip():
            raise Step22BlockedError("VERIFIED_DECISION_MAKER_REQUIRED")
    else:
        if verification not in {"VERIFIED", "BUSINESS_FALLBACK"}:
            raise Step22BlockedError("GENERAL_BUSINESS_ROUTE_NOT_VERIFIED")
    safe = {
        "lead_id": lead_id,
        "route_type": route_type,
        "route_value": route_value,
        "person_name": record.get("person_name"),
        "verified_role": record.get("verified_role"),
        "verification_status": verification,
        "source_url": source_url,
        "confidence": record.get("confidence"),
        "contradictions": record.get("contradictions", []),
    }
    fingerprint = sha256(_json(safe).encode("utf-8")).hexdigest()
    _production_event_once(database, event_type="PRODUCTION_DECISION_MAKER_VERIFIED", entity_id=fingerprint, metadata=safe)
    if route_type != "GENERAL_BUSINESS_EMAIL" and database.connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='lead_decision_makers'").fetchone():
        existing = database.connection.execute("SELECT id FROM lead_decision_makers WHERE lead_id=? AND record_fingerprint=?", (lead_id, fingerprint)).fetchone()
        if existing is None:
            database.connection.execute(
                """INSERT INTO lead_decision_makers
                   (lead_id, public_name, current_role, public_linkedin_url, public_business_email, business_domain,
                    source_type, source_url, observed_at, confidence, verification_status,
                    verification_reason, record_fingerprint, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (lead_id, record["person_name"], record["verified_role"], record.get("public_linkedin_url"), record.get("public_business_email"), record.get("business_domain"),
                 record.get("source_type", "PUBLIC_BUSINESS_SOURCE"), source_url, record.get("observed_at", _iso(now)), float(record.get("confidence", 0.0)), verification,
                 "VERIFIED_PUBLIC_DECISION_MAKER", fingerprint, _iso(now), _iso(now)),
            )
    return safe


def _production_validate_linkedin(message: Mapping[str, Any], *, profile_url: str) -> tuple[bool, list[str]]:
    errors: list[str] = []
    body = message.get("body") if isinstance(message.get("body"), str) else ""
    stage = str(message.get("stage") or "").upper()
    if not body.strip():
        errors.append("LINKEDIN_BODY_REQUIRED")
    if not _production_is_public_url(profile_url) or "linkedin.com" not in urlsplit(profile_url).hostname.casefold():
        errors.append("VERIFIED_LINKEDIN_PROFILE_REQUIRED")
    if stage == "CONNECTION_NOTE":
        if len(body) > 300:
            errors.append("LINKEDIN_CONNECTION_NOTE_TOO_LONG")
        if "http://" in body.casefold() or "https://" in body.casefold() or "www." in body.casefold():
            errors.append("LINKEDIN_CONNECTION_NOTE_MUST_BE_LINK_FREE")
        if "?" in body:
            errors.append("LINKEDIN_CONNECTION_NOTE_QUESTION_FORBIDDEN")
    if message.get("automatic_send") is not False:
        errors.append("AUTOMATIC_LINKEDIN_SEND_FORBIDDEN")
    return not errors, sorted(set(errors))


def _production_validate_gmail(message: Mapping[str, Any], context: Mapping[str, Any]) -> tuple[bool, list[str]]:
    validation = validate_versioned_message_policy(message, context)
    return bool(validation.get("valid")), list(validation.get("error_codes", []))


def _production_gmail_reference(message: Mapping[str, Any], reference: Mapping[str, Any], *, expected_recipient: str) -> dict[str, Any]:
    actions = set(reference.get("actions", [])) if isinstance(reference.get("actions"), list) else set()
    labels = set(reference.get("labels", [])) if isinstance(reference.get("labels"), list) else set()
    subject = message.get("subject")
    body = message.get("body")
    sender = reference.get("sender")
    recipient = reference.get("recipient")
    content_hash = sha256(_json({"to": expected_recipient, "from": sender, "subject": subject, "body": body}).encode("utf-8")).hexdigest()
    errors = []
    if reference.get("provider") != "COMPOSIO": errors.append("GMAIL_PROVIDER_MUST_BE_COMPOSIO")
    if not {"CREATE_DRAFT", "GET_DRAFT"} <= actions: errors.append("GMAIL_CREATE_AND_GET_REQUIRED")
    if not reference.get("draft_id"): errors.append("GMAIL_DRAFT_ID_REQUIRED")
    if recipient != expected_recipient: errors.append("GMAIL_RECIPIENT_MISMATCH")
    if sender != GMAIL_ALLOWED_ACCOUNT: errors.append("GMAIL_SENDER_MISMATCH")
    if reference.get("subject") != subject: errors.append("GMAIL_SUBJECT_MISMATCH")
    if reference.get("body") != body: errors.append("GMAIL_BODY_MISMATCH")
    if reference.get("content_hash") != content_hash: errors.append("GMAIL_CONTENT_HASH_MISMATCH")
    if "DRAFT" not in labels or "SENT" in labels or reference.get("sent") is not False: errors.append("GMAIL_DRAFT_ONLY_REQUIRED")
    return {"valid": not errors, "errors": sorted(set(errors)), "draft_id": reference.get("draft_id"), "message_id": reference.get("message_id"), "thread_id": reference.get("thread_id"), "content_hash": content_hash}


def _production_notification_reference(reference: Mapping[str, Any], *, topic_id: int) -> dict[str, Any]:
    errors = []
    if str(reference.get("provider") or "").upper() not in NOTIFICATION_BACKENDS: errors.append("NOTIFICATION_BACKEND_INVALID")
    if reference.get("channel_id") != NOTIFICATION_CHANNEL_ID: errors.append("NOTIFICATION_GROUP_MISMATCH")
    if reference.get("topic_id") != topic_id: errors.append("NOTIFICATION_TOPIC_MISMATCH")
    if not reference.get("message_id"): errors.append("NOTIFICATION_MESSAGE_ID_REQUIRED")
    return {"valid": not errors, "errors": sorted(set(errors)), "message_id": reference.get("message_id"), "topic_id": reference.get("topic_id")}


def _production_commit_candidate(database: Database, *, candidate: Mapping[str, Any], lead_id: int, campaign: Mapping[str, Any], now: datetime, processed_index: int) -> dict[str, Any]:
    research = candidate["research"]
    valid, reason = _production_valid_research(research)
    if not valid:
        database.log_event(event_type="PRODUCTION_CANDIDATE_HELD", entity_type="production", entity_id=str(candidate["idempotency_key"]), metadata={"reason": reason, "lead_id": lead_id, "lane": candidate["lane"]})
        return {"status": "HELD", "reason": reason, "lead_id": lead_id}
    for item in research["evidence"]:
        database.insert_structured_evidence(
            lead_id=lead_id,
            signal_key=item["signal_key"],
            signal_value=item["signal_value"],
            source_type=item.get("source_type", "PUBLIC_BUSINESS_SOURCE"),
            observation=item["observation"],
            source_url=item.get("source_url"),
            confidence=item.get("confidence"),
            observed_or_inferred=item.get("observed_or_inferred", "OBSERVED"),
            pain_hypothesis=item.get("pain_hypothesis"),
            collected_at=item.get("collected_at") or research["retrieved_at"],
        )
    qualification = research["gate_statuses"]
    deterministic = database.qualify_lead(lead_id=lead_id, campaign_id=campaign["id"], gate_statuses=qualification)
    matched = research["proof_outcome_match"]
    if not deterministic.get("qualifies") or not 1 <= len(matched) <= 2:
        status = "REJECTED" if deterministic.get("qualification_result") == "REJECT" else "HELD"
        database.connection.execute("UPDATE leads SET status=?, updated_at=? WHERE id=?", ("REJECTED" if status == "REJECTED" else "DISCOVERED", _iso(now), lead_id))
        database.log_event(event_type="PRODUCTION_QUALIFICATION_RECORDED", entity_type="production", entity_id=str(candidate["idempotency_key"]), metadata={"lead_id": lead_id, "status": status, "score": deterministic.get("score"), "reason": deterministic.get("reason"), "proof_outcome_match": matched})
        return {"status": status, "lead_id": lead_id, "score": deterministic.get("score"), "reason": deterministic.get("reason")}
    decision = _production_record_decision_maker(database, lead_id=lead_id, record=candidate["decision_maker"], now=now)
    messages = candidate.get("messages")
    if not isinstance(messages, list) or not messages:
        database.log_event(event_type="PRODUCTION_CANDIDATE_HELD", entity_type="production", entity_id=str(candidate["idempotency_key"]), metadata={"reason": "MESSAGES_REQUIRED", "lead_id": lead_id})
        return {"status": "HELD", "reason": "MESSAGES_REQUIRED", "lead_id": lead_id}
    committed_messages = []
    for message in messages[:3]:
        if not isinstance(message, Mapping):
            return {"status": "HELD", "reason": "MESSAGE_RECORD_INVALID", "lead_id": lead_id}
        context = message.get("policy_context")
        channel = str(message.get("channel") or "").upper()
        if message.get("review_status") != "PASS":
            return {"status": "HELD", "reason": "REVIEWER_PASS_REQUIRED", "lead_id": lead_id}
        if not isinstance(context, Mapping):
            return {"status": "HELD", "reason": "MESSAGE_POLICY_CONTEXT_REQUIRED", "lead_id": lead_id}
        if channel == "EMAIL":
            ok, errors = _production_validate_gmail(message, context)
        elif channel == "LINKEDIN":
            ok, errors = _production_validate_linkedin(message, profile_url=candidate["decision_maker"].get("public_linkedin_url", ""))
        else:
            ok, errors = False, ["MESSAGE_CHANNEL_INVALID"]
        if not ok:
            database.log_event(event_type="PRODUCTION_MESSAGE_HELD", entity_type="production", entity_id=str(message.get("idempotency_key") or candidate["idempotency_key"]), metadata={"lead_id": lead_id, "errors": errors, "channel": channel})
            return {"status": "HELD", "reason": ";".join(errors), "lead_id": lead_id}
        external = message.get("gmail_reference") if channel == "EMAIL" else None
        if channel == "EMAIL":
            gmail = _production_gmail_reference(message, external or {}, expected_recipient=candidate["decision_maker"].get("route_value") or candidate.get("public_business_email"))
            if not gmail["valid"]:
                return {"status": "HELD", "reason": ";".join(gmail["errors"]), "lead_id": lead_id}
            notification = _production_notification_reference(message.get("notification_reference", {}), topic_id=NOTIFICATION_CHANNELS["outbound_queue"])
            if not notification["valid"]:
                return {"status": "HELD", "reason": ";".join(notification["errors"]), "lead_id": lead_id}
            external_summary = {"gmail": gmail, "notification": notification}
        else:
            notification = _production_notification_reference(message.get("notification_reference", {}), topic_id=NOTIFICATION_CHANNELS["linkedin_queue"])
            if not notification["valid"]:
                return {"status": "HELD", "reason": ";".join(notification["errors"]), "lead_id": lead_id}
            external_summary = {"notification": notification, "linkedin_profile_url": candidate["decision_maker"].get("public_linkedin_url")}
        key = str(message.get("idempotency_key") or f"{candidate['idempotency_key']}:{channel}:{message.get('stage', 'INITIAL')}")
        if _production_event_exists(database, "PRODUCTION_MESSAGE_COMMITTED", key):
            committed_messages.append({"channel": channel, "idempotency_key": key, "reused": True})
            continue
        touch_number = int(message.get("touch_number", 1))
        if touch_number < 1 or touch_number > 3:
            return {"status": "HELD", "reason": "TOUCH_NUMBER_INVALID", "lead_id": lead_id}
        status = "DRAFT_REVIEW_PENDING" if channel == "EMAIL" else "LINKEDIN_QUEUE_REVIEW_PENDING"
        database.connection.execute("INSERT INTO outreach (lead_id, channel, touch_number, status, created_at, sent_at) VALUES (?, ?, ?, ?, ?, NULL)", (lead_id, channel, touch_number, status, _iso(now)))
        metadata = {"run_id": candidate.get("run_id"), "lane": candidate["lane"], "lead_id": lead_id, "campaign_id": campaign["id"], "channel": channel, "stage": message.get("stage", "INITIAL"), "touch_number": touch_number, "policy_version": context.get("message_policy_version"), "review_status": message.get("review_status"), "message_hash": sha256(str(message.get("body", "")).encode("utf-8")).hexdigest(), "external": external_summary}
        _production_event_once(database, event_type="PRODUCTION_MESSAGE_COMMITTED", entity_id=key, metadata=metadata)
        committed_messages.append({"channel": channel, "idempotency_key": key, "reused": False, "external": external_summary})
    database.connection.execute("UPDATE leads SET status='REVIEW_PENDING', updated_at=? WHERE id=?", (_iso(now), lead_id))
    database.log_event(event_type="PRODUCTION_CANDIDATE_COMMITTED", entity_type="production", entity_id=str(candidate["idempotency_key"]), metadata={"lead_id": lead_id, "lane": candidate["lane"], "campaign_id": campaign["id"], "decision_maker": decision, "messages": committed_messages, "processed_index": processed_index})
    return {"status": "QUALIFIED", "lead_id": lead_id, "score": deterministic.get("score"), "messages": committed_messages, "decision_maker": decision}


def _production_lock(lock_path: Path):
    import errno
    import fcntl
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as error:
        handle.close()
        if error.errno in {errno.EACCES, errno.EAGAIN}:
            raise Step22BlockedError("PRODUCTION_RUN_ALREADY_ACTIVE") from None
        raise
    return handle

def configure_production_runtime(database: Database | str | Path = DEFAULT_DB_PATH, *, scheduler_installed: bool = False) -> dict[str, Any]:
    """Enable the approved production flags without enabling any sends."""
    path = database.path if isinstance(database, Database) else Path(database)
    if path.resolve() != DEFAULT_DB_PATH.resolve():
        raise Step22BlockedError("PRODUCTION_CANONICAL_DATABASE_REQUIRED")
    db = database if isinstance(database, Database) else Database(path)
    close_after = not isinstance(database, Database)
    try:
        migrate_step22(db)
        values = {
            "system_state": "ACTIVE",
            "discovery_mode": "LIVE",
            "daily_candidate_processing_cap": 20,
            "daily_message_cap": 20,
            "max_touches": 3,
            "apify_daily_run_cap": 2,
            "apify_actor_id": "compass~crawler-google-places",
            "apify_max_items_per_run": 25,
            "apify_max_total_charge_usd": 0.50,
            "apify_max_concurrent_runs": 1,
            "website_enrichment_daily_quota": 20,
            "drafting_enabled": 1,
            "drafting_daily_cap": 20,
            "drafting_provider_mode": "HERMES_AGENT",
            "gmail_portfolio_connected": 1,
            "gmail_draft_integration_enabled": 1,
            "gmail_draft_daily_cap": 20,
            "gmail_provider_mode": "COMPOSIO",
            "gmail_account_configured": 1,
            "gmail_send_enabled": 0,
            "followup_enabled": 1,
            "reply_ingestion_enabled": 1,
            "followup_provider_mode": "HERMES_AGENT",
            "followup_policy_status": "APPROVED_FOR_PREPARATION",
            "followup_cadence_status": "APPROVED",
            "followup_daily_cap": 20,
            "decision_maker_enrichment_enabled": 1,
            "personalization_research_enabled": 1,
            "linkedin_provider_mode": "HERMES_AGENT",
            "linkedin_message_policy_status": "APPROVED_FOR_PREPARATION",
            "linkedin_sending_enabled": 0,
            "linkedin_daily_cap": 20,
            "notifications_enabled": 1,
            "notification_backend": "JSON_FILE",
            "notification_target_configured": 1,
            "notification_daily_cap": 40,
            "scheduler_enabled": 1 if scheduler_installed else 0,
            "scheduler_mode": "HERMES_NATIVE_CRON",
            "scheduler_trigger_mode": "HERMES_NATIVE",
            "scheduler_install_state": "INSTALLED" if scheduler_installed else "NOT_INSTALLED",
            "scheduler_timezone_policy": "UTC",
            "scheduler_daily_run_time": "09:00 UTC weekdays",
            "scheduler_weekly_review_schedule": "17:00 UTC Fridays",
            "scheduler_max_concurrent_runs": 1,
            "scheduler_catchup_enabled": 0,
        }
        for key, value in values.items():
            db.set_config(key, value)
        db.log_event(event_type="PRODUCTION_RUNTIME_CONFIGURED", entity_type="production", entity_id="paldo-os-runtime", metadata={"scheduler_installed": scheduler_installed, "gmail_send_enabled": 0, "linkedin_sending_enabled": 0, "apify_actor_id": values["apify_actor_id"], "apify_daily_run_cap": 2, "apify_max_total_charge_usd": 0.50})
        return {key: db.get_config(key) for key in ("system_state", "discovery_mode", "daily_candidate_processing_cap", "daily_message_cap", "apify_daily_run_cap", "apify_max_items_per_run", "apify_max_total_charge_usd", "drafting_enabled", "drafting_daily_cap", "gmail_draft_integration_enabled", "gmail_draft_daily_cap", "gmail_send_enabled", "followup_enabled", "linkedin_daily_cap", "linkedin_sending_enabled", "notifications_enabled", "scheduler_enabled", "scheduler_install_state", "scheduler_timezone_policy")}
    finally:
        if close_after:
            db.close()


def run_production_cycle(database: Database | str | Path = DEFAULT_DB_PATH, *, handoff: Mapping[str, Any], fixture_override: bool = False, now: Optional[datetime] = None) -> dict[str, Any]:
    """Commit one live handoff produced by the Hermes cron agent.

    The cron agent performs Apify, MCP, research, Writer, Reviewer, Gmail, and
    Notification operations. This function performs no external calls; it only
    validates the bounded handoff and commits safe canonical references.
    """
    if fixture_override:
        raise Step22BlockedError("PRODUCTION_FIXTURE_OVERRIDE_FORBIDDEN")
    path = database.path if isinstance(database, Database) else Path(database)
    if path.resolve() != DEFAULT_DB_PATH.resolve():
        raise Step22BlockedError("PRODUCTION_CANONICAL_DATABASE_REQUIRED")
    if not isinstance(handoff, Mapping):
        raise Step22BlockedError("PRODUCTION_HANDOFF_REQUIRED")
    _production_reject_secret_keys(handoff)
    if handoff.get("mode") != "LIVE" or handoff.get("external_orchestrator") != "HERMES_CRON_AGENT":
        raise Step22BlockedError("LIVE_HERMES_CRON_HANDOFF_REQUIRED")
    candidates = handoff.get("candidates")
    actor_runs = handoff.get("actor_runs")
    if not isinstance(candidates, list) or len(candidates) > 50:
        raise Step22BlockedError("DISCOVERY_CANDIDATE_CAP_EXCEEDED")
    if not isinstance(actor_runs, list) or len(actor_runs) != 2:
        raise Step22BlockedError("EXACTLY_TWO_APIFY_RUNS_REQUIRED")
    actor_lanes = set()
    for item in actor_runs:
        if not isinstance(item, Mapping):
            raise Step22BlockedError("APIFY_RUN_RECORD_INVALID")
        lane = str(item.get("lane", "")).upper()
        actor_lanes.add(lane)
        if lane not in {"PRIMARY", "SECONDARY"} or item.get("actor_id") != "compass~crawler-google-places" or item.get("state") != "SUCCEEDED" or not item.get("remote_run_id") or not item.get("dataset_id"):
            raise Step22BlockedError("SUCCESSFUL_APIFY_DATASET_REQUIRED")
        if isinstance(item.get("item_count"), bool) or not isinstance(item.get("item_count"), int) or not 0 <= item["item_count"] <= 25:
            raise Step22BlockedError("APIFY_DATASET_ITEM_CAP_EXCEEDED")
        if isinstance(item.get("cost_usd", 0), bool) or not isinstance(item.get("cost_usd", 0), (int, float)) or not 0 <= float(item.get("cost_usd", 0)) <= 0.50:
            raise Step22BlockedError("APIFY_COST_CAP_EXCEEDED")
    if actor_lanes != {"PRIMARY", "SECONDARY"}:
        raise Step22BlockedError("ONE_SUCCESSFUL_APIFY_RUN_PER_LANE_REQUIRED")
    by_lane = {"PRIMARY": 0, "SECONDARY": 0}
    for item in candidates:
        if not isinstance(item, Mapping) or item.get("lane") not in by_lane:
            raise Step22BlockedError("CANDIDATE_LANE_INVALID")
        by_lane[item["lane"]] += 1
    if any(count > 25 for count in by_lane.values()):
        raise Step22BlockedError("DISCOVERY_LANE_CAP_EXCEEDED")
    current = _now(now)
    db = database if isinstance(database, Database) else Database(path)
    close_after = not isinstance(database, Database)
    lock = None
    summary = {"ok": False, "status": "BLOCKED", "run_id": handoff.get("run_id"), "lanes": {}, "counts": {"discovered": len(candidates), "deduplicated": 0, "researched": 0, "qualified": 0, "held": 0, "rejected": 0, "queued": 0, "gmail_drafts": 0, "linkedin_queue": 0}, "actor_runs": []}
    try:
        migrate_step22(db)
        config = db.read_config()
        if config.get("system_state") != "ACTIVE" or config.get("discovery_mode") != "LIVE":
            raise Step22BlockedError("PRODUCTION_SYSTEM_MUST_BE_ACTIVE_AND_LIVE")
        lock = _production_lock(DEFAULT_DB_PATH.parent / ".paldo-os-production.lock")
        run_id = str(handoff.get("run_id") or "").strip()
        if not run_id:
            raise Step22BlockedError("PRODUCTION_RUN_ID_REQUIRED")
        existing = db.connection.execute("SELECT metadata FROM events WHERE event_type='PRODUCTION_CYCLE_COMPLETED' AND entity_type='production' AND entity_id=?", (run_id,)).fetchone()
        if existing is not None:
            prior = json.loads(existing["metadata"])
            prior["reused"] = True
            return prior
        expected_actor = db.get_config("apify_actor_id")
        lane_names = {"PRIMARY": "Primary region local services", "SECONDARY": "Secondary region local services"}
        for actor in actor_runs:
            if not isinstance(actor, Mapping) or actor.get("lane") not in lane_names or actor.get("actor_id") != expected_actor or actor.get("state") != "SUCCEEDED" or not actor.get("remote_run_id") or not actor.get("dataset_id"):
                raise Step22BlockedError("APIFY_SUCCESSFUL_DATASET_HANDOFF_REQUIRED")
            cost = actor.get("cost_usd", 0)
            if isinstance(cost, bool) or not isinstance(cost, (int, float)) or cost < 0:
                raise Step22BlockedError("APIFY_COST_RECORD_INVALID")
            summary["actor_runs"].append({"lane": actor["lane"], "actor_id": actor["actor_id"], "remote_run_id": actor["remote_run_id"], "dataset_id": actor["dataset_id"], "item_count": actor.get("item_count", 0), "cost_usd": cost})
            _production_event_once(db, event_type="PRODUCTION_APIFY_RUN_RECORDED", entity_id=str(actor["remote_run_id"]), metadata={"run_id": run_id, "lane": actor["lane"], "actor_id": actor["actor_id"], "dataset_id": actor["dataset_id"], "item_count": actor.get("item_count", 0), "cost_usd": cost, "state": "SUCCEEDED"})
        processed_total = 0
        for lane in ("PRIMARY", "SECONDARY"):
            campaign = db.get_campaign_by_name(lane_names[lane])
            if campaign is None:
                raise Step22BlockedError("CAMPAIGN_NOT_FOUND")
            with db.connection:
                db.connection.execute("UPDATE campaigns SET status='INACTIVE'")
                db.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (campaign["id"],))
            lane_summary = {"discovered": 0, "deduplicated": 0, "researched": 0, "qualified": 0, "held": 0, "rejected": 0, "queued": 0, "gmail_drafts": 0, "linkedin_queue": 0, "lead_ids": [], "errors": []}
            lane_candidates = [item for item in candidates if item.get("lane") == lane]
            lane_summary["discovered"] = len(lane_candidates)
            for raw in lane_candidates:
                business_name = str(raw.get("business_name") or raw.get("name") or "").strip()
                if business_name.casefold() in EXCLUDED_BUSINESS_NAMES:
                    lane_summary["rejected"] += 1
                    _production_event_once(db, event_type="PRODUCTION_CANDIDATE_REJECTED", entity_id=str(raw.get("idempotency_key") or raw.get("source_record_id") or business_name), metadata={"reason": "EXCLUDED_BUSINESS", "lane": lane})
                    continue
                source_id = str(raw.get("source_record_id") or "").strip()
                if not source_id:
                    lane_summary["held"] += 1
                    continue
                key = str(raw.get("idempotency_key") or f"{run_id}:{lane}:{source_id}")
                if _production_event_exists(db, "PRODUCTION_CANDIDATE_COMMITTED", key) or _production_event_exists(db, "PRODUCTION_CANDIDATE_HELD", key) or _production_event_exists(db, "PRODUCTION_CANDIDATE_REJECTED", key):
                    lane_summary["deduplicated"] += 1
                    summary["counts"]["deduplicated"] += 1
                    continue
                normalized = normalize_candidate(raw, source="APIFY", mode=DiscoveryMode.LIVE, provenance_type="LIVE", collected_at=raw.get("collected_at") or _iso(current))
                db._step4_live_ingestion_authorized = True
                try:
                    ingestion = ingest_candidate(db, normalized)
                finally:
                    db._step4_live_ingestion_authorized = False
                if getattr(ingestion, "outcome", None) not in {"ACCEPTED", "DUPLICATE"} or getattr(ingestion, "lead_id", None) is None:
                    lane_summary["rejected"] += 1
                    _production_event_once(db, event_type="PRODUCTION_CANDIDATE_REJECTED", entity_id=key, metadata={"reason": getattr(ingestion, "reason", "INGESTION_REJECTED"), "lane": lane})
                    continue
                lead_id = int(ingestion.lead_id)
                lane_summary["lead_ids"].append(lead_id)
                if processed_total >= 20:
                    lane_summary["queued"] += 1
                    summary["counts"]["queued"] += 1
                    _production_event_once(db, event_type="PRODUCTION_CANDIDATE_QUEUED", entity_id=key, metadata={"reason": "DAILY_RESEARCH_CAP", "lead_id": lead_id, "lane": lane})
                    continue
                candidate = dict(raw)
                candidate["idempotency_key"] = key
                candidate["run_id"] = run_id
                candidate["source_record_id"] = source_id
                if not isinstance(candidate.get("research"), Mapping):
                    lane_summary["held"] += 1
                    _production_event_once(db, event_type="PRODUCTION_CANDIDATE_HELD", entity_id=key, metadata={"reason": "RESEARCH_PACKET_REQUIRED", "lead_id": lead_id, "lane": lane})
                    continue
                lane_summary["researched"] += 1
                processed_total += 1
                result = _production_commit_candidate(db, candidate=candidate, lead_id=lead_id, campaign=campaign, now=current, processed_index=processed_total)
                if result["status"] == "QUALIFIED":
                    lane_summary["qualified"] += 1
                    lane_summary["gmail_drafts"] += sum(1 for item in result.get("messages", []) if item.get("channel") == "EMAIL" and not item.get("reused"))
                    lane_summary["linkedin_queue"] += sum(1 for item in result.get("messages", []) if item.get("channel") == "LINKEDIN" and not item.get("reused"))
                elif result["status"] == "REJECTED":
                    lane_summary["rejected"] += 1
                else:
                    lane_summary["held"] += 1
            summary["lanes"][lane] = lane_summary
            for key in ("researched", "qualified", "held", "rejected", "gmail_drafts", "linkedin_queue"):
                summary["counts"][key] += lane_summary[key]
            with db.connection:
                db.connection.execute("UPDATE campaigns SET status='INACTIVE'")
        summary["ok"] = True
        summary["status"] = "COMPLETED"
        summary["completed_at"] = _iso(current)
        _production_event_once(db, event_type="PRODUCTION_CYCLE_COMPLETED", entity_id=run_id, metadata=summary)
        return summary
    except Exception as error:
        summary["status"] = "FAILED"
        summary["error_category"] = error.args[0] if isinstance(error, Step22BlockedError) and error.args else type(error).__name__
        if db.connection:
            with db.connection:
                db.connection.execute("UPDATE campaigns SET status='INACTIVE'")
            if summary.get("run_id"):
                _production_event_once(db, event_type="PRODUCTION_CYCLE_FAILED", entity_id=str(summary["run_id"]), metadata={"status": "FAILED", "error_category": summary["error_category"]})
        raise
    finally:
        db._step4_live_ingestion_authorized = False
        if lock is not None:
            import fcntl
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            lock.close()
        if close_after:
            db.close()


__all__ = [
    "ACTIVE_MESSAGE_POLICY_VERSION",
    "BOUNDED_RUN_MODE",
    "GMAIL_ALLOWED_ACCOUNT",
    "GMAIL_ALLOWED_ACTIONS",
    "STEP22_MIGRATION_VERSION",
    "Step22BlockedError",
    "build_message_policy_context",
    "build_outbound_queue_card",
    "collect_basic_reporting",
    "configure_production_runtime",
    "migrate_step22",
    "record_manual_event",
    "run_bounded_pilot",
    "run_production_cycle",
]
