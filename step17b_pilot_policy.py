"""Step 17B fixture-only pilot messaging policy.

This module stores no rows and never generates, sends, or submits messages. It
keeps the proposed pilot contract separate from Step 10's TEST_V0_1 policy and
from the approved Step 9 offer CTA.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import re
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit

from paldo_os_outbound import DEFAULT_DB_PATH, Database
from step10_drafting import DRAFTING_POLICY_VERSION, IMMEDIATE_OFFER_CTA
from step15_identity_personalization import BUSINESS_FIRST, LINKEDIN_ONLY
from step16_campaign_intelligence import PRIMARY_CAMPAIGN_KEY, SECONDARY_CAMPAIGN_KEY


PILOT_POLICY_VERSION = "PILOT_V0_1"
ACTIVE_MESSAGE_POLICY_VERSION = "PALDO_OS_V1_0"
ACTIVE_MESSAGE_POLICY_STATUS = "APPROVED_FOR_DRAFT_CREATION"
PILOT_READINESS_STATUS = "READY_FOR_PILOT"
ACTIVE_MESSAGE_AUTOMATIC_SENDING_APPROVED = False
PILOT_POLICY_STATUS = "PROPOSED"
PILOT_PRODUCTION_APPROVED = False
CASE_STUDY_PLACEHOLDER = "[CASE_STUDY_URL]"
APPROVED_CASE_STUDY_URL = "https://example.com/work/case-study"
MAX_OUTBOUND_TOUCHES = 3
OPERATOR_LOCAL_IDENTITY = "I’m a local automation specialist working with owner-led service businesses."
OPERATOR_LOCAL_REQUIRED_INTRO = "Hi, I’m a local automation specialist working with owner-led service businesses."
OPERATOR_INTERNATIONAL_IDENTITY = "I’m an automation specialist working with small service businesses."
APPROVED_CASE_STUDY_PROOF = "I helped move Harborview Services’s paper-based client records into a searchable digital system."
INTERNATIONAL_PROOF_PHRASE = "an local service business"
APPROVED_PROOF_OUTCOMES = (
    "Reduced no-shows",
    "Reduced manual data entry",
    "Made client records searchable",
)
ALLOWED_CHANNELS = frozenset({"EMAIL", "LINKEDIN_CONNECTION", "LINKEDIN_MESSAGE"})
LINKEDIN_CONNECTION_CHANNEL = "LINKEDIN_CONNECTION"
CASE_STUDY_INVITATION = "CASE_STUDY_INVITATION"

_LINK_RE = re.compile(r"https?://[^\s)\]>]+", re.IGNORECASE)
_YEAR_DETAIL_RE = re.compile(r"\b(?:\d{1,2}(?:st|nd|rd|th)?[- ]year|first[- ]year|second[- ]year|third[- ]year|fourth[- ]year)\b", re.IGNORECASE)
_UNSUPPORTED_PROOF_TERMS = (
    "increased bookings",
    "reduced no-shows",
    "reduced no shows",
    "increased revenue",
    "generated revenue",
    "saved time",
    "reactivated leads",
    "improved retention",
    "improved conversion",
)
_UNSUPPORTED_PAIN_TERMS = (
    "losing leads",
    "forgetting follow-ups",
    "forgetting follow ups",
    "front desk is overwhelmed",
    "front desk overwhelmed",
    "high no-show rate",
    "high no show rate",
    "empty appointment slots",
    "revenue leakage",
)


class PilotPolicyValidationError(ValueError):
    """Raised for malformed policy inputs."""


@dataclass(frozen=True)
class PilotPolicy:
    policy_version: str = PILOT_POLICY_VERSION
    status: str = PILOT_POLICY_STATUS
    production_approved: bool = PILOT_PRODUCTION_APPROVED
    approved_case_study_url: Optional[str] = APPROVED_CASE_STUDY_URL
    channel_request_status: str = "PENDING_POLICY"
    max_outbound_touches: int = MAX_OUTBOUND_TOUCHES
    primary_invitation: str = CASE_STUDY_INVITATION
    existing_drafting_policy_version: str = DRAFTING_POLICY_VERSION
    offer_cta: str = IMMEDIATE_OFFER_CTA
    operator_identity: str = OPERATOR_LOCAL_IDENTITY
    proof: str = APPROVED_CASE_STUDY_PROOF


DEFAULT_PILOT_POLICY = PilotPolicy()


def build_pilot_policy_contract(policy: PilotPolicy = DEFAULT_PILOT_POLICY) -> dict[str, Any]:
    """Return the bounded proposed policy contract without persisting it."""
    return {
        "policy_version": policy.policy_version,
        "status": policy.status,
        "production_approved": policy.production_approved,
        "channel_request_status": policy.channel_request_status,
        "channels": {
            "business_first": ["EMAIL", "LINKEDIN"],
            "linkedin_only": ["LINKEDIN"],
        },
        "max_outbound_touches": policy.max_outbound_touches,
        "reply_stops_cold_sequence": True,
        "manual_operator_review_required": True,
        "primary_invitation": policy.primary_invitation,
        "case_study_url": policy.approved_case_study_url,
        "case_study_placeholder": CASE_STUDY_PLACEHOLDER,
        "existing_drafting_policy_version": policy.existing_drafting_policy_version,
        "existing_offer_cta": policy.offer_cta,
        "operator_identity": policy.operator_identity,
        "proof": policy.proof,
        "production_activation": False,
        "scheduler_selection": False,
        "provider_selection": False,
    }


def _url_tokens(text: str) -> list[str]:
    return _LINK_RE.findall(text or "")


def _exact_https_url(url: str) -> bool:
    parsed = urlsplit(url)
    return (
        parsed.scheme == "https"
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )


def evaluate_case_study_readiness(
    policy: PilotPolicy = DEFAULT_PILOT_POLICY,
    *,
    fixture: bool,
    requested_url: Optional[str] = None,
) -> dict[str, Any]:
    """Gate the case-study URL without guessing or fetching it."""
    url = requested_url
    if fixture and url == CASE_STUDY_PLACEHOLDER:
        return {"state": "READY_FIXTURE", "reasons": [], "url": CASE_STUDY_PLACEHOLDER}
    if not fixture and not url:
        return {"state": "NOT_READY", "reasons": ["CASE_STUDY_URL_UNSET"], "url": None}
    if url == CASE_STUDY_PLACEHOLDER:
        return {"state": "NOT_READY", "reasons": ["PLACEHOLDER_NOT_ALLOWED_FOR_REAL"], "url": None}
    if not url:
        if not fixture:
            return {"state": "NOT_READY", "reasons": ["CASE_STUDY_URL_UNSET"], "url": None}
        return {"state": "NOT_READY", "reasons": ["CASE_STUDY_URL_INVALID"], "url": None}
    if not policy.approved_case_study_url:
        if not fixture and isinstance(url, str) and _exact_https_url(url):
            return {"state": "NOT_READY", "reasons": ["CASE_STUDY_URL_NOT_APPROVED"], "url": None}
        return {"state": "NOT_READY", "reasons": ["CASE_STUDY_URL_INVALID"], "url": None}
    if url != policy.approved_case_study_url:
        return {"state": "NOT_READY", "reasons": ["URL_NOT_EXACT_APPROVED_EXCEPTION"], "url": None}
    if not isinstance(url, str) or not _exact_https_url(url):
        return {"state": "NOT_READY", "reasons": ["CASE_STUDY_URL_INVALID"], "url": None}
    return {"state": "READY_APPROVED_URL", "reasons": [], "url": url}


def _runtime_database_is_fixture(database: Database) -> bool:
    return database.path.resolve() != DEFAULT_DB_PATH.resolve()


def evaluate_pilot_runtime_readiness(database: Database, *, fixture_override: bool) -> dict[str, Any]:
    """Check the fixture boundary while leaving all canonical state untouched."""
    reasons: list[str] = []
    if not fixture_override:
        reasons.append("FIXTURE_ONLY_REQUIRED")
    if not _runtime_database_is_fixture(database):
        reasons.append("CANONICAL_DATABASE_BLOCKED")
    config = database.read_config()
    if config.get("system_state") != "PAUSED":
        reasons.append("SYSTEM_NOT_PAUSED")
    if int(config.get("daily_message_cap", 0)) != 0:
        reasons.append("LIVE_MESSAGE_CAP_NOT_ZERO")
    if int(config.get("gmail_send_enabled", 0)) != 0:
        reasons.append("GMAIL_SEND_NOT_DISABLED")
    if int(config.get("linkedin_sending_enabled", 0)) != 0:
        reasons.append("LINKEDIN_SEND_NOT_DISABLED")
    return {
        "state": "READY_FIXTURE" if not reasons else "BLOCKED",
        "reasons": sorted(set(reasons)),
        "pilot_readiness_status": database.get_config("pilot_readiness_status"),
        "message_policy_version": database.get_config("message_policy_version"),
        "message_policy_status": database.get_config("message_policy_status"),
        "automatic_sending_approved": bool(database.get_config("message_policy_automatic_sending", 0)),
    }


def evaluate_touch_limit(touches_used: int, *, replied: bool) -> dict[str, Any]:
    """Enforce one shared allowance across email and LinkedIn."""
    if isinstance(touches_used, bool) or not isinstance(touches_used, int) or touches_used < 0:
        raise PilotPolicyValidationError("touches_used must be a non-negative integer")
    reasons: list[str] = []
    if replied:
        reasons.append("REPLY_STOPS_COLD_SEQUENCE")
    if touches_used >= MAX_OUTBOUND_TOUCHES:
        reasons.append("SHARED_TOUCH_LIMIT_REACHED")
    return {"allowed": not reasons, "touches_used": touches_used, "remaining": max(0, MAX_OUTBOUND_TOUCHES - touches_used), "reasons": reasons}


def _append_error(errors: list[str], code: str) -> None:
    if code not in errors:
        errors.append(code)


def validate_pilot_message(message: Mapping[str, Any], policy: PilotPolicy = DEFAULT_PILOT_POLICY) -> dict[str, Any]:
    """Validate a fictional or future approved case-study invitation."""
    errors: list[str] = []
    channel = message.get("channel")
    body = message.get("body")
    subject = message.get("subject")
    fixture = bool(message.get("fixture", False))
    requested_url = message.get("case_study_url")
    if channel not in ALLOWED_CHANNELS:
        _append_error(errors, "CHANNEL_UNSUPPORTED")
    if not isinstance(body, str) or not body.strip():
        _append_error(errors, "BODY_REQUIRED")
        body = ""
    if channel == "EMAIL" and (not isinstance(subject, str) or not subject.strip()):
        _append_error(errors, "SUBJECT_REQUIRED")
    if isinstance(subject, str) and re.match(r"^\s*(re|fwd|fw)\s*:", subject, re.IGNORECASE):
        _append_error(errors, "SUBJECT_PREFIX_FORBIDDEN")
    if OPERATOR_LOCAL_IDENTITY not in body:
        _append_error(errors, "FIRST_PERSON_IDENTITY_REQUIRED")
    if _YEAR_DETAIL_RE.search(body):
        _append_error(errors, "UNAPPROVED_IDENTITY_DETAIL")
    if "Harborview" in body and "Harborview Services" not in body:
        _append_error(errors, "FULL_CASE_STUDY_NAME_REQUIRED")
    if "I built Harborview" in body or "Operator builds" in body:
        _append_error(errors, "FIRST_PERSON_PROOF_REQUIRED")
    if APPROVED_CASE_STUDY_PROOF not in body:
        _append_error(errors, "APPROVED_PROOF_REQUIRED")
    lowered = body.casefold()
    if any(term in lowered for term in _UNSUPPORTED_PROOF_TERMS):
        _append_error(errors, "UNSUPPORTED_PROOF_CLAIM")
    if any(term in lowered for term in _UNSUPPORTED_PAIN_TERMS):
        _append_error(errors, "UNSUPPORTED_PAIN_ASSERTION")
    if IMMEDIATE_OFFER_CTA.casefold().replace("[business name]", "") in lowered or "15-minute conversation" in lowered:
        _append_error(errors, "CALL_CTA_NOT_COMBINED_WITH_CASE_STUDY")
    if "attachment" in lowered or ".pdf" in lowered:
        _append_error(errors, "ATTACHMENTS_FORBIDDEN")
    if channel == LINKEDIN_CONNECTION_CHANNEL and (CASE_STUDY_PLACEHOLDER in body or _url_tokens(body)):
        _append_error(errors, "LINKEDIN_CONNECTION_LINK_FORBIDDEN")
    readiness = evaluate_case_study_readiness(policy, fixture=fixture, requested_url=requested_url)
    errors.extend(reason for reason in readiness["reasons"] if reason not in errors)
    tokens = _url_tokens(body)
    if len(tokens) > 1 or any(token != requested_url for token in tokens):
        _append_error(errors, "ONLY_CASE_STUDY_LINK_ALLOWED")
    if requested_url and requested_url not in body:
        _append_error(errors, "CASE_STUDY_LINK_MISSING_FROM_BODY")
    if not fixture and readiness["state"] != "READY_APPROVED_URL":
        _append_error(errors, "REAL_DRAFTING_NOT_READY")
    return {
        "valid": not errors,
        "error_codes": errors,
        "normalized_body": body if not errors else None,
        "channel": channel,
        "case_study_state": readiness["state"],
    }


def select_relevant_outcomes(proposed_angle: str) -> list[str]:
    """Return the one or two approved outcomes matching the researched angle."""
    if not isinstance(proposed_angle, str) or not proposed_angle.strip():
        return []
    lowered = proposed_angle.casefold()
    matches: list[str] = []
    if any(term in lowered for term in ("no-show", "no show", "missed appointment", "cancellation", "confirmation", "reminder")):
        matches.append("Reduced no-shows")
    if any(term in lowered for term in ("manual entry", "manual data", "encoding", "re-enter", "reenter", "repetitive entry")):
        matches.append("Reduced manual data entry")
    if any(term in lowered for term in ("searchable", "history", "records", "treatment plan", "client details", "lookup")):
        matches.append("Made client records searchable")
    return matches


def select_relevant_outcome(proposed_angle: str) -> Optional[str]:
    """Backward-compatible single-outcome selector."""
    matches = select_relevant_outcomes(proposed_angle)
    return matches[0] if len(matches) == 1 else None


def _proof_for_outcome(outcome: str, *, local: bool, combined: bool = False) -> str:
    if combined:
        if local:
            return "At Harborview Services in Westport, I built a system that reduced manual data entry and made client records searchable."
        return "I built a similar system for an local service business that reduced manual data entry and made client records searchable."
    if outcome == "Reduced no-shows":
        return "At Harborview Services in Westport, I helped reduce no-shows." if local else "I helped reduce no-shows at an local service business I worked with."
    if outcome == "Reduced manual data entry":
        return "At Harborview Services in Westport, I helped reduce manual data entry." if local else "I helped reduce manual data entry at an local service business I worked with."
    return "At Harborview Services in Westport, I helped make client records searchable." if local else "I helped make client records searchable at an local service business I worked with."


def build_message_policy_context(*, campaign_key: str, channel: str, proposed_angle: str, message_stage: str = "INITIAL", original_subject: Optional[str] = None, thread_id: Optional[str] = None, recipient_first_name: Optional[str] = None) -> dict[str, Any]:
    """Build the approved versioned Writer contract without generating message text."""
    if campaign_key not in {PRIMARY_CAMPAIGN_KEY, SECONDARY_CAMPAIGN_KEY}:
        raise PilotPolicyValidationError("campaign_key is not an active Step 22 campaign")
    if channel not in {"EMAIL", "LINKEDIN"}:
        raise PilotPolicyValidationError("channel is not supported by the Step 22 message path")
    if message_stage not in {"INITIAL", "FOLLOWUP_1", "FINAL_FOLLOWUP"}:
        raise PilotPolicyValidationError("message_stage is not supported by PALDO_OS_V1_0")
    selected_outcomes = select_relevant_outcomes(proposed_angle)
    if not 1 <= len(selected_outcomes) <= 2:
        raise PilotPolicyValidationError("proposed_angle must map to one or two approved outcomes")
    local = campaign_key == PRIMARY_CAMPAIGN_KEY
    proof = _proof_for_outcome(selected_outcomes[0], local=local, combined=len(selected_outcomes) == 2)
    return {
        "message_policy_version": ACTIVE_MESSAGE_POLICY_VERSION,
        "policy_status": ACTIVE_MESSAGE_POLICY_STATUS,
        "automatic_sending_approved": ACTIVE_MESSAGE_AUTOMATIC_SENDING_APPROVED,
        "manual_operator_review_required": True,
        "campaign_key": campaign_key,
        "market": "PRIMARY" if local else "INTERNATIONAL",
        "channel": channel,
        "message_stage": message_stage,
        "original_subject": original_subject,
        "thread_id": thread_id,
        "recipient_first_name": recipient_first_name,
        "proposed_angle": " ".join(proposed_angle.split())[:500],
        "selected_outcome": selected_outcomes[0],
        "selected_outcomes": selected_outcomes,
        "proof": proof,
        "case_study_url": APPROVED_CASE_STUDY_URL,
        "required_identity": OPERATOR_LOCAL_REQUIRED_INTRO if local else OPERATOR_INTERNATIONAL_IDENTITY,
        "writer_skill_sequence": ["product-marketing", "cold-email", "Unslop"],
        "reviewer_required": True,
        "max_total_touches": MAX_OUTBOUND_TOUCHES,
        "followups_same_thread": True,
        "followups_retain_subject": True,
    }


def validate_versioned_message_policy(message: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the active Step 22 copy contract for one staged message."""
    errors: list[str] = []
    body_value = message.get("body")
    body: str = body_value if isinstance(body_value, str) else ""
    subject_value = message.get("subject")
    subject: str = subject_value if isinstance(subject_value, str) else ""
    stage = message.get("message_stage", context.get("message_stage", "INITIAL"))
    if context.get("message_policy_version") != ACTIVE_MESSAGE_POLICY_VERSION:
        errors.append("MESSAGE_POLICY_VERSION_INVALID")
    if context.get("policy_status") != ACTIVE_MESSAGE_POLICY_STATUS:
        errors.append("MESSAGE_POLICY_STATUS_INVALID")
    if context.get("automatic_sending_approved") is not False:
        errors.append("AUTOMATIC_SENDING_NOT_ALLOWED")
    if stage not in {"INITIAL", "FOLLOWUP_1", "FINAL_FOLLOWUP"}:
        errors.append("MESSAGE_STAGE_INVALID")
    if not subject.strip():
        errors.append("SUBJECT_REQUIRED")
    if re.match(r"^\s*(re|fwd|fw)\s*:", subject, re.IGNORECASE):
        errors.append("SUBJECT_PREFIX_FORBIDDEN")
    if not body.strip():
        errors.append("BODY_REQUIRED")
    question_count = body.count("?")
    if stage in {"INITIAL", "FOLLOWUP_1"} and question_count != 1:
        errors.append("EXACTLY_ONE_QUESTION_CTA_REQUIRED")
    if stage == "FINAL_FOLLOWUP" and question_count != 0:
        errors.append("FINAL_FOLLOWUP_MUST_HAVE_NO_QUESTION")
    normalized = body.casefold().replace("’", "'")
    if "handoff" in normalized:
        errors.append("HANDOFF_JARGON_FORBIDDEN")
    if any(term in normalized for term in ("guarantee", "guaranteed", "roi", "revenue", "%", "save you", "will increase")):
        errors.append("GUARANTEES_OR_UNSUPPORTED_METRICS_FORBIDDEN")
    if any(phrase in normalized for phrase in (
        "not because i'm assuming there's a problem",
        "not because i'm assuming there is a problem",
        "that is a hypothesis, not a claim",
        "i haven't confirmed this is happening",
    )):
        errors.append("DEFENSIVE_UNCERTAINTY_FORBIDDEN")
    if any(phrase in normalized for phrase in ("compare how", "compare the current process", "compare how things are handled")):
        errors.append("VAGUE_COMPARE_REQUEST_FORBIDDEN")
    local = context.get("market") == "PRIMARY"
    if stage == "INITIAL":
        recipient_first_name = context.get("recipient_first_name")
        if recipient_first_name is not None and (not isinstance(recipient_first_name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z' -]{0,79}", recipient_first_name.strip())):
            errors.append("RECIPIENT_FIRST_NAME_INVALID")
        elif isinstance(recipient_first_name, str) and recipient_first_name.strip() and not body.startswith(f"Hi {recipient_first_name.strip()},"):
            errors.append("VERIFIED_OWNER_GREETING_REQUIRED")
        if local:
            if not body.startswith(OPERATOR_LOCAL_REQUIRED_INTRO):
                errors.append("LOCAL_INTRO_REQUIRED")
            if OPERATOR_LOCAL_IDENTITY not in body:
                errors.append("LOCAL_IDENTITY_REQUIRED")
        else:
            if OPERATOR_INTERNATIONAL_IDENTITY not in body:
                errors.append("INTERNATIONAL_IDENTITY_REQUIRED")
            if INTERNATIONAL_PROOF_PHRASE not in body:
                errors.append("INTERNATIONAL_PROOF_PHRASE_REQUIRED")
            body_without_urls = _LINK_RE.sub("", body).casefold()
            if any(term in body_without_urls for term in ("harborview", "westport", "northfield")):
                errors.append("INTERNATIONAL_LOCAL_DETAIL_FORBIDDEN")
        selected_outcomes = context.get("selected_outcomes")
        if not isinstance(selected_outcomes, list):
            selected_outcomes = [context.get("selected_outcome")]
        if not 1 <= len(selected_outcomes) <= 2 or any(outcome not in APPROVED_PROOF_OUTCOMES for outcome in selected_outcomes):
            errors.append("APPROVED_OUTCOME_COUNT_INVALID")
        proof = context.get("proof")
        if not isinstance(proof, str) or proof not in body:
            errors.append("SELECTED_OUTCOME_PROOF_REQUIRED")
        urls = _url_tokens(body)
        url = context.get("case_study_url")
        if urls != [url]:
            errors.append("EXACT_CASE_STUDY_LINK_REQUIRED")
    else:
        if _url_tokens(body):
            errors.append("FOLLOWUP_LINK_FORBIDDEN")
        expected_thread = context.get("thread_id")
        if not isinstance(expected_thread, str) or not expected_thread:
            errors.append("FOLLOWUP_THREAD_CONTEXT_REQUIRED")
        elif message.get("thread_id") != expected_thread:
            errors.append("FOLLOWUP_THREAD_MISMATCH")
        expected_subject = context.get("original_subject")
        if not isinstance(expected_subject, str) or not expected_subject:
            errors.append("FOLLOWUP_SUBJECT_CONTEXT_REQUIRED")
        elif subject != expected_subject:
            errors.append("FOLLOWUP_SUBJECT_MISMATCH")
    return {"valid": not errors, "error_codes": sorted(set(errors))}


__all__ = [
    "ACTIVE_MESSAGE_POLICY_VERSION",
    "ACTIVE_MESSAGE_POLICY_STATUS",
    "ACTIVE_MESSAGE_AUTOMATIC_SENDING_APPROVED",
    "APPROVED_CASE_STUDY_URL",
    "APPROVED_PROOF_OUTCOMES",
    "CASE_STUDY_PLACEHOLDER",
    "DEFAULT_PILOT_POLICY",
    "IMMEDIATE_OFFER_CTA",
    "INTERNATIONAL_PROOF_PHRASE",
    "LINKEDIN_ONLY",
    "MAX_OUTBOUND_TOUCHES",
    "APPROVED_CASE_STUDY_PROOF",
    "PILOT_POLICY_VERSION",
    "PILOT_READINESS_STATUS",
    "PilotPolicy",
    "PilotPolicyValidationError",
    "OPERATOR_LOCAL_IDENTITY",
    "OPERATOR_LOCAL_REQUIRED_INTRO",
    "OPERATOR_INTERNATIONAL_IDENTITY",
    "build_message_policy_context",
    "build_pilot_policy_contract",
    "evaluate_case_study_readiness",
    "evaluate_pilot_runtime_readiness",
    "evaluate_touch_limit",
    "select_relevant_outcome",
    "select_relevant_outcomes",
    "validate_pilot_message",
    "validate_versioned_message_policy",
]
