"""Static, versioned campaign intelligence for Paldo OS Step 16.

This module contains only operator-supplied campaign context. It does not
retrieve sources, generate copy, change campaign state, or perform transport.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any, Mapping, Optional


CAMPAIGN_INTELLIGENCE_VERSION = "1.0.0"
PRIMARY_CAMPAIGN_KEY = "primary_region_local_services"
SECONDARY_CAMPAIGN_KEY = "secondary_region_local_services"


class CampaignIntelligenceValidationError(ValueError):
    """Raised when static intelligence is malformed or makes an unsupported claim."""


def _concept(concept_type: str, label: str, signals: list[str], outcomes: list[str]) -> dict[str, Any]:
    return {
        "concept_type": concept_type,
        "label": label,
        "observable_signals": list(signals),
        "desired_outcomes": list(outcomes),
    }


_PROOF_ASSET = {
    "name": "Harborview",
    "description": (
        "Harborview is proof that Operator designed and built a working local-service-business "
        "operational automation pipeline covering digital/searchable client records, "
        "client/history visibility, booking pipeline, appointment status, "
        "attendance/no-show tracking, visibility into who needs follow-up, "
        "follow-up workflow visibility, lead reactivation, and structured next actions "
        "across the business pipeline."
    ),
    "allowed_claims": [
        "Operator designed and built the working business operational pipeline.",
        "The pipeline covers records, bookings, appointment status, attendance, follow-up, reactivation, and next actions.",
    ],
    "quantified_outcomes": [],
}


_COMMON_VOCABULARY = [
    "no-shows",
    "last-minute cancellations",
    "appointment confirmation",
    "appointment reminders",
    "rebooking / prebooking",
    "client retention",
    "lapsed-client reactivation",
    "lead reactivation",
    "win-back campaign",
    "consultation conversion",
    "missed-call recovery",
    "waitlist / backfill",
    "revenue leakage",
    "inquiry-to-booking conversion",
    "consultation follow-up",
    "follow-up ownership",
    "dormant leads",
    "empty appointment slots",
    "booking pipeline",
    "front-desk workload",
    "multi-channel inquiries",
    "appointment status",
    "next action",
    "booking friction",
]


_PRIMARY = {
    "intelligence_id": "local-services.campaign-intelligence.primary-region",
    "campaign_key": PRIMARY_CAMPAIGN_KEY,
    "campaign_name": "Primary region local services",
    "geography": "Northfield",
    "industry": "local service and local businesses",
    "version": CAMPAIGN_INTELLIGENCE_VERSION,
    "status": "FIXTURE_ONLY_CONTEXT",
    "approval_status": "OPERATOR_SUPPLIED",
    "industry_vocabulary": list(_COMMON_VOCABULARY),
    "problem_search_phrases": [
        "local service business no show problem",
        "local service business no shows",
        "last minute cancellations local service business",
        "local business cancellations",
        "clients not rebooking",
        "local service business lead follow up",
        "old leads not converting",
        "front desk overwhelmed local service business",
        "missed calls during treatments",
        "manual appointment reminders",
        "clients still DM instead of booking",
        "empty appointment slots",
        "consultation follow up",
        "consultations not converting",
        "reactivate old local service business leads",
        "reduce no shows",
        "local business booking problems",
    ],
    "peer_language": [
        "People book and then disappear.",
        "Same-day cancellations leave holes in the schedule.",
        "I am tired of chasing confirmations.",
        "By the time we reply they already booked somewhere else.",
        "I miss calls while I am with a client.",
        "They still message us even though we have a booking link.",
        "Follow-ups get forgotten when we are busy.",
        "We have old leads just sitting there.",
        "They come once and never rebook.",
        "Front desk is doing everything manually.",
        "We are juggling DMs, calls, and bookings.",
        "I do not know who needs following up next.",
        "I need a better way to keep the calendar full.",
        "Someone has to remember every follow-up.",
    ],
    "local_language": [
        "PM us",
        "message us",
        "DM us",
        "book your appointment",
        "reserve your slot",
        "limited slots",
        "walk-ins",
        "by appointment",
        "free consultation",
        "message for available slots",
        "Facebook Messenger",
        "Instagram",
        "WhatsApp",
        "Viber",
        "phone calls",
        "website booking",
        "manual confirmation",
        "GCash / deposit confirmation",
        "promos",
        "packages",
    ],
    "pain_families": {
        "inquiry_booking_leakage": _concept(
            "PAIN_FAMILY",
            "Inquiry to booking leakage",
            ["multiple inquiry channels", "Messenger or Instagram inquiries", "phone calls", "booking forms", "consultation requests", "promotions generating inquiries"],
            ["respond faster", "make follow-up consistent", "move qualified inquiries toward appointments", "reduce inquiries falling through the cracks"],
        ),
        "no_shows_cancellations": _concept(
            "PAIN_FAMILY",
            "No-shows and cancellations",
            ["cancellation policy", "deposits", "appointment confirmation", "reminders", "rebooking instructions", "limited slots"],
            ["improve confirmation workflows", "make rescheduling and recovery easier", "help staff refill open capacity"],
        ),
        "follow_up_ownership": _concept(
            "PAIN_FAMILY",
            "Follow-up ownership",
            ["multiple staff or contact channels", "manual confirmation", "consultations", "high-value treatments", "repeated visits"],
            ["make clear who needs follow-up", "give staff a next action", "reduce reliance on memory and manual checking"],
        ),
        "rebooking_retention": _concept(
            "PAIN_FAMILY",
            "Rebooking and retention",
            ["recurring treatments", "packages", "memberships", "repeat-service model", "post-treatment instructions"],
            ["make rebooking more systematic", "keep past clients engaged", "improve continuity between visits"],
        ),
        "dormant_reactivation": _concept(
            "PAIN_FAMILY",
            "Dormant lead and client reactivation",
            ["promotions", "old client database", "consultations", "recurring services", "previous inquiries"],
            ["identify leads or previous clients worth reconnecting with", "systematically reactivate opportunities"],
        ),
        "front_desk_coordination": _concept(
            "PAIN_FAMILY",
            "Front-desk and staff coordination",
            ["several communication channels", "several practitioners", "several services", "long opening hours", "multiple booking routes"],
            ["reduce repetitive manual coordination", "centralize status", "give staff clearer operational visibility"],
        ),
    },
    "urgency_signals": [
        "new branch", "new location", "business expansion", "front-desk or reception hiring",
        "new service launch", "new treatment launch", "major promotion", "anniversary promotion",
        "limited-slot promotion", "seasonal campaign", "holiday campaign", "heavy advertising activity",
        "new practitioner", "extended opening hours", "membership or package launch", "high visible booking activity",
    ],
    "objections": [
        "We already have booking software.",
        "Our staff already handles follow-ups.",
        "Most bookings come through Messenger.",
        "We are too small for automation.",
        "We do not need another CRM.",
        "We already send reminders.",
        "We do not want AI talking to clients incorrectly.",
        "We handle sensitive client information.",
        "We do not want to replace our existing software.",
        "We need more customers, not automation.",
        "We do not want something complicated.",
    ],
    "lead_magnet_signals": [
        "free consultation", "free assessment", "free skin analysis", "free guide", "free checklist",
        "free template", "free audit", "DM us", "comment [keyword]", "message us", "claim your slot", "reserve your slot",
    ],
    "proof_assets": {"harborview": copy.deepcopy(_PROOF_ASSET)},
    "trust_context": {
        "optional": True,
        "approved_context": [
            "independent automation specialist",
            "builds booking, follow-up, and internal workflow systems",
            "based in or connected to the pilot region",
            "documents every claim with public evidence",
        ],
    },
    "prohibited_assumptions": [
        "Do not assume every business uses the social inquiry to appointment workflow.",
        "Do not attribute peer-language complaints to a business without equivalent public evidence.",
        "Do not infer no-show, cancellation, rebooking, revenue, staffing, or tool problems from vocabulary alone.",
        "Do not invent urgency, performance results, or quantified outcomes.",
    ],
    "messaging_policy": {
        "status": "UNDECIDED",
        "production_copy": False,
        "subject_line": "UNDECIDED",
        "opening_style": "UNDECIDED",
        "cta": "UNDECIDED",
        "proof_placement": "UNDECIDED",
    },
}


_US = {
    "intelligence_id": "local-services.campaign-intelligence.secondary-region",
    "campaign_key": SECONDARY_CAMPAIGN_KEY,
    "campaign_name": "Secondary region local services",
    "geography": "United States",
    "industry": "local service businesses",
    "version": CAMPAIGN_INTELLIGENCE_VERSION,
    "status": "FIXTURE_ONLY_CONTEXT",
    "approval_status": "OPERATOR_SUPPLIED",
    "industry_vocabulary": [
        "speed-to-lead", "consultation conversion", "online booking conversion", "client retention",
        "client retention", "membership retention", "no-show rate", "cancellation rate", "rebooking rate",
        "first rebook", "lead nurturing", "dormant-lead reactivation", "client reactivation", "win-back",
        "treatment plan follow-up", "front desk", "intake", "missed-call handling", "lead response time",
        "revenue per appointment", "schedule utilization",
    ],
    "problem_search_phrases": [
        "local service business lead follow up", "local service business lead follow up", "old leads not converting",
        "front desk overwhelmed local service business", "missed calls during treatments", "consultation follow up",
        "consultations not converting", "reactivate old local service business leads", "reduce no shows",
        "clients not rebooking", "last minute cancellations local service business", "empty appointment slots",
    ],
    "peer_language": [
        "People book and then disappear.",
        "Same-day cancellations leave holes in the schedule.",
        "By the time we reply they already booked somewhere else.",
        "Follow-ups get forgotten when we are busy.",
        "We have old leads just sitting there.",
        "They come once and never rebook.",
        "I do not know who needs following up next.",
        "We spend money getting inquiries but not all of them book.",
    ],
    "local_language": [],
    "pain_families": {
        "inquiry_booking_leakage": _concept(
            "PAIN_FAMILY",
            "Inquiry to booking leakage",
            ["multi-channel inquiries", "lead response time", "consultation requests", "online booking conversion"],
            ["improve speed-to-lead", "move qualified inquiries toward consultations", "reduce inquiry leakage"],
        ),
        "no_shows_cancellations": _concept(
            "PAIN_FAMILY",
            "No-shows and cancellations",
            ["no-show rate", "cancellation rate", "appointment confirmation", "reminders", "schedule utilization"],
            ["improve confirmation and recovery workflows", "make rescheduling easier", "protect schedule utilization"],
        ),
        "follow_up_ownership": _concept(
            "PAIN_FAMILY",
            "Follow-up ownership",
            ["front desk", "intake", "treatment plan follow-up", "lead nurturing"],
            ["make the next action visible", "clarify ownership", "reduce repetitive manual coordination"],
        ),
        "rebooking_retention": _concept(
            "PAIN_FAMILY",
            "Rebooking and retention",
            ["first rebook", "membership retention", "client retention", "treatment plan follow-up"],
            ["improve rebooking consistency", "support continuity between visits", "keep past clients engaged"],
        ),
        "dormant_reactivation": _concept(
            "PAIN_FAMILY",
            "Dormant lead and client reactivation",
            ["dormant-lead reactivation", "client reactivation", "win-back", "old leads"],
            ["identify opportunities worth reconnecting with", "systematically reactivate dormant demand"],
        ),
        "front_desk_coordination": _concept(
            "PAIN_FAMILY",
            "Front-desk and staff coordination",
            ["front desk", "intake", "missed-call handling", "multiple booking routes"],
            ["centralize status", "give staff clearer operational visibility", "reduce manual coordination"],
        ),
    },
    "urgency_signals": [
        "new branch", "new location", "business expansion", "front-desk or reception hiring", "new practitioner",
        "new service launch", "new treatment launch", "major promotion", "seasonal campaign", "heavy advertising activity",
        "extended opening hours", "membership or package launch", "high visible booking activity",
    ],
    "objections": [
        "We already have booking software.",
        "Our staff already handles follow-ups.",
        "We are too small for automation.",
        "We do not need another CRM.",
        "Our current system works.",
        "We already send reminders.",
        "This sounds expensive.",
        "We handle sensitive client information.",
        "We do not want to replace our existing software.",
        "We need more customers, not automation.",
        "We do not want something complicated.",
    ],
    "lead_magnet_signals": [
        "free consultation", "free assessment", "free skin analysis", "free guide", "free checklist",
        "free audit", "message us", "claim your slot", "reserve your slot",
    ],
    "proof_assets": {"harborview": copy.deepcopy(_PROOF_ASSET)},
    "trust_context": [],
    "prohibited_assumptions": [
        "Do not assume a business has a specific no-show, cancellation, retention, or staffing problem.",
        "Do not treat industry vocabulary as evidence about an individual business.",
        "Do not invent urgency, performance results, revenue, conversion rates, or quantified outcomes.",
        "Do not force US terminology into a Northfield/local personalization packet.",
    ],
    "messaging_policy": {
        "status": "UNDECIDED",
        "production_copy": False,
        "subject_line": "UNDECIDED",
        "opening_style": "UNDECIDED",
        "cta": "UNDECIDED",
        "proof_placement": "UNDECIDED",
    },
}


_PACKS = {PRIMARY_CAMPAIGN_KEY: _PRIMARY, SECONDARY_CAMPAIGN_KEY: _US}
_QUANTIFIED_OUTCOME = re.compile(
    r"(?:\b\d+(?:\.\d+)?\s*%|\b(?:increased|reduced|improved|saved|generated)\b[^.]{0,80}\b(?:revenue|bookings|no-shows|conversions|appointments)\b)",
    re.IGNORECASE,
)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _validate_pack_shape(pack: Mapping[str, Any]) -> None:
    required = {
        "intelligence_id", "campaign_key", "campaign_name", "geography", "industry", "version",
        "status", "approval_status", "industry_vocabulary", "problem_search_phrases", "peer_language",
        "pain_families", "urgency_signals", "objections", "lead_magnet_signals", "proof_assets",
        "trust_context", "prohibited_assumptions", "messaging_policy",
    }
    missing = sorted(required - set(pack))
    if missing:
        raise CampaignIntelligenceValidationError("missing intelligence fields: " + ", ".join(missing))
    if pack["version"] != CAMPAIGN_INTELLIGENCE_VERSION:
        raise CampaignIntelligenceValidationError("unsupported intelligence version")
    if pack["status"] != "FIXTURE_ONLY_CONTEXT" or pack["approval_status"] != "OPERATOR_SUPPLIED":
        raise CampaignIntelligenceValidationError("intelligence must remain operator-supplied fixture context")
    if pack["messaging_policy"].get("status") != "UNDECIDED" or pack["messaging_policy"].get("production_copy") is not False:
        raise CampaignIntelligenceValidationError("production messaging policy must remain undecided")


def validate_campaign_intelligence(pack: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return a deep copy of one static campaign pack."""
    _validate_pack_shape(pack)
    proof_text = _json(pack.get("proof_assets", {}))
    if _QUANTIFIED_OUTCOME.search(proof_text):
        raise CampaignIntelligenceValidationError("quantified proof outcome is not independently verified")
    if "Routed Cloud" in _json(pack):
        raise CampaignIntelligenceValidationError("unsupported sender branding")
    if pack["campaign_key"] not in _PACKS:
        raise CampaignIntelligenceValidationError("campaign key is not supported")
    return copy.deepcopy(dict(pack))


def list_campaign_intelligence_keys() -> tuple[str, ...]:
    """Return the only two Step 16 campaign packs in deterministic order."""
    return tuple(sorted(_PACKS))


def get_campaign_intelligence(campaign_key: str) -> dict[str, Any]:
    """Return one validated copy of a campaign pack."""
    if campaign_key not in _PACKS:
        raise CampaignIntelligenceValidationError("unknown campaign intelligence key")
    return validate_campaign_intelligence(_PACKS[campaign_key])


def campaign_intelligence_for_campaign(database: Any, campaign_id: int) -> dict[str, Any]:
    """Resolve existing campaign structure to one of the two static packs."""
    row = database.connection.execute("SELECT name FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    if row is None:
        raise CampaignIntelligenceValidationError("campaign not found")
    names = {
        "Primary region local services": PRIMARY_CAMPAIGN_KEY,
        "Secondary region local services": SECONDARY_CAMPAIGN_KEY,
    }
    key = names.get(row["name"])
    if key is None:
        raise CampaignIntelligenceValidationError("campaign has no Step 16 intelligence pack")
    return get_campaign_intelligence(key)


def select_campaign_concepts(campaign_key: str, concept_keys: Optional[list[str]] = None) -> list[dict[str, Any]]:
    """Select compact, typed references without copying production messaging."""
    pack = get_campaign_intelligence(campaign_key)
    available = pack["pain_families"]
    selected_keys = sorted(available) if concept_keys is None else sorted(set(concept_keys))
    unknown = [key for key in selected_keys if key not in available]
    if unknown:
        raise CampaignIntelligenceValidationError("unknown concept keys: " + ", ".join(unknown))
    return [
        {
            "campaign_key": campaign_key,
            "version": pack["version"],
            "concept_key": key,
            "concept_type": available[key]["concept_type"],
            "label": available[key]["label"],
            "observable_signals": list(available[key]["observable_signals"]),
            "desired_outcomes": list(available[key]["desired_outcomes"]),
        }
        for key in selected_keys
    ]


def campaign_intelligence_fingerprint(campaign_key: str, concept_keys: Optional[list[str]] = None) -> str:
    """Return a deterministic fingerprint for a pack or selected references."""
    pack = get_campaign_intelligence(campaign_key)
    selected = select_campaign_concepts(campaign_key, concept_keys)
    return hashlib.sha256(_json({"pack": pack, "selected": selected}).encode("utf-8")).hexdigest()


__all__ = [
    "CAMPAIGN_INTELLIGENCE_VERSION",
    "CampaignIntelligenceValidationError",
    "PRIMARY_CAMPAIGN_KEY",
    "SECONDARY_CAMPAIGN_KEY",
    "campaign_intelligence_fingerprint",
    "campaign_intelligence_for_campaign",
    "get_campaign_intelligence",
    "list_campaign_intelligence_keys",
    "select_campaign_concepts",
    "validate_campaign_intelligence",
]
