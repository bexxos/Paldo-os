"""Deterministic Step 3 candidate discovery and ingestion primitives.

This module is intentionally source-independent and network-free. Adapters accept
fictional records in DRY_RUN; LIVE is only a guarded, explicitly disabled seam
for future collectors. No raw payloads or credentials are persisted.
"""

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional
from urllib.parse import urlsplit, urlunsplit


STEP3_MIGRATION_VERSION = 4
GOOGLE_PLACES_SOURCE = "GOOGLE_PLACES"
APIFY_SOURCE = "APIFY"
SUPPORTED_SOURCES = frozenset({GOOGLE_PLACES_SOURCE, APIFY_SOURCE})


class DiscoveryMode(str, Enum):
    DRY_RUN = "DRY_RUN"
    LIVE = "LIVE"


class CandidateStatus(str, Enum):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    HELD = "HELD"
    DUPLICATE = "DUPLICATE"


class LiveModeBlockedError(RuntimeError):
    """Raised whenever LIVE collection is not permitted or not implemented."""


@dataclass(frozen=True)
class NormalizedCandidate:
    """Allowlisted, source-independent candidate contract.

    ``observed_values`` contains only original values from the public allowlist;
    it is not an unrestricted source payload.
    """

    source: Optional[str]
    source_record_id: Optional[str]
    source_url: Optional[str]
    collected_at: str
    business_name: Optional[str]
    business_category: Optional[str]
    website_url: Optional[str]
    normalized_domain: Optional[str]
    public_business_email: Optional[str]
    normalized_email: Optional[str]
    public_business_phone: Optional[str]
    normalized_phone: Optional[str]
    street_address: Optional[str]
    city: Optional[str]
    region_state: Optional[str]
    postal_code: Optional[str]
    country: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    google_place_id: Optional[str]
    booking_url: Optional[str]
    business_status: Optional[str]
    raw_payload_hash: str
    ingestion_status: str = CandidateStatus.PENDING.value
    rejection_hold_reason: Optional[str] = None
    provenance_mode: str = DiscoveryMode.DRY_RUN.value
    provenance_type: str = "FIXTURE"
    observed_values: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class IngestionResult:
    """Result of one deterministic candidate ingestion decision."""

    candidate_id: int
    status: str
    reason: Optional[str]
    lead_id: Optional[int]
    event_id: Optional[int]
    idempotent: bool = False

    def as_dict(self):
        return asdict(self)


_PUBLIC_FIELDS = (
    "source_record_id",
    "source_url",
    "business_name",
    "business_category",
    "website_url",
    "domain",
    "public_business_email",
    "public_business_phone",
    "street_address",
    "city",
    "region_state",
    "postal_code",
    "country",
    "latitude",
    "longitude",
    "google_place_id",
    "booking_url",
    "business_status",
    "rating",
    "review_count",
)


def _collapsed_text(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    return " ".join(unicodedata.normalize("NFKC", value).split()) or None


def _normalized_label(value):
    value = _collapsed_text(value)
    return value.casefold().title() if value else None


def _normalized_country(value):
    value = _collapsed_text(value)
    if not value:
        return None
    aliases = {
        "ph": "PH",
        "philippines": "PH",
        "the philippines": "PH",
        "us": "US",
        "usa": "US",
        "united states": "US",
        "united states of america": "US",
    }
    return aliases.get(value.casefold(), value.upper() if len(value) == 2 else value.casefold().title())


def _safe_scalar(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)) and all(isinstance(item, (str, int, float, bool)) for item in value):
        return list(value)
    return None


def _first(record, *keys):
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return None


def _address_component(record, *types):
    components = record.get("address_components")
    if not isinstance(components, list):
        return None
    for component in components:
        if not isinstance(component, dict):
            continue
        component_types = component.get("types")
        if not isinstance(component_types, list):
            continue
        if any(component_type in types for component_type in component_types):
            return _first(component, "long_name", "short_name", "name")
    return None


def _extract_allowlisted(record):
    if not isinstance(record, Mapping):
        return {}
    geometry = record.get("geometry")
    location = geometry.get("location") if isinstance(geometry, Mapping) else record.get("location")
    if not isinstance(location, Mapping):
        location = None
    extracted = {
        "source_record_id": _first(record, "source_record_id", "record_id", "place_id", "google_place_id", "placeId"),
        "source_url": _first(record, "source_url", "google_maps_url", "googleMapsUrl", "url"),
        "business_name": _first(record, "business_name", "name", "title", "businessName"),
        "business_category": _first(record, "business_category", "category", "categoryName", "primary_type", "industry"),
        "website_url": _first(record, "website_url", "website"),
        "domain": _first(record, "normalized_domain", "domain"),
        "public_business_email": _first(record, "public_business_email", "business_email", "email"),
        "public_business_phone": _first(
            record, "public_business_phone", "business_phone", "phone", "international_phone_number", "national_phone_number"
        ),
        "street_address": _first(record, "street_address", "address_line_1", "address", "formatted_address"),
        "city": _first(record, "city", "locality" , "town") or _address_component(record, "locality", "postal_town"),
        "region_state": _first(record, "region_state", "region", "state", "stateCode")
        or _address_component(record, "administrative_area_level_1"),
        "postal_code": _first(record, "postal_code", "zip", "zipcode", "postalCode") or _address_component(record, "postal_code"),
        "country": _first(record, "country", "countryCode") or _address_component(record, "country"),
        "latitude": _first(record, "latitude", "lat")
        if _first(record, "latitude", "lat") is not None
        else (location.get("lat") if isinstance(location, Mapping) else None),
        "longitude": _first(record, "longitude", "lng", "lon")
        if _first(record, "longitude", "lng", "lon") is not None
        else (location.get("lng") if isinstance(location, Mapping) else None),
        "google_place_id": _first(record, "google_place_id", "place_id", "placeId"),
        "booking_url": _first(record, "booking_url", "appointment_url", "reservation_url"),
        "business_status": _first(record, "business_status", "status", "businessStatus"),
        "rating": _first(record, "rating"),
        "review_count": _first(record, "review_count", "reviewsCount", "reviewCount", "user_rating_count"),
    }
    if extracted["business_category"] is None:
        types = record.get("types")
        if isinstance(types, list) and types:
            extracted["business_category"] = types[0]
    if extracted["business_status"] is None:
        if record.get("permanentlyClosed") is True:
            extracted["business_status"] = "CLOSED_PERMANENTLY"
        elif record.get("temporarilyClosed") is True:
            extracted["business_status"] = "CLOSED_TEMPORARILY"
    return {key: _safe_scalar(value) for key, value in extracted.items() if _safe_scalar(value) is not None}


def stable_source_payload_hash(raw_record, source=None):
    """Hash only the deterministic, public, allowlisted source snapshot."""
    extracted = _extract_allowlisted(raw_record)
    snapshot = {}
    for key in _PUBLIC_FIELDS:
        if key not in extracted:
            continue
        value = extracted[key]
        if key in {"source_url", "website_url", "booking_url"}:
            try:
                value = normalize_url(value)
            except ValueError:
                continue
        elif key == "public_business_email":
            try:
                from paldo_os_outbound import normalize_email

                value = normalize_email(value)
            except (ImportError, ValueError):
                continue
        snapshot[key] = value
    if source in SUPPORTED_SOURCES:
        snapshot["source"] = source
    serialized = json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def normalize_url(value):
    """Return a safe canonical HTTP(S) URL without credentials or fragments."""
    if not isinstance(value, str):
        raise ValueError("URL must be text")
    raw = unicodedata.normalize("NFKC", value).strip()
    parsed = urlsplit(raw)
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.netloc:
        raise ValueError("URL must use http or https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("URL credentials are not allowed")
    try:
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as error:
        raise ValueError("URL host is invalid") from error
    if not hostname:
        raise ValueError("URL host is required")
    try:
        hostname = hostname.encode("idna").decode("ascii").casefold().rstrip(".")
    except UnicodeError as error:
        raise ValueError("URL host is invalid") from error
    if hostname.startswith("www."):
        hostname = hostname[4:]
    if not hostname:
        raise ValueError("URL host is required")
    default_port = (parsed.scheme.casefold() == "http" and port == 80) or (
        parsed.scheme.casefold() == "https" and port == 443
    )
    netloc = hostname if port is None or default_port else f"{hostname}:{port}"
    return urlunsplit((parsed.scheme.casefold(), netloc, parsed.path or "/", parsed.query, ""))


def _normalize_phone(value, country):
    if not isinstance(value, str):
        return None
    raw = value.strip()
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    explicit_country = raw.startswith("+")
    if explicit_country and digits.startswith("63") and len(digits) == 12:
        return "+" + digits
    if explicit_country and digits.startswith("1") and len(digits) == 11:
        return "+" + digits
    if country == "PH":
        if len(digits) == 11 and digits.startswith("0"):
            return "+63" + digits[1:]
        if len(digits) == 10 and digits.startswith("9"):
            return "+63" + digits
        if len(digits) == 12 and digits.startswith("63"):
            return "+" + digits
    if country == "US":
        if len(digits) == 10:
            return "+1" + digits
        if len(digits) == 11 and digits.startswith("1"):
            return "+" + digits
    return None


def _mode_value(mode):
    if isinstance(mode, DiscoveryMode):
        return mode.value
    if isinstance(mode, str) and mode in {item.value for item in DiscoveryMode}:
        return mode
    raise ValueError("mode must be DRY_RUN or LIVE")


def _provenance_type_value(value):
    if value is None:
        return "FIXTURE"
    value = value.value if isinstance(value, Enum) else value
    if value not in {"FIXTURE", "DRY_RUN", "LIVE"}:
        raise ValueError("provenance_type must be FIXTURE, DRY_RUN, or LIVE")
    return value


def _rejected_candidate(*, source, source_record_id, collected_at, payload_hash, mode, provenance_type, reason, observed=None):
    return NormalizedCandidate(
        source=source,
        source_record_id=source_record_id,
        source_url=None,
        collected_at=collected_at,
        business_name=None,
        business_category=None,
        website_url=None,
        normalized_domain=None,
        public_business_email=None,
        normalized_email=None,
        public_business_phone=None,
        normalized_phone=None,
        street_address=None,
        city=None,
        region_state=None,
        postal_code=None,
        country=None,
        latitude=None,
        longitude=None,
        google_place_id=None,
        booking_url=None,
        business_status=None,
        raw_payload_hash=payload_hash,
        ingestion_status=CandidateStatus.REJECTED.value,
        rejection_hold_reason=reason,
        provenance_mode=mode,
        provenance_type=provenance_type,
        observed_values=observed or {},
    )


def normalize_candidate(raw_record, *, source, collected_at=None, mode=DiscoveryMode.DRY_RUN, provenance_type="FIXTURE"):
    """Normalize a fictional/public source record without making network calls."""
    mode_value = _mode_value(mode)
    provenance_type = _provenance_type_value(provenance_type)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    collected_at = now if collected_at is None else _collapsed_text(collected_at)
    if not collected_at:
        collected_at = now
    extracted = _extract_allowlisted(raw_record)
    payload_hash = stable_source_payload_hash(raw_record, source)
    normalized_source = _collapsed_text(source)
    normalized_record_id = _collapsed_text(extracted.get("source_record_id"))
    observed = {
        key: value
        for key, value in extracted.items()
        if key in _PUBLIC_FIELDS and value is not None
    }
    if normalized_source not in SUPPORTED_SOURCES or not normalized_record_id:
        return _rejected_candidate(
            source=normalized_source,
            source_record_id=normalized_record_id,
            collected_at=collected_at,
            payload_hash=payload_hash,
            mode=mode_value,
            provenance_type=provenance_type,
            reason="missing or invalid source identity",
            observed=observed,
        )

    business_name = _collapsed_text(extracted.get("business_name"))
    if not business_name:
        return _rejected_candidate(
            source=normalized_source,
            source_record_id=normalized_record_id,
            collected_at=collected_at,
            payload_hash=payload_hash,
            mode=mode_value,
            provenance_type=provenance_type,
            reason="malformed record: business name is required",
            observed=observed,
        )

    errors = []
    normalized_source_url = None
    normalized_website = None
    normalized_booking = None
    for field_name, raw_value in (
        ("source_url", extracted.get("source_url")),
        ("website_url", extracted.get("website_url")),
        ("booking_url", extracted.get("booking_url")),
    ):
        if raw_value is None:
            continue
        try:
            value = normalize_url(raw_value)
        except ValueError:
            errors.append(f"invalid or unsafe {field_name}")
            observed.pop(field_name, None)
            continue
        if field_name == "source_url":
            normalized_source_url = value
        elif field_name == "website_url":
            normalized_website = value
        else:
            normalized_booking = value

    public_email = _collapsed_text(extracted.get("public_business_email"))
    normalized_email = None
    if public_email is not None:
        try:
            from paldo_os_outbound import normalize_email

            normalized_email = normalize_email(public_email)
        except (ImportError, ValueError):
            errors.append("invalid public business email")
            observed.pop("public_business_email", None)

    normalized_domain = None
    for domain_value in (normalized_website, extracted.get("domain"), normalized_email.rsplit("@", 1)[1] if normalized_email else None):
        if domain_value:
            try:
                from paldo_os_outbound import normalize_domain

                normalized_domain = normalize_domain(domain_value)
                break
            except (ImportError, ValueError):
                if domain_value == normalized_website:
                    errors.append("invalid website domain")

    if normalized_domain is None and extracted.get("domain") is not None:
        errors.append("invalid domain")
        observed.pop("domain", None)

    country = _normalized_country(extracted.get("country"))
    normalized_phone = _normalize_phone(extracted.get("public_business_phone"), country)
    public_phone = _collapsed_text(extracted.get("public_business_phone"))
    category = _collapsed_text(extracted.get("business_category"))
    street_address = _collapsed_text(extracted.get("street_address"))
    city = _normalized_label(extracted.get("city"))
    region_state = _normalized_label(extracted.get("region_state"))
    postal_code = _collapsed_text(extracted.get("postal_code"))
    google_place_id = _collapsed_text(extracted.get("google_place_id"))
    status = _collapsed_text(extracted.get("business_status"))
    status_value = status.upper().replace(" ", "_") if status else None
    if status_value in {"CLOSED_TEMPORARILY", "CLOSED_PERMANENTLY", "PERMANENTLY_CLOSED", "CLOSED_PERMANENT"}:
        errors.append("inactive or permanently closed business")
    if any(bool(raw_record.get(key)) for key in ("private_contact", "sensitive_contact", "contact_is_private", "private_or_sensitive_contact")) if isinstance(raw_record, Mapping) else False:
        errors.append("private or sensitive contact")
    flags = raw_record.get("contact_flags") if isinstance(raw_record, Mapping) else None
    if isinstance(flags, (list, tuple)) and any(str(flag).casefold() in {"private", "sensitive", "leaked"} for flag in flags):
        errors.append("private or sensitive contact")

    latitude = extracted.get("latitude")
    longitude = extracted.get("longitude")
    try:
        latitude = None if latitude is None else float(latitude)
        longitude = None if longitude is None else float(longitude)
        if latitude is not None and not -90 <= latitude <= 90:
            raise ValueError
        if longitude is not None and not -180 <= longitude <= 180:
            raise ValueError
    except (TypeError, ValueError):
        errors.append("malformed coordinates")
        latitude = longitude = None

    reason = "; ".join(dict.fromkeys(errors)) or None
    return NormalizedCandidate(
        source=normalized_source,
        source_record_id=normalized_record_id,
        source_url=normalized_source_url,
        collected_at=collected_at,
        business_name=business_name,
        business_category=category,
        website_url=normalized_website,
        normalized_domain=normalized_domain,
        public_business_email=public_email,
        normalized_email=normalized_email,
        public_business_phone=public_phone,
        normalized_phone=normalized_phone,
        street_address=street_address,
        city=city,
        region_state=region_state,
        postal_code=postal_code,
        country=country,
        latitude=latitude,
        longitude=longitude,
        google_place_id=google_place_id,
        booking_url=normalized_booking,
        business_status=status_value,
        raw_payload_hash=payload_hash,
        ingestion_status=CandidateStatus.REJECTED.value if reason else CandidateStatus.PENDING.value,
        rejection_hold_reason=reason,
        provenance_mode=mode_value,
        provenance_type=provenance_type,
        observed_values=observed,
    )


class GooglePlacesAdapter:
    """Future Google Places seam; currently accepts only supplied fixtures."""

    #: Placeholder hubs for the pilot region; replace with the hubs you target.
    FUTURE_HUBS = ("Westport", "Riverton", "Fairview", "Harbor area")

    def normalize_records(self, records: Iterable[Mapping[str, Any]], *, mode=DiscoveryMode.DRY_RUN, collected_at=None):
        mode_value = _mode_value(mode)
        if mode_value == DiscoveryMode.LIVE.value:
            raise LiveModeBlockedError("LIVE Google Places collection is disabled in Step 3; no network call made")
        return [
            normalize_candidate(record, source=GOOGLE_PLACES_SOURCE, collected_at=collected_at, mode=mode_value, provenance_type="FIXTURE")
            for record in records
        ]


class ApifyAdapter:
    """Replaceable Apify seam using an injected collector/actor interface."""

    def __init__(self, collector: Optional[Callable[..., Iterable[Mapping[str, Any]]]] = None):
        self.collector = collector

    def normalize_records(self, records: Iterable[Mapping[str, Any]], *, mode=DiscoveryMode.DRY_RUN, collected_at=None):
        mode_value = _mode_value(mode)
        if mode_value == DiscoveryMode.LIVE.value:
            raise LiveModeBlockedError("LIVE Apify collection is disabled in Step 3; no network call made")
        return [
            normalize_candidate(record, source=APIFY_SOURCE, collected_at=collected_at, mode=mode_value, provenance_type="FIXTURE")
            for record in records
        ]

    def collect_records(self, *, fixture_records=None, mode=DiscoveryMode.DRY_RUN, **collector_kwargs):
        mode_value = _mode_value(mode)
        if mode_value == DiscoveryMode.LIVE.value:
            raise LiveModeBlockedError("LIVE Apify collection is disabled in Step 3; no network call made")
        if fixture_records is not None:
            return list(fixture_records)
        if self.collector is None:
            return []
        return list(self.collector(**collector_kwargs))


_STEP3_SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_candidates (
    id INTEGER PRIMARY KEY,
    source TEXT,
    source_record_id TEXT,
    source_url TEXT,
    collected_at TEXT NOT NULL,
    business_name TEXT,
    business_category TEXT,
    website_url TEXT,
    normalized_domain TEXT,
    public_business_email TEXT,
    normalized_email TEXT,
    public_business_phone TEXT,
    normalized_phone TEXT,
    street_address TEXT,
    city TEXT,
    region_state TEXT,
    postal_code TEXT,
    country TEXT,
    latitude REAL,
    longitude REAL,
    google_place_id TEXT,
    booking_url TEXT,
    business_status TEXT,
    raw_payload_hash TEXT NOT NULL,
    ingestion_status TEXT NOT NULL CHECK (ingestion_status IN ('PENDING', 'ACCEPTED', 'REJECTED', 'HELD', 'DUPLICATE')),
    ingestion_decision TEXT NOT NULL CHECK (ingestion_decision IN ('PENDING', 'ACCEPT', 'REJECT', 'HOLD', 'DUPLICATE')),
    rejection_hold_reason TEXT,
    provenance_mode TEXT NOT NULL CHECK (provenance_mode IN ('DRY_RUN', 'LIVE')),
    provenance_type TEXT NOT NULL CHECK (provenance_type IN ('FIXTURE', 'DRY_RUN', 'LIVE')),
    observed_values TEXT NOT NULL,
    lead_id INTEGER REFERENCES leads(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS discovery_candidates_source_record_unique
    ON discovery_candidates(source, source_record_id)
    WHERE source IS NOT NULL AND source_record_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS discovery_candidates_email_idx ON discovery_candidates(normalized_email);
CREATE INDEX IF NOT EXISTS discovery_candidates_domain_idx ON discovery_candidates(normalized_domain);
CREATE INDEX IF NOT EXISTS discovery_candidates_google_place_idx ON discovery_candidates(google_place_id);
CREATE INDEX IF NOT EXISTS discovery_candidates_phone_idx ON discovery_candidates(normalized_phone);
"""


def _utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def migrate_step3(database_or_path):
    """Apply the additive, transactional Step 3 migration exactly once."""
    if hasattr(database_or_path, "connection"):
        database = database_or_path
        close_after = False
    else:
        from paldo_os_outbound import Database

        database = Database(Path(database_or_path))
        close_after = True
    try:
        if hasattr(database, "migrate_step2"):
            database.migrate_step2()
        timestamp = _utc_now()
        with database.connection:
            database.connection.executescript(_STEP3_SCHEMA)
            database.connection.executemany(
                """INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
                   VALUES (?, ?, ?)""",
                ((STEP3_MIGRATION_VERSION, "step3_candidate_discovery_and_ingestion", timestamp),),
            )
        return STEP3_MIGRATION_VERSION
    finally:
        if close_after:
            database.close()


def _step3_ready(database):
    return database.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'discovery_candidates'"
    ).fetchone() is not None


def _is_durable_database(database):
    try:
        from paldo_os_outbound import DEFAULT_DB_PATH

        return Path(database.path).resolve() == Path(DEFAULT_DB_PATH).resolve()
    except (AttributeError, ImportError, OSError):
        return False


def _safe_reason(candidate):
    return candidate.rejection_hold_reason


def _candidate_values(candidate, status, decision, reason, lead_id, timestamp):
    return (
        candidate.source,
        candidate.source_record_id,
        candidate.source_url,
        candidate.collected_at,
        candidate.business_name,
        candidate.business_category,
        candidate.website_url,
        candidate.normalized_domain,
        candidate.public_business_email,
        candidate.normalized_email,
        candidate.public_business_phone,
        candidate.normalized_phone,
        candidate.street_address,
        candidate.city,
        candidate.region_state,
        candidate.postal_code,
        candidate.country,
        candidate.latitude,
        candidate.longitude,
        candidate.google_place_id,
        candidate.booking_url,
        candidate.business_status,
        candidate.raw_payload_hash,
        status,
        decision,
        reason,
        candidate.provenance_mode,
        candidate.provenance_type,
        json.dumps(dict(candidate.observed_values), ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        lead_id,
        timestamp,
        timestamp,
    )


def _result_from_row(row, *, idempotent=False, event_id=None):
    return IngestionResult(
        candidate_id=row["id"],
        status=row["ingestion_status"],
        reason=row["rejection_hold_reason"],
        lead_id=row["lead_id"],
        event_id=event_id,
        idempotent=idempotent,
    )


def _same_identity_text(left, right):
    left_text = _collapsed_text(left)
    right_text = _collapsed_text(right)
    if not left_text or not right_text:
        return False
    left_key = "".join(
        character
        for character in unicodedata.normalize("NFKD", left_text).casefold()
        if not unicodedata.combining(character)
    )
    right_key = "".join(
        character
        for character in unicodedata.normalize("NFKD", right_text).casefold()
        if not unicodedata.combining(character)
    )
    return left_key == right_key


def _find_possible_duplicate(database, candidate):
    if candidate.normalized_phone:
        row = database.connection.execute(
            """SELECT id FROM discovery_candidates
               WHERE normalized_phone = ? LIMIT 1""",
            (candidate.normalized_phone,),
        ).fetchone()
        if row is not None:
            return True
        row = database.connection.execute(
            "SELECT id FROM leads WHERE phone = ? LIMIT 1", (candidate.normalized_phone,)
        ).fetchone()
        if row is not None:
            return True
    if candidate.business_name and candidate.street_address:
        rows = database.connection.execute(
            """SELECT business_name, street_address FROM discovery_candidates
               WHERE business_name IS NOT NULL AND street_address IS NOT NULL"""
        ).fetchall()
        if any(
            _same_identity_text(candidate.business_name, row["business_name"])
            and _same_identity_text(candidate.street_address, row["street_address"])
            for row in rows
        ):
            return True
        rows = database.connection.execute(
            "SELECT business_name, location FROM leads WHERE business_name IS NOT NULL AND location IS NOT NULL"
        ).fetchall()
        if any(
            _same_identity_text(candidate.business_name, row["business_name"])
            and _same_identity_text(candidate.street_address, row["location"])
            for row in rows
        ):
            return True
    return False


def _find_strong_duplicate(database, candidate):
    if candidate.normalized_email or candidate.normalized_domain:
        candidate_clauses = []
        candidate_params = []
        lead_clauses = []
        lead_params = []
        if candidate.normalized_email:
            candidate_clauses.append("normalized_email = ?")
            candidate_params.append(candidate.normalized_email)
            lead_clauses.append("email = ?")
            lead_params.append(candidate.normalized_email)
        if candidate.normalized_domain:
            candidate_clauses.append("normalized_domain = ?")
            candidate_params.append(candidate.normalized_domain)
            lead_clauses.append("domain = ?")
            lead_params.append(candidate.normalized_domain)
        row = database.connection.execute(
            f"""SELECT id, ingestion_status, rejection_hold_reason, lead_id
                FROM discovery_candidates WHERE {' OR '.join(candidate_clauses)} ORDER BY id LIMIT 1""",
            candidate_params,
        ).fetchone()
        if row is not None:
            return row
        row = database.connection.execute(
            f"SELECT id FROM leads WHERE {' OR '.join(lead_clauses)} ORDER BY id LIMIT 1",
            lead_params,
        ).fetchone()
        if row is not None:
            return row
    return None


def _insert_candidate_only(database, candidate, status, decision, reason, timestamp):
    cursor = database.connection.execute(
        """INSERT INTO discovery_candidates
           (source, source_record_id, source_url, collected_at, business_name, business_category,
            website_url, normalized_domain, public_business_email, normalized_email,
            public_business_phone, normalized_phone, street_address, city, region_state,
            postal_code, country, latitude, longitude, google_place_id, booking_url,
            business_status, raw_payload_hash, ingestion_status, ingestion_decision,
            rejection_hold_reason, provenance_mode, provenance_type, observed_values,
            lead_id, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        _candidate_values(candidate, status, decision, reason, None, timestamp),
    )
    return cursor.lastrowid


def _lead_location(candidate):
    return ", ".join(
        value for value in (
            candidate.street_address,
            candidate.city,
            candidate.region_state,
            candidate.postal_code,
        ) if value
    ) or None


def _safe_event_metadata(candidate, status):
    return {
        "collected_at": candidate.collected_at,
        "ingestion_status": status,
        "provenance_mode": candidate.provenance_mode,
        "provenance_type": candidate.provenance_type,
        "raw_payload_hash": candidate.raw_payload_hash,
        "source": candidate.source,
        "source_record_id": candidate.source_record_id,
        "source_url": candidate.source_url,
    }


def ingest_candidate(database, candidate: NormalizedCandidate):
    """Persist one candidate transactionally, optionally creating a DISCOVERED lead."""
    if not _step3_ready(database):
        raise RuntimeError("Step 3 migration has not been applied")
    if not isinstance(candidate, NormalizedCandidate):
        raise TypeError("candidate must be a NormalizedCandidate")
    if _is_durable_database(database) and candidate.provenance_type in {"FIXTURE", "DRY_RUN"}:
        raise LiveModeBlockedError("DRY_RUN fixture ingestion is disabled for the durable database")
    with database.connection:
        existing = None
        if candidate.source and candidate.source_record_id:
            existing = database.connection.execute(
                "SELECT * FROM discovery_candidates WHERE source = ? AND source_record_id = ?",
                (candidate.source, candidate.source_record_id),
            ).fetchone()
        if existing is not None:
            return _result_from_row(existing, idempotent=True)

        status = candidate.ingestion_status
        reason = _safe_reason(candidate)
        decision = "PENDING"
        if status == CandidateStatus.PENDING.value:
            if candidate.normalized_email or candidate.normalized_domain:
                if (candidate.normalized_email and database.is_suppressed(email=candidate.normalized_email)) or (
                    candidate.normalized_domain and database.is_suppressed(domain=candidate.normalized_domain)
                ):
                    status, decision, reason = CandidateStatus.REJECTED.value, "REJECT", "suppressed normalized email or domain"
                elif candidate.source == GOOGLE_PLACES_SOURCE and candidate.google_place_id and database.connection.execute(
                    """SELECT 1 FROM discovery_candidates
                       WHERE source = ? AND google_place_id = ? LIMIT 1""",
                    (GOOGLE_PLACES_SOURCE, candidate.google_place_id),
                ).fetchone() is not None:
                    status, decision, reason = CandidateStatus.DUPLICATE.value, "DUPLICATE", "repeat Google place ID"
                elif _find_strong_duplicate(database, candidate) is not None:
                    status, decision, reason = CandidateStatus.DUPLICATE.value, "DUPLICATE", "normalized email or domain already identifies a record"
                elif _find_possible_duplicate(database, candidate):
                    status, decision, reason = CandidateStatus.HELD.value, "HOLD", "possible duplicate based on phone or name and address"
                else:
                    status, decision, reason = CandidateStatus.ACCEPTED.value, "ACCEPT", None
            else:
                status, decision, reason = CandidateStatus.HELD.value, "HOLD", "missing normalized email and domain; identity not invented"
        elif status == CandidateStatus.REJECTED.value:
            decision = "REJECT"
        elif status == CandidateStatus.HELD.value:
            decision = "HOLD"
        elif status == CandidateStatus.DUPLICATE.value:
            decision = "DUPLICATE"
        elif status == CandidateStatus.ACCEPTED.value:
            decision = "ACCEPT"
        else:
            status, decision, reason = CandidateStatus.REJECTED.value, "REJECT", "unsupported ingestion status"

        timestamp = _utc_now()
        candidate_id = _insert_candidate_only(database, candidate, status, decision, reason, timestamp)
        lead_id = None
        if status == CandidateStatus.ACCEPTED.value:
            domain = candidate.normalized_domain or (
                candidate.normalized_email.rsplit("@", 1)[1] if candidate.normalized_email else None
            )
            cursor = database.connection.execute(
                """INSERT INTO leads
                   (business_name, website, domain, email, phone, industry, country, location,
                    source, source_url, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DISCOVERED', ?, ?)""",
                (
                    candidate.business_name,
                    candidate.website_url,
                    domain,
                    candidate.normalized_email,
                    candidate.normalized_phone or candidate.public_business_phone,
                    candidate.business_category,
                    candidate.country,
                    _lead_location(candidate),
                    candidate.source,
                    candidate.source_url,
                    timestamp,
                    timestamp,
                ),
            )
            lead_id = cursor.lastrowid
            database.connection.execute(
                "UPDATE discovery_candidates SET lead_id = ? WHERE id = ?", (lead_id, candidate_id)
            )
        event_metadata = _safe_event_metadata(candidate, status)
        event_cursor = database.connection.execute(
            """INSERT INTO events (event_type, entity_type, entity_id, metadata, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                "discovery_candidate_ingested",
                "discovery_candidate",
                str(candidate_id),
                json.dumps(event_metadata, sort_keys=True, separators=(",", ":")),
                timestamp,
            ),
        )
        return IngestionResult(candidate_id, status, reason, lead_id, event_cursor.lastrowid)


def check_live_guard(database, *, source, campaign_id, operator_explicitly_requested_live=False, env=None):
    """Return ``(allowed, safe_reason)`` without making a network call."""
    if source not in SUPPORTED_SOURCES:
        return False, "LIVE blocked: unsupported source"
    if database.get_config("system_state") == "PAUSED":
        return False, "LIVE blocked: system_state is PAUSED"
    campaign = database.get_campaign(campaign_id) if campaign_id is not None else None
    if campaign is None or campaign.get("status") != "ACTIVE":
        return False, "LIVE blocked: selected campaign must be ACTIVE"
    quota_key = "google_places_daily_request_cap" if source == GOOGLE_PLACES_SOURCE else "apify_daily_run_cap"
    quota = database.get_config(quota_key)
    if not isinstance(quota, int) or quota <= 0:
        return False, f"LIVE blocked: source quota {quota_key} must be positive"
    credential_key = "GOOGLE_PLACES_API_KEY" if source == GOOGLE_PLACES_SOURCE else "APIFY_API_TOKEN"
    environment = os.environ if env is None else env
    if not isinstance(environment, Mapping) or not environment.get(credential_key):
        return False, f"LIVE blocked: credential {credential_key} is missing"
    if operator_explicitly_requested_live is not True:
        return False, "LIVE blocked: explicit operator confirmation is required"
    return True, "LIVE prerequisites satisfied"


def run_discovery(
    database,
    adapter,
    *,
    raw_records=None,
    mode=None,
    campaign_id=None,
    operator_explicitly_requested_live=False,
    env=None,
):
    """Normalize and ingest supplied fixtures, or reject guarded LIVE mode."""
    mode_value = database.get_config("discovery_mode") if mode is None else _mode_value(mode)
    if mode_value == DiscoveryMode.LIVE.value:
        allowed, reason = check_live_guard(
            database,
            source=GOOGLE_PLACES_SOURCE if isinstance(adapter, GooglePlacesAdapter) else APIFY_SOURCE,
            campaign_id=campaign_id,
            operator_explicitly_requested_live=operator_explicitly_requested_live,
            env=env,
        )
        if not allowed:
            raise LiveModeBlockedError(reason)
        raise LiveModeBlockedError("LIVE collection is disabled in Step 3; no network call made")
    if _is_durable_database(database) and raw_records:
        raise LiveModeBlockedError("DRY_RUN fixture ingestion is disabled for the durable database")
    if raw_records is None:
        if _is_durable_database(database):
            return []
        collect_records = getattr(adapter, "collect_records", None)
        raw_records = collect_records(mode=DiscoveryMode.DRY_RUN.value) if collect_records else []
    candidates = adapter.normalize_records(raw_records or [], mode=DiscoveryMode.DRY_RUN.value)
    return [ingest_candidate(database, candidate) for candidate in candidates]


__all__ = [
    "APIFY_SOURCE",
    "GOOGLE_PLACES_SOURCE",
    "STEP3_MIGRATION_VERSION",
    "ApifyAdapter",
    "CandidateStatus",
    "DiscoveryMode",
    "GooglePlacesAdapter",
    "IngestionResult",
    "LiveModeBlockedError",
    "NormalizedCandidate",
    "check_live_guard",
    "ingest_candidate",
    "migrate_step3",
    "normalize_candidate",
    "normalize_url",
    "run_discovery",
    "stable_source_payload_hash",
]


# Step 3 refinement layer: preserve the v4 schema and legacy status values while
# adding explicit outcomes and durable source-run summaries in an additive v5
# migration. The public functions below intentionally remain standard-library
# only and network-free.
_migrate_step3_v4 = migrate_step3
STEP3_REFINEMENT_MIGRATION_VERSION = 5


class IngestionOutcome(str, Enum):
    ACCEPTED = "ACCEPTED"
    UPDATED = "UPDATED"
    DUPLICATE = "DUPLICATE"
    POSSIBLE_DUPLICATE_HOLD = "POSSIBLE_DUPLICATE_HOLD"
    SUPPRESSED = "SUPPRESSED"
    INVALID = "INVALID"
    INACTIVE_BUSINESS = "INACTIVE_BUSINESS"
    INSUFFICIENT_IDENTITY = "INSUFFICIENT_IDENTITY"
    SOURCE_ERROR = "SOURCE_ERROR"


@dataclass(frozen=True)
class IngestionResult:
    candidate_id: Optional[int]
    status: str
    reason: Optional[str]
    lead_id: Optional[int]
    event_id: Optional[int]
    idempotent: bool = False
    outcome: str = ""
    run_id: Optional[int] = None

    def __post_init__(self):
        if not self.outcome:
            object.__setattr__(self, "outcome", self.status)

    @property
    def legacy_status(self):
        return _legacy_status_for(self.status)

    def as_dict(self):
        values = asdict(self)
        values["legacy_status"] = self.legacy_status
        return values


_REFINEMENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS discovery_runs (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('DRY_RUN', 'LIVE')),
    provenance_type TEXT NOT NULL CHECK (provenance_type IN ('FIXTURE', 'DRY_RUN', 'LIVE')),
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'SOURCE_ERROR', 'BLOCKED')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    input_count INTEGER NOT NULL DEFAULT 0,
    accepted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    possible_duplicate_hold_count INTEGER NOT NULL DEFAULT 0,
    suppressed_count INTEGER NOT NULL DEFAULT 0,
    invalid_count INTEGER NOT NULL DEFAULT 0,
    inactive_business_count INTEGER NOT NULL DEFAULT 0,
    insufficient_identity_count INTEGER NOT NULL DEFAULT 0,
    source_error_count INTEGER NOT NULL DEFAULT 0,
    metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS discovery_runs_source_idx ON discovery_runs(source);
CREATE INDEX IF NOT EXISTS discovery_runs_started_idx ON discovery_runs(started_at);
"""

_REFINEMENT_OUTCOMES = tuple(outcome.value for outcome in IngestionOutcome)


def _table_columns(database, table_name):
    return {row[1] for row in database.connection.execute(f"PRAGMA table_info({table_name})")}


def migrate_step3(database_or_path):
    """Apply the original Step 3 migration and additive outcome/run migration."""
    if hasattr(database_or_path, "connection"):
        database = database_or_path
        close_after = False
    else:
        from paldo_os_outbound import Database

        database = Database(Path(database_or_path))
        close_after = True
    try:
        _migrate_step3_v4(database)
        with database.connection:
            database.connection.executescript(_REFINEMENT_SCHEMA)
            candidate_columns = _table_columns(database, "discovery_candidates")
            if "run_id" not in candidate_columns:
                database.connection.execute(
                    "ALTER TABLE discovery_candidates ADD COLUMN run_id INTEGER REFERENCES discovery_runs(id) ON DELETE SET NULL"
                )
            if "ingestion_outcome" not in candidate_columns:
                allowed = ", ".join(repr(value) for value in _REFINEMENT_OUTCOMES + ("PENDING",))
                database.connection.execute(
                    "ALTER TABLE discovery_candidates ADD COLUMN ingestion_outcome TEXT NOT NULL DEFAULT 'PENDING' CHECK (ingestion_outcome IN (" + allowed + "))"
                )
                database.connection.execute(
                    """UPDATE discovery_candidates
                       SET ingestion_outcome = CASE ingestion_status
                           WHEN 'ACCEPTED' THEN 'ACCEPTED'
                           WHEN 'DUPLICATE' THEN 'DUPLICATE'
                           WHEN 'HELD' THEN 'POSSIBLE_DUPLICATE_HOLD'
                           WHEN 'REJECTED' THEN 'INVALID'
                           ELSE 'INSUFFICIENT_IDENTITY'
                       END"""
                )
            database.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
                   VALUES (?, ?, ?)""",
                (STEP3_REFINEMENT_MIGRATION_VERSION, "step3_discovery_runs_and_named_ingestion_outcomes", _utc_now()),
            )
        return STEP3_REFINEMENT_MIGRATION_VERSION
    finally:
        if close_after:
            database.close()


def _legacy_status_for(outcome):
    outcome = outcome.value if isinstance(outcome, IngestionOutcome) else outcome
    if outcome in {IngestionOutcome.ACCEPTED.value, IngestionOutcome.UPDATED.value}:
        return CandidateStatus.ACCEPTED.value
    if outcome == IngestionOutcome.DUPLICATE.value:
        return CandidateStatus.DUPLICATE.value
    if outcome in {IngestionOutcome.POSSIBLE_DUPLICATE_HOLD.value, IngestionOutcome.INSUFFICIENT_IDENTITY.value}:
        return CandidateStatus.HELD.value
    return CandidateStatus.REJECTED.value


def _decision_for_outcome(outcome):
    outcome = outcome.value if isinstance(outcome, IngestionOutcome) else outcome
    if outcome in {IngestionOutcome.ACCEPTED.value, IngestionOutcome.UPDATED.value}:
        return "ACCEPT"
    if outcome == IngestionOutcome.DUPLICATE.value:
        return "DUPLICATE"
    if outcome in {IngestionOutcome.POSSIBLE_DUPLICATE_HOLD.value, IngestionOutcome.INSUFFICIENT_IDENTITY.value}:
        return "HOLD"
    return "REJECT"


def _safe_outcome_reason(candidate):
    reason = (candidate.rejection_hold_reason or "").casefold()
    if "closed" in reason or "inactive" in reason:
        return IngestionOutcome.INACTIVE_BUSINESS.value, "business is inactive or permanently closed"
    if candidate.ingestion_status == CandidateStatus.REJECTED.value:
        return IngestionOutcome.INVALID.value, candidate.rejection_hold_reason or "invalid candidate record"
    if candidate.ingestion_status == CandidateStatus.HELD.value:
        return IngestionOutcome.POSSIBLE_DUPLICATE_HOLD.value, candidate.rejection_hold_reason or "possible duplicate requires review"
    if not candidate.normalized_email and not candidate.normalized_domain:
        return IngestionOutcome.INSUFFICIENT_IDENTITY.value, "missing normalized email and domain; identity not invented"
    return None, None


def _candidate_is_suppressed(database, candidate):
    return bool(
        (candidate.normalized_email and database.is_suppressed(email=candidate.normalized_email))
        or (candidate.normalized_domain and database.is_suppressed(domain=candidate.normalized_domain))
    )


def _new_candidate_outcome(database, candidate):
    intrinsic_outcome, intrinsic_reason = _safe_outcome_reason(candidate)
    if intrinsic_outcome is not None:
        return intrinsic_outcome, intrinsic_reason
    if _candidate_is_suppressed(database, candidate):
        return IngestionOutcome.SUPPRESSED.value, "suppressed normalized email or domain"
    if candidate.source == GOOGLE_PLACES_SOURCE and candidate.google_place_id and database.connection.execute(
        "SELECT 1 FROM discovery_candidates WHERE source = ? AND google_place_id = ? LIMIT 1",
        (GOOGLE_PLACES_SOURCE, candidate.google_place_id),
    ).fetchone() is not None:
        return IngestionOutcome.DUPLICATE.value, "repeat Google place ID"
    if _find_strong_duplicate(database, candidate) is not None:
        return IngestionOutcome.DUPLICATE.value, "normalized email or domain already identifies a record"
    if _find_possible_duplicate(database, candidate):
        return IngestionOutcome.POSSIBLE_DUPLICATE_HOLD.value, "possible duplicate based on phone or name and address"
    return IngestionOutcome.ACCEPTED.value, None


def _run_source_for_adapter(adapter):
    source = getattr(adapter, "source", None)
    if source in SUPPORTED_SOURCES:
        return source
    return GOOGLE_PLACES_SOURCE if isinstance(adapter, GooglePlacesAdapter) else APIFY_SOURCE


def _create_discovery_run(database, *, source, mode, provenance_type):
    timestamp = _utc_now()
    cursor = database.connection.execute(
        """INSERT INTO discovery_runs (source, mode, provenance_type, status, started_at, metadata)
           VALUES (?, ?, ?, 'RUNNING', ?, ?)""",
        (source, mode, provenance_type, timestamp, json.dumps({"source": source, "mode": mode, "provenance_type": provenance_type}, sort_keys=True)),
    )
    return cursor.lastrowid


def _finish_discovery_run(database, run_id, results, *, status="COMPLETED", input_count=None):
    counts = {outcome.value: 0 for outcome in IngestionOutcome}
    for result in results:
        counts[result.outcome] = counts.get(result.outcome, 0) + 1
    with database.connection:
        database.connection.execute(
            """UPDATE discovery_runs SET status = ?, completed_at = ?, input_count = ?,
               accepted_count = ?, updated_count = ?, duplicate_count = ?,
               possible_duplicate_hold_count = ?, suppressed_count = ?, invalid_count = ?,
               inactive_business_count = ?, insufficient_identity_count = ?, source_error_count = ?
               WHERE id = ?""",
            (
                status, _utc_now(), len(results) if input_count is None else input_count,
                counts.get(IngestionOutcome.ACCEPTED.value, 0), counts.get(IngestionOutcome.UPDATED.value, 0),
                counts.get(IngestionOutcome.DUPLICATE.value, 0), counts.get(IngestionOutcome.POSSIBLE_DUPLICATE_HOLD.value, 0),
                counts.get(IngestionOutcome.SUPPRESSED.value, 0), counts.get(IngestionOutcome.INVALID.value, 0),
                counts.get(IngestionOutcome.INACTIVE_BUSINESS.value, 0), counts.get(IngestionOutcome.INSUFFICIENT_IDENTITY.value, 0),
                counts.get(IngestionOutcome.SOURCE_ERROR.value, 0), run_id,
            ),
        )


def _event_for_result(database, *, candidate_id, run_id, candidate, outcome, reason, event_type="discovery_candidate_ingested"):
    if candidate is not None:
        # Keep routine candidate events on the established allowlisted shape;
        # the authoritative named outcome lives in discovery_candidates and the
        # aggregate discovery_runs summary.
        metadata = _safe_event_metadata(candidate, _legacy_status_for(outcome))
    else:
        metadata = {
            "run_id": run_id,
            "ingestion_outcome": outcome,
            "reason": reason,
        }
    cursor = database.connection.execute(
        """INSERT INTO events (event_type, entity_type, entity_id, metadata, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            event_type, "discovery_candidate" if candidate_id is not None else "discovery_run",
            str(candidate_id or run_id), json.dumps(metadata, sort_keys=True, separators=(",", ":")), _utc_now(),
        ),
    )
    return cursor.lastrowid


def _insert_refined_candidate(database, candidate, *, outcome, reason, run_id, timestamp):
    status = _legacy_status_for(outcome)
    decision = _decision_for_outcome(outcome)
    cursor = database.connection.execute(
        """INSERT INTO discovery_candidates
           (source, source_record_id, source_url, collected_at, business_name, business_category,
            website_url, normalized_domain, public_business_email, normalized_email,
            public_business_phone, normalized_phone, street_address, city, region_state,
            postal_code, country, latitude, longitude, google_place_id, booking_url,
            business_status, raw_payload_hash, ingestion_status, ingestion_decision,
            rejection_hold_reason, provenance_mode, provenance_type, observed_values,
            lead_id, created_at, updated_at, run_id, ingestion_outcome)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            candidate.source, candidate.source_record_id, candidate.source_url, candidate.collected_at,
            candidate.business_name, candidate.business_category, candidate.website_url,
            candidate.normalized_domain, candidate.public_business_email, candidate.normalized_email,
            candidate.public_business_phone, candidate.normalized_phone, candidate.street_address,
            candidate.city, candidate.region_state, candidate.postal_code, candidate.country,
            candidate.latitude, candidate.longitude, candidate.google_place_id, candidate.booking_url,
            candidate.business_status, candidate.raw_payload_hash, status, decision, reason,
            candidate.provenance_mode, candidate.provenance_type,
            json.dumps(dict(candidate.observed_values), ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            None, timestamp, timestamp, run_id, outcome,
        ),
    )
    return cursor.lastrowid


def _parsed_timestamp(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError):
        return datetime.min.replace(tzinfo=timezone.utc)


def _evidence_strength(candidate):
    fields = (
        "business_name", "business_category", "website_url", "normalized_domain",
        "normalized_email", "normalized_phone", "street_address", "city", "region_state",
        "postal_code", "country", "latitude", "longitude", "google_place_id", "booking_url",
        "business_status",
    )
    return sum(getattr(candidate, field) is not None for field in fields)


def _stored_evidence_strength(row):
    fields = (
        "business_name", "business_category", "website_url", "normalized_domain",
        "normalized_email", "normalized_phone", "street_address", "city", "region_state",
        "postal_code", "country", "latitude", "longitude", "google_place_id", "booking_url",
        "business_status",
    )
    return sum(row[field] is not None for field in fields)


def _update_existing_candidate(database, existing, candidate, *, run_id, timestamp):
    """Apply newer evidence while preserving existing non-empty values."""
    fields = (
        "source_url", "collected_at", "business_name", "business_category", "website_url",
        "normalized_domain", "public_business_email", "normalized_email", "public_business_phone",
        "normalized_phone", "street_address", "city", "region_state", "postal_code", "country",
        "latitude", "longitude", "google_place_id", "booking_url", "business_status",
    )
    values = {field: getattr(candidate, field) for field in fields}
    for field in fields:
        if values[field] is None and existing[field] is not None:
            values[field] = existing[field]
    old_observed = json.loads(existing["observed_values"] or "{}")
    merged_observed = dict(old_observed)
    merged_observed.update(dict(candidate.observed_values))
    with database.connection:
        database.connection.execute(
            """UPDATE discovery_candidates SET source_url=?, collected_at=?, business_name=?,
               business_category=?, website_url=?, normalized_domain=?, public_business_email=?,
               normalized_email=?, public_business_phone=?, normalized_phone=?, street_address=?,
               city=?, region_state=?, postal_code=?, country=?, latitude=?, longitude=?,
               google_place_id=?, booking_url=?, business_status=?, raw_payload_hash=?,
               observed_values=?, ingestion_status='ACCEPTED', ingestion_decision='ACCEPT',
               rejection_hold_reason=NULL, lead_id=?, updated_at=?, run_id=?, ingestion_outcome='UPDATED'
               WHERE id=?""",
            (
                *(values[field] for field in fields), candidate.raw_payload_hash,
                json.dumps(merged_observed, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                existing["lead_id"], timestamp, run_id, existing["id"],
            ),
        )
        if existing["lead_id"] is not None:
            domain = values["normalized_domain"] or (values["normalized_email"].rsplit("@", 1)[1] if values["normalized_email"] else None)
            database.connection.execute(
                """UPDATE leads SET website=?, domain=?, email=?, phone=?, industry=?, country=?,
                   location=?, source=?, source_url=?, updated_at=? WHERE id=?""",
                (
                    values["website_url"], domain, values["normalized_email"],
                    values["normalized_phone"] or values["public_business_phone"], values["business_category"],
                    values["country"], ", ".join(value for value in (values["street_address"], values["city"], values["region_state"], values["postal_code"]) if value) or None,
                    candidate.source, values["source_url"], timestamp, existing["lead_id"],
                ),
            )
    return existing["id"], existing["lead_id"]


def _mark_existing_suppressed(database, existing, *, run_id, reason, timestamp):
    with database.connection:
        database.connection.execute(
            """UPDATE discovery_candidates SET ingestion_status='REJECTED', ingestion_decision='REJECT',
               rejection_hold_reason=?, ingestion_outcome='SUPPRESSED', updated_at=?, run_id=? WHERE id=?""",
            (reason, timestamp, run_id, existing["id"]),
        )
    return existing["id"], existing["lead_id"]


def ingest_candidate(database, candidate: NormalizedCandidate, *, run_id=None):
    """Persist one normalized candidate with suppression-first named outcomes."""
    if not _step3_ready(database):
        raise RuntimeError("Step 3 migration has not been applied")
    if not isinstance(candidate, NormalizedCandidate):
        raise TypeError("candidate must be a NormalizedCandidate")
    if (candidate.provenance_mode == DiscoveryMode.LIVE.value or candidate.provenance_type == "LIVE") and not getattr(database, "_step4_live_ingestion_authorized", False):
        raise LiveModeBlockedError("LIVE candidate ingestion is disabled without Step 4 authorization; no network call made")
    if _is_durable_database(database) and candidate.provenance_type in {"FIXTURE", "DRY_RUN"}:
        raise LiveModeBlockedError("DRY_RUN fixture ingestion is disabled for the durable database")
    own_run = run_id is None
    with database.connection:
        if own_run:
            run_id = _create_discovery_run(
                database, source=candidate.source or "UNKNOWN", mode=candidate.provenance_mode,
                provenance_type=candidate.provenance_type,
            )
        existing = None
        if candidate.source and candidate.source_record_id:
            existing = database.connection.execute(
                "SELECT * FROM discovery_candidates WHERE source=? AND source_record_id=?",
                (candidate.source, candidate.source_record_id),
            ).fetchone()
        timestamp = _utc_now()
        if _candidate_is_suppressed(database, candidate):
            reason = "suppressed normalized email or domain"
            if existing is not None:
                candidate_id, lead_id = _mark_existing_suppressed(database, existing, run_id=run_id, reason=reason, timestamp=timestamp)
                idempotent = False
            else:
                candidate_id = _insert_refined_candidate(database, candidate, outcome=IngestionOutcome.SUPPRESSED.value, reason=reason, run_id=run_id, timestamp=timestamp)
                lead_id = None
                idempotent = False
            event_id = _event_for_result(database, candidate_id=candidate_id, run_id=run_id, candidate=candidate, outcome=IngestionOutcome.SUPPRESSED.value, reason=reason)
            result = IngestionResult(candidate_id, IngestionOutcome.SUPPRESSED.value, reason, lead_id, event_id, idempotent, IngestionOutcome.SUPPRESSED.value, run_id)
        elif existing is not None:
            if (
                _parsed_timestamp(candidate.collected_at) > _parsed_timestamp(existing["collected_at"])
                and _evidence_strength(candidate) >= _stored_evidence_strength(existing)
            ):
                intrinsic_outcome, intrinsic_reason = _safe_outcome_reason(candidate)
                if intrinsic_outcome is not None:
                    outcome, reason = intrinsic_outcome, intrinsic_reason
                    candidate_id, lead_id, idempotent = existing["id"], existing["lead_id"], False
                else:
                    candidate_id, lead_id = _update_existing_candidate(database, existing, candidate, run_id=run_id, timestamp=timestamp)
                    outcome, reason, idempotent = IngestionOutcome.UPDATED.value, "newer source evidence applied; existing non-empty values preserved", False
                event_id = _event_for_result(database, candidate_id=candidate_id, run_id=run_id, candidate=candidate, outcome=outcome, reason=reason)
                result = IngestionResult(candidate_id, outcome, reason, lead_id, event_id, idempotent, outcome, run_id)
            else:
                outcome = IngestionOutcome.DUPLICATE.value
                reason = "same source record already imported; older or weaker evidence ignored"
                # Do not emit a second candidate event for an idempotent repeat.
                result = IngestionResult(existing["id"], outcome, reason, existing["lead_id"], None, True, outcome, run_id)
        else:
            outcome, reason = _new_candidate_outcome(database, candidate)
            candidate_id = _insert_refined_candidate(database, candidate, outcome=outcome, reason=reason, run_id=run_id, timestamp=timestamp)
            lead_id = None
            if outcome == IngestionOutcome.ACCEPTED.value:
                domain = candidate.normalized_domain or (candidate.normalized_email.rsplit("@", 1)[1] if candidate.normalized_email else None)
                cursor = database.connection.execute(
                    """INSERT INTO leads
                       (business_name, website, domain, email, phone, industry, country, location,
                        source, source_url, status, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DISCOVERED', ?, ?)""",
                    (
                        candidate.business_name, candidate.website_url, domain, candidate.normalized_email,
                        candidate.normalized_phone or candidate.public_business_phone, candidate.business_category,
                        candidate.country, _lead_location(candidate), candidate.source, candidate.source_url,
                        timestamp, timestamp,
                    ),
                )
                lead_id = cursor.lastrowid
                database.connection.execute("UPDATE discovery_candidates SET lead_id=? WHERE id=?", (lead_id, candidate_id))
            event_id = _event_for_result(database, candidate_id=candidate_id, run_id=run_id, candidate=candidate, outcome=outcome, reason=reason)
            result = IngestionResult(candidate_id, outcome, reason, lead_id, event_id, False, outcome, run_id)
        if own_run:
            _finish_discovery_run(database, run_id, [result])
        return result


def _safe_source_error_result(database, *, run_id, reason="source collection failed"):
    event_id = _event_for_result(database, candidate_id=None, run_id=run_id, candidate=None, outcome=IngestionOutcome.SOURCE_ERROR.value, reason=reason, event_type="discovery_source_error")
    result = IngestionResult(None, IngestionOutcome.SOURCE_ERROR.value, reason, None, event_id, False, IngestionOutcome.SOURCE_ERROR.value, run_id)
    _finish_discovery_run(database, run_id, [result], status="SOURCE_ERROR", input_count=0)
    return result


def run_discovery(database, adapter, *, raw_records=None, mode=None, campaign_id=None, operator_explicitly_requested_live=False, env=None):
    """Run a fixture-only discovery pass or stop safely at the LIVE guard."""
    mode_value = database.get_config("discovery_mode") if mode is None else _mode_value(mode)
    source = _run_source_for_adapter(adapter)
    if mode_value == DiscoveryMode.LIVE.value:
        allowed, reason = check_live_guard(
            database, source=source, campaign_id=campaign_id,
            operator_explicitly_requested_live=operator_explicitly_requested_live, env=env,
        )
        if not allowed:
            raise LiveModeBlockedError(reason)
        raise LiveModeBlockedError("LIVE collection is disabled in Step 3; no network call made")
    if _is_durable_database(database) and raw_records:
        raise LiveModeBlockedError("DRY_RUN fixture ingestion is disabled for the durable database")
    if raw_records is None:
        if _is_durable_database(database):
            return []
        collector = getattr(adapter, "collect_records", None)
        try:
            raw_records = collector(mode=DiscoveryMode.DRY_RUN.value) if collector else []
        except Exception:
            with database.connection:
                run_id = _create_discovery_run(database, source=source, mode=DiscoveryMode.DRY_RUN.value, provenance_type="FIXTURE")
                return [_safe_source_error_result(database, run_id=run_id)]
    with database.connection:
        run_id = _create_discovery_run(database, source=source, mode=DiscoveryMode.DRY_RUN.value, provenance_type="FIXTURE")
    try:
        candidates = adapter.normalize_records(raw_records or [], mode=DiscoveryMode.DRY_RUN.value)
    except Exception:
        with database.connection:
            return [_safe_source_error_result(database, run_id=run_id)]
    results = [ingest_candidate(database, candidate, run_id=run_id) for candidate in candidates]
    _finish_discovery_run(database, run_id, results, input_count=len(candidates))
    return results


def show_discovery_status(database):
    """Return safe operational status without contact details or raw payloads."""
    config = database.read_config()
    counts = {}
    for table in ("discovery_runs", "discovery_candidates", "leads", "outreach"):
        counts[table] = database.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] if _table_exists(database, table) else 0
    return {
        "mode": config["discovery_mode"], "system_state": config["system_state"],
        "google_places_daily_request_cap": config["google_places_daily_request_cap"],
        "apify_daily_run_cap": config["apify_daily_run_cap"],
        "campaigns": [{"name": row["name"], "status": row["status"]} for row in database.get_campaigns()],
        "counts": counts,
    }


def _table_exists(database, table_name):
    return database.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    ).fetchone() is not None


def validate_fixture_input(raw_records, *, source):
    """Validate fixtures without persistence and without returning contact data."""
    candidates = [normalize_candidate(record, source=source, mode=DiscoveryMode.DRY_RUN.value, provenance_type="FIXTURE") for record in raw_records]
    invalid = [candidate for candidate in candidates if candidate.ingestion_status == CandidateStatus.REJECTED.value]
    return {
        "valid": not invalid, "mode": DiscoveryMode.DRY_RUN.value, "provenance_type": "FIXTURE",
        "source": source, "record_count": len(candidates), "invalid_count": len(invalid),
        "records": [{"source_record_id": candidate.source_record_id, "status": candidate.ingestion_status, "reason": candidate.rejection_hold_reason} for candidate in candidates],
    }


def _preview_outcome(database, candidate):
    if _candidate_is_suppressed(database, candidate):
        return IngestionOutcome.SUPPRESSED.value, "suppressed normalized email or domain"
    intrinsic_outcome, intrinsic_reason = _safe_outcome_reason(candidate)
    if intrinsic_outcome is not None:
        return intrinsic_outcome, intrinsic_reason
    return _new_candidate_outcome(database, candidate)


def preview_dry_run_ingestion(database, adapter, *, raw_records=None):
    """Preview decisions without writing runs, candidates, leads, events, or scores."""
    if raw_records is None:
        collector = getattr(adapter, "collect_records", None)
        try:
            raw_records = collector(mode=DiscoveryMode.DRY_RUN.value) if collector else []
        except Exception:
            return {"mode": "DRY_RUN", "provenance_type": "FIXTURE", "source_error": True, "reason": "source collection failed", "outcomes": {}}
    try:
        candidates = adapter.normalize_records(raw_records or [], mode=DiscoveryMode.DRY_RUN.value)
    except Exception:
        return {"mode": "DRY_RUN", "provenance_type": "FIXTURE", "source_error": True, "reason": "source normalization failed", "outcomes": {}}
    outcomes = {}
    records = []
    for candidate in candidates:
        outcome, reason = _preview_outcome(database, candidate)
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
        records.append({"source_record_id": candidate.source_record_id, "outcome": outcome, "reason": reason})
    return {"mode": "DRY_RUN", "provenance_type": "FIXTURE", "record_count": len(candidates), "outcomes": outcomes, "records": records}


def import_fictional_fixtures(database, adapter, *, raw_records):
    """Explicitly named mutating command for DRY_RUN fictional fixtures only."""
    return run_discovery(database, adapter, raw_records=raw_records, mode=DiscoveryMode.DRY_RUN.value)


def list_run_summary(database, *, limit=50):
    """List safe run summaries without source-contact details or raw payloads."""
    rows = database.connection.execute(
        """SELECT id, source, mode, provenance_type, status, started_at, completed_at,
           input_count, accepted_count, updated_count, duplicate_count,
           possible_duplicate_hold_count, suppressed_count, invalid_count,
           inactive_business_count, insufficient_identity_count, source_error_count
           FROM discovery_runs ORDER BY id LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


__all__ = [
    "APIFY_SOURCE", "GOOGLE_PLACES_SOURCE", "STEP3_MIGRATION_VERSION", "STEP3_REFINEMENT_MIGRATION_VERSION",
    "ApifyAdapter", "CandidateStatus", "DiscoveryMode", "GooglePlacesAdapter", "IngestionOutcome", "IngestionResult",
    "LiveModeBlockedError", "NormalizedCandidate", "check_live_guard", "import_fictional_fixtures", "ingest_candidate",
    "list_run_summary", "migrate_step3", "normalize_candidate", "normalize_url", "preview_dry_run_ingestion",
    "run_discovery", "show_discovery_status", "stable_source_payload_hash", "validate_fixture_input",
]


# Step 4: live-ready Google Places API (New) client. The client is deliberately
# transport-injected for tests and remains unreachable through the durable
# default safety state until every explicit live gate passes.
from dataclasses import dataclass as _dataclass
import time as _time
import urllib.error as _urllib_error
import urllib.request as _urllib_request


STEP4_MIGRATION_VERSION = 6
GOOGLE_PLACES_ENDPOINT = "https://places.googleapis.com/v1/places:searchText"
GOOGLE_QUERY_PLAN_CAMPAIGN = "Primary region local services"


class GooglePlacesCostProfile(str, Enum):
    DISCOVERY_PRO = "DISCOVERY_PRO"
    CONTACT_ENTERPRISE = "CONTACT_ENTERPRISE"


DISCOVERY_PRO_FIELDS = (
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.location",
    "places.types",
    "places.primaryType",
    "places.businessStatus",
    "places.googleMapsUri",
    "nextPageToken",
)
CONTACT_ENTERPRISE_FIELDS = DISCOVERY_PRO_FIELDS + (
    "places.websiteUri",
    "places.nationalPhoneNumber",
    "places.rating",
    "places.userRatingCount",
)

PRIMARY_GOOGLE_QUERIES = (
    "local service business in Westport, Northfield",
    "local business in Westport, Northfield",
    "local service business in Riverton, Northfield",
    "local business in Riverton, Northfield",
    "local service business in Fairview, Northfield",
    "local business near Harbor District, Northfield",
)


@_dataclass(frozen=True)
class GooglePlacesHTTPResponse:
    status_code: int
    body: Mapping[str, Any]


class GooglePlacesRequestError(RuntimeError):
    """Safe Google request failure; never includes credentials or response dumps."""

    def __init__(self, category, *, status_code=None):
        self.category = category
        self.status_code = status_code
        suffix = f" (HTTP {status_code})" if status_code is not None else ""
        super().__init__(f"Google Places request failed: {category}{suffix}")


@_dataclass(frozen=True)
class GooglePlacesSearchResult:
    query: str
    records: tuple
    page_count: int
    request_count: int


@_dataclass(frozen=True)
class _GoogleQueryRow:
    id: int
    query_text: str
    language_code: str
    region_code: str
    page_size: int
    max_pages: int


def _google_profile_value(profile):
    if isinstance(profile, GooglePlacesCostProfile):
        return profile
    try:
        return GooglePlacesCostProfile(str(profile))
    except ValueError as error:
        raise ValueError("cost_profile must be DISCOVERY_PRO or CONTACT_ENTERPRISE") from error


def _google_field_mask(profile):
    fields = DISCOVERY_PRO_FIELDS if profile == GooglePlacesCostProfile.DISCOVERY_PRO else CONTACT_ENTERPRISE_FIELDS
    mask = ",".join(fields)
    if "*" in mask:
        raise AssertionError("wildcard field masks are prohibited")
    return mask


def _safe_google_response_body(body):
    if isinstance(body, Mapping):
        return dict(body)
    if isinstance(body, (bytes, bytearray)):
        try:
            parsed = json.loads(bytes(body).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    if isinstance(body, str):
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _default_google_transport(*, url, headers, body, timeout_seconds):
    request = _urllib_request.Request(
        url,
        data=json.dumps(body, separators=(",", ":")).encode("utf-8"),
        headers=dict(headers),
        method="POST",
    )
    try:
        with _urllib_request.urlopen(request, timeout=timeout_seconds) as response:
            return GooglePlacesHTTPResponse(response.status, _safe_google_response_body(response.read()))
    except _urllib_error.HTTPError as error:
        # Read only for status classification; never expose or persist the body.
        try:
            error.read()
        except OSError:
            pass
        return GooglePlacesHTTPResponse(error.code, {})
    except _urllib_error.URLError as error:
        raise GooglePlacesRequestError("network") from None
    except TimeoutError:
        raise GooglePlacesRequestError("timeout") from None


class GooglePlacesClient:
    """Google Places API (New) text-search client with bounded request behavior."""

    def __init__(
        self,
        *,
        env=None,
        transport=None,
        cost_profile=GooglePlacesCostProfile.DISCOVERY_PRO,
        enable_contact_enterprise=False,
        timeout_seconds=10,
        max_retries=2,
        retry_delay_seconds=0.0,
        allow_missing_api_key=False,
    ):
        self.env = os.environ if env is None else env
        self.transport = _default_google_transport if transport is None else transport
        self.cost_profile = _google_profile_value(cost_profile)
        if self.cost_profile == GooglePlacesCostProfile.CONTACT_ENTERPRISE and enable_contact_enterprise is not True:
            raise ValueError("CONTACT_ENTERPRISE requires explicit enablement")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or not 0 <= max_retries <= 5:
            raise ValueError("max_retries must be between 0 and 5")
        if isinstance(retry_delay_seconds, bool) or not isinstance(retry_delay_seconds, (int, float)) or retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be non-negative")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.allow_missing_api_key = allow_missing_api_key
        self.last_request = None

    @staticmethod
    def _status_category(status_code):
        return {
            400: "invalid_request",
            401: "authentication",
            402: "billing",
            403: "permission",
            404: "invalid_request",
            408: "transient",
            409: "invalid_request",
            429: "transient",
            500: "transient",
            502: "transient",
            503: "transient",
            504: "transient",
        }.get(status_code, "http_error")

    @staticmethod
    def _response_value(response):
        if isinstance(response, GooglePlacesHTTPResponse):
            return response
        if isinstance(response, Mapping):
            status_code = response.get("status_code", response.get("status", 200))
            body = response.get("body", response)
            return GooglePlacesHTTPResponse(int(status_code), _safe_google_response_body(body))
        raise GooglePlacesRequestError("invalid_response")

    def _request_page(self, body, *, query, page_number, observer=None, quota_remaining=None):
        api_key = self.env.get("GOOGLE_PLACES_API_KEY") if isinstance(self.env, Mapping) else None
        if not api_key and not self.allow_missing_api_key:
            raise GooglePlacesRequestError("authentication")
        if quota_remaining is not None and quota_remaining <= 0:
            raise GooglePlacesRequestError("quota")
        headers = {
            "Content-Type": "application/json",
            "X-Goog-FieldMask": _google_field_mask(self.cost_profile),
        }
        if api_key:
            headers["X-Goog-Api-Key"] = api_key
        self.last_request = {"url": GOOGLE_PLACES_ENDPOINT, "headers": dict(headers), "body": dict(body)}
        attempts = 0
        while True:
            attempts += 1
            try:
                response = self._response_value(
                    self.transport(
                        url=GOOGLE_PLACES_ENDPOINT,
                        headers=headers,
                        body=dict(body),
                        timeout_seconds=self.timeout_seconds,
                    )
                )
            except (TimeoutError, _urllib_error.URLError) as raw_error:
                error = GooglePlacesRequestError("timeout" if isinstance(raw_error, TimeoutError) else "network")
                if observer:
                    observer({"query_text": query, "page_number": page_number, "status_code": None, "error_category": error.category, "result_count": 0})
                if attempts <= self.max_retries and (quota_remaining is None or attempts < quota_remaining + 1):
                    if self.retry_delay_seconds:
                        _time.sleep(self.retry_delay_seconds)
                    continue
                raise error from None
            except GooglePlacesRequestError as error:
                if observer:
                    observer({"query_text": query, "page_number": page_number, "status_code": error.status_code, "error_category": error.category, "result_count": 0})
                if error.category in {"timeout", "network", "transient"} and attempts <= self.max_retries and (quota_remaining is None or attempts < quota_remaining + 1):
                    if self.retry_delay_seconds:
                        _time.sleep(self.retry_delay_seconds)
                    continue
                raise
            category = self._status_category(response.status_code)
            if response.status_code == 200:
                if observer:
                    observer({"query_text": query, "page_number": page_number, "status_code": 200, "error_category": None, "result_count": len(response.body.get("places", [])) if isinstance(response.body.get("places", []), list) else 0})
                return response.body, attempts
            if observer:
                observer({"query_text": query, "page_number": page_number, "status_code": response.status_code, "error_category": category, "result_count": 0})
            if category == "transient" and attempts <= self.max_retries and (quota_remaining is None or attempts < quota_remaining + 1):
                if self.retry_delay_seconds:
                    _time.sleep(self.retry_delay_seconds)
                continue
            raise GooglePlacesRequestError(category, status_code=response.status_code)

    @staticmethod
    def _place_record(place):
        if not isinstance(place, Mapping):
            return None
        display_name = place.get("displayName")
        if isinstance(display_name, Mapping):
            display_name = display_name.get("text")
        location = place.get("location") if isinstance(place.get("location"), Mapping) else {}
        types = place.get("types") if isinstance(place.get("types"), list) else []
        record = {
            "source_record_id": place.get("id"),
            "google_place_id": place.get("id"),
            "name": display_name,
            "formatted_address": place.get("formattedAddress"),
            "latitude": location.get("latitude"),
            "longitude": location.get("longitude"),
            "types": types,
            "primary_type": place.get("primaryType") or (types[0] if types else None),
            "business_status": place.get("businessStatus"),
            "google_maps_url": place.get("googleMapsUri"),
        }
        optional = {
            "website_url": place.get("websiteUri"),
            "public_business_phone": place.get("nationalPhoneNumber"),
            "rating": place.get("rating"),
            "user_rating_count": place.get("userRatingCount"),
        }
        record.update({key: value for key, value in optional.items() if value is not None})
        return {key: value for key, value in record.items() if value is not None}

    def search_text(
        self,
        query,
        *,
        language_code="en",
        region_code="PH",
        page_size=20,
        max_pages=1,
        page_token=None,
        quota_remaining=None,
        observer=None,
    ):
        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must be non-empty text")
        if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 20:
            raise ValueError("page_size must be between 1 and 20")
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
            raise ValueError("max_pages must be at least 1")
        records = []
        token = page_token
        page_count = 0
        request_count = 0
        while page_count < max_pages:
            body = {
                "textQuery": query,
                "languageCode": language_code,
                "regionCode": region_code,
                "pageSize": page_size,
            }
            if token:
                body["pageToken"] = token
            page_count += 1
            response_body, attempts = self._request_page(
                body,
                query=query,
                page_number=page_count,
                observer=observer,
                quota_remaining=None if quota_remaining is None else quota_remaining - request_count,
            )
            request_count += attempts
            places = response_body.get("places", []) if isinstance(response_body, Mapping) else []
            if isinstance(places, list):
                records.extend(record for record in (self._place_record(place_item) for place_item in places) if record is not None)
            token = response_body.get("nextPageToken") if isinstance(response_body, Mapping) else None
            if not token:
                break
        return GooglePlacesSearchResult(query, tuple(records), page_count, request_count)


def _step4_google_normalize_records(self, records, *, mode=DiscoveryMode.DRY_RUN, collected_at=None):
    mode_value = _mode_value(mode)
    if mode_value == DiscoveryMode.LIVE.value:
        return [
            normalize_candidate(record, source=GOOGLE_PLACES_SOURCE, collected_at=collected_at, mode=mode_value, provenance_type="LIVE")
            for record in records
        ]
    return _step3_google_adapter_normalize_records(self, records, mode=mode_value, collected_at=collected_at)


_step3_google_adapter_normalize_records = GooglePlacesAdapter.normalize_records
GooglePlacesAdapter.normalize_records = _step4_google_normalize_records


_STEP4_SCHEMA = """
CREATE TABLE IF NOT EXISTS google_query_plans (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    source TEXT NOT NULL CHECK (source = 'GOOGLE_PLACES'),
    query_order INTEGER NOT NULL,
    query_text TEXT NOT NULL,
    language_code TEXT NOT NULL DEFAULT 'en',
    region_code TEXT NOT NULL DEFAULT 'PH',
    page_size INTEGER NOT NULL DEFAULT 20 CHECK (page_size BETWEEN 1 AND 20),
    max_pages INTEGER NOT NULL DEFAULT 1 CHECK (max_pages >= 1),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (campaign_id, query_order),
    UNIQUE (campaign_id, query_text)
);
CREATE INDEX IF NOT EXISTS google_query_plans_campaign_idx ON google_query_plans(campaign_id, query_order);
CREATE TABLE IF NOT EXISTS google_places_request_log (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER REFERENCES campaigns(id) ON DELETE SET NULL,
    query_text TEXT NOT NULL,
    page_number INTEGER NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('DRY_RUN', 'LIVE')),
    status TEXT NOT NULL CHECK (status IN ('SUCCEEDED', 'FAILED')),
    error_category TEXT,
    http_status INTEGER,
    result_count INTEGER NOT NULL DEFAULT 0,
    requested_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS google_places_request_log_time_idx ON google_places_request_log(requested_at);
CREATE INDEX IF NOT EXISTS google_places_request_log_campaign_idx ON google_places_request_log(campaign_id);
"""


def migrate_step4(database_or_path):
    """Apply Step 3 plus idempotent Google query-plan/request-log schema."""
    if hasattr(database_or_path, "connection"):
        database = database_or_path
        close_after = False
    else:
        from paldo_os_outbound import Database
        database = Database(Path(database_or_path))
        close_after = True
    try:
        migrate_step3(database)
        timestamp = _utc_now()
        with database.connection:
            database.connection.executescript(_STEP4_SCHEMA)
            campaign = database.get_campaign_by_name(GOOGLE_QUERY_PLAN_CAMPAIGN)
            if campaign:
                for order, query_text in enumerate(PRIMARY_GOOGLE_QUERIES, 1):
                    database.connection.execute(
                        """INSERT OR IGNORE INTO google_query_plans
                           (campaign_id, source, query_order, query_text, language_code, region_code,
                            page_size, max_pages, enabled, created_at, updated_at)
                           VALUES (?, 'GOOGLE_PLACES', ?, ?, 'en', 'PH', 20, 1, 1, ?, ?)""",
                        (campaign["id"], order, query_text, timestamp, timestamp),
                    )
            database.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
                   VALUES (?, ?, ?)""",
                (STEP4_MIGRATION_VERSION, "step4_google_places_query_plans_and_request_log", timestamp),
            )
        return STEP4_MIGRATION_VERSION
    finally:
        if close_after:
            database.close()


def _google_campaign(database, campaign_id):
    campaign = database.get_campaign(campaign_id) if campaign_id is not None else database.get_campaign_by_name(GOOGLE_QUERY_PLAN_CAMPAIGN)
    if campaign is None:
        raise ValueError("Google Places campaign was not found")
    return campaign


def preview_google_query_plan(database, campaign_id=None):
    campaign = _google_campaign(database, campaign_id)
    rows = database.connection.execute(
        """SELECT id, campaign_id, source, query_order, query_text, language_code,
                  region_code, page_size, max_pages, enabled
           FROM google_query_plans WHERE campaign_id = ? AND enabled = 1 ORDER BY query_order""",
        (campaign["id"],),
    ).fetchall()
    return [dict(row) for row in rows]


def set_google_query_plan(database, campaign_id, queries, *, language_code="en", region_code="PH", page_size=20, max_pages=1):
    """Replace one campaign's configurable Google query plan after validation."""
    campaign = _google_campaign(database, campaign_id)
    if not isinstance(queries, (list, tuple)) or not queries:
        raise ValueError("queries must be a non-empty list")
    if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 20:
        raise ValueError("page_size must be between 1 and 20")
    if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
        raise ValueError("max_pages must be at least 1")
    normalized_queries = []
    for query in queries:
        if not isinstance(query, str) or not query.strip() or query.strip() in normalized_queries:
            raise ValueError("queries must contain unique non-empty text")
        normalized_queries.append(query.strip())
    timestamp = _utc_now()
    with database.connection:
        database.connection.execute("DELETE FROM google_query_plans WHERE campaign_id = ?", (campaign["id"],))
        database.connection.executemany(
            """INSERT INTO google_query_plans
               (campaign_id, source, query_order, query_text, language_code, region_code,
                page_size, max_pages, enabled, created_at, updated_at)
               VALUES (?, 'GOOGLE_PLACES', ?, ?, ?, ?, ?, ?, 1, ?, ?)""",
            ((campaign["id"], index, query, language_code, region_code, page_size, max_pages, timestamp, timestamp) for index, query in enumerate(normalized_queries, 1)),
        )
    return preview_google_query_plan(database, campaign["id"])


def _google_used_requests_today(database, campaign_id):
    today = datetime.now(timezone.utc).date().isoformat()
    return database.connection.execute(
        "SELECT COUNT(*) FROM google_places_request_log WHERE campaign_id = ? AND mode = 'LIVE' AND requested_at LIKE ?",
        (campaign_id, today + "%"),
    ).fetchone()[0]


def estimate_google_request_count(database, campaign_id=None):
    campaign = _google_campaign(database, campaign_id)
    plan = preview_google_query_plan(database, campaign["id"])
    estimated = sum(row["max_pages"] for row in plan)
    quota = database.get_config("google_places_daily_request_cap")
    used = _google_used_requests_today(database, campaign["id"])
    remaining = max(0, quota - used)
    return {
        "campaign_id": campaign["id"],
        "query_count": len(plan),
        "estimated_requests": estimated,
        "quota": quota,
        "used_requests": used,
        "remaining_quota": remaining,
        "within_quota": estimated <= remaining,
    }


def google_places_readiness(database, campaign_id=None, *, env=None):
    campaign = _google_campaign(database, campaign_id)
    environment = os.environ if env is None else env
    credential_present = bool(environment.get("GOOGLE_PLACES_API_KEY")) if isinstance(environment, Mapping) else False
    estimate = estimate_google_request_count(database, campaign["id"])
    state = database.get_config("system_state")
    mode = database.get_config("discovery_mode")
    return {
        "endpoint": GOOGLE_PLACES_ENDPOINT,
        "default_cost_profile": GooglePlacesCostProfile.DISCOVERY_PRO.value,
        "campaign_id": campaign["id"],
        "campaign_status": campaign["status"],
        "system_state": state,
        "discovery_mode": mode,
        "credential_present": credential_present,
        "credential_value_returned": False,
        "quota": estimate["quota"],
        "remaining_quota": estimate["remaining_quota"],
        "estimated_requests": estimate["estimated_requests"],
        "ready": state != "PAUSED" and campaign["status"] == "ACTIVE" and mode == "LIVE" and credential_present and estimate["quota"] > 0 and estimate["within_quota"],
    }


def _record_google_request(database, *, campaign_id, details, mode="LIVE"):
    status = "SUCCEEDED" if details.get("error_category") is None and details.get("status_code") == 200 else "FAILED"
    with database.connection:
        database.connection.execute(
            """INSERT INTO google_places_request_log
               (campaign_id, query_text, page_number, mode, status, error_category, http_status, result_count, requested_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (campaign_id, details.get("query_text", ""), details.get("page_number", 0), mode, status,
             details.get("error_category"), details.get("status_code"), details.get("result_count", 0), _utc_now()),
        )


def mock_google_execution(database, campaign_id=None, *, transport, cost_profile=GooglePlacesCostProfile.DISCOVERY_PRO, max_retries=0):
    """Execute the query plan against an injected mock transport without persistence."""
    campaign = _google_campaign(database, campaign_id)
    client = GooglePlacesClient(
        transport=transport,
        env={},
        allow_missing_api_key=True,
        cost_profile=cost_profile,
        enable_contact_enterprise=cost_profile == GooglePlacesCostProfile.CONTACT_ENTERPRISE,
        max_retries=max_retries,
    )
    request_count = 0
    record_count = 0
    errors = []
    for row in preview_google_query_plan(database, campaign["id"]):
        try:
            result = client.search_text(
                row["query_text"], language_code=row["language_code"], region_code=row["region_code"],
                page_size=row["page_size"], max_pages=row["max_pages"], quota_remaining=None,
            )
            request_count += result.request_count
            record_count += len(result.records)
        except GooglePlacesRequestError as error:
            errors.append(error.category)
    return {"mode": "DRY_RUN", "provenance_type": "FIXTURE", "campaign_id": campaign["id"], "request_count": request_count, "record_count": record_count, "errors": sorted(set(errors))}


def execute_google_live(
    database,
    campaign_id=None,
    *,
    env=None,
    operator_confirmation=False,
    transport=None,
    cost_profile=GooglePlacesCostProfile.DISCOVERY_PRO,
    max_retries=2,
):
    """Future explicitly confirmed LIVE execution; all gates run before transport."""
    campaign = _google_campaign(database, campaign_id)
    environment = os.environ if env is None else env
    if database.get_config("discovery_mode") != DiscoveryMode.LIVE.value:
        raise LiveModeBlockedError("LIVE blocked: discovery_mode must be LIVE")
    allowed, reason = check_live_guard(
        database,
        source=GOOGLE_PLACES_SOURCE,
        campaign_id=campaign["id"],
        operator_explicitly_requested_live=operator_confirmation,
        env=environment,
    )
    if not allowed:
        raise LiveModeBlockedError(reason)
    estimate = estimate_google_request_count(database, campaign["id"])
    if estimate["estimated_requests"] > estimate["remaining_quota"]:
        raise LiveModeBlockedError("LIVE blocked: estimated requests exceed remaining Google Places quota")
    profile = _google_profile_value(cost_profile)
    client = GooglePlacesClient(
        env=environment,
        transport=transport,
        cost_profile=profile,
        enable_contact_enterprise=profile == GooglePlacesCostProfile.CONTACT_ENTERPRISE,
        max_retries=max_retries,
    )
    timestamp = _utc_now()
    with database.connection:
        run_id = _create_discovery_run(database, source=GOOGLE_PLACES_SOURCE, mode=DiscoveryMode.LIVE.value, provenance_type="LIVE")
    adapter = GooglePlacesAdapter()
    results = []
    errors = []
    total_requests = 0
    total_records = 0
    try:
        for row in preview_google_query_plan(database, campaign["id"]):
            def observer(details, row=row):
                _record_google_request(database, campaign_id=campaign["id"], details=details, mode="LIVE")
            try:
                result = client.search_text(
                    row["query_text"], language_code=row["language_code"], region_code=row["region_code"],
                    page_size=row["page_size"], max_pages=row["max_pages"],
                    quota_remaining=estimate["remaining_quota"] - total_requests, observer=observer,
                )
                total_requests += result.request_count
                total_records += len(result.records)
                candidates = adapter.normalize_records(result.records, mode=DiscoveryMode.LIVE.value, collected_at=timestamp)
                database._step4_live_ingestion_authorized = True
                try:
                    results.extend(_step3_ingest_refined(database, candidate, run_id=run_id) for candidate in candidates)
                finally:
                    database._step4_live_ingestion_authorized = False
            except GooglePlacesRequestError as error:
                errors.append(error.category)
                if error.category == "quota":
                    break
        _finish_discovery_run(database, run_id, results, status="SOURCE_ERROR" if errors else "COMPLETED", input_count=total_records)
    finally:
        database._step4_live_ingestion_authorized = False
    return {
        "mode": "LIVE", "provenance_type": "LIVE", "campaign_id": campaign["id"],
        "run_id": run_id, "request_count": total_requests, "record_count": total_records,
        "ingestion_count": len(results), "errors": sorted(set(errors)),
    }


_step3_ingest_refined = ingest_candidate

__all__ += (
    "CONTACT_ENTERPRISE_FIELDS", "DISCOVERY_PRO_FIELDS", "GOOGLE_PLACES_ENDPOINT", "GOOGLE_QUERY_PLAN_CAMPAIGN",
    "GooglePlacesClient", "GooglePlacesCostProfile", "GooglePlacesHTTPResponse", "GooglePlacesRequestError",
    "GooglePlacesSearchResult", "PRIMARY_GOOGLE_QUERIES", "STEP4_MIGRATION_VERSION", "execute_google_live",
    "estimate_google_request_count", "google_places_readiness", "migrate_step4", "mock_google_execution",
    "preview_google_query_plan", "set_google_query_plan",
)


# Step 5: resumable, live-ready Apify Actor lifecycle. All network access is
# transport-injected in tests and is gated by the durable operating state.
from dataclasses import replace as _replace
import math as _math
from urllib.parse import quote as _quote, urlencode as _urlencode


STEP5_MIGRATION_VERSION = 7
DEFAULT_APIFY_ACTOR_ID = "compass~crawler-google-places"
APIFY_API_BASE_URL = "https://api.apify.com/v2"


class ApifyRunState(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    ABORTED = "ABORTED"
    TIMED_OUT = "TIMED-OUT"


class ApifyInputError(ValueError):
    """Raised when an Actor input requests unsafe, unbounded, or private data."""


class ApifyRunBlockedError(RuntimeError):
    """Raised when a future live Actor run fails a safety or lifecycle gate."""


class ApifyRequestError(RuntimeError):
    """Safe Apify failure; stores only redacted provider error metadata."""

    def __init__(self, category, *, status_code=None, provider_type=None, provider_message=None):
        self.category = category
        self.status_code = status_code
        self.provider_type = _safe_apify_text(provider_type)
        self.provider_message = _safe_apify_text(provider_message)
        suffix = f" (HTTP {status_code})" if status_code is not None else ""
        detail = f": {self.provider_type}: {self.provider_message}" if self.provider_type or self.provider_message else ""
        super().__init__(f"Apify request failed: {category}{suffix}{detail}")


def _safe_apify_text(value, *, token=None):
    if value is None:
        return None
    text = str(value)
    if token:
        text = text.replace(str(token), "[REDACTED]")
    text = re.sub(r"(?i)(authorization|bearer|token|api[_-]?key|password|secret)\\s*[=:]\\s*[^\\s,;]+", r"\\1=[REDACTED]", text)
    text = re.sub(r"(?i)([?&](?:token|api[_-]?key|access_token)=)[^&\\s]+", r"\\1[REDACTED]", text)
    return text[:300]


def _apify_error_details(body, *, token=None):
    if not isinstance(body, Mapping):
        return None, None
    nested = body.get("error")
    error_map: Mapping = nested if isinstance(nested, Mapping) else body
    provider_type = error_map.get("type") or error_map.get("errorType") or error_map.get("code")
    provider_message = error_map.get("message") or error_map.get("detail") or error_map.get("error")
    return _safe_apify_text(provider_type, token=token), _safe_apify_text(provider_message, token=token)


@_dataclass(frozen=True)
class ApifyHTTPResponse:
    status_code: int
    body: Any


_APIFY_DEFAULT_INPUT = {
    "language": "en",
    "countryCode": "us",
    "maxCrawledPlacesPerSearch": 20,
    "skipClosedPlaces": True,
    "website": "allPlaces",
    "searchMatching": "all",
    "scrapePlaceDetailPage": False,
    "scrapeContacts": False,
    "scrapeSocialMediaProfiles": {"facebooks": False, "instagrams": False, "youtubes": False, "tiktoks": False, "twitters": False},
    "maximumLeadsEnrichmentRecords": 0,
    "verifyLeadsEnrichmentEmails": False,
    "maxReviews": 0,
    "scrapeReviewsPersonalData": False,
    "maxImages": 0,
    "scrapeImageAuthors": False,
    "enableCompetitorAnalysis": False,
}
_APIFY_INPUT_KEYS = frozenset(_APIFY_DEFAULT_INPUT) | {"locationQuery", "searchStringsArray"}


def _apify_text(value, field):
    if not isinstance(value, str) or not " ".join(value.split()):
        raise ApifyInputError(f"{field} must be a non-empty text value")
    return " ".join(value.split())


def _apify_bounded_items(value):
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 25:
        raise ApifyInputError("max items must be between 1 and 25")
    return value


def _apify_safe_setting(key, value):
    prohibited = {
        "scrapeContacts": value is not False,
        "scrapeReviewsPersonalData": value is not False,
        "scrapeImageAuthors": value is not False,
        "enableCompetitorAnalysis": value is not False,
        "verifyLeadsEnrichmentEmails": value is not False,
        "maximumLeadsEnrichmentRecords": value != 0,
        "maxReviews": value != 0,
        "maxImages": value != 0,
    }
    if prohibited.get(key):
        raise ApifyInputError(f"{key} is prohibited in Step 5")
    if key == "maxCrawledPlacesPerSearch":
        return _apify_bounded_items(value)
    if key == "countryCode":
        if value not in {"us", "ph"}:
            raise ApifyInputError("countryCode must be us or ph")
        return value
    expected = _APIFY_DEFAULT_INPUT.get(key)
    if expected is not None and value != expected:
        raise ApifyInputError(f"{key} must remain at the safe Step 5 value")
    return value


def build_apify_safe_input(location, search_term, *, max_items=20, country_code="us", overrides=None):
    """Build one bounded public-place search with all enrichment disabled."""
    max_items = _apify_bounded_items(max_items)
    location = _apify_text(location, "location")
    search_term = _apify_text(search_term, "search term")
    country_code = str(country_code).lower()
    if country_code not in {"us", "ph"}:
        raise ApifyInputError("country_code must be us or ph")
    safe = dict(_APIFY_DEFAULT_INPUT)
    safe.update({"locationQuery": location, "searchStringsArray": [search_term], "countryCode": country_code, "maxCrawledPlacesPerSearch": max_items})
    if overrides:
        if not isinstance(overrides, Mapping):
            raise ApifyInputError("overrides must be a mapping")
        unknown = set(overrides) - _APIFY_INPUT_KEYS
        if unknown:
            raise ApifyInputError("unsupported Actor input option")
        for key, value in overrides.items():
            if key in {"locationQuery", "searchStringsArray"}:
                raise ApifyInputError(f"{key} is fixed to one bounded search")
            safe[key] = _apify_safe_setting(key, value)
    if safe["maxCrawledPlacesPerSearch"] > max_items:
        raise ApifyInputError("maxCrawledPlacesPerSearch cannot exceed max items")
    return safe


def preview_apify_input(location, search_term, *, max_items=20, overrides=None):
    return {
        "mode": DiscoveryMode.DRY_RUN.value,
        "provenance_type": "FIXTURE",
        "input": build_apify_safe_input(location, search_term, max_items=max_items, overrides=overrides),
    }


_APIFY_SCHEMA = """
CREATE TABLE IF NOT EXISTS apify_runs (
    id INTEGER PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    actor_id TEXT NOT NULL,
    location TEXT NOT NULL,
    search_term TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('DRY_RUN', 'LIVE')),
    state TEXT NOT NULL CHECK (state IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT')),
    remote_run_id TEXT NOT NULL UNIQUE,
    dataset_id TEXT,
    safe_input_json TEXT NOT NULL,
    started_at TEXT NOT NULL,
    last_polled_at TEXT,
    completed_at TEXT,
    dataset_fetched_at TEXT,
    item_count INTEGER NOT NULL DEFAULT 0,
    ingested_count INTEGER NOT NULL DEFAULT 0,
    error_category TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS apify_runs_campaign_idx ON apify_runs(campaign_id, id);
CREATE INDEX IF NOT EXISTS apify_runs_state_idx ON apify_runs(state, mode);
CREATE INDEX IF NOT EXISTS apify_runs_started_idx ON apify_runs(started_at);
"""


def migrate_step5(database_or_path):
    """Apply the additive, transactional Step 5 migration without recreating data."""
    if hasattr(database_or_path, "connection"):
        database = database_or_path
        close_after = False
    else:
        from paldo_os_outbound import Database

        database = Database(Path(database_or_path))
        close_after = True
    try:
        migrate_step4(database)
        timestamp = _utc_now()
        config_defaults = (
            ("apify_actor_id", DEFAULT_APIFY_ACTOR_ID, "text"),
            ("apify_max_items_per_run", "25", "integer"),
            ("apify_max_total_charge_usd", "0", "text"),
            ("apify_max_concurrent_runs", "1", "integer"),
        )
        with database.connection:
            database.connection.executescript(_APIFY_SCHEMA)
            database.connection.executemany(
                """INSERT OR IGNORE INTO system_config (key, value, value_type) VALUES (?, ?, ?)""",
                config_defaults,
            )
            database.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
                   VALUES (?, ?, ?)""",
                (STEP5_MIGRATION_VERSION, "step5_resumable_apify_actor", timestamp),
            )
        return STEP5_MIGRATION_VERSION
    finally:
        if close_after:
            database.close()


def _apify_config(database):
    config = database.read_config()
    actor_id = config.get("apify_actor_id") or DEFAULT_APIFY_ACTOR_ID
    max_items = config.get("apify_max_items_per_run", 20)
    max_charge = config.get("apify_max_total_charge_usd", 0.0)
    max_concurrent = config.get("apify_max_concurrent_runs", 1)
    return config, str(actor_id), max_items, float(max_charge), max_concurrent


def _apify_run_count_today(database):
    today = _utc_now()[:10]
    return database.connection.execute(
        "SELECT COUNT(*) FROM apify_runs WHERE mode='LIVE' AND started_at LIKE ? AND COALESCE(error_category, '') != 'SMOKE_TEST_EXCLUDED'", (today + "%",)
    ).fetchone()[0]


def _apify_in_progress_count(database):
    return database.connection.execute(
        "SELECT COUNT(*) FROM apify_runs WHERE mode='LIVE' AND state='RUNNING'"
    ).fetchone()[0]


def preview_apify_cost_limits(database, campaign_id=None):
    config, actor_id, max_items, max_charge, max_concurrent = _apify_config(database)
    quota = int(config.get("apify_daily_run_cap", 0))
    used = _apify_run_count_today(database) if _table_exists(database, "apify_runs") else 0
    return {
        "actor_id": actor_id,
        "daily_run_quota": quota,
        "runs_used_today": used,
        "remaining_run_quota": max(0, quota - used),
        "max_items_per_run": max_items,
        "max_total_charge_usd": max_charge,
        "max_concurrent_runs": max_concurrent,
        "in_progress_runs": _apify_in_progress_count(database) if _table_exists(database, "apify_runs") else 0,
        "campaign_id": campaign_id,
        "mode": config.get("discovery_mode"),
        "system_state": config.get("system_state"),
    }


def apify_readiness(database, *, campaign_id=None, env=None):
    config, actor_id, max_items, max_charge, max_concurrent = _apify_config(database)
    environment = os.environ if env is None else env
    token_present = isinstance(environment, Mapping) and bool(environment.get("APIFY_API_TOKEN"))
    campaign = database.get_campaign(campaign_id) if campaign_id is not None else database.get_campaign_by_name("Secondary region local services")
    campaign_active = bool(campaign and campaign["status"] == "ACTIVE")
    quota = int(config.get("apify_daily_run_cap", 0))
    used = _apify_run_count_today(database) if _table_exists(database, "apify_runs") else 0
    in_progress = _apify_in_progress_count(database) if _table_exists(database, "apify_runs") else 0
    gates = {
        "system_not_paused": config.get("system_state") != "PAUSED",
        "discovery_live": config.get("discovery_mode") == "LIVE",
        "campaign_active": campaign_active,
        "credential_present": token_present,
        "positive_daily_run_quota": quota > used,
        "positive_max_total_charge_usd": isinstance(max_charge, (int, float)) and max_charge > 0,
        "max_items_bounded": isinstance(max_items, int) and 1 <= max_items <= 25,
        "concurrency_available": isinstance(max_concurrent, int) and max_concurrent >= 1 and in_progress < max_concurrent,
    }
    return {
        "ready": all(gates.values()),
        "actor_id": actor_id,
        "credential_present": token_present,
        "credential_value_returned": False,
        "gates": gates,
        "daily_run_quota": quota,
        "runs_used_today": used,
        "remaining_run_quota": max(0, quota - used),
        "max_items_per_run": max_items,
        "max_total_charge_usd": max_charge,
        "max_concurrent_runs": max_concurrent,
        "in_progress_runs": in_progress,
        "campaign_id": campaign["id"] if campaign else None,
        "mode": config.get("discovery_mode"),
        "system_state": config.get("system_state"),
    }


class ApifyClient:
    """Small Apify v2 client; transport is injectable and credentials stay in memory."""

    def __init__(self, *, env=None, transport=None, base_url=APIFY_API_BASE_URL, timeout_seconds=15):
        self.env = os.environ if env is None else env
        self.transport = transport or _default_apify_transport
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def _request(self, *, method, path, body=None, query=None):
        token = self.env.get("APIFY_API_TOKEN") if isinstance(self.env, Mapping) else None
        if not token:
            raise ApifyRequestError("authentication")
        url = self.base_url + path
        if query:
            url += "?" + _urlencode(query)
        headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        try:
            response = self.transport(method=method, url=url, headers=headers, body=body, timeout_seconds=self.timeout_seconds)
        except ApifyRequestError:
            raise
        except (TimeoutError, _urllib_error.URLError) as raw_error:
            raise ApifyRequestError("timeout" if isinstance(raw_error, TimeoutError) else "network") from None
        except Exception:
            raise ApifyRequestError("source_error") from None
        if not isinstance(response, ApifyHTTPResponse):
            if isinstance(response, Mapping):
                response = ApifyHTTPResponse(int(response.get("status_code", 200)), response.get("body", response))
            else:
                raise ApifyRequestError("source_error")
        if response.status_code not in {200, 201, 202}:
            category = {401: "authentication", 402: "billing", 403: "permission", 408: "timeout", 429: "transient"}.get(response.status_code, "transient" if response.status_code >= 500 else "invalid_request")
            provider_type, provider_message = _apify_error_details(response.body, token=token)
            raise ApifyRequestError(category, status_code=response.status_code, provider_type=provider_type, provider_message=provider_message)
        return response.body

    @staticmethod
    def _data(body):
        if isinstance(body, Mapping) and isinstance(body.get("data"), Mapping):
            return dict(body["data"])
        return dict(body) if isinstance(body, Mapping) else {}

    def start_actor(self, *, actor_id, run_input, max_items, max_total_charge_usd):
        body = self._request(
            method="POST",
            path=f"/actors/{_quote(actor_id, safe='')}/runs",
            query={"maxTotalChargeUsd": f"{float(max_total_charge_usd):.2f}"},
            body=dict(run_input),
        )
        return self._data(body)

    def get_run_status(self, run_id):
        return self._data(self._request(method="GET", path=f"/actor-runs/{_quote(str(run_id), safe='')}", body=None))

    def fetch_dataset(self, dataset_id):
        body = self._request(method="GET", path=f"/datasets/{_quote(str(dataset_id), safe='')}/items", query={"clean": "true", "format": "json"}, body=None)
        if isinstance(body, list):
            return list(body)
        if isinstance(body, Mapping) and isinstance(body.get("items"), list):
            return list(body["items"])
        return list(body.get("data", [])) if isinstance(body, Mapping) and isinstance(body.get("data"), list) else []


def _default_apify_transport(*, method, url, headers, body, timeout_seconds):
    request = _urllib_request.Request(
        url,
        data=json.dumps(body, separators=(",", ":")).encode("utf-8") if body is not None else None,
        headers=dict(headers),
        method=method,
    )
    try:
        with _urllib_request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read()
            try:
                return ApifyHTTPResponse(response.status, json.loads(raw.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return ApifyHTTPResponse(response.status, {})
    except _urllib_error.HTTPError as error:
        raw = error.read()
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            body = {}
        return ApifyHTTPResponse(error.code, body)
    except _urllib_error.URLError:
        raise ApifyRequestError("network") from None
    except TimeoutError:
        raise ApifyRequestError("timeout") from None


def _apify_campaign(database, campaign_id):
    campaign = database.get_campaign(campaign_id)
    if not campaign:
        raise ApifyRunBlockedError("LIVE blocked: campaign not found")
    return campaign


def _apify_live_gate(database, campaign_id, *, env, operator_confirmation):
    config, actor_id, max_items, max_charge, max_concurrent = _apify_config(database)
    if config.get("system_state") == "PAUSED":
        raise ApifyRunBlockedError("LIVE blocked: system is PAUSED")
    if config.get("discovery_mode") != "LIVE":
        raise ApifyRunBlockedError("LIVE blocked: discovery_mode must be LIVE")
    campaign = _apify_campaign(database, campaign_id)
    if campaign["status"] != "ACTIVE":
        raise ApifyRunBlockedError("LIVE blocked: campaign must be ACTIVE")
    if not isinstance(env, Mapping) or not env.get("APIFY_API_TOKEN"):
        raise ApifyRunBlockedError("LIVE blocked: credential APIFY_API_TOKEN is missing")
    quota = int(config.get("apify_daily_run_cap", 0))
    used = _apify_run_count_today(database)
    if quota <= used:
        raise ApifyRunBlockedError("LIVE blocked: Apify daily run quota is exhausted")
    if not isinstance(max_charge, (int, float)) or max_charge <= 0:
        raise ApifyRunBlockedError("LIVE blocked: maximum total charge must be positive")
    if isinstance(max_items, bool) or not isinstance(max_items, int) or not 1 <= max_items <= 25:
        raise ApifyRunBlockedError("LIVE blocked: max items must be between 1 and 25")
    if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent < 1:
        raise ApifyRunBlockedError("LIVE blocked: max concurrent runs is invalid")
    if _apify_in_progress_count(database) >= max_concurrent:
        raise ApifyRunBlockedError("LIVE blocked: an in-progress Apify run already exists")
    if operator_confirmation is not True:
        raise ApifyRunBlockedError("LIVE blocked: explicit operator confirmation is required")
    if not actor_id.strip():
        raise ApifyRunBlockedError("LIVE blocked: Actor ID is missing")
    return campaign, actor_id, max_items, max_charge


def _apify_state(status):
    value = str(status or "RUNNING").upper().replace("_", "-")
    if value in {"TIMED-OUT", "TIMEDOUT"}:
        return ApifyRunState.TIMED_OUT.value
    if value in {state.value for state in ApifyRunState}:
        return value
    if value in {"READY", "RUNNING"}:
        return ApifyRunState.RUNNING.value
    raise ApifyRequestError("invalid_status")


def _apify_run_row(database, run_id):
    return database.connection.execute(
        "SELECT * FROM apify_runs WHERE remote_run_id=? OR CAST(id AS TEXT)=?", (str(run_id), str(run_id))
    ).fetchone()


def _safe_apify_row(row):
    if row is None:
        return None
    return {
        "run_id": row["id"], "remote_run_id": row["remote_run_id"], "campaign_id": row["campaign_id"],
        "actor_id": row["actor_id"], "location": row["location"], "search_term": row["search_term"],
        "mode": row["mode"], "state": row["state"], "dataset_id": row["dataset_id"],
        "started_at": row["started_at"], "last_polled_at": row["last_polled_at"], "completed_at": row["completed_at"],
        "dataset_fetched_at": row["dataset_fetched_at"], "item_count": row["item_count"],
        "ingested_count": row["ingested_count"], "error_category": row["error_category"],
    }


def summarize_apify_run(database, run_id=None):
    if run_id is not None:
        return _safe_apify_row(_apify_run_row(database, run_id))
    rows = database.connection.execute("SELECT * FROM apify_runs ORDER BY id").fetchall()
    return [_safe_apify_row(row) for row in rows]


def _safe_apify_error(error):
    if isinstance(error, ApifyRequestError):
        return error
    return ApifyRequestError("source_error")


def start_apify_live_run(database, campaign_id, location, search_term, *, country_code="US", env=None, operator_confirmation=False, apify_client=None, overrides=None):
    environment = os.environ if env is None else env
    campaign, actor_id, max_items, max_charge = _apify_live_gate(database, campaign_id, env=environment, operator_confirmation=operator_confirmation)
    safe_input = build_apify_safe_input(location, search_term, max_items=max_items, country_code=country_code, overrides=overrides)
    client = apify_client or ApifyClient(env=environment)
    try:
        response = client.start_actor(actor_id=actor_id, run_input=safe_input, max_items=max_items, max_total_charge_usd=max_charge)
    except Exception as error:
        raise _safe_apify_error(error) from None
    remote_run_id = response.get("id") or response.get("runId") if isinstance(response, Mapping) else None
    if not remote_run_id:
        raise ApifyRequestError("source_error")
    state = _apify_state(response.get("status") if isinstance(response, Mapping) else "RUNNING")
    now = _utc_now()
    dataset_id = response.get("defaultDatasetId") if isinstance(response, Mapping) else None
    completed = now if state != ApifyRunState.RUNNING.value else None
    with database.connection:
        database.connection.execute(
            """INSERT INTO apify_runs (campaign_id, actor_id, location, search_term, mode, state, remote_run_id,
               dataset_id, safe_input_json, started_at, completed_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'LIVE', ?, ?, ?, ?, ?, ?, ?, ?)""",
            (campaign["id"], actor_id, safe_input["locationQuery"], safe_input["searchStringsArray"][0], state,
             str(remote_run_id), dataset_id, json.dumps(safe_input, ensure_ascii=False, sort_keys=True), now, completed, now, now),
        )
    return _safe_apify_row(_apify_run_row(database, remote_run_id))


def poll_apify_run(database, run_id, *, apify_client=None):
    row = _apify_run_row(database, run_id)
    if row is None:
        raise ApifyRunBlockedError("Apify run not found")
    if row["state"] in {state.value for state in ApifyRunState if state != ApifyRunState.RUNNING}:
        return _safe_apify_row(row)
    client = apify_client or ApifyClient()
    try:
        response = client.get_run_status(row["remote_run_id"])
        state = _apify_state(response.get("status") if isinstance(response, Mapping) else None)
    except Exception as error:
        safe_error = _safe_apify_error(error)
        now = _utc_now()
        with database.connection:
            database.connection.execute("UPDATE apify_runs SET error_category=?, last_polled_at=?, updated_at=? WHERE id=?", (safe_error.category, now, now, row["id"]))
        raise safe_error from None
    now = _utc_now()
    dataset_id = response.get("defaultDatasetId") if isinstance(response, Mapping) else None
    terminal = state != ApifyRunState.RUNNING.value
    with database.connection:
        database.connection.execute(
            """UPDATE apify_runs SET state=?, dataset_id=COALESCE(?, dataset_id), last_polled_at=?,
               completed_at=CASE WHEN ? THEN ? ELSE completed_at END,
               error_category=CASE WHEN ? THEN ? ELSE error_category END, updated_at=? WHERE id=?""",
            (state, dataset_id, now, terminal, now, terminal and state != ApifyRunState.SUCCEEDED.value,
             None if state == ApifyRunState.SUCCEEDED.value else "actor_" + state.casefold().replace("-", "_"), now, row["id"]),
        )
    return _safe_apify_row(_apify_run_row(database, row["id"]))


def resume_apify_run(database, run_id, *, apify_client=None):
    """Resume polling a persisted run; this operation never starts a new run."""
    return poll_apify_run(database, run_id, apify_client=apify_client)


def fetch_apify_dataset(database, run_id, *, apify_client=None):
    row = _apify_run_row(database, run_id)
    if row is None:
        raise ApifyRunBlockedError("Apify run not found")
    if row["state"] != ApifyRunState.SUCCEEDED.value:
        raise ApifyRunBlockedError("dataset can be fetched only after SUCCEEDED")
    if not row["dataset_id"]:
        raise ApifyRequestError("source_error")
    client = apify_client or ApifyClient()
    try:
        records = client.fetch_dataset(row["dataset_id"])
        if not isinstance(records, list):
            raise ApifyRequestError("invalid_response")
    except Exception as error:
        safe_error = _safe_apify_error(error)
        now = _utc_now()
        with database.connection:
            database.connection.execute("UPDATE apify_runs SET error_category=?, updated_at=? WHERE id=?", (safe_error.category, now, row["id"]))
        raise safe_error from None
    discovery_run_id = None
    try:
        with database.connection:
            discovery_run_id = _create_discovery_run(database, source=APIFY_SOURCE, mode=DiscoveryMode.LIVE.value, provenance_type="LIVE")
        # The existing adapter performs the allowlisted mapping. DRY_RUN is used
        # only for its source-collection guard; authorization is applied after
        # the completed dataset has been fetched by this Step 5 operation.
        mapped = ApifyAdapter().normalize_records(records, mode=DiscoveryMode.DRY_RUN.value, collected_at=_utc_now())
        candidates = [_replace(candidate, provenance_mode=DiscoveryMode.LIVE.value, provenance_type="LIVE") for candidate in mapped]
        database._step4_live_ingestion_authorized = True
        try:
            results = [_step3_ingest_refined(database, candidate, run_id=discovery_run_id) for candidate in candidates]
        finally:
            database._step4_live_ingestion_authorized = False
        _finish_discovery_run(database, discovery_run_id, results, input_count=len(records))
    except Exception as error:
        database._step4_live_ingestion_authorized = False
        if discovery_run_id is not None:
            _safe_source_error_result(database, run_id=discovery_run_id)
        safe_error = _safe_apify_error(error)
        now = _utc_now()
        with database.connection:
            database.connection.execute("UPDATE apify_runs SET error_category=?, updated_at=? WHERE id=?", (safe_error.category, now, row["id"]))
        raise safe_error from None
    now = _utc_now()
    with database.connection:
        database.connection.execute(
            "UPDATE apify_runs SET dataset_fetched_at=?, item_count=?, ingested_count=?, error_category=NULL, updated_at=? WHERE id=?",
            (now, len(records), len(results), now, row["id"]),
        )
    outcomes = {}
    for result in results:
        outcome = result.outcome if hasattr(result, "outcome") else result.status
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
    summary = _safe_apify_row(_apify_run_row(database, row["id"]))
    summary["outcomes"] = outcomes
    return summary


__all__ += (
    "APIFY_API_BASE_URL", "ApifyClient", "ApifyHTTPResponse", "ApifyInputError", "ApifyRequestError",
    "ApifyRunBlockedError", "ApifyRunState", "DEFAULT_APIFY_ACTOR_ID", "STEP5_MIGRATION_VERSION",
    "apify_readiness", "build_apify_safe_input", "fetch_apify_dataset", "migrate_step5",
    "poll_apify_run", "preview_apify_cost_limits", "preview_apify_input", "resume_apify_run",
    "start_apify_live_run", "summarize_apify_run",
)
