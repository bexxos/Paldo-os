"""Deterministic, bounded Step 6 public-website enrichment.

The module is network-capable only through injected HTTP and DNS interfaces. Tests
use fictional routes and fake resolvers; no caller is allowed to bypass the
URL/DNS checks, page limits, robots check, or live-operation gates.
"""

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from html.parser import HTMLParser
import hashlib
import ipaddress
import os
import re
import socket
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional
from urllib.parse import unquote, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from urllib.error import HTTPError, URLError

from paldo_os_outbound import Database, normalize_domain, normalize_email
from step3_discovery import migrate_step5


STEP6_MIGRATION_VERSION = 8
MAX_HTML_PAGES = 5
MAX_PAGE_BYTES = 1024 * 1024
MAX_REDIRECTS = 3
REQUEST_TIMEOUT_SECONDS = 10
MIN_DOMAIN_INTERVAL_SECONDS = 1.0


class WebsiteEnrichmentBlockedError(RuntimeError):
    """Raised before any HTTP request when a live-enrichment gate fails."""


class WebsiteSafetyError(ValueError):
    """Raised for unsafe URL, DNS, redirect, or response conditions."""


class TemporaryDNSFailure(RuntimeError):
    """Injected DNS/MX adapter signal meaning the result is currently unknown."""


class EmailValidationState(str, Enum):
    VALID_SYNTAX_AND_MX = "VALID_SYNTAX_AND_MX"
    VALID_SYNTAX_MX_UNKNOWN = "VALID_SYNTAX_MX_UNKNOWN"
    VALID_SYNTAX_NO_MX = "VALID_SYNTAX_NO_MX"
    INVALID = "INVALID"
    SUPPRESSED = "SUPPRESSED"


class WebsiteRunState(str, Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ROBOTS_DENIED = "ROBOTS_DENIED"
    RENDER_REQUIRED = "RENDER_REQUIRED"
    BLOCKED = "BLOCKED"
    SOURCE_ERROR = "SOURCE_ERROR"


@dataclass
class WebsiteHTTPResponse:
    requested_url: str
    final_url: str
    status_code: int
    headers: Mapping[str, str]
    body: bytes
    redirects: tuple[str, ...] = ()


@dataclass(frozen=True)
class WebsitePage:
    url: str
    status: str
    content_hash: Optional[str]
    observed_at: str
    status_code: Optional[int] = None
    content_type: Optional[str] = None
    redirect_count: int = 0
    evidence_snippet: str = ""
    visible_text: str = ""
    anchors: tuple[Mapping[str, Any], ...] = ()
    has_form: bool = False
    public_email_candidates: tuple[Mapping[str, Any], ...] = ()

    @staticmethod
    def empty(url):
        return WebsitePage(url=url, status="UNCHECKED", content_hash=None, observed_at=_now())


@dataclass(frozen=True)
class WebsiteCrawlResult:
    state: str
    pages: tuple[WebsitePage, ...]
    website_url: str
    error_category: Optional[str] = None

    @property
    def fetched_pages(self):
        return tuple(page for page in self.pages if page.status == "FETCHED")

    def as_dict(self):
        return {
            "state": self.state,
            "website_url": self.website_url,
            "page_count": len(self.fetched_pages),
            "visited_count": len(self.pages),
            "error_category": self.error_category,
            "pages": [
                {
                    "url": page.url,
                    "status": page.status,
                    "content_hash": page.content_hash,
                    "observed_at": page.observed_at,
                    "status_code": page.status_code,
                    "content_type": page.content_type,
                    "redirect_count": page.redirect_count,
                    "evidence_snippet": page.evidence_snippet,
                }
                for page in self.pages
            ],
        }


def _now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _short_text(value, limit=280):
    text = " ".join(str(value or "").split())
    return text[:limit]


def _host(url):
    return (urlsplit(url).hostname or "").casefold().rstrip(".")


def _is_unsafe_ip(value):
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    return any((
        address.is_private,
        address.is_loopback,
        address.is_link_local,
        address.is_multicast,
        address.is_reserved,
        address.is_unspecified,
    ))


def _safe_resolved_addresses(dns_client, hostname):
    try:
        addresses = dns_client.resolve(hostname)
    except TemporaryDNSFailure:
        raise WebsiteSafetyError("dns_unknown") from None
    except Exception:
        raise WebsiteSafetyError("dns_unresolved") from None
    if not addresses:
        raise WebsiteSafetyError("dns_unresolved")
    for address in addresses:
        if _is_unsafe_ip(str(address)):
            raise WebsiteSafetyError("unsafe_destination")
    return tuple(str(address) for address in addresses)


def validate_website_url(url, *, dns_client=None):
    """Canonicalize and validate a public HTTP(S) URL before a request."""
    if not isinstance(url, str) or not url.strip():
        raise WebsiteSafetyError("url_invalid")
    parsed = urlsplit(url.strip())
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"}:
        raise WebsiteSafetyError("scheme_not_allowed")
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc.rsplit("}", 1)[-1]:
        raise WebsiteSafetyError("url_credentials_not_allowed")
    try:
        port = parsed.port
    except ValueError:
        raise WebsiteSafetyError("port_invalid") from None
    if port is not None and port not in {80, 443}:
        raise WebsiteSafetyError("port_not_allowed")
    hostname = parsed.hostname
    if not hostname:
        raise WebsiteSafetyError("host_missing")
    try:
        hostname = hostname.encode("idna").decode("ascii").casefold().rstrip(".")
    except UnicodeError:
        raise WebsiteSafetyError("host_invalid") from None
    blocked_names = {"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"}
    blocked_suffixes = (".local", ".internal", ".lan", ".home.arpa", ".localhost")
    if hostname in blocked_names or hostname.endswith(blocked_suffixes) or "." not in hostname:
        raise WebsiteSafetyError("internal_hostname")
    if _is_unsafe_ip(hostname):
        raise WebsiteSafetyError("unsafe_destination")
    resolver = dns_client or SocketDNSResolver()
    _safe_resolved_addresses(resolver, hostname)
    default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    netloc = hostname if port is None or default_port else f"{hostname}:{port}"
    return urlunsplit((scheme, netloc, parsed.path or "/", parsed.query, ""))


class SocketDNSResolver:
    """Minimal resolver interface; MX remains UNKNOWN without a DNS package."""

    def resolve(self, host):
        values = {item[4][0] for item in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)}
        return sorted(values)

    def lookup_mx(self, domain):
        return None


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, msg, headers, newurl):
        return None


class SafeWebsiteHTTPClient:
    """Bounded urllib client with no cookie jar and no JavaScript rendering."""

    def __init__(self, *, redirect_validator=None):
        self.opener = build_opener(_NoRedirectHandler())
        self.redirect_validator = redirect_validator

    def fetch(self, url, *, timeout_seconds, max_redirects):
        current = url
        redirects = []
        for _ in range(max_redirects + 1):
            request = Request(current, headers={
                "Accept": "text/html, text/plain;q=0.5",
                "User-Agent": "Paldo-OS bounded website enrichment",
            })
            try:
                response = self.opener.open(request, timeout=timeout_seconds)
                status = int(getattr(response, "status", response.getcode()))
                headers = dict(response.headers.items())
                body = response.read(MAX_PAGE_BYTES + 1)
            except HTTPError as error:
                status = int(error.code)
                headers = dict(error.headers.items()) if error.headers else {}
                location = headers.get("Location") or headers.get("location")
                if status in {301, 302, 303, 307, 308} and location:
                    next_url = urljoin(current, location)
                    if self.redirect_validator:
                        next_url = self.redirect_validator(next_url)
                    else:
                        return WebsiteHTTPResponse(url, current, status, headers, b"", (next_url,))
                    redirects.append(next_url)
                    if len(redirects) > max_redirects:
                        return WebsiteHTTPResponse(url, current, status, headers, b"", tuple(redirects))
                    current = next_url
                    continue
                body = error.read(MAX_PAGE_BYTES + 1)
            except (TimeoutError, socket.timeout):
                raise TimeoutError("website request timed out") from None
            except (URLError, OSError):
                raise RuntimeError("website source error") from None
            return WebsiteHTTPResponse(url, current, status, headers, body, tuple(redirects))
        return WebsiteHTTPResponse(url, current, 599, {}, b"", tuple(redirects))


class _PublicHTMLParser(HTMLParser):
    _hidden_tags = {"script", "style", "head", "noscript", "template", "svg"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._stack = []
        self._visible = []
        self._anchors = []
        self._active_anchor = None
        self.has_form = False
        self.mailto_emails = []

    @staticmethod
    def _attrs(attrs):
        return {str(key).casefold(): value for key, value in attrs}

    def handle_starttag(self, tag, attrs):
        tag = tag.casefold()
        attributes = self._attrs(attrs)
        parent_hidden = bool(self._stack and self._stack[-1][1])
        style = (attributes.get("style") or "").casefold().replace(" ", "")
        hidden = parent_hidden or tag in self._hidden_tags or "display:none" in style or "visibility:hidden" in style or "hidden" in attributes or attributes.get("aria-hidden", "").casefold() == "true"
        self._stack.append((tag, hidden))
        if tag == "form" and not hidden:
            self.has_form = True
        if tag == "a":
            self._active_anchor = {"href": attributes.get("href"), "text_parts": [], "hidden": hidden}
            href = attributes.get("href") or ""
            if not hidden and href.casefold().startswith("mailto:"):
                value = unquote(href[7:]).split("?", 1)[0].strip()
                if value:
                    self.mailto_emails.append(value)

    def handle_endtag(self, tag):
        tag = tag.casefold()
        while self._stack:
            current, _hidden = self._stack.pop()
            if current == tag:
                break
        if tag == "a" and self._active_anchor is not None:
            anchor = dict(self._active_anchor)
            anchor["text"] = _short_text(" ".join(anchor.pop("text_parts")), 180)
            anchor["hidden"] = bool(anchor.pop("hidden"))
            self._anchors.append(anchor)
            self._active_anchor = None

    def handle_data(self, data):
        if self._stack and self._stack[-1][1]:
            return
        text = " ".join(str(data).split())
        if not text:
            return
        self._visible.append(text)
        if self._active_anchor is not None:
            self._active_anchor["text_parts"].append(text)

    @property
    def visible_text(self):
        return _short_text(" ".join(self._visible), 100000)

    @property
    def anchors(self):
        result = []
        for anchor in self._anchors:
            href = anchor.get("href")
            if href:
                result.append({"href": href, "text": anchor.get("text", "")})
        return tuple(result)


def _parse_html(body):
    parser = _PublicHTMLParser()
    parser.feed(body.decode("utf-8", errors="replace"))
    parser.close()
    return parser


_EMAIL_RE = re.compile(r"[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+", re.IGNORECASE)
_ROLE_LOCAL_PARTS = {"info", "hello", "contact", "admin", "appointments", "appointment", "booking", "bookings", "reception", "office", "frontdesk", "support"}
_PLACEHOLDER_LOCAL_PARTS = {"example", "test", "user", "name", "email", "yourname", "your-email", "placeholder", "sample", "foo", "bar"}
_PLACEHOLDER_DOMAINS = {"example.com", "example.org", "example.net", "example.test", "domain.com", "test.com", "localhost"}


def _valid_email_syntax(value):
    if not isinstance(value, str) or not _EMAIL_RE.fullmatch(value):
        return False
    local, _, domain = value.rpartition("@")
    if not local or local.startswith(".") or local.endswith(".") or ".." in local:
        return False
    try:
        normalized_domain = normalize_domain(domain)
    except ValueError:
        return False
    if len(value) > 254 or any(label.startswith("-") or label.endswith("-") for label in normalized_domain.split(".")):
        return False
    return True


def _email_excluded(original, context):
    normalized = original.casefold()
    local, _, domain = normalized.partition("@")
    if not local or not domain or local in {"noreply", "no-reply", "donotreply", "do-not-reply"} or local.startswith("noreply"):
        return True
    if local in _PLACEHOLDER_LOCAL_PARTS or domain in _PLACEHOLDER_DOMAINS:
        return True
    lower_context = context.casefold()
    if any(word in lower_context for word in ("reviewer", "client", "customer", "client email", "leaked", "private contact")):
        return True
    if re.search(r"\.(?:png|jpe?g|gif|webp|svg)\b", lower_context):
        return True
    if any(token in lower_context for token in ("utm_", "tracking", "pixel")):
        return True
    return False


def _email_context(text, start, end, window=90):
    left = max(0, start - window)
    right = min(len(text), end + window)
    return _short_text(text[left:right], 280)


def extract_public_email_candidates(html, *, source_url, website_domain):
    """Extract only visible or mailto-published public business email candidates."""
    parser = _parse_html(html.encode("utf-8") if isinstance(html, str) else bytes(html))
    text = parser.visible_text
    mailto = {value.casefold() for value in parser.mailto_emails}
    found = {}
    for match in _EMAIL_RE.finditer(text):
        original = match.group(0)
        context = _email_context(text, match.start(), match.end())
        if not _valid_email_syntax(original) or _email_excluded(original, _email_context(text, match.start(), match.end(), window=45)):
            continue
        try:
            normalized = normalize_email(original)
            domain = normalize_domain(normalized.rsplit("@", 1)[1])
        except ValueError:
            continue
        if normalized in found:
            continue
        local = normalized.split("@", 1)[0]
        classification = "ROLE_BASED" if local in _ROLE_LOCAL_PARTS else "NAMED_PUBLIC"
        try:
            site_domain = normalize_domain(website_domain)
        except ValueError:
            site_domain = ""
        alignment = "ALIGNED" if domain == site_domain or domain.endswith("." + site_domain) else "MISMATCH"
        found[normalized] = {
            "original_email": original,
            "normalized_email": normalized,
            "source_url": source_url,
            "evidence_snippet": context,
            "observed_at": _now(),
            "classification": classification,
            "domain_alignment": alignment,
            "confidence": 0.95 if classification == "ROLE_BASED" else 0.80,
            "source_kind": "MAILTO" if normalized in mailto else "VISIBLE_TEXT",
        }
    for original in parser.mailto_emails:
        if not _valid_email_syntax(original):
            continue
        try:
            normalized = normalize_email(original)
        except ValueError:
            continue
        if normalized in found:
            continue
        context = _short_text("mailto " + original, 280)
        if _email_excluded(original, context):
            continue
        domain = normalized.rsplit("@", 1)[1]
        site_domain = normalize_domain(website_domain)
        local = normalized.split("@", 1)[0]
        found[normalized] = {
            "original_email": original,
            "normalized_email": normalized,
            "source_url": source_url,
            "evidence_snippet": context,
            "observed_at": _now(),
            "classification": "ROLE_BASED" if local in _ROLE_LOCAL_PARTS else "NAMED_PUBLIC",
            "domain_alignment": "ALIGNED" if domain == site_domain or domain.endswith("." + site_domain) else "MISMATCH",
            "confidence": 0.95 if local in _ROLE_LOCAL_PARTS else 0.80,
            "source_kind": "MAILTO",
        }
    return tuple(found.values())


def _positive(text, pattern):
    return re.search(pattern, text, re.IGNORECASE | re.DOTALL)


def _observation(text, pattern):
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return _email_context(text, match.start(), match.end())


_WORKFLOW_KEYS = (
    "appointment_channels",
    "public_business_email",
    "email_explicitly_used_for_booking",
    "website_booking_form",
    "internal_booking_page",
    "external_booking_provider_link",
    "phone_booking",
    "messenger_booking",
    "instagram_booking",
    "whatsapp_booking",
    "cancellation_route",
    "rescheduling_route",
    "deposit_policy",
    "no_show_policy",
    "reminders",
    "post_visit_follow_up",
    "recurring_appointments",
    "multiple_practitioners",
    "multiple_services",
    "multiple_locations",
    "out_of_hours_inquiry_option",
)


def extract_workflow_evidence(pages: Iterable[WebsitePage], *, website_domain):
    """Extract explicit factual signals; absent evidence remains UNCLEAR."""
    pages = tuple(pages)
    all_text = " ".join(page.visible_text for page in pages if page.status == "FETCHED")
    all_anchors = tuple(anchor for page in pages if page.status == "FETCHED" for anchor in page.anchors)
    facts = {}

    def add(key, value, text, pattern, negative_pattern=None):
        if key not in facts or facts[key]["signal_value"] != "YES":
            observation_pattern = pattern if value == "YES" else negative_pattern
            facts[key] = {
                "signal_key": key,
                "signal_value": value,
                "observation": _observation(text, observation_pattern) if observation_pattern else _short_text(text[:280]),
                "extracted_signal": {"key": key, "value": value},
                "possible_pain_hypothesis": None,
                "source_url": next((page.url for page in pages if page.status == "FETCHED" and text in page.visible_text), pages[0].url if pages else None),
                "observed_at": _now(),
            }

    negative_patterns = {
        "email_explicitly_used_for_booking": r"(?:do not|don't|not) (?:use|accept|take)[^.!?]{0,60}(?:email|e-mail)",
        "website_booking_form": r"(?:no|without) (?:online )?(?:booking|appointment form)|(?:do not|don't) offer[^.!?]{0,60}(?:booking|appointment)",
        "internal_booking_page": r"(?:no|without) internal (?:booking|appointment) page",
        "external_booking_provider_link": r"(?:no|without) external booking",
        "phone_booking": r"(?:do not|don't|not) (?:take|accept)[^.!?]{0,60}(?:phone|call|text) booking",
        "messenger_booking": r"(?:do not|don't|not) (?:take|accept)[^.!?]{0,60}messenger",
        "instagram_booking": r"(?:do not|don't|not) (?:take|accept)[^.!?]{0,60}instagram",
        "whatsapp_booking": r"(?:do not|don't|not) (?:take|accept)[^.!?]{0,60}whatsapp",
        "cancellation_route": r"(?:no|without) cancellation (?:route|option|policy)",
        "rescheduling_route": r"(?:no|without) reschedul",
        "deposit_policy": r"(?:no|without) deposit|deposit (?:is )?not required",
        "reminders": r"(?:no|without) (?:appointment )?reminders?",
        "post_visit_follow_up": r"(?:no|without) (?:post[- ]visit|follow[- ]up|aftercare)",
        "recurring_appointments": r"(?:no|without) recurring appointments?",
        "multiple_practitioners": r"only one practitioner|single practitioner",
        "multiple_services": r"only one service|single service",
        "multiple_locations": r"only one location|single location",
        "out_of_hours_inquiry_option": r"(?:no|without) (?:after[- ]hours|out[- ]of[- ]hours) (?:inquir|support|option)",
    }
    email_pattern = r"(?:email|e-mail)[^.!?]{0,80}(?:appointment|booking|schedule)"
    email_match = _EMAIL_RE.search(all_text) or next((page.public_email_candidates[0] for page in pages if page.status == "FETCHED" and page.public_email_candidates), None)
    add("public_business_email", "YES" if email_match else "UNCLEAR", all_text, _EMAIL_RE.pattern)
    email_value = "NO" if _positive(all_text, negative_patterns["email_explicitly_used_for_booking"]) else ("YES" if _positive(all_text, email_pattern) else "UNCLEAR")
    add("email_explicitly_used_for_booking", email_value, all_text, email_pattern, negative_patterns["email_explicitly_used_for_booking"])
    form_page = next((page for page in pages if page.status == "FETCHED" and page.has_form and _positive(page.visible_text, r"(?:book|booking|appointment|schedule|request)")), None)
    form_pattern = r"(?:book|booking|appointment|schedule|request)"
    form_value = "NO" if _positive(all_text, negative_patterns["website_booking_form"]) else ("YES" if form_page else "UNCLEAR")
    add("website_booking_form", form_value, form_page.visible_text if form_page else all_text, form_pattern, negative_patterns["website_booking_form"])
    internal = next((anchor for anchor in all_anchors if _positive(urlsplit(anchor.get("href", "")).path, r"(?:book|booking|appointment|schedule)")), None)
    internal_value = "NO" if _positive(all_text, negative_patterns["internal_booking_page"]) else ("YES" if internal else "UNCLEAR")
    add("internal_booking_page", internal_value, internal.get("text", "") if internal else all_text, r"(?:book|booking|appointment|schedule)", negative_patterns["internal_booking_page"])
    external = next((anchor for anchor in all_anchors if not anchor.get("same_site", True) and _positive((anchor.get("href", "") + " " + anchor.get("text", "")), r"(?:book|booking|appointment|schedule|calendly|acuity|mindbody|vagaro|booksy|fresha)")), None)
    external_pattern = r"(?:book|booking|appointment|schedule|calendly|acuity|mindbody|vagaro|booksy|fresha)"
    external_value = "NO" if _positive(all_text, negative_patterns["external_booking_provider_link"]) else ("YES" if external else "UNCLEAR")
    add("external_booking_provider_link", external_value, (external or {}).get("text", "") if external else all_text, external_pattern, negative_patterns["external_booking_provider_link"])
    phone_pattern = r"(?:call|phone|text)[^.!?]{0,60}(?:book|booking|appointment|schedule)|(?:book|booking|appointment|schedule)[^.!?]{0,60}(?:call|phone|text)"
    phone_value = "NO" if _positive(all_text, negative_patterns["phone_booking"]) else ("YES" if _positive(all_text, phone_pattern) else "UNCLEAR")
    add("phone_booking", phone_value, all_text, phone_pattern, negative_patterns["phone_booking"])
    for key, word in (("messenger_booking", "messenger"), ("instagram_booking", "instagram"), ("whatsapp_booking", "whatsapp")):
        pattern = rf"{word}[^.!?]{{0,60}}(?:book|booking|appointment|schedule)|(?:book|booking|appointment|schedule)[^.!?]{{0,60}}{word}"
        negative = negative_patterns[key]
        value = "NO" if _positive(all_text, negative) else ("YES" if _positive(all_text, pattern) else "UNCLEAR")
        add(key, value, all_text, pattern, negative)
    patterns = {
        "cancellation_route": r"cancel(?:lation)?",
        "rescheduling_route": r"reschedul",
        "deposit_policy": r"deposit",
        "no_show_policy": r"no[- ]show",
        "reminders": r"reminder|appointment confirmation",
        "post_visit_follow_up": r"post[- ]visit|follow[- ]up|aftercare|check[- ]in after",
        "recurring_appointments": r"recurring|repeat session|series of treatments|ongoing sessions",
        "multiple_practitioners": r"(?:our|meet|team of) (?:practitioners|doctors|therapists|nurses)|practitioners?\s*[:,-]",
        "multiple_services": r"(?:facial|laser|injectable|botox|dermal filler|treatment|service).{0,100}(?:facial|laser|injectable|botox|dermal filler|treatment|service)",
        "multiple_locations": r"(?:two|multiple|several) locations|locations?[^.!?]{0,80}(?:and|,)",
        "out_of_hours_inquiry_option": r"out[- ]of[- ]hours|after[- ]hours|24\s*/\s*7",
    }
    for key, pattern in patterns.items():
        negative = negative_patterns.get(key)
        value = "NO" if negative and _positive(all_text, negative) else ("YES" if _positive(all_text, pattern) else "UNCLEAR")
        add(key, value, all_text, pattern, negative)
    add("appointment_channels", "YES" if any(facts[key]["signal_value"] == "YES" for key in ("website_booking_form", "internal_booking_page", "external_booking_provider_link", "phone_booking", "messenger_booking", "instagram_booking", "whatsapp_booking")) else "UNCLEAR", all_text, r"(?:book|booking|appointment|schedule)")
    return tuple(facts[key] for key in _WORKFLOW_KEYS)


def validate_public_email(email, *, dns_client, website_domain=None, suppression_checker=None, duplicate_checker=None):
    """Validate syntax and mocked MX state; never probes SMTP."""
    result = {
        "original_email": email,
        "normalized_email": None,
        "domain": None,
        "state": EmailValidationState.INVALID.value,
        "dns_status": "NOT_CHECKED",
        "duplicate": False,
        "domain_alignment": "UNKNOWN",
        "confidence": 0.0,
    }
    if not isinstance(email, str) or not _valid_email_syntax(email.strip()):
        return result
    try:
        normalized = normalize_email(email.strip())
        domain = normalize_domain(normalized.rsplit("@", 1)[1])
    except ValueError:
        return result
    result["normalized_email"] = normalized
    result["domain"] = domain
    if website_domain:
        try:
            site_domain = normalize_domain(website_domain)
            result["domain_alignment"] = "ALIGNED" if domain == site_domain or domain.endswith("." + site_domain) else "MISMATCH"
        except ValueError:
            result["domain_alignment"] = "UNKNOWN"
    if suppression_checker and suppression_checker(normalized):
        result.update({"state": EmailValidationState.SUPPRESSED.value, "dns_status": "NOT_CHECKED", "confidence": 1.0})
        return result
    if duplicate_checker:
        result["duplicate"] = bool(duplicate_checker(normalized))
    try:
        mx = dns_client.lookup_mx(domain)
    except TemporaryDNSFailure:
        result.update({"state": EmailValidationState.VALID_SYNTAX_MX_UNKNOWN.value, "dns_status": "TEMPORARY_FAILURE", "confidence": 0.55})
        return result
    except Exception:
        result.update({"state": EmailValidationState.VALID_SYNTAX_MX_UNKNOWN.value, "dns_status": "UNKNOWN", "confidence": 0.50})
        return result
    if mx:
        result.update({"state": EmailValidationState.VALID_SYNTAX_AND_MX.value, "dns_status": "MX_PRESENT", "confidence": 0.90})
    else:
        result.update({"state": EmailValidationState.VALID_SYNTAX_NO_MX.value, "dns_status": "NO_MX", "confidence": 0.70})
    return result


def _robots_allows(body, path):
    groups = []
    agents = []
    rules = []
    active = False
    for raw_line in body.decode("utf-8", errors="replace").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        field, value = [part.strip() for part in line.split(":", 1)]
        field = field.casefold()
        if field == "user-agent":
            if active and agents and rules:
                groups.append((tuple(agents), tuple(rules)))
                agents, rules = [], []
            agents.append(value.casefold())
            active = True
        elif field in {"allow", "disallow"} and active:
            rules.append((field, value))
    if active and agents and rules:
        groups.append((tuple(agents), tuple(rules)))
    applicable = [rules for agents, rules in groups if "*" in agents]
    if not applicable:
        return True
    combined = [rule for rules in applicable for rule in rules]
    matches = [(len(value), field == "allow") for field, value in combined if value and path.startswith(value)]
    if not matches:
        return True
    return max(matches, key=lambda item: item[0])[1]


def _crawlable_link(url):
    path = urlsplit(url).path.casefold()
    return not path.endswith((
        ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".zip", ".rar", ".7z", ".tar", ".gz", ".mp4", ".mov", ".avi", ".exe", ".dmg",
    ))


def _link_priority(anchor, url):
    value = (anchor.get("text", "") + " " + urlsplit(url).path).casefold()
    groups = (
        (0, ("book", "booking", "appointment", "schedule")),
        (1, ("contact", "reach", "location")),
        (2, ("service", "treatment", "price")),
        (3, ("about", "team", "practitioner", "doctor")),
        (4, ("faq", "policy", "cancel", "reschedul", "terms", "deposit", "reminder")),
    )
    for priority, words in groups:
        if any(word in value for word in words):
            return priority
    return 99


def _page_with_status(url, status, *, response=None, snippet="", visible_text="", anchors=(), has_form=False, redirect_count=0, public_email_candidates=()):
    return WebsitePage(
        url=url,
        status=status,
        content_hash=hashlib.sha256(response.body).hexdigest() if response is not None else None,
        observed_at=_now(),
        status_code=response.status_code if response is not None else None,
        content_type=_header(response.headers, "content-type") if response is not None else None,
        redirect_count=redirect_count,
        evidence_snippet=snippet,
        visible_text=visible_text,
        anchors=anchors,
        has_form=has_form,
        public_email_candidates=tuple(public_email_candidates),
    )


def _header(headers, name):
    for key, value in (headers or {}).items():
        if str(key).casefold() == name.casefold():
            return str(value)
    return ""


def crawl_website(
    website_url,
    *,
    http_client=None,
    dns_client=None,
    max_pages=MAX_HTML_PAGES,
    max_page_bytes=MAX_PAGE_BYTES,
    max_redirects=MAX_REDIRECTS,
    timeout_seconds=REQUEST_TIMEOUT_SECONDS,
    min_interval_seconds=MIN_DOMAIN_INTERVAL_SECONDS,
    clock=time.monotonic,
    sleep=time.sleep,
):
    """Crawl one public site with deterministic page selection and bounded effects."""
    dns_client = dns_client or SocketDNSResolver()
    try:
        start_url = validate_website_url(website_url, dns_client=dns_client)
    except WebsiteSafetyError as error:
        page = _page_with_status(str(website_url), "UNSAFE_DESTINATION", snippet=str(error))
        return WebsiteCrawlResult(WebsiteRunState.BLOCKED.value, (page,), str(website_url), str(error))
    root_host = _host(start_url)

    def validate_redirect_target(url):
        redirect_url = validate_website_url(url, dns_client=dns_client)
        if _host(redirect_url) != root_host:
            raise WebsiteSafetyError("redirect to unrelated domain rejected")
        return redirect_url

    if http_client is None:
        http_client = SafeWebsiteHTTPClient(redirect_validator=validate_redirect_target)
    last_request = {}
    pages = []
    robots_body = b""

    def request_page(url, *, robots=False):
        nonlocal robots_body
        try:
            canonical = validate_website_url(url, dns_client=dns_client)
        except WebsiteSafetyError as error:
            status = "UNSAFE_DESTINATION" if not robots else "ROBOTS_UNAVAILABLE"
            return _page_with_status(url, status, snippet=str(error))
        if _host(canonical) != root_host:
            return _page_with_status(canonical, "UNSAFE_REDIRECT", snippet="unrelated domain rejected")
        elapsed = clock() - last_request.get(root_host, -float("inf"))
        if elapsed < min_interval_seconds:
            sleep(min_interval_seconds - elapsed)
        last_request[root_host] = clock()
        try:
            response = http_client.fetch(canonical, timeout_seconds=timeout_seconds, max_redirects=max_redirects)
        except (TimeoutError, socket.timeout):
            return _page_with_status(canonical, "TIMEOUT", snippet="request timeout")
        except WebsiteSafetyError as error:
            return _page_with_status(canonical, "UNSAFE_REDIRECT", snippet=str(error))
        except Exception:
            return _page_with_status(canonical, "SOURCE_ERROR", snippet="source request failed")
        redirects = tuple(response.redirects or ())
        if len(redirects) > max_redirects:
            return _page_with_status(canonical, "REDIRECT_LIMIT", response=response, redirect_count=len(redirects), snippet="redirect limit exceeded")
        for redirect in redirects:
            try:
                redirect_url = validate_website_url(urljoin(canonical, redirect), dns_client=dns_client)
            except WebsiteSafetyError as error:
                return _page_with_status(canonical, "UNSAFE_REDIRECT", response=response, redirect_count=len(redirects), snippet=str(error))
            if _host(redirect_url) != root_host:
                return _page_with_status(canonical, "UNSAFE_REDIRECT", response=response, redirect_count=len(redirects), snippet="redirect to unrelated domain rejected")
        try:
            final_url = validate_website_url(response.final_url or canonical, dns_client=dns_client)
        except WebsiteSafetyError as error:
            return _page_with_status(canonical, "UNSAFE_REDIRECT", response=response, redirect_count=len(redirects), snippet=str(error))
        if _host(final_url) != root_host:
            return _page_with_status(canonical, "UNSAFE_REDIRECT", response=response, redirect_count=len(redirects), snippet="final redirect to unrelated domain rejected")
        content_type = _header(response.headers, "content-type")
        if not robots and not content_type.casefold().split(";", 1)[0].strip() == "text/html":
            return _page_with_status(canonical, "NON_HTML", response=response, redirect_count=len(redirects), snippet="content type is not HTML")
        try:
            declared_size = int(_header(response.headers, "content-length")) if _header(response.headers, "content-length") else None
        except ValueError:
            declared_size = None
        if declared_size is not None and declared_size > max_page_bytes or len(response.body) > max_page_bytes:
            return _page_with_status(canonical, "PAGE_TOO_LARGE", response=response, redirect_count=len(redirects), snippet="response body exceeds limit")
        if robots:
            robots_body = bytes(response.body)
            return _page_with_status(canonical, "ROBOTS_FETCHED", response=response, redirect_count=len(redirects), snippet="robots.txt checked")
        parser = _parse_html(response.body)
        visible_text = parser.visible_text
        page_emails = extract_public_email_candidates(response.body, source_url=canonical, website_domain=root_host)
        if len(visible_text.strip()) < 20 and re.search(rb"<script\b|__next_data__|data-reactroot", response.body, re.IGNORECASE):
            return _page_with_status(canonical, "RENDER_REQUIRED", response=response, redirect_count=len(redirects), snippet="meaningful content requires JavaScript", visible_text=visible_text, anchors=parser.anchors, has_form=parser.has_form, public_email_candidates=page_emails)
        anchors = []
        for anchor in parser.anchors:
            href = anchor.get("href") or ""
            if href.casefold().startswith(("mailto:", "tel:", "javascript:")):
                continue
            absolute = urljoin(final_url, href)
            try:
                absolute = validate_website_url(absolute, dns_client=dns_client)
            except WebsiteSafetyError:
                continue
            anchors.append({
                "href": absolute,
                "text": anchor.get("text", ""),
                "same_site": _host(absolute) == root_host,
            })
        anchors.sort(key=lambda item: (_link_priority(item, item["href"]), item["href"], item["text"].casefold()))
        return _page_with_status(canonical, "FETCHED", response=response, redirect_count=len(redirects), snippet=_short_text(visible_text), visible_text=visible_text, anchors=tuple(anchors), has_form=parser.has_form, public_email_candidates=page_emails)

    robots_url = urlunsplit((urlsplit(start_url).scheme, urlsplit(start_url).netloc, "/robots.txt", "", ""))
    robots_page = request_page(robots_url, robots=True)
    pages.append(robots_page)
    if robots_page.status in {"UNSAFE_DESTINATION", "ROBOTS_UNAVAILABLE", "TIMEOUT", "SOURCE_ERROR"}:
        return WebsiteCrawlResult(WebsiteRunState.BLOCKED.value, tuple(pages), start_url, "robots_unavailable")
    if robots_page.status == "ROBOTS_FETCHED" and robots_page.status_code == 200:
        if not _robots_allows(robots_body, urlsplit(start_url).path or "/"):
            pages[-1] = replace(robots_page, status="ROBOTS_DENIED", evidence_snippet="robots.txt disallows this site")
            return WebsiteCrawlResult(WebsiteRunState.ROBOTS_DENIED.value, tuple(pages), start_url, "robots_denied")
    if robots_page.status not in {"ROBOTS_FETCHED", "ROBOTS_NOT_FOUND"} and robots_page.status_code != 404:
        return WebsiteCrawlResult(WebsiteRunState.BLOCKED.value, tuple(pages), start_url, robots_page.status.casefold())

    queue = [start_url]
    seen = set()
    while queue and len([page for page in pages if page.status == "FETCHED"]) < max_pages:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)
        page = request_page(url)
        pages.append(page)
        if page.status == "RENDER_REQUIRED":
            return WebsiteCrawlResult(WebsiteRunState.RENDER_REQUIRED.value, tuple(pages), start_url, "render_required")
        if page.status in {"UNSAFE_REDIRECT", "UNSAFE_DESTINATION"}:
            return WebsiteCrawlResult(WebsiteRunState.BLOCKED.value, tuple(pages), start_url, page.status.casefold())
        if page.status != "FETCHED":
            continue
        candidates = []
        for anchor in page.anchors:
            if not anchor.get("same_site"):
                continue
            absolute = anchor["href"]
            if absolute not in seen and absolute not in queue and _crawlable_link(absolute):
                candidates.append(anchor)
        queue.extend(item["href"] for item in sorted(candidates, key=lambda item: (_link_priority(item, item["href"]), item["href"], item["text"].casefold())))
    errors = [page.status for page in pages if page.status in {"TIMEOUT", "SOURCE_ERROR", "PAGE_TOO_LARGE", "NON_HTML", "REDIRECT_LIMIT"}]
    state = WebsiteRunState.SOURCE_ERROR.value if errors and not any(page.status == "FETCHED" for page in pages) else WebsiteRunState.COMPLETED.value
    return WebsiteCrawlResult(state, tuple(pages), start_url, errors[0].casefold() if errors else None)


_STEP6_SCHEMA = """
CREATE TABLE IF NOT EXISTS website_enrichment_runs (
    id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id) ON DELETE RESTRICT,
    campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE RESTRICT,
    website_url TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('DRY_RUN', 'LIVE')),
    state TEXT NOT NULL CHECK (state IN ('RUNNING', 'COMPLETED', 'ROBOTS_DENIED', 'RENDER_REQUIRED', 'BLOCKED', 'SOURCE_ERROR')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    page_count INTEGER NOT NULL DEFAULT 0,
    contact_count INTEGER NOT NULL DEFAULT 0,
    evidence_count INTEGER NOT NULL DEFAULT 0,
    error_category TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS website_enrichment_runs_candidate_idx ON website_enrichment_runs(candidate_id, id);
CREATE INDEX IF NOT EXISTS website_enrichment_runs_started_idx ON website_enrichment_runs(started_at);

CREATE TABLE IF NOT EXISTS website_enrichment_pages (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES website_enrichment_runs(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    content_hash TEXT,
    observed_at TEXT NOT NULL,
    status TEXT NOT NULL,
    status_code INTEGER,
    content_type TEXT,
    redirect_count INTEGER NOT NULL DEFAULT 0,
    evidence_snippet TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS website_enrichment_pages_run_url_idx ON website_enrichment_pages(run_id, url);

CREATE TABLE IF NOT EXISTS public_contact_candidates (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES website_enrichment_runs(id) ON DELETE CASCADE,
    candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id) ON DELETE RESTRICT,
    original_email TEXT NOT NULL,
    normalized_email TEXT NOT NULL,
    source_url TEXT NOT NULL,
    evidence_snippet TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    classification TEXT NOT NULL CHECK (classification IN ('ROLE_BASED', 'NAMED_PUBLIC')),
    domain_alignment TEXT NOT NULL CHECK (domain_alignment IN ('ALIGNED', 'MISMATCH', 'UNKNOWN')),
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    validation_state TEXT NOT NULL CHECK (validation_state IN ('VALID_SYNTAX_AND_MX', 'VALID_SYNTAX_MX_UNKNOWN', 'VALID_SYNTAX_NO_MX', 'INVALID', 'SUPPRESSED')),
    dns_status TEXT NOT NULL,
    duplicate_detected INTEGER NOT NULL DEFAULT 0 CHECK (duplicate_detected IN (0, 1))
);
CREATE UNIQUE INDEX IF NOT EXISTS public_contact_candidates_run_email_idx ON public_contact_candidates(run_id, normalized_email);

CREATE TABLE IF NOT EXISTS workflow_evidence (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES website_enrichment_runs(id) ON DELETE CASCADE,
    candidate_id INTEGER NOT NULL REFERENCES discovery_candidates(id) ON DELETE RESTRICT,
    signal_key TEXT NOT NULL,
    signal_value TEXT NOT NULL CHECK (signal_value IN ('YES', 'NO', 'UNCLEAR')),
    observation TEXT NOT NULL,
    extracted_signal TEXT NOT NULL,
    possible_pain_hypothesis TEXT,
    source_url TEXT,
    observed_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS workflow_evidence_run_signal_idx ON workflow_evidence(run_id, signal_key);
"""


def migrate_step6(database_or_path):
    """Apply the additive v8 website-enrichment migration idempotently."""
    if hasattr(database_or_path, "connection"):
        database = database_or_path
        close_after = False
    else:
        database = Database(Path(database_or_path))
        close_after = True
    try:
        migrate_step5(database)
        with database.connection:
            database.connection.executescript(_STEP6_SCHEMA)
            database.connection.execute(
                """INSERT OR IGNORE INTO system_config (key, value, value_type)
                   VALUES ('website_enrichment_daily_quota', '0', 'integer')"""
            )
            database.connection.execute(
                """INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
                   VALUES (?, ?, ?)""",
                (STEP6_MIGRATION_VERSION, "step6_bounded_website_enrichment", _now()),
            )
        return STEP6_MIGRATION_VERSION
    finally:
        if close_after:
            database.close()


def _config(database):
    values = database.read_config()
    return values, int(values.get("website_enrichment_daily_quota", 0))


def _run_count_today(database):
    today = _now()[:10]
    return database.connection.execute(
        "SELECT COUNT(*) FROM website_enrichment_runs WHERE mode='LIVE' AND started_at LIKE ?", (today + "%",)
    ).fetchone()[0]


def website_enrichment_readiness(database, *, campaign_id=None):
    values, quota = _config(database)
    campaign = database.get_campaign(campaign_id) if campaign_id is not None else None
    used = _run_count_today(database) if _table_exists(database, "website_enrichment_runs") else 0
    gates = {
        "system_not_paused": values.get("system_state") != "PAUSED",
        "discovery_live": values.get("discovery_mode") == "LIVE",
        "campaign_active": bool(campaign and campaign["status"] == "ACTIVE"),
        "positive_daily_website_quota": quota > used,
    }
    return {
        "ready": all(gates.values()),
        "gates": gates,
        "daily_website_enrichment_quota": quota,
        "runs_used_today": used,
        "remaining_quota": max(0, quota - used),
        "campaign_id": campaign["id"] if campaign else None,
        "mode": values.get("discovery_mode"),
        "system_state": values.get("system_state"),
        "credential_value_returned": False,
    }


def _table_exists(database, name):
    return database.connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _candidate_for_enrichment(database, candidate_id):
    return database.connection.execute("SELECT * FROM discovery_candidates WHERE id=?", (candidate_id,)).fetchone()


def _gate_enrichment(database, candidate_id, campaign_id, *, operator_confirmation, dns_client):
    values, quota = _config(database)
    if values.get("system_state") == "PAUSED":
        raise WebsiteEnrichmentBlockedError("PAUSED system blocks website enrichment")
    if values.get("discovery_mode") != "LIVE":
        raise WebsiteEnrichmentBlockedError("discovery_mode must be LIVE")
    campaign = database.get_campaign(campaign_id)
    if not campaign or campaign["status"] != "ACTIVE":
        raise WebsiteEnrichmentBlockedError("active campaign required")
    if quota <= _run_count_today(database):
        raise WebsiteEnrichmentBlockedError("website enrichment quota is exhausted")
    if operator_confirmation is not True:
        raise WebsiteEnrichmentBlockedError("explicit operator confirmation required")
    candidate = _candidate_for_enrichment(database, candidate_id)
    if candidate is None:
        raise WebsiteEnrichmentBlockedError("candidate not found")
    if candidate["ingestion_status"] not in {"ACCEPTED", "HELD"}:
        raise WebsiteEnrichmentBlockedError("candidate must be ACCEPTED or HELD")
    if not candidate["website_url"]:
        raise WebsiteEnrichmentBlockedError("safe website is required")
    try:
        website_url = validate_website_url(candidate["website_url"], dns_client=dns_client)
    except WebsiteSafetyError as error:
        raise WebsiteEnrichmentBlockedError(f"unsafe website: {error}") from None
    return candidate, website_url


def _stored_email_duplicate(database, normalized_email):
    return database.connection.execute(
        "SELECT 1 FROM leads WHERE email=? UNION SELECT 1 FROM public_contact_candidates WHERE normalized_email=? LIMIT 1",
        (normalized_email, normalized_email),
    ).fetchone() is not None


def enrich_website_candidate(
    database,
    candidate_id,
    campaign_id,
    *,
    operator_confirmation=False,
    http_client=None,
    dns_client=None,
    clock=time.monotonic,
    sleep=time.sleep,
):
    """Run one gated bounded enrichment and persist only hashes/snippets/signals."""
    if not _table_exists(database, "website_enrichment_runs"):
        raise RuntimeError("Step 6 migration has not been applied")
    dns_client = dns_client or SocketDNSResolver()
    candidate, website_url = _gate_enrichment(
        database, candidate_id, campaign_id,
        operator_confirmation=operator_confirmation,
        dns_client=dns_client,
    )
    timestamp = _now()
    with database.connection:
        cursor = database.connection.execute(
            """INSERT INTO website_enrichment_runs
               (candidate_id, campaign_id, website_url, mode, state, started_at, created_at, updated_at)
               VALUES (?, ?, ?, 'LIVE', 'RUNNING', ?, ?, ?)""",
            (candidate_id, campaign_id, website_url, timestamp, timestamp, timestamp),
        )
        run_id = cursor.lastrowid
    crawl = crawl_website(website_url, http_client=http_client, dns_client=dns_client, clock=clock, sleep=sleep)
    domain = _host(website_url)
    contacts = []
    for page in crawl.fetched_pages:
        contacts.extend(page.public_email_candidates)
    # Only parsed public contact candidates cross the crawl/persistence boundary;
    # raw HTML is never retained in the page model or database.
    evidence = extract_workflow_evidence(crawl.fetched_pages, website_domain=domain)
    with database.connection:
        for page in crawl.pages:
            database.connection.execute(
                """INSERT INTO website_enrichment_pages
                   (run_id, url, content_hash, observed_at, status, status_code, content_type,
                    redirect_count, evidence_snippet)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, page.url, page.content_hash, page.observed_at, page.status, page.status_code, page.content_type, page.redirect_count, page.evidence_snippet),
            )
        unique_contacts = {}
        for contact in contacts:
            unique_contacts[contact["normalized_email"]] = contact
        for contact in unique_contacts.values():
            validation = validate_public_email(
                contact["normalized_email"],
                dns_client=dns_client,
                website_domain=domain,
                suppression_checker=lambda value: database.is_suppressed(email=value) or database.is_suppressed(domain=value.rsplit("@", 1)[1]),
                duplicate_checker=lambda value: _stored_email_duplicate(database, value),
            )
            database.connection.execute(
                """INSERT INTO public_contact_candidates
                   (run_id, candidate_id, original_email, normalized_email, source_url, evidence_snippet,
                    observed_at, classification, domain_alignment, confidence, validation_state,
                    dns_status, duplicate_detected)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, candidate_id, contact["original_email"], contact["normalized_email"], contact["source_url"], contact["evidence_snippet"], contact["observed_at"], contact["classification"], validation["domain_alignment"], validation["confidence"], validation["state"], validation["dns_status"], 1 if validation["duplicate"] else 0),
            )
        for item in evidence:
            database.connection.execute(
                """INSERT INTO workflow_evidence
                   (run_id, candidate_id, signal_key, signal_value, observation,
                    extracted_signal, possible_pain_hypothesis, source_url, observed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, candidate_id, item["signal_key"], item["signal_value"], item["observation"], str(item["extracted_signal"]), item["possible_pain_hypothesis"], item["source_url"], item["observed_at"]),
            )
        completed = _now()
        database.connection.execute(
            """UPDATE website_enrichment_runs
               SET state=?, completed_at=?, page_count=?, contact_count=?, evidence_count=?, error_category=?, updated_at=?
               WHERE id=?""",
            (crawl.state, completed, len(crawl.fetched_pages), len(unique_contacts), len(evidence), crawl.error_category, completed, run_id),
        )
    return {
        "run_id": run_id,
        "candidate_id": candidate_id,
        "campaign_id": campaign_id,
        "website_url": website_url,
        "state": crawl.state,
        "page_count": len(crawl.fetched_pages),
        "contact_count": len(unique_contacts),
        "evidence_count": len(evidence),
        "error_category": crawl.error_category,
    }


__all__ = [
    "EmailValidationState",
    "MAX_HTML_PAGES",
    "MAX_PAGE_BYTES",
    "MAX_REDIRECTS",
    "MIN_DOMAIN_INTERVAL_SECONDS",
    "REQUEST_TIMEOUT_SECONDS",
    "SafeWebsiteHTTPClient",
    "SocketDNSResolver",
    "TemporaryDNSFailure",
    "WebsiteCrawlResult",
    "WebsiteEnrichmentBlockedError",
    "WebsiteSafetyError",
    "WebsiteHTTPResponse",
    "WebsitePage",
    "WebsiteRunState",
    "crawl_website",
    "enrich_website_candidate",
    "extract_public_email_candidates",
    "extract_workflow_evidence",
    "migrate_step6",
    "validate_public_email",
    "validate_website_url",
    "website_enrichment_readiness",
]
