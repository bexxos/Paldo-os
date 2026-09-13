import tempfile
import unittest
from pathlib import Path

from paldo_os_outbound import Database
from step3_discovery import APIFY_SOURCE, DiscoveryMode, ingest_candidate, normalize_candidate
from step6_website_enrichment import (
    EmailValidationState,
    WebsiteHTTPResponse,
    WebsitePage,
    WebsiteRunState,
    TemporaryDNSFailure,
    WebsiteEnrichmentBlockedError,
    crawl_website,
    enrich_website_candidate,
    extract_public_email_candidates,
    extract_workflow_evidence,
    migrate_step6,
    validate_public_email,
    validate_website_url,
    website_enrichment_readiness,
)


class FakeDNS:
    def __init__(self, answers=None, mx_answers=None):
        self.answers = dict(answers or {})
        self.mx_answers = dict(mx_answers or {})
        self.resolve_calls = []
        self.mx_calls = []

    def resolve(self, host):
        self.resolve_calls.append(host)
        value = self.answers.get(host, ["93.184.216.34"])
        if isinstance(value, list) and value and isinstance(value[0], (list, tuple)):
            return list(value.pop(0))
        if isinstance(value, Exception):
            raise value
        return list(value)

    def lookup_mx(self, domain):
        self.mx_calls.append(domain)
        value = self.mx_answers.get(domain, ["mail." + domain])
        if isinstance(value, Exception):
            raise value
        return list(value)


class FakeHTTPClient:
    def __init__(self, routes=None):
        self.routes = dict(routes or {})
        self.calls = []
        self.active = 0
        self.max_active = 0

    def fetch(self, url, *, timeout_seconds, max_redirects):
        self.calls.append({"url": url, "timeout_seconds": timeout_seconds, "max_redirects": max_redirects})
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            route = self.routes.get(url)
            if isinstance(route, Exception):
                raise route
            if callable(route):
                route = route(url)
            if route is None:
                return WebsiteHTTPResponse(
                    requested_url=url,
                    final_url=url,
                    status_code=404,
                    headers={"content-type": "text/plain"},
                    body=b"not found",
                )
            return route
        finally:
            self.active -= 1


class FakeClock:
    def __init__(self):
        self.current = 100.0
        self.sleeps = []

    def now(self):
        return self.current

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.current += seconds


def html_response(url, html, *, final_url=None, redirects=(), content_type="text/html; charset=utf-8"):
    return WebsiteHTTPResponse(
        requested_url=url,
        final_url=final_url or url,
        status_code=200,
        headers={"content-type": content_type, "content-length": str(len(html.encode("utf-8")))},
        body=html.encode("utf-8"),
        redirects=tuple(redirects),
    )


def fixture_site():
    root = "https://fictional-business.test"
    return {
        root + "/robots.txt": WebsiteHTTPResponse(
            requested_url=root + "/robots.txt",
            final_url=root + "/robots.txt",
            status_code=404,
            headers={"content-type": "text/plain"},
            body=b"not found",
        ),
        root + "/": html_response(
            root + "/",
            """
            <html><head><meta name='email' content='hidden@example.test'></head><body>
            <h1>Fictional Business</h1>
            <p>Appointments: email <a href='mailto:appointments@fictional-business.test'>appointments@fictional-business.test</a>.</p>
            <p>Call 555-010-2000 for booking.</p>
            <a href='/contact'>Contact</a>
            <a href='/book'>Book an appointment</a>
            <a href='/services'>Services</a>
            <a href='/team'>Our team</a>
            <a href='https://external.test/track'>External</a>
            <a href='/brochure.pdf'>PDF</a>
            </body></html>
            """,
        ),
        root + "/contact": html_response(
            root + "/contact",
            "<html><body><h1>Contact</h1><p>General office: info@fictional-business.test</p><p>Phone booking is available.</p><p>Message us on Messenger, Instagram, or WhatsApp for booking.</p></body></html>",
        ),
        root + "/book": html_response(
            root + "/book",
            """
            <html><body><h1>Book an appointment</h1>
            <form action='/book' method='post'><input name='preferred_time'></form>
            <p>Use our internal booking form. Cancellation and rescheduling are handled by reception.</p>
            <p>A deposit is required. No-show appointments may be charged.</p>
            <p>Appointment reminders are sent. We offer post-visit follow-up and recurring treatment sessions.</p>
            <a href='https://booking-provider.test/fictional'>Book with our booking provider</a>
            </body></html>
            """,
        ),
        root + "/services": html_response(
            root + "/services",
            "<html><body><h1>Services</h1><p>We offer facials, laser treatments, and injectables.</p><a href='/faq'>FAQ and policies</a></body></html>",
        ),
        root + "/team": html_response(
            root + "/team",
            "<html><body><h1>Meet our practitioners</h1><p>Dr Ana Smith and Dr Ben Jones welcome clients at two locations.</p><p>Out-of-hours inquiries can be left for the office.</p></body></html>",
        ),
        root + "/faq": html_response(
            root + "/faq",
            "<html><body><h1>FAQ</h1><p>Questions about appointments and cancellation policy.</p></body></html>",
        ),
    }


class Step6WebsiteEnrichmentTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.database = Database(Path(self.tempdir.name) / "step6.sqlite3")
        migrate_step6(self.database)
        self.addCleanup(self.database.close)
        self.campaign = self.database.get_campaign_by_name("Secondary region local services")

    def set_live_quota(self, quota=1):
        with self.database.connection:
            self.database.connection.execute("UPDATE system_config SET value='ACTIVE' WHERE key='system_state'")
            self.database.connection.execute("UPDATE system_config SET value='LIVE' WHERE key='discovery_mode'")
            self.database.connection.execute("UPDATE system_config SET value=? WHERE key='website_enrichment_daily_quota'", (str(quota),))
            self.database.connection.execute("UPDATE campaigns SET status='INACTIVE'")
            self.database.connection.execute("UPDATE campaigns SET status='ACTIVE' WHERE id=?", (self.campaign["id"],))

    def add_candidate(self, *, website="https://fictional-business.test", email=None):
        record = {
            "source_record_id": "fictional-place-" + str(self.database.connection.execute("SELECT COUNT(*) FROM discovery_candidates").fetchone()[0] + 1),
            "business_name": "Fictional Business",
            "business_category": "Local service business",
            "website_url": website,
            "public_business_phone": "+1 555 010 2000",
            "country": "US",
            "city": "Testville",
        }
        if email:
            record["public_business_email"] = email
        candidate = normalize_candidate(record, source=APIFY_SOURCE, mode=DiscoveryMode.DRY_RUN, provenance_type="FIXTURE")
        result = ingest_candidate(self.database, candidate)
        return result.candidate_id

    def test_migration_is_idempotent_and_safe_defaults_are_preserved(self):
        migrate_step6(self.database)
        tables = {
            row[0]
            for row in self.database.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        self.assertTrue({"website_enrichment_runs", "website_enrichment_pages", "public_contact_candidates", "workflow_evidence"}.issubset(tables))
        self.assertEqual(self.database.read_config()["system_state"], "PAUSED")
        self.assertEqual(self.database.read_config()["discovery_mode"], "DRY_RUN")
        self.assertEqual(self.database.read_config()["website_enrichment_daily_quota"], 0)

    def test_safe_same_domain_crawl_is_bounded_deterministic_and_rate_limited(self):
        http = FakeHTTPClient(fixture_site())
        dns = FakeDNS()
        clock = FakeClock()
        result = crawl_website("https://fictional-business.test/", http_client=http, dns_client=dns, clock=clock.now, sleep=clock.sleep)
        self.assertEqual(result.state, WebsiteRunState.COMPLETED.value)
        html_pages = [page for page in result.pages if page.status == "FETCHED"]
        self.assertLessEqual(len(html_pages), 5)
        self.assertEqual(http.max_active, 1)
        self.assertTrue(clock.sleeps)
        self.assertTrue(all(seconds >= 1 for seconds in clock.sleeps))
        self.assertNotIn("https://external.test/track", [call["url"] for call in http.calls])
        self.assertNotIn("/brochure.pdf", [call["url"] for call in http.calls])
        self.assertEqual([page.url for page in html_pages[:3]], [
            "https://fictional-business.test/",
            "https://fictional-business.test/book",
            "https://fictional-business.test/contact",
        ])

    def test_robots_denial_stops_before_homepage(self):
        root = "https://fictional-business.test"
        http = FakeHTTPClient({
            root + "/robots.txt": WebsiteHTTPResponse(root + "/robots.txt", root + "/robots.txt", 200, {"content-type": "text/plain"}, b"User-agent: *\nDisallow: /\n"),
            root + "/": html_response(root + "/", "<html><body>Home</body></html>"),
        })
        result = crawl_website(root + "/", http_client=http, dns_client=FakeDNS())
        self.assertEqual(result.state, WebsiteRunState.ROBOTS_DENIED.value)
        self.assertEqual([call["url"] for call in http.calls], [root + "/robots.txt"])
        self.assertEqual(result.pages[-1].status, "ROBOTS_DENIED")

    def test_page_size_redirect_and_timeout_are_safe_page_outcomes(self):
        root = "https://fictional-business.test"
        cases = (
            ("PAGE_TOO_LARGE", html_response(root + "/", "x", content_type="text/html"), {"content-length": "1048577"}),
            ("REDIRECT_LIMIT", WebsiteHTTPResponse(root + "/", root + "/final", 200, {"content-type": "text/html"}, b"<html><body>ok</body></html>", redirects=(root + "/1", root + "/2", root + "/3", root + "/4")), None),
            ("TIMEOUT", TimeoutError("timeout"), None),
        )
        for expected, response, headers in cases:
            with self.subTest(expected=expected):
                if headers:
                    response.headers.update(headers)
                http = FakeHTTPClient({root + "/robots.txt": WebsiteHTTPResponse(root + "/robots.txt", root + "/robots.txt", 404, {"content-type": "text/plain"}, b""), root + "/": response})
                result = crawl_website(root + "/", http_client=http, dns_client=FakeDNS())
                self.assertEqual(result.pages[-1].status, expected)

    def test_ssrf_destinations_credentials_and_unsupported_ports_are_rejected(self):
        for url in (
            "http://localhost/",
            "http://127.0.0.1/",
            "http://169.254.169.254/",
            "http://[::1]/",
            "http://business.test:8080/",
            "https://user:password@business.test/",
            "file:///tmp/site",
        ):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    validate_website_url(url, dns_client=FakeDNS())

    def test_unsafe_unrelated_redirect_is_rejected_before_external_fetch(self):
        root = "https://fictional-business.test"
        http = FakeHTTPClient({
            root + "/robots.txt": WebsiteHTTPResponse(root + "/robots.txt", root + "/robots.txt", 404, {"content-type": "text/plain"}, b""),
            root + "/": html_response(root + "/", "<html><body>home</body></html>", final_url="https://other.test/home", redirects=("https://other.test/home",)),
        })
        result = crawl_website(root + "/", http_client=http, dns_client=FakeDNS())
        self.assertEqual(result.pages[-1].status, "UNSAFE_REDIRECT")
        self.assertNotIn("https://other.test/home", [call["url"] for call in http.calls])

    def test_dns_rebinding_is_blocked_on_request_revalidation(self):
        root = "https://fictional-business.test"
        dns = FakeDNS({"fictional-business.test": [["93.184.216.34"], ["93.184.216.34"], ["127.0.0.1"]]})
        http = FakeHTTPClient({root + "/robots.txt": WebsiteHTTPResponse(root + "/robots.txt", root + "/robots.txt", 404, {"content-type": "text/plain"}, b""), root + "/": html_response(root + "/", "<html><body>home</body></html>")})
        result = crawl_website(root + "/", http_client=http, dns_client=dns)
        self.assertEqual(result.pages[-1].status, "UNSAFE_DESTINATION")
        self.assertEqual([call["url"] for call in http.calls], [root + "/robots.txt"])

    def test_non_html_and_javascript_only_content_produce_safe_outcomes(self):
        root = "https://fictional-business.test"
        not_html = FakeHTTPClient({root + "/robots.txt": WebsiteHTTPResponse(root + "/robots.txt", root + "/robots.txt", 404, {"content-type": "text/plain"}, b""), root + "/": html_response(root + "/", "PDF", content_type="application/pdf")})
        result = crawl_website(root + "/", http_client=not_html, dns_client=FakeDNS())
        self.assertEqual(result.pages[-1].status, "NON_HTML")
        js_only = FakeHTTPClient({root + "/robots.txt": WebsiteHTTPResponse(root + "/robots.txt", root + "/robots.txt", 404, {"content-type": "text/plain"}, b""), root + "/": html_response(root + "/", "<html><head><script>renderApp()</script></head><body><div id='app'></div></body></html>")})
        result = crawl_website(root + "/", http_client=js_only, dns_client=FakeDNS())
        self.assertEqual(result.state, WebsiteRunState.RENDER_REQUIRED.value)
        self.assertEqual(result.pages[-1].status, "RENDER_REQUIRED")

    def test_public_email_extraction_is_visible_role_aware_and_excludes_false_positives(self):
        html = """
        <html><head><meta name='email' content='hidden@example.test'></head><body>
        <p>Appointments: <a href='mailto:Appointments@Business.test?subject=booking'>Appointments@Business.test</a></p>
        <p>Owner contact: dr.ana@business.test</p>
        <p>Gmail office: business.office@gmail.com</p>
        <a href='mailto:contact@business.test'></a>
        <p>noreply@noreply.test example@example.com client@example.org reviewer@business.test</p>
        <span style='display:none'>hidden@business.test</span>
        <script>leaked@business.test</script>
        </body></html>
        """
        found = extract_public_email_candidates(html, source_url="https://business.test/contact", website_domain="business.test")
        normalized = {item["normalized_email"]: item for item in found}
        self.assertEqual(set(normalized), {"appointments@business.test", "dr.ana@business.test", "business.office@gmail.com", "contact@business.test"})
        self.assertEqual(normalized["appointments@business.test"]["classification"], "ROLE_BASED")
        self.assertEqual(normalized["dr.ana@business.test"]["classification"], "NAMED_PUBLIC")
        self.assertEqual(normalized["business.office@gmail.com"]["domain_alignment"], "MISMATCH")
        self.assertTrue(all(item["evidence_snippet"] for item in found))

    def test_email_validation_supports_mx_missing_temporary_failure_suppression_and_duplicate(self):
        dns = FakeDNS(mx_answers={"business.test": ["mail.business.test"], "nomx.test": [], "temporary.test": TemporaryDNSFailure("temporary")})
        valid = validate_public_email("info@business.test", dns_client=dns)
        self.assertEqual(valid["state"], EmailValidationState.VALID_SYNTAX_AND_MX.value)
        self.assertEqual(validate_public_email("info@nomx.test", dns_client=dns)["state"], EmailValidationState.VALID_SYNTAX_NO_MX.value)
        self.assertEqual(validate_public_email("info@temporary.test", dns_client=dns)["state"], EmailValidationState.VALID_SYNTAX_MX_UNKNOWN.value)
        self.assertEqual(validate_public_email("not-an-email", dns_client=dns)["state"], EmailValidationState.INVALID.value)
        suppressed = validate_public_email("info@business.test", dns_client=dns, suppression_checker=lambda value: value == "info@business.test")
        self.assertEqual(suppressed["state"], EmailValidationState.SUPPRESSED.value)
        duplicate = validate_public_email("info@business.test", dns_client=dns, duplicate_checker=lambda value: True)
        self.assertTrue(duplicate["duplicate"])
        self.assertNotIn("smtp", repr(duplicate).casefold())

    def test_workflow_evidence_is_explicit_tri_state_and_does_not_invent_pain(self):
        pages = [
            WebsitePage(
                url="https://business.test/book",
                status="FETCHED",
                content_hash="hash",
                observed_at="2026-09-02T00:00:00+00:00",
                status_code=200,
                content_type="text/html",
                redirect_count=0,
                evidence_snippet="Book online. Cancellation, rescheduling, deposit, no-show, reminders, and follow-up policy are listed.",
                visible_text="Book an appointment through our form. Call us for booking. We also use Messenger and WhatsApp for booking. Cancellation and rescheduling are handled by reception. A deposit is required. No-show appointments may be charged. Appointment reminders and post-visit follow-up are provided.",
                anchors=(
                    {"href": "https://business.test/book", "text": "Book appointment", "same_site": True},
                    {"href": "https://booking-provider.test/business", "text": "Book with our booking provider", "same_site": False},
                ),
                has_form=True,
            )
        ]
        evidence = extract_workflow_evidence(pages, website_domain="business.test")
        by_key = {item["signal_key"]: item for item in evidence}
        for key in ("website_booking_form", "internal_booking_page", "external_booking_provider_link", "cancellation_route", "rescheduling_route", "deposit_policy", "no_show_policy", "reminders", "post_visit_follow_up", "phone_booking", "messenger_booking", "whatsapp_booking"):
            self.assertEqual(by_key[key]["signal_value"], "YES")
        self.assertIsNone(by_key["website_booking_form"]["possible_pain_hypothesis"])
        missing = extract_workflow_evidence([WebsitePage.empty("https://business.test/")], website_domain="business.test")
        self.assertTrue(all(item["signal_value"] == "UNCLEAR" for item in missing))
        negative = extract_workflow_evidence([WebsitePage(url="https://business.test/", status="FETCHED", content_hash="hash", observed_at="2026-09-02T00:00:00+00:00", visible_text="We do not offer online booking. No deposit is required.")], website_domain="business.test")
        negative_by_key = {item["signal_key"]: item for item in negative}
        self.assertEqual(negative_by_key["website_booking_form"]["signal_value"], "NO")
        self.assertEqual(negative_by_key["deposit_policy"]["signal_value"], "NO")

    def test_live_gates_require_safe_candidate_and_positive_quota_without_http_call(self):
        candidate_id = self.add_candidate()
        http = FakeHTTPClient(fixture_site())
        dns = FakeDNS()
        with self.assertRaisesRegex(WebsiteEnrichmentBlockedError, "PAUSED"):
            enrich_website_candidate(self.database, candidate_id, self.campaign["id"], operator_confirmation=True, http_client=http, dns_client=dns)
        self.assertEqual(http.calls, [])
        self.set_live_quota(0)
        with self.assertRaisesRegex(WebsiteEnrichmentBlockedError, "quota"):
            enrich_website_candidate(self.database, candidate_id, self.campaign["id"], operator_confirmation=True, http_client=http, dns_client=dns)
        self.assertEqual(http.calls, [])

    def test_enrichment_persists_bounded_pages_contacts_validation_and_evidence_without_touching_leads(self):
        candidate_id = self.add_candidate()
        lead_count_before = self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        self.set_live_quota(1)
        http = FakeHTTPClient(fixture_site())
        dns = FakeDNS(mx_answers={"fictional-business.test": ["mail.fictional-business.test"]})
        result = enrich_website_candidate(
            self.database,
            candidate_id,
            self.campaign["id"],
            operator_confirmation=True,
            http_client=http,
            dns_client=dns,
        )
        self.assertEqual(result["state"], WebsiteRunState.COMPLETED.value)
        self.assertLessEqual(result["page_count"], 5)
        contacts = self.database.connection.execute("SELECT * FROM public_contact_candidates").fetchall()
        self.assertTrue(any(row["normalized_email"] == "appointments@fictional-business.test" for row in contacts))
        self.assertTrue(any(row["validation_state"] == EmailValidationState.VALID_SYNTAX_AND_MX.value for row in contacts))
        evidence = self.database.connection.execute("SELECT * FROM workflow_evidence").fetchall()
        self.assertTrue(any(row["signal_key"] == "cancellation_route" and row["signal_value"] == "YES" for row in evidence))
        self.assertTrue(all(row["possible_pain_hypothesis"] is None for row in evidence))
        page_columns = {row[1] for row in self.database.connection.execute("PRAGMA table_info(website_enrichment_pages)")}
        self.assertNotIn("body", page_columns)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM leads").fetchone()[0], lead_count_before)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM qualification_results").fetchone()[0], 0)
        self.assertEqual(self.database.connection.execute("SELECT COUNT(*) FROM outreach").fetchone()[0], 0)

    def test_held_candidate_is_eligible_but_unsafe_website_is_blocked(self):
        candidate_id = self.add_candidate()
        with self.database.connection:
            self.database.connection.execute("UPDATE discovery_candidates SET ingestion_status='HELD', ingestion_outcome='POSSIBLE_DUPLICATE_HOLD' WHERE id=?", (candidate_id,))
        self.set_live_quota(2)
        result = enrich_website_candidate(self.database, candidate_id, self.campaign["id"], operator_confirmation=True, http_client=FakeHTTPClient(fixture_site()), dns_client=FakeDNS())
        self.assertEqual(result["state"], WebsiteRunState.COMPLETED.value)
        unsafe_id = self.add_candidate(website="http://127.0.0.1/")
        with self.assertRaisesRegex(WebsiteEnrichmentBlockedError, "unsafe"):
            enrich_website_candidate(self.database, unsafe_id, self.campaign["id"], operator_confirmation=True, http_client=FakeHTTPClient(), dns_client=FakeDNS())

    def test_readiness_does_not_return_any_credential_value(self):
        status = website_enrichment_readiness(self.database)
        self.assertFalse(status["ready"])
        self.assertNotIn("SECRET", repr(status))
        self.assertNotIn("SHOULD_NOT_MATTER", repr(status))


if __name__ == "__main__":
    unittest.main()
