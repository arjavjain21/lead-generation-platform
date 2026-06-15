"""
Tests for `identifier_utils.normalize_domain`.

This helper is the single source of truth for stripping a raw CSV
"website" or "domain" cell into a bare, lowercased domain. If a
provider client receives an un-normalized value, the API call 404s
or returns no useful data — and we silently lose the lead. These
tests pin down every shape that has been seen in real scraper
output (Mesterh, Eagle, etc.) so the helper can't regress.
"""

from __future__ import annotations

import os
import sys
import unittest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import identifier_utils as u  # noqa: E402


class TestNormalizeDomainEmptyAndNoise(unittest.TestCase):
    def test_empty_string(self):
        self.assertEqual(u.normalize_domain(""), "")

    def test_none(self):
        self.assertEqual(u.normalize_domain(None), "")

    def test_whitespace_only(self):
        self.assertEqual(u.normalize_domain("   "), "")

    def test_nan_token(self):
        self.assertEqual(u.normalize_domain("nan"), "")

    def test_none_token(self):
        self.assertEqual(u.normalize_domain("none"), "")

    def test_na_token(self):
        self.assertEqual(u.normalize_domain("N/A"), "")

    def test_dash(self):
        self.assertEqual(u.normalize_domain("-"), "")

    def test_non_string(self):
        # Anything we can't stringify is treated as missing.
        self.assertEqual(u.normalize_domain(object()), "")

    def test_email_rejected(self):
        # An email address is not a domain — sending it as a domain
        # to a provider would burn an API call.
        self.assertEqual(u.normalize_domain("user@example.com"), "")


class TestNormalizeDomainBare(unittest.TestCase):
    def test_bare_lower(self):
        self.assertEqual(u.normalize_domain("acme.com"), "acme.com")

    def test_bare_mixed_case(self):
        self.assertEqual(u.normalize_domain("ACME.com"), "acme.com")

    def test_bare_with_trailing_slash(self):
        self.assertEqual(u.normalize_domain("acme.com/"), "acme.com")

    def test_bare_with_trailing_dot(self):
        # FQDN notation: drop the trailing dot.
        self.assertEqual(u.normalize_domain("acme.com."), "acme.com")

    def test_bare_with_whitespace(self):
        self.assertEqual(u.normalize_domain("  acme.com  "), "acme.com")

    def test_subdomain_preserved(self):
        # Subdomains are valid domains.
        self.assertEqual(u.normalize_domain("shop.acme.com"), "shop.acme.com")

    def test_hyphenated_domain(self):
        self.assertEqual(u.normalize_domain("mesterh-service.de"), "mesterh-service.de")

    def test_bare_token_no_dot_rejected(self):
        # A single token like "acme" or "localhost" is not a domain
        # we want to send to providers.
        self.assertEqual(u.normalize_domain("acme"), "")
        self.assertEqual(u.normalize_domain("localhost"), "")


class TestNormalizeDomainProtocol(unittest.TestCase):
    def test_https_prefix(self):
        self.assertEqual(
            u.normalize_domain("https://acme.com"),
            "acme.com",
        )

    def test_http_prefix(self):
        self.assertEqual(
            u.normalize_domain("http://acme.com"),
            "acme.com",
        )

    def test_https_uppercase(self):
        self.assertEqual(
            u.normalize_domain("HTTPS://Acme.com"),
            "acme.com",
        )

    def test_protocol_with_www(self):
        self.assertEqual(
            u.normalize_domain("https://www.acme.com"),
            "acme.com",
        )

    def test_protocol_with_www_uppercase(self):
        self.assertEqual(
            u.normalize_domain("HTTP://WWW.ACME.COM"),
            "acme.com",
        )

    def test_protocol_with_subdomain(self):
        self.assertEqual(
            u.normalize_domain("https://shop.acme.com"),
            "shop.acme.com",
        )


class TestNormalizeDomainPathAndQuery(unittest.TestCase):
    def test_path_stripped(self):
        self.assertEqual(
            u.normalize_domain("https://acme.com/some/path"),
            "acme.com",
        )

    def test_query_string_stripped(self):
        # The exact pattern that produced 18 emails from 96k rows.
        self.assertEqual(
            u.normalize_domain("https://mesterh-service.de/?utm_source=gmb"),
            "mesterh-service.de",
        )

    def test_fragment_stripped(self):
        self.assertEqual(
            u.normalize_domain("https://acme.com/path#section"),
            "acme.com",
        )

    def test_path_query_and_fragment(self):
        self.assertEqual(
            u.normalize_domain("https://www.acme.com/a/b?c=1&d=2#x"),
            "acme.com",
        )

    def test_utm_tracking_full_pattern(self):
        # This is the literal pattern from the failed job's log.
        self.assertEqual(
            u.normalize_domain("https://mesterh-service.de/?utm_source=gmb&utm_medium=organic"),
            "mesterh-service.de",
        )

    def test_no_protocol_with_path(self):
        # Some CSVs drop the protocol but keep the path.
        self.assertEqual(
            u.normalize_domain("acme.com/some/path"),
            "acme.com",
        )

    def test_no_protocol_with_query(self):
        self.assertEqual(
            u.normalize_domain("acme.com?ref=fb"),
            "acme.com",
        )


class TestNormalizeDomainWWWVariants(unittest.TestCase):
    def test_www_stripped(self):
        self.assertEqual(u.normalize_domain("www.acme.com"), "acme.com")

    def test_www_with_path(self):
        self.assertEqual(
            u.normalize_domain("www.acme.com/about"),
            "acme.com",
        )

    def test_www_uppercase_stripped(self):
        self.assertEqual(u.normalize_domain("WWW.ACME.com"), "acme.com")


class TestNormalizeDomainIdempotent(unittest.TestCase):
    def test_normalized_input_unchanged(self):
        for raw in ("acme.com", "https://acme.com", "www.acme.com/"):
            normalized = u.normalize_domain(raw)
            self.assertEqual(u.normalize_domain(normalized), normalized)

    def test_normalized_input_unchanged_with_path(self):
        raw = "https://www.acme.com/about?x=1"
        normalized = u.normalize_domain(raw)
        self.assertEqual(u.normalize_domain(normalized), normalized)


class TestNormalizeDomainRejectsNonDomains(unittest.TestCase):
    def test_url_with_space_rejected(self):
        # A value with a literal space should not be passed to a provider.
        self.assertEqual(u.normalize_domain("https://acme .com"), "")

    def test_garbage_rejected(self):
        self.assertEqual(u.normalize_domain("not a domain"), "")


class TestBuildRowIdentifierPayloadNormalizesDomain(unittest.TestCase):
    """End-to-end: build_row_identifier_payload must produce a clean domain
    even when the input column is a full URL with tracking junk."""

    def test_full_url_with_tracking_normalized(self):
        row = {"website": "https://mesterh-service.de/?utm_source=gmb"}
        payload = u.build_row_identifier_payload(row, domain_col="website")
        self.assertEqual(payload["domain"], "mesterh-service.de")
        # input_domain mirrors the normalized form.
        self.assertEqual(payload["input_domain"], "mesterh-service.de")
        # input_fields_used still marks domain as used.
        self.assertIn("domain", payload["input_fields_used"])

    def test_bare_domain_preserved(self):
        row = {"website": "acme.com"}
        payload = u.build_row_identifier_payload(row, domain_col="website")
        self.assertEqual(payload["domain"], "acme.com")
        self.assertEqual(payload["input_domain"], "acme.com")

    def test_empty_domain_marks_no_input(self):
        row = {"website": "nan"}
        payload = u.build_row_identifier_payload(row, domain_col="website")
        self.assertEqual(payload["domain"], "")
        self.assertEqual(payload["input_domain"], "")
        self.assertNotIn("domain", payload["input_fields_used"])

    def test_email_value_rejected(self):
        row = {"website": "user@example.com"}
        payload = u.build_row_identifier_payload(row, domain_col="website")
        self.assertEqual(payload["domain"], "")
        self.assertNotIn("domain", payload["input_fields_used"])


class TestNormalizeDomainMatchesCompanyFallbackKey(unittest.TestCase):
    """The company-fallback dedupe key must equal the per-row identifier
    payload's domain — otherwise the same site would be enriched twice
    (or, worse, dedupe would miss and we'd waste API spend)."""

    def test_keys_match_for_url_inputs(self):
        from enrichment import company_fallback  # noqa: E402

        for raw in (
            "https://www.acme.com/about?x=1",
            "acme.com",
            "WWW.ACME.COM/",
            "http://acme.com",
        ):
            from_enrich = u.normalize_domain(raw)
            from_fb = company_fallback.normalize_domain_key(raw)
            self.assertEqual(from_enrich, from_fb, f"mismatch for {raw!r}")


if __name__ == "__main__":
    unittest.main()
