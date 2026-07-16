"""
Regression tests for ``routes._build_domain_only_fallback_route_result``.

Background: domain_only requests that returned 0 contacts (e.g., small
clinics with no LinkedIn presence) showed ``routing.provider_attempts=[]``
in the API response, which looked like the system didn't try anything.
Actually the cascade DID try Contacts DB + Blitz at the company level —
the routing layer just wasn't surfacing it.

This helper synthesizes a realistic route_result from the data_sources
signals so the response accurately reports what was attempted.
"""

from __future__ import annotations

import os
import sys

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import routes  # noqa: E402
from enrichment import pipeline  # noqa: E402


class TestBuildDomainOnlyFallbackRouteResult:
    """Verify the synthesized routing block for domain_only mode."""

    def test_no_company_linkedin_found_shows_no_linkedin_reason(self):
        """The user's positiveimpactclinic.com case: no LinkedIn URL anywhere."""
        data_sources = {
            "company_linkedin": "not_found",
            "contacts": "not_found",
            "emails": "not_found",
        }
        result = routes._build_domain_only_fallback_route_result(
            data_sources=data_sources,
            company_linkedin_url="",
        )
        assert result["no_email_reason"] == "domain_only_no_linkedin"
        assert result["mode"] == "domain_only"
        # Both company-lookup providers tried, both missed
        assert "company_by_domain@contacts_db" in result["provider_attempts"]
        assert "domain_to_linkedin@blitz" in result["provider_attempts"]
        # Contacts DB contacts_enriched was also tried (no LinkedIn needed)
        assert "company_contacts_enriched@contacts_db" in result["provider_attempts"]
        # Blitz waterfall was NOT tried (requires company LinkedIn URL)
        assert "waterfall_icp_search@blitz" not in result["provider_attempts"]
        # Source path is human-readable
        assert "no company LinkedIn URL" in result["source_path"]

    def test_company_linkedin_found_via_contacts_db_no_dms(self):
        """Contacts DB had the company but no decision makers for it."""
        data_sources = {
            "company_linkedin": "contacts_db",
            "contacts": "not_found",
            "emails": "not_found",
        }
        result = routes._build_domain_only_fallback_route_result(
            data_sources=data_sources,
            company_linkedin_url="https://linkedin.com/company/acme",
        )
        assert result["no_email_reason"] == "domain_only_no_contacts"
        # Contacts DB succeeded for company lookup → Blitz was NOT tried for company
        assert "company_by_domain@contacts_db" in result["provider_attempts"]
        assert "domain_to_linkedin@blitz" not in result["provider_attempts"]
        # Company LinkedIn was found → waterfall was tried
        assert "waterfall_icp_search@blitz" in result["provider_attempts"]
        assert "company_contacts_enriched@contacts_db" in result["provider_attempts"]
        assert "no decision makers" in result["source_path"]

    def test_company_linkedin_found_via_blitz_no_dms(self):
        """Contacts DB missed company, Blitz found it, but no DMs."""
        data_sources = {
            "company_linkedin": "blitz",
            "contacts": "not_found",
            "emails": "not_found",
        }
        result = routes._build_domain_only_fallback_route_result(
            data_sources=data_sources,
            company_linkedin_url="https://linkedin.com/company/acme",
        )
        assert result["no_email_reason"] == "domain_only_no_contacts"
        # Both company-lookup providers tried (Contacts DB missed, Blitz succeeded)
        assert "company_by_domain@contacts_db" in result["provider_attempts"]
        assert "domain_to_linkedin@blitz" in result["provider_attempts"]
        # Company LinkedIn found → waterfall tried
        assert "waterfall_icp_search@blitz" in result["provider_attempts"]

    def test_dms_found_but_no_email_defensive_fallback(self):
        """When DMs are found but per-DM cascade missed, the helper returns
        an empty no_email_reason (the per-DM routing would have populated
        normally). This branch rarely triggers in practice."""
        data_sources = {
            "company_linkedin": "contacts_db",
            "contacts": "contacts_db",
            "emails": "not_found",
        }
        result = routes._build_domain_only_fallback_route_result(
            data_sources=data_sources,
            company_linkedin_url="https://linkedin.com/company/acme",
        )
        assert result["no_email_reason"] == ""
        assert "per-DM cascade ran" in result["source_path"]

    def test_attempt_order_is_stable(self):
        """Attempts appear in execution order: company lookups first, then DMs."""
        data_sources = {"company_linkedin": "not_found", "contacts": "not_found"}
        result = routes._build_domain_only_fallback_route_result(
            data_sources=data_sources,
            company_linkedin_url="",
        )
        attempts = result["provider_attempts"]
        # company_by_domain comes before domain_to_linkedin
        assert attempts.index("company_by_domain@contacts_db") < attempts.index("domain_to_linkedin@blitz")
        # Company-level lookups come before DM discovery
        assert attempts.index("domain_to_linkedin@blitz") < attempts.index("company_contacts_enriched@contacts_db")

    def test_returns_provider_attempts_json_empty_list(self):
        """The JSON-style attempts are populated by the route-based cascade,
        not by this helper. We return an empty list so _build_routing_response
        doesn't crash when iterating."""
        data_sources = {"company_linkedin": "not_found", "contacts": "not_found"}
        result = routes._build_domain_only_fallback_route_result(
            data_sources=data_sources,
            company_linkedin_url="",
        )
        assert result["provider_attempts_json"] == []
        assert result["providers_called"] == []
        assert result["providers_skipped"] == []

    def test_handles_missing_data_source_keys_gracefully(self):
        """Defensive: missing keys shouldn't crash the helper."""
        result = routes._build_domain_only_fallback_route_result(
            data_sources={},  # no keys
            company_linkedin_url="",
        )
        # Defaults to "both tried, both missed"
        assert result["no_email_reason"] == "domain_only_no_linkedin"
        assert len(result["provider_attempts"]) >= 2


class TestDomainOnlyRoutingIntegration:
    """Integration: _build_routing_response consumes the synthetic result."""

    def test_build_routing_response_uses_synthetic_result(self):
        """Verify the full pipeline: synthetic result → _build_routing_response
        produces a routing block with non-empty provider_attempts."""
        data_sources = {"company_linkedin": "not_found", "contacts": "not_found"}
        synthetic = routes._build_domain_only_fallback_route_result(
            data_sources=data_sources,
            company_linkedin_url="",
        )
        routing_block = routes._build_routing_response(
            route={"mode": "domain_only", "steps": []},
            route_result=synthetic,
            debug=False,
        )
        assert routing_block["mode"] == "domain_only"
        assert routing_block["no_email_reason"] == "domain_only_no_linkedin"
        assert len(routing_block["provider_attempts"]) >= 2
        assert routing_block["provider_attempts"][0] == "company_by_domain@contacts_db"

    def test_no_email_reason_constant_matches_helper_output(self):
        """Lock in the constant value so API consumers can rely on it."""
        assert pipeline.NO_EMAIL_REASON_DOMAIN_ONLY_NO_LINKEDIN == "domain_only_no_linkedin"
        assert pipeline.NO_EMAIL_REASON_DOMAIN_ONLY_NO_CONTACTS == "domain_only_no_contacts"
