"""
Route-order + capability-gate tests for the 2026-10-10 cascade reorder and
the QuickEnrich provider (pipeline.route_enrichment — the pure router used by
GET /enrich and CSV-job routed rows).

Canonical order (owner decision 2026-10-14):
  name_domain arm : contacts_db -> smartprospect -> quickenrich -> getleads
                    -> blitz -> moltsets -> better_enrich -> wizleads
  linkedin arm    : contacts_db -> quickenrich -> getleads -> blitz(person_enrich_by_linkedin)
                    -> blitz(find_work_email) -> moltsets(best_email_linkedin)
  domain DM discovery (in _enrich_domain, not route_enrichment):
                    contacts_db -> quickenrich -> blitz -> getleads
                    (moltsets does NOT join the domain arm — search quota)

Run:
    cd backend && python -m pytest enrichment/tests/test_quickenrich_routing.py -v
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from enrichment import pipeline  # noqa: E402

P = pipeline.ROUTE_PROVIDER_CONTACTS_DB
B = pipeline.ROUTE_PROVIDER_BLITZ
G = pipeline.ROUTE_PROVIDER_GETLEADS
S = pipeline.ROUTE_PROVIDER_SMARTPROSPECT
Q = pipeline.ROUTE_PROVIDER_QUICKENRICH
W = pipeline.ROUTE_PROVIDER_WIZLEADS
BE = pipeline.ROUTE_PROVIDER_BETTER_ENRICH
M = pipeline.ROUTE_PROVIDER_MOLTSETS


def _providers(route: dict, identifier: str | None = None) -> list[str]:
    steps = route["steps"]
    if identifier is not None:
        steps = [s for s in steps if s["identifier"] == identifier]
    return [s["provider"] for s in steps]


class TestNameDomainOrder:
    def test_full_name_and_domain_order(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe", first_name="Jane", last_name="Doe", domain="acme.com"
        )
        assert route["mode"] == "name_domain"
        # 2026-10-14: moltsets 6th, better_enrich re-enabled 7th, wizleads terminal.
        assert _providers(route) == [P, S, Q, G, B, M, BE, W]

    def test_contacts_db_always_first(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe", first_name="Jane", last_name="Doe", domain="acme.com"
        )
        assert route["steps"][0]["provider"] == P

    def test_smartprospect_is_first_paid_tier(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe", first_name="Jane", last_name="Doe", domain="acme.com"
        )
        assert route["steps"][1]["provider"] == S
        assert route["steps"][1]["method"] == pipeline.ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT

    def test_quickenrich_third(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe", first_name="Jane", last_name="Doe", domain="acme.com"
        )
        assert route["steps"][2]["provider"] == Q
        assert route["steps"][2]["method"] == pipeline.ROUTE_METHOD_EMPLOYEE_SEARCH_QUICKENRICH

    def test_blitz_demoted_to_fifth(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe", first_name="Jane", last_name="Doe", domain="acme.com"
        )
        assert _providers(route)[4] == B
        assert _providers(route).index(B) > _providers(route).index(G)

    def test_tail_tiers_moltsets_be_wizleads(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe", first_name="Jane", last_name="Doe", domain="acme.com"
        )
        seq = _providers(route)
        assert seq[5] == M
        assert seq[6] == BE
        assert seq[7] == W  # wizleads is again the terminal tier

    def test_single_token_name_drops_first_last_providers(self):
        # "Plato" -> first_name="Plato", last_name="" -> SP/QE/GL/moltsets/WizLeads
        # capability-dropped; blitz (full_name+domain) + BE remain.
        route = pipeline.route_enrichment(full_name="Plato", domain="acme.com")
        assert _providers(route) == [P, B, BE]


class TestLinkedInOrder:
    def test_linkedin_only_order(self):
        route = pipeline.route_enrichment(
            linkedin_url="https://www.linkedin.com/in/janedoe"
        )
        assert route["mode"] == "linkedin_only"
        assert _providers(route) == [P, Q, G, B, B, M]

    def test_quickenrich_before_getleads_and_blitz(self):
        route = pipeline.route_enrichment(
            linkedin_url="https://www.linkedin.com/in/janedoe"
        )
        seq = _providers(route)
        assert seq.index(Q) < seq.index(G) < seq.index(B)

    def test_linkedin_first_with_name_domain_fallback(self):
        route = pipeline.route_enrichment(
            linkedin_url="https://www.linkedin.com/in/janedoe",
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            domain="acme.com",
        )
        assert route["mode"] == "linkedin_first"
        li = _providers(route, identifier=pipeline.ROUTE_IDENTIFIER_LINKEDIN)
        nd = _providers(route, identifier=pipeline.ROUTE_IDENTIFIER_NAME_DOMAIN)
        assert li == [P, Q, G, B, B, M]
        # WizLeads present in the name-domain fallback arm (G16 fix); full
        # 2026-10-14 tail order applies there too.
        assert nd == [P, S, Q, G, B, M, BE, W]


class TestQuickEnrichCapabilityGates:
    def test_qe_by_linkedin_dropped_without_linkedin(self):
        route = pipeline.route_enrichment(full_name="Jane Doe", domain="acme.com")
        methods = [s["method"] for s in route["steps"]]
        assert pipeline.ROUTE_METHOD_EMPLOYEE_SEARCH_LINKEDIN_QUICKENRICH not in methods

    def test_qe_by_name_dropped_without_first_last(self):
        route = pipeline.route_enrichment(
            linkedin_url="https://www.linkedin.com/in/janedoe", domain="acme.com"
        )
        # No full_name -> no name_domain arm at all; linkedin arm only.
        assert _providers(route) == [P, Q, G, B, B, M]

    def test_qe_by_linkedin_present_with_linkedin_only(self):
        route = pipeline.route_enrichment(
            linkedin_url="https://www.linkedin.com/in/janedoe"
        )
        methods = [s["method"] for s in route["steps"]]
        assert pipeline.ROUTE_METHOD_EMPLOYEE_SEARCH_LINKEDIN_QUICKENRICH in methods


class TestQuickEnrichRestrictors:
    def test_force_provider_quickenrich(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            domain="acme.com",
            force_provider="quickenrich",
        )
        assert _providers(route) == [Q]

    def test_force_provider_quickenrich_linkedin(self):
        route = pipeline.route_enrichment(
            linkedin_url="https://www.linkedin.com/in/janedoe",
            force_provider="quickenrich",
        )
        assert _providers(route) == [Q]

    def test_force_provider_quickenrich_cannot_use_input(self):
        route = pipeline.route_enrichment(
            linkedin_url="https://www.linkedin.com/in/janedoe",  # QE needs /in/ — usable
            force_provider="quickenrich",
        )
        # With a personal URL it CAN use the input; without any QE-compatible
        # input (phone only) it cannot:
        route2 = pipeline.route_enrichment(phone="+15550001111", force_provider="quickenrich")
        assert route2["no_email_reason"] == pipeline.NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT

    def test_force_provider_globally_disabled_rejected(self, monkeypatch):
        """Forcing a globally-disabled provider returns zero steps with the
        DISTINCT provider_disabled reason — not the input-shape reason
        (route-surface analog of test_global_killswitch_precedence.py).
        Since 2026-10-14 every family_map provider is enabled, so the
        disable is simulated via the ENABLED_PROVIDERS dict."""
        monkeypatch.setitem(pipeline.providers.ENABLED_PROVIDERS, "better_enrich", False)
        route = pipeline.route_enrichment(
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            domain="acme.com",
            force_provider="better_enrich",
        )
        assert route["steps"] == []
        assert route["no_email_reason"] == "provider_disabled"

    def test_selected_providers_filters_to_selection(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            domain="acme.com",
            selected_providers=["quickenrich", "getleads"],
        )
        assert _providers(route) == [P, Q, G]  # contacts_db always kept

    def test_selected_providers_contacts_db_always_kept(self):
        route = pipeline.route_enrichment(
            full_name="Jane Doe",
            first_name="Jane",
            last_name="Doe",
            domain="acme.com",
            selected_providers=["blitz"],
        )
        assert _providers(route) == [P, B]


class TestInputHardening:
    def test_full_name_composed_from_split_names(self):
        """G15: split-only names must activate the name arm."""
        route = pipeline.route_enrichment(
            first_name="Jane", last_name="Doe", domain="acme.com"
        )
        assert route["mode"] == "name_domain"
        assert _providers(route) == [P, S, Q, G, B, M, BE, W]

    def test_whitespace_only_full_name_is_invalid(self):
        route = pipeline.route_enrichment(full_name="   ", domain="acme.com")
        assert route["mode"] == "domain_only"  # not name_domain — gates see ""

    def test_company_url_layered_rejection(self):
        """Company-page URLs: the ROUTER's parse gate accepts any linkedin.com
        path (the /company/ reroute happens in POST /enrich and
        list_builder._is_company_linkedin_url — identifier_utils is shared
        with company-URL fields and must not become /in/-strict). The PAID
        person endpoints reject /company/ at the client boundary:
        quickenrich_client (see test_quickenrich_client) and, since G7,
        getleads_client.find_email_by_linkedin. This test pins the router
        layer's contract: parseable but unchanged mode."""
        route = pipeline.route_enrichment(
            linkedin_url="https://www.linkedin.com/company/acme/"
        )
        assert route["mode"] == "linkedin_only"
        assert route["no_email_reason"] == ""


class TestRegistries:
    def test_quickenrich_in_valid_providers(self):
        assert "quickenrich" in pipeline.VALID_PROVIDERS

    def test_normalize_source(self):
        assert pipeline._normalize_source("quickenrich_email") == "quickenrich"
        assert pipeline._normalize_source("quickenrich_contacts") == "quickenrich"

    def test_provider_labels(self):
        assert (
            pipeline._provider_label(pipeline.ROUTE_METHOD_EMPLOYEE_SEARCH_QUICKENRICH) == Q
        )
        assert (
            pipeline._provider_label(
                pipeline.ROUTE_METHOD_EMPLOYEE_SEARCH_LINKEDIN_QUICKENRICH
            )
            == Q
        )

    def test_method_is_paid(self):
        assert pipeline._method_is_paid(pipeline.ROUTE_METHOD_EMPLOYEE_SEARCH_QUICKENRICH)
        assert pipeline._method_is_free(pipeline.ROUTE_METHOD_PERSON_BY_NAME_DOMAIN)

    def test_moltsets_registry(self):
        assert "moltsets" in pipeline.VALID_PROVIDERS
        assert (
            pipeline._provider_label(pipeline.ROUTE_METHOD_BEST_EMAIL_LINKEDIN_MOLTSETS) == M
        )
        assert (
            pipeline._provider_label(pipeline.ROUTE_METHOD_SEARCH_EMAIL_NAME_MOLTSETS) == M
        )
        assert pipeline._method_is_paid(pipeline.ROUTE_METHOD_BEST_EMAIL_LINKEDIN_MOLTSETS)
        assert pipeline._normalize_source("moltsets_email") == "moltsets"
