"""Tests for the provider-selection gate on the company/page-email fallbacks.

2026-09-11 RCA: company_fallback.run_company_fallbacks ignored the
request-time ``selected_providers`` allowlist, so a job uploaded as
"Blitz + GetLeads + SmartProspect" still burned BetterEnrich calls on
every no-hit domain. The gate mirrors the person-waterfall semantics:
a selection that omits ``better_enrich`` skips BOTH BetterEnrich
fallbacks (company email + facebook page email); ``None`` (no
selection) keeps the pre-existing behavior (global switches decide).
"""
import asyncio
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))


def _run(coro):
    # asyncio.run() instead of the deprecated get_event_loop(): other test
    # modules close the thread's loop via asyncio.run(), after which
    # get_event_loop() raises — order pollution.
    return asyncio.run(coro)


def _flags_on():
    """Patch both fallback flags on (tests must not depend on env)."""
    from enrichment import company_fallback

    return patch.multiple(
        company_fallback.fb_cfg,
        ENABLE_COMPANY_EMAIL_FALLBACK=True,
        ENABLE_FACEBOOK_EMAIL_FALLBACK=True,
        ALLOW_GENERIC_COMPANY_EMAIL=True,
        ALLOW_COMPANY_EMAIL_AS_FINAL=True,
    )


class TestBetterEnrichAllowed:
    """Unit tests for the better_enrich_allowed() gate."""

    def test_selection_without_better_enrich_blocks(self):
        from enrichment.company_fallback import better_enrich_allowed

        assert better_enrich_allowed(["blitz", "getleads", "smartprospect"]) is False

    def test_selection_with_better_enrich_allows(self):
        from enrichment.company_fallback import better_enrich_allowed

        assert better_enrich_allowed(["blitz", "better_enrich"]) is True

    def test_no_selection_uses_global_switch(self):
        from enrichment import company_fallback, providers

        original = dict(providers.ENABLED_PROVIDERS)
        try:
            providers.ENABLED_PROVIDERS["better_enrich"] = False
            assert company_fallback.better_enrich_allowed(None) is False
            providers.ENABLED_PROVIDERS["better_enrich"] = True
            assert company_fallback.better_enrich_allowed(None) is True
        finally:
            providers.ENABLED_PROVIDERS.clear()
            providers.ENABLED_PROVIDERS.update(original)


class TestRunCompanyFallbacksGate:
    """run_company_fallbacks must not call BetterEnrich when deselected."""

    def test_deselected_makes_no_better_enrich_calls(self):
        from enrichment import company_fallback

        calls = []

        async def fail_company(*args, **kwargs):
            calls.append("find_company_email")
            raise AssertionError("find_company_email must not be called when deselected")

        async def fail_facebook(*args, **kwargs):
            calls.append("find_email_from_facebook_page")
            raise AssertionError("find_email_from_facebook_page must not be called when deselected")

        with _flags_on():
            with patch.object(
                company_fallback.better_enrich_client,
                "find_company_email",
                side_effect=fail_company,
            ), patch.object(
                company_fallback.better_enrich_client,
                "find_email_from_facebook_page",
                side_effect=fail_facebook,
            ):
                result = _run(
                    company_fallback.run_company_fallbacks(
                        object(),
                        domain="acme.com",
                        facebook_url="https://facebook.com/acme",
                        selected_providers=["blitz", "getleads", "smartprospect"],
                    )
                )

        assert result["providers_called"] == []
        assert result["company_email"] == ""
        # Honest reason: the fallback tier was disabled by the selection.
        assert result["no_email_reason"] == "company_email_fallback_disabled"

    def test_selected_better_enrich_still_runs(self):
        from enrichment import company_fallback

        async def fake_company(client, website=None, **kwargs):
            return {"email": "blake@acme.com", "email_status": "verified"}

        with _flags_on():
            with patch.object(
                company_fallback.better_enrich_client,
                "find_company_email",
                side_effect=fake_company,
            ) as mock_company:
                result = _run(
                    company_fallback.run_company_fallbacks(
                        object(),
                        domain="acme.com",
                        facebook_url="",
                        selected_providers=["blitz", "better_enrich"],
                    )
                )

        assert mock_company.await_count == 1
        assert result["company_email"] == "blake@acme.com"
        assert result["company_email_source"] == "better_enrich_company_email"
        assert "better_enrich_company_email" in result["providers_called"]

    def test_no_selection_keeps_existing_behavior(self):
        from enrichment import company_fallback

        async def fake_company(client, website=None, **kwargs):
            return {"email": "blake@acme.com", "email_status": "verified"}

        with _flags_on():
            with patch.object(
                company_fallback.better_enrich_client,
                "find_company_email",
                side_effect=fake_company,
            ) as mock_company:
                result = _run(
                    company_fallback.run_company_fallbacks(
                        object(),
                        domain="acme.com",
                        facebook_url="",
                        selected_providers=None,
                    )
                )

        assert mock_company.await_count == 1
        assert result["company_email"] == "blake@acme.com"


class TestListBuilderWiring:
    """The Flow-1 helper threads selected_providers into the fallback."""

    def test_flow1_helper_skips_better_enrich_when_deselected(self):
        from enrichment import company_fallback, list_builder

        async def fail_company(*args, **kwargs):
            raise AssertionError("find_company_email must not be called when deselected")

        rows = [{"domain": "acme.com", "dm_email": "", "row_status": "no_contacts"}]

        with _flags_on():
            with patch.object(
                company_fallback.better_enrich_client,
                "find_company_email",
                side_effect=fail_company,
            ):
                _run(
                    list_builder._apply_company_fallback_to_output_rows(
                        object(),
                        rows,
                        domain="acme.com",
                        facebook_url="",
                        dedupe=company_fallback.CompanyFallbackDedupe(),
                        selected_providers=["blitz", "getleads", "smartprospect"],
                    )
                )

        # Row untouched by the fallback tier (no company_email set).
        assert rows[0].get("company_email", "") == ""

    def test_flow1_helper_runs_when_selected(self):
        from enrichment import company_fallback, list_builder

        async def fake_company(client, website=None, **kwargs):
            return {"email": "blake@acme.com", "email_status": "verified"}

        rows = [{"domain": "acme.com", "dm_email": "", "row_status": "no_contacts"}]

        with _flags_on():
            with patch.object(
                company_fallback.better_enrich_client,
                "find_company_email",
                side_effect=fake_company,
            ) as mock_company:
                _run(
                    list_builder._apply_company_fallback_to_output_rows(
                        object(),
                        rows,
                        domain="acme.com",
                        facebook_url="",
                        dedupe=company_fallback.CompanyFallbackDedupe(),
                        selected_providers=["blitz", "better_enrich"],
                    )
                )

        assert mock_company.await_count == 1
        assert rows[0].get("company_email") == "blake@acme.com"
