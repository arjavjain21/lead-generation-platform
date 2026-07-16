"""
Tests for the ``selected_providers`` allowlist feature on the
``/api/enrichment/enrich`` endpoint.

When ``selected_providers`` is set on ``UnifiedEnrichRequest`` (or passed as
the ``?selected_providers=`` CSV query param on GET /enrich), the cascade
restricts itself to providers in that list. ``contacts_db`` is always allowed
even when not explicitly listed (mirrors ``list_builder.py`` convention).
Mutually exclusive with ``force_provider``.

Surfaces tested:
  * Group A: ``pipeline.route_enrichment`` step filtering (pure routing).
  * Group B: ``_should_skip_provider`` in both pipeline.py and routes.py.
  * Group C: ``UnifiedEnrichRequest`` model construction (validation is in
    the route handler, not the Pydantic model).
  * Group D-validation: ``_unified_enrich_logic`` validation (mutual
    exclusion, empty list, invalid names) — the shared core that owns
    these checks. Invoked directly.
  * Group D-filtering: GET /api/enrichment/enrich end-to-end via
    TestClient with all provider HTTP calls mocked hermetically.
  * Group D-post: POST /api/enrichment/enrich validation 400s (the POST
    endpoint has its own parallel cascade implementation separate from
    ``_unified_enrich_logic``, so validation is tested independently).
  * Group E: Regression — ``force_provider`` still works unchanged.

Run:
    cd /var/www/lead-generation-platform/backend
    source venv/bin/activate
    python -m pytest enrichment/tests/test_selected_providers.py -v
"""

from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Make backend root importable so `enrichment` resolves regardless of cwd.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

# Some modules read env at import time; seed a harmless token so imports succeed.
os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import pipeline as pipeline_mod  # noqa: E402
from enrichment import routes as routes_mod  # noqa: E402
from enrichment import blitz_client as bc  # noqa: E402
from enrichment import contacts_client as cc  # noqa: E402
from enrichment import contacts_writer  # noqa: E402


# ---------------------------------------------------------------------------
# Group A: pipeline.route_enrichment step filtering
# ---------------------------------------------------------------------------


class TestRouteEnrichmentSelectedProviders:
    """Pure routing tests. No I/O, no mocks. Verify selected_providers
    allowlist filtering on the planned cascade steps."""

    def test_selected_providers_contacts_db_and_smartprospect_with_name_domain(self):
        """A1: selected_providers=[contacts_db, smartprospect] with name+domain
        yields only contacts_db + smartprospect steps."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            selected_providers=["contacts_db", "smartprospect"],
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "contacts_db" in providers
        assert "smartprospect" in providers
        # None of the disallowed ones sneak in.
        assert "blitz" not in providers
        assert "wizleads" not in providers
        assert "better_enrich" not in providers

    def test_selected_providers_smartprospect_keeps_contacts_db_mandatory(self):
        """A2: selected_providers=[smartprospect] (contacts_db NOT listed)
        still keeps contacts_db steps — it's the mandatory first step."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            selected_providers=["smartprospect"],
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "contacts_db" in providers, (
            "contacts_db must always be allowed even when not in selected_providers"
        )
        assert "smartprospect" in providers
        # blitz/wizleads/better_enrich all filtered out.
        assert "blitz" not in providers
        assert "wizleads" not in providers
        assert "better_enrich" not in providers

    def test_selected_providers_contacts_db_only(self):
        """A3: selected_providers=[contacts_db] → only free tier runs."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            selected_providers=["contacts_db"],
        )
        providers = [s["provider"] for s in result["steps"]]
        assert providers == ["contacts_db"]
        assert "smartprospect" not in providers
        assert "blitz" not in providers
        assert "wizleads" not in providers
        assert "better_enrich" not in providers

    def test_selected_providers_blitz_and_better_enrich(self):
        """A4: selected_providers=[blitz, better_enrich] → only those two
        plus the mandatory contacts_db step."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            selected_providers=["blitz", "better_enrich"],
        )
        providers = [s["provider"] for s in result["steps"]]
        # contacts_db always present, plus the two selected.
        assert "contacts_db" in providers
        assert "blitz" in providers
        assert "better_enrich" in providers
        # smartprospect and wizleads filtered out.
        assert "smartprospect" not in providers
        assert "wizleads" not in providers

    def test_selected_providers_none_returns_full_cascade(self):
        """A5: selected_providers=None (baseline) → all 5 providers in cascade."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            selected_providers=None,
        )
        providers = [s["provider"] for s in result["steps"]]
        # Full cascade: contacts_db -> blitz -> smartprospect -> wizleads -> better_enrich
        assert providers == [
            "contacts_db",
            "blitz",
            "smartprospect",
            "wizleads",
            "better_enrich",
        ]

    def test_selected_providers_empty_list_surfaces_no_email_reason(self):
        """A6: selected_providers=[] (empty list). The HTTP layer rejects this
        with 400, but at the route level the allowlist + mandatory
        contacts_db injection must produce an explicit signal.

        On a phone-only input (which has no contacts_db step), filtering
        leaves zero steps and route_enrichment surfaces
        ``forced_provider_cannot_use_input`` so the caller sees a clear
        signal rather than a silent empty cascade."""
        result = pipeline_mod.route_enrichment(
            phone="+1-555-0100",
            selected_providers=[],
        )
        assert result["steps"] == []
        assert (
            result["no_email_reason"]
            == pipeline_mod.NO_EMAIL_REASON_FORCED_PROVIDER_CANNOT_USE_INPUT
        )

    def test_selected_providers_linkedin_only_with_blitz(self):
        """A7: linkedin_only mode with selected_providers=[blitz] → only
        blitz steps present (plus the mandatory contacts_db step)."""
        result = pipeline_mod.route_enrichment(
            linkedin_url="https://linkedin.com/in/jane",
            selected_providers=["blitz"],
        )
        providers = [s["provider"] for s in result["steps"]]
        # contacts_db always allowed; blitz explicitly selected.
        assert "contacts_db" in providers
        assert "blitz" in providers
        # No other providers sneak in.
        assert "smartprospect" not in providers
        assert "wizleads" not in providers
        assert "better_enrich" not in providers

    def test_force_provider_and_selected_providers_independently_testable(self):
        """A8: force_provider is a separate code path. Verify we can test it
        without combining with selected_providers. force_provider=blitz
        restricts to blitz only (no contacts_db, no smartprospect)."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            force_provider="blitz",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert all(p == "blitz" for p in providers), (
            f"force_provider=blitz must produce only blitz steps, got {providers}"
        )


# ---------------------------------------------------------------------------
# Group B: _should_skip_provider (both pipeline.py and routes.py versions)
# ---------------------------------------------------------------------------


class TestShouldSkipProviderSelected:
    """Verify _should_skip_provider honors selected_providers correctly in
    both pipeline.py and routes.py implementations."""

    def test_pipeline_selected_providers_skips_unlisted_provider(self):
        """B1: With selected_providers=[smartprospect], blitz must be skipped."""
        assert pipeline_mod._should_skip_provider(
            "blitz", None, ["smartprospect"]
        ) is True

    def test_pipeline_selected_providers_never_skips_contacts_db(self):
        """B2: contacts_db is always allowed even when not in selected_providers."""
        assert pipeline_mod._should_skip_provider(
            "contacts_db", None, ["smartprospect"]
        ) is False

    def test_pipeline_selected_providers_allows_listed_provider(self):
        """B3: A provider explicitly listed in selected_providers is allowed."""
        assert pipeline_mod._should_skip_provider(
            "smartprospect", None, ["smartprospect"]
        ) is False

    def test_pipeline_no_selected_providers_falls_back_to_global_enable(self):
        """B4: selected_providers=None → no allowlist filter; fall back to
        global ENABLED_PROVIDERS check (blitz is enabled by default → False)."""
        # Ensure no env leakage from prior tests.
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_BLITZ", None)
            assert pipeline_mod._should_skip_provider("blitz", None, None) is False

    def test_pipeline_force_provider_takes_precedence_over_selected(self):
        """B5: When force_provider is set, it wins — even if the provider is
        in selected_providers, force_provider=contacts_db must skip blitz."""
        assert pipeline_mod._should_skip_provider(
            "blitz", "contacts_db", ["blitz"]
        ) is True

    def test_routes_selected_providers_skips_unlisted_provider(self):
        """B1 (routes): mirror of pipeline behavior."""
        assert routes_mod._should_skip_provider(
            "blitz", None, ["smartprospect"]
        ) is True

    def test_routes_selected_providers_never_skips_contacts_db(self):
        """B2 (routes): contacts_db always allowed."""
        assert routes_mod._should_skip_provider(
            "contacts_db", None, ["smartprospect"]
        ) is False

    def test_routes_selected_providers_allows_listed_provider(self):
        """B3 (routes): listed provider is allowed."""
        assert routes_mod._should_skip_provider(
            "smartprospect", None, ["smartprospect"]
        ) is False

    def test_routes_no_selected_providers_falls_back_to_global_enable(self):
        """B4 (routes): no filter → global enable check."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ENABLE_BLITZ", None)
            assert routes_mod._should_skip_provider("blitz", None, None) is False

    def test_routes_force_provider_takes_precedence_over_selected(self):
        """B5 (routes): force_provider wins over selected_providers."""
        assert routes_mod._should_skip_provider(
            "blitz", "contacts_db", ["blitz"]
        ) is True


# ---------------------------------------------------------------------------
# Group C: UnifiedEnrichRequest model construction
# ---------------------------------------------------------------------------


class TestUnifiedEnrichRequestModel:
    """Verify the Pydantic model accepts selected_providers without validation.
    All semantic validation lives in the route handler (_unified_enrich_logic)."""

    def test_constructs_with_valid_provider_list(self):
        """C1: Clean construction with valid provider names."""
        req = routes_mod.UnifiedEnrichRequest(
            domain="x.com",
            selected_providers=["contacts_db", "smartprospect"],
        )
        assert req.selected_providers == ["contacts_db", "smartprospect"]

    def test_constructs_with_invalid_provider_name(self):
        """C2: Invalid provider names are accepted at model construction time
        (validation happens in the route handler). The field is stored as-is."""
        req = routes_mod.UnifiedEnrichRequest(
            domain="x.com",
            selected_providers=["invalid_name"],
        )
        # Stored verbatim — handler rejects it later with a 400.
        assert req.selected_providers == ["invalid_name"]

    def test_constructs_with_empty_list(self):
        """C3: Empty list also constructs (empty-list check is in handler)."""
        req = routes_mod.UnifiedEnrichRequest(
            domain="x.com",
            selected_providers=[],
        )
        assert req.selected_providers == []

    def test_default_is_none(self):
        """Sanity: when not provided, the field defaults to None (no filter)."""
        req = routes_mod.UnifiedEnrichRequest(domain="x.com")
        assert req.selected_providers is None

    def test_constructs_without_domain_when_linkedin_provided(self):
        """selected_providers is orthogonal to the identifier inputs."""
        req = routes_mod.UnifiedEnrichRequest(
            linkedin_url="https://linkedin.com/in/jane",
            selected_providers=["blitz"],
        )
        assert req.selected_providers == ["blitz"]


# ---------------------------------------------------------------------------
# Group D: Integration tests
# ---------------------------------------------------------------------------
#
# Two surfaces:
#   D-validation: ``_unified_enrich_logic`` is the shared core that owns all
#     the new validation rules (mutual exclusion, empty list, invalid names).
#     It's invoked directly here so the test exercises the real validation
#     path without depending on which HTTP handler happens to call it.
#   D-filtering: GET /api/enrichment/enrich is the only HTTP surface that
#     currently threads ``selected_providers`` all the way through to the
#     cascade. We hit it via TestClient and assert the routing block reflects
#     the allowlist. All provider HTTP calls are mocked hermetically.


def _make_user() -> dict:
    return {
        "user_id": "test-user-1",
        "email": "test@example.com",
        "is_admin": True,
    }


def _override_auth():
    """Override auth.get_current_user_with_api_key so we don't need a real JWT."""
    from shared import auth as _auth
    return {
        _auth.get_current_user_with_api_key: lambda: _make_user(),
        _auth.get_current_user: lambda: _make_user(),
    }


@pytest.fixture
def client():
    """TestClient with auth dependency overridden."""
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.update(_override_auth())
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def _stub_cascade_providers():
    """Return a tuple of patch objects that stub every provider HTTP call to
    a safe 'not found' default. Tests apply them via start/stop."""
    return (
        patch.object(contacts_writer, "is_v2_enabled", MagicMock(return_value=False)),
        patch.object(cc, "company_by_domain", AsyncMock(return_value=None)),
        patch.object(cc, "company_contacts_enriched", AsyncMock(return_value=[])),
        patch.object(cc, "person_by_linkedin", AsyncMock(return_value=None)),
        patch.object(cc, "person_by_name_and_domain", AsyncMock(return_value=None)),
        patch.object(cc, "extract_email_from_contacts_response", MagicMock(return_value=None)),
        patch.object(bc, "waterfall_icp_search", AsyncMock(return_value={"results": []})),
        patch.object(bc, "person_enrich_by_linkedin", AsyncMock(return_value={"found": False, "email": ""})),
        patch.object(bc, "find_work_email", AsyncMock(return_value={"found": False, "email": ""})),
        patch.object(bc, "person_enrich", AsyncMock(return_value={"found": False, "person": {}})),
        patch.object(bc, "domain_to_linkedin", AsyncMock(return_value={"found": False, "company_linkedin_url": ""})),
    )


class TestUnifiedEnrichLogicValidation:
    """Validation tests for _unified_enrich_logic — the shared core that
    owns the selected_providers validation rules. We invoke it directly so
    the test exercises the real validation code path."""

    def _run(self, req):
        """Run _unified_enrich_logic with the given request, returning the
        raised HTTPException (or None if no exception)."""
        import asyncio
        from fastapi import HTTPException

        async def go():
            with patch.object(routes_mod, "_record_unified_enrich_stats"), \
                 patch.object(routes_mod, "sync_contacts") as fake_sync:
                fake_sync.sync_enrichment_to_contacts = MagicMock(
                    return_value={"synced": 0, "skipped": 0, "failed": 0}
                )
                return await routes_mod._unified_enrich_logic(
                    req, {"email": "test@example.com", "user_id": 1}
                )

        try:
            asyncio.run(go())
        except HTTPException as e:
            return e
        return None

    def test_force_and_selected_mutually_exclusive_returns_400(self):
        """D2: force_provider + selected_providers → 400 'mutually exclusive'."""
        req = routes_mod.UnifiedEnrichRequest(
            domain="acme.com",
            force_provider="blitz",
            selected_providers=["blitz"],
        )
        exc = self._run(req)
        assert exc is not None, "Expected HTTPException, got None"
        assert exc.status_code == 400
        assert "mutually exclusive" in exc.detail, (
            f"Expected 'mutually exclusive' in detail, got: {exc.detail}"
        )

    def test_empty_selected_providers_returns_400(self):
        """D3: selected_providers=[] → 400 with non-empty hint."""
        req = routes_mod.UnifiedEnrichRequest(
            domain="acme.com",
            selected_providers=[],
        )
        exc = self._run(req)
        assert exc is not None
        assert exc.status_code == 400
        assert "non-empty" in exc.detail, (
            f"Expected 'non-empty' hint in detail, got: {exc.detail}"
        )

    def test_unknown_provider_returns_400(self):
        """D4: selected_providers=['fake_provider'] → 400 'Invalid provider'."""
        req = routes_mod.UnifiedEnrichRequest(
            domain="acme.com",
            selected_providers=["fake_provider"],
        )
        exc = self._run(req)
        assert exc is not None
        assert exc.status_code == 400
        assert "Invalid provider" in exc.detail, (
            f"Expected 'Invalid provider' in detail, got: {exc.detail}"
        )

    def test_valid_selected_providers_does_not_raise_validation_400(self):
        """Sanity: a clean selected_providers list passes validation. The
        call will proceed into the cascade (which is fully mocked); we just
        verify no validation HTTPException is raised."""
        patches = _stub_cascade_providers()
        for p in patches:
            p.start()
        try:
            req = routes_mod.UnifiedEnrichRequest(
                domain="acme.com",
                full_name="Jane Doe",
                selected_providers=["contacts_db", "smartprospect"],
            )
            exc = self._run(req)
        finally:
            for p in patches:
                p.stop()
        # Validation passes — no HTTPException from validation.
        # (If a non-validation exception bubbles up, the test will error.)
        assert exc is None or getattr(exc, "status_code", 200) != 400, (
            f"Valid selected_providers should not 400, got: {exc}"
        )


class TestGetEnrichSelectedProviders:
    """Integration tests for GET /api/enrichment/enrich with ?selected_providers=.

    The GET endpoint flows through _unified_enrich_logic, which threads
    selected_providers into pipeline.route_enrichment. We verify the
    end-to-end filtering behavior via TestClient."""

    def test_get_with_csv_selected_providers_filters_cascade(self, client):
        """D1/D5: GET /enrich with ?selected_providers=contacts_db,smartprospect
        yields a routing block whose providers_called contains only
        contacts_db and smartprospect. Disallowed providers never appear
        in providers_called or provider_attempts_json."""
        patches = _stub_cascade_providers()
        with patch.object(routes_mod, "_record_unified_enrich_stats"), \
             patch.object(routes_mod, "sync_contacts") as fake_sync:
            fake_sync.sync_enrichment_to_contacts = MagicMock(
                return_value={"synced": 0, "skipped": 0, "failed": 0}
            )
            for p in patches:
                p.start()
            try:
                resp = client.get(
                    "/api/enrichment/enrich",
                    params={
                        "domain": "acme.com",
                        "full_name": "Jane Doe",
                        "selected_providers": "contacts_db,smartprospect",
                        "max_results": 5,
                        "debug": "true",
                    },
                )
            finally:
                for p in patches:
                    p.stop()

        assert resp.status_code == 200, resp.text
        body = resp.json()
        routing = body.get("routing", {})
        called = set(routing.get("providers_called", []))
        attempts = routing.get("provider_attempts_json", [])
        attempted_providers = {a.get("provider", "") for a in attempts}

        # Selected providers (plus always-allowed contacts_db) must be called.
        assert "contacts_db" in called
        assert "smartprospect" in called
        # Disallowed providers must NOT appear anywhere in the routing block.
        assert "blitz" not in called
        assert "wizleads" not in called
        assert "better_enrich" not in called
        assert "blitz" not in attempted_providers
        assert "wizleads" not in attempted_providers
        assert "better_enrich" not in attempted_providers

    def test_get_with_force_and_selected_returns_400(self, client):
        """GET with both force_provider and selected_providers → 400
        'mutually exclusive'."""
        resp = client.get(
            "/api/enrichment/enrich",
            params={
                "domain": "acme.com",
                "force_provider": "blitz",
                "selected_providers": "blitz",
            },
        )
        assert resp.status_code == 400
        assert "mutually exclusive" in resp.json().get("detail", "")

    def test_get_with_unknown_provider_returns_400(self, client):
        """GET with ?selected_providers=fake_provider → 400 'Invalid provider'."""
        resp = client.get(
            "/api/enrichment/enrich",
            params={
                "domain": "acme.com",
                "selected_providers": "fake_provider",
            },
        )
        assert resp.status_code == 400
        assert "Invalid provider" in resp.json().get("detail", "")

    def test_get_empty_selected_providers_param_treated_as_no_filter(self, client):
        """Empty CSV (?selected_providers=) is parsed to None (no filter),
        so the cascade runs with all enabled providers. This is distinct
        from sending an explicit empty list via JSON, which 400s."""
        patches = _stub_cascade_providers()
        with patch.object(routes_mod, "_record_unified_enrich_stats"), \
             patch.object(routes_mod, "sync_contacts") as fake_sync:
            fake_sync.sync_enrichment_to_contacts = MagicMock(
                return_value={"synced": 0, "skipped": 0, "failed": 0}
            )
            for p in patches:
                p.start()
            try:
                resp = client.get(
                    "/api/enrichment/enrich",
                    params={
                        "domain": "acme.com",
                        "full_name": "Jane Doe",
                        "selected_providers": "",
                        "max_results": 5,
                    },
                )
            finally:
                for p in patches:
                    p.stop()
        # Empty query string → parsed to None → no filter → 200 (not 400).
        assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# Group D-post: POST /enrich validation (POST has its own cascade impl)
# ---------------------------------------------------------------------------


class TestPostEnrichSelectedProvidersValidation:
    """POST /api/enrichment/enrich has a parallel cascade implementation
    separate from _unrich_enrich_logic. Verify the selected_providers
    validation 400s fire on POST too — not just GET.

    These tests do NOT mock provider HTTP calls; they only verify that
    bad input is rejected before any cascade runs. Auth is overridden
    via the ``client`` fixture so no API key is needed.
    """

    def test_post_with_force_and_selected_returns_400(self, client):
        """POST with both force_provider and selected_providers → 400."""
        resp = client.post(
            "/api/enrichment/enrich",
            json={
                "domain": "acme.com",
                "force_provider": "blitz",
                "selected_providers": ["contacts_db"],
            },
        )
        assert resp.status_code == 400
        assert "mutually exclusive" in resp.json().get("detail", "")

    def test_post_with_unknown_provider_returns_400(self, client):
        """POST with selected_providers=['fake_provider'] → 400."""
        resp = client.post(
            "/api/enrichment/enrich",
            json={
                "domain": "acme.com",
                "selected_providers": ["contacts_db", "fake_provider"],
            },
        )
        assert resp.status_code == 400
        assert "Invalid provider" in resp.json().get("detail", "")

    def test_post_with_empty_list_returns_400(self, client):
        """POST with selected_providers=[] → 400 (empty list rejected)."""
        resp = client.post(
            "/api/enrichment/enrich",
            json={
                "domain": "acme.com",
                "selected_providers": [],
            },
        )
        assert resp.status_code == 400
        assert "non-empty" in resp.json().get("detail", "")

    def test_post_with_valid_selected_providers_passes_validation(self, client):
        """POST with valid selected_providers passes the validation gate.
        We don't assert on the cascade result (would require mocking); we
        only confirm the request is not rejected with a 400. Any non-400
        response (200 or 5xx from unmocked providers) means validation
        passed."""
        # Stub all providers to avoid hitting real APIs. Return empty/None
        # so the cascade completes quickly without finding anything.
        patches = _stub_cascade_providers()
        with patch.object(routes_mod, "_record_unified_enrich_stats"), \
             patch.object(routes_mod, "sync_contacts") as fake_sync:
            fake_sync.sync_enrichment_to_contacts = MagicMock(
                return_value={"synced": 0, "skipped": 0, "failed": 0}
            )
            for p in patches:
                p.start()
            try:
                resp = client.post(
                    "/api/enrichment/enrich",
                    json={
                        "domain": "acme.com",
                        "full_name": "Jane Doe",
                        "selected_providers": ["contacts_db", "smartprospect"],
                    },
                )
            finally:
                for p in patches:
                    p.stop()
        assert resp.status_code == 200, resp.text


# ---------------------------------------------------------------------------
# Group E: Regression — force_provider still works unchanged
# ---------------------------------------------------------------------------


class TestForceProviderRegression:
    """Confirm force_provider behavior is preserved — independent of the
    new selected_providers feature."""

    def test_force_provider_blitz_restricts_to_blitz_only(self):
        """E1: force_provider='blitz' yields only blitz steps."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            force_provider="blitz",
        )
        providers = [s["provider"] for s in result["steps"]]
        # Only blitz — NO contacts_db (force_provider is strict).
        assert all(p == "blitz" for p in providers), (
            f"force_provider=blitz must produce only blitz steps, got {providers}"
        )
        assert "contacts_db" not in providers
        assert "smartprospect" not in providers
        assert "wizleads" not in providers
        assert "better_enrich" not in providers

    def test_force_provider_contacts_db_skips_paid(self):
        """Sanity: force_provider=contacts_db still restricts to free tier."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            force_provider="contacts_db",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert providers == ["contacts_db"]

    def test_force_provider_smartprospect(self):
        """Sanity: force_provider=smartprospect still restricts to smartprospect."""
        result = pipeline_mod.route_enrichment(
            full_name="Jane Doe",
            domain="acme.com",
            force_provider="smartprospect",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert providers == ["smartprospect"]

    def test_force_provider_does_not_invoke_selected_providers_path(self):
        """E1 strictness: when force_provider is set and selected_providers is
        also passed to _should_skip_provider, force_provider wins and the
        selected_providers list is ignored entirely."""
        # force_provider=blitz, selected_providers=[contacts_db, smartprospect]
        # — the only allowed provider should be blitz.
        assert pipeline_mod._should_skip_provider(
            "contacts_db", "blitz", ["contacts_db", "smartprospect"]
        ) is True
        assert pipeline_mod._should_skip_provider(
            "smartprospect", "blitz", ["contacts_db", "smartprospect"]
        ) is True
        assert pipeline_mod._should_skip_provider(
            "blitz", "blitz", ["contacts_db", "smartprospect"]
        ) is False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
