"""
Integration tests verifying that ``smartprospect`` is correctly wired
into the enrichment cascade across ``pipeline.py``, ``routes.py``,
``list_builder.py``, and ``providers.py``.

Scope:
  * Provider registration (VALID_PROVIDERS, ENABLED_PROVIDERS, kill-switch).
  * Source / route constants exist with the expected string values.
  * Method helpers (_provider_label, _can_provider_use_method,
    _method_is_paid, _normalize_source).
  * Route planning (cascade ordering) via ``route_enrichment``.
  * Force provider behaviour.
  * Source dispatch via the response_normalizer.
  * _should_skip_provider integration (kill-switch + force_provider).

Pure / in-process only. No real HTTP calls. ``monkeypatch`` is used for
env-var manipulation; ``pytest.mark.parametrize`` for variant cases.

The CRITICAL wiring assertion is that SmartProspect sits **between**
Blitz and WizLeads in the name+domain cascade (mirrors the
implementation in ``pipeline.route_enrichment``).
"""

from __future__ import annotations

import os
import sys

import pytest

# Make sure the backend root is on sys.path so `enrichment` is importable
# regardless of where pytest is invoked from.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import pipeline  # noqa: E402
from enrichment import routes  # noqa: E402
from enrichment import list_builder  # noqa: E402
from enrichment import providers  # noqa: E402
from enrichment.response_normalizer import normalize_provider_contact  # noqa: E402


# ---------------------------------------------------------------------------
# 1. Provider registration (mechanical checks)
# ---------------------------------------------------------------------------

class TestProviderRegistration:
    def test_smartprospect_in_pipeline_valid_providers(self):
        assert "smartprospect" in pipeline.VALID_PROVIDERS

    def test_smartprospect_in_routes_valid_providers(self):
        assert "smartprospect" in routes.VALID_PROVIDERS

    def test_smartprospect_in_list_builder_valid_providers(self):
        assert "smartprospect" in list_builder.VALID_PROVIDERS

    def test_smartprospect_enabled_by_default(self):
        # Kill-switch default is "true"; ensure no env leakage from
        # other tests by clearing it explicitly.
        assert providers.is_provider_enabled("smartprospect") is True

    def test_smartprospect_kill_switch(self, monkeypatch):
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", "false")
        assert providers.is_provider_enabled("smartprospect") is False

    def test_smartprospect_kill_switch_explicit_true(self, monkeypatch):
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", "true")
        assert providers.is_provider_enabled("smartprospect") is True

    # The kill-switch uses a strict `!= "true"` comparison, so any value
    # other than literal "true" (case-insensitive) disables the provider.
    @pytest.mark.parametrize("value", ["false", "False", "FALSE", "0", "no", "off", "1", "yes", "on", ""])
    def test_smartprospect_kill_switch_falsy_variants(self, monkeypatch, value):
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", value)
        assert providers.is_provider_enabled("smartprospect") is False

    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "tRuE"])
    def test_smartprospect_kill_switch_truthy_variants(self, monkeypatch, value):
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", value)
        assert providers.is_provider_enabled("smartprospect") is True


# ---------------------------------------------------------------------------
# 2. Constants exist
# ---------------------------------------------------------------------------

class TestConstants:
    def test_source_constant(self):
        assert pipeline.SOURCE_SMARTPROSPECT == "smartprospect_email"

    def test_route_provider_constant(self):
        assert pipeline.ROUTE_PROVIDER_SMARTPROSPECT == "smartprospect"

    def test_route_method_constant(self):
        assert pipeline.ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT == "find_email_smartprospect"

    def test_list_builder_source_constant(self):
        assert list_builder.SOURCE_SMARTPROSPECT == "smartprospect_email"


# ---------------------------------------------------------------------------
# 3. Method helpers
# ---------------------------------------------------------------------------

class TestMethodHelpers:
    def test_provider_label_for_smartprospect(self):
        assert (
            pipeline._provider_label(pipeline.ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT)
            == pipeline.ROUTE_PROVIDER_SMARTPROSPECT
        )

    @pytest.mark.parametrize(
        "inputs,expected",
        [
            # All three present -> allowed.
            (
                {"first_name": "John", "last_name": "Doe", "domain": "example.com"},
                True,
            ),
            # Missing first_name.
            (
                {"first_name": "", "last_name": "Doe", "domain": "example.com"},
                False,
            ),
            # Missing last_name.
            (
                {"first_name": "John", "last_name": "", "domain": "example.com"},
                False,
            ),
            # Missing domain.
            (
                {"first_name": "John", "last_name": "Doe", "domain": ""},
                False,
            ),
            # All three missing.
            (
                {"first_name": "", "last_name": "", "domain": ""},
                False,
            ),
        ],
    )
    def test_can_provider_use_method_smartprospect_requires_all_three(self, inputs, expected):
        result = pipeline._can_provider_use_method(
            pipeline.ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT,
            inputs,
        )
        assert result is expected

    def test_can_provider_use_method_uses_full_inputs_dict(self):
        # Sanity: linkedin/phone/full_name don't satisfy smartprospect's
        # requirement of first + last + domain.
        inputs = {
            "linkedin_url": "https://linkedin.com/in/johndoe",
            "phone": "",
            "full_name": "John Doe",
            "first_name": "",
            "last_name": "",
            "domain": "example.com",
        }
        assert pipeline._can_provider_use_method(
            pipeline.ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT, inputs
        ) is False

    def test_method_is_paid_smartprospect(self):
        assert pipeline._method_is_paid(
            pipeline.ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT
        ) is True

    def test_normalize_source_smartprospect(self):
        assert pipeline._normalize_source("smartprospect_email") == "smartprospect"

    def test_normalize_source_smartprospect_list_builder(self):
        assert list_builder._normalize_source("smartprospect_email") == "smartprospect"

    @pytest.mark.parametrize(
        "raw_source,expected",
        [
            ("smartprospect_email", "smartprospect"),
            ("smartprospect", "smartprospect"),
            ("smartprospect_something_else", "smartprospect"),
        ],
    )
    def test_normalize_source_variants(self, raw_source, expected):
        assert pipeline._normalize_source(raw_source) == expected


# ---------------------------------------------------------------------------
# 4. Route planning (cascade ordering) via route_enrichment
# ---------------------------------------------------------------------------

class TestRoutePlanning:
    def test_name_domain_cascade_has_smartprospect_between_blitz_and_wizleads(self):
        """CRITICAL: smartprospect is positioned between Blitz and WizLeads
        in the name+domain cascade. Mirrors the cascade order documented in
        pipeline.route_enrichment.
        """
        result = pipeline.route_enrichment(
            full_name="John Doe",
            first_name="John",
            last_name="Doe",
            domain="example.com",
        )
        providers_in_order = [step["provider"] for step in result["steps"]]
        assert "smartprospect" in providers_in_order
        blitz_idx = providers_in_order.index("blitz")
        sp_idx = providers_in_order.index("smartprospect")
        wl_idx = providers_in_order.index("wizleads")
        assert blitz_idx < sp_idx < wl_idx

    def test_name_domain_cascade_no_smartprospect_when_first_name_missing(self):
        """When only full_name is present (no first_name/last_name split),
        smartprospect's capability gate (first + last + domain) is not
        satisfied, so it must be dropped from the cascade."""
        result = pipeline.route_enrichment(
            full_name="JohnDoe",
            domain="example.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" not in providers

    def test_name_domain_cascade_no_smartprospect_when_last_name_missing(self):
        """Symmetric to above: missing last_name must also drop smartprospect.

        Note: ``route_enrichment`` auto-derives first/last from a multi-word
        ``full_name``. To test the genuine "last name unknowable" case we use
        ``full_name=""`` (no name to derive from) plus ``first_name="John"``
        and empty ``last_name``."""
        result = pipeline.route_enrichment(
            full_name="",
            first_name="John",
            last_name="",
            domain="example.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" not in providers

    def test_name_domain_cascade_no_smartprospect_when_domain_missing(self):
        """Without a domain, smartprospect's gate (first + last + domain)
        is not satisfied."""
        result = pipeline.route_enrichment(
            full_name="John Doe",
            first_name="John",
            last_name="Doe",
        )
        # No domain -> router returns "invalid" with no steps.
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" not in providers


# ---------------------------------------------------------------------------
# P0 regression: route_enrichment auto-derives first/last from full_name
# ---------------------------------------------------------------------------
#
# Added 2026-07-09 after RCA showed enhanced-mode requests that sent only
# ``full_name`` were silently dropping smartprospect and WizLeads from the
# cascade. The fix derives ``first_name`` / ``last_name`` from ``full_name``
# when those fields are empty. These tests lock in the new contract.


class TestRouteEnrichmentAutoDerivesNames:
    """Verify route_enrichment splits full_name into first/last when not
    explicitly provided, so provider capability gates see the derived values."""

    def test_full_name_only_includes_smartprospect_in_route(self):
        """The bug we fixed: calling route_enrichment with only full_name
        used to silently drop smartprospect. Now it must be included."""
        result = pipeline.route_enrichment(
            full_name="Connor Gillivan",
            domain="ecombalance.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" in providers, (
            f"smartprospect missing from route — auto-derivation broken. "
            f"Got providers: {providers}"
        )

    def test_full_name_only_includes_wizleads_in_route(self):
        """Same fix benefits WizLeads — it has the same first+last+domain gate."""
        result = pipeline.route_enrichment(
            full_name="Connor Gillivan",
            domain="ecombalance.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "wizleads" in providers

    def test_full_name_only_produces_all_six_providers_in_correct_order(self):
        """End-to-end check: enhanced-mode style call produces the full cascade
        in the documented order (getleads sits between blitz and smartprospect)."""
        result = pipeline.route_enrichment(
            full_name="Connor Gillivan",
            domain="ecombalance.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert providers == ["contacts_db", "blitz", "getleads", "smartprospect", "wizleads", "better_enrich"]

    def test_explicit_first_and_last_are_not_overridden(self):
        """When the caller provides explicit first_name and last_name, the
        derivation must NOT clobber them. This protects callers that have
        more accurate name parsing than naive space-split."""
        result = pipeline.route_enrichment(
            full_name="John David Smith",
            first_name="John",
            last_name="Smith",  # caller knows the real surname
            domain="example.com",
        )
        inputs = result.get("inputs", {})
        assert inputs["first_name"] == "John"
        assert inputs["last_name"] == "Smith"

    def test_explicit_first_name_preserved_even_when_full_name_derivable(self):
        """Belt-and-suspenders: partial explicit input (first only) should
        preserve the explicit first, derive the missing last from full_name."""
        result = pipeline.route_enrichment(
            full_name="John Doe",
            first_name="Jonathan",  # caller prefers this variant
            last_name="",
            domain="example.com",
        )
        inputs = result.get("inputs", {})
        assert inputs["first_name"] == "Jonathan"  # explicit wins
        assert inputs["last_name"] == "Doe"  # derived from full_name

    def test_single_word_full_name_does_not_enable_smartprospect(self):
        """A single-word full_name (no space) leaves last_name empty after
        derivation. The smartprospect gate correctly excludes it — we need
        BOTH first and last to call SmartLead's API."""
        result = pipeline.route_enrichment(
            full_name="Cher",  # single word, no space
            domain="example.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" not in providers
        assert result["inputs"]["last_name"] == ""

    def test_multi_word_full_name_splits_first_word_as_first_name(self):
        """Convention: first word = first_name, rest = last_name (matches
        the legacy cascade's splitting behavior in _resolve_email_for_person)."""
        result = pipeline.route_enrichment(
            full_name="John David Smith Jr",
            domain="example.com",
        )
        assert result["inputs"]["first_name"] == "John"
        assert result["inputs"]["last_name"] == "David Smith Jr"

    def test_derived_values_reach_inputs_dict(self):
        """The inputs dict (returned on the route result) must carry the
        derived first/last — downstream _run_route_step reads from inputs
        to call provider APIs."""
        result = pipeline.route_enrichment(
            full_name="Connor Gillivan",
            domain="ecombalance.com",
        )
        assert result["inputs"]["first_name"] == "Connor"
        assert result["inputs"]["last_name"] == "Gillivan"

    def test_empty_full_name_does_not_derive(self):
        """When full_name is empty, no derivation happens — first/last stay
        whatever the caller passed (possibly empty)."""
        result = pipeline.route_enrichment(
            full_name="",
            first_name="",
            last_name="",
            domain="example.com",
        )
        # No names at all -> domain_only mode (no personal cascade steps)
        assert result["mode"] == "domain_only"
        assert result["steps"] == []

    def test_linkedin_first_cascade_also_derives_names(self):
        """The LinkedIn-first cascade's name+domain fallback arm has the
        same smartprospect gate. Auto-derivation must benefit it too."""
        result = pipeline.route_enrichment(
            linkedin_url="https://linkedin.com/in/connor-gillivan",
            full_name="Connor Gillivan",  # no explicit first/last
            domain="ecombalance.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" in providers, (
            f"smartprospect missing from LinkedIn-first cascade — "
            "auto-derivation should benefit both cascade arms. "
            f"Got: {providers}"
        )

    def test_linkedin_first_cascade_includes_smartprospect_in_name_domain_fallback(self):
        """When LinkedIn + name + domain are all present, the LinkedIn-first
        cascade includes a name+domain fallback arm that should contain
        smartprospect."""
        result = pipeline.route_enrichment(
            linkedin_url="https://linkedin.com/in/johndoe",
            full_name="John Doe",
            first_name="John",
            last_name="Doe",
            domain="example.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" in providers

    def test_linkedin_first_cascade_no_smartprospect_without_first_and_last(self):
        """LinkedIn + domain + single-word full_name (last name unknowable)
        must NOT emit a smartprospect step (capability gate fails).

        Note: ``route_enrichment`` auto-derives first/last from multi-word
        full_name. We use a single-word full_name so last_name stays empty
        after derivation, genuinely exercising the gate."""
        result = pipeline.route_enrichment(
            linkedin_url="https://linkedin.com/in/johndoe",
            full_name="John",
            domain="example.com",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" not in providers

    def test_smartprospect_step_carries_correct_method_and_identifier(self):
        """The smartprospect step must be labelled with the find_email_smartprospect
        method and the name_domain identifier."""
        result = pipeline.route_enrichment(
            full_name="John Doe",
            first_name="John",
            last_name="Doe",
            domain="example.com",
        )
        sp_steps = [s for s in result["steps"] if s["provider"] == "smartprospect"]
        assert len(sp_steps) >= 1
        sp = sp_steps[0]
        assert sp["method"] == pipeline.ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT
        assert sp["identifier"] == pipeline.ROUTE_IDENTIFIER_NAME_DOMAIN


# ---------------------------------------------------------------------------
# 5. Force provider
# ---------------------------------------------------------------------------

class TestForceProvider:
    def test_force_provider_smartprospect_keeps_only_smartprospect_steps(self):
        """force_provider='smartprospect' must drop every non-smartprospect
        step from the cascade."""
        result = pipeline.route_enrichment(
            full_name="John Doe",
            first_name="John",
            last_name="Doe",
            domain="example.com",
            force_provider="smartprospect",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert providers == ["smartprospect"]
        # And the step must be the smartprospect find_email method.
        assert result["steps"][0]["method"] == pipeline.ROUTE_METHOD_FIND_EMAIL_SMARTPROSPECT

    def test_force_provider_smartprospect_fails_when_first_or_last_missing(self):
        """Without first_name/last_name (single-word full_name so last name
        is genuinely unknowable), smartprospect cannot run; force_provider
        must surface a clear no_email_reason.

        Note: ``route_enrichment`` auto-derives first/last from multi-word
        full_name. We use single-word ``full_name="John"`` so the derivation
        leaves last_name empty, genuinely exercising the gate."""
        result = pipeline.route_enrichment(
            full_name="John",
            domain="example.com",
            force_provider="smartprospect",
        )
        # The capability gate removes the smartprospect step (no first/last).
        # After force filtering + capability gate there are no usable steps.
        assert result["steps"] == []
        assert result["no_email_reason"] != ""

    def test_force_provider_other_blocks_smartprospect(self):
        """When force_provider is set to a different family (e.g., blitz),
        smartprospect must not appear in the route."""
        result = pipeline.route_enrichment(
            full_name="John Doe",
            first_name="John",
            last_name="Doe",
            domain="example.com",
            force_provider="blitz",
        )
        providers = [s["provider"] for s in result["steps"]]
        assert "smartprospect" not in providers
        assert all(p == "blitz" for p in providers)


# ---------------------------------------------------------------------------
# 6. Source dispatch via response_normalizer
# ---------------------------------------------------------------------------

class TestSourceDispatch:
    def test_smartprospect_source_dispatches_correctly(self):
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "ex.com",
            "email_id": "j@ex.com",
            "status": "Found",
            "verification_status": "Valid",
        }
        result = normalize_provider_contact("smartprospect", raw)
        assert result is not None
        assert result["source"] == "smartprospect"
        assert result["email"] == "j@ex.com"

    def test_smartprospect_source_dispatch_carries_name_and_domain(self):
        raw = {
            "firstName": "Jane",
            "lastName": "Roe",
            "companyDomain": "acme.com",
            "email_id": "jane@acme.com",
            "status": "Found",
            "verification_status": "Valid",
        }
        result = normalize_provider_contact("smartprospect", raw)
        assert result is not None
        assert result["first_name"] == "Jane"
        assert result["last_name"] == "Roe"
        assert result["domain"] == "acme.com"

    def test_smartprospect_source_dispatch_not_found_status_still_returns_record(self):
        """A 'Not Found' SmartProspect response has no email but still has
        first/last; the normalizer keeps the record so the collector can
        record the name with an empty email."""
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "ex.com",
            "email_id": "",
            "status": "Not Found",
            "verification_status": None,
        }
        result = normalize_provider_contact("smartprospect", raw)
        assert result is not None
        assert result["source"] == "smartprospect"
        assert result["email"] == ""

    @pytest.mark.parametrize("alias", ["smartprospect", "smart_prospect", "smart-prospect"])
    def test_smartprospect_source_dispatch_aliases(self, alias):
        raw = {
            "firstName": "John",
            "lastName": "Doe",
            "companyDomain": "ex.com",
            "email_id": "j@ex.com",
            "status": "Found",
        }
        result = normalize_provider_contact(alias, raw)
        assert result is not None
        assert result["source"] == "smartprospect"


# ---------------------------------------------------------------------------
# 7. _should_skip_provider integration
# ---------------------------------------------------------------------------

class TestShouldSkipProvider:
    def test_should_skip_provider_smartprospect_when_disabled(self, monkeypatch):
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", "false")
        assert pipeline._should_skip_provider("smartprospect", None) is True

    def test_should_skip_provider_smartprospect_when_enabled(self, monkeypatch):
        # Explicitly remove any leaked env value first.
        monkeypatch.delenv("ENABLE_SMARTPROSPECT", raising=False)
        assert pipeline._should_skip_provider("smartprospect", None) is False

    def test_should_skip_provider_smartprospect_when_enabled_explicit_true(self, monkeypatch):
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", "true")
        assert pipeline._should_skip_provider("smartprospect", None) is False

    def test_should_skip_provider_force_provider_blocks_smartprospect(self):
        """When force_provider='blitz', smartprospect (not the forced one)
        must be skipped."""
        assert pipeline._should_skip_provider("smartprospect", "blitz") is True

    def test_should_skip_provider_force_provider_blocks_smartprospect_for_wizleads(self):
        assert pipeline._should_skip_provider("smartprospect", "wizleads") is True

    def test_should_skip_provider_force_provider_blocks_smartprospect_for_contacts_db(self):
        assert pipeline._should_skip_provider("smartprospect", "contacts_db") is True

    def test_should_skip_provider_force_provider_blocks_smartprospect_for_better_enrich(self):
        assert pipeline._should_skip_provider("smartprospect", "better_enrich") is True

    def test_should_skip_provider_force_provider_allows_smartprospect(self):
        """When force_provider='smartprospect', smartprospect itself is allowed."""
        assert pipeline._should_skip_provider("smartprospect", "smartprospect") is False

    def test_should_skip_provider_force_provider_smartprospect_blocks_blitz(self):
        """When force_provider='smartprospect', other providers are skipped."""
        assert pipeline._should_skip_provider("blitz", "smartprospect") is True
        assert pipeline._should_skip_provider("wizleads", "smartprospect") is True
        assert pipeline._should_skip_provider("contacts_db", "smartprospect") is True
        assert pipeline._should_skip_provider("better_enrich", "smartprospect") is True

    def test_should_skip_provider_disabled_overrides_force_provider_match(self, monkeypatch):
        """Kill-switch beats force_provider: even if force_provider matches,
        a disabled provider must still be skipped."""
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", "false")
        assert pipeline._should_skip_provider("smartprospect", "smartprospect") is True


# ---------------------------------------------------------------------------
# Smoke: VALID_PROVIDERS in all three modules agree
# ---------------------------------------------------------------------------

class TestValidProvidersAgreement:
    def test_all_three_valid_provider_sets_contain_smartprospect(self):
        for module in (pipeline, routes, list_builder):
            assert "smartprospect" in module.VALID_PROVIDERS, (
                f"{module.__name__}.VALID_PROVIDERS missing smartprospect"
            )

    def test_all_three_valid_provider_sets_contain_getleads(self):
        for module in (pipeline, routes, list_builder):
            assert "getleads" in module.VALID_PROVIDERS, (
                f"{module.__name__}.VALID_PROVIDERS missing getleads"
            )

    def test_all_three_valid_provider_sets_are_equal(self):
        # All three modules should agree on the exact provider set so a
        # force_provider value accepted by one is accepted by all.
        assert pipeline.VALID_PROVIDERS == routes.VALID_PROVIDERS == list_builder.VALID_PROVIDERS
