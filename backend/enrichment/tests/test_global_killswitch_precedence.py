"""
Tests for global kill-switch precedence in the three ``_should_skip_provider``
implementations (pipeline.py, routes.py, list_builder.py).

Invariant: the ``ENABLED_PROVIDERS`` global kill switch (providers.py) BEATS
the request-time restrictors. ``force_provider`` or ``selected_providers``
naming a globally disabled provider must NOT re-enable it.

Regression context (2026-10-10): ``list_builder._should_skip_provider`` checked
``force_provider`` first, so external callers sending ``force_provider=
"better_enrich"`` (still in VALID_PROVIDERS, so validation passed) bypassed the
2026-09-29 disable and kept hammering the dead key with ~950 403s/hr. pipeline
and routes already checked global first; list_builder now matches.

Run:
    cd /var/www/lead-generation-platform/backend
    source venv/bin/activate
    python -m pytest enrichment/tests/test_global_killswitch_precedence.py -v
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from enrichment import list_builder, pipeline, providers, routes  # noqa: E402


def _disable(monkeypatch, name: str) -> None:
    """Force a provider into the globally-disabled state for the test."""
    monkeypatch.setitem(providers.ENABLED_PROVIDERS, name, False)


def _enable(monkeypatch, name: str) -> None:
    monkeypatch.setitem(providers.ENABLED_PROVIDERS, name, True)


class TestListBuilderGlobalKillPrecedence:
    """The regression: list_builder must check global enablement FIRST."""

    def test_force_provider_cannot_override_global_disable(self, monkeypatch):
        _disable(monkeypatch, "better_enrich")
        assert list_builder._should_skip_provider(
            "better_enrich", force_provider="better_enrich"
        ) is True

    def test_selected_providers_cannot_override_global_disable(self, monkeypatch):
        _disable(monkeypatch, "better_enrich")
        assert list_builder._should_skip_provider(
            "better_enrich", selected_providers=["contacts_db", "better_enrich"]
        ) is True

    def test_globally_disabled_skipped_with_no_restrictors(self, monkeypatch):
        _disable(monkeypatch, "better_enrich")
        assert list_builder._should_skip_provider("better_enrich") is True

    def test_enabled_provider_still_forceable(self, monkeypatch):
        _enable(monkeypatch, "better_enrich")
        assert list_builder._should_skip_provider(
            "better_enrich", force_provider="better_enrich"
        ) is False
        # Every other provider is skipped under force.
        assert list_builder._should_skip_provider("blitz", force_provider="better_enrich") is True

    def test_enabled_provider_still_selectable(self, monkeypatch):
        _enable(monkeypatch, "better_enrich")
        assert list_builder._should_skip_provider(
            "better_enrich", selected_providers=["better_enrich"]
        ) is False

    def test_contacts_db_always_allowed_in_selection(self):
        # contacts_db stays allowed under selected_providers even when omitted
        # (mandatory first step) — preserved behavior, not part of the kill fix.
        assert list_builder._should_skip_provider(
            "contacts_db", selected_providers=["blitz"]
        ) is False


class TestAllThreeGatesAgree:
    """pipeline, routes, and list_builder must give identical verdicts for
    every (provider, force, selected) combination — desync is how the bypass
    happened."""

    _IMPLS = (
        ("pipeline", pipeline._should_skip_provider),
        ("routes", routes._should_skip_provider),
        ("list_builder", list_builder._should_skip_provider),
    )

    def test_disabled_provider_agreement_matrix(self, monkeypatch):
        _disable(monkeypatch, "better_enrich")
        cases = [
            ("better_enrich", None, None),
            ("better_enrich", "better_enrich", None),
            ("better_enrich", None, ["contacts_db", "better_enrich"]),
            ("blitz", "better_enrich", None),
        ]
        for provider, force, selected in cases:
            verdicts = {
                name: impl(provider, force_provider=force, selected_providers=selected)
                for name, impl in self._IMPLS
            }
            assert len(set(verdicts.values())) == 1, (
                f"gate desync for provider={provider} force={force} "
                f"selected={selected}: {verdicts}"
            )

    def test_enabled_provider_agreement_matrix(self, monkeypatch):
        _enable(monkeypatch, "better_enrich")
        cases = [
            ("better_enrich", "better_enrich", None),
            ("better_enrich", None, ["better_enrich"]),
            ("blitz", "blitz", None),
            ("blitz", None, None),
        ]
        for provider, force, selected in cases:
            verdicts = {
                name: impl(provider, force_provider=force, selected_providers=selected)
                for name, impl in self._IMPLS
            }
            assert len(set(verdicts.values())) == 1, (
                f"gate desync for provider={provider} force={force} "
                f"selected={selected}: {verdicts}"
            )

    def test_real_disabled_providers_blocked_everywhere(self):
        # Live-config smoke: whatever is globally disabled right now must be
        # reported as skipped by all three gates under every restrictor.
        for name, enabled in providers.ENABLED_PROVIDERS.items():
            if enabled:
                continue
            for force in (None, name):
                for selected in (None, [name]):
                    for impl_name, impl in self._IMPLS:
                        assert impl(name, force_provider=force, selected_providers=selected) is True, (
                            f"{impl_name} lets globally-disabled {name} through "
                            f"(force={force}, selected={selected})"
                        )


class TestCompanyFallbackGate:
    """company_fallback.better_enrich_allowed must ALSO check the global kill
    switch before honoring a selection (2026-10-10: resumed jobs carrying
    legacy selections with better_enrich kept firing the dead key)."""

    def test_selection_cannot_override_global_disable(self, monkeypatch):
        from enrichment import company_fallback

        monkeypatch.setitem(providers.ENABLED_PROVIDERS, "better_enrich", False)
        assert company_fallback.better_enrich_allowed(["blitz", "better_enrich"]) is False

    def test_enabled_provider_respects_selection(self, monkeypatch):
        from enrichment import company_fallback

        monkeypatch.setitem(providers.ENABLED_PROVIDERS, "better_enrich", True)
        assert company_fallback.better_enrich_allowed(["better_enrich"]) is True
        assert company_fallback.better_enrich_allowed(["blitz"]) is False

    def test_no_selection_uses_global(self, monkeypatch):
        from enrichment import company_fallback

        monkeypatch.setitem(providers.ENABLED_PROVIDERS, "better_enrich", True)
        assert company_fallback.better_enrich_allowed(None) is True
        monkeypatch.setitem(providers.ENABLED_PROVIDERS, "better_enrich", False)
        assert company_fallback.better_enrich_allowed(None) is False
