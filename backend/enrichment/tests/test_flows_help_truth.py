"""/flows/help cascade-table truth tests (PR-0.2, 2026-11-11).

The self-documentation payload's provider table was a hand-maintained list
that drifted from reality (it omitted leadocean + moltsets and carried a
stale better_enrich label). It is now derived from the explicit
_HELP_CASCADE_TIERS order with per-tier status computed live from
providers.get_enabled_providers(), so env kill switches are reflected
automatically. These tests pin the order and the live-status derivation.
"""
from __future__ import annotations

import os
import sys

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import providers  # noqa: E402
from enrichment import routes  # noqa: E402

EXPECTED_ORDER = [
    "contacts_db",
    "smartprospect",
    "quickenrich",
    "leadocean",       # tier 4 since 2026-11-06
    "getleads",
    "blitz",
    "moltsets",        # tier 6 since 2026-10-14
    "better_enrich",
    "wizleads",        # terminal
    "prospeo",         # code-present, end-to-end off
]


def test_help_cascade_lists_all_tiers_in_executor_order():
    tiers = routes._LIST_BUILDING_HELP_PAYLOAD["providers"]["cascade_order"]
    assert [t["name"] for t in tiers] == EXPECTED_ORDER


def test_help_tier_status_matches_live_provider_flags():
    """status must equal providers.is_provider_enabled at build time — never
    a second hand-maintained enable list."""
    tiers = routes._LIST_BUILDING_HELP_PAYLOAD["providers"]["cascade_order"]
    for tier in tiers:
        expected = "enabled" if providers.is_provider_enabled(tier["name"]) else "disabled"
        assert tier["status"] == expected, f"{tier['name']}: {tier['status']} != {expected}"


def test_help_table_reflects_env_kill_switch(monkeypatch):
    """Flipping an env kill switch (e.g. ENABLE_BETTER_ENRICH) must flip the
    row's status on rebuild — the drift this PR fixed."""
    import importlib

    monkeypatch.setenv("ENABLE_BETTER_ENRICH", "false")
    try:
        importlib.reload(routes)
        tiers = routes._LIST_BUILDING_HELP_PAYLOAD["providers"]["cascade_order"]
        by_name = {t["name"]: t["status"] for t in tiers}
        assert by_name["better_enrich"] == "disabled"
    finally:
        # conftest's env snapshot-restore handles os.environ; restore the
        # module so later tests see the original (env-at-import) state.
        monkeypatch.delenv("ENABLE_BETTER_ENRICH", raising=False)
        importlib.reload(routes)
        assert routes._LIST_BUILDING_HELP_PAYLOAD["providers"]["cascade_order"][0]["name"] == "contacts_db"


def test_help_cascade_arms_document_leadocean_terminal_discovery():
    arms = routes._LIST_BUILDING_HELP_PAYLOAD["providers"]["cascade_arms"]
    assert "leadocean(domain->people, terminal)" in arms


def test_help_every_tier_has_rate_and_role_copy():
    for tier in routes._LIST_BUILDING_HELP_PAYLOAD["providers"]["cascade_order"]:
        assert tier["rate"], f"{tier['name']} missing rate"
        assert tier["role"], f"{tier['name']} missing role"
