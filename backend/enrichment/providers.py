"""
Centralized provider configuration.

Single source of truth for which enrichment providers are enabled.
To disable a provider, set its value to False here — no cascade logic changes needed.

The cascade logic in pipeline.py, routes.py, and list_builder.py
reads from this module to decide whether to call each provider.
"""

import os
from typing import Final

# Single source of truth — edit here to enable/disable providers
ENABLED_PROVIDERS: Final[dict[str, bool]] = {
    "contacts_db": True,
    "blitz": True,
    "smartprospect": True,   # SmartLead Find Emails API (prospect-api.smartlead.ai), 30 RPS, batch up to 10
    "wizleads": True,
    "better_enrich": True,   # ← re-enabled 2026-10-14 (owner request, 7th tier after moltsets). Fast kill: ENABLE_BETTER_ENRICH env (added with the moltsets integration — the 2026-09-29 disable needed a code deploy). Watch credits: /api/v1/credits balance was 0.1 at re-enable.
    "getleads": True,   # GetLeads (app.getleads.io), email + decision-makers enrichment
    "quickenrich": True,   # QuickEnrich (app.quickenrich.io), person email + domain decision-makers. Standard plan ($99/mo, 2026-10): 1000 RPM search / 300 RPM domain. Kill switch ENABLE_QUICKENRICH.
    "moltsets": True,   # MoltSets (api.moltsets.com), person email tier 6th ($97/mo: 15k rec/5h, 75k rec/wk, 7.5k searches/5h). Kill switch ENABLE_MOLTSETS. Cooldown + miss store in moltsets_guards.py
    "prospeo": False,   # ← disable Prospeo (was paid, temporarily disabled)
}


def is_provider_enabled(provider: str) -> bool:
    """
    Check if a provider is enabled.

    Args:
        provider: Provider name (e.g., "contacts_db", "blitz", "prospeo")

    Returns:
        True if enabled, False otherwise. Defaults to False for unknown providers.
    """
    if not ENABLED_PROVIDERS.get(provider, False):
        return False
    # Per-provider env kill-switches (default true unless explicitly disabled)
    if provider == "smartprospect":
        if os.environ.get("ENABLE_SMARTPROSPECT", "true").lower() != "true":
            return False
    if provider == "getleads":
        if os.environ.get("ENABLE_GETLEADS", "true").lower() != "true":
            return False
    if provider == "quickenrich":
        if os.environ.get("ENABLE_QUICKENRICH", "true").lower() != "true":
            return False
    if provider == "better_enrich":
        if os.environ.get("ENABLE_BETTER_ENRICH", "true").lower() != "true":
            return False
    if provider == "moltsets":
        if os.environ.get("ENABLE_MOLTSETS", "true").lower() != "true":
            return False
    return True


def get_enabled_providers() -> list[str]:
    """
    Return list of enabled provider names.

    Returns:
        List of provider names that are currently enabled.
    """
    return [name for name, enabled in ENABLED_PROVIDERS.items() if enabled]
