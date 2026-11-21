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
    "leadocean": True,   # LeadOcean (api.leadocean.io), person email tier 4 — directly after quickenrich (Agency UGC $247/mo: 1M enrich/day, 25M search/mo, 200 RPS). Kill switch ENABLE_LEADOCEAN. Daily-wall cooldown + miss store in leadocean_guards.py
    "prospeo": False,   # ← disable Prospeo (was paid, temporarily disabled)
    # Internal Email Finder service (127.0.0.1:8910, systemd email-finder.service):
    # terminal pattern-guessing fallback validated via the shared MailTester
    # proxy. ARMED 2026-11-21 (approved 5% canary): ENABLE_EMAIL_FINDER env
    # is the runtime kill switch; EMAIL_FINDER_CANARY_PERCENT gates traffic
    # (0 = armed but no traffic; watchdog halt file overrides everything).
    "email_finder": True,
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
    if provider == "leadocean":
        if os.environ.get("ENABLE_LEADOCEAN", "true").lower() != "true":
            return False
    if provider == "email_finder":
        # Runtime kill switch (armed once the dict entry flips to True).
        if os.environ.get("ENABLE_EMAIL_FINDER", "true").lower() != "true":
            return False
    return True


def email_finder_canary_bucket(identifier: str) -> int:
    """Deterministic 0-99 cohort bucket from a stable identifier.

    sha1-based, so the same person (keyed on first+last+domain, lowercased)
    lands in the same bucket across jobs, restarts, and workers — no
    cohort registry needed.
    """
    import hashlib
    return int(hashlib.sha1(identifier.encode("utf-8")).hexdigest(), 16) % 100


# Persistent canary halt state, written by the watchdog
# (tools/canary_watchdog.py) when a safety threshold trips. File-based so
# the halt survives restarts and is visible to every worker without env
# changes. Cleared only by an operator after investigation.
EMAIL_FINDER_HALT_FILE = os.environ.get(
    "EMAIL_FINDER_HALT_FILE", "/mnt/disk/email-finder/canary_halt"
)


def _canary_halted() -> bool:
    try:
        return os.path.exists(EMAIL_FINDER_HALT_FILE)
    except OSError:
        return True  # fail closed: unreadable state halts the tier


def email_finder_rollout_allows(identifier: str, force_provider: str = "") -> bool:
    """Canary gate for the Email Finder terminal tier.

    Requires the provider enabled (ENABLED_PROVIDERS + ENABLE_EMAIL_FINDER
    env kill switch) AND the identifier falling inside the
    EMAIL_FINDER_CANARY_PERCENT cohort (0=off default, 5=canary, 100=full).
    An explicit force_provider=email_finder diagnostic request bypasses the
    canary but never the enable/kill switches or a watchdog halt.
    """
    if not is_provider_enabled("email_finder"):
        return False
    if _canary_halted():
        return False
    if force_provider == "email_finder":
        return True
    try:
        pct = int(os.environ.get("EMAIL_FINDER_CANARY_PERCENT", "0"))
    except ValueError:
        pct = 0
    if pct <= 0:
        return False
    if pct >= 100:
        return True
    return email_finder_canary_bucket(identifier) < pct


def get_enabled_providers() -> list[str]:
    """
    Return list of enabled provider names.

    Returns:
        List of provider names that are currently enabled.
    """
    return [name for name, enabled in ENABLED_PROVIDERS.items() if enabled]
