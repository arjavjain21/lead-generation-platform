"""
Centralized provider configuration.

Single source of truth for which enrichment providers are enabled.
To disable a provider, set its value to False here — no cascade logic changes needed.

The cascade logic in pipeline.py, routes.py, and list_builder.py
reads from this module to decide whether to call each provider.
"""

from typing import Final

# Single source of truth — edit here to enable/disable providers
ENABLED_PROVIDERS: Final[dict[str, bool]] = {
    "contacts_db": True,
    "blitz": True,
    "better_enrich": True,
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
    return ENABLED_PROVIDERS.get(provider, False)


def get_enabled_providers() -> list[str]:
    """
    Return list of enabled provider names.

    Returns:
        List of provider names that are currently enabled.
    """
    return [name for name, enabled in ENABLED_PROVIDERS.items() if enabled]
