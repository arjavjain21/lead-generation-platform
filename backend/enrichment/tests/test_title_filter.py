"""Tests for the TITLE_SEARCH_POOL knob in enrichment/title_filter.py.

Cost note: the pool widens only the FREE contacts_db decision-maker fetch
(consumers pass max(max_results, TITLE_SEARCH_POOL)); it never changes Blitz
spend — the waterfall bills 1 record per result RETURNED and receives the raw
max_results. The default must therefore stay a contacts_db-load tuning knob,
not a billing one: 12 candidates keep the local title gate precise without
over-fetching a free-but-slow 75 RPS API.

TITLE_SEARCH_POOL is read at import time, so the override tests reload the
module under a patched environment and ALWAYS reload it back under the real
environment afterwards (see _pool_with_env) — no state leaks across tests.
"""
import importlib
import os
import sys
from pathlib import Path

# Add backend to path (same bootstrap as sibling enrichment/tests files).
backend_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_dir))

from enrichment import title_filter


def _pool_with_env(env_value):
    """Reload title_filter under TITLE_SEARCH_POOL=<env_value> and return the
    module's post-reload pool. ``env_value=None`` simulates the variable being
    unset (the default path). The module is reloaded back under the caller's
    real environment in a finally block, so ordering pollution is impossible.
    """
    saved = os.environ.pop("TITLE_SEARCH_POOL", None)
    try:
        if env_value is not None:
            os.environ["TITLE_SEARCH_POOL"] = str(env_value)
        importlib.reload(title_filter)
        return title_filter.TITLE_SEARCH_POOL
    finally:
        if saved is None:
            os.environ.pop("TITLE_SEARCH_POOL", None)
        else:
            os.environ["TITLE_SEARCH_POOL"] = saved
        importlib.reload(title_filter)


class TestTitleSearchPool:
    def test_default_pool_is_12(self):
        """Unset env -> 12: enough candidates for the local gate, ~4x less
        contacts_db load than the old 50 on a free-but-slow 75 RPS API."""
        assert _pool_with_env(None) == 12

    def test_env_override_wins(self):
        """An explicit TITLE_SEARCH_POOL in the environment beats the default."""
        assert _pool_with_env("40") == 40
        assert _pool_with_env("3") == 3

    def test_module_restored_after_override(self):
        """Guard against ordering pollution: after an override probe, the
        imported module must be back on the default (12) for other tests."""
        _pool_with_env("99")
        assert title_filter.TITLE_SEARCH_POOL == 12


if __name__ == "__main__":
    TestTitleSearchPool().test_default_pool_is_12()
    TestTitleSearchPool().test_env_override_wins()
    TestTitleSearchPool().test_module_restored_after_override()
    print("\nAll title_filter pool tests passed!")
