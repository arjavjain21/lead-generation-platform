"""SmartProspect shared cross-process limiter — loop-until-granted contract.

Mirrors test_getleads_rate_limit_loop.py. SmartProspect ran a per-process
30 RPS asyncio limiter while 2+ gunicorn workers each enforced their own —
collectively far above the 2000 req/min account cap, observed as a 22% 429
rate (150,916 of 681,947 calls, 30d to 2026-09-13). The shared SQLite token
bucket (shared/rate_limiter.py) fixes the coordination; these tests pin:

- ``_acquire_rate_limit`` re-acquires after every wait (only a grant exits)
- env ``SMARTPROSPECT_RATE_LIMIT_RPM`` / ``_BURST`` feed the bucket
- a real-clock soak: N concurrent acquires are throttled to the refill rate
"""
from __future__ import annotations

import asyncio
import os
import sys

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import smartprospect_client as spc  # noqa: E402


def test_acquire_rate_limit_loops_until_granted(monkeypatch):
    """Denied twice then granted: must re-acquire after EACH wait (3 calls)."""
    waits = [0.5, 0.25, 0.0]
    calls = {"n": 0}
    slept: list[float] = []

    def fake_acquire(provider, refill_per_sec, capacity=None):
        calls["n"] += 1
        assert provider == "smartprospect"
        return waits[calls["n"] - 1]

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(spc.rate_limiter, "acquire_token", fake_acquire)
    monkeypatch.setattr(spc.asyncio, "sleep", fake_sleep)

    asyncio.run(spc._acquire_rate_limit())

    assert calls["n"] == 3, "must re-acquire after each wait"
    assert slept == [0.5, 0.25]


def test_rpm_env_parsed(monkeypatch):
    monkeypatch.setenv("SMARTPROSPECT_RATE_LIMIT_RPM", "1900")
    monkeypatch.setenv("SMARTPROSPECT_RATE_LIMIT_BURST", "2")
    import importlib

    fresh = importlib.reload(spc)
    try:
        assert fresh._REFILL_PER_SEC == 1900.0 / 60.0
        assert fresh._CAPACITY == 2.0
    finally:
        importlib.reload(spc)


def test_concurrent_acquires_throttled_to_refill(tmp_path, monkeypatch):
    """Real-clock soak: 20 concurrent acquires at 120 RPM must take >= bound.

    120 RPM = 2 tokens/sec. A full bucket grants ``capacity`` instantly and
    the rest must drip at refill rate: with capacity 1, 20 acquires need at
    least (20-1)/2 = 9.5s if the bucket starts drained. We pre-drain via a
    fresh DB so the bound holds from a cold start.
    """
    from shared import rate_limiter as rl

    rl.configure_db_path(str(tmp_path / "rl_soak.db"))

    async def soak():
        await asyncio.gather(*(spc._acquire_rate_limit() for _ in range(20)))

    import time as _time

    monkeypatch.setenv("SMARTPROSPECT_RATE_LIMIT_RPM", "120")
    # Re-derive the constants the client passes (reload to pick up env).
    import importlib

    importlib.reload(spc)
    try:
        # Drain the fresh bucket with one acquire first so capacity is spent.
        asyncio.run(spc._acquire_rate_limit())
        start = _time.monotonic()
        asyncio.run(soak())
        elapsed = _time.monotonic() - start
        # 19 remaining acquires at 2/sec, allow small scheduling slack downward.
        assert elapsed >= 8.0, f"20 acquires at 120 RPM finished in {elapsed:.2f}s — over-admitting"
    finally:
        importlib.reload(spc)
        rl.configure_db_path(None)
