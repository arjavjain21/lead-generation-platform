"""Regression: ``_acquire_rate_limit`` must LOOP until a token is granted.

2026-08-14 incident: the original implementation slept the returned wait and
proceeded WITHOUT re-acquiring. Concurrent denials compute similar waits and
all proceed together after sleeping — admitting N calls per ~1 refilled token,
bursting past the GetLeads 100 RPM account limit (observed: 41% 429 rate,
p90 latency 37s). Only a GRANT consumes a token, so the caller must re-check
after every wait until granted.
"""
from __future__ import annotations

import asyncio
import os
import sys

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import getleads_client as glc  # noqa: E402


def test_acquire_rate_limit_loops_until_granted(monkeypatch):
    """Denied twice then granted: must re-acquire after EACH wait (3 calls)."""
    waits = [0.5, 0.25, 0.0]
    calls = {"n": 0}
    slept: list[float] = []

    def fake_acquire(provider, refill_per_sec, capacity=None):
        calls["n"] += 1
        return waits[calls["n"] - 1]

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(glc.rate_limiter, "acquire_token", fake_acquire)
    monkeypatch.setattr(glc.asyncio, "sleep", fake_sleep)

    asyncio.run(glc._acquire_rate_limit())

    assert calls["n"] == 3, (
        "must re-acquire after each wait (loop until granted), "
        "not proceed after a single sleep"
    )
    assert slept == [0.5, 0.25], "must sleep each returned wait before re-acquiring"


def test_acquire_rate_limit_returns_immediately_when_granted(monkeypatch):
    """First acquire grants (wait=0): exactly one call, no sleep."""
    calls = {"n": 0}

    def fake_acquire(provider, refill_per_sec, capacity=None):
        calls["n"] += 1
        return 0.0

    async def fake_sleep(seconds):
        raise AssertionError("must not sleep when granted immediately")

    monkeypatch.setattr(glc.rate_limiter, "acquire_token", fake_acquire)
    monkeypatch.setattr(glc.asyncio, "sleep", fake_sleep)

    asyncio.run(glc._acquire_rate_limit())
    assert calls["n"] == 1
