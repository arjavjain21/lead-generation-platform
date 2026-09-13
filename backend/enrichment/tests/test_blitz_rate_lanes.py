"""Blitz per-endpoint rate lanes.

The Blitz legacy plan (2026-09) allocates 50 RPS PER ENDPOINT in separate
buckets, but the client historically enforced ONE global lane — /email,
/domain-to-linkedin and /v2/search/* all serialized behind the same
``_MIN_REQUEST_INTERVAL``, halving achievable concurrent throughput. These
tests pin the lane contract:

- URL → lane mapping (email / discovery / search / default)
- same-lane acquires serialize at the lane's interval
- different lanes do NOT block each other
- per-lane env overrides (BLITZ_RPS_EMAIL / _DISCOVERY / _SEARCH) win over
  the global BLITZ_RPS default
"""
from __future__ import annotations

import asyncio
import os
import sys
import time

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import blitz_client as bc  # noqa: E402


def test_lane_for_url_mapping():
    assert bc._lane_for_url("https://api.blitz-api.ai/v2/enrichment/email") == "email"
    assert (
        bc._lane_for_url("https://api.blitz-api.ai/v2/enrichment/domain-to-linkedin")
        == "discovery"
    )
    assert (
        bc._lane_for_url("https://api.blitz-api.ai/v2/enrichment/linkedin-to-domain")
        == "discovery"
    )
    assert bc._lane_for_url("https://api.blitz-api.ai/v2/enrichment/company") == "discovery"
    assert bc._lane_for_url("https://api.blitz-api.ai/v2/search/waterfall-icp-keyword") == "search"
    assert bc._lane_for_url("https://api.blitz-api.ai/v2/search/companies") == "search"
    assert bc._lane_for_url("https://api.blitz-api.ai/v2/enrichment/phone") == "default"
    assert bc._lane_for_url("https://api.blitz-api.ai/v2/enrichment/person") == "default"
    assert bc._lane_for_url("https://api.blitz-api.ai/v2/whatever") == "default"


def test_lane_env_override_wins_over_global(monkeypatch):
    monkeypatch.setenv("BLITZ_RPS", "50")
    monkeypatch.setenv("BLITZ_RPS_EMAIL", "200")
    assert bc._lane_rps("email") == 200.0
    assert bc._lane_rps("discovery") == 50.0
    monkeypatch.delenv("BLITZ_RPS_EMAIL")
    assert bc._lane_rps("email") == 50.0


def test_same_lane_serializes(monkeypatch):
    """Two acquires on ONE lane must be spaced >= the lane interval."""
    monkeypatch.setenv("BLITZ_RPS", "100")  # 10ms interval
    monkeypatch.delenv("BLITZ_RPS_EMAIL", raising=False)
    bc._lane_last_request["email"] = 0.0

    async def run():
        await bc._acquire_rate_limit("email")
        await bc._acquire_rate_limit("email")

    start = time.monotonic()
    asyncio.run(run())
    elapsed = time.monotonic() - start
    assert elapsed >= 0.009, f"same-lane acquires must serialize, took {elapsed:.4f}s"


def test_different_lanes_do_not_block(monkeypatch):
    """An acquire on lane B must not wait behind lane A's interval."""
    monkeypatch.setenv("BLITZ_RPS", "10")  # 100ms interval — cross-blocking would be obvious
    # email lane JUST fired; discovery lane is idle
    bc._lane_last_request["email"] = time.monotonic()
    bc._lane_last_request["discovery"] = 0.0

    async def run():
        await bc._acquire_rate_limit("discovery")

    start = time.monotonic()
    asyncio.run(run())
    elapsed = time.monotonic() - start
    assert elapsed < 0.05, f"discovery lane blocked by email lane: {elapsed:.4f}s"
