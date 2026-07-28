"""
Tests for the 2026-07-28 /enrich throughput + OOM-hardening changes:

1. Per-worker concurrency semaphore (`_ENRICH_SEMAPHORE`) -> fast HTTP 429
   when saturated (the missing global bound that let fan-in OOM the box).
2. Mailtester circuit breaker -> fail-fast (no HTTP call) when open.
3. Mailtester TTL result memo -> a repeat validation is served from cache.
4. Shared cascade httpx clients -> lazy singletons (one per worker).

Async pattern: the project does NOT use pytest-asyncio; we wrap async code in
``asyncio.run(...)`` inside sync test functions (same convention as
``test_mailtester_timeout.py``).
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any, Callable

import httpx
import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import mailtester_client as mt  # noqa: E402
from enrichment import routes  # noqa: E402
from shared.circuit_breaker import CircuitBreaker  # noqa: E402


def _make_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.fixture(autouse=True)
def _reset_mailtester_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate mailtester module state (cache, breaker, rate limiter, sleep)."""
    monkeypatch.setattr(mt, "_last_request_time", 0.0, raising=True)
    mt._mailtester_cache.clear()
    monkeypatch.setattr(
        mt,
        "_mailtester_breaker",
        CircuitBreaker("mailtester", failure_threshold=5, recovery_timeout=60.0),
    )

    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(mt.asyncio, "sleep", _no_sleep, raising=True)


# ---------------------------------------------------------------------------
# 1. Concurrency semaphore -> 429 fast when saturated
# ---------------------------------------------------------------------------


def test_acquire_enrich_slot_returns_429_when_saturated(monkeypatch: pytest.MonkeyPatch) -> None:
    """A saturated semaphore must fail fast with HTTP 429 + Retry-After."""
    from fastapi import HTTPException

    sem = asyncio.Semaphore(1)
    monkeypatch.setattr(routes, "_ENRICH_SEMAPHORE", sem)
    monkeypatch.setattr(routes, "ENRICH_QUEUE_TIMEOUT", 0.1)

    # Saturate the single slot (value 1 -> 0).
    asyncio.run(sem.acquire())

    with pytest.raises(HTTPException) as ei:
        asyncio.run(routes._acquire_enrich_slot())

    assert ei.value.status_code == 429
    assert ei.value.headers.get("Retry-After") == "3"


def test_acquire_enrich_slot_passes_through_when_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """With a free slot, acquire succeeds and the caller must release it."""
    sem = asyncio.Semaphore(1)
    monkeypatch.setattr(routes, "_ENRICH_SEMAPHORE", sem)
    monkeypatch.setattr(routes, "ENRICH_QUEUE_TIMEOUT", 1.0)

    asyncio.run(routes._acquire_enrich_slot())  # should not raise
    assert sem._value == 0  # acquired, not yet released


# ---------------------------------------------------------------------------
# 2. Mailtester circuit breaker -> fail fast (no HTTP) when open
# ---------------------------------------------------------------------------


def test_mailtester_breaker_fails_fast_when_open() -> None:
    """An open breaker must FAIL OPEN immediately, making zero HTTP calls."""
    breaker = CircuitBreaker("mailtester", failure_threshold=5, recovery_timeout=60.0)

    async def open_it() -> None:
        for _ in range(5):
            await breaker.record_failure()

    asyncio.run(open_it())
    mt._mailtester_breaker = breaker  # forced OPEN

    call_count = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(200, json={"code": "ok"})

    async def go() -> Any:
        client = _make_client(handler)
        try:
            return await mt.verify_email(client, "person@example.com")
        finally:
            await client.aclose()

    with pytest.raises(RuntimeError, match="circuit open"):
        asyncio.run(go())
    assert call_count["n"] == 0, "breaker should fail fast without calling the upstream"


# ---------------------------------------------------------------------------
# 3. TTL result memo -> repeat validation served from cache
# ---------------------------------------------------------------------------


def test_mailtester_cache_serves_repeat_validation_from_cache() -> None:
    """A second validation of the same email must not hit the upstream."""
    call_count = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        return httpx.Response(
            200, json={"code": "ok", "message": "valid", "email": "person@example.com"}
        )

    async def go() -> tuple[Any, Any]:
        client = _make_client(handler)
        try:
            r1 = await mt.verify_email(client, "person@example.com")
            r2 = await mt.verify_email(client, "person@example.com")
            return r1, r2
        finally:
            await client.aclose()

    r1, r2 = asyncio.run(go())
    assert r1["valid"] is True
    assert r2["valid"] is True
    assert call_count["n"] == 1, "second call should be served from the TTL cache"


def test_mailtester_cache_does_not_cache_failures() -> None:
    """A fail-open (timeout) must NOT be cached — the next call still tries."""
    call_count = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        call_count["n"] += 1
        raise httpx.ReadTimeout("read timed out")

    async def go() -> None:
        client = _make_client(handler)
        try:
            for _ in range(2):
                with pytest.raises(RuntimeError):
                    await mt.verify_email(client, "flaky@example.com")
        finally:
            await client.aclose()

    asyncio.run(go())
    # Both calls attempted the upstream (nothing cached on failure): each call
    # does _MAX_RETRIES + 1 attempts, and the fresh fixture breaker (threshold
    # 5) stays closed across these 2 calls, so neither is short-circuited.
    assert call_count["n"] == 2 * (mt._MAX_RETRIES + 1)


# ---------------------------------------------------------------------------
# 4. Shared cascade httpx clients -> lazy singletons
# ---------------------------------------------------------------------------


def test_shared_http_clients_are_singletons() -> None:
    """_get_blitz_http / _get_contacts_http return one client per worker."""
    routes._shared_blitz_http = None
    routes._shared_contacts_http = None
    try:
        blitz_a = routes._get_blitz_http()
        blitz_b = routes._get_blitz_http()
        contacts = routes._get_contacts_http()
        assert blitz_a is blitz_b, "blitz client must be a singleton"
        assert blitz_a is not contacts, "blitz and contacts clients must differ"
    finally:
        # Close any created clients and reset so other tests are unaffected.
        for c in (routes._shared_blitz_http, routes._shared_contacts_http):
            if c is not None:
                asyncio.run(c.aclose())
        routes._shared_blitz_http = None
        routes._shared_contacts_http = None


# ---------------------------------------------------------------------------
# 5. /enrich response cache (key normalization, TTL, eviction, disable)
# ---------------------------------------------------------------------------

from types import SimpleNamespace


def _req(**kw: Any) -> Any:
    """Build a minimal request-like object for cache-key tests."""
    base = dict(
        domain=None, full_name=None, first_name=None, last_name=None,
        linkedin_url=None, company_linkedin_url=None, force_provider=None,
        selected_providers=None, max_results=5, titles=None, source=None, cascade=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def test_cache_key_normalizes_domain_scheme_case_and_slash() -> None:
    a = routes._enrich_cache_key(_req(domain="http://castrovalleynaturalgrocery.com/"))
    b = routes._enrich_cache_key(_req(domain="https://CastroValleyNaturalGrocery.com"))
    c = routes._enrich_cache_key(_req(domain="castrovalleynaturalgrocery.com"))
    assert a == b == c, "scheme/case/trailing-slash variants must share a cache entry"


def test_cache_key_differs_on_name_and_restrictors() -> None:
    base = routes._enrich_cache_key(_req(domain="x.com", full_name="Jane Doe"))
    assert base != routes._enrich_cache_key(_req(domain="x.com", full_name="John Doe"))
    assert base != routes._enrich_cache_key(_req(domain="x.com", full_name="Jane Doe", force_provider="blitz"))


def test_cache_set_get_roundtrip() -> None:
    routes._enrich_response_cache.clear()
    routes._enrich_cache_set("k1", {"contacts": ["a"]})
    assert routes._enrich_cache_get("k1") == {"contacts": ["a"]}
    assert routes._enrich_cache_get("missing") is None


def test_cache_expires_after_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    routes._enrich_response_cache.clear()
    t0 = [1000.0]
    monkeypatch.setattr(routes.time, "monotonic", lambda: t0[0])
    routes._enrich_cache_set("k", "v")  # expiry = now + TTL
    assert routes._enrich_cache_get("k") == "v"
    t0[0] += routes.ENRICH_CACHE_TTL + 0.01  # advance past TTL
    assert routes._enrich_cache_get("k") is None


def test_cache_evicts_soonest_expiring_when_full(monkeypatch: pytest.MonkeyPatch) -> None:
    routes._enrich_response_cache.clear()
    monkeypatch.setattr(routes, "ENRICH_CACHE_MAX", 3)
    t0 = [0.0]
    monkeypatch.setattr(routes.time, "monotonic", lambda: t0[0])
    t0[0] = 0; routes._enrich_cache_set("a", 1)
    t0[0] = 10; routes._enrich_cache_set("b", 2)
    t0[0] = 20; routes._enrich_cache_set("c", 3)  # full
    t0[0] = 30; routes._enrich_cache_set("d", 4)  # evict soonest-expiring = "a"
    assert "a" not in routes._enrich_response_cache
    assert "d" in routes._enrich_response_cache
    assert len(routes._enrich_response_cache) == 3


def test_cache_disabled_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    routes._enrich_response_cache.clear()
    monkeypatch.setattr(routes, "ENRICH_RESPONSE_CACHE", False)
    routes._enrich_cache_set("k", "v")  # no-op when disabled
    assert routes._enrich_cache_get("k") is None
