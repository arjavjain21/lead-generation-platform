"""
Regression tests for the timeout-handler bug in ``enrichment.mailtester_client``.

Background: the retry block previously did ``except httpx.TimeoutError`` which
does not exist in httpx (the correct base class for all timeout subclasses is
``httpx.TimeoutException``). When the HTTP call raised a timeout
(``ReadTimeout``/``ConnectTimeout``/etc.), evaluating the bogus attribute
raised ``AttributeError``. That AttributeError escaped the retry/fail-open
logic (sibling ``except`` clauses do not catch errors raised while evaluating
another handler's type expression) and propagated to callers, which catch it
generically and DROP the email being verified — inverting the intended
fail-OPEN policy into fail-CLOSED.

These tests pin the contract: a timeout must be retried, and when all retries
are exhausted ``verify_email`` must FAIL OPEN (raise ``RuntimeError``), never
``AttributeError``.

Mocking strategy: ``httpx.MockTransport`` (built into httpx, no extra dep).
No real HTTP calls are ever made.

Async pattern: the project does NOT use ``pytest-asyncio`` (not in the venv).
We follow the project convention of wrapping the async code under test in
``asyncio.run(...)`` inside synchronous test functions, the same pattern used
by ``test_smartprospect_client.py`` and ``test_raw_contact_collector.py``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any, Callable

import httpx
import pytest

# Make sure the backend root is on sys.path so `enrichment` is importable
# regardless of where pytest is invoked from.
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import mailtester_client as mt  # noqa: E402
from shared.circuit_breaker import CircuitBreaker  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    """Build an AsyncClient backed by a MockTransport using ``handler``."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport)


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Isolate every test:

    * Reset the module-level rate limiter so the first call doesn't sleep.
    * Clear the TTL result cache and reset the circuit breaker so a prior
      test's cached success / accumulated failures can't leak in.
    * Patch ``asyncio.sleep`` to a no-op so retry backoffs don't slow the
      suite (attempt 3 backoff is up to 16s, totalling >30s real time).
    """
    monkeypatch.setattr(mt, "_last_request_time", 0.0, raising=True)
    mt._mailtester_cache.clear()
    # Fresh breaker so failures accumulated in other tests can't open the
    # circuit and short-circuit this test's retry path.
    monkeypatch.setattr(
        mt,
        "_mailtester_breaker",
        CircuitBreaker("mailtester", failure_threshold=5, recovery_timeout=60.0),
    )

    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(mt.asyncio, "sleep", _no_sleep, raising=True)


# ---------------------------------------------------------------------------
# Timeout handler — retry + fail-open contract
# ---------------------------------------------------------------------------


class TestTimeoutHandler:
    def test_readtimeout_retried_then_fails_open(self):
        """
        Every call raises ``httpx.ReadTimeout`` (a ``TimeoutException``
        subclass). After exhausting retries, ``verify_email`` must FAIL OPEN
        by raising ``RuntimeError`` — it must never leak ``AttributeError``
        (the original bug from ``except httpx.TimeoutError``).
        """
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            raise httpx.ReadTimeout("read timed out")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await mt.verify_email(client, "john.doe@example.com")
            finally:
                await client.aclose()

        with pytest.raises(RuntimeError, match="failing open"):
            asyncio.run(go())

        # 1 initial + _MAX_RETRIES retries.
        assert call_count["n"] == mt._MAX_RETRIES + 1, (
            f"expected {mt._MAX_RETRIES + 1} attempts, got {call_count['n']}"
        )

    def test_readtimeout_then_success(self):
        """A timeout on attempt 1 followed by a 200 should succeed with 2 calls."""
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise httpx.ReadTimeout("read timed out")
            return httpx.Response(
                200,
                json={"code": "ok", "message": "valid", "email": "john.doe@example.com"},
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await mt.verify_email(client, "john.doe@example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result["valid"] is True
        assert result["code"] == "ok"
        assert result["email"] == "john.doe@example.com"
        assert call_count["n"] == 2, f"expected exactly 2 HTTP calls, got {call_count['n']}"

    def test_connecttimeout_also_handled(self):
        """
        Every ``TimeoutException`` subclass (ConnectTimeout, PoolTimeout, ...)
        must hit the retry path — not just ``ReadTimeout``. This guards the
        base-class catch.
        """
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            raise httpx.ConnectTimeout("connect timed out")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await mt.verify_email(client, "john.doe@example.com")
            finally:
                await client.aclose()

        with pytest.raises(RuntimeError, match="failing open"):
            asyncio.run(go())
        assert call_count["n"] == mt._MAX_RETRIES + 1, (
            f"expected {mt._MAX_RETRIES + 1} attempts, got {call_count['n']}"
        )

    def test_no_attributeerror_leaks_on_timeout(self):
        """
        Explicit regression: a timeout must not surface as AttributeError.
        Under the old ``except httpx.TimeoutError`` code, this test raised
        ``AttributeError: module 'httpx' has no attribute 'TimeoutError'``.
        """
        def handler(req: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("read timed out")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await mt.verify_email(client, "john.doe@example.com")
            finally:
                await client.aclose()

        try:
            asyncio.run(go())
        except RuntimeError:
            # Expected fail-open path.
            pass
        except AttributeError as exc:  # pragma: no cover - regression guard
            pytest.fail(f"timeout leaked AttributeError instead of failing open: {exc}")
        else:
            pytest.fail("expected RuntimeError (fail open) on persistent timeout")
