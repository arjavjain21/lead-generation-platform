"""
Configuration tests for ``enrichment.mailtester_client._get_api_key``.

Background (security fix 2026-11-10 / PR-0.1): ``_get_api_key`` previously
fell back to a plaintext API key committed in source when
``MAILTESTER_API_KEY`` was unset. The fallback is removed; the key is
env-required, matching the blitz_client / phone_enrichment pattern.

Contract pinned here:
1. Missing OR empty ``MAILTESTER_API_KEY`` → ``verify_email`` raises
   ``RuntimeError`` BEFORE any HTTP call is attempted (no request can ever
   be sent without a key — and none is logged or embedded anywhere).
2. With the env var set, the key travels ONLY as the ``key`` query param of
   the proxy call, and ``_get_api_key`` returns exactly the env value.
3. No 40-char hex literal remains in the module source (the old key was one;
   it must be treated as burned and rotated ops-side).

The RuntimeError raised on missing config is the SAME type callers already
catch as the fail-open signal (validator outage) — callers accept the email
unverified with ``mailtester_code="unavailable"``. That fail-OPEN risk is
documented in the PR report: a production deploy without the env var set
silently disables verification. The env var IS set in production .env today.

Mocking strategy mirrors ``test_mailtester_timeout.py`` (httpx.MockTransport,
asyncio.run in sync tests — the project does not use pytest-asyncio).
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path
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

_TEST_KEY = "unit-test-mailtester-key"


def _make_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport)


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate module state (rate limiter, cache, breaker, sleep) per test."""
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
# 1. Missing / empty configuration → RuntimeError before any HTTP call
# ---------------------------------------------------------------------------


class TestMissingKey:
    def test_unset_env_raises_runtime_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MAILTESTER_API_KEY", raising=False)

        with pytest.raises(RuntimeError, match="MAILTESTER_API_KEY"):
            mt._get_api_key()

    def test_empty_env_raises_runtime_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAILTESTER_API_KEY", "")
        with pytest.raises(RuntimeError, match="MAILTESTER_API_KEY"):
            mt._get_api_key()

    def test_whitespace_env_raises_runtime_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAILTESTER_API_KEY", "   ")
        with pytest.raises(RuntimeError, match="MAILTESTER_API_KEY"):
            mt._get_api_key()

    def test_verify_email_raises_before_any_http_call(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No request may ever be attempted without a configured key."""
        monkeypatch.delenv("MAILTESTER_API_KEY", raising=False)
        calls = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:  # pragma: no cover - must not run
            calls["n"] += 1
            return httpx.Response(200, json={"code": "ok", "message": "valid", "email": "x"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await mt.verify_email(client, "john.doe@example.com")
            finally:
                await client.aclose()

        with pytest.raises(RuntimeError, match="MAILTESTER_API_KEY"):
            asyncio.run(go())
        assert calls["n"] == 0, "verify_email must raise before attempting any HTTP call"

    def test_runtime_error_is_the_caller_fail_open_signal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The raise must be RuntimeError (NOT ValueError/AttributeError): the
        pipeline/list_builder gates catch exactly RuntimeError and fail OPEN
        (accept the email unverified, mailtester_code="unavailable") — the
        same handling as a validator outage. Any other exception type would
        invert the policy (fail-closed drop)."""
        monkeypatch.delenv("MAILTESTER_API_KEY", raising=False)

        async def go() -> Any:
            client = _make_client(lambda req: httpx.Response(200, json={}))  # pragma: no cover
            try:
                return await mt.verify_email(client, "john.doe@example.com")
            finally:
                await client.aclose()

        # Caller-side pattern (pipeline.py contacts_db gate):
        try:
            asyncio.run(go())
        except RuntimeError:
            accepted_unverified = True  # fail-open path taken
        else:  # pragma: no cover
            accepted_unverified = False
        assert accepted_unverified, "missing-key error must remain catchable as RuntimeError (fail-open contract)"


# ---------------------------------------------------------------------------
# 2. Valid configuration → key used exactly as given, only in the query param
# ---------------------------------------------------------------------------


class TestKeyFromEnv:
    def test_get_api_key_returns_env_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAILTESTER_API_KEY", _TEST_KEY)
        assert mt._get_api_key() == _TEST_KEY

    def test_verify_email_sends_env_key_as_query_param(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAILTESTER_API_KEY", _TEST_KEY)
        seen: dict[str, str] = {}

        def handler(req: httpx.Request) -> httpx.Response:
            seen["key"] = str(req.url.params.get("key", ""))
            seen["email"] = str(req.url.params.get("email", ""))
            return httpx.Response(
                200,
                json={"code": "ok", "message": "valid", "email": "jane.doe@example.com"},
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await mt.verify_email(client, "jane.doe@example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result["valid"] is True and result["code"] == "ok"
        assert seen["key"] == _TEST_KEY
        assert seen["email"] == "jane.doe@example.com"

    def test_key_not_in_response_result(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The result dict returned to callers must never echo the key."""
        monkeypatch.setenv("MAILTESTER_API_KEY", _TEST_KEY)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json={"code": "mb", "message": "catch-all", "email": "a@example.com"},
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await mt.verify_email(client, "a@example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert _TEST_KEY not in str(result)


# ---------------------------------------------------------------------------
# 3. No hardcoded secret remains in the module source
# ---------------------------------------------------------------------------

_HEX_LITERAL_RE = re.compile(r"[0-9a-f]{40}")


class TestNoHardcodedSecret:
    def test_no_40char_hex_literal_in_module_source(self) -> None:
        """The old key was a 40-char hex token; none may remain in source."""
        source = Path(mt.__file__).read_text(encoding="utf-8")
        hits = _HEX_LITERAL_RE.findall(source)
        assert not hits, f"40-char hex literal(s) found in mailtester_client.py (count={len(hits)}) — remove hardcoded secrets"

    def test_no_api_key_fallback_expression(self) -> None:
        """Guard against regressing to an `or "<literal>"` fallback shape."""
        source = Path(mt.__file__).read_text(encoding="utf-8")
        get_key_block = source.split("def _get_api_key", 1)[1].split("def ", 1)[0]
        assert 'os.getenv("MAILTESTER_API_KEY"' in get_key_block
        assert " or \"" not in get_key_block, "_get_api_key must not fall back to a literal"
