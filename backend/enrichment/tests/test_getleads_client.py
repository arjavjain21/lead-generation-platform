"""
Comprehensive unit tests for ``enrichment.getleads_client``.

Covers:
  * ``find_email`` happy paths:
      - found with email_status VALID -> email + verification_status "Valid"
        + phone captured.
      - not found (success:true, email:null, data:null) -> None.
      - partial (data populated, no email_address) -> None.
  * ``find_email`` input validation (empty name/domain, missing API key).
  * ``find_email`` kill switch (``ENABLE_GETLEADS=false``).
  * ``find_email`` error handling: 402 insufficient credits (_ProviderError),
    429 retry succeeds, 500 retries exhausted, network exception.
  * ``find_email`` request payload + auth header shape.
  * ``find_emails_batch`` chunking at 100: preserves order + length.
  * ``find_emails_batch`` 250-item input -> 3 calls (100/100/50), never >100.
  * ``find_emails_batch`` defensive padding on short API response.
  * Rate limiter triggers a sleep in a tight loop.
  * ``get_credits_balance`` returns None (no documented endpoint).

Mocking strategy: ``httpx.MockTransport`` (built into httpx, no extra dep).
No real HTTP calls are ever made.

Async pattern: the project does NOT use ``pytest-asyncio`` (it is not in
the venv). We follow the project convention of wrapping the async code
under test in ``asyncio.run(...)`` inside synchronous test functions, the
same pattern used by ``test_smartprospect_client.py``.
"""

from __future__ import annotations

import asyncio
import json
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

from enrichment import getleads_client as gl  # noqa: E402
from enrichment import pipeline  # noqa: E402  (for _ProviderError / _is_provider_error)

# The real shared limiter function, captured at import time (BEFORE the
# autouse fixture stubs it per-test). Rate-limit tests restore it.
_REAL_ACQUIRE_TOKEN = gl.rate_limiter.acquire_token


# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

_API_KEY = "test-key-xyz"
_ENDPOINT = "https://app.getleads.io/api/v1/enrich/from-person"


def _ok_response(results: list[dict[str, Any]]) -> bytes:
    """Build a success JSON body (``{ok, results, creditsRemaining}``)."""
    return json.dumps(
        {"ok": True, "results": results, "creditsRemaining": None}
    ).encode("utf-8")


def _result_item(
    first: str = "John",
    last: str = "Doe",
    domain: str = "example.com",
    email: Any = None,
    email_status: str = "VALID",
    linkedin: Any = None,
    phone: Any = None,
) -> dict[str, Any]:
    """
    Build a single from-person ``results[]`` item.

    When ``email`` is None this produces the not-found shape
    (``success:true, email:null, data:null``) — matching the live API,
    which NEVER returns ``success:false``.
    """
    if email is None:
        return {
            "first_name": first,
            "last_name": last,
            "email_domain": domain,
            "success": True,
            "email": None,
            "profileUrl": None,
            "data": None,
        }
    data: dict[str, Any] = {
        "first_name": first,
        "last_name": last,
        "email_address": email,
        "email_domain": domain,
        "email_status": email_status,
    }
    if linkedin:
        data["person_linkedin_url"] = linkedin
    if phone:
        data["cellphone"] = phone
    return {
        "first_name": first,
        "last_name": last,
        "email_domain": domain,
        "success": True,
        "email": email,
        "profileUrl": linkedin,
        "data": data,
    }


def _partial_item(
    first: str = "Troy",
    last: str = "Nelson",
    domain: str = "example.com",
    linkedin: Any = None,
    phone: Any = None,
) -> dict[str, Any]:
    """
    Build a 'partial' result item: data is a populated person record but has
    NO ``email_address`` (only ``email_domain``). The live API emits this for
    people it matched but could not find an email for.
    """
    data: dict[str, Any] = {
        "first_name": first,
        "last_name": last,
        "email_domain": domain,
        "job_title": "Manager",
    }
    if linkedin:
        data["person_linkedin_url"] = linkedin
    if phone:
        data["cellphone"] = phone
    return {
        "first_name": first,
        "last_name": last,
        "email_domain": domain,
        "success": True,
        "email": None,
        "profileUrl": linkedin,
        "data": data,
    }


def _make_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    """Build an AsyncClient backed by a MockTransport using ``handler``."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport)


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Isolate every test from the others:

    * Set a known-good API key (the module reads ``API_KEY`` at import time,
      so tests patch the module attribute directly to avoid import-order
      coupling).
    * Ensure the kill switch is ON.
    * Reset the module-level circuit breaker (shared global singleton).
    * Stub the shared rate limiter to always grant (wait=0) so tests never
      touch the SQLite limiter DB or sleep on rate limits. Tests that care
      about rate-limit behavior point the real limiter at a temp DB.
    * Patch ``asyncio.sleep`` to a no-op so retry backoffs don't slow the
      suite. Individual tests that care about sleep counts install their
      own patch.
    """
    monkeypatch.setattr(gl, "API_KEY", _API_KEY, raising=True)
    monkeypatch.setenv("ENABLE_GETLEADS", "true")

    # Reset circuit breaker internal state.
    cb = gl._getleads_circuit
    from shared.circuit_breaker import CircuitState

    cb._state = CircuitState.CLOSED
    cb._failure_count = 0
    cb._last_failure_time = 0.0
    cb._half_open_calls = 0

    # Rate limiter: always grant. The shared SQLite-backed limiter has its
    # own dedicated test module (test_rate_limiter.py); here we only need
    # the client to never block.
    monkeypatch.setattr(
        gl.rate_limiter, "acquire_token", lambda *args, **kwargs: 0.0
    )

    # Make asyncio.sleep instant across the board (retries, rate limiter).
    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(gl.asyncio, "sleep", _no_sleep, raising=True)


# ---------------------------------------------------------------------------
# find_email — happy path
# ---------------------------------------------------------------------------


class TestFindEmailHappyPath:
    def test_found_valid(self):
        """Found with email_status VALID -> email + verification_status 'Valid' + phone."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_ok_response(
                    [
                        _result_item(
                            "John",
                            "Doe",
                            "example.com",
                            email="john.doe@example.com",
                            email_status="VALID",
                            linkedin="https://www.linkedin.com/in/john-doe",
                            phone="+1 555-123-4567",
                        )
                    ]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "john.doe@example.com"
        assert result["verification_status"] == "Valid"
        assert result["phone"] == "+1 555-123-4567"
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["domain"] == "example.com"
        assert result["linkedin_url"] == "https://www.linkedin.com/in/john-doe"

    def test_found_non_valid_status_is_unknown(self):
        """email_status present but not 'VALID' -> verification_status 'unknown'."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_ok_response(
                    [
                        _result_item(
                            "Jane",
                            "Roe",
                            "acme.com",
                            email="jane@acme.com",
                            email_status="CATCH_ALL",
                        )
                    ]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "Jane", "Roe", "acme.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "jane@acme.com"
        assert result["verification_status"] == "unknown"

    def test_not_found_returns_none(self):
        """
        Not-found shape (success:true, email:null, data:null) -> None.
        The API never returns success:false; we gate on email_address.
        """
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_ok_response([_result_item("Ryan", "Verba", "jotform.com")]),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "Ryan", "Verba", "jotform.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_partial_no_email_address_returns_none(self):
        """
        Partial: data is a populated person record (job_title, etc.) but
        email_address is absent. Must be treated as not-found -> None.
        """
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_ok_response(
                    [
                        _partial_item(
                            "Troy",
                            "Nelson",
                            "example.com",
                            linkedin="https://www.linkedin.com/in/troy",
                            phone="+1 555-0000",
                        )
                    ]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "Troy", "Nelson", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_empty_results_array_returns_none(self):
        """If the API returns an empty results list, treat as not-found."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=_ok_response([]))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None


# ---------------------------------------------------------------------------
# find_email — input validation
# ---------------------------------------------------------------------------


class TestFindEmailValidation:
    def test_empty_first_name(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made for invalid input")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "", "Doe", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_empty_last_name(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made for invalid input")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_empty_company_domain(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made for invalid input")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_missing_api_key_no_http_call(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(gl, "API_KEY", "", raising=True)

        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made without API key")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None


# ---------------------------------------------------------------------------
# find_email — kill switch
# ---------------------------------------------------------------------------


class TestFindEmailKillSwitch:
    def test_kill_switch_off_returns_none_no_http(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ENABLE_GETLEADS", "false")

        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made when kill switch is off")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None


# ---------------------------------------------------------------------------
# find_email — error handling
# ---------------------------------------------------------------------------


class TestFindEmailErrors:
    def test_402_returns_insufficient_credits_provider_error(self):
        """402 surfaces as a falsy _ProviderError with insufficient_credits."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(402, json={"ok": False, "message": "insufficient credits"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        # Must be a _ProviderError, not None.
        assert pipeline._is_provider_error(result), f"expected _ProviderError, got {result!r}"
        # Falsy so cascade treats it as skip.
        assert not result
        as_dict = result.to_dict()
        assert as_dict["provider"] == "getleads"
        assert as_dict["error_type"] == "insufficient_credits"
        # find_email re-wraps with the single-contact method name.
        assert as_dict["method"] == "find_email"

    def test_429_then_200_retry_succeeds(self):
        """A 429 followed by a 200 should succeed and make exactly 2 calls."""
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return httpx.Response(429, headers={"Retry-After": "0"}, json={"ok": False, "message": "rate"})
            return httpx.Response(
                200,
                content=_ok_response(
                    [_result_item("John", "Doe", "example.com", email="john.doe@example.com")]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "john.doe@example.com"
        assert call_count["n"] == 2, f"expected exactly 2 HTTP calls, got {call_count['n']}"

    def test_500_exhausts_retries_returns_none(self):
        """500 x4 (initial + 3 retries) should exhaust retries and return None."""
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            return httpx.Response(500, json={"ok": False, "message": "server"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is None
        # 1 initial + 3 retries = 4 attempts total.
        assert call_count["n"] == 4, f"expected 4 attempts, got {call_count['n']}"

    def test_network_exception_returns_none(self):
        """httpx.ConnectError on every attempt returns None after retries."""
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            raise httpx.ConnectError("connection refused")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is None
        # 1 initial + 3 retries.
        assert call_count["n"] == 4

    def test_request_payload_and_auth_header_shape(self):
        """Verify the request body uses 'items' + Bearer auth (no query param)."""
        captured: dict[str, Any] = {}

        def handler(req: httpx.Request) -> httpx.Response:
            captured["url"] = str(req.url)
            captured["params"] = dict(req.url.params)
            captured["headers"] = dict(req.headers)
            captured["json"] = json.loads(req.content)
            return httpx.Response(
                200,
                content=_ok_response(
                    [_result_item("John", "Doe", "example.com", email="john.doe@example.com")]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        asyncio.run(go())
        assert captured["url"].startswith(_ENDPOINT)
        # Auth via Bearer header, NOT a query param.
        assert captured["headers"].get("authorization") == f"Bearer {_API_KEY}"
        assert "api_key" not in captured["params"], "GetLeads must NOT use ?api_key= query param"
        # Body uses 'items' with snake_case keys.
        assert captured["json"] == {
            "items": [
                {
                    "first_name": "John",
                    "last_name": "Doe",
                    "email_domain": "example.com",
                }
            ]
        }


# ---------------------------------------------------------------------------
# find_emails_batch — chunking
# ---------------------------------------------------------------------------


def _make_contacts(n: int) -> list[dict[str, str]]:
    return [
        {"first_name": f"First{i}", "last_name": f"Last{i}", "email_domain": f"corp{i}.com"}
        for i in range(n)
    ]


class TestFindEmailsBatchChunking:
    def test_single_contact_one_call(self):
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            sent = json.loads(req.content)["items"]
            items = [
                _result_item(
                    c["first_name"],
                    c["last_name"],
                    c["email_domain"],
                    email=f"{c['first_name'].lower()}@{c['email_domain']}",
                )
                for c in sent
            ]
            return httpx.Response(200, content=_ok_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, _make_contacts(1))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 1
        assert call_count["n"] == 1
        assert result[0]["first_name"] == "First0"
        assert result[0]["email"] == "first0@corp0.com"

    def test_101_contacts_chunks_at_100_preserves_order_and_length(self):
        """101 contacts -> 2 calls (100 + 1), order + length preserved."""
        call_count = {"n": 0}
        chunk_sizes: list[int] = []

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            sent = json.loads(req.content)["items"]
            chunk_sizes.append(len(sent))
            assert len(sent) <= 100, "must never send more than 100 items"
            items = [
                _result_item(
                    c["first_name"],
                    c["last_name"],
                    c["email_domain"],
                    email=f"{c['first_name'].lower()}@{c['email_domain']}",
                )
                for c in sent
            ]
            return httpx.Response(200, content=_ok_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, _make_contacts(101))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 101
        assert call_count["n"] == 2
        assert chunk_sizes == [100, 1]
        # Order preserved.
        for i, entry in enumerate(result):
            assert entry["first_name"] == f"First{i}"
            assert entry["last_name"] == f"Last{i}"
            assert entry["domain"] == f"corp{i}.com"
            assert entry["email"] == f"first{i}@corp{i}.com"

    def test_250_items_three_calls_100_100_50(self):
        """250-item batch -> 3 calls (100/100/50), never sends >100."""
        call_count = {"n": 0}
        chunk_sizes: list[int] = []

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            sent = json.loads(req.content)["items"]
            chunk_sizes.append(len(sent))
            assert len(sent) <= 100, "must never send more than 100 items"
            items = [
                _result_item(
                    c["first_name"],
                    c["last_name"],
                    c["email_domain"],
                    email=f"{c['first_name'].lower()}@{c['email_domain']}",
                )
                for c in sent
            ]
            return httpx.Response(200, content=_ok_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, _make_contacts(250))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 250
        assert call_count["n"] == 3
        assert chunk_sizes == [100, 100, 50]
        for i, entry in enumerate(result):
            assert entry["email"] == f"first{i}@corp{i}.com"

    def test_empty_list_no_call(self):
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            return httpx.Response(200, content=_ok_response([]))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, [])
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result == []
        assert call_count["n"] == 0

    def test_not_found_items_padded_in_place(self):
        """
        Batch where the API returns a mix of found + not-found items must
        preserve positions: found items carry the email, not-found items
        get email="" (same dict shape).
        """
        def handler(req: httpx.Request) -> httpx.Response:
            sent = json.loads(req.content)["items"]
            items = []
            for idx, c in enumerate(sent):
                if idx % 2 == 0:
                    items.append(
                        _result_item(
                            c["first_name"],
                            c["last_name"],
                            c["email_domain"],
                            email=f"{c['first_name'].lower()}@{c['email_domain']}",
                        )
                    )
                else:
                    items.append(_result_item(c["first_name"], c["last_name"], c["email_domain"]))
            return httpx.Response(200, content=_ok_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, _make_contacts(4))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 4
        # Even indices found, odd not-found.
        assert result[0]["email"] == "first0@corp0.com"
        assert result[1]["email"] == ""
        assert result[1]["verification_status"] == "unknown"
        assert result[2]["email"] == "first2@corp2.com"
        assert result[3]["email"] == ""


# ---------------------------------------------------------------------------
# find_emails_batch — defensive padding
# ---------------------------------------------------------------------------


class TestFindEmailsBatchPadding:
    def test_short_api_response_padded_with_not_found(self):
        """
        If the API returns fewer results than items sent, the output list
        still has one entry per input — missing entries are Not Found.
        """
        def handler(req: httpx.Request) -> httpx.Response:
            sent = json.loads(req.content)["items"]
            # Only return the first item's result; drop the rest.
            first = sent[0]
            return httpx.Response(
                200,
                content=_ok_response(
                    [
                        _result_item(
                            first["first_name"],
                            first["last_name"],
                            first["email_domain"],
                            email="found@x.com",
                        )
                    ]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, _make_contacts(5))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 5, "output length must match input length"
        assert result[0]["email"] == "found@x.com"
        # Remaining four are padded Not Found entries.
        for entry in result[1:]:
            assert entry["email"] == ""
            assert entry["verification_status"] == "unknown"

    def test_invalid_contacts_padded_in_place(self):
        """
        Contacts missing required fields are normalized to None and replaced
        with a Not Found entry in the original position.
        """
        contacts = [
            {"first_name": "John", "last_name": "Doe", "email_domain": "example.com"},
            {"first_name": "", "last_name": "Bad", "email_domain": "x.com"},  # invalid
            {"first_name": "Jane", "last_name": "Roe", "email_domain": "acme.com"},
        ]

        def handler(req: httpx.Request) -> httpx.Response:
            sent = json.loads(req.content)["items"]
            items = [
                _result_item(
                    c["first_name"],
                    c["last_name"],
                    c["email_domain"],
                    email=f"{c['first_name'].lower()}@{c['email_domain']}",
                )
                for c in sent
            ]
            return httpx.Response(200, content=_ok_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, contacts)
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 3
        assert result[0]["email"] == "john@example.com"
        # Middle entry was invalid -> Not Found in place.
        assert result[1]["email"] == ""
        assert result[1]["verification_status"] == "unknown"
        assert result[2]["email"] == "jane@acme.com"


# ---------------------------------------------------------------------------
# find_emails_batch — error in one chunk
# ---------------------------------------------------------------------------


class TestFindEmailsBatchPartialFailure:
    def test_first_chunk_ok_second_chunk_402(self):
        """
        On 402 (insufficient credits) in any chunk, the batch short-circuits:
        all valid contacts become Not Found entries (mirrors smartprospect).
        Output length still matches input length.
        """
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            if call_count["n"] == 1:
                sent = json.loads(req.content)["items"]
                items = [
                    _result_item(
                        c["first_name"],
                        c["last_name"],
                        c["email_domain"],
                        email="found@x.com",
                    )
                    for c in sent
                ]
                return httpx.Response(200, content=_ok_response(items))
            # Second chunk -> 402.
            return httpx.Response(402, json={"ok": False, "message": "no credits"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, _make_contacts(101))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        # Output length still matches input length.
        assert len(result) == 101
        # credit_error branch rebuilds flat_valid as all-Not-Found.
        for entry in result:
            assert entry["email"] == ""
            assert entry["verification_status"] == "unknown"
        # Exactly 2 HTTP calls — second chunk short-circuits the batch.
        assert call_count["n"] == 2

    def test_chunk_returns_none_pads_that_chunk(self):
        """
        If a chunk fails with a non-402 recoverable error (e.g. 500 exhausted),
        ``_post_enrich`` returns None and the batch pads that chunk with
        Not Found entries — without affecting other chunks.
        """
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            if call_count["n"] == 1:
                sent = json.loads(req.content)["items"]
                items = [
                    _result_item(
                        c["first_name"],
                        c["last_name"],
                        c["email_domain"],
                        email="found@x.com",
                    )
                    for c in sent
                ]
                return httpx.Response(200, content=_ok_response(items))
            # Second chunk always 500 -> exhausts retries -> None.
            return httpx.Response(500, json={"ok": False, "message": "server"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, _make_contacts(101))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 101
        # Chunk 1 (100 contacts) succeeded with emails.
        for i in range(100):
            assert result[i]["email"] == "found@x.com", f"idx {i}"
        # Chunk 2 (1 contact) failed -> Not Found.
        assert result[100]["email"] == ""
        assert result[100]["verification_status"] == "unknown"


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


class TestRateLimiting:
    def test_tight_loop_triggers_sleep(
        self, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ):
        """
        Calling find_email in a tight loop should eventually make the rate
        limiter invoke ``asyncio.sleep``. Uses the REAL shared SQLite token
        bucket against a temp DB with a frozen clock, so the bucket grants
        once (~capacity) and then denies every back-to-back call — the
        client must translate wait>0 into asyncio.sleep(wait).
        """
        from shared import rate_limiter

        # Restore the REAL acquire_token (the autouse fixture stubbed it).
        monkeypatch.setattr(
            rate_limiter, "acquire_token", _REAL_ACQUIRE_TOKEN, raising=True
        )
        rate_limiter.configure_db_path(str(tmp_path / "rl_test.db"))
        # Frozen-but-advancing wall clock: starts frozen so the bucket denies
        # back-to-back calls with wait > 0, and each rate-limit sleep advances
        # it (as real time would) so the subsequent re-acquire grants —
        # _acquire_rate_limit loops until granted, so the test terminates.
        clock = {"t": 1_234_567.0}
        monkeypatch.setattr(
            rate_limiter.time, "time", lambda: clock["t"], raising=True
        )

        # Restore a real sleep counter (override the autouse no-op for this
        # test) that also advances the clock while "sleeping".
        sleep_calls: list[float] = []

        async def _counting_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            clock["t"] += delay + 0.001
            return None

        monkeypatch.setattr(gl.asyncio, "sleep", _counting_sleep, raising=True)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_ok_response(
                    [_result_item("John", "Doe", "example.com", email="john.doe@example.com")]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                for _ in range(3):
                    await gl.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        asyncio.run(go())
        # Restore the limiter's default DB path (temp file cleanup).
        rate_limiter.configure_db_path(None)
        # With a frozen clock the shared bucket grants the first call(s)
        # (up to capacity) and denies the rest with wait > 0. Over 3
        # back-to-back calls at least one positive sleep must fire.
        assert any(d > 0 for d in sleep_calls), f"expected at least one rate-limit sleep, got {sleep_calls}"


# ---------------------------------------------------------------------------
# find_email_by_linkedin / find_emails_by_linkedin_batch (from-linkedin)
# ---------------------------------------------------------------------------

_LINKEDIN_ENDPOINT = "https://app.getleads.io/api/v1/enrich/from-linkedin"


def _li_result_item(
    linkedin_url: str,
    email: Any = None,
    email_status: str = "VALID",
) -> dict[str, Any]:
    """
    Build a single from-linkedin ``results[]`` item.

    When ``email`` is None this produces the not-found shape
    (``success:true, email:null, data:null``). The echo key is
    ``linkedinUrl`` (camelCase) — NOT ``profileUrl`` (from-person only).
    """
    if email is None:
        return {
            "linkedinUrl": linkedin_url,
            "success": True,
            "email": None,
            "data": None,
        }
    return {
        "linkedinUrl": linkedin_url,
        "success": True,
        "email": email,
        "data": {
            "first_name": "Brian",
            "last_name": "McCord",
            "person_full_name": "Brian McCord",
            "job_title": "Vice President of Safety",
            "org_company_name": "Greenwaste",
            "person_linkedin_url": linkedin_url,
            "email_address": email,
            "email_domain": "greenwaste.com",
            "email_status": email_status,
            "cellphone": "+1 555-987-6543",
            "person_city": "Discovery Bay",
            "person_country_name": "United States",
        },
    }


class TestFindEmailByLinkedin:
    def test_found_uses_linkedinurl_echo_shape(self):
        """Hit: data.email_address truthy -> full 20-key shape; linkedin_url
        falls back to the linkedinUrl echo."""
        def handler(req: httpx.Request) -> httpx.Response:
            assert str(req.url).startswith(_LINKEDIN_ENDPOINT)
            body = json.loads(req.content)
            assert body["items"] == [
                {"linkedin_url": "https://www.linkedin.com/in/brian-mccord-pg"}
            ]
            return httpx.Response(
                200,
                content=_ok_response([
                    _li_result_item(
                        "https://www.linkedin.com/in/brian-mccord-pg",
                        email="bmccord@greenwaste.com",
                    )
                ]),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email_by_linkedin(
                    client, "https://www.linkedin.com/in/brian-mccord-pg"
                )
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "bmccord@greenwaste.com"
        assert result["verification_status"] == "Valid"
        assert result["linkedin_url"] == "https://www.linkedin.com/in/brian-mccord-pg"
        assert result["phone"] == "+1 555-987-6543"
        assert result["job_title"] == "Vice President of Safety"

    def test_not_found_email_null_data_null_returns_none(self):
        """Miss shape (success:true, email:null, data:null) -> None."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_ok_response([
                    _li_result_item("http://www.linkedin.com/in/brian-helgoe-81821726")
                ]),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email_by_linkedin(
                    client, "http://www.linkedin.com/in/brian-helgoe-81821726"
                )
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_402_returns_insufficient_credits_provider_error(self):
        """402 surfaces as a falsy _ProviderError with the linkedin method name."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(402, json={"ok": False, "message": "no credits"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email_by_linkedin(
                    client, "https://www.linkedin.com/in/jane"
                )
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert pipeline._is_provider_error(result)
        assert not result
        as_dict = result.to_dict()
        assert as_dict["provider"] == "getleads"
        assert as_dict["error_type"] == "insufficient_credits"
        assert as_dict["method"] == "find_email_by_linkedin"

    def test_empty_url_no_http_call(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("no HTTP call for empty linkedin_url")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email_by_linkedin(client, "")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None


def _make_urls(n: int) -> list[str]:
    return [f"https://www.linkedin.com/in/user-{i}" for i in range(n)]


class TestFindEmailsByLinkedinBatch:
    def test_chunking_order_and_length_250(self):
        """250 URLs -> 3 calls (100/100/50), order + length preserved,
        never >100 items per request."""
        call_count = {"n": 0}
        chunk_sizes: list[int] = []

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            sent = json.loads(req.content)["items"]
            chunk_sizes.append(len(sent))
            assert len(sent) <= 100
            return httpx.Response(
                200,
                content=_ok_response([
                    _li_result_item(
                        item["linkedin_url"],
                        email=f"user{chunk_sizes[-1]}@found.com",
                    )
                    for item in sent
                ]),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_by_linkedin_batch(client, _make_urls(250))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 250
        assert call_count["n"] == 3
        assert chunk_sizes == [100, 100, 50]
        # Order preserved + URL carried through on every entry.
        for i, entry in enumerate(result):
            assert entry["linkedin_url"] == f"https://www.linkedin.com/in/user-{i}"

    def test_misses_padded_with_not_found_shape(self):
        """A mix of hits and misses: position preserved, misses carry
        email='' and the input URL."""
        def handler(req: httpx.Request) -> httpx.Response:
            sent = json.loads(req.content)["items"]
            items = [
                _li_result_item(
                    item["linkedin_url"],
                    email="hit@x.com" if i % 2 == 0 else None,
                )
                for i, item in enumerate(sent)
            ]
            return httpx.Response(200, content=_ok_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_by_linkedin_batch(client, _make_urls(4))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 4
        assert result[0]["email"] == "hit@x.com"
        assert result[1]["email"] == ""
        assert result[1]["verification_status"] == "unknown"
        assert result[1]["linkedin_url"] == "https://www.linkedin.com/in/user-1"
        assert result[2]["email"] == "hit@x.com"
        assert result[3]["email"] == ""

    def test_101_urls_two_chunks(self):
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            sent = json.loads(req.content)["items"]
            assert len(sent) <= 100
            return httpx.Response(
                200,
                content=_ok_response([
                    _li_result_item(item["linkedin_url"]) for item in sent
                ]),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_by_linkedin_batch(client, _make_urls(101))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 101
        assert call_count["n"] == 2
        # All misses -> all padded.
        assert all(e["email"] == "" for e in result)

    def test_402_short_circuits_to_all_not_found(self):
        """402 in a chunk pads that chunk with Not Found entries (no raise)."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(402, json={"ok": False, "message": "no credits"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_by_linkedin_batch(client, _make_urls(3))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 3
        assert all(e["email"] == "" for e in result)

    def test_empty_list_no_call(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("no HTTP call for empty list")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_by_linkedin_batch(client, [])
            finally:
                await client.aclose()

        assert asyncio.run(go()) == []


# ---------------------------------------------------------------------------
# get_credits_balance
# ---------------------------------------------------------------------------


class TestGetCreditsBalance:
    def test_returns_none_no_http_call(self):
        """No reliable balance endpoint -> always None, no HTTP call."""
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("get_credits_balance must not make an HTTP call")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.get_credits_balance(client)
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None


# ---------------------------------------------------------------------------
# Kill switch also applies to batch path
# ---------------------------------------------------------------------------


class TestKillSwitchBatch:
    def test_batch_returns_empty_when_kill_switch_off(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ENABLE_GETLEADS", "false")

        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("no HTTP call when kill switch is off")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_emails_batch(client, _make_contacts(3))
            finally:
                await client.aclose()

        assert asyncio.run(go()) == []


# ---------------------------------------------------------------------------
# Phase 2 (full capture, 2026-08-14) — _normalize_result_item field set
# ---------------------------------------------------------------------------


class TestNormalizeResultItemPhase2:
    """The 29-field data blob must survive normalization (not just 7 keys)."""

    RICH_DATA: dict[str, Any] = {
        "first_name": "Zac", "last_name": "Chaffin",
        "person_full_name": "Zac Chaffin",
        "job_title": "Chief Financial Officer",
        "org_company_name": "Earthworks Inc",
        "job_is_current": True,
        "job_function": "Finance & Accounting", "job_level": "C-Team",
        "person_linkedin_url": "https://www.linkedin.com/in/zac-chaffin-0475a023",
        "linkedin_connections_count": 215,
        "linkedin_headline": "CFO at Earthworks, Inc.",
        "country_name_org": "United States", "city_org": "Alvarado",
        "person_city": "Alvarado", "person_country_name": "United States",
        "revenue_range_org": "$50M to <$100M",
        "industry_linkedin_org": "Facilities Services",
        "employee_count_range_org": "201 to 500",
        "cellphone": "+1 817-825-4777",
        "email_address": "zac@earth.works", "email_domain": "earth.works",
        "email_status": "VALID",
        "email_last_verified_at": "2026-06-05T18:32:42",
    }

    RICH_ITEM: dict[str, Any] = {
        "first_name": "Zac", "last_name": "Chaffin",
        "email_domain": "earth.works",
        "profileUrl": "https://www.linkedin.com/in/zac-chaffin-0475a023",
        "email": "zac@earth.works",
        "data": dict(RICH_DATA),
    }

    def test_found_item_returns_all_phase2_keys(self):
        out = gl._normalize_result_item(self.RICH_ITEM)
        assert out["job_title"] == "Chief Financial Officer"
        assert out["linkedin_headline"] == "CFO at Earthworks, Inc."
        assert out["person_full_name"] == "Zac Chaffin"
        assert out["company_name"] == "Earthworks Inc"
        assert out["company_industry"] == "Facilities Services"
        assert out["employee_count"] == "201 to 500"
        assert out["revenue"] == "$50M to <$100M"
        assert out["city"] == "Alvarado"
        assert out["country"] == "United States"
        # int -> string coercion
        assert out["linkedin_connections"] == "215"
        assert out["email_last_verified_at"] == "2026-06-05T18:32:42"
        assert out["job_level"] == "C-Team"
        assert out["job_function"] == "Finance & Accounting"
        # Pre-existing keys unchanged.
        assert out["email"] == "zac@earth.works"
        assert out["verification_status"] == "Valid"
        assert out["phone"] == "+1 817-825-4777"

    def test_found_item_passes_raw_blob_through(self):
        out = gl._normalize_result_item(self.RICH_ITEM)
        assert out["_raw_getleads"] == self.RICH_DATA

    def test_not_found_item_mirrors_keys_but_not_raw_blob(self):
        nf = gl._normalize_result_item({
            "first_name": "Zac", "last_name": "Chaffin",
            "email_domain": "earth.works", "email": None, "data": None,
        })
        for key in (
            "job_title", "linkedin_headline", "person_full_name",
            "company_name", "company_industry", "employee_count", "revenue",
            "city", "country", "linkedin_connections",
            "email_last_verified_at", "job_level", "job_function",
        ):
            assert nf[key] == "", key
        assert "_raw_getleads" not in nf

    def test_find_email_result_carries_phase2_keys(self):
        """End-to-end through find_email: the returned dict carries the new
        fields (the pipeline's getleads_dm snapshot reads exactly these)."""
        item = {
            "first_name": self.RICH_ITEM["first_name"],
            "last_name": self.RICH_ITEM["last_name"],
            "email_domain": "earth.works",
            "profileUrl": self.RICH_ITEM["profileUrl"],
            "email": "zac@earth.works",
            "data": dict(self.RICH_DATA),
        }

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=_ok_response([item]))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await gl.find_email(client, "Zac", "Chaffin", "earth.works")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "zac@earth.works"
        assert result["job_title"] == "Chief Financial Officer"
        assert result["city"] == "Alvarado"
        assert result["revenue"] == "$50M to <$100M"
        assert result["job_level"] == "C-Team"
