"""
Comprehensive unit tests for ``enrichment.smartprospect_client``.

Covers:
  * ``find_email`` happy paths (Found/Valid, Found/null verification, Not Found).
  * ``find_email`` input validation (empty name/domain, missing API key).
  * ``find_email`` kill switch (``ENABLE_SMARTPROSPECT=false``).
  * ``find_email`` error handling: 402 insufficient credits, 429 retry
    succeeds, 500 retries exhausted, network exception.
  * ``find_emails_batch`` chunking (1, 10, 11, 25, empty).
  * ``find_emails_batch`` defensive padding on short API response.
  * ``find_emails_batch`` partial chunk failure (first ok, second 402).
  * Rate limiter triggers a sleep in a tight loop.
  * ``get_credits_balance`` returns None (no documented endpoint).

Mocking strategy: ``httpx.MockTransport`` (built into httpx, no extra dep).
No real HTTP calls are ever made.

Async pattern: the project does NOT use ``pytest-asyncio`` (it is not in
the venv). We follow the project convention of wrapping the async code
under test in ``asyncio.run(...)`` inside synchronous test functions, the
same pattern used by ``test_contacts_writer.py`` and
``test_raw_contact_collector.py``.
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

from enrichment import smartprospect_client as sp  # noqa: E402
from enrichment import pipeline  # noqa: E402  (for _ProviderError / _is_provider_error)


# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

_API_KEY = "test-key-123"
_ENDPOINT = "https://prospect-api.smartlead.ai/api/v1/search-email-leads/search-contacts/find-emails"


def _found_response(items: list[dict[str, Any]]) -> bytes:
    """Build a success JSON body with the given ``data`` items."""
    import json

    return json.dumps({"success": True, "data": items}).encode("utf-8")


def _make_client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.AsyncClient:
    """Build an AsyncClient backed by a MockTransport using ``handler``."""
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport)


def _contact(
    first: str = "John",
    last: str = "Doe",
    domain: str = "example.com",
    email: str = "john.doe@example.com",
    status: str = "Found",
    verification: Any = "Valid",
) -> dict[str, Any]:
    """Build a single API response item."""
    item: dict[str, Any] = {
        "firstName": first,
        "lastName": last,
        "companyDomain": domain,
        "email_id": email,
        "status": status,
    }
    # Mimic the real API: verification_status may be omitted entirely.
    if verification is not None:
        item["verification_status"] = verification
    return item


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Isolate every test from the others:

    * Set a known-good API key (the module reads ``API_KEY`` at import time,
      so tests patch the module attribute directly to avoid import-order
      coupling).
    * Ensure the kill switch is ON.
    * Reset the module-level circuit breaker (shared global singleton).
    * Reset the module-level rate limiter (``_last_request_time``).
    * Patch ``asyncio.sleep`` to a no-op so retry backoffs don't slow the
      suite. Individual tests that care about sleep counts install their
      own patch.
    """
    monkeypatch.setattr(sp, "API_KEY", _API_KEY, raising=True)
    monkeypatch.setenv("ENABLE_SMARTPROSPECT", "true")

    # Reset circuit breaker internal state.
    cb = sp._smartprospect_circuit
    cb._state = sp.CircuitState.CLOSED if hasattr(sp, "CircuitState") else cb._state
    # CircuitState lives in shared.circuit_breaker; reset by attribute name.
    from shared.circuit_breaker import CircuitState

    cb._state = CircuitState.CLOSED
    cb._failure_count = 0
    cb._last_failure_time = 0.0
    cb._half_open_calls = 0

    # Reset rate limiter.
    monkeypatch.setattr(sp, "_last_request_time", 0.0, raising=True)

    # Make asyncio.sleep instant across the board (retries, rate limiter).
    async def _no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(sp.asyncio, "sleep", _no_sleep, raising=True)


# ---------------------------------------------------------------------------
# find_email — happy path
# ---------------------------------------------------------------------------


class TestFindEmailHappyPath:
    def test_found_valid(self):
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_found_response([_contact()]),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "john.doe@example.com"
        assert result["status"] == "Found"
        assert result["verification_status"] == "Valid"
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["domain"] == "example.com"

    def test_found_null_verification_status(self):
        """verification_status omitted from API → normalized to None."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_found_response(
                    [_contact(verification=None)]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "john.doe@example.com"
        # The normalizer returns item.get("verification_status") which is
        # None when the key is absent.
        assert result["verification_status"] is None

    def test_not_found_returns_dict_with_empty_email(self):
        """
        Not-found contacts still return a dict (email="", status="Not Found")
        per the docstring, so callers can treat the result uniformly.
        """
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_found_response(
                    [
                        {
                            "firstName": "John",
                            "lastName": "Doe",
                            "companyDomain": "example.com",
                            "email_id": "",
                            "status": "Not Found",
                        }
                    ]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == ""
        assert result["status"] == "Not Found"

    def test_empty_data_array_returns_not_found_shape(self):
        """If the API returns an empty data list, we still get a Not Found dict."""
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=_found_response([]))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == ""
        assert result["status"] == "Not Found"

    def test_invalid_status_discards_email(self):
        """
        Regression: real SmartLead API sometimes returns status="Invalid"
        with a populated email_id (the candidate pattern was found but the
        verifier flagged it as bad). We must DISCARD the email so the cascade
        falls through to WizLeads/BetterEnrich — but preserve the raw status
        on the returned dict for audit/logging.
        """
        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                content=_found_response(
                    [
                        {
                            "firstName": "Sundar",
                            "lastName": "Pichai",
                            "companyDomain": "google.com",
                            "email_id": "sundar.pichai@google.com",
                            "status": "Invalid",
                            "verification_status": None,
                        }
                    ]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "Sundar", "Pichai", "google.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "", f"Invalid email must be discarded, got {result['email']!r}"
        assert result["status"] == "Invalid", "Status preserved for audit"


# ---------------------------------------------------------------------------
# find_email — input validation
# ---------------------------------------------------------------------------


class TestFindEmailValidation:
    def _call_count(self) -> int:
        # We detect whether an HTTP call was made by using a handler that
        # raises if invoked; the absence of the exception means no call.
        # Simpler: return a sentinel via closure.
        raise AssertionError("HTTP call should not be made for invalid input")

    def test_empty_first_name(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made for invalid input")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "", "Doe", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_empty_last_name(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made for invalid input")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_empty_company_domain(self):
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made for invalid input")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None

    def test_missing_api_key_no_http_call(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(sp, "API_KEY", "", raising=True)

        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made without API key")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None


# ---------------------------------------------------------------------------
# find_email — kill switch
# ---------------------------------------------------------------------------


class TestFindEmailKillSwitch:
    def test_kill_switch_off_returns_none_no_http(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", "false")

        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("HTTP call should not be made when kill switch is off")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
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
            return httpx.Response(402, json={"error": "insufficient credits"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        # Must be a _ProviderError, not None.
        assert pipeline._is_provider_error(result), f"expected _ProviderError, got {result!r}"
        # Falsy so cascade treats it as skip.
        assert not result
        as_dict = result.to_dict()
        assert as_dict["provider"] == "smartprospect"
        assert as_dict["error_type"] == "insufficient_credits"
        # find_email re-wraps with the single-contact method name.
        assert as_dict["method"] == "find_email"

    def test_429_then_200_retry_succeeds(self):
        """A 429 followed by a 200 should succeed and make exactly 2 calls."""
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return httpx.Response(429, headers={"Retry-After": "0"}, json={"error": "rate"})
            return httpx.Response(200, content=_found_response([_contact()]))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is not None
        assert result["email"] == "john.doe@example.com"
        assert call_count["n"] == 2, f"expected exactly 2 HTTP calls, got {call_count['n']}"

    def test_500_exhausts_retries_returns_none(self):
        """
        500 x4 (initial + 3 retries) should exhaust retries and return None.

        ``find_email`` converts the underlying ``None`` to ``None``.
        """
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            return httpx.Response(500, json={"error": "server"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
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
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result is None
        # 1 initial + 3 retries.
        assert call_count["n"] == 4

    def test_request_payload_shape(self):
        """Verify the request body and query params match the API contract."""
        captured: dict[str, Any] = {}

        def handler(req: httpx.Request) -> httpx.Response:
            captured["url"] = str(req.url)
            captured["params"] = dict(req.url.params)
            captured["json"] = __import__("json").loads(req.content)
            return httpx.Response(200, content=_found_response([_contact()]))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        asyncio.run(go())
        assert captured["url"].startswith(_ENDPOINT)
        assert captured["params"]["api_key"] == _API_KEY
        assert captured["json"] == {
            "contacts": [
                {
                    "firstName": "John",
                    "lastName": "Doe",
                    "companyDomain": "example.com",
                }
            ]
        }


# ---------------------------------------------------------------------------
# find_emails_batch — chunking
# ---------------------------------------------------------------------------


def _make_contacts(n: int) -> list[dict[str, str]]:
    return [
        {"firstName": f"First{i}", "lastName": f"Last{i}", "companyDomain": f"corp{i}.com"}
        for i in range(n)
    ]


class TestFindEmailsBatchChunking:
    def test_one_contact_single_call(self):
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            body = __import__("json").loads(req.content)
            sent = body["contacts"][0]
            return httpx.Response(
                200,
                content=_found_response(
                    [_contact(first=sent["firstName"], last=sent["lastName"], domain=sent["companyDomain"])]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, _make_contacts(1))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 1
        assert call_count["n"] == 1
        assert result[0]["first_name"] == "First0"

    def test_ten_contacts_single_call(self):
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            body = __import__("json").loads(req.content)
            items = [
                _contact(first=c["firstName"], last=c["lastName"], domain=c["companyDomain"])
                for c in body["contacts"]
            ]
            return httpx.Response(200, content=_found_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, _make_contacts(10))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 10
        assert call_count["n"] == 1, "10 contacts should fit in one chunk"

    def test_eleven_contacts_two_calls(self):
        call_count = {"n": 0}
        chunk_sizes: list[int] = []

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            body = __import__("json").loads(req.content)
            chunk_sizes.append(len(body["contacts"]))
            items = [
                _contact(first=c["firstName"], last=c["lastName"], domain=c["companyDomain"])
                for c in body["contacts"]
            ]
            return httpx.Response(200, content=_found_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, _make_contacts(11))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 11
        assert call_count["n"] == 2
        assert chunk_sizes == [10, 1]
        # Order preserved.
        for i, entry in enumerate(result):
            assert entry["first_name"] == f"First{i}"
            assert entry["last_name"] == f"Last{i}"
            assert entry["domain"] == f"corp{i}.com"

    def test_twentyfive_contacts_three_calls(self):
        call_count = {"n": 0}
        chunk_sizes: list[int] = []

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            body = __import__("json").loads(req.content)
            chunk_sizes.append(len(body["contacts"]))
            items = [
                _contact(first=c["firstName"], last=c["lastName"], domain=c["companyDomain"])
                for c in body["contacts"]
            ]
            return httpx.Response(200, content=_found_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, _make_contacts(25))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 25
        assert call_count["n"] == 3
        assert chunk_sizes == [10, 10, 5]
        for i, entry in enumerate(result):
            assert entry["first_name"] == f"First{i}"

    def test_empty_list_no_call(self):
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            return httpx.Response(200, content=_found_response([]))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, [])
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert result == []
        assert call_count["n"] == 0


# ---------------------------------------------------------------------------
# find_emails_batch — defensive padding
# ---------------------------------------------------------------------------


class TestFindEmailsBatchPadding:
    def test_short_api_response_padded_with_not_found(self):
        """
        If the API returns fewer data items than contacts sent, the output
        list still has one entry per input — missing entries are Not Found.
        """
        def handler(req: httpx.Request) -> httpx.Response:
            body = __import__("json").loads(req.content)
            # Only return the first contact's result; drop the rest.
            first = body["contacts"][0]
            return httpx.Response(
                200,
                content=_found_response(
                    [_contact(first=first["firstName"], last=first["lastName"], domain=first["companyDomain"])]
                ),
            )

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, _make_contacts(5))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 5, "output length must match input length"
        assert result[0]["email"] == "john.doe@example.com"
        # Remaining four are padded Not Found entries.
        for entry in result[1:]:
            assert entry["email"] == ""
            assert entry["status"] == "Not Found"

    def test_invalid_contacts_padded_in_place(self):
        """
        Contacts missing required fields are normalized to None and replaced
        with a Not Found entry in the original position.
        """
        contacts = [
            {"firstName": "John", "lastName": "Doe", "companyDomain": "example.com"},
            {"firstName": "", "lastName": "Bad", "companyDomain": "x.com"},  # invalid
            {"firstName": "Jane", "lastName": "Roe", "companyDomain": "acme.com"},
        ]

        def handler(req: httpx.Request) -> httpx.Response:
            body = __import__("json").loads(req.content)
            items = [
                _contact(first=c["firstName"], last=c["lastName"], domain=c["companyDomain"])
                for c in body["contacts"]
            ]
            return httpx.Response(200, content=_found_response(items))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, contacts)
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 3
        assert result[0]["email"] == "john.doe@example.com"
        # Middle entry was invalid → Not Found in place.
        assert result[1]["email"] == ""
        assert result[1]["status"] == "Not Found"
        assert result[2]["email"] == "john.doe@example.com"


# ---------------------------------------------------------------------------
# find_emails_batch — error in one chunk
# ---------------------------------------------------------------------------


class TestFindEmailsBatchPartialFailure:
    def test_first_chunk_ok_second_chunk_402(self):
        """
        Read-the-implementation behavior for a 402 on chunk 2:

        On insufficient credits the batch short-circuits: the whole
        ``valid_payload`` (every valid contact, including chunk 1) is
        re-emitted as Not Found entries. This is the documented contract
        in the docstring ("the remaining contacts are returned as Not
        Found entries") and the code path at the ``credit_error`` branch
        rebuilds ``flat_valid`` from scratch.

        So the final output is ALL Not Found — not "chunk 1 results plus
        Not Found padding for chunk 2". This is arguably surprising but
        intentional (the cascade is expected to detect the failure via
        other channels). We assert the actual behavior here.
        """
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            if call_count["n"] == 1:
                body = __import__("json").loads(req.content)
                items = [
                    _contact(first=c["firstName"], last=c["lastName"], domain=c["companyDomain"])
                    for c in body["contacts"]
                ]
                return httpx.Response(200, content=_found_response(items))
            # Second chunk → 402.
            return httpx.Response(402, json={"error": "no credits"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, _make_contacts(11))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        # Output length still matches input length.
        assert len(result) == 11
        # credit_error branch rebuilds flat_valid as all-Not-Found.
        for entry in result:
            assert entry["email"] == ""
            assert entry["status"] == "Not Found"
        # Exactly 2 HTTP calls — second chunk short-circuits the batch.
        assert call_count["n"] == 2

    def test_chunk_returns_none_pads_that_chunk(self):
        """
        If a chunk fails with a non-402 recoverable error (e.g. 500 exhausted),
        ``_post_find_emails`` returns None and the batch pads that chunk with
        Not Found entries — without affecting other chunks.
        """
        call_count = {"n": 0}

        def handler(req: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            if call_count["n"] == 1:
                body = __import__("json").loads(req.content)
                items = [
                    _contact(first=c["firstName"], last=c["lastName"], domain=c["companyDomain"])
                    for c in body["contacts"]
                ]
                return httpx.Response(200, content=_found_response(items))
            # Second chunk always 500 → exhausts retries → None.
            return httpx.Response(500, json={"error": "server"})

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, _make_contacts(11))
            finally:
                await client.aclose()

        result = asyncio.run(go())
        assert len(result) == 11
        # Chunk 1 (10 contacts) succeeded with emails.
        for i in range(10):
            assert result[i]["email"] == "john.doe@example.com", f"idx {i}"
        # Chunk 2 (1 contact) failed → Not Found.
        assert result[10]["email"] == ""
        assert result[10]["status"] == "Not Found"


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


class TestRateLimiting:
    def test_tight_loop_triggers_sleep(self, monkeypatch: pytest.MonkeyPatch):
        """
        Calling find_email in a tight loop should eventually make the rate
        limiter invoke ``asyncio.sleep`` (the min request interval is
        ~33ms, so back-to-back calls will need to wait).
        """
        # Restore a real sleep counter (override the autouse no-op for this test).
        sleep_calls: list[float] = []

        async def _counting_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            return None

        monkeypatch.setattr(sp.asyncio, "sleep", _counting_sleep, raising=True)

        def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=_found_response([_contact()]))

        async def go() -> Any:
            client = _make_client(handler)
            try:
                for _ in range(5):
                    await sp.find_email(client, "John", "Doe", "example.com")
            finally:
                await client.aclose()

        asyncio.run(go())
        # At 30 RPS the limiter sleeps when elapsed < ~33ms. Over 5 sequential
        # calls at least one sleep should fire (the first call sees
        # _last_request_time == 0.0 so it won't sleep, subsequent ones likely
        # will). Assert at least one positive sleep was requested.
        assert any(d > 0 for d in sleep_calls), f"expected at least one rate-limit sleep, got {sleep_calls}"


# ---------------------------------------------------------------------------
# get_credits_balance
# ---------------------------------------------------------------------------


class TestGetCreditsBalance:
    def test_returns_none_no_http_call(self):
        """No documented balance endpoint → always None, no HTTP call."""
        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("get_credits_balance must not make an HTTP call")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.get_credits_balance(client)
            finally:
                await client.aclose()

        assert asyncio.run(go()) is None


# ---------------------------------------------------------------------------
# Kill switch also applies to batch + balance paths
# ---------------------------------------------------------------------------


class TestKillSwitchBatch:
    def test_batch_returns_empty_when_kill_switch_off(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ENABLE_SMARTPROSPECT", "false")

        def handler(req: httpx.Request) -> httpx.Response:
            raise AssertionError("no HTTP call when kill switch is off")

        async def go() -> Any:
            client = _make_client(handler)
            try:
                return await sp.find_emails_batch(client, _make_contacts(3))
            finally:
                await client.aclose()

        assert asyncio.run(go()) == []
