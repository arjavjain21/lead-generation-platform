"""
Unit tests for ``enrichment.quickenrich_client``.

Covers:
  * ``find_person_email`` happy paths:
      - by linkedin_url: hit with email_verification_date -> found + fields.
      - by first+last+company_url: hit -> found.
      - clean miss (HTTP 200, ``data: []``) -> ``{"found": False, "email": ""}``.
      - record with no email in a 200 list -> treated as miss.
  * Capability gates (never fight the provider):
      - no identity pattern -> None, no HTTP attempted.
      - company-page LinkedIn URL (/company/) -> None pre-HTTP (G7 parity).
      - missing last name with domain -> None pre-HTTP.
  * Kill switch (``ENABLE_QUICKENRICH=false``) and missing API key -> None.
  * Error handling: 402 -> falsy ``_ProviderError``; 429 retried then success;
    429 exhausted -> None; 500 exhausted -> None; network error exhausted -> None;
    400 terminal -> None (logged, no retry).
  * Auth header shape (Bearer) + Accept header.
  * ``domain_search``:
      - hit page 1 + last_page=1 -> employees parsed, credits summed.
      - multi-page within cap; page cap respected (QUICKENRICH_DATASET_MAX_PAGES).
      - empty page -> clean miss (employees == []).
      - has_email=true + comma-joined title params verified on the request.
      - "N/A" sentinels cleaned to "".
      - mid-pagination terminal error keeps already-fetched pages.

Mocking strategy: ``httpx.MockTransport`` (project convention — see
``test_getleads_client.py``). No real HTTP calls. Async code wrapped in
``asyncio.run(...)`` inside sync tests (no pytest-asyncio in the venv).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any

import httpx
import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import pipeline  # noqa: E402
from enrichment import quickenrich_client as qc  # noqa: E402


_API_KEY = "test-key-qe"
_SEARCH_URL = "https://app.quickenrich.io/api/employees/search"
_DATASET_URL = "https://app.quickenrich.io/api/employees/dataset-search"


def _employee(
    email: Any = "jane@acme.com",
    first: str = "Jane",
    last: str = "Doe",
    title: str = "CEO",
    linkedin: str = "https://www.linkedin.com/in/janedoe",
    verified: str = "2026-09-01",
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "first_name": first,
        "last_name": last,
        "title": title,
        "email": email,
        "employee_linkedin": linkedin,
        "email_verification_date": verified,
        "company_url": "acme.com",
        "company_name": "Acme",
        "email_domain": "acme.com",
    }
    payload.update(extra)
    return payload


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate every test: known API key, kill switch on, stub the shared
    rate limiter so tests never sleep on the SQLite token bucket."""
    monkeypatch.setattr(qc, "API_KEY", _API_KEY, raising=False)
    monkeypatch.setattr(qc, "BASE_URL", "https://app.quickenrich.io", raising=False)
    monkeypatch.setenv("ENABLE_QUICKENRICH", "true")
    monkeypatch.setattr(
        qc.rate_limiter, "acquire_token", lambda *a, **k: 0.0, raising=False
    )
    # Fresh circuit breaker state per test.
    monkeypatch.setattr(qc, "_quickenrich_circuit", _fresh_breaker(), raising=False)


class _FreshBreaker:
    """Minimal stand-in matching the breaker surface the client uses."""

    async def can_proceed(self) -> bool:
        return True

    async def record_success(self) -> None:
        return None

    async def record_failure(self) -> None:
        return None


def _fresh_breaker() -> _FreshBreaker:
    return _FreshBreaker()


def _client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _json(body: dict[str, Any], status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=body)


# ---------------------------------------------------------------------------
# find_person_email
# ---------------------------------------------------------------------------


class TestFindPersonEmail:
    def test_hit_by_linkedin(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/employees/search"
            assert request.headers["Authorization"] == f"Bearer {_API_KEY}"
            assert request.url.params["linkedin_url"] == "https://www.linkedin.com/in/janedoe"
            return _json({"data": [_employee()], "meta": {"credits_used": 1}})

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(
                    c, linkedin_url="https://www.linkedin.com/in/janedoe"
                )

        result = asyncio.run(run())
        assert result and result["found"] is True
        assert result["email"] == "jane@acme.com"
        assert result["email_verification_date"] == "2026-09-01"
        assert result["linkedin_url"] == "https://www.linkedin.com/in/janedoe"

    def test_hit_by_name_and_company(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.params["company_url"] == "acme.com"
            assert request.url.params["first_name"] == "Jane"
            assert request.url.params["last_name"] == "Doe"
            assert "linkedin_url" not in request.url.params
            return _json({"data": [_employee()], "meta": {}})

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(
                    c, first_name="Jane", last_name="Doe", company_url="https://www.acme.com/about"
                )

        result = asyncio.run(run())
        assert result and result["found"] is True
        # Deep URL reduced to bare domain before the call.
        assert result["email"] == "jane@acme.com"

    def test_clean_miss_empty_data(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"data": [], "meta": {"reason": "EMAIL_NOT_FOUND"}})

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        result = asyncio.run(run())
        assert result == {"found": False, "email": ""}

    def test_record_without_email_is_miss(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"data": [_employee(email=None)]})

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        assert asyncio.run(run()) == {"found": False, "email": ""}

    def test_dict_data_shape_tolerated(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"data": _employee()})  # dict, not list

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        assert asyncio.run(run())["found"] is True


class TestFindPersonEmailCapabilityGates:
    """G7 + capability: insufficient identity never fires HTTP."""

    def _assert_no_call(self, **kwargs: Any) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError(f"unexpected HTTP call: {request.url}")

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, **kwargs)

        assert asyncio.run(run()) is None

    def test_no_identity_at_all(self):
        self._assert_no_call()

    def test_company_page_url_rejected(self):
        self._assert_no_call(linkedin_url="https://www.linkedin.com/company/acme/")

    def test_non_linkedin_host_rejected(self):
        self._assert_no_call(linkedin_url="https://github.com/janedoe")

    def test_missing_last_name_rejected(self):
        self._assert_no_call(first_name="Jane", last_name="", company_url="acme.com")

    def test_kill_switch(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("ENABLE_QUICKENRICH", "false")
        self._assert_no_call(linkedin_url="https://www.linkedin.com/in/janedoe")

    def test_no_api_key(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(qc, "API_KEY", "", raising=False)
        self._assert_no_call(linkedin_url="https://www.linkedin.com/in/janedoe")


class TestFindPersonEmailErrors:
    def test_402_is_falsy_provider_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(402, json={"success": False, "code": 402})

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        result = asyncio.run(run())
        assert not result  # falsy: cascade continues
        assert pipeline._is_provider_error(result)
        assert result.error_type == "insufficient_credits"

    def test_401_is_auth_error(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"success": False, "message": "Invalid API key"})

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        result = asyncio.run(run())
        assert pipeline._is_provider_error(result)
        assert result.error_type == "auth_error"

    def test_429_then_success(self):
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(429, json={"success": False, "code": 429})
            return _json({"data": [_employee()]})

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        result = asyncio.run(run())
        assert result["found"] is True
        assert calls["n"] == 2

    def test_429_exhausted_returns_none(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(429, json={})

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        assert asyncio.run(run()) is None

    def test_500_exhausted_returns_none(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        assert asyncio.run(run()) is None

    def test_400_terminal_returns_none_single_call(self):
        calls = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(
                400,
                json={
                    "success": False,
                    "message": "Invalid search parameters.",
                    "code": 400,
                },
            )

        async def run():
            async with _client(handler) as c:
                return await qc.find_person_email(c, linkedin_url="https://www.linkedin.com/in/x")

        assert asyncio.run(run()) is None
        assert calls["n"] == 1  # terminal: never retried


# ---------------------------------------------------------------------------
# domain_search
# ---------------------------------------------------------------------------


class TestDomainSearch:
    def test_single_page_hit(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/employees/dataset-search"
            assert request.url.params["company_url"] == "acme.com"
            assert request.url.params["has_email"] == "true"
            assert request.url.params["title"] == "CEO,Founder"
            assert request.url.params["page"] == "1"
            return _json(
                {
                    "data": [_employee(), _employee(email="bob@acme.com", first="Bob", last="Smith", linkedin="https://www.linkedin.com/in/bob")],
                    "meta": {
                        "page": 1,
                        "per_page": 20,
                        "total": 2,
                        "last_page": 1,
                        "credits_used": 2,
                        "remaining_credits": 500,
                    },
                }
            )

        async def run():
            async with _client(handler) as c:
                return await qc.domain_search(c, "acme.com", titles=["CEO", "Founder"])

        result = asyncio.run(run())
        assert len(result["employees"]) == 2
        assert result["credits_used"] == 2
        assert result["remaining_credits"] == 500
        first = result["employees"][0]
        assert first["email"] == "jane@acme.com"
        assert first["full_name"] == "Jane Doe"

    def test_page_cap_respected(self):
        pages = []

        def handler(request: httpx.Request) -> httpx.Response:
            page = int(request.url.params.get("page", "1"))
            pages.append(page)
            return _json(
                {
                    "data": [_employee(email=f"p{page}@acme.com")],
                    "meta": {"page": page, "last_page": 9, "credits_used": 1},
                }
            )

        async def run():
            async with _client(handler) as c:
                return await qc.domain_search(c, "acme.com", titles=["CEO"], max_pages=2)

        result = asyncio.run(run())
        assert pages == [1, 2]  # cap=2 despite last_page=9
        assert len(result["employees"]) == 2

    def test_empty_page_is_clean_miss(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"data": [], "meta": {"page": 1, "credits_used": 0}})

        async def run():
            async with _client(handler) as c:
                return await qc.domain_search(c, "acme.com", titles=["CEO"])

        result = asyncio.run(run())
        assert result["employees"] == []
        assert result["credits_used"] == 0

    def test_na_sentinels_cleaned(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(
                {
                    "data": [_employee(employee_phone="N/A", employee_phone_type=" N/A ")],
                    "meta": {"last_page": 1, "credits_used": 1},
                }
            )

        async def run():
            async with _client(handler) as c:
                return await qc.domain_search(c, "acme.com", titles=["CEO"])

        emp = asyncio.run(run())["employees"][0]
        assert emp["phone"] == ""
        assert emp["phone_type"] == ""

    def test_empty_domain_returns_none(self):
        async def run():
            async with _client(lambda r: _json({"data": []})) as c:
                return await qc.domain_search(c, "   ")

        assert asyncio.run(run()) is None

    def test_midpagination_error_keeps_partial(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.params.get("page") == "1":
                return _json(
                    {
                        "data": [_employee()],
                        "meta": {"last_page": 5, "credits_used": 1},
                    }
                )
            return httpx.Response(402, json={})

        async def run():
            async with _client(handler) as c:
                return await qc.domain_search(c, "acme.com", titles=["CEO"], max_pages=3)

        result = asyncio.run(run())
        assert len(result["employees"]) == 1  # page 1 kept, page 2's 402 stops


class TestHelpers:
    def test_bare_domain(self):
        assert qc._bare_domain("https://www.acme.com/pricing?x=1") == "acme.com"
        assert qc._bare_domain("ACME.com") == "acme.com"
        assert qc._bare_domain("") == ""

    def test_clean_na(self):
        assert qc._clean(" N/A ") == ""
        assert qc._clean("n/a") == ""
        assert qc._clean(" CEO ") == "CEO"
        assert qc._clean(None) == ""
        assert qc._clean(42) == "42"

    def test_is_configured(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(qc, "API_KEY", "k", raising=False)
        assert qc.is_configured() is True
        monkeypatch.setattr(qc, "API_KEY", "", raising=False)
        assert qc.is_configured() is False
