"""GetLeads decision-makers domain lookup — client contract.

Live-verified response shape (2026-09-13, /api/v1/contacts/lookup/decision-makers):
``{ok, contacts: [{First Name, Last Name, Email, Email Verification Status,
Current Job Title, Department / Function, Seniority Level, Company Name,
Company Domain, Contact City/State/Country, Company Industry (LinkedIn),
Employee Count Range, Revenue Range, Contact LinkedIn URL, Cellphone, Persona,
About Me, ...}], total_available, query_credits_used, creditsRemaining, ...}``.

This is the domain-level DISCOVERY fallback for the cascade: fires only when
contacts_db AND the Blitz waterfall found no decision makers. Tests pin:
- normalization to the canonical shape (find_email-compatible keys)
- kill switch / missing key / bad input -> []
- 402 -> falsy _ProviderError; 400 / non-200 / junk body -> []
- request body is {domain, limit, require_email} (NOT the items envelope)
"""
from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

import httpx
import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import getleads_client as gl  # noqa: E402
from enrichment import pipeline as pipeline_mod  # noqa: E402

_API_KEY = "glb_live_test_key"


_LIVE_DM_CONTACT = {
    "First Name": "Cristina",
    "Last Name": "Cordova",
    "Email": "cristina@linear.app",
    "Email Verification Status": "VALID",
    "Current Job Title": "Chief Operating Officer",
    "Department / Function": "Operations",
    "Seniority Level": "C-Team",
    "Company Name": "Linear",
    "Company Domain": "linear.app",
    "Contact City": "San Francisco",
    "Contact State": "California",
    "Contact Country": "United States",
    "Contact Continent": "North America",
    "Contact Global Region": "NORAM",
    "Company Industry (LinkedIn)": "Software Development",
    "Employee Count Range": "51 to 200",
    "Revenue Range": "",
    "Contact LinkedIn URL": "https://www.linkedin.com/in/cristinajcordova",
    "Cellphone": "+1 732-619-7402",
    "Persona": "COO / Operations Executive",
    "About Me": "I lead the Go-To-Market and Operations functions at Linear.",
}


def _ok_body(contacts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "ok": True,
        "contacts": contacts,
        "total_available": len(contacts),
        "query_credits_used": len(contacts),
        "creditsRemaining": None,
        "offset": 0,
        "limit": 5000,
        "returned": len(contacts),
        "has_more": False,
    }


@pytest.fixture(autouse=True)
def _gl_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(gl, "API_KEY", _API_KEY, raising=True)
    monkeypatch.setenv("ENABLE_GETLEADS", "true")
    # Shared limiter stubbed to grant instantly (limiter has its own tests).
    monkeypatch.setattr(gl.rate_limiter, "acquire_token", lambda *a, **k: 0.0, raising=True)
    # Reset the circuit breaker between tests.
    from shared.circuit_breaker import CircuitState

    gl._getleads_circuit._state = CircuitState.CLOSED
    gl._getleads_circuit._failure_count = 0
    gl._getleads_circuit._last_failure_time = 0.0
    gl._getleads_circuit._half_open_calls = 0
    # Instant sleeps so retry paths don't slow the suite.
    async def _no_sleep(_d: float) -> None:
        return None

    monkeypatch.setattr(gl.asyncio, "sleep", _no_sleep, raising=True)


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class TestNormalize:
    def test_canonical_shape(self):
        out = gl._normalize_dm_contact(dict(_LIVE_DM_CONTACT))
        assert out["email"] == "cristina@linear.app"
        assert out["verification_status"] == "Valid"
        assert out["first_name"] == "Cristina"
        assert out["last_name"] == "Cordova"
        assert out["domain"] == "linear.app"
        assert out["linkedin_url"].endswith("/in/cristinajcordova")
        assert out["phone"] == "+1 732-619-7402"
        assert out["job_title"] == "Chief Operating Officer"
        assert out["job_level"] == "C-Team"
        assert out["job_function"] == "Operations"
        assert out["company_name"] == "Linear"
        assert out["company_industry"] == "Software Development"
        assert out["employee_count"] == "51 to 200"
        assert out["city"] == "San Francisco"
        assert out["country"] == "United States"
        assert out["person_full_name"] == "Cristina Cordova"
        assert out["_raw_getleads"]["Persona"] == "COO / Operations Executive"

    def test_missing_keys_default_empty(self):
        out = gl._normalize_dm_contact({"Email": ""})
        assert out["email"] == ""
        assert out["verification_status"] == "unknown"
        assert out["first_name"] == ""


class TestLookupDecisionMakers:
    def test_happy_path(self):
        async def handler(req: httpx.Request) -> httpx.Response:
            assert req.url.path == "/api/v1/contacts/lookup/decision-makers"
            body = req.read()
            import json

            sent = json.loads(body)
            # Request contract: {domain, limit, require_email} — NOT items.
            assert sent == {"domain": "linear.app", "limit": 5, "require_email": True}
            return httpx.Response(200, json=_ok_body([dict(_LIVE_DM_CONTACT)]))

        async def go():
            c = _client(handler)
            try:
                return await gl.lookup_decision_makers(c, "Linear.App", limit=5)
            finally:
                await c.aclose()

        results = asyncio.run(go())
        assert len(results) == 1
        assert results[0]["email"] == "cristina@linear.app"

    def test_kill_switch_returns_empty(self, monkeypatch):
        monkeypatch.setenv("ENABLE_GETLEADS", "false")

        async def handler(req):  # pragma: no cover - must not be called
            raise AssertionError("kill switch must prevent the HTTP call")

        async def go():
            c = _client(handler)
            try:
                return await gl.lookup_decision_makers(c, "linear.app")
            finally:
                await c.aclose()

        assert asyncio.run(go()) == []

    def test_402_returns_provider_error(self):
        async def handler(req: httpx.Request) -> httpx.Response:
            return httpx.Response(402, json={"ok": False, "message": "no credits"})

        async def go():
            c = _client(handler)
            try:
                return await gl.lookup_decision_makers(c, "linear.app")
            finally:
                await c.aclose()

        out = asyncio.run(go())
        assert pipeline_mod._is_provider_error(out)

    def test_no_contacts_and_junk_body_return_empty(self):
        async def empty_handler(req):
            return httpx.Response(200, json=_ok_body([]))

        async def junk_handler(req):
            return httpx.Response(200, text="<html>gateway error</html>")

        async def go(handler):
            c = _client(handler)
            try:
                return await gl.lookup_decision_makers(c, "linear.app")
            finally:
                await c.aclose()

        assert asyncio.run(go(empty_handler)) == []
        assert asyncio.run(go(junk_handler)) == []
