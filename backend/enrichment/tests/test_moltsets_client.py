"""
Unit tests for ``enrichment.moltsets_client`` (2026-10-14 integration).

Covers:
  * ``find_person_email`` happy paths:
      - by linkedin_url: 200 + status=ok + email + risk A -> found, verified yes
      - by name+domain: routed to search_business_email_by_name
      - risk B -> verified yes; risk C -> verified unknown (policy: mailtester
        decides at the call site); risk F -> unknown
  * MoltSets miss semantics: HTTP 200 + status="not_found" -> None + miss
    marker recorded (NEVER a 404 interpretation)
  * Risk policy: risk D -> None, NO miss marker (bad best-email is not a
    permanent miss); custom MOLTSETS_REJECT_RISK honoured
  * Guard wiring:
      - active fair-use cooldown -> None, ZERO HTTP attempts
      - recent miss marker -> None, zero HTTP attempts
      - clean not_found -> marker recorded (second call makes no HTTP)
  * Fair-use errors: ``fair_use_limit_exceeded`` / ``fair_use_requests_exceeded``
    -> cooldown armed + falsy ``_ProviderError`` carrying the code
  * 200-with-partial-error (batch fair-use cap) -> treated as fair-use error
  * Retries: ``rate_limited`` retried then success; terminal 401 no retry
  * Kill switch + missing API key -> falsy _ProviderError
  * ``metadata.fair_use`` on 2xx -> ledgered via moltsets_guards
  * ``get_usage`` happy path + miss

Mocking strategy: ``httpx.MockTransport`` (project convention). Guards use a
temp SQLite file (blitz_miss_store test pattern). No real HTTP calls.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import moltsets_client as mc  # noqa: E402
from enrichment import moltsets_guards  # noqa: E402
from enrichment import pipeline  # noqa: E402
from shared import db as shared_db  # noqa: E402


_API_KEY = "test-key-ms"
_TOOLS_BASE = "https://api.moltsets.com/api/v1/tools"


@pytest.fixture(autouse=True)
def fresh_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolate every test: key + kill switch, stubbed rate limiter, temp DB
    for the guards, clean cooldown/miss state."""
    tmp_db = tmp_path / "moltsets_jobs.db"
    monkeypatch.setattr(shared_db, "DB_PATH", tmp_db)
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)
    monkeypatch.setattr(moltsets_guards, "_schema_initialized", False)
    monkeypatch.setattr(mc, "API_KEY", _API_KEY, raising=False)
    monkeypatch.setenv("ENABLE_MOLTSETS", "true")
    monkeypatch.delenv("MOLTSETS_REJECT_RISK", raising=False)
    monkeypatch.setattr(
        mc.rate_limiter, "acquire_token", lambda *a, **k: 0.0, raising=False
    )
    yield tmp_db
    if getattr(shared_db._local, "conn", None) is not None:
        shared_db._local.conn.close()
        shared_db._local.conn = None


def _client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _json(body: dict[str, Any], status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=body)


def _ok_results(email: str = "jane@acme.com", risk: str = "A", **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "email": email,
        "risk_score": risk,
        "first_name": "Jane",
        "last_name": "Doe",
        "linkedin_url": "https://www.linkedin.com/in/janedoe",
    }
    payload.update(extra)
    return payload


def _is_provider_error(value: Any) -> bool:
    return pipeline._is_provider_error(value)


# ---------------------------------------------------------------------------
# Endpoint selection + happy paths
# ---------------------------------------------------------------------------

class TestFindPersonEmail:
    def test_hit_by_linkedin_risk_a(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v1/tools/linkedin_to_best_email"
            assert request.headers["Authorization"] == f"Bearer {_API_KEY}"
            assert request.headers["User-Agent"]  # mandatory, non-empty
            body = json.loads(request.content)
            assert body == {"linkedin_url": "https://www.linkedin.com/in/janedoe"}
            return _json({
                "status": "ok",
                "results": _ok_results(risk="A"),
                "metadata": {"fair_use": {"records_5h": 10}},
            })

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert result is not None and result["found"] is True
        assert result["email"] == "jane@acme.com"
        assert result["risk_score"] == "A"
        assert result["verified_hint"] == "yes"
        assert result["source_endpoint"] == "linkedin_to_best_email"
        # fair-use block ledgered
        snap = moltsets_guards.latest_usage_snapshot()
        assert snap is not None and snap["source"] == "response"

    def test_hit_by_name_domain_routes_to_search(self):
        def handler(request: httpx.Request) -> httpx.Request:
            assert request.url.path == "/api/v1/tools/search_business_email_by_name"
            body = json.loads(request.content)
            assert body == {
                "first_name": "Jane",
                "last_name": "Doe",
                "company_domain": "acme.com",
            }
            return _json({"status": "ok", "results": _ok_results(risk="B")})

        result = asyncio.run(mc.find_person_email(
            _client(handler), first_name="Jane", last_name="Doe", company_domain="acme.com",
        ))
        assert result is not None
        assert result["verified_hint"] == "yes"  # risk B
        assert result["source_endpoint"] == "search_business_email_by_name"

    def test_risk_c_verified_unknown(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"status": "ok", "results": _ok_results(risk="C")})

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert result is not None
        assert result["verified_hint"] == "unknown"

    def test_no_identity_returns_none_without_http(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return _json({"status": "ok"})

        result = asyncio.run(mc.find_person_email(_client(handler), first_name="Jane"))
        assert result is None
        assert calls == []

    def test_clean_not_found_records_miss(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            # MoltSets contract: miss is 200 + status=not_found, NEVER 404.
            return _json({"status": "not_found", "results": None})

        client = _client(handler)
        key = moltsets_guards.person_key(linkedin_url="https://www.linkedin.com/in/janedoe")
        result = asyncio.run(mc.find_person_email(client, linkedin_url=key.replace("li:", "")))
        assert result is None
        assert moltsets_guards.is_recent_person_miss(key) is True
        # Second call: miss marker short-circuits — zero new HTTP.
        result2 = asyncio.run(mc.find_person_email(client, linkedin_url=key.replace("li:", "")))
        assert result2 is None
        assert len(calls) == 1

    def test_risk_d_rejected_without_miss_marker(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"status": "ok", "results": _ok_results(risk="D")})

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert result is None
        key = moltsets_guards.person_key(linkedin_url="https://www.linkedin.com/in/janedoe")
        assert moltsets_guards.is_recent_person_miss(key) is False

    def test_custom_reject_risk_env(self, monkeypatch):
        monkeypatch.setenv("MOLTSETS_REJECT_RISK", "D,F")
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"status": "ok", "results": _ok_results(risk="F")})
        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert result is None

    def test_ok_with_no_email_treated_as_miss(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"status": "ok", "results": {"message": "nothing here"}})

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert result is None


# ---------------------------------------------------------------------------
# Guards: cooldown + fair-use errors
# ---------------------------------------------------------------------------

class TestFairUseAndCooldown:
    def test_fair_use_error_arms_cooldown_and_errors(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({
                "error": {
                    "code": "fair_use_limit_exceeded",
                    "message": "15k records / 5h window exhausted",
                    "retry_after": 7200,
                }
            }, status=429)

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert _is_provider_error(result) is True
        assert getattr(result, "error_type", "") == "fair_use_limit_exceeded"
        cooldown = moltsets_guards.get_active_cooldown()
        assert cooldown is not None
        assert cooldown["code"] == "fair_use_limit_exceeded"
        assert 7100 < cooldown["remaining_seconds"] <= 7200

    def test_cooldown_active_skips_http_entirely(self):
        moltsets_guards.set_cooldown("fair_use_requests_exceeded", "spent", retry_after=3600.0)
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return _json({"status": "ok", "results": _ok_results()})

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert result is None  # skip looks exactly like a miss to the cascade
        assert calls == []
        # And no miss marker — cooldown is transient account state.
        key = moltsets_guards.person_key(linkedin_url="https://www.linkedin.com/in/janedoe")
        assert moltsets_guards.is_recent_person_miss(key) is False

    def test_weekly_window_code_also_arms(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({
                "error": {"code": "fair_use_requests_exceeded", "message": "75k/week"},
            }, status=429)

        result = asyncio.run(mc.find_person_email(
            _client(handler), first_name="J", last_name="D", company_domain="a.com",
        ))
        assert _is_provider_error(result) is True
        assert moltsets_guards.get_active_cooldown() is not None

    def test_200_partial_error_with_fair_use_code(self):
        """Batches can 200 with partial results + a fair-use error block."""
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({
                "status": "ok",
                "results": None,
                "error": {"code": "fair_use_limit_exceeded", "message": "mid-batch cap"},
                "metadata": {},
            })

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert _is_provider_error(result) is True
        assert moltsets_guards.get_active_cooldown() is not None


# ---------------------------------------------------------------------------
# Retries + kill switch
# ---------------------------------------------------------------------------

class TestRetriesAndSwitches:
    def test_rate_limited_retried_then_success(self):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            if len(attempts) < 2:
                return _json({"error": {"code": "rate_limited", "message": "slow down"}}, status=429)
            return _json({"status": "ok", "results": _ok_results()})

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert result is not None and result["found"] is True
        assert len(attempts) == 2

    def test_terminal_401_not_retried(self):
        attempts = []

        def handler(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            return _json({"error": {"code": "unauthorized", "message": "bad key"}}, status=401)

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert _is_provider_error(result) is True
        assert len(attempts) == 1

    def test_kill_switch(self, monkeypatch):
        monkeypatch.setenv("ENABLE_MOLTSETS", "false")

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("must not be reached")

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert _is_provider_error(result) is True
        assert getattr(result, "error_type", "") == "disabled"

    def test_missing_api_key(self, monkeypatch):
        monkeypatch.setattr(mc, "API_KEY", "", raising=False)

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("must not be reached")

        result = asyncio.run(mc.find_person_email(
            _client(handler), linkedin_url="https://www.linkedin.com/in/janedoe",
        ))
        assert _is_provider_error(result) is True


# ---------------------------------------------------------------------------
# get_usage
# ---------------------------------------------------------------------------

class TestGetUsage:
    def test_usage_snapshot_ledgered(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v1/tools/get_usage"
            body = json.loads(request.content)
            assert body == {"period": "week"}
            return _json({
                "status": "ok",
                "results": {"records_used": 42000, "records_limit": 75000},
            })

        result = asyncio.run(mc.get_usage(_client(handler), period="week"))
        assert result == {"records_used": 42000, "records_limit": 75000}
        snap = moltsets_guards.latest_usage_snapshot()
        assert snap is not None and snap["source"] == "get_usage"

    def test_usage_miss_returns_none(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json({"status": "not_found", "results": None})

        result = asyncio.run(mc.get_usage(_client(handler)))
        assert result is None
