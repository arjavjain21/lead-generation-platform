"""
Unit tests for ``enrichment.leadocean_client`` (2026-11-06 integration).

Live-verified contract (probe session 2026-11-06):
- ``GET /v2/people/email/work?linkedin_url=`` → 200 ``{success, data: V2Email | null}``;
  HTTP 404 = no profile. V2Email = {email, status, verified_batch_date}.
- ``GET /v2/people/email/personal?linkedin_url=`` → 200 ``{success, data: [{email,
  verified_batch_date}...]}`` — ranked list, NO status. Empty list = no personal.
- Misses bill 1 record → miss-store is budget protection (7d TTL).
- Daily 1M wall throttles instead of erroring → cooldown makes it an instant skip.

Covers:
  * work-email hit: verified status → verified_hint yes
  * work-email hit: catch_all/untested → hint unknown (mailtester at call site)
  * work null → personal fallback runs (ranked list, first wins, no status)
  * 404 (no profile) → miss marker, NO personal call
  * rejected status (invalid/role/abuse/spam_trap) → personal fallback runs
  * work null + personal empty → definitive miss marker
  * guards: cooldown skip (zero HTTP), miss-store skip (zero HTTP),
    lane-busy bounded-wait skip (no marker, retryable)
  * 402 → daily-exhausted cooldown armed + falsy _ProviderError
  * meta.notice → cooldown armed with resetsAt
  * kill switch / missing key → falsy _ProviderError
  * get_usage happy path + ledger

Mocking: httpx.MockTransport; guards on a temp SQLite file (blitz_miss_store
pattern). No real HTTP.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import leadocean_client as lo  # noqa: E402
from enrichment import leadocean_guards  # noqa: E402
from enrichment import pipeline  # noqa: E402
from shared import db as shared_db  # noqa: E402


_API_KEY = "test-key-lo"
_URL = "https://www.linkedin.com/in/janedoe"


@pytest.fixture(autouse=True)
def fresh_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    tmp_db = tmp_path / "leadocean_jobs.db"
    monkeypatch.setattr(shared_db, "DB_PATH", tmp_db)
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)
    monkeypatch.setattr(leadocean_guards, "_schema_initialized", False)
    monkeypatch.setattr(lo, "API_KEY", _API_KEY, raising=False)
    monkeypatch.setattr(lo, "_last_usage_check", time.monotonic(), raising=False)
    monkeypatch.setenv("ENABLE_LEADOCEAN", "true")
    monkeypatch.setenv("LEADOCEAN_GUARDS_ARMED", "true")
    monkeypatch.delenv("LEADOCEAN_REJECT_STATUS", raising=False)
    monkeypatch.setattr(
        lo.rate_limiter, "acquire_token", lambda *a, **k: 0.0, raising=False
    )
    yield tmp_db
    if getattr(shared_db._local, "conn", None) is not None:
        shared_db._local.conn.close()
        shared_db._local.conn = None


def _client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _json(status: int, body: dict[str, Any]) -> httpx.Response:
    return httpx.Response(status, json=body)


def _is_provider_error(value: Any) -> bool:
    return pipeline._is_provider_error(value)


class TestWorkEmailHits:
    def test_verified_work_email(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            assert request.headers["x-api-key"] == _API_KEY
            assert request.url.params["linkedin_url"] == _URL
            return _json(200, {"success": True, "data": {
                "email": "jane@acme.com", "status": "verified",
                "verified_batch_date": "2026-09-20",
            }})

        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert result is not None and result["found"] is True
        assert result["email"] == "jane@acme.com"
        assert result["email_type"] == "work"
        assert result["status"] == "verified"
        assert result["verified_hint"] == "yes"
        assert calls == ["/v2/people/email/work"]  # no personal call on a hit

    def test_catch_all_hint_unknown(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(200, {"success": True, "data": {
                "email": "jane@acme.com", "status": "catch_all",
            }})

        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert result is not None
        assert result["verified_hint"] == "unknown"

    def test_no_url_returns_none_without_http(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return _json(200, {"success": True, "data": None})

        assert asyncio.run(lo.find_person_email(_client(handler), linkedin_url="")) is None
        assert calls == []


class TestPersonalFallback:
    def test_work_null_personal_hit(self):
        """The live-probe scenario: bill gates — work null, 3 personal."""
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v2/people/email/work":
                return _json(200, {"success": True, "data": None})
            return _json(200, {"success": True, "data": [
                {"email": "jane.doe@gmail.com", "verified_batch_date": None},
                {"email": "jdoe@yahoo.com", "verified_batch_date": None},
            ]})

        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert result is not None and result["found"] is True
        assert result["email"] == "jane.doe@gmail.com"  # ranked first wins
        assert result["email_type"] == "personal"
        assert result["status"] == ""                    # personal publishes none
        assert result["verified_hint"] == "unknown"      # always mailtester-gated

    def test_rejected_status_falls_to_personal(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v2/people/email/work":
                return _json(200, {"success": True, "data": {
                    "email": "role@acme.com", "status": "role",
                }})
            return _json(200, {"success": True, "data": [
                {"email": "jane.doe@gmail.com", "verified_batch_date": None},
            ]})

        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert result is not None
        assert result["email_type"] == "personal"

    def test_custom_reject_env(self, monkeypatch):
        monkeypatch.setenv("LEADOCEAN_REJECT_STATUS", "invalid,role,abuse,spam_trap,risky")
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v2/people/email/work":
                return _json(200, {"success": True, "data": {
                    "email": "jane@acme.com", "status": "risky",
                }})
            return _json(200, {"success": True, "data": []})

        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert result is None  # risky rejected + no personal = definitive miss


class TestMisses:
    def test_404_no_profile_marks_miss_no_personal_call(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            return _json(404, {"success": False, "error": {"message": "No profile for this key"}})

        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert result is None
        assert calls == ["/v2/people/email/work"]  # no profile → no personal either
        key = leadocean_guards.person_key(_URL)
        assert leadocean_guards.is_recent_person_miss(key) is True

    def test_work_null_personal_empty_marks_miss(self):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/v2/people/email/work":
                return _json(200, {"success": True, "data": None})
            return _json(200, {"success": True, "data": []})

        client = _client(handler)
        assert asyncio.run(lo.find_person_email(client, linkedin_url=_URL)) is None
        key = leadocean_guards.person_key(_URL)
        assert leadocean_guards.is_recent_person_miss(key) is True
        # Second attempt: miss store short-circuits — zero new HTTP.
        calls = []

        def handler2(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return _json(200, {"success": True, "data": None})

        assert asyncio.run(lo.find_person_email(_client(handler2), linkedin_url=_URL)) is None
        assert calls == []

    def test_miss_store_skip_zero_http(self):
        leadocean_guards.record_person_miss(leadocean_guards.person_key(_URL), "email")

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("miss-cached person must not reach HTTP")

        assert asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL)) is None


class TestQuotaAndCooldown:
    def test_cooldown_active_skips_http(self):
        leadocean_guards.set_cooldown("daily_enrich_exhausted", "wall", retry_after=3600.0)

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("cooldown must skip HTTP entirely")

        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert result is None
        key = leadocean_guards.person_key(_URL)
        assert leadocean_guards.is_recent_person_miss(key) is False  # retryable

    def test_402_arms_daily_cooldown(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(402, {"success": False})

        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert _is_provider_error(result) is True
        cooldown = leadocean_guards.get_active_cooldown()
        assert cooldown is not None and cooldown["code"] == "daily_enrich_exhausted"

    def test_meta_notice_arms_cooldown_with_resetsAt(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(200, {
                "success": True, "data": None,
                "meta": {"notice": {
                    "code": "throttled", "message": "past window",
                    "resetsAt": "2099-01-01T00:00:00.000Z",
                }},
            })

        asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        cooldown = leadocean_guards.get_active_cooldown()
        assert cooldown is not None and cooldown["code"] == "throttled"

    def test_lane_busy_skip(self, monkeypatch):
        monkeypatch.setattr(
            lo.rate_limiter, "acquire_token", lambda *a, **k: 30.0, raising=False
        )

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("lane-busy skip must not reach HTTP")

        t0 = time.monotonic()
        result = asyncio.run(lo.find_person_email(_client(handler), linkedin_url=_URL))
        assert result is None
        assert time.monotonic() - t0 < 3.0
        assert leadocean_guards.is_recent_person_miss(leadocean_guards.person_key(_URL)) is False


class TestSwitches:
    def test_kill_switch(self, monkeypatch):
        monkeypatch.setenv("ENABLE_LEADOCEAN", "false")
        result = asyncio.run(lo.find_person_email(_client(lambda r: _json(200, {})), linkedin_url=_URL))
        assert _is_provider_error(result) is True
        assert getattr(result, "error_type", "") == "disabled"

    def test_missing_key(self, monkeypatch):
        monkeypatch.setattr(lo, "API_KEY", "", raising=False)
        result = asyncio.run(lo.find_person_email(_client(lambda r: _json(200, {})), linkedin_url=_URL))
        assert _is_provider_error(result) is True


class TestUsage:
    def test_maybe_check_usage_arms_daily_cooldown(self):
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/usage"
            return _json(200, {"success": True, "data": {
                "used": 0, "resetsAt": "2099-01-01T00:00:00Z",
                "pace": {"enrich": [{"window": "d", "limit": 1000000, "used": 1000000,
                                     "resetsAt": "2099-01-01T00:00:00.000Z"}]},
            }})

        # Force the lazy window open.
        lo._last_usage_check = 0.0
        asyncio.run(lo.maybe_check_usage(_client(handler)))
        cooldown = leadocean_guards.get_active_cooldown()
        assert cooldown is not None and cooldown["code"] == "daily_enrich_exhausted"
        snap = leadocean_guards.latest_usage_snapshot()
        assert snap is not None and snap["source"] == "usage"

    def test_usage_healthy_no_cooldown(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(200, {"success": True, "data": {
                "pace": {"enrich": [{"window": "d", "limit": 1000000, "used": 5}]},
            }})

        lo._last_usage_check = 0.0
        asyncio.run(lo.maybe_check_usage(_client(handler)))
        assert leadocean_guards.get_active_cooldown() is None

    def test_maybe_check_usage_arms_search_cooldown(self):
        """Phase 2: a spent 25M/mo search window arms search_exhausted."""
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(200, {"success": True, "data": {
                "pace": {
                    "enrich": [{"window": "d", "limit": 1000000, "used": 5}],
                    "search": [{"window": "p", "limit": 25000000, "used": 25000000,
                                "resetsAt": "2099-01-01T00:00:00.000Z"}],
                },
            }})

        lo._last_usage_check = 0.0
        asyncio.run(lo.maybe_check_usage(_client(handler)))
        cooldown = leadocean_guards.get_active_cooldown()
        assert cooldown is not None and cooldown["code"] == "search_exhausted"


# ---------------------------------------------------------------------------
# Phase 2 — domain→people discovery + free counts (2026-11-07)
# ---------------------------------------------------------------------------

_DOMAIN = "stripe.com"


def _person_row(
    slug: str = "aakash-sasikumar",
    *,
    has_email: bool = True,
    title: str = "Engineering, Ml Infra",
) -> dict[str, Any]:
    """One live-probed leadocean.person.v1 row (stripe.com, 2026-11-07)."""
    return {
        "profile_data": {
            "profile_url": f"https://www.linkedin.com/in/{slug}",
            "profile_full_name": "Aakash Sasikumar",
            "profile_first_name": "Aakash",
            "profile_last_name": "Sasikumar",
            "profile_headline": "ML @ Stripe",
            "profile_address": {"city": "Edmonton", "country_code": "CA"},
        },
        "contact_data": {
            "contact_current_experiences": [{
                "company_name": "Stripe", "company_domain": "stripe.com",
                "job_title": title,
                "job_seniority": {"job_level": "Other", "score": None},
                "current": True,
            }],
            "has_email": has_email, "email_status": "catch_all",
            "email_type": "work", "email_types": ["work"], "has_phone": False,
        },
        "meta": {"key": f"https://www.linkedin.com/in/{slug}",
                 "source_ids": {"person_id": "25615657", "company_id": "55330212"}},
    }


class TestDiscovery:
    def test_two_pages_via_cursor(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append((request.url.path, str(request.url.params.get("cursor") or "")))
            if len(calls) == 1:
                return _json(200, {"success": True,
                                   "data": [_person_row("p1"), _person_row("p2")],
                                   "meta": {"nextCursor": "CUR1", "records": 2}})
            return _json(200, {"success": True, "data": [_person_row("p3")], "meta": {"records": 1}})

        result = asyncio.run(lo.search_company_people(_client(handler), _DOMAIN, max_people=5))
        assert result is not None
        assert len(result["people"]) == 3
        assert result["pages"] == 2 and result["rows_seen"] == 3
        assert calls[0][1] == "" and calls[1][1] == "CUR1"  # cursor threaded
        first = result["people"][0]
        assert first["linkedin_url"] == "https://www.linkedin.com/in/p1"
        assert first["title"] == "Engineering, Ml Infra"
        assert first["company_domain"] == "stripe.com"

    def test_early_stop_at_max_people(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            return _json(200, {"success": True, "data": [_person_row("a"), _person_row("b")],
                               "meta": {"nextCursor": "CUR1", "records": 2}})

        result = asyncio.run(lo.search_company_people(_client(handler), _DOMAIN, max_people=2))
        assert result is not None and len(result["people"]) == 2
        assert calls == ["/v1/companies/stripe.com/people"]  # target met → no page 2

    def test_has_email_false_rows_dropped(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(200, {"success": True,
                               "data": [_person_row("noemail", has_email=False), _person_row("yes")],
                               "meta": {"records": 2}})

        result = asyncio.run(lo.search_company_people(_client(handler), _DOMAIN))
        assert result is not None
        assert [p["linkedin_url"].rsplit("/", 1)[-1] for p in result["people"]] == ["yes"]

    def test_all_rows_without_email_marks_domain_miss(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(200, {"success": True, "data": [_person_row("x", has_email=False)],
                               "meta": {"records": 1}})

        assert asyncio.run(lo.search_company_people(_client(handler), _DOMAIN)) is None
        assert leadocean_guards.is_recent_person_miss(leadocean_guards.domain_key(_DOMAIN)) is True

    def test_404_marks_domain_miss_and_replays_skip(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            return _json(404, {"success": False, "error": {"message": "No people for this company"}})

        assert asyncio.run(lo.search_company_people(_client(handler), _DOMAIN)) is None
        assert leadocean_guards.is_recent_person_miss(leadocean_guards.domain_key(_DOMAIN)) is True
        # Replay: miss store short-circuits — zero new HTTP.
        assert asyncio.run(lo.search_company_people(_client(handler), _DOMAIN)) is None
        assert len(calls) == 1

    def test_row_cap_stops_paging(self, monkeypatch):
        monkeypatch.setattr(lo, "_DISCOVERY_MAX_ROWS", 2, raising=False)
        monkeypatch.setattr(lo, "_DISCOVERY_PAGE_SIZE", 2, raising=False)
        monkeypatch.setattr(lo, "_DISCOVERY_MAX_PAGES", 2, raising=False)
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            return _json(200, {"success": True,
                               "data": [_person_row("a", has_email=False), _person_row("b", has_email=False)],
                               "meta": {"nextCursor": "MORE", "records": 2}})

        result = asyncio.run(lo.search_company_people(_client(handler), _DOMAIN))
        assert result is None  # no has_email rows within the cap
        assert calls == ["/v1/companies/stripe.com/people"]  # cap stops page 2
        # Row-cap truncation is NOT a definitive miss — no marker written.
        assert leadocean_guards.is_recent_person_miss(leadocean_guards.domain_key(_DOMAIN)) is False

    def test_search_cooldown_skips_http(self):
        leadocean_guards.set_search_exhausted_cooldown(resets_at="2099-01-01T00:00:00.000Z")

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("search cooldown must skip HTTP entirely")

        assert asyncio.run(lo.search_company_people(_client(handler), _DOMAIN)) is None

    def test_402_arms_search_cooldown(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return _json(402, {"success": False})

        result = asyncio.run(lo.search_company_people(_client(handler), _DOMAIN))
        assert _is_provider_error(result) is True
        cooldown = leadocean_guards.get_active_cooldown()
        assert cooldown is not None and cooldown["code"] == "search_exhausted"

    def test_discovery_lane_kill_switch_silent(self, monkeypatch):
        monkeypatch.setenv("ENABLE_LEADOCEAN_DISCOVERY", "false")

        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("lane off must not reach HTTP")

        assert asyncio.run(lo.search_company_people(_client(handler), _DOMAIN)) is None

    def test_global_kill_switch_errors(self, monkeypatch):
        monkeypatch.setenv("ENABLE_LEADOCEAN", "false")
        result = asyncio.run(lo.search_company_people(_client(lambda r: _json(200, {})), _DOMAIN))
        assert _is_provider_error(result) is True
        assert getattr(result, "error_type", "") == "disabled"

    def test_empty_domain_returns_none(self):
        assert asyncio.run(lo.search_company_people(_client(lambda r: _json(200, {})), "")) is None


class TestCountCompanies:
    def test_count_passthrough_repeated_industry_params(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.params.get_list("industry"))
            return _json(200, {"success": True, "data": {
                "total": 2253898, "totalIsExact": True,
                "facets": {"industry": [{"value": "Dentists", "count": 11304}]},
            }})

        result = asyncio.run(lo.count_companies(_client(handler), ["Dentists", "Construction"]))
        assert result is not None and result["total"] == 2253898
        assert calls == [["Dentists", "Construction"]]  # repeatable OR params

    def test_invalid_industry_never_reaches_http(self):
        def handler(request: httpx.Request) -> httpx.Response:
            raise AssertionError("invalid industry must not reach HTTP")

        result = asyncio.run(lo.count_companies(_client(handler), ["Not A Real Industry"]))
        assert _is_provider_error(result) is True
        assert getattr(result, "error_type", "") == "invalid_industry"

    def test_empty_industries_error(self):
        result = asyncio.run(lo.count_companies(_client(lambda r: _json(200, {})), []))
        assert _is_provider_error(result) is True

    def test_disabled_kill_switch(self, monkeypatch):
        monkeypatch.setenv("ENABLE_LEADOCEAN", "false")
        result = asyncio.run(lo.count_companies(_client(lambda r: _json(200, {})), ["Dentists"]))
        assert _is_provider_error(result) is True
