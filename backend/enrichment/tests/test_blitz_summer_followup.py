"""
Tests for the Blitz summer-followup filters (2026-10-15).

Live-verified contract (probe session 2026-10-15):
- ``company.mx = {"provider": {"include": [...], "exclude": [...]}}`` on
  /v2/search/people + /v2/company/tam-by-people (values 422-probed: google,
  microsoft, zoho, proofpoint, mimecast, barracuda, yahoo, godaddy).
- ``people.is_recent_hire`` STRICT boolean (1/0 rejected); also rides inside
  each waterfall cascade entry.
- ``include_past_jobs`` / ``include_active_jobs`` exist but are deliberately
  NOT wired (past-job matches = people who left — wrong cohort for DM
  discovery).
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

from enrichment import blitz_search  # noqa: E402
from enrichment import tam_routes  # noqa: E402


@pytest.fixture(autouse=True)
def _blitz_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Some test modules leave BLITZ_API_KEY unset for the process; the
    client checks it before every call. Force a deterministic key."""
    monkeypatch.setenv("BLITZ_API_KEY", "test-key-blitz")


def _client(captured: dict[str, Any]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["payload"] = __import__("json").loads(request.content)
        return httpx.Response(200, json={"results": [], "cursor": None})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class TestTamCompanyMxFilter:
    def test_include_only(self):
        f = tam_routes.TamCompanyFilters(mx_providers_include=["google", "microsoft"])
        assert f.to_payload()["mx"] == {"provider": {"include": ["google", "microsoft"]}}

    def test_exclude_only(self):
        f = tam_routes.TamCompanyFilters(mx_providers_exclude=["godaddy"])
        assert f.to_payload()["mx"] == {"provider": {"exclude": ["godaddy"]}}

    def test_unset_omits_key(self):
        assert "mx" not in tam_routes.TamCompanyFilters().to_payload()

    def test_extra_keys_still_forbidden(self):
        with pytest.raises(Exception):
            tam_routes.TamCompanyFilters(mx="not-a-field")


class TestTamPeopleRecentHire:
    def test_true_emitted(self):
        f = tam_routes.TamPeopleFilters(is_recent_hire=True)
        assert f.to_payload()["is_recent_hire"] is True

    def test_unset_omitted(self):
        assert "is_recent_hire" not in tam_routes.TamPeopleFilters().to_payload()


class TestFindPeopleBatchFilters:
    def test_is_recent_hire_into_people_filter(self):
        captured: dict[str, Any] = {}
        result = asyncio.run(blitz_search.find_people_batch(
            _client(captured),
            ["https://www.linkedin.com/company/stripe"],
            job_title_include=["CEO"],
            is_recent_hire=True,
            max_pages=1,
        ))
        assert isinstance(result, dict)
        people = captured["payload"].get("people", {})
        # STRICT boolean — never 1/0.
        assert people.get("is_recent_hire") is True

    def test_none_omits_filter(self):
        captured: dict[str, Any] = {}
        asyncio.run(blitz_search.find_people_batch(
            _client(captured),
            ["https://www.linkedin.com/company/stripe"],
            max_pages=1,
        ))
        assert "is_recent_hire" not in captured["payload"].get("people", {})
