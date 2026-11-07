"""
Route tests for the LeadOcean free-count endpoints (phase 2, 2026-11-07):
``GET /api/enrichment/leadocean/count`` + ``/leadocean/industries``.

Handlers are invoked DIRECTLY (not via TestClient) — this box has a known
pre-existing TestClient lifespan hang class (jobs.db lock contention; see
session memory 2026-11-06), and these handlers need no app machinery beyond
their own body. ``current_user`` is passed explicitly, bypassing Depends.
Real HTTP is avoided by monkeypatching ``leadocean_client.count_companies``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import leadocean_industries  # noqa: E402
from enrichment import leadocean_client as lo  # noqa: E402
from enrichment import routes  # noqa: E402


_USER = {"id": 1, "email": "test@example.com"}


class TestCountRoute:
    def test_empty_industries_400(self):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(routes.leadocean_company_count(industry=[], current_user=_USER))
        assert exc.value.status_code == 400

    def test_invalid_industry_422_with_suggestions(self):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(routes.leadocean_company_count(
                industry=["Not A Real Industry"], current_user=_USER))
        assert exc.value.status_code == 422
        detail = exc.value.detail
        assert detail["invalid"] == ["Not A Real Industry"]
        assert isinstance(detail["suggestions"], list) and 0 < len(detail["suggestions"]) <= 10
        assert "industries" in detail["hint"]

    def test_valid_industry_passthrough(self, monkeypatch):
        captured = {}

        async def fake_count(client, industries):
            captured["industries"] = industries
            return {"total": 11304, "totalIsExact": True, "facets": {}}

        monkeypatch.setattr(lo, "count_companies", fake_count)
        result = asyncio.run(routes.leadocean_company_count(
            industry=["Dentists"], current_user=_USER))
        assert result["success"] is True
        assert result["data"]["total"] == 11304
        assert result["meta"]["industries_queried"] == ["Dentists"]
        assert result["meta"]["free"] is True
        assert captured["industries"] == ["Dentists"]

    def test_vendor_no_data_502(self, monkeypatch):
        async def fake_count(client, industries):
            return lo._provider_error("count_companies", "quota_exhausted", "402")

        monkeypatch.setattr(lo, "count_companies", fake_count)
        with pytest.raises(HTTPException) as exc:
            asyncio.run(routes.leadocean_company_count(
                industry=["Dentists"], current_user=_USER))
        assert exc.value.status_code == 502


class TestIndustriesRoute:
    def test_industries_lists_enum(self):
        result = asyncio.run(routes.leadocean_industry_values(current_user=_USER))
        assert result["count"] == leadocean_industries.LEADOCEAN_INDUSTRY_COUNT
        assert result["count"] > 500
        assert "Dentists" in result["values"]
        assert len(result["values"]) == result["count"]
