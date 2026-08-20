"""
TDD tests for the 2026-07-29 /enrich edge fixes (audit findings #1 + #2).

1. Domain normalization: the /enrich handlers must run every domain through
   ``identifier_utils.normalize_domain`` BEFORE any provider call, so a deep URL
   like ``https://www.walgreens.com/locator/.../id=15007`` is reduced to
   ``walgreens.com`` (was being sent raw -> Blitz 422 + contacts_db 404, the
   ~9,120-error/15-min storm) and an email like ``user@example.com`` is rejected
   (the old ``"." in domain`` check wrongly accepted it).

2. Blitz circuit breaker: 4xx payload errors (422/400/404) must NOT trip the
   breaker (only 5xx / network errors do). A bad-payload storm must not blackout
   Blitz for valid rows.

All tests are synchronous and drive async code via ``asyncio.run`` (pytest-asyncio
is intentionally not a dependency in this repo).
"""
from __future__ import annotations

import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

_BE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BE not in sys.path:
    sys.path.insert(0, _BE)

from enrichment import (  # noqa: E402
    blitz_client,
    contacts_client as cc,
    contacts_writer,
    pipeline,
    routes as routes_mod,
)


def _run_logic(req):
    """Invoke _unified_enrich_logic directly; return (HTTPException|None, result)."""
    from fastapi import HTTPException

    async def go():
        with patch.object(routes_mod, "_record_unified_enrich_stats"), \
                patch.object(routes_mod, "sync_contacts") as fs:
            fs.sync_enrichment_to_contacts = MagicMock(
                return_value={"synced": 0, "skipped": 0, "failed": 0}
            )
            return await routes_mod._unified_enrich_logic(
                req, {"email": "t@e.com", "user_id": 1, "id": 1}
            )

    try:
        return None, asyncio.run(go())
    except HTTPException as e:
        return e, None


class TestEnrichDomainNormalization:
    """The GET /enrich core (_unified_enrich_logic) must normalize domains at the
    edge. The cascade (run_enrichment_route) is mocked so the only provider call
    is the handler's own company_by_domain — a clean capture point."""

    def _stub(self):
        return [
            patch.object(contacts_writer, "is_v2_enabled", MagicMock(return_value=False)),
            patch.object(cc, "company_by_domain", AsyncMock(return_value=None)),
            patch.object(cc, "person_by_linkedin", AsyncMock(return_value=None)),
            patch.object(pipeline, "run_enrichment_route", AsyncMock(return_value={})),
        ]

    def test_deep_url_domain_is_normalized_before_provider_call(self):
        deep = ("https://www.walgreens.com/locator/walgreens-425+fuller+ave+ne-"
                "grand+rapids-mi-49503/id=15007")
        patches = self._stub()
        for p in patches:
            p.start()
        try:
            req = routes_mod.UnifiedEnrichRequest(domain=deep, full_name="Adrian Nguyen")
            exc, _ = _run_logic(req)
            # Capture while the patch is still active (stop() restores the real fn).
            calls = list(cc.company_by_domain.call_args_list)
        finally:
            for p in patches:
                p.stop()

        assert exc is None, f"deep URL should enrich cleanly (normalized to walgreens.com), got: {exc}"
        assert calls, "company_by_domain was never called (expected enhanced-mode lookup)"
        domains = [c.args[1] for c in calls]
        assert all(d == "walgreens.com" for d in domains), (
            f"un-normalized domain reached the provider: {domains}"
        )

    def test_email_as_domain_is_rejected_400(self):
        # OLD code: "user@example.com" has a dot -> accepted -> sent to providers.
        # normalize_domain rejects emails -> "" -> 400 at the edge.
        patches = self._stub()
        for p in patches:
            p.start()
        try:
            req = routes_mod.UnifiedEnrichRequest(domain="user@example.com", full_name="Jane Doe")
            exc, _ = _run_logic(req)
        finally:
            for p in patches:
                p.stop()

        assert exc is not None and exc.status_code == 400, (
            f"email-as-domain should be rejected with 400, got: {exc}"
        )


class TestBlitzBreaker4xx:
    """4xx payload errors must not trip the breaker; 5xx must."""

    def _resp(self, status):
        return httpx.Response(
            status,
            request=httpx.Request("POST", "https://api.blitz-api.ai/v2/enrichment/person"),
        )

    def test_422_does_not_trip_breaker(self):
        client = MagicMock()
        client.post = AsyncMock(return_value=self._resp(422))
        record_failure = AsyncMock()
        with patch.object(blitz_client._blitz_circuit, "can_proceed", AsyncMock(return_value=True)), \
                patch.object(blitz_client._blitz_circuit, "record_failure", record_failure), \
                patch.object(blitz_client._blitz_circuit, "record_success", AsyncMock()), \
                patch.object(blitz_client, "_headers", MagicMock(return_value={})):
            with pytest.raises(httpx.HTTPStatusError):
                asyncio.run(blitz_client._post_with_retry(client, "https://x", {"domain": "acme.com"}, 10.0))
        record_failure.assert_not_called()  # 422 = payload error, not service health

    def test_500_still_trips_breaker(self):
        """Regression guard: the 4xx exclusion must NOT also exclude 5xx."""
        client = MagicMock()
        client.post = AsyncMock(return_value=self._resp(500))
        record_failure = AsyncMock()

        async def _nosleep(_seconds):
            return None

        with patch.object(blitz_client._blitz_circuit, "can_proceed", AsyncMock(return_value=True)), \
                patch.object(blitz_client._blitz_circuit, "record_failure", record_failure), \
                patch.object(blitz_client._blitz_circuit, "record_success", AsyncMock()), \
                patch.object(blitz_client, "_headers", MagicMock(return_value={})), \
                patch("asyncio.sleep", _nosleep):
            with pytest.raises(httpx.HTTPStatusError):
                asyncio.run(blitz_client._post_with_retry(client, "https://x", {"domain": "acme.com"}, 10.0))
        record_failure.assert_called()  # 5xx is a real failure -> trips
