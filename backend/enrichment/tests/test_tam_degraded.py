"""TAM blitz-unavailable degradation tests (PR-B, 2026-11-11 RCA).

The shared blitz_api breaker can open from UNRELATED lanes (waterfall
429/timeout storms from concurrent enrichment jobs — observed live: 1,131
waterfall-icp 429s/24h while TAM's own tam-by-people calls were healthy).
Contract pinned here:
- breaker-open NEVER crashes the job mid-pull;
- a run that completes on GetLeads is labeled degraded coverage;
- breaker-open + zero companies fails RETRYABLY (never a legitimate zero);
- company-type-filtered runs (no GL equivalent) fail with their options.
"""
from __future__ import annotations

import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from shared.circuit_breaker import CircuitBreakerError  # noqa: E402


def _breaker_error():
    try:
        raise CircuitBreakerError("blitz_api")
    except CircuitBreakerError as e:
        return e


class TestTamDegraded(unittest.TestCase):
    def _run(self, sources, companies_type=None, gl_rows=None, gl_filters=None):
        from enrichment import tam_flow
        params = {
            "sources": sources,
            "max_companies": 100,
            "user_id": "u1",
            "company_filters": ({"type": ["Privately Held"]} if companies_type else {}),
            "people": {"job_title_include": ["CEO"]},
            "getleads_filters": gl_filters if gl_filters is not None else (
                {"industries": ["Software"]} if "getleads" in sources else {}
            ),
        }
        gl_pull = AsyncMock(return_value=(gl_rows or [], 0))

        async def go():
            with patch("enrichment.tam_flow.blitz_client.tam_by_people",
                       new=AsyncMock(side_effect=_breaker_error())), \
                 patch("enrichment.tam_flow._getleads_company_pull", new=gl_pull), \
                 patch("enrichment.tam_flow._backup_companies_to_contacts_db",
                       new=AsyncMock(return_value={})), \
                 patch("enrichment.tam_flow._industry_to_keywords_on_422", return_value=None), \
                 patch.object(tam_flow, "_emit_progress", new=AsyncMock()):
                return await tam_flow.run_tam_flow("job-degraded-1", params)
        return asyncio.run(go()), gl_pull

    def test_blitz_down_with_gl_fallback_degrades_labeled(self):
        gl = [{"name": "GlCo", "domain": "gl.example", "industry": "Software",
               "size": "", "revenue": ""}]
        summary, gl_pull = self._run(["blitz", "getleads"], gl_rows=gl)
        self.assertEqual(summary["companies_found"], 1)
        self.assertTrue(summary.get("blitz_unavailable"))
        self.assertIn("degraded", summary.get("coverage", ""))
        self.assertIn("GetLeads", summary["coverage"])
        gl_pull.assert_awaited()  # fallback actually ran

    def test_blitz_down_zero_rows_fails_retryably(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._run(["blitz", "getleads"], gl_rows=[])
        self.assertIn("temporarily unavailable", str(ctx.exception))
        self.assertIn("Retry", str(ctx.exception))

    def test_blitz_down_type_filter_fails_with_options(self):
        # Type filter kills the GL leg → no fallback → retryable error that
        # explains WHY (type is Blitz-only).
        with self.assertRaises(RuntimeError) as ctx:
            self._run(["blitz", "getleads"], companies_type=True, gl_rows=[])
        self.assertIn("no GetLeads equivalent", str(ctx.exception))

    def test_blitz_only_source_zero_rows_retryable(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._run(["blitz"], gl_rows=[])
        self.assertIn("circuit", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
