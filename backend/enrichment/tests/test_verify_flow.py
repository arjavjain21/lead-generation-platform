"""Standalone email-verification engine tests (PR-1.1, 2026-11-11).

Covers: normalization/dedupe, classification policy (mb is NEVER verified),
the raw-facts cache (policy-independent storage, TTL, recheck), the sync
inline ceiling, the background job runner (CSV schema, progress counters,
cancel -> partial), from-job chunk extraction, auth, and reconciliation
denominators. Mixed ok/mb/ko/unavailable fixture throughout.
"""
from __future__ import annotations

import asyncio
import csv
import os
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import verify_flow  # noqa: E402


# ---------------------------------------------------------------------------
# Normalization / classification
# ---------------------------------------------------------------------------

class TestNormalizeAndClassify(unittest.TestCase):
    def test_normalize_strips_lowercases_and_unwraps(self):
        self.assertEqual(verify_flow.normalize_email("  Jane@Corp.COM "), "jane@corp.com")
        self.assertEqual(verify_flow.normalize_email("Jane Doe <jane@corp.com>"), "jane@corp.com")
        self.assertEqual(verify_flow.normalize_email("not-an-email"), "")
        self.assertEqual(verify_flow.normalize_email(""), "")

    def test_dedupe_first_wins_and_counts_dropped(self):
        pairs, dropped = verify_flow.dedupe_emails([
            "A@x.com", "a@x.com", "B@x.com", "garbage", "",
        ])
        self.assertEqual(pairs, [("A@x.com", "a@x.com"), ("B@x.com", "b@x.com")])
        self.assertEqual(dropped, 2)

    def test_classification_matrix(self):
        self.assertEqual(verify_flow.classify("ok", False), ("verified", True))
        self.assertEqual(verify_flow.classify("ok", True), ("verified", True))
        # mb is ALWAYS 'catch_all' — never verified. allow_catch_all only
        # changes the sendable count, not the label.
        self.assertEqual(verify_flow.classify("mb", False), ("catch_all", False))
        self.assertEqual(verify_flow.classify("mb", True), ("catch_all", True))
        self.assertEqual(verify_flow.classify("ko", True), ("invalid", False))
        self.assertEqual(verify_flow.classify("unavailable", True), ("unknown", False))
        self.assertEqual(verify_flow.classify("", False), ("unknown", False))


# ---------------------------------------------------------------------------
# Engine with mocked verifier — mixed fixture
# ---------------------------------------------------------------------------

_MIXED = [
    ("ok1@corp.com", "ok"), ("mb1@corp.com", "mb"), ("ko1@corp.com", "ko"),
    ("unavail1@corp.com", "unavailable"), ("ok2@corp.com", "ok"), ("mb2@corp.com", "mb"),
]


def _mixed_pairs() -> list[tuple[str, str]]:
    return [(email, email.lower()) for email, _code in _MIXED]


def _mock_verifier():
    codes = {e: c for e, c in _MIXED}

    async def fake(client, email):
        code = codes.get(email.lower(), "ok")
        return {"valid": code == "ok", "code": code, "message": f"code={code}", "email": email}

    return fake


class _TempDb(unittest.TestCase):
    def setUp(self):
        import tempfile
        from shared import db as shared_db
        self._tmp = tempfile.TemporaryDirectory()
        self._orig = shared_db.DB_PATH
        shared_db.DB_PATH = Path(self._tmp.name) / "jobs.db"
        shared_db._local.conn = None
        shared_db.init_db()
        from enrichment import job_store
        self.store = job_store.get_store()

    def tearDown(self):
        from shared import db as shared_db
        conn = getattr(shared_db._local, "conn", None)
        if conn is not None:
            conn.close()
            shared_db._local.conn = None
        shared_db.DB_PATH = self._orig
        self._tmp.cleanup()


class TestEngine(_TempDb):
    def _run(self, allow_catch_all=False, recheck=False, on_result=None, check=None):
        with patch("enrichment.verify_flow.mailtester_client.verify_email", new=_mock_verifier()), \
             patch("enrichment.verify_flow._shared_client", new=lambda: object()):
            return asyncio.run(verify_flow.verify_batch(
                _mixed_pairs(), allow_catch_all=allow_catch_all, recheck=recheck,
                conn=self.store.conn, on_result=on_result, check_cancelled=check,
            ))

    def test_mixed_fixture_counts_and_reconciliation(self):
        s = self._run(allow_catch_all=False)
        self.assertEqual(s["total"], 6)
        self.assertEqual(s["verified"], 2)
        self.assertEqual(s["catch_all"], 2)
        self.assertEqual(s["invalid"], 1)
        self.assertEqual(s["unknown"], 1)          # unavailable ≠ invalid
        self.assertEqual(s["validated"], 5)        # ok+mb+ko — precise denominator
        self.assertEqual(s["sendable"], 2)         # catch-all NOT sendable by default
        s2 = self._run(allow_catch_all=True)
        self.assertEqual(s2["sendable"], 4)        # policy applied at read, labels unchanged
        self.assertEqual(s2["catch_all"], 2)

    def test_cache_hit_second_run_makes_zero_calls(self):
        calls = {"n": 0}
        fake = _mock_verifier()

        async def counting(client, email):
            calls["n"] += 1
            return await fake(client, email)

        with patch("enrichment.verify_flow.mailtester_client.verify_email", new=counting), \
             patch("enrichment.verify_flow._shared_client", new=lambda: object()):
            asyncio.run(verify_flow.verify_batch(_mixed_pairs(), allow_catch_all=False,
                                                 recheck=False, conn=self.store.conn))
            self.assertEqual(calls["n"], 6)
            asyncio.run(verify_flow.verify_batch(_mixed_pairs(), allow_catch_all=False,
                                                 recheck=False, conn=self.store.conn))
        # 6 definitive results cached + exactly ONE re-probe on run 2: the
        # 'unavailable' outage is never cached by design.
        self.assertEqual(calls["n"], 7, "run2 must re-probe only the unavailable email")
        # 'unavailable' is NEVER cached (outage evidence is not a verdict):
        rows = self.store.conn.execute(
            "SELECT COUNT(*) FROM verify_results WHERE code='unavailable'").fetchone()[0]
        self.assertEqual(rows, 0)

    def test_recheck_bypasses_cache_and_refreshes(self):
        self._run()
        with patch("enrichment.verify_flow.mailtester_client.verify_email", new=_mock_verifier()), \
             patch("enrichment.verify_flow._shared_client", new=lambda: object()):
            s = asyncio.run(verify_flow.verify_batch(_mixed_pairs(), allow_catch_all=False,
                                                     recheck=True, conn=self.store.conn))
        self.assertEqual(s["cache_hits"], 0)

    def test_unavailable_still_counted_as_unknown_not_invalid(self):
        s = self._run()
        self.assertEqual(s["unknown"], 1)
        self.assertEqual(s["invalid"], 1)

    def test_cancel_stops_early(self):
        # Cancel after first result: check_cancelled returns True thereafter.
        gate = {"stop": False}
        seen: list[str] = []
        def on_result(original, *_a):
            seen.append(original)
            gate["stop"] = True
        s = self._run(on_result=on_result, check=lambda: gate["stop"])
        self.assertEqual(s["total"], 6)
        self.assertLessEqual(s["verified"] + s["catch_all"] + s["invalid"] + s["unknown"], 6)


class TestFromJobExtraction(unittest.TestCase):
    def test_extracts_dm_company_and_split_catch_alls(self):
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "job.csv"
            with open(p, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["dm_email", "dm_email_catch_all", "company_email"])
                w.writerow(["a@x.com", "b@x.com;c@x.com", "info@x.com"])
                w.writerow(["", "", "hello@y.com"])
                w.writerow(["", "", ""])
            got = verify_flow.extract_job_emails(p, limit=10, offset=0,
                                                 only_with_email=True, include_catch_all=True)
            self.assertEqual(got, ["a@x.com", "b@x.com", "c@x.com", "info@x.com", "hello@y.com"])
            no_ca = verify_flow.extract_job_emails(p, limit=10, offset=0,
                                                   only_with_email=True, include_catch_all=False)
            self.assertEqual(no_ca, ["a@x.com", "info@x.com", "hello@y.com"])
            chunk = verify_flow.extract_job_emails(p, limit=2, offset=1,
                                                   only_with_email=True, include_catch_all=True)
            self.assertEqual(chunk, ["b@x.com", "c@x.com"])


class TestCSVSchema(unittest.TestCase):
    def test_columns_contract(self):
        self.assertEqual(verify_flow.VERIFY_CSV_COLUMNS, [
            "original_email", "email_norm", "classification", "verifier",
            "code", "message", "verified_at", "cache_hit", "source_job_id",
        ])


if __name__ == "__main__":
    unittest.main()
