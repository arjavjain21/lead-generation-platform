"""Catch-all retention tests (PR-1.2, 2026-11-11).

Contract: when MailTester returns ``mb`` (catch-all domain) and the accept
policy (MAILTESTER_ACCEPT_CODES, default ``ok``) rejects the candidate, the
address is RETAINED in the additive trailing ``dm_email_catch_all`` column
and counted in the job's ``catch_all_retained`` counter. ``dm_email``
semantics are completely unchanged — the cascade still falls through to the
next provider / not-found exactly as before. ``ko`` is never retained.

Async pattern: asyncio.run in sync tests (project convention).
"""
from __future__ import annotations

import asyncio
import os
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import list_builder  # noqa: E402
from enrichment import mailtester_client as mt  # noqa: E402


def _mb(email: str) -> dict[str, Any]:
    return {"valid": False, "code": "mb", "message": "catch-all domain", "email": email}


def _ok(email: str) -> dict[str, Any]:
    return {"valid": True, "code": "ok", "message": "valid", "email": email}


class TestNoteRejectedCandidate(unittest.TestCase):
    def test_mb_appended_to_dict_and_list_sinks(self):
        d: dict[str, Any] = {}
        mt.note_rejected_candidate(d, "a@catchall.com", "mb")
        self.assertEqual(d.get("catch_all_candidates"), ["a@catchall.com"])
        lst: list[str] = []
        mt.note_rejected_candidate(lst, "b@catchall.com", "mb")
        self.assertEqual(lst, ["b@catchall.com"])

    def test_ko_and_ok_never_retained(self):
        d: dict[str, Any] = {}
        mt.note_rejected_candidate(d, "dead@example.com", "ko")
        mt.note_rejected_candidate(d, "good@example.com", "ok")
        self.assertEqual(d.get("catch_all_candidates", []), [])

    def test_kill_switch_off_disables_retention(self):
        with patch.dict(os.environ, {"ENABLE_CATCH_ALL_RETENTION": "false"}):
            d: dict[str, Any] = {}
            mt.note_rejected_candidate(d, "a@catchall.com", "mb")
            self.assertEqual(d.get("catch_all_candidates", []), [])

    def test_never_raises_on_odd_sink(self):
        mt.note_rejected_candidate(None, "a@catchall.com", "mb")  # no-op, no raise
        mt.note_rejected_candidate(42, "a@catchall.com", "mb")  # type: ignore[arg-type]


class _RetentionBase(unittest.TestCase):
    def setUp(self):
        import tempfile
        from shared import db as shared_db
        self._tmp = tempfile.TemporaryDirectory()
        self._orig_db = shared_db.DB_PATH
        shared_db.DB_PATH = Path(self._tmp.name) / "jobs.db"
        shared_db._local.conn = None
        shared_db.init_db()

    def tearDown(self):
        from shared import db as shared_db
        conn = getattr(shared_db._local, "conn", None)
        if conn is not None:
            conn.close()
            shared_db._local.conn = None
        shared_db.DB_PATH = self._orig_db
        self._tmp.cleanup()


class TestListBuilderRetention(_RetentionBase):
    """Drive the REAL list_builder contacts_db gate with a mocked MailTester
    returning `mb`, and assert the row keeps the candidate while dm_email
    falls through (unchanged policy)."""

    def _run(self, mt_result: dict[str, Any]):
        events: list[dict] = []
        with patch(
            "enrichment.list_builder.contacts_client.company_by_domain",
            new=AsyncMock(return_value={
                "name": "Catchall Corp",
                "linkedin_url": "https://linkedin.com/company/catchall-corp",
                "industry": "Software",
            }),
        ), patch(
            "enrichment.list_builder.contacts_client.company_contacts_enriched",
            new=AsyncMock(return_value=[{
                "full_name": "Jane Doe", "first_name": "Jane", "last_name": "Doe",
                "title": "CEO", "headline": "Chief Executive Officer",
                "linkedin_url": "https://linkedin.com/in/janedoe",
                "email": "jane@catchall-corp.example",
            }]),
        ), patch(
            "enrichment.list_builder.contacts_client.person_by_name_and_domain",
            new=AsyncMock(return_value={
                "full_name": "Jane Doe", "first_name": "Jane", "last_name": "Doe",
                "headline": "CEO", "linkedin_url": "https://linkedin.com/in/janedoe",
                "email": "jane@catchall-corp.example",
            }),
        ), patch(
            "enrichment.list_builder.mailtester_client.verify_email",
            new=AsyncMock(return_value=mt_result),
        ), patch(
            "enrichment.list_builder._attach_seg_columns", new=AsyncMock(return_value=None),
        ), patch(
            "enrichment.list_builder.blitz_client.domain_to_linkedin",
            new=AsyncMock(return_value={
                "found": True,
                "company_linkedin_url": "https://linkedin.com/company/catchall-corp",
            }),
        ), patch(
            "enrichment.list_builder.blitz_client.waterfall_icp_search",
            new=AsyncMock(return_value={"results": []}),
        ), patch(
            "enrichment.list_builder.company_fallback.run_company_fallbacks",
            new=AsyncMock(return_value={}),
        ):
            rows = asyncio.run(list_builder.run_domain_enrichment(
                rows=[{"domain": "catchall-corp.example"}],
                domain_col="domain",
                max_decision_makers=3,
                job_id="test-catch-all-retention",
                write_incremental=False,
                on_progress=events.append,
            ))
        return rows, events

    def test_mb_candidate_retained_and_dm_email_falls_through(self):
        rows, events = self._run(_mb("jane@catchall-corp.example"))
        row = next(r for r in rows if r.get("dm_full_name") == "Jane Doe")
        self.assertEqual(row.get("dm_email_catch_all"), "jane@catchall-corp.example",
                         "the mb candidate must be retained in the additive column")
        self.assertEqual(row.get("dm_email"), "",
                         "dm_email policy UNCHANGED — the cascade fell through")
        self.assertEqual(row.get("dm_email_source"), "not_found",
                         "cascade continued past the rejected contacts_db candidate")
        # Counter: exactly one row with a retained candidate.
        self.assertEqual(sum(e.get("catch_all_retained", 0) for e in events), 1)

    def test_ok_result_has_no_retention_and_uses_email(self):
        rows, events = self._run(_ok("jane@catchall-corp.example"))
        row = next(r for r in rows if r.get("dm_full_name") == "Jane Doe")
        self.assertEqual(row.get("dm_email"), "jane@catchall-corp.example")
        self.assertEqual(row.get("dm_email_catch_all"), "")
        self.assertEqual(sum(e.get("catch_all_retained", 0) for e in events), 0)

    def test_kill_switch_off_leaves_column_empty(self):
        with patch.dict(os.environ, {"ENABLE_CATCH_ALL_RETENTION": "false"}):
            rows, _ = self._run(_mb("jane@catchall-corp.example"))
        row = next(r for r in rows if r.get("dm_full_name") == "Jane Doe")
        self.assertEqual(row.get("dm_email_catch_all"), "")


class TestColumnPlumbing(unittest.TestCase):
    def test_column_after_mx_in_both_lists_prefix_preserved(self):
        self.assertIn("dm_email_catch_all", list_builder.ENRICHED_COLUMNS)
        self.assertIn("dm_email_catch_all", __import__("enrichment.pipeline", fromlist=["ENRICHED_COLUMNS"]).ENRICHED_COLUMNS)
        # pipeline stays an exact prefix of list_builder
        from enrichment import pipeline
        self.assertEqual(
            list_builder.ENRICHED_COLUMNS[:len(pipeline.ENRICHED_COLUMNS)],
            list(pipeline.ENRICHED_COLUMNS),
        )

    def test_empty_enriched_carries_blank_column(self):
        self.assertEqual(list_builder._empty_enriched().get("dm_email_catch_all"), "")
