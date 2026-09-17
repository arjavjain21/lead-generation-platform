"""Tests for the TAM-by-People flow runner (enrichment/tam_flow.py).

Coverage:
  * flatten_tam_company — full projection, nullable domain, employee_growth
    first-entry percentage, missing fields -> None.
  * Pagination loop — cursor propagation between pages, stop at cursor null.
  * max_companies cap — mid-page truncation, no extra page fetched.
  * Loop guards (2026-09 regression) — TAM_MAX_PAGES hard cap breaks a
    cursor-cycling server bug; 5 consecutive empty pages break the pull;
    the empty counter resets on a non-empty page (interleaved never trips).
  * Cancel — checked between pages; partial CSV kept, status 'cancelled'.
  * Incremental CSV — header written up-front, rows flushed per page (file
    is complete on disk BEFORE the run finishes).
  * Cursor persistence — job_state row carries the live cursor + row count.
  * Flow-1 chain — reuses routes._run_domain_enrich_job (mocked recorder),
    dedupes domains, sets source_type='tam_chain' + parent_job_id. With
    exact_titles the stored cascade stays UNBRACKETED (the local title gate
    matches literally) and exact_titles=True is forwarded to the runner.
  * Chain skip — create_enrichment_job with zero domains -> chain_skipped.

Network is fully mocked (blitz_client.tam_by_people is monkeypatched); the
job store runs on a temp DB via the shared.db.DB_PATH swap pattern used by
test_contacts_writer / test_seg_csv.
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Optional
from unittest.mock import AsyncMock, patch

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from shared import db as shared_db  # noqa: E402

from enrichment import job_store  # noqa: E402
from enrichment import tam_flow  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _company_entry(index: int, *, domain: Optional[str] = None) -> dict[str, Any]:
    """One tam-by-people result entry with the full company projection."""
    return {
        "company": {
            "linkedin_url": f"https://linkedin.com/company/co-{index}",
            "linkedin_id": 1000 + index,
            "name": f"Company {index}",
            "about": "about text",
            "specialties": ["spec-a"],
            "industry": "Software Development",
            "type": "Privately Held",
            "size": "11-50",
            "employees_on_linkedin": 25,
            "followers": 900 + index,
            "founded_year": 2015,
            "hq": {
                "city": "Berlin",
                "state": "Berlin",
                "country_code": "DE",
                "country_name": "Germany",
                "region": "Berlin",
                "continent": "Europe",
            },
            "domain": domain if domain is not None else f"co{index}.example",
            "website": f"https://co{index}.example",
            "slogan": "slogan text",
            "revenue": "$1M-$5M",
            "employee_growth": [
                {"percentage": 12.5, "timespan": "1 year"},
                {"percentage": 30.0, "timespan": "2 years"},
            ],
        },
        "matched_people": 3 + index,
    }


def _page(entries: list[dict[str, Any]], cursor: Optional[str]) -> dict[str, Any]:
    return {"results": entries, "cursor": cursor}


class _TempDbTestCase(unittest.TestCase):
    """Temp jobs DB + temp output dir per test (mirrors test_contacts_writer)."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._orig_db_path = shared_db.DB_PATH
        shared_db.DB_PATH = Path(self._tmpdir.name) / "jobs_test.db"
        shared_db._local.conn = None
        shared_db.init_db()
        self._ensure_restart_support_columns()
        # jobs.user_id has an FK into users (get_db sets PRAGMA foreign_keys=ON),
        # so the auth tables must exist on the temp DB too. NOTE: shared.auth
        # keeps its OWN DB_PATH module attr + thread-local conn — patch both.
        from shared import auth as shared_auth
        self._orig_auth_db_path = shared_auth.DB_PATH
        shared_auth.DB_PATH = shared_db.DB_PATH
        shared_auth._local.conn = None
        shared_auth.init_auth_db()
        self._orig_output_dir = tam_flow.OUTPUT_DIR
        tam_flow.OUTPUT_DIR = Path(self._tmpdir.name) / "outputs"
        # The runner backs companies up to the real contacts DB by default —
        # mock the upsert for EVERY test so no test ever writes to
        # leadsdatabase.cc. Individual backup tests override the mock.
        self._backup_mock = AsyncMock(return_value={"success": True})
        self._orig_upsert = tam_flow.contacts_client.upsert_business_record_async
        tam_flow.contacts_client.upsert_business_record_async = self._backup_mock

    @staticmethod
    def _ensure_restart_support_columns() -> None:
        """A fresh init_db() lacks the one-time add_restart_support columns
        (name_col / first_name_col / last_name_col / cascade_config /
        max_results) — prod got them via migrations/add_restart_support.py.
        Add them idempotently so job creation works on the temp DB."""
        conn = shared_db.get_db()
        existing = {
            row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        for column, decl in (
            ("name_col", "TEXT"),
            ("first_name_col", "TEXT"),
            ("last_name_col", "TEXT"),
            ("cascade_config", "TEXT"),
            ("max_results", "INTEGER DEFAULT 5"),
        ):
            if column not in existing:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {column} {decl}")
        conn.commit()

    def tearDown(self):
        conn = getattr(shared_db._local, "conn", None)
        if conn is not None:
            conn.close()
            shared_db._local.conn = None
        from shared import auth as shared_auth
        auth_conn = getattr(shared_auth._local, "conn", None)
        if auth_conn is not None:
            auth_conn.close()
            shared_auth._local.conn = None
        shared_auth.DB_PATH = self._orig_auth_db_path
        shared_db.DB_PATH = self._orig_db_path
        tam_flow.OUTPUT_DIR = self._orig_output_dir
        tam_flow.contacts_client.upsert_business_record_async = self._orig_upsert
        self._tmpdir.cleanup()

    def _create_tam_job(self, job_id: str = "tam-test-job") -> str:
        store = job_store.get_store()
        # FK target row (jobs.user_id -> users.user_id is enforced by
        # PRAGMA foreign_keys=ON in get_db()).
        store.conn.execute(
            "INSERT OR IGNORE INTO users (user_id, email, password_hash,"
            " is_admin, created_at) VALUES (?, ?, ?, 1, datetime('now'))",
            ("test-user-1", "tam-test@example.com", "x" * 8),
        )
        store.conn.commit()
        store.create_enrichment_job(
            job_id=job_id,
            user_id="test-user-1",
            total=100,
            filename="tam_test.csv",
            domain_col="domain",
            source_type="tam_flow",
        )
        return job_id

    def _read_csv(self) -> tuple[list[str], list[dict[str, str]]]:
        path = tam_flow.OUTPUT_DIR / "tam-test-job.csv"
        with open(path, newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            header = next(reader)
            rows = [dict(zip(header, r)) for r in reader]
        return header, rows


# ---------------------------------------------------------------------------
# flatten_tam_company
# ---------------------------------------------------------------------------

class TestFlattenTamCompany(unittest.TestCase):

    def test_full_projection_flattens_all_columns(self):
        row = tam_flow.flatten_tam_company(_company_entry(0))
        self.assertEqual(row["name"], "Company 0")
        self.assertEqual(row["domain"], "co0.example")
        self.assertEqual(row["website"], "https://co0.example")
        self.assertEqual(row["linkedin_url"], "https://linkedin.com/company/co-0")
        self.assertEqual(row["industry"], "Software Development")
        self.assertEqual(row["type"], "Privately Held")
        self.assertEqual(row["size"], "11-50")
        self.assertEqual(row["employees_on_linkedin"], 25)
        self.assertEqual(row["followers"], 900)
        self.assertEqual(row["founded_year"], 2015)
        self.assertEqual(row["hq_city"], "Berlin")
        self.assertEqual(row["hq_state"], "Berlin")
        self.assertEqual(row["hq_country_code"], "DE")
        self.assertEqual(row["hq_region"], "Berlin")
        self.assertEqual(row["revenue"], "$1M-$5M")
        self.assertEqual(row["slogan"], "slogan text")
        self.assertEqual(row["employee_growth_1y"], 12.5)
        self.assertEqual(row["matched_people"], 3)
        # Column contract: exactly the documented columns, in order.
        self.assertEqual(list(row.keys()), list(tam_flow.TAM_CSV_COLUMNS))

    def test_first_employee_growth_entry_wins(self):
        entry = _company_entry(1)
        entry["company"]["employee_growth"] = [
            {"percentage": 4.2, "timespan": "1 year"},
            {"percentage": 99.0, "timespan": "5 years"},
        ]
        self.assertEqual(
            tam_flow.flatten_tam_company(entry)["employee_growth_1y"], 4.2
        )

    def test_nullable_fields_become_none(self):
        entry = _company_entry(2)
        entry["company"]["domain"] = None  # nullable in the API
        entry["company"].pop("website", None)
        entry["company"]["employee_growth"] = []
        entry["company"].pop("hq", None)
        row = tam_flow.flatten_tam_company(entry)
        self.assertIsNone(row["domain"], "domain is nullable in the API")
        self.assertIsNone(row["website"])
        self.assertIsNone(row["employee_growth_1y"])
        self.assertIsNone(row["hq_city"])

    def test_empty_entry_is_all_none(self):
        row = tam_flow.flatten_tam_company({})
        self.assertEqual(list(row.keys()), list(tam_flow.TAM_CSV_COLUMNS))
        self.assertTrue(all(value is None for value in row.values()))


# ---------------------------------------------------------------------------
# Pagination / cap / cancel / CSV / cursor
# ---------------------------------------------------------------------------

class TestRunTamFlow(_TempDbTestCase):

    def _patch_tam(self, pages: list[dict[str, Any]]):
        """Patch blitz_client.tam_by_people with a page script.

        Returns (mock, cursors_seen) — cursors_seen records the cursor kwarg
        of every call so tests can assert pagination threading.
        """
        cursors_seen: list[Optional[str]] = []
        script = list(pages)

        async def fake_tam(_http, *, company_filters, people_filters,
                           max_results, cursor):
            cursors_seen.append(cursor)
            self.assertEqual(max_results, tam_flow.TAM_PAGE_SIZE)
            self.assertEqual(company_filters, {"employee_range": ["11-50"]})
            self.assertEqual(people_filters, {"job_level": ["C-Team"]})
            return script.pop(0)

        return fake_tam, cursors_seen

    def test_pagination_stops_at_null_cursor(self):
        self._create_tam_job()
        fake_tam, cursors_seen = self._patch_tam([
            _page([_company_entry(i) for i in range(2)], cursor="c1"),
            _page([_company_entry(i) for i in range(2, 4)], cursor=None),
        ])
        events: list[dict[str, Any]] = []

        async def on_progress(event):
            events.append(event)

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            summary = asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 100,
                },
                on_progress=on_progress,
            ))

        self.assertEqual(summary["status"], "done")
        self.assertEqual(summary["companies_found"], 4)
        self.assertEqual(summary["pages"], 2)
        self.assertFalse(summary["capped"])
        self.assertEqual(cursors_seen, [None, "c1"], "cursor threads page->page")
        header, rows = self._read_csv()
        self.assertEqual(header, list(tam_flow.TAM_CSV_COLUMNS))
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[0]["name"], "Company 0")
        # One SSE event per page + one final tam_backup event.
        self.assertEqual(len(events), 3)
        self.assertEqual(events[1]["companies_found"], 4)
        self.assertFalse(events[1]["has_next_page"])
        self.assertEqual(events[-1]["stage"], "tam_backup")
        self.assertEqual(events[-1]["companies_backed_up"], 4)
        # result_count is persisted on the job row.
        job = job_store.get_store().get_job("tam-test-job")
        self.assertEqual(job["result_count"], 4)

    def test_max_companies_cap_truncates_mid_page_and_stops(self):
        self._create_tam_job()
        fake_tam, cursors_seen = self._patch_tam([
            _page([_company_entry(i) for i in range(5)], cursor="c1"),
            _page([_company_entry(i) for i in range(5, 10)], cursor="c2"),
        ])

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            summary = asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 3,
                },
            ))

        self.assertEqual(summary["status"], "done")
        self.assertEqual(summary["companies_found"], 3)
        self.assertTrue(summary["capped"], "stopped with a cursor outstanding")
        self.assertEqual(len(cursors_seen), 1, "cap prevents the next page fetch")
        _header, rows = self._read_csv()
        self.assertEqual(len(rows), 3)

    def test_hard_ceiling_clamps_max_companies(self):
        self._create_tam_job()
        # Page 1 has 1 row + an outstanding cursor: with an absurd
        # max_companies the loop must continue (clamped to TAM_MAX_COMPANIES,
        # not applied as-is), pulling page 2 as normal.
        fake_tam, cursors_seen = self._patch_tam([
            _page([_company_entry(0)], cursor="c1"),
            _page([_company_entry(1)], cursor=None),
        ])
        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            summary = asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 9_999_999,  # clamped to TAM_MAX_COMPANIES
                },
            ))
        self.assertEqual(summary["status"], "done")
        self.assertEqual(summary["companies_found"], 2)
        self.assertEqual(cursors_seen, [None, "c1"])

    def test_page_cap_stops_cursor_cycling_loop(self):
        """Regression (2026-09): a cursor-cycling server bug (cursor never
        null) must not loop forever — TAM_MAX_PAGES hard-stops the pull,
        keeping every row already written."""
        self._create_tam_job()
        calls = {"count": 0}

        async def cycling_tam(_http, *, company_filters, people_filters,
                              max_results, cursor):
            calls["count"] += 1
            # Always another page, never exhausted (cycling-cursor bug).
            return _page([_company_entry(calls["count"] - 1)], cursor="cycle")

        with patch("enrichment.blitz_client.tam_by_people", new=cycling_tam), \
                patch.object(tam_flow, "TAM_MAX_PAGES", 6):
            summary = asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 1000,
                },
            ))

        self.assertEqual(summary["status"], "done")
        self.assertEqual(summary["pages"], 6, "hard cap stops the loop")
        self.assertEqual(calls["count"], 6, "no fetch beyond the cap")
        self.assertEqual(summary["companies_found"], 6, "every page kept")
        self.assertTrue(summary["capped"], "stopped with a cursor outstanding")
        _header, rows = self._read_csv()
        self.assertEqual(len(rows), 6)

    def test_consecutive_empty_pages_stop_the_pull(self):
        """5 consecutive empty pages with a live cursor end the run (server
        signalling exhaustion without a null cursor — or a failure loop)."""
        self._create_tam_job()
        calls = {"count": 0}

        async def empty_tam(_http, *, company_filters, people_filters,
                            max_results, cursor):
            calls["count"] += 1
            return _page([], cursor="loop")  # empty but never exhausted

        with patch("enrichment.blitz_client.tam_by_people", new=empty_tam):
            summary = asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 1000,
                },
            ))

        self.assertEqual(summary["status"], "done")
        self.assertEqual(summary["pages"], tam_flow.TAM_EMPTY_PAGE_LIMIT)
        self.assertEqual(calls["count"], tam_flow.TAM_EMPTY_PAGE_LIMIT)
        self.assertEqual(summary["companies_found"], 0)
        self.assertTrue(summary["capped"], "stopped with a cursor outstanding")

    def test_empty_page_counter_resets_on_nonempty_page(self):
        """Interleaved empty pages (empty, empty, hit, empty...) never trip
        the guard — only CONSECUTIVE empty pages do."""
        self._create_tam_job()
        # 2 empty, 1 hit, 2 empty, 1 hit, then exhausted: without the reset
        # the run would stop at the 5th page instead of draining the cursor.
        script = [
            _page([], cursor="c1"),
            _page([], cursor="c2"),
            _page([_company_entry(0)], cursor="c3"),
            _page([], cursor="c4"),
            _page([], cursor="c5"),
            _page([_company_entry(1)], cursor=None),
        ]
        fake_tam, cursors_seen = self._patch_tam(script)

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            summary = asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 1000,
                },
            ))

        self.assertEqual(summary["status"], "done")
        self.assertEqual(summary["pages"], 6, "drained every page — no false trip")
        self.assertEqual(summary["companies_found"], 2)
        self.assertFalse(summary["capped"], "ended on a null cursor")
        self.assertEqual(cursors_seen, [None, "c1", "c2", "c3", "c4", "c5"])

    def test_cancel_between_pages_keeps_partial_csv(self):
        self._create_tam_job()
        cancel_flag = {"armed": False}
        cursors_seen: list[Optional[str]] = []
        pages = [
            _page([_company_entry(i) for i in range(2)], cursor="c1"),
            _page([_company_entry(i) for i in range(2, 4)], cursor=None),
        ]

        served = {"count": 0}

        async def fake_tam(_http, *, company_filters, people_filters,
                           max_results, cursor):
            cursors_seen.append(cursor)
            page = pages.pop(0)
            served["count"] += 1
            if served["count"] == 1:
                # Page 1 just got served — arm the cancel so the between-pages
                # check fires before page 2 is ever requested.
                cancel_flag["armed"] = True
            return page

        def should_cancel() -> bool:
            return cancel_flag["armed"]

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            summary = asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 100,
                    "should_cancel": should_cancel,
                },
            ))

        self.assertEqual(summary["status"], "cancelled")
        self.assertEqual(summary["companies_found"], 2, "page-1 rows survive")
        self.assertEqual(len(cursors_seen), 1, "never fetched page 2")
        _header, rows = self._read_csv()
        self.assertEqual(len(rows), 2)

    def test_incremental_flush_writes_page_before_run_completes(self):
        self._create_tam_job()
        observed_during_run: list[tuple[int, int]] = []  # (header_cols, data_rows)

        async def fake_tam(_http, *, company_filters, people_filters,
                           max_results, cursor):
            if cursor is None:
                return _page([_company_entry(i) for i in range(2)], cursor="c1")
            # Before serving page 2, the page-1 rows must already be on disk.
            path = tam_flow.OUTPUT_DIR / "tam-test-job.csv"
            with open(path, newline="", encoding="utf-8") as handle:
                content = list(csv.reader(handle))
            observed_during_run.append((len(content[0]), len(content) - 1))
            return _page([_company_entry(i) for i in range(2, 3)], cursor=None)

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 100,
                },
            ))

        self.assertEqual(
            observed_during_run,
            [(len(tam_flow.TAM_CSV_COLUMNS), 2)],
            "header + page-1 rows flushed before the second page is fetched",
        )

    def test_cursor_persisted_in_job_state_each_page(self):
        self._create_tam_job()
        fake_tam, _cursors = self._patch_tam([
            _page([_company_entry(i) for i in range(2)], cursor="c1"),
            _page([_company_entry(i) for i in range(2, 5)], cursor=None),
        ])
        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 100,
                },
            ))

        # The final persisted state reflects the last page (cursor exhausted).
        progress = tam_flow.read_tam_progress("tam-test-job")
        self.assertIsNotNone(progress)
        self.assertEqual(progress["kind"], "tam_progress")
        self.assertIsNone(progress["cursor"])
        self.assertEqual(progress["rows_written"], 5)
        self.assertEqual(progress["pages"], 2)

    def test_read_tam_progress_ignores_non_tam_rows(self):
        self._create_tam_job()
        store = job_store.get_store()
        store.save_job_state("tam-test-job", "cancelled")
        self.assertIsNone(tam_flow.read_tam_progress("tam-test-job"))
        self.assertIsNone(tam_flow.read_tam_progress("no-such-job"))


# ---------------------------------------------------------------------------
# Flow-1 chain
# ---------------------------------------------------------------------------

class TestChainedEnrichmentJob(_TempDbTestCase):

    def test_chain_creates_flow1_job_and_dedupes_domains(self):
        self._create_tam_job()
        companies = [
            {"name": "A", "domain": "a.example"},
            {"name": "A2", "domain": "a.example"},  # dupe -> collapsed
            {"name": "B", "domain": "b.example"},
            {"name": "C", "domain": ""},            # no domain -> dropped
        ]
        recorder = AsyncMock()

        with patch("enrichment.routes._run_domain_enrich_job", new=recorder):
            result = asyncio.run(self._chain(companies))

        chained_job_id = result.get("chained_job_id")
        self.assertTrue(chained_job_id)
        self.assertEqual(result["chained_total"], 2)
        self.assertEqual(result["deduped_count"], 1)

        job = job_store.get_store().get_job(chained_job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job["job_type"], "enrichment", "never a new job_type")
        self.assertEqual(job["source_type"], "tam_chain")
        self.assertEqual(job["parent_job_id"], "tam-test-job")
        self.assertEqual(job["domain_col"], "domain")
        self.assertEqual(job["total"], 2)

        recorder.assert_awaited_once()
        kwargs = recorder.await_args.kwargs
        self.assertEqual(kwargs["job_id"], chained_job_id)
        self.assertEqual(kwargs["domain_col"], "domain")
        self.assertEqual(len(kwargs["rows"]), 2)
        # The Flow-1 runner was scheduled with the TAM job as parent context.
        self.assertEqual(kwargs["rows"][0]["domain"], "a.example")

    def test_chain_exact_titles_stores_plain_cascade_and_forwards_flag(self):
        """Regression (2026-09): the chained job's cascade must stay UNBRACKETED.

        The chained Flow-1 job's local title gate
        (title_filter.person_matches_titles) matches include-titles literally,
        so a stored "[CEO]" cascade rejected every person (100% drop).
        Exact matching now travels as exact_titles=True on the Flow-1 runner,
        which bracket-wraps at Blitz-call time — mirroring how
        /flows/domain-enrich persists plain cascades + forwards the flag.
        """
        self._create_tam_job()
        companies = [{"name": "A", "domain": "a.example"}]
        recorder = AsyncMock()

        with patch("enrichment.routes._run_domain_enrich_job", new=recorder):
            result = asyncio.run(self._chain(
                companies, titles=["CEO", "Founder"], exact_titles=True
            ))

        job = job_store.get_store().get_job(result["chained_job_id"])
        cascade = json.loads(job["cascade_config"])
        self.assertEqual(
            cascade[0]["include_title"], ["CEO", "Founder"],
            "stored cascade must stay unbracketed",
        )

        recorder.assert_awaited_once()
        kwargs = recorder.await_args.kwargs
        self.assertIs(
            kwargs.get("exact_titles"), True,
            "exact_titles forwarded to the Flow-1 runner for Blitz-time bracketing",
        )

        # End-to-end gate sanity: the stored cascade now ADMITS a matching
        # person (the pre-fix bracketed cascade rejected them).
        from enrichment.title_filter import person_matches_titles
        self.assertTrue(person_matches_titles(
            "CEO", "Chief Executive Officer",
            cascade[0]["include_title"], cascade[0]["exclude_title"],
        ))

    def test_chain_without_exact_titles_keeps_plain_titles(self):
        self._create_tam_job()
        recorder = AsyncMock()
        with patch("enrichment.routes._run_domain_enrich_job", new=recorder):
            result = asyncio.run(self._chain(
                [{"name": "A", "domain": "a.example"}], titles=["CEO"]
            ))
        job = job_store.get_store().get_job(result["chained_job_id"])
        cascade = json.loads(job["cascade_config"])
        self.assertEqual(cascade[0]["include_title"], ["CEO"])
        recorder.assert_awaited_once()
        self.assertIs(recorder.await_args.kwargs.get("exact_titles"), False)

    def test_chain_skipped_when_no_domains(self):
        self._create_tam_job()
        recorder = AsyncMock()
        with patch("enrichment.routes._run_domain_enrich_job", new=recorder):
            result = asyncio.run(self._chain([{"name": "A", "domain": ""}]))
        self.assertIn("chain_skipped", result)
        self.assertEqual(result["chain_skipped"], "no_companies_with_domain")
        recorder.assert_not_awaited()

    async def _chain(self, companies, titles=None, exact_titles=False):
        result = tam_flow.create_chained_enrichment_job(
            "tam-test-job",
            "test-user-1",
            companies,
            titles=titles,
            max_decision_makers=5,
            providers=None,
            exact_titles=exact_titles,
        )
        # Let the fire-and-forget Flow-1 task actually start so awaited-mock
        # assertions below are deterministic.
        await asyncio.sleep(0)
        return result


class TestCompanyBackup(_TempDbTestCase):
    """The contacts-DB backup guarantee: every domain-bearing company the
    TAM flow produces lands in the contacts database, best-effort."""

    def _run(self, entries, *, max_companies=100):
        self._create_tam_job()

        async def fake_tam(_http, *, company_filters, people_filters,
                           max_results, cursor):
            return _page(entries, cursor=None)

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            return asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": max_companies,
                },
            ))

    def test_domain_rows_backed_up_domainless_skipped(self):
        entries = [
            _company_entry(0, domain="alpha.example"),
            _company_entry(1, domain=""),            # domainless -> skipped
            _company_entry(2, domain="gamma.example"),
        ]
        summary = self._run(entries)

        self.assertEqual(summary["companies_found"], 3)
        self.assertEqual(summary["companies_backed_up"], 2)
        self.assertEqual(summary["companies_skipped_no_domain"], 1)
        self.assertEqual(summary["companies_backup_failed"], 0)
        backed_up_domains = [
            call.kwargs["domain"] for call in self._backup_mock.await_args_list
        ]
        self.assertEqual(backed_up_domains, ["alpha.example", "gamma.example"])
        # Full company shape forwarded to the business upsert.
        first = self._backup_mock.await_args_list[0].kwargs
        self.assertEqual(first["company_name"], "Company 0")
        self.assertEqual(first["company_website"], "https://co0.example")
        self.assertEqual(first["city"], "Berlin")
        self.assertEqual(first["city_state"], "Berlin")

    def test_duplicate_domains_backed_up_once(self):
        entries = [
            _company_entry(0, domain="dupe.example"),
            _company_entry(1, domain="dupe.example"),
            _company_entry(2, domain="solo.example"),
        ]
        summary = self._run(entries)

        self.assertEqual(summary["companies_backed_up"], 2)
        self.assertEqual(self._backup_mock.await_count, 2)

    def test_upsert_failure_never_fails_the_job(self):
        self._backup_mock.side_effect = RuntimeError("contacts DB down")
        summary = self._run([
            _company_entry(0, domain="alpha.example"),
            _company_entry(1, domain=""),
        ])

        self.assertEqual(summary["status"], "done")
        self.assertEqual(summary["companies_found"], 2)
        self.assertEqual(summary["companies_backed_up"], 0)
        self.assertEqual(summary["companies_backup_failed"], 1)
        # CSV is complete regardless of backup health.
        _header, rows = self._read_csv()
        self.assertEqual(len(rows), 2)

    def test_cancelled_pull_still_backs_up_flushed_rows(self):
        self._create_tam_job()
        state = {"calls": 0}

        async def fake_tam(_http, *, company_filters, people_filters,
                           max_results, cursor):
            state["calls"] += 1
            if state["calls"] == 1:
                return _page([_company_entry(0, domain="kept.example")],
                             cursor="c1")
            raise AssertionError("cancel must stop the next page fetch")

        with patch("enrichment.blitz_client.tam_by_people", new=fake_tam):
            summary = asyncio.run(tam_flow.run_tam_flow(
                "tam-test-job",
                {
                    "company_filters": {"employee_range": ["11-50"]},
                    "people_filters": {"job_level": ["C-Team"]},
                    "max_companies": 100,
                    "should_cancel": lambda: state["calls"] >= 1,
                },
            ))

        self.assertEqual(summary["status"], "cancelled")
        self.assertEqual(summary["companies_backed_up"], 1)


if __name__ == "__main__":
    unittest.main()
