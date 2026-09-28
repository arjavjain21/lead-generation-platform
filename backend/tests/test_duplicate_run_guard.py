"""Superseded-duplicate restart guard + job naming + title-gate badge (2026-09-28).

Background: the 2026-09-23 museums incident. A user uploaded a file, realized
the strict-titles box was unchecked, canceled, and re-uploaded with it checked.
The corrected run completed — but a mass-resume days later also revived the
superseded FIRST upload, which ran for days re-spending providers on domains
the corrected run had already covered.

These tests pin the guard that refuses such restarts (409 + ?force=true
override), the display_name carry-over into restart children, and the
helpers behind the UI title-gate badge.
"""
import asyncio
import json
import sqlite3
from unittest import mock

import pytest
from fastapi import HTTPException

from enrichment import routes


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _memory_db(rows):
    """In-memory jobs table with sqlite3.Row rows, preloaded from tuples."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY, job_type TEXT, parent_job_id TEXT,
            status TEXT, user_id TEXT, original_filename TEXT, updated_at TEXT
        )"""
    )
    conn.executemany(
        "INSERT INTO jobs VALUES (?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    return conn


def _job_dict(job_id, **over):
    job = {
        "job_id": job_id,
        "job_type": "enrichment",
        "status": "partial",
        "user_id": "dguard-user",
        "filename": "dguardcsv",
        "domain_col": "website",
        "original_filename": "museum.csv",
        "parent_job_id": None,
        "cascade_config": json.dumps(
            [{"include_title": ["archivist"], "exclude_title": ["intern"],
              "location": ["WORLD"], "include_headline_search": True}]
        ),
        "selected_providers": '["contacts_db"]',
        "max_results": 5,
        "name_col": "",
        "first_name_col": "",
        "last_name_col": "",
        "linkedin_url_col": "",
        "phone_col": "",
        "company_name_col": "",
        "existing_email_col": "",
        "display_name": "Museums Q4 — strict",
        "normalize_domains": 1,
        "dedupe_by_domain": 1,
        "total": 2,
    }
    job.update(over)
    return job


def _mock_store(job_row, conn):
    store = mock.MagicMock()
    store.conn = conn
    store.get_job.return_value = job_row
    store.get_processed_indices.return_value = set()
    store.get_processed_domains.return_value = set()
    store.count_domain_checkpoints.return_value = 0
    store.increment_restart_count.return_value = 1
    return store


def _run_core(store, job_id, tmp_path, **kw):
    bg = mock.MagicMock()
    new_id = None
    with mock.patch.object(routes.job_store, "get_store", return_value=store), \
         mock.patch.object(routes, "OUTPUT_DIR", tmp_path), \
         mock.patch.object(routes, "UPLOAD_DIR", tmp_path):
        try:
            result = asyncio.run(routes._restart_job_core(
                job_id,
                current_user={"user_id": "dguard-user", "is_admin": True},
                background_tasks=bg,
                **kw,
            ))
            new_id = result.get("job_id")
            return result, bg
        finally:
            if new_id:
                routes._active_jobs.discard(new_id)
                routes._job_signals.pop(new_id, None)


def _write_csv(tmp_path):
    (tmp_path / "dguardcsv.csv").write_text("website\na.com\nb.com\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# _find_completed_run_for_file
# ---------------------------------------------------------------------------

class TestFindCompletedRun:
    def test_foreign_done_chain_is_found(self):
        conn = _memory_db([
            ("dg-zroot", "enrichment", None, "partial", "dguard-user", "museum.csv", "t0"),
            ("dg-head", "enrichment", "dg-zroot", "partial", "dguard-user", "museum.csv", "t1"),
            ("dg-done", "enrichment", None, "done", "dguard-user", "museum.csv", "t2"),
        ])
        store = _mock_store(_job_dict("dg-head", parent_job_id="dg-zroot"), conn)
        hit = routes._find_completed_run_for_file(store, store.get_job.return_value)
        assert hit is not None and hit["job_id"] == "dg-done"

    def test_same_chain_done_run_is_ignored(self):
        """A done run inside the SAME restart chain is a legitimate resume
        target (the all-rows-done path) — not a duplicate."""
        conn = _memory_db([
            ("dg-zroot", "enrichment", None, "partial", "dguard-user", "museum.csv", "t0"),
            ("dg-head", "enrichment", "dg-zroot", "partial", "dguard-user", "museum.csv", "t1"),
            ("dg-done-own", "enrichment", "dg-zroot", "done", "dguard-user", "museum.csv", "t2"),
        ])
        store = _mock_store(_job_dict("dg-head", parent_job_id="dg-zroot"), conn)
        assert routes._find_completed_run_for_file(store, store.get_job.return_value) is None

    def test_other_user_or_other_file_does_not_block(self):
        conn = _memory_db([
            ("dg-zroot", "enrichment", None, "partial", "dguard-user", "museum.csv", "t0"),
            ("dg-head", "enrichment", "dg-zroot", "partial", "dguard-user", "museum.csv", "t1"),
            ("dg-done-other-user", "enrichment", None, "done", "someone-else", "museum.csv", "t2"),
            ("dg-done-other-file", "enrichment", None, "done", "dguard-user", "other.csv", "t2"),
        ])
        store = _mock_store(_job_dict("dg-head", parent_job_id="dg-zroot"), conn)
        assert routes._find_completed_run_for_file(store, store.get_job.return_value) is None

    def test_blank_original_filename_never_blocks(self):
        """Chained jobs (google_maps_chain) carry no original_filename."""
        conn = _memory_db([("dg-head", "enrichment", None, "partial", "dguard-user", "", "t1")])
        store = _mock_store(_job_dict("dg-head", original_filename=""), conn)
        assert routes._find_completed_run_for_file(store, store.get_job.return_value) is None


# ---------------------------------------------------------------------------
# _restart_job_core guard behavior
# ---------------------------------------------------------------------------

class TestRestartCoreGuard:
    def test_guard_raises_409_on_superseded_duplicate(self, tmp_path):
        _write_csv(tmp_path)
        conn = _memory_db([
            ("dg-zroot", "enrichment", None, "partial", "dguard-user", "museum.csv", "t0"),
            ("dg-head2", "enrichment", "dg-zroot", "partial", "dguard-user", "museum.csv", "t1"),
            ("dg-done2", "enrichment", None, "done", "dguard-user", "museum.csv", "2026-09-25T20:00:00"),
        ])
        store = _mock_store(_job_dict("dg-head2", parent_job_id="dg-zroot"), conn)
        with pytest.raises(HTTPException) as exc_info:
            _run_core(store, "dg-head2", tmp_path)
        assert exc_info.value.status_code == 409
        assert "completed run" in exc_info.value.detail
        assert "dg-done2"[:8] in exc_info.value.detail
        store.create_enrichment_job.assert_not_called()

    def test_force_true_overrides_guard(self, tmp_path):
        _write_csv(tmp_path)
        conn = _memory_db([
            ("dg-zroot", "enrichment", None, "partial", "dguard-user", "museum.csv", "t0"),
            ("dg-head3", "enrichment", "dg-zroot", "partial", "dguard-user", "museum.csv", "t1"),
            ("dg-done3", "enrichment", None, "done", "dguard-user", "museum.csv", "t2"),
        ])
        store = _mock_store(_job_dict("dg-head3", parent_job_id="dg-zroot"), conn)
        result, bg = _run_core(store, "dg-head3", tmp_path, force=True)
        assert bg.add_task.call_count == 1
        assert result["job_id"]

    def test_display_name_carried_to_restart_child(self, tmp_path):
        _write_csv(tmp_path)
        conn = _memory_db([
            ("dg-head4", "enrichment", None, "partial", "dguard-user", "museum.csv", "t1"),
        ])
        store = _mock_store(_job_dict("dg-head4"), conn)
        _run_core(store, "dg-head4", tmp_path)
        kwargs = store.create_enrichment_job.call_args.kwargs
        assert kwargs.get("display_name") == "Museums Q4 — strict"


# ---------------------------------------------------------------------------
# _title_gate_state + _sanitize_job_name
# ---------------------------------------------------------------------------

class TestTitleGateState:
    def test_off_when_marker_present(self):
        cascade = json.dumps([{"include_title": ["ceo"], "strict_titles": False}])
        assert routes._title_gate_state(cascade) == "off"

    def test_on_with_custom_titles(self):
        cascade = json.dumps([{"include_title": ["archivist"], "exclude_title": ["intern"]}])
        assert routes._title_gate_state(cascade) == "on"

    def test_default_without_cascade(self):
        assert routes._title_gate_state(None) == "default"
        assert routes._title_gate_state("") == "default"

    def test_default_on_garbage_json(self):
        assert routes._title_gate_state("not-json{") == "default"


class TestSanitizeJobName:
    def test_collapses_whitespace_and_caps_length(self):
        assert routes._sanitize_job_name("  a   b  ") == "a b"
        assert len(routes._sanitize_job_name("x" * 500)) == 120

    def test_empty_and_none_return_empty(self):
        assert routes._sanitize_job_name(None) == ""
        assert routes._sanitize_job_name("   ") == ""

    def test_drops_control_characters(self):
        assert "\n" not in routes._sanitize_job_name("name\nwith\tnewlines")
