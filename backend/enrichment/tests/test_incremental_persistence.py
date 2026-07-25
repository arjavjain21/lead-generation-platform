"""
Regression tests for incremental enrichment persistence.

Locks in four fixes that shipped together for the per-row checkpoint +
/restart resume flow:

1. ``JobStoreBase.write_checkpoints_batch`` is idempotent. Its INSERT OR
   REPLACE on PRIMARY KEY (job_id, row_index) means re-writing overlapping
   indices must NOT inflate the count nor raise. The incremental writer
   relies on this to checkpoint whole batches without first diff-ing
   against existing rows.

2. ``_count_csv_data_rows`` counts CSV RECORDS via ``csv.reader`` (not
   physical lines). A quoted cell with an embedded newline must NOT
   inflate the row count — otherwise the resume-info endpoint would
   over-report ``partial_csv_rows`` and the UI would show phantom rows.

3. (C1 — critical) ``POST /restart`` computes "unprocessed" in DEDUPED
   row space, not raw-CSV space. Checkpoints are written by the runner
   in deduped space (the runner processes ``deduped_rows``), so the
   resume filter must also operate in deduped space. The bug was: filter
   against raw-CSV indices while checkpoints live in deduped indices →
   resume re-processes the WRONG rows (collapses shift everything down).

4. (C2) ``POST /restart`` short-circuits when every deduped row is
   already checkpointed (e.g. the job crashed after checkpointing
   everything but before ``set_done``). In that case no background
   enrichment task is scheduled — the prior partial is carried forward
   as the new job's complete result.

These tests run against the real ``jobs.db`` (same as siblings). Tests
3 and 4 mock ``routes.job_store.get_store`` so no real job rows are
touched; tests 1 creates a real test user + job and cleans up.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import routes  # noqa: E402
from shared import db  # noqa: E402
from shared.job_store_base import JobStoreBase  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_test_user(conn, user_id: str) -> None:
    """Insert a user so the jobs.user_id FK constraint is satisfied.

    Mirrors the pattern in ``test_source_tracking_integration.py``.
    Uses INSERT OR IGNORE so re-runs don't fail on a stale user from a
    prior run."""
    import hashlib
    conn.execute(
        "INSERT OR IGNORE INTO users (user_id, email, password_hash, created_at) "
        "VALUES (?, ?, ?, ?)",
        (
            user_id,
            f"{user_id}@test.example",
            hashlib.sha256(b"test_password").hexdigest(),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def _cleanup_job(conn, job_id: str, user_id: str | None = None) -> None:
    """Best-effort cleanup so test rows don't accumulate as 'abandoned'
    jobs that the reaper would sweep on the next server restart."""
    try:
        conn.execute("DELETE FROM job_events WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM job_checkpoints WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM job_state WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM enrichment_stats WHERE job_id = ?", (job_id,))
        conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
        if user_id:
            conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        conn.commit()
    except Exception:
        conn.rollback()


# ---------------------------------------------------------------------------
# 1. write_checkpoints_batch idempotency
# ---------------------------------------------------------------------------

class TestWriteCheckpointsBatchIdempotency:
    """INSERT OR REPLACE on PRIMARY KEY (job_id, row_index). Overlapping
    writes must not duplicate rows nor raise."""

    def test_batch_write_then_overlap_write(self):
        db.init_db()
        conn = db.get_db()
        ts = int(time.time() * 1000)
        user_id = f"incr_persist_user_{ts}"
        job_id = f"incr_persist_job_{ts}"
        _make_test_user(conn, user_id)
        store = JobStoreBase(conn)
        try:
            store.create_job(
                job_id=job_id,
                user_id=user_id,
                job_type="enrichment",
                total=10,
                filename="fake.csv",
                domain_col="website",
            )

            # First batch: 5 indices.
            store.write_checkpoints_batch(job_id, [0, 1, 2, 3, 4])
            assert store.get_processed_indices(job_id) == {0, 1, 2, 3, 4}
            assert store.get_checkpoint_count(job_id) == 5

            # Overlapping batch: 2 already-present (2,3), 1 new (9).
            # INSERT OR REPLACE must NOT inflate the count.
            store.write_checkpoints_batch(job_id, [2, 3, 9])
            assert store.get_processed_indices(job_id) == {0, 1, 2, 3, 4, 9}
            assert store.get_checkpoint_count(job_id) == 6
        finally:
            deleted = store.cleanup_checkpoints(job_id)
            assert deleted == 6, f"cleanup_checkpoints deleted {deleted}, expected 6"
            _cleanup_job(conn, job_id, user_id)


# ---------------------------------------------------------------------------
# 2. _count_csv_data_rows counts RECORDS, not physical lines
# ---------------------------------------------------------------------------

class TestCountCsvDataRowsHandlesEmbeddedNewlines:
    """H2 fix: csv.reader (not physical line counting) so a quoted cell
    with an embedded newline does not inflate the data-row count."""

    def test_quoted_embedded_newline_is_one_record(self, tmp_path):
        path = tmp_path / "weird.csv"
        # Header + 2 data RECORDS, but the 2nd record's company cell
        # contains an embedded newline inside quotes — 4 physical lines
        # total. A naive ``sum(1 for line in f) - 1`` would return 3.
        path.write_text(
            "name,company\n"
            "alice,Acme\n"
            'bob,"Smith & Co\nLLC"\n',
            encoding="utf-8",
        )
        assert routes._count_csv_data_rows(path) == 2


# ---------------------------------------------------------------------------
# Shared fixture for the /restart tests (3 and 4)
# ---------------------------------------------------------------------------

def _original_job_dict(original_id: str, csv_name: str) -> dict:
    """The dict returned by the mocked ``store.get_job(...)``.

    Status 'partial' is one of the restart-eligible statuses. Flags
    ``normalize_domains=1`` + ``dedupe_by_domain=1`` mirror the default
    new-job config so the dedupe logic actually runs."""
    return {
        "job_id": original_id,
        "user_id": "u",
        "job_type": "enrichment",
        "status": "partial",
        "filename": csv_name,            # no .csv suffix; restart appends it
        "domain_col": "website",
        "original_filename": "test.csv",
        "name_col": "",
        "first_name_col": "",
        "last_name_col": "",
        "cascade_config": "",            # falsy → falls back to DEFAULT_CASCADE
        "max_results": 5,
        "selected_providers": '["contacts_db"]',
        "linkedin_url_col": "",
        "phone_col": "",
        "company_name_col": "",
        "existing_email_col": "",
        "normalize_domains": 1,
        "dedupe_by_domain": 1,
    }


def _build_store_mock(original_id: str, processed: set[int], csv_name: str):
    """MagicMock store that satisfies every method ``restart_enrichment_job``
    touches on the resume path. ``conn.execute().fetchone()`` returns None
    so the 'existing restart in progress' guard does not falsely fire."""
    store = mock.MagicMock()
    store.conn.execute.return_value.fetchone.return_value = None
    store.get_job.return_value = _original_job_dict(original_id, csv_name)
    # Checkpoints live in DEDUPED space — this is the contract the test
    # is verifying.
    store.get_processed_indices.return_value = processed
    return store


def _write_dup_csv(tmp_path: Path, csv_name: str) -> Path:
    """Write a CSV whose raw rows are [a, b, a(dup), c, d] in the
    ``website`` column. Deduped (normalize=True) → [a, b, c, d] = 4 rows
    in the same order; the dup a.com is dropped at raw index 2."""
    path = tmp_path / f"{csv_name}.csv"
    path.write_text(
        "website\na.com\nb.com\na.com\nc.com\nd.com\n",
        encoding="utf-8",
    )
    return path


# ---------------------------------------------------------------------------
# 3. C1 REGRESSION — /restart unprocessed filter is in DEDUPED space
# ---------------------------------------------------------------------------

class TestRestartUnprocessedInDedupedSpace:
    """The critical regression test. If unprocessed is computed in raw-CSV
    space while checkpoints live in deduped space, the resumed job would
    re-process the wrong rows. With processed={0,1,2} (a, b, c done in
    deduped space), unprocessed must be [3] = d.com ONLY — not the
    raw-CSV index-3 row (which is c.com)."""

    def test_only_truly_unprocessed_deduped_row_is_scheduled(self, tmp_path):
        original_id = f"orig-{uuid.uuid4()}"
        csv_name = "test_resume_input"
        _write_dup_csv(tmp_path, csv_name)

        # dedupe → [a, b, c, d] = 4 rows; processed={0,1,2} → only d (idx 3)
        # remains unprocessed.
        store = _build_store_mock(
            original_id, processed={0, 1, 2}, csv_name=csv_name
        )

        bg = mock.MagicMock()
        patches = [
            mock.patch.object(routes.job_store, "get_store", return_value=store),
            mock.patch.object(routes, "OUTPUT_DIR", tmp_path),
            mock.patch.object(routes, "UPLOAD_DIR", tmp_path),
        ]
        for p in patches:
            p.start()
        new_job_id = None
        try:
            result = asyncio.run(routes.restart_enrichment_job(
                job_id=original_id,
                background_tasks=bg,
                current_user={"user_id": "u", "is_admin": True},
            ))
            new_job_id = result["job_id"]
        finally:
            for p in patches:
                p.stop()
            # /restart adds the new job to the in-memory active set + signal
            # map; clean both so we don't leak state into sibling tests.
            if new_job_id is not None:
                routes._active_jobs.discard(new_job_id)
                routes._job_signals.pop(new_job_id, None)

        # Exactly one background enrichment task scheduled.
        assert bg.add_task.call_count == 1, (
            f"expected exactly 1 background task, got {bg.add_task.call_count}"
        )
        scheduled_rows = bg.add_task.call_args.kwargs["rows"]

        # The critical assertion: only the d.com row was scheduled.
        # If unprocessed were computed in raw-CSV space (the bug), this
        # would be len==2 (c.com + d.com) and scheduled_rows[0]['website']
        # would be 'c.com'.
        assert len(scheduled_rows) == 1, (
            f"expected exactly 1 unprocessed row, got {len(scheduled_rows)} "
            "(raw-space bug would have produced 2)"
        )
        assert scheduled_rows[0]["website"] == "d.com", (
            f"expected d.com (deduped idx 3), got "
            f"{scheduled_rows[0]['website']!r} (raw-CSV idx 3 is c.com — "
            "that would be the bug)"
        )

        # The new job's total is the unprocessed count (1), not the raw
        # CSV's 5 nor the deduped total's 4.
        assert result["total"] == 1


# ---------------------------------------------------------------------------
# 4. C2 — /restart short-circuits when every deduped row is processed
# ---------------------------------------------------------------------------

class TestRestartAllProcessedShortCircuit:
    """When every deduped row is already checkpointed, /restart must NOT
    schedule any background task. The renamed prior partial is carried
    forward as the new job's completed output (set_done)."""

    def test_no_background_task_when_all_processed(self, tmp_path):
        original_id = f"orig-{uuid.uuid4()}"
        csv_name = "test_resume_input_done"
        _write_dup_csv(tmp_path, csv_name)

        # Prior partial output exists at OUTPUT_DIR/<original_id>.csv so
        # /restart reads it, then renames it to _partial.csv, then (in
        # the short-circuit branch) copies it forward as the new job's
        # completed output.
        prior_partial = tmp_path / f"{original_id}.csv"
        prior_partial.write_text(
            "website,dm_email\na.com,x@a.com\n",
            encoding="utf-8",
        )

        # All 4 deduped rows already checkpointed.
        store = _build_store_mock(
            original_id, processed={0, 1, 2, 3}, csv_name=csv_name
        )

        bg = mock.MagicMock()
        patches = [
            mock.patch.object(routes.job_store, "get_store", return_value=store),
            mock.patch.object(routes, "OUTPUT_DIR", tmp_path),
            mock.patch.object(routes, "UPLOAD_DIR", tmp_path),
        ]
        for p in patches:
            p.start()
        try:
            result = asyncio.run(routes.restart_enrichment_job(
                job_id=original_id,
                background_tasks=bg,
                current_user={"user_id": "u", "is_admin": True},
            ))
        finally:
            for p in patches:
                p.stop()

        # CRITICAL: no background enrichment task was scheduled — there
        # is nothing left to do.
        assert bg.add_task.call_count == 0, (
            "all deduped rows already done — must not schedule background work"
        )

        # A new job row was created and marked done with the carried
        # partial as its output_path.
        assert store.create_enrichment_job.called, (
            "/restart must create a new job even when short-circuiting"
        )
        assert store.set_done.called, (
            "/restart must mark the new job done when carrying the prior partial"
        )
        # set_failed is the else-branch when no partial exists — must
        # NOT fire here.
        assert not store.set_failed.called

        # The prior partial was renamed (no longer at <original_id>.csv)
        # and now lives at <original_id>_partial.csv.
        assert not (tmp_path / f"{original_id}.csv").exists(), (
            "prior partial should have been renamed to _partial.csv"
        )
        assert (tmp_path / f"{original_id}_partial.csv").exists(), (
            "renamed _partial.csv must exist"
        )

        # Result contract.
        assert result["restarted_from"] == original_id
        assert result["total"] == 4  # total_deduped
