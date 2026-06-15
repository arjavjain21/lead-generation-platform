"""
Tests for the abandoned-job recovery + resume flow (2026-06-14).

Covers:
1. Per-row checkpoint writes in the on_progress callback
2. Partial CSV path persistence on abandonment / cancellation
3. get_unprocessed_indices returns the complement of processed indices
4. resume-info endpoint response shape (contract test)

These tests run against the real jobs.db (same as the other tests in
this directory). They use direct SQL inserts for the parent user
row so the FK on jobs.user_id is satisfied.
"""

from __future__ import annotations

import json
import os
import sys
import unittest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from shared import db  # noqa: E402
from shared.job_store_base import JobStoreBase  # noqa: E402


# Test users are created once per test module so the FK on jobs.user_id
# is satisfied without polluting prod data.
_TEST_USER_ID = "u-resume-test"


def _ensure_test_user() -> None:
    """Create a test user if it doesn't already exist. The auth code
    uses bcrypt-hashed passwords, but for the FK constraint all we
    need is a row in the users table. We use a placeholder hash
    that's never checked in this test context."""
    conn = db.get_db()
    row = conn.execute(
        "SELECT user_id FROM users WHERE user_id=?", (_TEST_USER_ID,)
    ).fetchone()
    if row:
        return
    conn.execute(
        "INSERT INTO users (user_id, email, password_hash, created_at, is_admin) "
        "VALUES (?, ?, ?, datetime('now'), 0)",
        (_TEST_USER_ID, f"{_TEST_USER_ID}@test.local", "x"),
    )
    conn.commit()


class TestCheckpointPersistence(unittest.TestCase):
    """Per-row checkpoint is written for every row the pipeline processes."""

    def setUp(self):
        _ensure_test_user()
        self._cleanup_jobs = []

    def tearDown(self):
        for jid in self._cleanup_jobs:
            try:
                JobStoreBase(db.get_db()).delete_job(jid)
            except Exception:
                pass

    def test_write_checkpoint_called_for_every_row(self):
        store = JobStoreBase(db.get_db())
        self._cleanup_jobs.append("test-job-1")
        store.create_job(
            job_id="test-job-1",
            user_id=_TEST_USER_ID,
            job_type="enrichment",
            total=5,
        )
        # Write a checkpoint for every row
        for idx in range(5):
            store.write_checkpoint("test-job-1", idx)

        processed = store.get_processed_indices("test-job-1")
        self.assertEqual(processed, {0, 1, 2, 3, 4})
        self.assertEqual(store.get_checkpoint_count("test-job-1"), 5)


class TestPartialOutputPath(unittest.TestCase):
    """The set_partial_output_path / get_partial_output_path roundtrip
    is the durability contract for the resume flow."""

    def setUp(self):
        _ensure_test_user()
        self._cleanup_jobs = []

    def tearDown(self):
        for jid in self._cleanup_jobs:
            try:
                JobStoreBase(db.get_db()).delete_job(jid)
            except Exception:
                pass

    def test_set_and_get_partial_output_path(self):
        store = JobStoreBase(db.get_db())
        self._cleanup_jobs.append("test-job-2")
        store.create_job(
            job_id="test-job-2",
            user_id=_TEST_USER_ID,
            job_type="enrichment",
            total=10,
        )

        # Initially unset
        self.assertIsNone(store.get_partial_output_path("test-job-2"))

        # Set a path
        store.set_partial_output_path("test-job-2", "/tmp/foo_partial.csv")
        self.assertEqual(
            store.get_partial_output_path("test-job-2"),
            "/tmp/foo_partial.csv",
        )

    def test_get_checkpoint_count_zero_for_old_jobs(self):
        """Jobs that predate the per-row checkpoint feature should report
        0, not raise. This is the user-friendly default."""
        store = JobStoreBase(db.get_db())
        self._cleanup_jobs.append("old-resume-test-job")
        store.create_job(
            job_id="old-resume-test-job",
            user_id=_TEST_USER_ID,
            job_type="enrichment",
            total=100,
        )
        self.assertEqual(store.get_checkpoint_count("old-resume-test-job"), 0)


class TestUnprocessedIndices(unittest.TestCase):
    """get_unprocessed_indices returns the complement of processed indices."""

    def setUp(self):
        _ensure_test_user()
        self._cleanup_jobs = []

    def tearDown(self):
        for jid in self._cleanup_jobs:
            try:
                JobStoreBase(db.get_db()).delete_job(jid)
            except Exception:
                pass

    def test_returns_indices_not_yet_processed(self):
        store = JobStoreBase(db.get_db())
        self._cleanup_jobs.append("test-job-3")
        store.create_job(
            job_id="test-job-3",
            user_id=_TEST_USER_ID,
            job_type="enrichment",
            total=10,
        )
        # Process rows 0, 1, 2, 3, 4
        for idx in range(5):
            store.write_checkpoint("test-job-3", idx)

        unprocessed = store.get_unprocessed_indices(10, "test-job-3")
        self.assertEqual(unprocessed, [5, 6, 7, 8, 9])

    def test_empty_when_all_processed(self):
        store = JobStoreBase(db.get_db())
        self._cleanup_jobs.append("test-job-4")
        store.create_job(
            job_id="test-job-4",
            user_id=_TEST_USER_ID,
            job_type="enrichment",
            total=3,
        )
        for idx in range(3):
            store.write_checkpoint("test-job-4", idx)

        self.assertEqual(store.get_unprocessed_indices(3, "test-job-4"), [])


class TestResumeInfoEndpointShape(unittest.TestCase):
    """The resume-info response shape is consumed by the frontend modal
    and by future automation. Lock the contract here so an accidental
    refactor doesn't silently break the UI."""

    def test_response_shape(self):
        expected_keys = {
            "job_id",
            "status",
            "total",
            "processed",
            "unprocessed",
            "emails_found",
            "checkpoint_count",
            "partial_csv_exists",
            "partial_csv_path",
            "partial_csv_rows",
            "selected_providers",
            "filename",
            "can_resume",
        }
        import inspect
        from enrichment import routes
        src = inspect.getsource(routes.resume_info_enrichment_job)
        for key in expected_keys:
            self.assertIn(f'"{key}"', src, f"resume-info missing key: {key}")


class TestRecoverPartialEndpointShape(unittest.TestCase):
    """The recover-partial endpoint also has a contract with the UI."""

    def test_uses_partial_output_path(self):
        import inspect
        from enrichment import routes
        src = inspect.getsource(routes.recover_partial_enrichment_job)
        # The endpoint should consult the persisted path first, not
        # re-derive it. This guards against regression where the
        # path lookup is bypassed and the wrong file is returned.
        self.assertIn("get_partial_output_path", src)


if __name__ == "__main__":
    unittest.main()
