"""Domain-keyed enrichment checkpoints (2026-08-24 index-space resume fix).

The bug: ``_restart_job_core`` subtracted the parent's POSITIONAL checkpoints
(written in the parent's own deduped-subset index space) from the FULL
re-deduped file's index space. Resuming a gen-1 job whose total was a 4,124-row
subset of a 9,524-row file treated full-file indices 0..4123 as done (the wrong
rows) and 4124..9523 as remaining — each generation re-expanded and re-enriched
already-done domains (paid provider spend).

The fix: the runner also writes the dedupe-keyed DOMAIN of every completed row
to ``job_checkpoints_domains``, and resume filters by the UNION of done domains
across the whole restart chain. Keying is centralized in
``identifier_utils.domain_checkpoint_key`` so the writer and the filter can
never drift.

Covered:
  a) gen-2 resume excludes ONLY the done domains (9,524 - 450, not 9,524 - 4,124)
  b) chain-union: grandparent + parent domains both excluded
  c) fallback: ancestors without domain checkpoints do not crash (index fallback)
  d) cleanup_checkpoints clears domain rows too
  e) the write path: store helpers round-trip + the Flow 1 runner batch writer
"""
from __future__ import annotations

import asyncio
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from enrichment import identifier_utils, list_builder, routes  # noqa: E402
from shared.job_store_base import JobStoreBase  # noqa: E402


SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    user_id TEXT,
    job_type TEXT,
    status TEXT,
    parent_job_id TEXT,
    source_type TEXT DEFAULT '',
    restart_count INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT
);
CREATE TABLE job_checkpoints (
    job_id TEXT NOT NULL,
    row_index INTEGER NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (job_id, row_index)
);
CREATE TABLE job_checkpoints_domains (
    job_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (job_id, domain)
);
"""


@pytest.fixture
def temp_conn():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    yield conn, db_path
    conn.close()
    Path(db_path).unlink(missing_ok=True)


def _insert_job(conn, job_id, parent=None, job_type="enrichment",
                source_type="restart", status="abandoned"):
    conn.execute(
        """INSERT INTO jobs (job_id, user_id, job_type, status, parent_job_id,
             source_type, created_at, updated_at)
           VALUES (?, 'u1', ?, ?, ?, ?, '2026-08-24T00:00:00Z', '2026-08-24T00:00:00Z')""",
        (job_id, job_type, status, parent, source_type),
    )
    conn.commit()


def _job_dict(job_id, **overrides):
    base = {
        "job_id": job_id,
        "user_id": "u1",
        "job_type": "enrichment",
        "status": "abandoned",
        "domain_col": "website",
        "normalize_domains": 1,
        "dedupe_by_domain": 1,
    }
    return {**base, **overrides}


def _domain_rows(n, start=0):
    """n unique-domain rows: [{'website': 'd00000.com'}, ...]."""
    return [{"website": f"d{start + i:05d}.com"} for i in range(n)]


# ---------------------------------------------------------------------------
# (a) THE RCA SCENARIO — resume excludes only done domains
# ---------------------------------------------------------------------------

class TestGen2ResumeExcludesOnlyDoneDomains:
    FULL = 9524          # full-file deduped row count
    PARENT_TOTAL = 4124  # the parent job's own (subset) total
    DONE = 450           # domains the parent actually completed

    def _setup_parent(self, conn):
        """gen-1 job: total=4124 subset, 450 domain checkpoints, and — the
        trap — positional index checkpoints 0..4123 in its OWN subset space."""
        parent_id = "gen1-parent"
        _insert_job(conn, parent_id, source_type="csv_upload")
        store = JobStoreBase(conn)
        store.write_domain_checkpoints_batch(
            parent_id, [r["website"] for r in _domain_rows(self.DONE)]
        )
        store.write_checkpoints_batch(parent_id, list(range(self.PARENT_TOTAL)))
        return parent_id

    def test_remaining_is_full_minus_done_domains(self, temp_conn):
        """9,524 deduped rows, 450 done domains -> 9,074 remain.

        The buggy index math produced 9,524-4,124 = 5,400 (and the WRONG
        rows — full-file positions 4124..9523). Must never re-include a done
        domain, never drop an undone one."""
        conn, _ = temp_conn
        parent_id = self._setup_parent(conn)
        store = JobStoreBase(conn)
        deduped_all = _domain_rows(self.FULL)

        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict(parent_id), deduped_all, "website", True
        )

        assert used_domain is True
        assert len(remaining) == self.FULL - self.DONE
        # The first remaining row is the 451st domain (index 450), NOT the
        # 4,125th (index 4124) — that distinction is the whole fix.
        assert remaining[0]["website"] == "d00450.com"
        done = {r["website"] for r in _domain_rows(self.DONE)}
        assert not done.intersection(r["website"] for r in remaining)

    def test_endpoint_schedules_only_undone_rows(self, temp_conn, tmp_path):
        """End-to-end through restart_enrichment_job: the scheduled background
        rows are exactly the 9,074 undone domains."""
        conn, _ = temp_conn
        parent_id = self._setup_parent(conn)
        csv_name = "resume_full_file"
        csv_path = tmp_path / f"{csv_name}.csv"
        csv_path.write_text(
            "website\n" + "\n".join(r["website"] for r in _domain_rows(self.FULL)) + "\n",
            encoding="utf-8",
        )

        store = mock.MagicMock()
        store.conn.execute.return_value.fetchone.return_value = None
        store.get_job.return_value = _job_dict(
            parent_id, status="partial", filename=csv_name,
            original_filename="full.csv", cascade_config="",
            selected_providers='["contacts_db"]', max_results=5,
            name_col="", first_name_col="", last_name_col="",
            linkedin_url_col="", phone_col="", company_name_col="",
            existing_email_col="", dedupe_by_domain=1,
        )
        # The parent's own index set — the trap: 4,124 subset-space indices.
        store.get_processed_indices.return_value = set(range(self.PARENT_TOTAL))
        store.get_processed_domains.side_effect = (
            lambda jid: {r["website"] for r in _domain_rows(self.DONE)}
        )
        store.count_domain_checkpoints.return_value = self.DONE

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
                job_id=parent_id,
                background_tasks=bg,
                current_user={"user_id": "u1", "is_admin": True},
            ))
            new_job_id = result["job_id"]
        finally:
            for p in patches:
                p.stop()
            if new_job_id is not None:
                routes._active_jobs.discard(new_job_id)
                routes._job_signals.pop(new_job_id, None)

        assert bg.add_task.call_count == 1
        scheduled = bg.add_task.call_args.kwargs["rows"]
        assert len(scheduled) == self.FULL - self.DONE, (
            f"expected {self.FULL - self.DONE} remaining rows, got {len(scheduled)} "
            f"(index-space bug yields {self.FULL - self.PARENT_TOTAL})"
        )
        done = {r["website"] for r in _domain_rows(self.DONE)}
        assert not done.intersection(r["website"] for r in scheduled)
        assert result["total"] == self.FULL - self.DONE


# ---------------------------------------------------------------------------
# (b) chain-union: grandparent + parent domains both excluded
# ---------------------------------------------------------------------------

class TestChainUnion:
    def test_grandparent_and_parent_domains_both_excluded(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "grand", source_type="csv_upload")
        _insert_job(conn, "parent", parent="grand")
        _insert_job(conn, "child", parent="parent")
        store.write_domain_checkpoints_batch("grand", ["g1.com", "shared.com"])
        store.write_domain_checkpoints_batch("parent", ["p1.com", "shared.com"])

        deduped_all = [
            {"website": "g1.com"},
            {"website": "p1.com"},
            {"website": "shared.com"},
            {"website": "fresh.com"},
        ]
        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict("child"), deduped_all, "website", True
        )

        assert used_domain is True
        assert [r["website"] for r in remaining] == ["fresh.com"]

    def test_chain_walk_is_capped_and_cycle_safe(self, temp_conn):
        """A corrupt parent cycle must terminate; the original job's own
        checkpoints still apply."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "a", parent="b")
        _insert_job(conn, "b", parent="a")  # cycle
        store.write_domain_checkpoints_batch("a", ["done-a.com"])
        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict("a"),
            [{"website": "done-a.com"}, {"website": "open.com"}],
            "website", True,
        )
        assert used_domain is True
        assert [r["website"] for r in remaining] == ["open.com"]

    def test_scraper_root_contributes_nothing_but_does_not_crash(self, temp_conn):
        """A scraper ancestor (no domain checkpoints, job_type='scraper') is
        skipped silently rather than treated as legacy-warning-worthy."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "scraper-root", job_type="scraper", source_type="")
        _insert_job(conn, "enr", parent="scraper-root", source_type="google_maps_chain")
        store.write_domain_checkpoints_batch("enr", ["e1.com"])
        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict("enr"),
            [{"website": "e1.com"}, {"website": "e2.com"}],
            "website", True,
        )
        assert used_domain is True
        assert [r["website"] for r in remaining] == ["e2.com"]


# ---------------------------------------------------------------------------
# (c) fallback: ancestors without domain checkpoints
# ---------------------------------------------------------------------------

class TestLegacyFallback:
    def test_no_domain_checkpoints_anywhere_keeps_index_semantics(self, temp_conn):
        """Pre-migration chain: original job has ONLY index checkpoints. Resume
        must fall back to today's subtraction and must not raise."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "legacy-parent", source_type="csv_upload")
        store.write_checkpoints_batch("legacy-parent", [0, 1, 2])

        deduped_all = _domain_rows(5)
        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict("legacy-parent"), deduped_all, "website", True
        )
        assert used_domain is False
        assert [r["website"] for r in remaining] == [
            r["website"] for r in deduped_all[3:]
        ]

    def test_legacy_ancestor_alongside_domain_parent_does_not_crash(self, temp_conn):
        """Mixed chain: grandparent legacy (index only), parent with domain
        checkpoints. Union applies for the parent; the legacy grandparent is
        tolerated (warned), never fatal."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "legacy-grand", source_type="csv_upload")
        _insert_job(conn, "dom-parent", parent="legacy-grand")
        _insert_job(conn, "head", parent="dom-parent")
        store.write_checkpoints_batch("legacy-grand", [0])
        store.write_domain_checkpoints_batch("dom-parent", ["p1.com"])

        deduped_all = [
            {"website": "p1.com"},
            {"website": "x1.com"},
            {"website": "x2.com"},
        ]
        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict("head"), deduped_all, "website", True
        )
        assert used_domain is True
        assert [r["website"] for r in remaining] == ["x1.com", "x2.com"]

    def test_all_done_short_circuit_when_domain_filter_empties_rows(self, temp_conn):
        """Every deduped row's domain is done -> zero remaining (the
        /restart 'all processed' short-circuit input)."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "all-done", source_type="csv_upload")
        store.write_domain_checkpoints_batch("all-done", ["a.com", "b.com"])
        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict("all-done"),
            [{"website": "a.com"}, {"website": "b.com"}],
            "website", True,
        )
        assert used_domain is True
        assert remaining == []


# ---------------------------------------------------------------------------
# (d) cleanup_checkpoints clears domain rows too
# ---------------------------------------------------------------------------

class TestCleanupCheckpointsDomains:
    def test_cleanup_clears_both_kinds(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        store.write_checkpoints_batch("job-x", [0, 1, 2])
        store.write_domain_checkpoints_batch("job-x", ["a.com", "b.com", "c.com"])

        deleted = store.cleanup_checkpoints("job-x")

        assert deleted == 6
        assert store.get_processed_indices("job-x") == set()
        assert store.get_processed_domains("job-x") == set()
        assert store.count_domain_checkpoints("job-x") == 0

    def test_cleanup_tolerates_missing_domain_table(self, temp_conn):
        """Old test fixtures / fresh DBs without job_checkpoints_domains must
        not crash the done-path cleanup."""
        conn, db_path = temp_conn
        conn.execute("DROP TABLE job_checkpoints_domains")
        conn.commit()
        store = JobStoreBase(conn)
        store.write_checkpoints_batch("job-y", [0, 1])
        assert store.cleanup_checkpoints("job-y") == 2

    def test_cleanup_does_not_touch_other_jobs(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        store.write_domain_checkpoints_batch("job-a", ["a.com"])
        store.write_domain_checkpoints_batch("job-b", ["b.com"])
        store.cleanup_checkpoints("job-a")
        assert store.get_processed_domains("job-b") == {"b.com"}


# ---------------------------------------------------------------------------
# (e) write path — store helpers + the Flow 1 runner batch writer
# ---------------------------------------------------------------------------

class TestDomainCheckpointStoreHelpers:
    def test_roundtrip_and_count(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        store.write_domain_checkpoints_batch("job-r", ["acme.com", "b.com"])
        assert store.get_processed_domains("job-r") == {"acme.com", "b.com"}
        assert store.count_domain_checkpoints("job-r") == 2

    def test_idempotent_overlap_does_not_inflate(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        store.write_domain_checkpoints_batch("job-i", ["a.com", "b.com"])
        store.write_domain_checkpoints_batch("job-i", ["b.com", "c.com"])
        assert store.count_domain_checkpoints("job-i") == 3
        assert store.get_processed_domains("job-i") == {"a.com", "b.com", "c.com"}

    def test_empty_and_noise_domains_skipped(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        store.write_domain_checkpoints_batch("job-e", ["", "", "  ", "a.com"])
        assert store.get_processed_domains("job-e") == {"a.com"}

    def test_empty_batch_is_a_noop(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        store.write_domain_checkpoints_batch("job-n", [])
        assert store.count_domain_checkpoints("job-n") == 0

    def test_get_processed_domains_missing_table_returns_empty(self, temp_conn):
        conn, _ = temp_conn
        conn.execute("DROP TABLE job_checkpoints_domains")
        conn.commit()
        store = JobStoreBase(conn)
        assert store.get_processed_domains("gone") == set()
        assert store.count_domain_checkpoints("gone") == 0


class TestRunnerWritesDomainCheckpoints:
    """The Flow 1 incremental writer must persist domains alongside indices."""

    @staticmethod
    def _provider_stubs():
        return [
            mock.patch("enrichment.list_builder.contacts_client.company_by_domain",
                       new=mock.AsyncMock(return_value={"linkedin_url": "https://linkedin.com/company/acme"})),
            mock.patch("enrichment.list_builder.contacts_client.company_contacts_enriched",
                       new=mock.AsyncMock(return_value=[])),
            mock.patch("enrichment.list_builder.contacts_client.person_by_name_and_domain",
                       new=mock.AsyncMock(return_value=None)),
            mock.patch("enrichment.list_builder.blitz_client.domain_to_linkedin",
                       new=mock.AsyncMock(return_value={"found": False, "company_linkedin_url": ""})),
            mock.patch("enrichment.list_builder.blitz_client.waterfall_icp_search",
                       new=mock.AsyncMock(return_value={"results": []})),
        ]

    def test_flush_writes_domains_with_indices(self, tmp_path):
        job_id = f"dom_ckpts_{uuid.uuid4().hex[:8]}"
        recorder = mock.MagicMock()
        recorder.get_job.return_value = None  # no stored cascade_config
        rows = [
            {"domain": "https://acme.com/?utm=1"},  # normalizes to acme.com
            {"domain": "example.com"},
            {"domain": ""},
        ]
        stubs = self._provider_stubs()
        for s in stubs:
            s.start()
        try:
            result = asyncio.run(list_builder.run_domain_enrichment(
                rows=rows,
                domain_col="domain",
                max_decision_makers=2,
                job_id=job_id,
                get_store_fn=lambda: recorder,
                output_path=tmp_path / f"{job_id}.csv",
                write_incremental=True,
            ))
        finally:
            for s in stubs:
                s.stop()

        assert isinstance(result, list) and len(result) >= 3
        recorder.write_checkpoints_batch.assert_called()
        domain_calls = [
            c.args[1] for c in recorder.write_domain_checkpoints_batch.call_args_list
        ]
        written = set().union(*domain_calls) if domain_calls else set()
        # 'acme.com' arrives NORMALIZED (same key dedupe uses), '' is skipped.
        assert written == {"acme.com", "example.com"}
        assert "" not in written
        # The CSV the checkpoints claim was actually flushed.
        assert (tmp_path / f"{job_id}.csv").stat().st_size > 0


# ---------------------------------------------------------------------------
# Keying parity — writer key == dedupe key == resume-filter key
# ---------------------------------------------------------------------------

class TestCheckpointKeyParity:
    @pytest.mark.parametrize("raw,normalize,expected", [
        ("https://Acme.com/?utm=x", True, "acme.com"),
        ("www.acme.com", True, "acme.com"),
        ("ACME.com", False, "acme.com"),
        ("acme.com/location/2", False, "acme.com/location/2"),
        ("", True, ""),
        (None, True, ""),
        ("user@example.com", True, ""),
        ("not a domain", True, ""),
    ])
    def test_key_matches_dedupe_key(self, raw, normalize, expected):
        assert identifier_utils.domain_checkpoint_key(raw, normalize) == expected

    def test_dedupe_collapses_exactly_like_checkpoint_key(self):
        rows = [
            {"website": "https://acme.com/?utm=1"},
            {"website": "acme.com"},
            {"website": "shop.acme.com"},
        ]
        kept, deduped_count, _ = identifier_utils.dedupe_rows_by_domain(
            rows, "website", normalize=True
        )
        assert deduped_count == 1  # first two collapse; subdomain stays
        keys = {identifier_utils.domain_checkpoint_key(r["website"], True) for r in kept}
        assert keys == {"acme.com", "shop.acme.com"}


# ---------------------------------------------------------------------------
# Schema bootstrap — init_db creates the table idempotently (hermetic)
# ---------------------------------------------------------------------------

class TestSchemaBootstrap:
    def test_init_db_creates_domain_table_idempotently(self):
        """init_db must create job_checkpoints_domains on a fresh DB and not
        raise on a second run (existing installs migrate on next boot)."""
        from shared import db as db_mod

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = Path(f.name)
        original_path = db_mod.DB_PATH
        db_mod.DB_PATH = db_path
        db_mod._local.conn = None
        try:
            db_mod.init_db()
            conn = db_mod.get_db()
            tables = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            assert "job_checkpoints_domains" in tables
            store = JobStoreBase(conn)
            store.write_domain_checkpoints_batch("boot-job", ["boot.com"])
            assert store.get_processed_domains("boot-job") == {"boot.com"}
            # Idempotent re-run.
            db_mod._local.conn = None
            db_mod.init_db()
        finally:
            db_mod._local.conn = None
            db_mod.DB_PATH = original_path
            db_path.unlink(missing_ok=True)
