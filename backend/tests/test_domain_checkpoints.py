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
import csv
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


# ===========================================================================
# C1 — dedupe=0: domain twins must never be silently dropped on resume
# ===========================================================================

class TestDedupeOffResume:
    """With ``dedupe_by_domain=0`` the file legitimately holds the SAME domain
    on multiple rows with different other columns (franchise locations —
    exactly why the user turned dedupe off). Domain checkpoints would mark
    the domain done after twin #1 and a resume would return [] for twin #2:
    permanent silent row loss. 13 live jobs have this config."""

    def _twins(self):
        return [
            {"website": "mcdonalds.com", "city": "Austin"},
            {"website": "mcdonalds.com", "city": "Dallas"},   # twin #2
            {"website": "acme.com", "city": ""},
        ]

    def test_resume_returns_the_second_twin(self, temp_conn):
        """gen0 completed twin #1 (a.com twin) only. The remaining set must
        still contain twin #2 — index subtraction, NOT domain filtering."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "dedup-off-parent", source_type="csv_upload")
        # gen0 processed row 0 (twin #1): index checkpoint + (legacy/wrong)
        # domain checkpoint both present — the filter must IGNORE the domain one.
        store.write_checkpoints_batch("dedup-off-parent", [0])
        store.write_domain_checkpoints_batch("dedup-off-parent", ["mcdonalds.com"])

        remaining, used_domain = routes._resume_remaining_rows(
            store,
            _job_dict("dedup-off-parent", dedupe_by_domain=0),
            self._twins(),
            "website",
            True,
            dedupe=False,
        )

        assert used_domain is False, "dedupe=0 must not use domain semantics"
        cities = [r["city"] for r in remaining]
        # twin #2 (Dallas) and acme.com survive; twin #1 (Austin) is done.
        assert cities == ["Dallas", ""]
        assert len(remaining) == 2

    def test_all_twin_rows_survive_when_domain_done(self, temp_conn):
        """Adversarial: BOTH rows of a done domain must be recoverable. If the
        whole file is twins of one domain and only index 0 is checkpointed,
        resume must return exactly the other twin."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "twin-parent", source_type="csv_upload")
        store.write_checkpoints_batch("twin-parent", [0])
        store.write_domain_checkpoints_batch("twin-parent", ["mcdonalds.com"])
        rows = [
            {"website": "mcdonalds.com", "n": 1},
            {"website": "mcdonalds.com", "n": 2},
        ]
        remaining, _ = routes._resume_remaining_rows(
            store, _job_dict("twin-parent", dedupe_by_domain=0),
            rows, "website", True, dedupe=False,
        )
        assert [r["n"] for r in remaining] == [2]

    def test_write_path_skips_domain_checkpoints_when_dedupe_off(self, tmp_path):
        """The Flow 1 runner must not even WRITE domain checkpoints for a
        dedupe=0 job (they are only meaningful when domains are unique)."""
        job_id = f"dedupoff_{uuid.uuid4().hex[:8]}"
        recorder = mock.MagicMock()
        recorder.get_job.return_value = None
        rows = [
            {"domain": "acme.com"},
            {"domain": "acme.com"},   # twin — must NOT create a checkpoint
            {"domain": "example.com"},
        ]
        stubs = TestRunnerWritesDomainCheckpoints._provider_stubs()
        for s in stubs:
            s.start()
        try:
            asyncio.run(list_builder.run_domain_enrichment(
                rows=rows,
                domain_col="domain",
                max_decision_makers=1,
                job_id=job_id,
                get_store_fn=lambda: recorder,
                output_path=tmp_path / f"{job_id}.csv",
                write_incremental=True,
                dedupe_on=False,
            ))
        finally:
            for s in stubs:
                s.stop()

        recorder.write_domain_checkpoints_batch.assert_not_called()
        # Index checkpoints still cover every completed row.
        recorder.write_checkpoints_batch.assert_called()

    def test_write_path_still_writes_domains_when_dedupe_on(self, tmp_path):
        """Control: the flag must not accidentally disable the normal path."""
        job_id = f"dedupon_{uuid.uuid4().hex[:8]}"
        recorder = mock.MagicMock()
        recorder.get_job.return_value = None
        stubs = TestRunnerWritesDomainCheckpoints._provider_stubs()
        for s in stubs:
            s.start()
        try:
            asyncio.run(list_builder.run_domain_enrichment(
                rows=[{"domain": "acme.com"}],
                domain_col="domain",
                max_decision_makers=1,
                job_id=job_id,
                get_store_fn=lambda: recorder,
                output_path=tmp_path / f"{job_id}.csv",
                write_incremental=True,
                dedupe_on=True,
            ))
        finally:
            for s in stubs:
                s.stop()
        recorder.write_domain_checkpoints_batch.assert_called()

    def test_pipeline_progress_hook_gates_on_dedupe_flag(self, temp_conn):
        """routes._run_job's on_progress consults the job's dedupe flag via
        _job_dedupe_on; a dedupe=0 job must not write domain checkpoints."""
        conn, db_path = temp_conn
        _insert_job(conn, "pipe-dedup-off", source_type="csv_upload")
        conn.execute("ALTER TABLE jobs ADD COLUMN dedupe_by_domain INTEGER DEFAULT 1")
        conn.execute("UPDATE jobs SET dedupe_by_domain=0 WHERE job_id='pipe-dedup-off'")
        conn.commit()
        from shared import db as db_mod
        orig_path = db_mod.DB_PATH
        db_mod.DB_PATH = Path(db_path)
        db_mod._local.conn = None
        try:
            assert routes._job_dedupe_on("pipe-dedup-off") is False
        finally:
            db_mod._local.conn = None
            db_mod.DB_PATH = orig_path
        routes._JOB_DEDUPE_CACHE.pop("pipe-dedup-off", None)

    def test_job_dedupe_on_defaults_true(self, temp_conn):
        conn, db_path = temp_conn
        _insert_job(conn, "plain-job", source_type="csv_upload")
        from shared import db as db_mod
        orig_path = db_mod.DB_PATH
        db_mod.DB_PATH = Path(db_path)
        db_mod._local.conn = None
        try:
            assert routes._job_dedupe_on("plain-job") is True
        finally:
            db_mod._local.conn = None
            db_mod.DB_PATH = orig_path
        routes._JOB_DEDUPE_CACHE.pop("plain-job", None)


# ===========================================================================
# M1 — blank-domain rows must not be re-processed / duplicated on resume
# ===========================================================================

class TestBlankDomainRowsOnResume:
    """Adversarial case: a file with blank-domain rows. Those rows have no
    domain key, so pure domain semantics NEVER mark them done — every resume
    re-processed and re-wrote them (prepend_rows already carried the output)
    -> duplicate rows in the final CSV and an unreachable all-done branch."""

    def _rows(self):
        return [
            {"website": "good1.com", "n": 1},
            {"website": "", "n": 2},       # blank
            {"website": "good2.com", "n": 3},
            {"website": "", "n": 4},       # blank
            {"website": "   ", "n": 5},    # blank (whitespace)
        ]

    def test_all_done_including_blanks_returns_empty(self, temp_conn):
        """The reviewer's exact adversarial test: 2 good + 3 blank rows, ALL
        completed by gen0 (indices 0..4 checkpointed, both domains done).
        Resume must return [] so the all-done short-circuit is reachable —
        no duplicates, no eternal re-processing."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "blank-parent", source_type="csv_upload")
        store.write_checkpoints_batch("blank-parent", [0, 1, 2, 3, 4])
        store.write_domain_checkpoints_batch("blank-parent", ["good1.com", "good2.com"])

        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict("blank-parent"), self._rows(), "website", True
        )
        assert used_domain is True
        assert remaining == []

    def test_unprocessed_blanks_still_resume(self, temp_conn):
        """Blanks the original job did NOT reach must still be processed —
        the index fallback must not resurrect already-done blanks either."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "blank-partial", source_type="csv_upload")
        # completed only rows 0 (good1) and 1 (blank#1)
        store.write_checkpoints_batch("blank-partial", [0, 1])
        store.write_domain_checkpoints_batch("blank-partial", ["good1.com"])

        remaining, used_domain = routes._resume_remaining_rows(
            store, _job_dict("blank-partial"), self._rows(), "website", True
        )
        assert used_domain is True
        assert [r["n"] for r in remaining] == [3, 4, 5]

    def test_done_blank_not_returned_twice(self, temp_conn):
        """A done blank must appear ZERO times in the remaining set (the
        duplication bug would have re-run it)."""
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "blank-one", source_type="csv_upload")
        store.write_checkpoints_batch("blank-one", [0, 1])
        store.write_domain_checkpoints_batch("blank-one", ["good1.com"])
        rows = [{"website": "good1.com"}, {"website": ""}]
        remaining, _ = routes._resume_remaining_rows(
            store, _job_dict("blank-one"), rows, "website", True
        )
        assert remaining == []

    def test_dedupe_keeps_all_blank_rows(self):
        """Contract check: dedupe_rows_by_domain does NOT collapse blank-domain
        rows into one (they pass through untouched) — so the resume filter's
        per-index blank handling is the correct granularity."""
        rows = [
            {"website": "", "n": 1},
            {"website": "", "n": 2},
            {"website": "a.com", "n": 3},
        ]
        kept, deduped_count, _ = identifier_utils.dedupe_rows_by_domain(
            rows, "website", True
        )
        assert deduped_count == 0
        assert len(kept) == 3


# ===========================================================================
# H1 — pipeline checkpoints AFTER the row is on disk
# ===========================================================================

class TestPipelineCheckpointAfterDiskWrite:
    """pipeline.process_row used to fire on_progress (which writes the index +
    domain checkpoints in routes._run_job) BEFORE the incremental CSV write.
    A crash in the gap marked a domain done while its row was absent from the
    partial CSV — silently missing from the final output."""

    def test_on_progress_fires_after_csv_write(self):
        """Drive run_pipeline with one row and record the ORDER of the CSV
        write vs the on_progress call."""
        import tempfile as _tf

        import enrichment.pipeline as pipeline_mod

        # The invariant under test, measured at the moment on_progress fires:
        # the row being reported must ALREADY be on disk. This is the exact
        # crash window H1 closes (checkpoint says done, CSV says missing).
        rows_on_disk_at_progress: list[int] = []

        async def fake_on_progress(e):
            # Read the real partial CSV through a separate handle and count
            # how many data rows are durable at this instant.
            try:
                with open(out_path, newline="", encoding="utf-8") as f:
                    rows_on_disk_at_progress.append(
                        max(0, sum(1 for _ in csv.reader(f)) - 1)
                    )
            except Exception:
                rows_on_disk_at_progress.append(-1)

        # Stub BOTH enrichment paths so no provider is called and exactly one
        # output row comes back per input row.
        async def fake_enrich_domain(*args, **kwargs):
            return [{**pipeline_mod._empty_enriched(), "input_domain": "acme.com",
                     "row_status": pipeline_mod.STATUS_ENRICHED}]

        async def fake_route(*args, **kwargs):
            return {"email": "", "source": "", "provider_attempts": [],
                    "provider_attempts_json": [], "providers_called": [],
                    "providers_skipped": [], "no_email_reason": "",
                    "final_email_status": "", "source_path": ""}

        with mock.patch.object(pipeline_mod, "_enrich_domain", fake_enrich_domain), \
             mock.patch.object(pipeline_mod, "run_enrichment_route", fake_route), \
             mock.patch.object(pipeline_mod, "_maybe_apply_company_fallbacks",
                               new=mock.AsyncMock(return_value=None)):
            with _tf.TemporaryDirectory() as td:
                out_path = Path(td) / "partial.csv"

                async def run():
                    return await pipeline_mod.run_pipeline(
                        rows=[{"website": f"d{i}.com"} for i in range(3)],
                        domain_col="website",
                        name_col=None,
                        first_name_col=None,
                        last_name_col=None,
                        cascade=[],
                        max_results=1,
                        on_progress=fake_on_progress,
                        write_incremental=True,
                        output_path=out_path,
                        job_id=None,
                        use_email_cache=False,
                    )

                result = asyncio.run(run())
                assert len(result) == 3
                final_rows = max(
                    0, sum(1 for _ in csv.reader(open(out_path, newline="", encoding="utf-8"))) - 1
                )
                assert final_rows == 3

        # Every progress event must observe its own row already durable.
        assert len(rows_on_disk_at_progress) == 3
        assert rows_on_disk_at_progress == [1, 2, 3], (
            "on_progress fired before the CSV write — a crash in that gap "
            f"checkpoints a row that is not on disk: {rows_on_disk_at_progress}"
        )

    def test_source_ordering_in_process_row(self):
        """Static guarantee: in pipeline.run_pipeline's process_row the
        on_progress call must textually FOLLOW the incremental CSV write
        block, so no code path can checkpoint a row that is not on disk."""
        import inspect
        import enrichment.pipeline as pipeline_mod

        src = inspect.getsource(pipeline_mod.run_pipeline)
        marker_progress = "await on_progress("
        marker_csv = "csv_writer.writerow(r)"
        assert marker_csv in src and marker_progress in src
        # Find the LAST csv write block and require every on_progress call
        # inside process_row to come after it.
        assert src.rindex(marker_csv) < src.rindex(marker_progress), (
            "run_pipeline: on_progress must be the LAST durable step (after "
            "the incremental CSV write)"
        )


# ===========================================================================
# M2 — delete_job clears index + domain checkpoints
# ===========================================================================

class TestDeleteJobCheckpoints:
    def test_delete_job_removes_both_checkpoint_kinds(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "del-job", source_type="csv_upload")
        store.write_checkpoints_batch("del-job", [0, 1])
        store.write_domain_checkpoints_batch("del-job", ["a.com", "b.com"])

        assert store.delete_job("del-job") is True

        assert store.get_processed_indices("del-job") == set()
        assert store.get_processed_domains("del-job") == set()
        row = conn.execute(
            "SELECT COUNT(*) FROM job_checkpoints WHERE job_id='del-job'"
        ).fetchone()
        assert row[0] == 0

    def test_delete_job_tolerates_missing_domain_table(self, temp_conn):
        conn, _ = temp_conn
        conn.execute("DROP TABLE job_checkpoints_domains")
        conn.commit()
        store = JobStoreBase(conn)
        _insert_job(conn, "del-job2", source_type="csv_upload")
        store.write_checkpoints_batch("del-job2", [0])
        assert store.delete_job("del-job2") is True

    def test_delete_missing_job_returns_false_and_cleans_nothing(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        store.write_domain_checkpoints_batch("other", ["x.com"])
        assert store.delete_job("never-existed") is False
        assert store.get_processed_domains("other") == {"x.com"}

    def test_delete_does_not_touch_other_jobs_checkpoints(self, temp_conn):
        conn, _ = temp_conn
        store = JobStoreBase(conn)
        _insert_job(conn, "keep-job", source_type="csv_upload")
        _insert_job(conn, "drop-job", source_type="csv_upload")
        store.write_domain_checkpoints_batch("keep-job", ["keep.com"])
        store.write_domain_checkpoints_batch("drop-job", ["drop.com"])
        store.delete_job("drop-job")
        assert store.get_processed_domains("keep-job") == {"keep.com"}


# ===========================================================================
# M3 — resume-info must not prefer a ~1% domain-checkpoint sample
# ===========================================================================

class TestResumeInfoDomainSpaceGuards:
    """The legacy pipeline runner (_run_job) writes a domain checkpoint only
    every 100th row. resume-info used to prefer domain space whenever ANY
    domain row existed, reporting ~99% remaining on a fully-processed job."""

    def _job(self, tmp_path, *, total, idx_count, dom_count, dedupe=1):
        """Build the smallest harness that exercises the endpoint's counting
        branch: a real temp DB + a store, then call the endpoint function."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA)
        conn.execute("ALTER TABLE jobs ADD COLUMN dedupe_by_domain INTEGER DEFAULT 1")
        conn.execute("ALTER TABLE jobs ADD COLUMN normalize_domains INTEGER DEFAULT 1")
        conn.execute("ALTER TABLE jobs ADD COLUMN total INTEGER DEFAULT 0")
        conn.execute("ALTER TABLE jobs ADD COLUMN emails_found INTEGER DEFAULT 0")
        conn.execute("ALTER TABLE jobs ADD COLUMN original_filename TEXT DEFAULT ''")
        conn.execute("ALTER TABLE jobs ADD COLUMN filename TEXT DEFAULT ''")
        conn.execute(
            "INSERT INTO jobs (job_id, user_id, job_type, status, parent_job_id, "
            "source_type, total, dedupe_by_domain, created_at, updated_at) "
            "VALUES ('info-job', 'u1', 'enrichment', 'abandoned', NULL, 'csv_upload', "
            "?, ?, '2026-08-24T00:00:00Z', '2026-08-24T00:00:00Z')",
            (total, dedupe),
        )
        conn.commit()
        store = JobStoreBase(conn)
        if idx_count:
            store.write_checkpoints_batch("info-job", list(range(idx_count)))
        if dom_count:
            store.write_domain_checkpoints_batch(
                "info-job", [f"d{i:06d}.com" for i in range(dom_count)]
            )
        return store, conn, db_path

    def _run_endpoint(self, store, tmp_path, user_ok=True):
        from shared import db as db_mod
        # _owns_job: admin bypass is simplest here
        user = {"user_id": "u1", "is_admin": True}
        with mock.patch.object(routes.job_store, "get_store", return_value=store), \
             mock.patch.object(routes, "OUTPUT_DIR", tmp_path):
            return asyncio.run(routes.enrichment_resume_info(
                job_id="info-job", current_user=user
            ))

    def test_sparse_domain_sample_falls_back_to_index_space(self, tmp_path):
        """10,000 rows, all done via index checkpoints, but only 20 domain rows
        exist (a crash-early pipeline sample: ≤ total/200). Domain space would
        report 9,980 remaining — index space is the honest number."""
        store, conn, db_path = self._job(
            tmp_path, total=10_000, idx_count=10_000, dom_count=20
        )
        try:
            info = self._run_endpoint(store, tmp_path)
        finally:
            conn.close()
            Path(db_path).unlink(missing_ok=True)
        assert info["checkpoint_count"] == 10_000
        assert info["unprocessed"] == 0
        assert info["domain_checkpoint_count"] == 20  # still reported for UI

    def test_pipeline_style_every_100th_row_sample_is_plausible(self, tmp_path):
        """Boundary documentation: a pipeline run that COMPLETED 10K rows
        writes exactly 100 domain rows (index % 100 == 0), which is 1% and
        above the total/200 plausibility line — domain space is used. The
        guard targets the crash-early case (a few rows, not a systematic 1%
        sample), where index space is unambiguous."""
        store, conn, db_path = self._job(
            tmp_path, total=10_000, idx_count=10_000, dom_count=100
        )
        try:
            info = self._run_endpoint(store, tmp_path)
        finally:
            conn.close()
            Path(db_path).unlink(missing_ok=True)
        # 100 > 10_000/200 = 50 -> plausible, domain space wins
        assert info["checkpoint_count"] == 100

    def test_dense_domain_checkpoints_prefer_domain_space(self, tmp_path):
        """Flow 1 runner (checkpoints every batch of 50): domain coverage is
        dense, so domain space stays the preferred report."""
        store, conn, db_path = self._job(
            tmp_path, total=1_000, idx_count=1_000, dom_count=990
        )
        try:
            info = self._run_endpoint(store, tmp_path)
        finally:
            conn.close()
            Path(db_path).unlink(missing_ok=True)
        assert info["checkpoint_count"] == 990
        assert info["unprocessed"] == 10

    def test_dedupe_off_never_uses_domain_space(self, tmp_path):
        """dedupe=0 resumes by INDEX (C1) — resume-info must agree or the
        button's number contradicts what a resume actually does."""
        store, conn, db_path = self._job(
            tmp_path, total=500, idx_count=100, dom_count=500, dedupe=0
        )
        try:
            info = self._run_endpoint(store, tmp_path)
        finally:
            conn.close()
            Path(db_path).unlink(missing_ok=True)
        assert info["checkpoint_count"] == 100
        assert info["unprocessed"] == 400
        assert info["domain_checkpoint_count"] == 500


# ===========================================================================
# P0 — auto-resume must not double-charge the chain attempt budget
# ===========================================================================

class TestAutoResumeNoDoubleIncrement:
    """claim_enrichment_resume_job bumps the ROOT's restart_count, then
    _restart_job_core used to bump the HEAD's again. When head == root one
    auto-resume cost +2, so ENRICHMENT_AUTO_RESUME_MAX_RESTARTS=2 gave a
    single retry — violating the "try twice" contract."""

    @staticmethod
    def _mock_store(job_id, csv_name):
        store = mock.MagicMock()
        store.conn.execute.return_value.fetchone.return_value = None
        store.get_job.return_value = _job_dict(
            job_id, status="partial", filename=csv_name,
            original_filename="full.csv", cascade_config="",
            selected_providers='["contacts_db"]', max_results=5,
            name_col="", first_name_col="", last_name_col="",
            linkedin_url_col="", phone_col="", company_name_col="",
            existing_email_col="", total=2,
        )
        store.get_processed_indices.return_value = set()
        store.get_processed_domains.return_value = set()
        store.count_domain_checkpoints.return_value = 0
        return store

    def _run_core(self, store, job_id, tmp_path, **kw):
        bg = mock.MagicMock()
        new_id = None
        with mock.patch.object(routes.job_store, "get_store", return_value=store), \
             mock.patch.object(routes, "OUTPUT_DIR", tmp_path), \
             mock.patch.object(routes, "UPLOAD_DIR", tmp_path):
            try:
                result = asyncio.run(routes._restart_job_core(
                    job_id,
                    current_user={"user_id": "u1", "is_admin": True},
                    background_tasks=bg,
                    **kw,
                ))
                new_id = result.get("job_id")
                return result, bg
            finally:
                if new_id:
                    routes._active_jobs.discard(new_id)
                    routes._job_signals.pop(new_id, None)

    def test_auto_resume_does_not_increment_again(self, tmp_path):
        """claim_already_won=True (auto-resume): the claim already bumped the
        chain ROOT, so _restart_job_core must not increment ANY row."""
        csv_name = "p0_auto"
        (tmp_path / f"{csv_name}.csv").write_text(
            "website\na.com\nb.com\n", encoding="utf-8"
        )
        store = self._mock_store("head-p0", csv_name)
        result, bg = self._run_core(
            store, "head-p0", tmp_path, auto=True, claim_already_won=True
        )
        assert bg.add_task.call_count == 1
        store.increment_restart_count.assert_not_called()

    def test_manual_restart_still_increments_the_job(self, tmp_path):
        """Control: claim_already_won=False (user-clicked Restart) preserves
        today's behavior — the restarted job's own counter goes up by one."""
        csv_name = "p0_manual"
        (tmp_path / f"{csv_name}.csv").write_text(
            "website\na.com\nb.com\n", encoding="utf-8"
        )
        store = self._mock_store("man-1", csv_name)
        result, bg = self._run_core(store, "man-1", tmp_path)
        assert bg.add_task.call_count == 1
        store.increment_restart_count.assert_once = None  # guard typo-proofing
        assert store.increment_restart_count.call_count == 1
        store.increment_restart_count.assert_called_once_with("man-1")

    def test_all_done_auto_path_does_not_increment(self, tmp_path):
        """The all-done short-circuit also creates a child — same rule applies
        there (it used to increment a second time)."""
        csv_name = "p0_alldone"
        (tmp_path / f"{csv_name}.csv").write_text(
            "website\na.com\n", encoding="utf-8"
        )
        store = self._mock_store("alldone-p0", csv_name)
        # Everything already done -> the short-circuit branch fires.
        store.get_processed_domains.return_value = {"a.com"}
        result, _ = self._run_core(
            store, "alldone-p0", tmp_path, auto=True, claim_already_won=True
        )
        assert result["total"] == 1
        store.increment_restart_count.assert_not_called()

    def test_all_done_manual_path_increments_once(self, tmp_path):
        csv_name = "p0_alldone2"
        (tmp_path / f"{csv_name}.csv").write_text(
            "website\na.com\n", encoding="utf-8"
        )
        store = self._mock_store("alldone-m", csv_name)
        store.get_processed_domains.return_value = {"a.com"}
        self._run_core(store, "alldone-m", tmp_path)
        assert store.increment_restart_count.call_count == 1

    def test_auto_resume_clears_claim_marker_after_child(self, tmp_path):
        """#10: once the child exists the claim marker must be dropped, or a
        FUTURE abandonment of the same job stays blocked until the 30-min
        stale window passes."""
        csv_name = "p0_clear"
        (tmp_path / f"{csv_name}.csv").write_text(
            "website\na.com\nb.com\n", encoding="utf-8"
        )
        store = self._mock_store("clear-p0", csv_name)
        executed: list[tuple[str, tuple]] = []

        def fake_execute(sql, params=()):
            executed.append((sql.strip(), tuple(params)))
            class _R:
                def __init__(self):
                    self.rowcount = 1
                def fetchone(self):
                    return None
            return _R()

        store.conn.execute.side_effect = fake_execute
        self._run_core(store, "clear-p0", tmp_path, auto=True, claim_already_won=True)
        clear_sqls = [s for s, _ in executed if "resume_claimed_at=NULL" in s]
        assert clear_sqls, "expected a resume_claimed_at clear after child creation"
