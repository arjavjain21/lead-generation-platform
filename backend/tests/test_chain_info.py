"""Tests for restart-chain metadata on enrichment job listings (chain_info).

Covers:
- ``chain_roots_for_jobs``: 1-hop / 3-hop walks, scraper-parent stop,
  dangling (missing) parent, cycles (2-node, self, chain-into-cycle), no
  parent, hop cap, and the pagination case where only the chain TAIL is on
  the page (ancestors fetched from the DB).
- ``chain_attempt_counts``: counts members across ALL enrichment jobs (not
  just the page) sharing a root.
- API shape: GET /api/enrichment/jobs returns ``chain_root_id`` +
  ``chain_attempts`` on every job row, and omits them (rather than erroring)
  when the chain lookup fails — the UI then falls back to client inference.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import chain_info, routes  # noqa: E402

OWNER_UID = "chain-info-owner"

SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    user_id TEXT,
    job_type TEXT,
    status TEXT,
    parent_job_id TEXT,
    original_filename TEXT DEFAULT '',
    filename TEXT DEFAULT '',
    total INTEGER DEFAULT 0,
    processed INTEGER DEFAULT 0,
    emails_found INTEGER DEFAULT 0,
    output_path TEXT,
    source_type TEXT DEFAULT '',
    hidden_from_ui INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT
);
CREATE TABLE job_checkpoints (
    job_id TEXT NOT NULL,
    row_index INTEGER NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (job_id, row_index)
);
"""


@pytest.fixture
def temp_db():
    """Isolated temp SQLite DB (never the live jobs.db)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    yield conn
    conn.close()
    Path(db_path).unlink(missing_ok=True)


def _insert(conn, job_id, job_type="enrichment", parent=None, user_id=OWNER_UID,
            status="done", filename="upload.csv"):
    iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, parent_job_id, "
        " original_filename, filename, total, processed, emails_found, "
        " created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (job_id, user_id, job_type, status, parent, filename, filename,
         10, 5, 2, iso, iso),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# 1. chain_roots_for_jobs — walk semantics
# ---------------------------------------------------------------------------

class TestChainRoots:
    def test_no_parent_is_own_root(self, temp_db):
        _insert(temp_db, "E1")
        rows = [{"job_id": "E1", "job_type": "enrichment", "parent_job_id": None}]
        assert chain_info.chain_roots_for_jobs(rows, temp_db) == {"E1": "E1"}

    def test_one_hop(self, temp_db):
        _insert(temp_db, "E1")
        _insert(temp_db, "E2", parent="E1")
        rows = [
            {"job_id": "E1", "job_type": "enrichment", "parent_job_id": None},
            {"job_id": "E2", "job_type": "enrichment", "parent_job_id": "E1"},
        ]
        assert chain_info.chain_roots_for_jobs(rows, temp_db) == {"E1": "E1", "E2": "E1"}

    def test_three_hops(self, temp_db):
        _insert(temp_db, "E1")
        _insert(temp_db, "E2", parent="E1")
        _insert(temp_db, "E3", parent="E2")
        _insert(temp_db, "E4", parent="E3")
        rows = [dict(r) for r in temp_db.execute(
            "SELECT job_id, job_type, parent_job_id FROM jobs").fetchall()]
        roots = chain_info.chain_roots_for_jobs(rows, temp_db)
        assert roots == {"E1": "E1", "E2": "E1", "E3": "E1", "E4": "E1"}

    def test_scraper_parent_is_stop_point(self, temp_db):
        # enrichment -> scraper is a Google-Maps chain root, NOT a restart
        # link: the enrichment job is itself the restart-chain root.
        _insert(temp_db, "S1", job_type="scraper")
        _insert(temp_db, "E1", parent="S1")
        rows = [dict(r) for r in temp_db.execute(
            "SELECT job_id, job_type, parent_job_id FROM jobs").fetchall()]
        roots = chain_info.chain_roots_for_jobs(rows, temp_db)
        assert roots["E1"] == "E1"

    def test_missing_parent_dangling_link(self, temp_db):
        _insert(temp_db, "E1", parent="ghost-job")
        rows = [{"job_id": "E1", "job_type": "enrichment", "parent_job_id": "ghost-job"}]
        assert chain_info.chain_roots_for_jobs(rows, temp_db) == {"E1": "E1"}

    def test_two_node_cycle_self_roots(self, temp_db):
        _insert(temp_db, "C1", parent="C2")
        _insert(temp_db, "C2", parent="C1")
        rows = [dict(r) for r in temp_db.execute(
            "SELECT job_id, job_type, parent_job_id FROM jobs").fetchall()]
        roots = chain_info.chain_roots_for_jobs(rows, temp_db)
        assert roots == {"C1": "C1", "C2": "C2"}  # degrade, never hang

    def test_self_cycle(self, temp_db):
        _insert(temp_db, "X1", parent="X1")
        rows = [{"job_id": "X1", "job_type": "enrichment", "parent_job_id": "X1"}]
        assert chain_info.chain_roots_for_jobs(rows, temp_db) == {"X1": "X1"}

    def test_chain_into_cycle_degrades(self, temp_db):
        _insert(temp_db, "C1", parent="C2")
        _insert(temp_db, "C2", parent="C1")
        _insert(temp_db, "K1", parent="C1")
        rows = [dict(r) for r in temp_db.execute(
            "SELECT job_id, job_type, parent_job_id FROM jobs").fetchall()]
        roots = chain_info.chain_roots_for_jobs(rows, temp_db)
        assert roots["K1"] == "K1"  # walks into the cycle, bails, self-roots

    def test_hop_cap_self_roots_beyond_10(self, temp_db):
        _insert(temp_db, "S0", job_type="scraper")
        prev = "S0"
        for i in range(1, 13):  # 12 enrichment hops — beyond the 10 cap
            _insert(temp_db, f"F{i}", parent=prev)
            prev = f"F{i}"
        rows = [dict(r) for r in temp_db.execute(
            "SELECT job_id, job_type, parent_job_id FROM jobs "
            "WHERE job_type='enrichment'").fetchall()]
        roots = chain_info.chain_roots_for_jobs(rows, temp_db)
        # Within-cap members resolve to the true root; the capped tail
        # degrades to self-rooted. Never hangs, never raises.
        assert roots["F1"] == "F1"
        assert roots["F5"] == "F1"
        assert roots["F12"] in ("F1", "F12")

    def test_pagination_tail_only_page_finds_root(self, temp_db):
        # THE bug this cures: only the newest attempt is on the page, its
        # ancestors live on other pages. The root must still resolve to the
        # FIRST attempt, not the off-page direct parent.
        _insert(temp_db, "S1", job_type="scraper")
        _insert(temp_db, "E1", parent="S1")
        _insert(temp_db, "E2", parent="E1")
        _insert(temp_db, "E3", parent="E2")
        page = [{"job_id": "E3", "job_type": "enrichment", "parent_job_id": "E2"}]
        assert chain_info.chain_roots_for_jobs(page, temp_db) == {"E3": "E1"}

    def test_empty_input(self, temp_db):
        assert chain_info.chain_roots_for_jobs([], temp_db) == {}

    def test_row_without_job_id_skipped(self, temp_db):
        rows = [{"job_type": "enrichment"}]
        assert chain_info.chain_roots_for_jobs(rows, temp_db) == {}


# ---------------------------------------------------------------------------
# 2. chain_attempt_counts
# ---------------------------------------------------------------------------

class TestChainAttemptCounts:
    def test_counts_all_members_not_just_page(self, temp_db):
        _insert(temp_db, "S1", job_type="scraper")
        _insert(temp_db, "E1", parent="S1")
        _insert(temp_db, "E2", parent="E1")
        _insert(temp_db, "E3", parent="E2")
        page = [{"job_id": "E3", "job_type": "enrichment", "parent_job_id": "E2"}]
        # 3 attempts exist; only 1 is on the page — the count must still be 3.
        assert chain_info.chain_attempt_counts(page, temp_db) == {"E1": 3}

    def test_single_attempt(self, temp_db):
        _insert(temp_db, "E1")
        page = [{"job_id": "E1", "job_type": "enrichment", "parent_job_id": None}]
        assert chain_info.chain_attempt_counts(page, temp_db) == {"E1": 1}

    def test_scraper_root_not_counted(self, temp_db):
        # Only enrichment jobs count as attempts (scraper parent excluded).
        _insert(temp_db, "S1", job_type="scraper")
        _insert(temp_db, "E1", parent="S1")
        page = [{"job_id": "E1", "job_type": "enrichment", "parent_job_id": "S1"}]
        assert chain_info.chain_attempt_counts(page, temp_db) == {"E1": 1}

    def test_two_chains_on_one_page(self, temp_db):
        _insert(temp_db, "A1")
        _insert(temp_db, "A2", parent="A1")
        _insert(temp_db, "B1")
        page = [dict(r) for r in temp_db.execute(
            "SELECT job_id, job_type, parent_job_id FROM jobs "
            "WHERE job_type='enrichment'").fetchall()]
        counts = chain_info.chain_attempt_counts(page, temp_db)
        assert counts == {"A1": 2, "B1": 1}

    def test_empty_page(self, temp_db):
        assert chain_info.chain_attempt_counts([], temp_db) == {}


# ---------------------------------------------------------------------------
# 3. API shape — GET /api/enrichment/jobs
# ---------------------------------------------------------------------------

def _make_user(user_id=OWNER_UID, is_admin=False):
    return {"user_id": user_id, "email": f"{user_id}@test.example", "is_admin": is_admin}


def _override_auth():
    from shared import auth
    return {auth.get_current_user: lambda: _make_user()}


@pytest.fixture
def enrichment_store(temp_db, monkeypatch):
    """Patch routes.job_store.get_store -> store over the temp DB so the
    endpoint never touches the live jobs.db (mirrors the scraper tests)."""
    from enrichment.job_store import EnrichmentJobStore

    monkeypatch.setattr(routes.job_store, "get_store", lambda: EnrichmentJobStore(temp_db))
    yield temp_db


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.update(_override_auth())
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


class TestApiShape:
    def test_jobs_list_carries_chain_fields(self, enrichment_store, client):
        _insert(enrichment_store, "S1", job_type="scraper")
        _insert(enrichment_store, "E1", parent="S1")
        _insert(enrichment_store, "E2", parent="E1")
        resp = client.get("/api/enrichment/jobs")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] >= 2
        by_id = {j["job_id"]: j for j in body["jobs"]}
        assert by_id["E1"]["chain_root_id"] == "E1"
        assert by_id["E1"]["chain_attempts"] == 2
        assert by_id["E2"]["chain_root_id"] == "E1"
        assert by_id["E2"]["chain_attempts"] == 2
        # The scraper job is NOT on the enrichment list (job_type filter).
        assert "S1" not in by_id

    def test_chain_lookup_failure_omits_fields(self, enrichment_store, client, monkeypatch):
        # Best-effort contract: a chain_info error must not 500 the listing;
        # the keys are absent and the UI falls back to client inference.
        _insert(enrichment_store, "E1")
        with patch.object(
            chain_info, "chain_roots_for_jobs", side_effect=RuntimeError("boom")
        ):
            # routes.py imported the function directly — patch the name the
            # endpoint actually calls.
            monkeypatch.setattr(
                routes, "chain_roots_for_jobs", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
            )
            resp = client.get("/api/enrichment/jobs")
        assert resp.status_code == 200
        job = resp.json()["jobs"][0]
        assert "chain_root_id" not in job
        assert "chain_attempts" not in job

    def test_empty_list_still_200(self, enrichment_store, client):
        resp = client.get("/api/enrichment/jobs")
        assert resp.status_code == 200
        assert resp.json()["jobs"] == []
