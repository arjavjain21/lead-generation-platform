"""Team-shared job visibility tests (multi-user rollout, 2026-11-26).

Contract: members (non-admin) see ALL team jobs and can open them — the job
board is shared; attribution lives on jobs.user_id + the audit log, not on
per-user isolation. Covers the surfaces NOT already pinned by the flipped
tests in test_enrichment_api_key_auth / test_enrichment_native_download /
test_scraper_partial_download:

- shared GET /api/jobs (main.py) — foreign job in a member's list
- shared GET /api/jobs/{job_id} — no 403 for a foreign owner
- phone GET /api/phone-enrichment/jobs — all-users listing
- _owns_job helpers hard-pinned to the team model

DB isolation: db.DB_PATH monkeypatched to a temp jobs.db with a minimal
jobs table (auth overrides carry the identity; no real users needed).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

import shared.db as db  # noqa: E402

MINIMAL_JOBS_SCHEMA = """
CREATE TABLE jobs (
    job_id        TEXT PRIMARY KEY,
    user_id       TEXT,
    job_type      TEXT,
    status        TEXT,
    parent_job_id TEXT,
    original_filename TEXT DEFAULT '',
    filename      TEXT DEFAULT '',
    total         INTEGER DEFAULT 0,
    hidden_from_ui INTEGER DEFAULT 0,
    created_at    TEXT,
    updated_at    TEXT
);
"""

FOREIGN_UID = "some-other-member"


@pytest.fixture()
def jobs_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "team_jobs.db")
    monkeypatch.setattr(db._local, "conn", None, raising=False)
    conn = db.get_db()
    conn.executescript(MINIMAL_JOBS_SCHEMA)
    now = datetime.now(timezone.utc).isoformat()
    for job_id, job_type in (
        ("foreign-scraper", "scraper"),
        ("foreign-enrich", "enrichment"),
        ("foreign-phone", "phone_enrichment"),
    ):
        conn.execute(
            "INSERT INTO jobs (job_id, user_id, job_type, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'done', ?, ?)",
            (job_id, FOREIGN_UID, job_type, now, now),
        )
    conn.commit()
    yield
    conn = getattr(db._local, "conn", None)
    if conn is not None:
        conn.close()
        db._local.conn = None


@pytest.fixture()
def client(jobs_db):
    from fastapi.testclient import TestClient
    from main import app
    from shared import auth as _auth

    member = {"user_id": "me-member", "email": "me@t", "is_admin": 0}
    app.dependency_overrides[_auth.get_current_user] = lambda: member
    app.dependency_overrides[_auth.get_current_user_with_api_key] = lambda: member
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()


class TestSharedJobsSurface:
    def test_member_lists_foreign_jobs(self, client):
        r = client.get("/api/jobs")
        assert r.status_code == 200
        ids = [j["job_id"] for j in r.json()["jobs"]]
        assert "foreign-scraper" in ids
        assert "foreign-enrich" in ids

    def test_member_opens_foreign_job_detail(self, client):
        r = client.get("/api/jobs/foreign-scraper")
        assert r.status_code == 200
        assert r.json()["user_id"] == FOREIGN_UID


class TestPhoneSurface:
    def test_member_lists_all_phone_jobs(self, client):
        r = client.get("/api/phone-enrichment/jobs")
        assert r.status_code == 200
        ids = [j["job_id"] for j in r.json()["jobs"]]
        assert "foreign-phone" in ids


class TestOwnsJobHelpers:
    def test_enrichment_owns_job_is_team_permissive(self):
        from enrichment.routes import _owns_job
        member = {"user_id": "me", "is_admin": 0}
        foreign_job = {"user_id": "someone-else"}
        assert _owns_job(foreign_job, member) is True

    def test_scraper_owns_job_is_team_permissive(self):
        from scraper.routes import _owns_job
        member = {"user_id": "me", "is_admin": 0}
        foreign_job = {"user_id": "someone-else"}
        assert _owns_job(foreign_job, member) is True


class TestJobLimit:
    def test_admin_exempt_and_member_capped(self, jobs_db, monkeypatch):
        from enrichment import routes as enr

        monkeypatch.setattr(enr, "MAX_JOBS_PER_USER", 2)
        # Admin: no deletion attempt regardless of count.
        enr.enforce_job_limit("service-account", is_admin=True)
        # Member at/over cap: oldest jobs deleted.
        conn = db.get_db()
        for i, ts in enumerate(("2026-01-01", "2026-01-02", "2026-01-03")):
            conn.execute(
                "INSERT INTO jobs (job_id, user_id, job_type, status, created_at, updated_at) "
                "VALUES (?, 'mine', 'enrichment', 'done', ?, ?)",
                (f"mine-{i}", ts, ts),
            )
        conn.commit()
        enr.enforce_job_limit("mine", is_admin=False)
        remaining = [
            r["job_id"]
            for r in conn.execute(
                "SELECT job_id FROM jobs WHERE user_id = 'mine'"
            ).fetchall()
        ]
        # Cap semantics: leave MAX-1 so the NEW job being created fits under
        # the cap (excess = len - MAX + 1 deleted).
        assert len(remaining) == enr.MAX_JOBS_PER_USER - 1
        assert "mine-0" not in remaining  # oldest deleted
        assert "mine-2" in remaining      # newest kept
