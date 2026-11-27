"""Audit trail tests: writer doctrine, retention, reads, middleware scoping.

Multi-user rollout (2026-11-26). What these pin:

- ``audit(..., sync=True)`` inserts immediately (login/lockout path).
- Queued writes never raise, survive queue-full by dropping, and flush on
  ``stop()``.
- ``prune_old`` removes only rows past retention (and is a no-op when the
  kill switch is off).
- ``count_recent`` counts distinct rows matching email OR ip (lockout).
- The middleware records mutations + download-shaped GETs, skips list GETs
  and the login path (the endpoint owns those rows), and captures identity
  from a Bearer JWT even when the endpoint 401/403/404s.

DB isolation (CRITICAL — tests share the prod DB path): both ``db.DB_PATH``
and ``auth.DB_PATH`` are monkeypatched to a temp file and the thread-local
connections are reset, per the test_auth_last_used_debounce precedent.
"""

from __future__ import annotations

import os
import sys
import time
import uuid

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

import shared.audit as audit  # noqa: E402
import shared.auth as auth  # noqa: E402
import shared.db as db  # noqa: E402


@pytest.fixture()
def temp_dbs(tmp_path, monkeypatch):
    """Fresh shared jobs.db (auth + audit schemas), fully isolated."""
    # auth and db share ONE temp file — mirrors production (same jobs.db).
    shared_db = tmp_path / "audit_shared.db"
    monkeypatch.setattr(db, "DB_PATH", shared_db)
    monkeypatch.setattr(db._local, "conn", None, raising=False)
    monkeypatch.setattr(auth, "DB_PATH", shared_db)
    monkeypatch.setattr(auth._local, "conn", None, raising=False)
    auth.init_auth_db()
    db.init_db()
    monkeypatch.setattr(audit, "AUDIT_ENABLED", True)
    yield shared_db
    audit.stop(timeout=1.0)
    conn = getattr(db._local, "conn", None)
    if conn is not None:
        conn.close()
        db._local.conn = None
    conn = getattr(auth._local, "conn", None)
    if conn is not None:
        conn.close()
        auth._local.conn = None


def _rows():
    return db.get_db().execute(
        "SELECT * FROM user_activity_log ORDER BY id"
    ).fetchall()


class TestAuditWriter:
    def test_sync_insert_is_immediate(self, temp_dbs):
        audit.audit("login", method="POST", path="/api/auth/login",
                    user={"user_id": "u1", "email": "a@t"}, status=200,
                    ip="1.2.3.4", sync=True)
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["action"] == "login"
        assert rows[0]["email"] == "a@t"
        assert rows[0]["ip"] == "1.2.3.4"

    def test_queued_write_flushes_on_stop(self, temp_dbs):
        audit.audit("download", method="GET", path="/api/scraper/jobs/x/download",
                    user={"user_id": "u1", "email": "a@t"}, status=200, ip="1.2.3.4")
        assert _rows() == [] or len(_rows()) == 0 or not _rows()  # may not have flushed yet
        audit.stop(timeout=2.0)
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["action"] == "download"

    def test_queue_full_never_raises(self, temp_dbs, monkeypatch):
        fresh = type(audit._queue)(maxsize=2)
        # Fill exactly to capacity (the setup itself must not raise Full).
        fresh.put_nowait((None,) * 11)
        fresh.put_nowait((None,) * 11)
        monkeypatch.setattr(audit, "_queue", fresh)
        # No writer thread — otherwise _ensure_started would drain the queue
        # and put_nowait would never hit the Full path we're testing.
        monkeypatch.setattr(audit, "_ensure_started", lambda: None)
        # audit() on a full queue must drop silently, never raise.
        audit.audit("x", sync=False)
        audit.audit("y", sync=False)

    def test_kill_switch_disables_writes(self, temp_dbs, monkeypatch):
        monkeypatch.setattr(audit, "AUDIT_ENABLED", False)
        audit.audit("login", sync=True)  # no-op, no raise
        assert _rows() == []

    def test_meta_is_truncated_json(self, temp_dbs):
        audit.audit("m", meta={"blob": "z" * 10_000}, sync=True,
                    user={"user_id": "u1"}, ip="1.1.1.1")
        row = _rows()[0]
        assert row["meta"] is not None
        assert len(row["meta"]) <= audit._META_MAX_CHARS


class TestRetentionAndReads:
    def test_prune_old_removes_only_aged_rows(self, temp_dbs):
        old_cutoff = "2020-01-01T00:00:00+00:00"
        conn = db.get_db()
        conn.execute(
            "INSERT INTO user_activity_log (ts, action) VALUES (?, 'old')", (old_cutoff,)
        )
        conn.execute(
            "INSERT INTO user_activity_log (ts, action) VALUES (?, 'new')",
            (time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),),
        )
        conn.commit()
        removed = audit.prune_old(days=90)
        assert removed == 1
        actions = [r["action"] for r in _rows()]
        assert actions == ["new"]

    def test_prune_noop_when_disabled(self, temp_dbs, monkeypatch):
        monkeypatch.setattr(audit, "AUDIT_ENABLED", False)
        assert audit.prune_old(days=90) == 0

    def test_count_recent_email_or_ip_union(self, temp_dbs):
        for i in range(4):
            audit.audit("login_failed", email="victim@t", ip=f"9.9.9.{i}",
                        status=401, sync=True)
        for i in range(3):
            audit.audit("login_failed", email=f"other{i}@t", ip="8.8.8.8",
                        status=401, sync=True)
        assert audit.count_recent(action="login_failed", email="victim@t") == 4
        assert audit.count_recent(action="login_failed", ip="8.8.8.8") == 3
        # Same email AND ip must not double-count (UNION of ids).
        for _ in range(2):
            audit.audit("login_failed", email="both@t", ip="7.7.7.7",
                        status=401, sync=True)
        assert audit.count_recent(action="login_failed", email="both@t", ip="7.7.7.7") == 2
        assert audit.count_recent(action="login_failed") == 0  # no filters

    def test_query_activity_filters_and_paginates(self, temp_dbs):
        for i in range(5):
            audit.audit("download", email=f"u{i}@t", status=200, sync=True)
        audit.audit("login", email="u0@t", status=200, sync=True)
        rows, total = audit.query_activity(email="u0@t")
        assert total == 2
        rows, total = audit.query_activity(action="download", limit=2, offset=0)
        assert total == 5
        assert len(rows) == 2


class TestMiddlewareScoping:
    """Mutation + download-shaped GET scoping via the real app stack."""

    @pytest.fixture()
    def client(self, temp_dbs):
        from fastapi.testclient import TestClient
        from main import app
        with TestClient(app) as c:
            yield c

    def _token(self, email="member@t", is_admin=False):
        return auth.create_token(
            {"user_id": "uid-" + email, "email": email, "is_admin": is_admin}
        )

    def test_list_get_not_audited(self, client):
        client.get("/api/health")
        client.get("/api/enrichment/jobs", headers={"Authorization": f"Bearer {self._token()}"})
        audit.stop(timeout=2.0)
        assert _rows() == []

    def test_login_path_not_audited_by_middleware(self, client):
        # The endpoint owns login rows; the middleware must not double-book.
        client.post("/api/auth/login", json={"email": "nobody@t", "password": "wrongpass"})
        rows = [r for r in _rows() if r["action"] != "login_failed"]
        assert rows == []

    def test_mutation_audited_with_identity(self, client):
        # Member hits an admin endpoint → 403, and the OUTERMOST middleware
        # still records the attempt with the JWT identity attached.
        r = client.post(
            "/api/admin/users",
            json={"email": "x@t", "password": "longenough1"},
            headers={"Authorization": f"Bearer {self._token('member@t')}"},
        )
        assert r.status_code == 403
        audit.stop(timeout=2.0)
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["action"] == "admin.users"
        assert rows[0]["status"] == 403
        assert rows[0]["email"] == "member@t"

    def test_download_get_audited_even_on_404(self, client):
        missing = str(uuid.uuid4())
        r = client.get(f"/api/scraper/jobs/{missing}/download?token={self._token()}")
        assert r.status_code == 404
        audit.stop(timeout=2.0)
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["action"] == "download"
        assert rows[0]["resource_id"] == missing
        assert rows[0]["status"] == 404

    def test_stream_and_progress_gets_not_audited(self, client):
        tok = self._token()
        client.get(f"/api/scraper/jobs/{str(uuid.uuid4())}/partial-progress?token={tok}")
        client.get(f"/api/enrichment/jobs/{str(uuid.uuid4())}/stream?token={tok}")
        audit.stop(timeout=2.0)
        assert _rows() == []
