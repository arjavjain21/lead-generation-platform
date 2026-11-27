"""Login hardening tests: lockout, telemetry, inactive accounts.

Multi-user rollout (2026-11-26). Pins:
- N failed logins for an email/IP → 429 with Retry-After (no new state:
  counted from user_activity_log 'login_failed' rows).
- Successful login stamps users.last_login_at/last_login_ip and writes a
  'login' audit row synchronously.
- Deactivated accounts fail closed with the same 401 shape as bad
  credentials (no user enumeration).

DB isolation: auth.DB_PATH + db.DB_PATH monkeypatched to temp files,
thread-local conns reset (test_auth_last_used_debounce precedent).
"""

from __future__ import annotations

import os
import sys

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

import shared.audit as audit  # noqa: E402
import shared.auth as auth  # noqa: E402
import shared.db as db  # noqa: E402


@pytest.fixture()
def auth_env(tmp_path, monkeypatch):
    # auth and db share ONE temp file — in production both modules point at
    # the same jobs.db, and the lockout path needs users + user_activity_log
    # side by side.
    shared_db = tmp_path / "lockout_shared.db"
    monkeypatch.setattr(auth, "DB_PATH", shared_db)
    monkeypatch.setattr(auth._local, "conn", None, raising=False)
    monkeypatch.setattr(db, "DB_PATH", shared_db)
    monkeypatch.setattr(db._local, "conn", None, raising=False)
    auth.init_auth_db()
    db.init_db()
    monkeypatch.setattr(audit, "AUDIT_ENABLED", True)
    monkeypatch.setenv("LOGIN_LOCKOUT_THRESHOLD", "3")
    auth.create_user("victim@test.local", "goodpassword1")
    yield
    audit.stop(timeout=1.0)
    for mod in (auth, db):
        conn = getattr(mod._local, "conn", None)
        if conn is not None:
            conn.close()
            mod._local.conn = None


@pytest.fixture()
def client(auth_env):
    from fastapi.testclient import TestClient
    from main import app
    with TestClient(app) as c:
        yield c


def _login(client, email="victim@test.local", password="goodpassword1"):
    return client.post("/api/auth/login", json={"email": email, "password": password})


class TestLockout:
    def test_threshold_failures_then_429(self, client):
        for _ in range(3):
            r = _login(client, password="wrongpassword")
            assert r.status_code == 401
        r = _login(client, password="wrongpassword")
        assert r.status_code == 429
        assert r.headers.get("retry-after") == "900"

    def test_lockout_counts_by_ip_too(self, client):
        # Same IP, different (nonexistent) emails still trips the IP bucket.
        for i in range(3):
            r = _login(client, email=f"nobody{i}@test.local", password="wrongpassword")
            assert r.status_code == 401
        assert _login(client).status_code == 429

    def test_correct_password_still_works_below_threshold(self, client):
        assert _login(client, password="wrongpassword").status_code == 401
        assert _login(client, password="wrongpassword").status_code == 401
        r = _login(client)  # 2 failures < threshold 3 → success allowed
        assert r.status_code == 200
        assert r.json()["token"]


class TestTelemetry:
    def test_success_stamps_last_login_and_writes_row(self, client):
        r = _login(client)
        assert r.status_code == 200
        user = auth.get_user_by_email("victim@test.local")
        assert user["last_login_at"] is not None
        row = db.get_db().execute(
            "SELECT * FROM user_activity_log WHERE action = 'login'"
        ).fetchone()
        assert row is not None
        assert row["email"] == "victim@test.local"

    def test_failure_writes_login_failed_row(self, client):
        _login(client, password="wrongpassword")
        row = db.get_db().execute(
            "SELECT * FROM user_activity_log WHERE action = 'login_failed'"
        ).fetchone()
        assert row is not None
        assert row["email"] == "victim@test.local"


class TestInactiveAccounts:
    def test_deactivated_user_gets_same_401(self, client):
        auth.set_user_active(
            auth.get_user_by_email("victim@test.local")["user_id"], False
        )
        r = _login(client)
        assert r.status_code == 401
        assert r.json()["detail"] == "Invalid email or password."

    def test_refresh_rejected_for_deactivated_user(self, client):
        token = _login(client).json()["token"]
        uid = auth.get_user_by_email("victim@test.local")["user_id"]
        auth.set_user_active(uid, False)
        r = client.post("/api/auth/refresh", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 403
