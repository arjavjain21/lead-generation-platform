"""Admin user-management surface tests (/api/admin/users*, /activity).

Multi-user rollout (2026-11-26). Pins:
- Members are 403-denied on every admin endpoint (require_admin).
- Create / reset-password / status / quota happy paths.
- Self-deactivation is refused (one fat click must not lock out the admin).
- Deactivating a user cascades to their API keys (verify_api_key → None).
- Per-user quota drives check_daily_request_limit; admins stay exempt.

Auth is exercised with REAL signed JWTs (no dependency overrides) — the
endpoints resolve identity through the normal Depends chain, matching how
the Team page will call them. DB isolation via monkeypatched
auth.DB_PATH + db.DB_PATH (thread-local conns reset).
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
def admin_env(tmp_path, monkeypatch):
    # auth and db share ONE temp file — production has both modules on the
    # same jobs.db; the quota + key-cascade paths read across both schemas.
    shared_db = tmp_path / "adm_shared.db"
    monkeypatch.setattr(auth, "DB_PATH", shared_db)
    monkeypatch.setattr(auth._local, "conn", None, raising=False)
    monkeypatch.setattr(db, "DB_PATH", shared_db)
    monkeypatch.setattr(db._local, "conn", None, raising=False)
    auth.init_auth_db()
    db.init_db()
    monkeypatch.setattr(audit, "AUDIT_ENABLED", True)
    auth.create_user("admin@test.local", "adminpass123", is_admin=True)
    auth.create_user("member@test.local", "memberpass1")
    db.invalidate_quota_cache()
    yield
    audit.stop(timeout=1.0)
    for mod in (auth, db):
        conn = getattr(mod._local, "conn", None)
        if conn is not None:
            conn.close()
            mod._local.conn = None


@pytest.fixture()
def client(admin_env):
    from fastapi.testclient import TestClient
    from main import app
    with TestClient(app) as c:
        yield c


def _jwt(client, email, password):
    r = client.post("/api/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


@pytest.fixture()
def admin_headers(client):
    return _jwt(client, "admin@test.local", "adminpass123")


@pytest.fixture()
def member_headers(client):
    return _jwt(client, "member@test.local", "memberpass1")


def _member_uid():
    return auth.get_user_by_email("member@test.local")["user_id"]


class TestAccessControl:
    def test_member_denied_on_all_admin_endpoints(self, client, member_headers):
        uid = _member_uid()
        assert client.get("/api/admin/users", headers=member_headers).status_code == 403
        assert client.get("/api/admin/activity", headers=member_headers).status_code == 403
        assert client.post(
            "/api/admin/users", headers=member_headers,
            json={"email": "x@t", "password": "longenough1"},
        ).status_code == 403
        assert client.post(
            f"/api/admin/users/{uid}/reset-password", headers=member_headers,
            json={"new_password": "whatever123"},
        ).status_code == 403
        assert client.post(
            f"/api/admin/users/{uid}/status", headers=member_headers,
            json={"is_active": False},
        ).status_code == 403
        assert client.post(
            f"/api/admin/users/{uid}/quota", headers=member_headers,
            json={"daily_quota": 500},
        ).status_code == 403


class TestUserLifecycle:
    def test_create_user(self, client, admin_headers):
        r = client.post(
            "/api/admin/users", headers=admin_headers,
            json={"email": "newbie@test.local", "password": "welcome123"},
        )
        assert r.status_code == 200
        assert r.json()["user"]["email"] == "newbie@test.local"
        assert r.json()["user"]["is_admin"] is False
        # New user can actually log in.
        assert _jwt(client, "newbie@test.local", "welcome123")

    def test_create_user_validation(self, client, admin_headers):
        r = client.post(
            "/api/admin/users", headers=admin_headers,
            json={"email": "bad", "password": "x"},
        )
        assert r.status_code == 400

    def test_reset_password(self, client, admin_headers):
        uid = _member_uid()
        r = client.post(
            f"/api/admin/users/{uid}/reset-password", headers=admin_headers,
            json={"new_password": "rotatedpass9"},
        )
        assert r.status_code == 200
        assert _jwt(client, "member@test.local", "rotatedpass9")

    def test_deactivate_cascades_to_api_keys(self, client, admin_headers):
        uid = _member_uid()
        key = auth.create_api_key(uid, "member key")
        assert auth.verify_api_key(key["api_key"]) is not None

        r = client.post(
            f"/api/admin/users/{uid}/status", headers=admin_headers,
            json={"is_active": False},
        )
        assert r.status_code == 200
        assert auth.verify_api_key(key["api_key"]) is None  # key dead too
        # Re-activating the user does NOT resurrect the killed key.
        client.post(f"/api/admin/users/{uid}/status", headers=admin_headers,
                    json={"is_active": True})
        assert auth.verify_api_key(key["api_key"]) is None

    def test_self_deactivation_refused(self, client, admin_headers):
        admin_uid = auth.get_user_by_email("admin@test.local")["user_id"]
        r = client.post(
            f"/api/admin/users/{admin_uid}/status", headers=admin_headers,
            json={"is_active": False},
        )
        assert r.status_code == 400

    def test_unknown_user_404(self, client, admin_headers):
        r = client.post(
            "/api/admin/users/does-not-exist/status", headers=admin_headers,
            json={"is_active": False},
        )
        assert r.status_code == 404


class TestQuota:
    def test_quota_override_drives_limit(self, client, admin_headers):
        uid = _member_uid()
        r = client.post(
            f"/api/admin/users/{uid}/quota", headers=admin_headers,
            json={"daily_quota": 5},
        )
        assert r.status_code == 200
        assert r.json()["daily_quota_resolved"] == 5

        db.record_api_requests(uid, 5)
        allowed, msg = db.check_daily_request_limit(uid, is_admin=False, estimated_requests=1)
        assert allowed is False

        # Admins stay exempt regardless of any stored quota.
        admin_uid = auth.get_user_by_email("admin@test.local")["user_id"]
        allowed, _ = db.check_daily_request_limit(admin_uid, is_admin=True, estimated_requests=10**9)
        assert allowed is True

    def test_quota_clear_falls_back_to_env_default(self, client, admin_headers, monkeypatch):
        uid = _member_uid()
        client.post(f"/api/admin/users/{uid}/quota", headers=admin_headers,
                    json={"daily_quota": 5})
        r = client.post(f"/api/admin/users/{uid}/quota", headers=admin_headers,
                        json={"daily_quota": None})
        assert r.status_code == 200
        assert r.json()["daily_quota_resolved"] == db.DEFAULT_USER_DAILY_QUOTA

    def test_env_default_when_no_override(self, admin_env):
        assert db.get_user_daily_quota(_member_uid()) == db.DEFAULT_USER_DAILY_QUOTA


class TestActivityFeed:
    def test_activity_lists_admin_actions(self, client, admin_headers):
        client.post(
            "/api/admin/users", headers=admin_headers,
            json={"email": "feed@test.local", "password": "longenough1"},
        )
        audit.stop(timeout=2.0)
        r = client.get("/api/admin/activity", headers=admin_headers,
                       params={"action": "admin.users"})
        assert r.status_code == 200
        events = r.json()["events"]
        assert len(events) >= 1
        assert events[0]["action"] == "admin.users"
        assert events[0]["email"] == "admin@test.local"

    def test_activity_email_filter(self, client, admin_headers):
        audit.audit("login", email="someone@t", status=200, sync=True)
        r = client.get("/api/admin/activity", headers=admin_headers,
                       params={"email": "someone@t"})
        assert all(e["email"] == "someone@t" for e in r.json()["events"])
