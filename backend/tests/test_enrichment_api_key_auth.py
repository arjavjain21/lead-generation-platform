"""Auth matrix for the API-key-opened enrichment surface (2026-09).

Contract under test — the drift-proof rule this repo now lives by:

    Everything under /api/enrichment/ accepts JWT Bearer OR X-API-Key,
    EXCEPT GET /jobs/{job_id}/stream (SSE — JWT only; API-key clients poll
    GET /jobs/{job_id} instead). /api/api-keys/* and /api/phone-enrichment/*
    are out of scope and stay JWT-only.

The 200-cases simulate a valid API key via dependency override of
``get_current_user_with_api_key`` (same pattern as the external scraper API
tests — no real key material in tests). The 401-cases clear all overrides so
the real dependency runs and rejects.

Job-store isolation: the ``client`` fixture patches ``routes.job_store.get_store``
to a store over a throwaway temp DB (schema mirrored from test_chain_info) so
the live 4.4 GB jobs.db is never touched.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from shared import auth as _auth  # noqa: E402

OWNER_UID = "apikey-owner"
OTHER_UID = "apikey-other"

# Minimal jobs schema — only the columns GET /jobs and GET /jobs/{id} read.
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


def _insert_job(conn, job_id, user_id=OWNER_UID, status="done"):
    iso = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO jobs (job_id, user_id, job_type, status, parent_job_id, "
        " original_filename, filename, total, processed, emails_found, "
        " output_path, source_type, created_at, updated_at) "
        "VALUES (?, ?, 'enrichment', ?, NULL, 'up.csv', 'up.csv', 3, 3, 1, "
        " NULL, 'csv_upload', ?, ?)",
        (job_id, user_id, status, iso, iso),
    )
    conn.commit()


def _make_user(user_id=OWNER_UID, is_admin=False):
    return {"user_id": user_id, "email": f"{user_id}@test.example", "is_admin": is_admin}


@pytest.fixture
def enrichment_store(temp_db, monkeypatch):
    """Patch routes.job_store.get_store -> store over the temp DB so
    endpoints never touch the live jobs.db (mirrors test_chain_info)."""
    from enrichment import routes
    from enrichment.job_store import EnrichmentJobStore

    monkeypatch.setattr(routes.job_store, "get_store", lambda: EnrichmentJobStore(temp_db))
    yield temp_db


@pytest.fixture
def client():
    """TestClient with NO auth overrides — real dependencies run, so
    unauthenticated requests exercise the actual 401 paths."""
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.clear()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def api_key_client():
    """TestClient simulating a valid X-API-Key via dependency override
    (pattern: tests/test_external_scraper_api.py::TestAuthMatrix)."""
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.clear()
    app.dependency_overrides[_auth.get_current_user_with_api_key] = lambda: _make_user()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 1. 401 without credentials — every converted endpoint must reject
# ---------------------------------------------------------------------------

# (method, path, json_body_or_None, is_multipart_upload)
NO_AUTH_CASES = [
    ("get", "/api/enrichment/jobs", None, False),
    ("get", "/api/enrichment/jobs/nonexistent-job-id", None, False),
    ("get", "/api/enrichment/jobs/nonexistent-job-id/download", None, False),
    ("get", "/api/enrichment/jobs/nonexistent-job-id/partial-download", None, False),
    ("get", "/api/enrichment/jobs/nonexistent-job-id/resume-info", None, False),
    ("get", "/api/enrichment/jobs/nonexistent-job-id/recover-partial", None, False),
    ("get", "/api/enrichment/jobs/nonexistent-job-id/shards", None, False),
    ("get", "/api/enrichment/jobs/nonexistent-job-id/shard/1", None, False),
    ("get", "/api/enrichment/search/options", None, False),
    ("get", "/api/enrichment/website-scrape/status", None, False),
    ("get", "/api/enrichment/stats/sources", None, False),
    ("post", "/api/enrichment/jobs/nonexistent-job-id/cancel", None, False),
    ("post", "/api/enrichment/jobs/nonexistent-job-id/restart", None, False),
    ("post", "/api/enrichment/jobs", {"upload_id": "x", "domain_col": "y"}, False),
    ("post", "/api/enrichment/search/employees", {}, False),
    ("post", "/api/enrichment/search/companies", {}, False),
    ("post", "/api/enrichment/search/companies/enrich", {"upload_id": "x"}, False),
    ("post", "/api/enrichment/flows/domain-enrich", {"upload_id": "x", "domain_col": "y"}, False),
    ("post", "/api/enrichment/by-linkedin-v2", {"upload_id": "x"}, False),
    ("post", "/api/enrichment/by-linkedin", {"upload_id": "x"}, False),
    ("post", "/api/enrichment/by-domains", {"upload_id": "x", "domain_col": "y"}, False),
    ("post", "/api/enrichment/upload", None, True),
]


class TestAuthMatrix:
    @pytest.mark.parametrize(
        "method,path,body,multipart", NO_AUTH_CASES, ids=[c[1] for c in NO_AUTH_CASES]
    )
    def test_401_without_credentials(self, client, method, path, body, multipart):
        if multipart:
            r = client.post(path, files={"file": ("tiny.csv", "a,b\n1,2\n", "text/csv")})
        elif method == "get":
            r = client.get(path)
        else:
            r = client.post(path, json=body)
        assert r.status_code == 401, (
            f"{method.upper()} {path} returned {r.status_code} — expected 401 "
            f"(auth must reject before validation/ownership)"
        )

    def test_200_with_api_key_dependency(self, enrichment_store, api_key_client):
        # API-key identity resolves on the job listing (empty temp store → 200).
        r = api_key_client.get("/api/enrichment/jobs")
        assert r.status_code == 200
        assert r.json()["jobs"] == []

    @pytest.mark.parametrize(
        "path",
        [
            "/api/enrichment/search/options",
            "/api/enrichment/website-scrape/status",
            "/api/enrichment/stats/sources",
        ],
    )
    def test_reads_200_with_api_key_dependency(self, api_key_client, path):
        r = api_key_client.get(path)
        assert r.status_code == 200, f"GET {path} → {r.status_code} with API-key identity"

    def test_jwt_still_accepted_after_swap(self, enrichment_store):
        # Production JWT path: Authorization: Bearer <jwt> with NO X-API-Key —
        # get_current_user_with_api_key must fall through to decode_token.
        # Uses a real signed token (no overrides) so the fallthrough is
        # exercised end-to-end.
        from fastapi.testclient import TestClient
        from main import app

        token = _auth.create_token(_make_user())
        app.dependency_overrides.clear()
        try:
            with TestClient(app) as c:
                r = c.get("/api/enrichment/jobs", headers={"Authorization": f"Bearer {token}"})
            assert r.status_code == 200
        finally:
            app.dependency_overrides.clear()

    def test_sse_stream_still_jwt_only(self, client, api_key_client):
        # The deliberate exception: SSE stays JWT-only (EventSource cannot send
        # headers; keys must never leak via query strings). API-key identity
        # resolves via get_current_user_with_api_key, but the stream endpoint
        # uses get_current_user_optional — which never reads X-API-Key.
        r = api_key_client.get("/api/enrichment/jobs/some-job-id/stream")
        assert r.status_code == 401

    def test_search_employees_200_with_api_key_dependency(self, api_key_client, monkeypatch):
        # /search/employees proxies to contacts_client.search_people — stub the
        # outbound call so the test asserts auth + shape, not the external DB.
        from enrichment import contacts_client

        async def _fake_search(client, **kwargs):
            return {"total": 0, "people": [], "limit": 50, "offset": 0}

        monkeypatch.setattr(contacts_client, "search_people", _fake_search)
        r = api_key_client.post("/api/enrichment/search/employees", json={})
        assert r.status_code == 200
        assert r.json()["flow"] == "people_search"


# ---------------------------------------------------------------------------
# 2. Key identity semantics — admin sees all jobs (documented behavior)
# ---------------------------------------------------------------------------

class TestKeyIdentity:
    def test_admin_key_sees_other_users_jobs(self, enrichment_store):
        # Admin-tier key: GET /jobs lists every user's jobs (user_id=None
        # scoping) and _owns_job passes for any row.
        from fastapi.testclient import TestClient
        from main import app

        _insert_job(enrichment_store, "other-user-job", user_id=OTHER_UID)
        app.dependency_overrides.clear()
        app.dependency_overrides[_auth.get_current_user_with_api_key] = lambda: _make_user(
            is_admin=True
        )
        try:
            with TestClient(app) as c:
                listing = c.get("/api/enrichment/jobs")
                detail = c.get("/api/enrichment/jobs/other-user-job")
        finally:
            app.dependency_overrides.clear()
        assert listing.status_code == 200
        ids = [j["job_id"] for j in listing.json()["jobs"]]
        assert "other-user-job" in ids
        assert detail.status_code == 200

    def test_non_admin_key_cannot_see_other_users_jobs(self, enrichment_store):
        # Non-admin key: strictly own jobs (403 on foreign job detail).
        from fastapi.testclient import TestClient
        from main import app

        _insert_job(enrichment_store, "other-user-job", user_id=OTHER_UID)
        app.dependency_overrides.clear()
        app.dependency_overrides[_auth.get_current_user_with_api_key] = lambda: _make_user(
            is_admin=False
        )
        try:
            with TestClient(app) as c:
                detail = c.get("/api/enrichment/jobs/other-user-job")
        finally:
            app.dependency_overrides.clear()
        assert detail.status_code == 403
