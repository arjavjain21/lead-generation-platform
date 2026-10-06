"""Tests for the Data Explorer BFF proxy (exploration/explorer_routes.py).

Pins the proxy contract the routes docstring promises:

1. Flag gate — DATA_EXPLORER_ENABLED=false (or a missing
   CONTACTS_API_EXPLORER_TOKEN) → 503 {"detail": "Data Explorer is
   disabled"} on every data route; GET /status still answers 200 with
   {"enabled": false, "contacts_api_ok": false}.
2. Passthrough — upstream JSON body + status relayed unchanged
   (people/search, people/facets, exports create 201, views DELETE 204).
3. Streaming download — upstream Content-Type + Content-Disposition pass
   through and the bytes are unchanged.
4. Auth — no credentials → 401 (real dependency, no overrides), matching
   the repo's auth-matrix convention (tests/test_enrichment_api_key_auth.py).
5. Upstream failure — a JSON error body is relayed with its status; a
   transport error or non-JSON error body becomes
   502 {"detail": "contacts api error"}.

HTTP is mocked by injecting httpx.MockTransport through the module's
``_transport`` test seam (no network, no real token material). Auth uses
the repo's dependency-override pattern; the anonymous fixture clears all
overrides so the real JWT dependency runs.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Callable

import httpx
import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from exploration import explorer_routes  # noqa: E402
from shared import auth as _auth  # noqa: E402

BASE = "https://contacts-api.test"
TOKEN = "explorer-test-token"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_user(user_id: str = "explorer-user") -> dict[str, Any]:
    return {"user_id": user_id, "email": f"{user_id}@test.example", "is_admin": False}


def _enable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Turn the Explorer on against a fake Contacts API base URL."""
    monkeypatch.setenv("DATA_EXPLORER_ENABLED", "true")
    monkeypatch.setenv("CONTACTS_API_EXPLORER_TOKEN", TOKEN)
    monkeypatch.setenv("CONTACTS_API_BASE_URL", BASE)


def _mock_upstream(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    """Route every upstream call into the given MockTransport handler."""
    monkeypatch.setattr(explorer_routes, "_transport", httpx.MockTransport(handler))


@pytest.fixture
def client():
    """TestClient with a valid user via dependency override (auth passes)."""
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.clear()
    app.dependency_overrides[_auth.get_current_user] = lambda: _make_user()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def anon_client():
    """TestClient with NO auth overrides — the real dependency runs (401)."""
    from fastapi.testclient import TestClient
    from main import app

    app.dependency_overrides.clear()
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# 1. Flag gate (disabled by default)
# ---------------------------------------------------------------------------

class TestFlagGate:
    def test_disabled_flag_returns_503(self, client, monkeypatch):
        monkeypatch.setenv("DATA_EXPLORER_ENABLED", "false")
        monkeypatch.setenv("CONTACTS_API_EXPLORER_TOKEN", TOKEN)
        r = client.post("/api/explorer/people/search", json={"seniority": "vp"})
        assert r.status_code == 503
        assert r.json() == {"detail": "Data Explorer is disabled"}

    def test_missing_token_returns_503(self, client, monkeypatch):
        _enable(monkeypatch)
        monkeypatch.setenv("CONTACTS_API_EXPLORER_TOKEN", "")
        r = client.get("/api/explorer/views")
        assert r.status_code == 503
        assert r.json() == {"detail": "Data Explorer is disabled"}

    def test_status_reports_disabled_without_error(self, client, monkeypatch):
        monkeypatch.setenv("DATA_EXPLORER_ENABLED", "false")
        monkeypatch.setenv("CONTACTS_API_EXPLORER_TOKEN", "")
        r = client.get("/api/explorer/status")
        assert r.status_code == 200
        assert r.json() == {"enabled": False, "contacts_api_ok": False}


# ---------------------------------------------------------------------------
# 2. Passthrough (enabled + mocked Contacts API)
# ---------------------------------------------------------------------------

class TestPassthrough:
    def test_people_search_body_and_status(self, client, monkeypatch):
        _enable(monkeypatch)
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            seen["method"] = request.method
            seen["auth"] = request.headers.get("Authorization")
            seen["body"] = request.read()
            return httpx.Response(200, json={"total": 1, "people": [{"p": 1}]})

        _mock_upstream(monkeypatch, handler)
        r = client.post("/api/explorer/people/search", json={"seniority": "vp"})
        assert r.status_code == 200
        assert r.json() == {"total": 1, "people": [{"p": 1}]}
        assert seen["method"] == "POST"
        assert seen["url"] == f"{BASE}/v1/data/people/search"
        assert seen["auth"] == f"Bearer {TOKEN}"
        assert seen["body"] == b'{"seniority": "vp"}'

    def test_people_facets_body_and_status(self, client, monkeypatch):
        _enable(monkeypatch)
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            return httpx.Response(
                200, json={"facets": {"seniority": [{"value": "vp", "count": 3}]}}
            )

        _mock_upstream(monkeypatch, handler)
        r = client.post("/api/explorer/people/facets", json={"field": "seniority"})
        assert r.status_code == 200
        assert r.json() == {"facets": {"seniority": [{"value": "vp", "count": 3}]}}
        assert seen["url"] == f"{BASE}/v1/data/people/facets"

    def test_export_create_relays_201(self, client, monkeypatch):
        _enable(monkeypatch)
        body = {"export_id": "exp-1", "status": "queued"}
        _mock_upstream(
            monkeypatch,
            lambda request: httpx.Response(201, json=body),
        )
        r = client.post(
            "/api/explorer/exports",
            json={"source_type": "people", "format": "csv"},
        )
        assert r.status_code == 201
        assert r.json() == body

    def test_views_delete_relays_204(self, client, monkeypatch):
        _enable(monkeypatch)
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            seen["method"] = request.method
            return httpx.Response(204)

        _mock_upstream(monkeypatch, handler)
        r = client.delete("/api/explorer/views/view-9")
        assert r.status_code == 204
        assert r.content == b""
        assert seen["method"] == "DELETE"
        assert seen["url"] == f"{BASE}/v1/data/views/view-9"

    def test_status_healthy_when_upstream_200(self, client, monkeypatch):
        _enable(monkeypatch)
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            return httpx.Response(200, json={"status": "ok"})

        _mock_upstream(monkeypatch, handler)
        r = client.get("/api/explorer/status")
        assert r.status_code == 200
        assert r.json() == {"enabled": True, "contacts_api_ok": True}
        assert seen["url"] == f"{BASE}/health"


# ---------------------------------------------------------------------------
# 3. Streaming download
# ---------------------------------------------------------------------------

class _AsyncBytes(httpx.AsyncByteStream):
    """Async response stream yielding fixed bytes.

    Needed because ``httpx.Response(content=...)`` marks the response as
    already-consumed, which ``aiter_raw()`` (the route's streaming path)
    rejects — with a real transport the body arrives as an unread stream.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data

    async def __aiter__(self):
        yield self._data


class TestDownload:
    def test_download_streams_content_type_through(self, client, monkeypatch):
        _enable(monkeypatch)
        csv_bytes = b"person_id,full_name\np1,Alice\n"
        seen: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            return httpx.Response(
                200,
                stream=_AsyncBytes(csv_bytes),
                headers={
                    "Content-Type": "text/csv",
                    "Content-Disposition": 'attachment; filename="people.csv"',
                },
            )

        _mock_upstream(monkeypatch, handler)
        with client.stream("GET", "/api/explorer/exports/exp-1/download") as r:
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/csv")
            assert r.headers["content-disposition"] == 'attachment; filename="people.csv"'
            assert b"".join(r.iter_bytes()) == csv_bytes
        assert seen["url"] == f"{BASE}/v1/exports/exp-1/download"

    def test_download_relays_upstream_error(self, client, monkeypatch):
        _enable(monkeypatch)
        _mock_upstream(
            monkeypatch,
            lambda request: httpx.Response(404, json={"detail": "export not found"}),
        )
        r = client.get("/api/explorer/exports/exp-404/download")
        assert r.status_code == 404
        assert r.json() == {"detail": "export not found"}


# ---------------------------------------------------------------------------
# 4. Auth (repo convention: 401 without credentials)
# ---------------------------------------------------------------------------

class TestAuth:
    def test_requires_authentication(self, anon_client):
        r = anon_client.post("/api/explorer/people/search", json={"x": 1})
        assert r.status_code == 401

    def test_status_also_requires_authentication(self, anon_client):
        r = anon_client.get("/api/explorer/status")
        assert r.status_code == 401


# ---------------------------------------------------------------------------
# 5. Upstream failures (assert the implemented contract)
# ---------------------------------------------------------------------------

class TestUpstreamFailures:
    def test_upstream_500_json_relayed_with_status(self, client, monkeypatch):
        _enable(monkeypatch)
        _mock_upstream(
            monkeypatch,
            lambda request: httpx.Response(500, json={"detail": "internal boom"}),
        )
        r = client.post("/api/explorer/people/search", json={"x": 1})
        assert r.status_code == 500
        assert r.json() == {"detail": "internal boom"}

    def test_transport_error_becomes_502(self, client, monkeypatch):
        _enable(monkeypatch)

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        _mock_upstream(monkeypatch, handler)
        r = client.post("/api/explorer/people/search", json={"x": 1})
        assert r.status_code == 502
        assert r.json() == {"detail": "contacts api error"}

    def test_non_json_error_body_becomes_502(self, client, monkeypatch):
        _enable(monkeypatch)
        _mock_upstream(
            monkeypatch,
            lambda request: httpx.Response(
                502, content=b"<html>bad gateway</html>", headers={"Content-Type": "text/html"}
            ),
        )
        r = client.post("/api/explorer/people/count", json={"x": 1})
        assert r.status_code == 502
        assert r.json() == {"detail": "contacts api error"}
