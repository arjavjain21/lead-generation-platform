"""Tests that MCP middleware auth runs off the event loop.

Context (RCA 2026-08-24): MCPAuthMiddleware.dispatch called the sync
_resolve_user (blocking SQLite read + write + commit) directly on the
event loop. Under SQLite write contention this stalled the whole worker
loop for up to busy_timeout=30s — the exact 30000ms MCP client timeout.
The fix routes _resolve_user through starlette's run_in_threadpool, the
same warm pool the sync FastAPI auth dependencies already use.
"""

import asyncio
import threading
from unittest.mock import patch

from starlette.responses import PlainTextResponse

import shared.auth as auth
import shared.mcp_auth as mcp_auth


def _make_request(path, headers):
    """Minimal duck-typed Request stand-in for dispatch-level tests."""
    url = type("U", (), {"path": path})()
    return type(
        "R", (), {"headers": dict(headers), "url": url, "scope": {}}
    )()


def _bare_middleware():
    """MCPAuthMiddleware instance without calling BaseHTTPMiddleware.__init__."""
    return mcp_auth.MCPAuthMiddleware.__new__(mcp_auth.MCPAuthMiddleware)


class TestDispatchOffEventLoop:
    def test_mcp_path_resolves_user_in_threadpool(self):
        """_resolve_user must run on a threadpool thread, not the loop thread."""
        seen_threads = []

        def fake_resolve(request):
            seen_threads.append(threading.current_thread())
            return {"user_id": "u1", "email": "x@y.z"}

        async def fake_call_next(request):
            return PlainTextResponse("ok")

        async def run():
            mw = _bare_middleware()
            with patch.object(
                mcp_auth.MCPAuthMiddleware, "_resolve_user", staticmethod(fake_resolve)
            ):
                await mw.dispatch(
                    _make_request("/mcp/", {"x-api-key": "lgp_test"}), fake_call_next
                )

        loop_thread = threading.current_thread()
        asyncio.run(run())

        assert len(seen_threads) == 1
        assert seen_threads[0] is not loop_thread

    def test_non_mcp_path_skips_auth_and_threadpool(self):
        """Non-/mcp paths pass through untouched — no auth, no threadpool."""
        calls = {"next": False}

        async def fake_call_next(request):
            calls["next"] = True
            return PlainTextResponse("ok")

        async def run():
            mw = _bare_middleware()
            with patch.object(
                mcp_auth, "run_in_threadpool", wraps=mcp_auth.run_in_threadpool
            ) as spy:
                response = await mw.dispatch(
                    _make_request("/api/enrichment/enrich", {}), fake_call_next
                )
            return response, spy

        response, spy = asyncio.run(run())
        assert calls["next"] is True
        assert response.status_code == 200
        assert spy.call_count == 0

    def test_no_credentials_returns_401_without_call_next(self):
        """No credentials → 401 emitted, downstream handler never invoked."""

        async def fake_call_next(request):  # pragma: no cover - must not run
            raise AssertionError("call_next must not be reached without credentials")

        async def run():
            mw = _bare_middleware()
            return await mw.dispatch(_make_request("/mcp/", {}), fake_call_next)

        response = asyncio.run(run())
        assert response.status_code == 401
        assert "Authentication required" in response.body.decode()

    def test_valid_user_is_set_on_scope_and_request_proceeds(self):
        """Authenticated request attaches scope['user'] and continues downstream."""
        fake_user = {"user_id": "u1", "email": "x@y.z"}
        scope_seen = {}

        async def fake_call_next(request):
            scope_seen["user"] = request.scope.get("user")
            return PlainTextResponse("ok")

        async def run():
            mw = _bare_middleware()
            with patch.object(
                mcp_auth.MCPAuthMiddleware,
                "_resolve_user",
                staticmethod(lambda req: fake_user),
            ):
                return await mw.dispatch(
                    _make_request("/mcp/", {"x-api-key": "lgp_test"}), fake_call_next
                )

        response = asyncio.run(run())
        assert response.status_code == 200
        assert scope_seen["user"] == fake_user


class TestResolveUserSemantics:
    def test_api_key_header_resolves(self):
        fake_user = {"user_id": "u1"}
        req = _make_request("/mcp/", {"x-api-key": "lgp_test"})
        with patch.object(auth, "verify_api_key", return_value=fake_user):
            assert mcp_auth.MCPAuthMiddleware._resolve_user(req) == fake_user

    def test_bearer_lgp_key_resolves(self):
        fake_user = {"user_id": "u1"}
        req = _make_request("/mcp/", {"authorization": "Bearer lgp_abc"})
        with patch.object(auth, "verify_api_key", return_value=fake_user):
            assert mcp_auth.MCPAuthMiddleware._resolve_user(req) == fake_user

    def test_bad_api_key_falls_through_to_jwt(self):
        """Bogus X-API-Key must not short-circuit — falls through to Bearer/JWT."""
        jwt_payload = {"user_id": "u2", "email": "jwt@x.y"}
        req = _make_request(
            "/mcp/", {"x-api-key": "lgp_bogus", "authorization": "Bearer ejwt"}
        )
        with (
            patch.object(auth, "verify_api_key", return_value=None) as verify_spy,
            patch.object(auth, "decode_token", return_value=jwt_payload) as decode_spy,
        ):
            resolved = mcp_auth.MCPAuthMiddleware._resolve_user(req)

        assert resolved == jwt_payload
        assert verify_spy.call_count == 1  # header path attempted
        assert decode_spy.call_count == 1  # JWT fallback attempted

    def test_all_credentials_invalid_returns_none(self):
        req = _make_request(
            "/mcp/", {"x-api-key": "lgp_bogus", "authorization": "Bearer ejwt"}
        )
        with (
            patch.object(auth, "verify_api_key", return_value=None),
            patch.object(auth, "decode_token", return_value=None),
        ):
            assert mcp_auth.MCPAuthMiddleware._resolve_user(req) is None
