"""Phase 1A + 1B tests: MCP server scaffold + MCPAuthMiddleware.

Covers:
- FastMCP instance is configured correctly (stateless, named)
- ``health://status`` resource returns "ok"
- Middleware self-scopes to ``/mcp`` (doesn't touch ``/api/*``)
- All auth scenarios: valid JWT, valid API key, expired JWT, bogus key,
  missing auth, malformed bearer
- ``request.scope["user"]`` is set correctly on success

Run:
    cd backend && source venv/bin/activate
    python -m pytest tests/test_mcp_phase1.py -v
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Ensure backend/ is on sys.path
_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))


class TestMCPServerScaffold(unittest.TestCase):
    """Phase 1A: FastMCP instance is configured correctly."""

    def setUp(self) -> None:
        # JWT_SECRET is required by auth module on import.
        os.environ.setdefault("JWT_SECRET", "test-secret-for-mcp-phase1")

    def test_mcp_instance_exists(self) -> None:
        from mcp_oracle import mcp
        self.assertIsNotNone(mcp, "mcp_oracle.mcp must be a FastMCP instance")

    def test_mcp_instance_name(self) -> None:
        from mcp_oracle import mcp
        # FastMCP stores its name on the underlying MCP server.
        # The exact attribute depends on SDK version; check common ones.
        name = getattr(mcp, "name", None) or getattr(mcp._mcp_server, "name", None)
        self.assertEqual(name, "listbuilding-docs-oracle", f"got {name!r}")

    def test_stateless_http_enabled(self) -> None:
        from mcp_oracle import mcp
        # stateless_http is stored on the FastMCP instance; verify it was set.
        # The exact attribute varies by SDK version — check both common shapes.
        is_stateless = (
            getattr(mcp, "stateless_http", None)
            or getattr(mcp, "_stateless_http", None)
            or getattr(getattr(mcp, "settings", None), "stateless_http", None)
        )
        self.assertTrue(is_stateless, "stateless_http must be True for multi-worker safety")

    def test_health_resource_callable(self) -> None:
        # The resource is registered as a callable; we can fetch it from the
        # FastMCP instance's resource manager and invoke it directly.
        from mcp_oracle.server import health_status
        self.assertEqual(health_status(), "ok")


class TestMCPAuthMiddleware(unittest.TestCase):
    """Phase 1B: MCPAuthMiddleware enforces auth on /mcp only."""

    def setUp(self) -> None:
        os.environ.setdefault("JWT_SECRET", "test-secret-for-mcp-phase1")
        # We construct an isolated Starlette app per test so each one is
        # hermetic — no shared state.
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.responses import JSONResponse
        from starlette.routing import Route
        from shared.mcp_auth import MCPAuthMiddleware

        def protected_endpoint(request):
            user = request.scope.get("user", {})
            return JSONResponse({"ok": True, "user_id": user.get("user_id")})

        def api_endpoint(request):
            return JSONResponse({"ok": True, "path": "api"})

        # Mount MCPAuthMiddleware on a Starlette app that simulates both
        # /mcp (protected) and /api (passthrough) routes.
        self.app = Starlette(
            routes=[
                Route("/mcp/protected", protected_endpoint),
                Route("/mcp/", protected_endpoint),
                Route("/api/enrichment/enrich", api_endpoint),
            ],
            middleware=[Middleware(MCPAuthMiddleware)],
        )

    async def _hit(self, path: str, headers: dict | None = None):
        """Invoke the ASGI app directly and return (status, body)."""
        from starlette.testclient import TestClient
        # TestClient handles async correctly for ASGI apps.
        with TestClient(self.app) as client:
            response = client.get(path, headers=headers or {})
            try:
                body = response.json()
            except Exception:
                body = response.text
            return response.status_code, body

    # --- passthrough tests (non-/mcp paths) ---

    def test_api_path_no_auth_passes_through(self) -> None:
        """Non-/mcp paths must NOT be gated by MCPAuthMiddleware."""
        import asyncio
        status, body = asyncio.run(self._hit("/api/enrichment/enrich"))
        self.assertEqual(status, 200, f"expected 200, got {status}: {body}")
        self.assertEqual(body, {"ok": True, "path": "api"})

    # --- rejection tests (no/bad auth on /mcp) ---

    def test_mcp_path_no_auth_returns_401(self) -> None:
        import asyncio
        status, body = asyncio.run(self._hit("/mcp/protected"))
        self.assertEqual(status, 401)
        self.assertIn("Authentication required", body["detail"])

    def test_mcp_path_bogus_bearer_returns_401(self) -> None:
        import asyncio
        status, _ = asyncio.run(self._hit(
            "/mcp/protected",
            headers={"Authorization": "Bearer not.a.real.jwt"},
        ))
        self.assertEqual(status, 401)

    def test_mcp_path_bogus_api_key_returns_401(self) -> None:
        import asyncio
        status, _ = asyncio.run(self._hit(
            "/mcp/protected",
            headers={"X-API-Key": "lgp_totally_bogus_key_that_does_not_exist"},
        ))
        self.assertEqual(status, 401)

    def test_mcp_path_malformed_bearer_returns_401(self) -> None:
        import asyncio
        status, _ = asyncio.run(self._hit(
            "/mcp/protected",
            headers={"Authorization": "Bearer"},  # no token after Bearer
        ))
        self.assertEqual(status, 401)

    # --- acceptance tests (valid auth on /mcp) ---

    def test_mcp_path_valid_jwt_returns_200(self) -> None:
        """Valid JWT → middleware sets scope['user'] → endpoint sees user_id."""
        import asyncio
        from shared import auth

        with patch.dict(os.environ, {"JWT_SECRET": "test-secret-for-mcp-phase1"}):
            token = auth.create_token({
                "user_id": "test-uid",
                "email": "test@example.com",
                "is_admin": 0,
            })
            status, body = asyncio.run(self._hit(
                "/mcp/protected",
                headers={"Authorization": f"Bearer {token}"},
            ))
        self.assertEqual(status, 200, f"expected 200, got {status}: {body}")
        self.assertEqual(body["user_id"], "test-uid")

    def test_mcp_path_valid_api_key_in_header_returns_200(self) -> None:
        """Valid API key in X-API-Key → middleware sets scope['user']."""
        import asyncio
        from shared import auth

        fake_user = {
            "user_id": "key-uid",
            "email": "key@example.com",
            "is_admin": False,
            "key_id": "test-key-id",
            "key_name": "test key",
        }
        with patch.object(auth, "verify_api_key", return_value=fake_user):
            status, body = asyncio.run(self._hit(
                "/mcp/protected",
                headers={"X-API-Key": "lgp_fake_but_mocked_key"},
            ))
        self.assertEqual(status, 200, f"expected 200, got {status}: {body}")
        self.assertEqual(body["user_id"], "key-uid")

    def test_mcp_path_valid_api_key_in_bearer_returns_200(self) -> None:
        """API key passed as Bearer (lgp_ prefix) is recognized."""
        import asyncio
        from shared import auth

        fake_user = {
            "user_id": "bearer-key-uid",
            "email": "bk@example.com",
            "is_admin": False,
            "key_id": "test-key-id",
            "key_name": "test key",
        }
        with patch.object(auth, "verify_api_key", return_value=fake_user):
            status, body = asyncio.run(self._hit(
                "/mcp/protected",
                headers={"Authorization": "Bearer lgp_fake_but_mocked_key"},
            ))
        self.assertEqual(status, 200)
        self.assertEqual(body["user_id"], "bearer-key-uid")

    def test_mcp_path_expired_jwt_returns_401(self) -> None:
        """Expired JWT (decode raises ExpiredSignatureError) → 401."""
        import asyncio
        import jwt
        from shared import auth

        # Manually craft an expired token.
        expired_token = jwt.encode(
            {"user_id": "expired", "email": "e@e.com", "is_admin": 0, "exp": 1},
            "test-secret-for-mcp-phase1",
            algorithm="HS256",
        )
        status, _ = asyncio.run(self._hit(
            "/mcp/protected",
            headers={"Authorization": f"Bearer {expired_token}"},
        ))
        self.assertEqual(status, 401)


if __name__ == "__main__":
    unittest.main()
