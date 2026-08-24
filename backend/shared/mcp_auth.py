"""ASGI middleware that enforces JWT or API-key auth on the mounted MCP server.

Why this exists
---------------
FastAPI's ``Depends()``-based auth chain does NOT propagate to mounted ASGI
sub-apps. Empirically verified (Phase 0 Agent C, 2026-07-16): if you mount
``app.mount("/mcp", mcp_app)`` without an explicit ASGI middleware, every MCP
tool becomes publicly accessible without credentials.

This middleware closes that gap by self-scoping to the ``/mcp`` path. It
reuses the existing ``shared.auth.verify_api_key()`` and
``shared.auth.decode_token()`` primitives — no new auth logic, no new secret
handling. On success it sets ``request.scope["user"]`` so downstream MCP
tools can read identity the same way regular endpoints read
``Depends(get_current_user_with_api_key)``.

Usage
-----
Mount on the parent FastAPI app, BEFORE ``app.mount("/mcp", ...)``:

    from shared.mcp_auth import MCPAuthMiddleware
    app.add_middleware(MCPAuthMiddleware)
    app.mount("/mcp", mcp_oracle.mcp.streamable_http_app())

For defense in depth, also add it directly on the MCP sub-app so the sub-app
stays protected even if the mount path ever changes:

    mcp_app = mcp_oracle.mcp.streamable_http_app()
    mcp_app.add_middleware(MCPAuthMiddleware)

The middleware self-scopes to ``/mcp`` so adding it to the parent app does
NOT enforce MCP auth on ``/api/*`` routes (which already have their own
``Depends()`` chain).
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from . import auth


# Request handler type for BaseHTTPMiddleware dispatch signature.
Handler = Callable[[Request], Awaitable[Any]]


class MCPAuthMiddleware(BaseHTTPMiddleware):
    """Enforce JWT-or-API-key auth on ``/mcp/*`` requests.

    Self-scopes to the ``/mcp`` path — requests to ``/api/*`` and other
    paths pass through untouched. Authentication order:

    1. ``X-API-Key`` header → calls ``auth.verify_api_key()``
    2. ``Authorization: Bearer <jwt>`` → calls ``auth.decode_token()``
    3. Neither succeeds → HTTP 401 ``{"detail": "Authentication required. ..."}``

    On success, the resolved user dict is attached to ``request.scope["user"]``
    so MCP tool implementations can read identity without re-parsing headers.
    """

    async def dispatch(self, request: Request, call_next: Handler) -> Any:
        # Pass-through for anything that isn't the MCP mount.
        if not request.url.path.startswith("/mcp"):
            return await call_next(request)

        # Blocking SQLite auth must never run on the event loop (RCA
        # 2026-08-24): under write contention it stalled MCP requests for
        # the full busy_timeout=30s. The threadpool is the same warm pool
        # the sync FastAPI auth dependencies already use, so per-thread
        # SQLite connections are reused rather than re-opened.
        user = await run_in_threadpool(self._resolve_user, request)
        if user is not None:
            request.scope["user"] = user
            return await call_next(request)

        return self._unauthorized_response()

    @staticmethod
    def _resolve_user(request: Request) -> dict | None:
        """Try API key first, then JWT bearer. Return user dict or None."""
        # Path A: API key in X-API-Key header.
        api_key = request.headers.get("x-api-key")
        if api_key:
            try:
                user = auth.verify_api_key(api_key)
            except Exception:
                user = None
            if user:
                return user

        # Path B: Bearer token (JWT or API key — the auth layer accepts both).
        auth_header = request.headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
            if not token:
                return None

            # Try as API key first (lgp_ prefix indicates an API key).
            if token.startswith("lgp_"):
                try:
                    user = auth.verify_api_key(token)
                except Exception:
                    user = None
                if user:
                    return user

            # Fall through to JWT validation.
            try:
                payload = auth.decode_token(token)
            except Exception:
                # decode_token raises HTTPException(401) on bad JWT — we
                # treat any exception as auth failure and let the caller
                # emit the standard 401.
                return None
            if payload:
                return payload

        return None

    @staticmethod
    def _unauthorized_response() -> JSONResponse:
        return JSONResponse(
            status_code=401,
            content={
                "detail": (
                    "Authentication required. Provide JWT via "
                    "'Authorization: Bearer <token>' or API key via "
                    "'X-API-Key: <key>'."
                ),
            },
            headers={"WWW-Authenticate": "Bearer"},
        )
