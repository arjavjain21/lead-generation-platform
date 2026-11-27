"""ASGI middleware recording per-user actions into the audit log.

Companion to ``shared/audit.py`` (see its module docstring for the write
doctrine). Scope is deliberately NARROW so the ~polling-heavy API surface
stays free of identity work:

- any non-GET/HEAD/OPTIONS request under ``/api/*`` (job creation, cancels,
  restarts, chains, uploads, api-key changes, admin ops — including the
  401/403 rejections, because this middleware is registered LAST and
  therefore runs OUTERMOST, seeing statuses produced by inner auth layers);
- GET/HEAD only when the path is download-shaped (last two segments contain
  ``download`` / ``partial-download`` / ``recover-partial`` / ``results`` /
  ``shards`` / ``shard``). Measured against production traffic (2026-11-26):
  the big GET volumes are job-status polls, SSE streams and Clay's
  ``GET /api/enrichment/enrich/{domain}`` loop — all excluded by this rule.

Everything else passes through with ZERO extra work — no identity
resolution, no queue writes.

Identity: ``auth.resolve_request_identity`` (JWT or API key — the same
resolver MCP auth uses), plus the ``?token=`` query fallback that native
CSV downloads rely on. The API-key path hits SQLite, so resolution runs in
the threadpool (never the event loop; same discipline as MCPAuthMiddleware).

``/mcp`` is out of scope for v1 — MCP tools authenticate via API key and
``api_keys.last_used_at`` already attributes that surface coarsely.
"""

from __future__ import annotations

import re
from typing import Any, Awaitable, Callable

from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from . import audit, auth

# Request handler type for BaseHTTPMiddleware dispatch signature.
Handler = Callable[[Request], Awaitable[Any]]

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

# GET/HEAD paths worth auditing: exports. Anything else read-only is noise.
_DOWNLOAD_SEGMENTS = {
    "download",
    "partial-download",
    "recover-partial",
    "results",
    "shards",
    "shard",
    "download-template",
}

_PASS_METHODS = {"GET", "HEAD", "OPTIONS"}


def _segments(path: str) -> list[str]:
    return [s for s in path.split("/") if s]


def _is_download_shaped(path: str) -> bool:
    segs = _segments(path)
    for seg in segs[-2:]:
        if seg in _DOWNLOAD_SEGMENTS or "download" in seg:
            return True
    return False


def _should_audit(method: str, path: str) -> bool:
    # The login endpoint records its own rows synchronously (success,
    # failure, lockout) so the lockout counter sees them immediately —
    # auditing it here too would double-book every login.
    if path == "/api/auth/login":
        return False
    if method.upper() not in _PASS_METHODS:
        return True
    return _is_download_shaped(path)


def _resource_id(path: str) -> str | None:
    """First UUID in the path (job id), else the group/cache/shard token."""
    segs = _segments(path)
    for seg in segs:
        if _UUID_RE.match(seg):
            return seg
    for i, seg in enumerate(segs):
        if seg in ("group", "download") and i + 1 < len(segs):
            candidate = segs[i + 1]
            if not _UUID_RE.match(candidate):
                return candidate
    return None


def _action_for(method: str, path: str, status: int | None) -> str:
    segs = _segments(path)
    last = segs[-1] if segs else "api"
    if "/auth/login" in path:
        return "login_failed" if (status is not None and status >= 400) else "login"
    if "cancel" in last:
        return "job.cancel"
    if "restart" in last:
        return "job.restart"
    if "resume" in last:
        return "job.resume"
    if last == "chain":
        return "job.chain"
    if method.upper() in _PASS_METHODS:
        return "download"
    if len(segs) >= 2 and segs[0] == "api" and segs[1] == "admin":
        return f"admin.{last}"
    return f"{method.lower()}.{last}"


def _resolve_identity(request: Request) -> dict | None:
    """Header-based identity first; native downloads authenticate via
    ``?token=`` (JWT) which headers never carry — fall back to that."""
    user = auth.resolve_request_identity(request)
    if user is not None:
        return user
    token = request.query_params.get("token")
    if not token:
        return None
    if token.startswith("lgp_"):
        try:
            return auth.verify_api_key(token)
        except Exception:
            return None
    try:
        return auth.decode_token(token)
    except Exception:
        return None


class AuditMiddleware(BaseHTTPMiddleware):
    """Record audited /api/* actions to shared/audit.py. Never raises into
    the request path — audit.audit() is fail-safe by construction."""

    async def dispatch(self, request: Request, call_next: Handler) -> Any:
        path = request.url.path
        if not audit.AUDIT_ENABLED or not path.startswith("/api/"):
            return await call_next(request)
        if not _should_audit(request.method, path):
            return await call_next(request)

        # Identity resolution may hit SQLite (API-key path) — keep it off
        # the event loop (RCA 2026-08-24 discipline).
        user = await run_in_threadpool(_resolve_identity, request)
        ip = audit.client_ip(request)
        resource_id = _resource_id(path)

        try:
            response = await call_next(request)
        except Exception:
            audit.audit(
                _action_for(request.method, path, 500),
                method=request.method,
                path=path,
                user=user,
                status=500,
                ip=ip,
                resource_id=resource_id,
            )
            raise
        audit.audit(
            _action_for(request.method, path, response.status_code),
            method=request.method,
            path=path,
            user=user,
            status=response.status_code,
            ip=ip,
            resource_id=resource_id,
        )
        return response
