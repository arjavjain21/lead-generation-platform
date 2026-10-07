"""Data Explorer BFF — thin, flag-gated proxy over the Contacts API.

The Data Explorer is a read-only browsing surface over the owned contacts
database. The Contacts API (``CONTACTS_API_BASE_URL``, default
https://leadsdatabase.cc) is the data service; this module only forwards
requests over HTTPS with a server-side bearer token
(``CONTACTS_API_EXPLORER_TOKEN``). **This platform never connects to the
contacts Postgres directly.**

Contract (pinned by exploration/tests/test_explorer_routes.py):

- Every route requires the authenticated user (JWT via
  ``shared.auth.get_current_user``).
- Disabled by default: unless ``DATA_EXPLORER_ENABLED`` is truthy AND the
  token is configured, every data route returns
  ``503 {"detail": "Data Explorer is disabled"}``.
- Thin proxy: request bodies / query params are forwarded as-is and upstream
  JSON responses are relayed unchanged with their status codes (including
  upstream 4xx/5xx with a JSON body). Transport failures and non-JSON error
  bodies become ``502 {"detail": "contacts api error"}``.
- The explorer token is never logged, returned, or echoed.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask

from shared import auth

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/explorer", tags=["explorer"])

# Timeouts (mirror enrichment/contacts_client.py conventions): search/count/
# facets 30s like search_people(); downloads have no read cap (large CSV /
# gzip streams pass through byte-identical) but a bounded connect; the
# /status health probe is short so the UI fails fast.
_JSON_TIMEOUT = httpx.Timeout(30.0)
_DOWNLOAD_TIMEOUT = httpx.Timeout(None, connect=10.0)
_HEALTH_TIMEOUT = httpx.Timeout(5.0)

# Test seam only: when set, every upstream client is built with this
# transport (tests inject httpx.MockTransport). Always None in production.
_transport: Optional[httpx.AsyncBaseTransport] = None


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

import asyncio

_DOWNLOAD_SEMAPHORE = asyncio.Semaphore(2)   # concurrent buffered downloads
_DOWNLOAD_MAX_BYTES = 256 * 1024 * 1024      # 256 MB hard cap (canary scale)


def _explorer_enabled() -> bool:
    """DATA_EXPLORER_ENABLED is a bool-ish env string, default off."""
    raw = os.getenv("DATA_EXPLORER_ENABLED", "false").strip().lower()
    return raw in ("1", "true", "yes")


def _token() -> str:
    """Server-side token for the Contacts API data-explorer surface."""
    return os.getenv("CONTACTS_API_EXPLORER_TOKEN", "").strip()


def _base_url() -> str:
    return os.getenv("CONTACTS_API_BASE_URL", "https://leadsdatabase.cc").rstrip("/")


def _canary_users() -> set[str]:
    """DATA_EXPLORER_CANARY_USERS: comma-separated emails allowed to see the
    Explorer while it is in canary. Empty/unset = flag applies to everyone
    (used only once GA is explicitly approved). Default-deny by convention:
    during canary this list is set to exactly one internal operator."""
    raw = os.getenv("DATA_EXPLORER_CANARY_USERS", "").strip().lower()
    return {u.strip() for u in raw.split(",") if u.strip()}


def _user_email(user) -> str:
    email = user.get("email") if isinstance(user, dict) else getattr(user, "email", None)
    return (email or "").strip().lower()


def _canary_allowed(user) -> bool:
    allowed = _canary_users()
    return True if not allowed else _user_email(user) in allowed


def _require_upstream_token(user=None) -> str:
    """Return the token, or 503 when the Explorer is disabled.

    Enabled = flag on AND token configured AND (during canary) the caller is
    in DATA_EXPLORER_CANARY_USERS. The token value itself is never logged or
    returned — only this pass/fail decision.
    """
    token = _token()
    if not _explorer_enabled() or not token:
        raise HTTPException(status_code=503, detail="Data Explorer is disabled")
    if user is not None and not _canary_allowed(user):
        # Non-canary users are indistinguishable from "disabled" by design.
        raise HTTPException(status_code=503, detail="Data Explorer is disabled")
    return token


def _open_client(token: str, timeout: httpx.Timeout) -> httpx.AsyncClient:
    """Build the upstream client. Its headers carry the token — never log them."""
    return httpx.AsyncClient(
        base_url=_base_url(),
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
        transport=_transport,
    )


# ---------------------------------------------------------------------------
# Proxy core
# ---------------------------------------------------------------------------

def _relay(upstream: httpx.Response, *, method: str, path: str) -> Response:
    """Convert an upstream response into ours, body passed through unchanged.

    - 204 → empty 204 (e.g. views DELETE).
    - JSON body → same status code + same JSON (any status, success or error).
    - Non-JSON error body → 502 {"detail": "contacts api error"} (never relay
      raw non-JSON error text).
    - Non-JSON success body → raw bytes with the upstream content type
      (defensive; these upstream endpoints are all JSON).
    """
    if upstream.status_code == 204:
        return Response(status_code=204)
    try:
        body = upstream.json()
    except ValueError:
        if upstream.is_success:
            return Response(
                content=upstream.content,
                status_code=upstream.status_code,
                media_type=upstream.headers.get("content-type"),
            )
        logger.warning(
            "Explorer upstream %s %s returned non-JSON error (status=%d)",
            method, path, upstream.status_code,
        )
        return JSONResponse(status_code=502, content={"detail": "contacts api error"})
    return JSONResponse(status_code=upstream.status_code, content=body)


async def _proxy_json(
    method: str,
    path: str,
    *,
    payload: Optional[dict[str, Any]] = None,
    params: Optional[list[tuple[str, str]]] = None,
    user=None,
) -> Response:
    """Forward one JSON request to the Contacts API data-explorer surface.

    Transport failures become 502 {"detail": "contacts api error"}; anything
    the upstream answers (status + JSON body) is relayed by ``_relay``.
    """
    token = _require_upstream_token(user=user)
    try:
        async with _open_client(token, _JSON_TIMEOUT) as client:
            upstream = await client.request(method, path, json=payload, params=params)
    except httpx.HTTPError as exc:
        logger.warning("Explorer upstream %s %s failed: %s", method, path, exc)
        return JSONResponse(status_code=502, content={"detail": "contacts api error"})
    return _relay(upstream, method=method, path=path)


def _query_params(request: Request) -> list[tuple[str, str]]:
    """Forward the full query string (duplicate keys preserved)."""
    return list(request.query_params.multi_items())


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@router.get("/status")
async def explorer_status(
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> dict[str, Any]:
    """Feature + upstream health snapshot for the Explorer UI.

    Always 200 — this is the route that *reports* the flag. ``enabled`` is
    false when the flag is off or the token is missing (the data routes 503
    in that state). ``contacts_api_ok`` is a GET {base}/health 200 check
    with a short timeout; any failure reports false, never an error.
    """
    enabled = _explorer_enabled() and bool(_token()) and _canary_allowed(current_user)
    contacts_api_ok = False
    if enabled:
        try:
            async with _open_client(_token(), _HEALTH_TIMEOUT) as client:
                resp = await client.get("/health")
            contacts_api_ok = resp.status_code == 200
        except httpx.HTTPError as exc:
            logger.warning("Explorer contacts-api health probe failed: %s", exc)
            contacts_api_ok = False
    return {"enabled": enabled, "contacts_api_ok": contacts_api_ok}


# ---------------------------------------------------------------------------
# People (search / count / facets / detail)
# ---------------------------------------------------------------------------

@router.post("/people/search")
async def people_search(
    payload: dict[str, Any],
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy POST /v1/data/people/search — request body passthrough."""
    return await _proxy_json("POST", "/v1/data/people/search", payload=payload, user=current_user)


@router.post("/people/count")
async def people_count(
    payload: dict[str, Any],
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy POST /v1/data/people/count — request body passthrough."""
    return await _proxy_json("POST", "/v1/data/people/count", payload=payload, user=current_user)


@router.post("/people/facets")
async def people_facets(
    payload: dict[str, Any],
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy POST /v1/data/people/facets — request body passthrough."""
    return await _proxy_json("POST", "/v1/data/people/facets", payload=payload, user=current_user)


@router.get("/people/{person_id}")
async def person_detail(
    person_id: str,
    request: Request,
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy GET /v1/data/people/{person_id} (query params forwarded)."""
    return await _proxy_json(
        "GET", f"/v1/data/people/{person_id}", params=_query_params(request),
        user=current_user,
    )


# ---------------------------------------------------------------------------
# Saved views
# ---------------------------------------------------------------------------

@router.get("/views")
async def list_views(
    request: Request,
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy GET /v1/data/views (query params forwarded)."""
    return await _proxy_json("GET", "/v1/data/views", params=_query_params(request), user=current_user)


@router.post("/views")
async def create_view(
    payload: dict[str, Any],
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy POST /v1/data/views — body ``{name, filters}`` passthrough."""
    return await _proxy_json("POST", "/v1/data/views", payload=payload, user=current_user)


@router.get("/views/{view_id}")
async def get_view(
    view_id: str,
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy GET /v1/data/views/{view_id}."""
    return await _proxy_json("GET", f"/v1/data/views/{view_id}", user=current_user)


@router.put("/views/{view_id}")
async def update_view(
    view_id: str,
    payload: dict[str, Any],
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy PUT /v1/data/views/{view_id} — body passthrough."""
    return await _proxy_json("PUT", f"/v1/data/views/{view_id}", payload=payload, user=current_user)


@router.delete("/views/{view_id}")
async def delete_view(
    view_id: str,
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy DELETE /v1/data/views/{view_id} — upstream 204 relays as 204."""
    return await _proxy_json("DELETE", f"/v1/data/views/{view_id}", user=current_user)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

@router.post("/exports")
async def create_export(
    payload: dict[str, Any],
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy POST /v1/exports — body ``{source_type, view_id?, filters?,
    format, gzip?}`` passthrough; upstream 201/200 body + status relayed."""
    return await _proxy_json("POST", "/v1/exports", payload=payload, user=current_user)


@router.get("/exports")
async def list_exports(
    request: Request,
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy GET /v1/exports (query params forwarded)."""
    return await _proxy_json("GET", "/v1/exports", params=_query_params(request), user=current_user)


@router.get("/exports/{export_id}")
async def get_export(
    export_id: str,
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Proxy GET /v1/exports/{export_id}."""
    return await _proxy_json("GET", f"/v1/exports/{export_id}", user=current_user)


@router.get("/exports/{export_id}/download")
async def download_export(
    export_id: str,
    current_user: dict[str, Any] = Depends(auth.get_current_user),
) -> Response:
    """Download the export file from the Contacts API (buffered).

    Gate 2A canary note: the original streaming pass-through
    (StreamingResponse over upstream.aiter_raw()) raised
    httpx.StreamConsumed inside the gunicorn worker stack while the same
    logic worked in isolation; rather than ship a fragile stream, buffer
    the artifact (canary files are KB–~150 MB) with a hard size cap and a
    concurrency guard. True streaming can be revisited for GA.
    """
    token = _require_upstream_token(user=current_user)
    path = f"/v1/exports/{export_id}/download"
    async with _DOWNLOAD_SEMAPHORE:
        async with _open_client(token, _DOWNLOAD_TIMEOUT) as client:
            try:
                upstream = await client.send(client.build_request("GET", path))
            except httpx.HTTPError as exc:
                logger.warning("Explorer upstream GET %s failed: %s", path, exc)
                return JSONResponse(status_code=502, content={"detail": "contacts api error"})
        if not upstream.is_success:
            return _relay(upstream, method="GET", path=path)
        declared = upstream.headers.get("content-length")
        if declared and int(declared) > _DOWNLOAD_MAX_BYTES:
            return JSONResponse(status_code=413, content={
                "detail": f"export file exceeds buffered-download cap ({_DOWNLOAD_MAX_BYTES} bytes); "
                          "contact ops for direct-fetch instructions"})
        body = upstream.content
        if len(body) > _DOWNLOAD_MAX_BYTES:
            return JSONResponse(status_code=413, content={"detail": "export file exceeds buffered-download cap"})
        headers = {"Content-Type": upstream.headers.get("content-type", "application/octet-stream")}
        for header_name in ("content-disposition", "content-encoding"):
            value = upstream.headers.get(header_name)
            if value:
                headers[header_name.title()] = value
        return Response(content=body, status_code=200, headers=headers)
