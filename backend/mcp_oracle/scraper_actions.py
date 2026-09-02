"""MCP scraper ACTION tools — the write-capable counterpart to the docs oracle.

Five tools that let an MCP client (Claude Code with an lgp_ API key) drive
the Google Maps scraper pipeline: estimate, cache query, create job, poll
status, fetch results, cancel. They call the SAME impl_* functions as
/api/external/scraper/* (scraper/external_helpers.py), so ownership, the
50K/day quota pre-check, and the MAX_EXTERNAL_SCRAPER_TASKS cap behave
identically over MCP and HTTP.

Identity: MCPAuthMiddleware authenticates every /mcp request and sets
request.scope["user"]; the MCP SDK threads the Starlette Request through
ServerMessageMetadata.request_context into ctx.request_context.request
(verified against mcp 1.28.1 — works in both JSON and SSE response modes).
_user_from_ctx reads it back.

Event-loop safety: FastMCP 1.28.1 executes SYNC tools directly on the event
loop, so every tool here is `async def` and all blocking work (SQLite, CSV
scans, centers CSV loads) is off-loaded via anyio.to_thread.run_sync — same
rationale as the 2026-08-24 MCPAuthMiddleware off-loop fix.
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

import anyio
from mcp.server.fastmcp import Context, FastMCP

from scraper import external_helpers as ext
from .server import mcp

# Hard ceiling for a single MCP results page — an LLM context can't usefully
# consume 1,000 rows of 34 fields.
MCP_MAX_RESULTS_PAGE = 200


def _user_from_ctx(ctx: Optional[Context]) -> dict[str, Any]:
    """Resolve the authenticated user MCPAuthMiddleware attached to the request.

    Raises ExternalError(unauthenticated) when absent — e.g. a tool invoked
    outside the authenticated mount.
    """
    request_context = getattr(ctx, "request_context", None) if ctx else None
    request = getattr(request_context, "request", None)
    user = getattr(request, "scope", {}).get("user") if request is not None else None
    if not user or "user_id" not in user:
        raise ext.ExternalError(
            "unauthenticated",
            "Could not resolve an authenticated user for this MCP request. "
            "Provide a valid X-API-Key or Bearer token.",
            401,
        )
    return user


def _run(fn, *args, **kwargs):
    """Run a blocking impl_* function on a worker thread (off the event loop)."""
    return anyio.to_thread.run_sync(lambda: fn(*args, **kwargs))


def _dump(payload: Any) -> str:
    """Serialize an impl payload as the JSON string tools return (oracle style)."""
    return json.dumps(payload, indent=2, default=str)


def _parse_fields(fields: Optional[list[str]]) -> Optional[list[str]]:
    if fields is None:
        return None
    return ext.validate_fields(fields)


# Tool functions are defined as plain async functions and registered against
# an explicit FastMCP instance via register_tools() — this keeps them testable
# against a fresh instance (StreamableHTTPSessionManager is once-per-instance)
# while server.py's gated import registers them on the production singleton.

async def scrape_local_businesses(
    query: str,
    mode: str = "all",
    country: str = "us",
    states: Optional[list[str]] = None,
    cities: Optional[list[str]] = None,
    zips: Optional[list[str]] = None,
    center_ids: Optional[list[str]] = None,
    expected_types: Optional[list[str]] = None,
    dry_run: bool = True,
    prefer_cache: bool = True,
    ctx: Context = None,
) -> str:
    """Estimate or create a Google Maps scrape job for local businesses.

    dry_run=true (DEFAULT) returns centers, task count (centers x 3 zooms),
    quota status, and a cache preview WITHOUT creating anything. Set
    dry_run=false to actually create the job — it is queued behind the
    platform dispatcher and must be polled with get_scrape_job_status.

    prefer_cache=true (DEFAULT) short-circuits creation when the 90-day
    cache already holds complete data for this exact request — the response
    then has served_from_cache=true with rows_available, costing nothing.

    modes: 'all' (whole country), 'states', 'cities', 'zips' (US), or
    'centers' (non-US, pass center_ids). Concurrency is server-managed.
    """
    user = _user_from_ctx(ctx)
    req = ext.ExternalScrapeRequest(
        query=query,
        mode=mode,
        country=country,
        states=states or [],
        cities=cities or [],
        zips=zips or [],
        center_ids=center_ids or [],
        expected_types=expected_types or [],
    )
    if dry_run:
        return _dump(await _run(ext.impl_estimate, user, req))
    create_req = ext.CreateJobRequest(
        query=query,
        mode=mode,
        country=country,
        states=states or [],
        cities=cities or [],
        zips=zips or [],
        center_ids=center_ids or [],
        expected_types=expected_types or [],
        prefer_cache=prefer_cache,
    )
    return _dump(await _run(ext.impl_create_job, user, create_req, "MCP"))



async def get_scrape_job_status(job_id: str, ctx: Context = None) -> str:
    """Get a scrape job's status: progress %, rows on disk, queue position,
    error, and result links. Poll this until status is terminal
    (done/failed/cancelled/abandoned), then call get_scrape_job_results."""
    user = _user_from_ctx(ctx)
    return _dump(await _run(ext.impl_job_status, user, job_id))



async def get_scrape_job_results(
    job_id: str,
    offset: int = 0,
    limit: int = 50,
    fields: Optional[list[str]] = None,
    ctx: Context = None,
) -> str:
    """Fetch scraped places from a COMPLETED job as JSON rows.

    Only works on terminal jobs — a running job returns a not_ready error
    with retry guidance; poll get_scrape_job_status first. limit is capped
    at 200 rows per call (page through with offset). fields defaults to a
    compact 13-column projection; pass ['name','phone','website'] etc. to
    customize, or ['all'] for every column.
    """
    user = _user_from_ctx(ctx)
    if fields == ["all"]:
        parsed: Optional[list[str]] = None
    else:
        parsed = _parse_fields(fields) if fields else list(ext.COMPACT_FIELDS)
    limit = max(1, min(int(limit), MCP_MAX_RESULTS_PAGE))
    data = await _run(
        ext.impl_job_results, user, job_id,
        offset=max(0, int(offset)), limit=limit, fields=parsed,
    )
    return _dump(data)



async def check_scrape_cache(
    query: str,
    mode: str = "all",
    country: str = "us",
    states: Optional[list[str]] = None,
    cities: Optional[list[str]] = None,
    zips: Optional[list[str]] = None,
    center_ids: Optional[list[str]] = None,
    expected_types: Optional[list[str]] = None,
    limit: int = 5,
    ctx: Context = None,
) -> str:
    """Check the 90-day scrape cache for an exact prior run. A full hit means
    the data is already available (free, instant); sample rows are included
    so you can judge coverage before fetching everything."""
    user = _user_from_ctx(ctx)
    req = ext.ExternalScrapeRequest(
        query=query,
        mode=mode,
        country=country,
        states=states or [],
        cities=cities or [],
        zips=zips or [],
        center_ids=center_ids or [],
        expected_types=expected_types or [],
    )
    limit = max(1, min(int(limit), MCP_MAX_RESULTS_PAGE))
    data = await _run(
        ext.impl_cache_query, user, req,
        offset=0, limit=limit, fields=list(ext.COMPACT_FIELDS),
    )
    return _dump(data)



async def cancel_scrape_job(job_id: str, ctx: Context = None) -> str:
    """Cancel a queued or running scrape job. Partial results scraped so far
    are preserved and cached."""
    user = _user_from_ctx(ctx)
    return _dump(await _run(ext.impl_cancel_job, user, job_id))


def register_tools(mcp_instance: FastMCP, *, enabled: bool = True) -> None:
    """Register the 5 action tools on a FastMCP instance (idempotent-ish:
    FastMCP replace()es same-name tools, so double registration is safe)."""
    if not enabled:
        return
    for fn in (
        scrape_local_businesses,
        get_scrape_job_status,
        get_scrape_job_results,
        check_scrape_cache,
        cancel_scrape_job,
    ):
        mcp_instance.add_tool(fn)


# Register on the production singleton. The env gate lives HERE (not only in
# server.py's conditional import) so the module self-disables no matter how
# it gets imported/reloaded.
if os.environ.get("ENABLE_MCP_SCRAPER_TOOLS", "true").lower() == "true":
    register_tools(mcp)
