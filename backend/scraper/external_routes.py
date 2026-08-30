"""External Google Maps scraper API — /api/external/scraper/*.

API-key-first surface for external clients (Clay, scripts, MCP wrappers) to
create scraper jobs, poll progress, and fetch place data as JSON — the same
pipeline the UI drives, with the same guardrails (ownership, 50K/day quota
pre-check, MAX_EXTERNAL_SCRAPER_TASKS cap, dispatcher caps downstream).

Every endpoint accepts X-API-Key (lgp_...) or a JWT Bearer token via
auth.get_current_user_with_api_key. All responses — success AND error — use
the {success, data, error, meta} envelope (error envelope installed in
main.py, path-scoped to /api/external/*).

Handlers that read CSVs or touch SQLite are plain `def` so FastAPI runs them
in its threadpool — blocking work never stalls the event loop.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query

from shared import auth
from . import external_helpers as ext

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/external/scraper", tags=["external-scraper"])


# ---------------------------------------------------------------------------
# Query-param parsing helpers
# ---------------------------------------------------------------------------

def _parse_fields(fields: Optional[str]) -> Optional[list[str]]:
    """Comma-separated field list → validated projection. None = all fields."""
    if fields is None or fields.strip() == "":
        return None
    return ext.validate_fields([f.strip() for f in fields.split(",") if f.strip()])


# ---------------------------------------------------------------------------
# Estimate & cache
# ---------------------------------------------------------------------------

@router.post("/estimate")
def estimate(req: ext.ExternalScrapeRequest, current_user: dict = Depends(auth.get_current_user_with_api_key)):
    """Dry-run: center/task estimate + quota status + read-only cache preview.

    No side effects — the cache preview does not mutate access statistics.
    """
    return ext.envelope(ext.impl_estimate(current_user, req))


@router.post("/cache")
def cache_query(
    req: ext.ExternalScrapeRequest,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=ext.MAX_CACHE_PAGE_LIMIT),
    fields: Optional[str] = Query(None, description="Comma-separated result fields, or 'compact'"),
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """Query the 90-day cache. A full hit returns metadata + inline rows —
    free, instant, no scraper.tech cost. A miss returns the fresh-run task
    estimate. A hit whose file is gone still returns file_available=false
    (not 404) so callers can decide to re-scrape."""
    parsed = _parse_fields(fields)
    data = ext.impl_cache_query(current_user, req, offset=offset, limit=limit, fields=parsed)
    if data.get("cached"):
        return ext.envelope(
            data, total=data.get("rows_available"), limit=limit, offset=offset
        )
    return ext.envelope(data)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

@router.post("/jobs")
def create_job(req: ext.CreateJobRequest, current_user: dict = Depends(auth.get_current_user_with_api_key)):
    """Create a scraper job. prefer_cache=true (default) short-circuits a
    full cache hit — returns served_from_cache with NO job row and NO cost.
    Fresh jobs queue behind the dispatcher (6 platform / 2 per worker)."""
    return ext.envelope(ext.impl_create_job(current_user, req, source="API"))


@router.get("/jobs")
def list_jobs(
    limit: int = Query(50, ge=1, le=ext.MAX_JOB_LIST_LIMIT),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None),
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """List the caller's scraper jobs (admins see all), newest first."""
    data = ext.impl_list_jobs(current_user, limit=limit, offset=offset, status=status)
    return ext.envelope(
        data["jobs"], total=data["total"], limit=limit, offset=offset
    )


@router.get("/jobs/{job_id}")
def job_status(job_id: str, current_user: dict = Depends(auth.get_current_user_with_api_key)):
    """Detailed status: progress %, rows_on_disk, queue position, links."""
    return ext.envelope(ext.impl_job_status(current_user, job_id))


@router.get("/jobs/{job_id}/results")
def job_results(
    job_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=ext.MAX_RESULT_PAGE_LIMIT),
    fields: Optional[str] = Query(None, description="Comma-separated result fields, or 'compact'"),
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    """JSON rows from a terminal job's CSV (paginated). 409 not_ready while
    running/queued — poll GET /jobs/{job_id} first."""
    parsed = _parse_fields(fields)
    data = ext.impl_job_results(current_user, job_id, offset=offset, limit=limit, fields=parsed)
    return ext.envelope(
        data, total=data["total_rows"], limit=limit, offset=offset
    )


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, current_user: dict = Depends(auth.get_current_user_with_api_key)):
    """Cancel a queued/running job via the shared cancel core."""
    return ext.envelope(ext.impl_cancel_job(current_user, job_id))


# ---------------------------------------------------------------------------
# Quota
# ---------------------------------------------------------------------------

@router.get("/quota")
def quota(current_user: dict = Depends(auth.get_current_user_with_api_key)):
    """Daily quota snapshot (limit/used/remaining/resets_at) + external task cap."""
    return ext.envelope(ext.impl_quota(current_user))
