"""
Google Maps Scraper API routes.

This module provides all scraper-related endpoints:
- Region discovery (states, cities)
- Job management (create, list, get, stream, download)
- Partial downloads for running jobs
- Contacts sync to leadsdatabase.cc
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from shared import auth, db
from . import centers as centers_module
from . import crawler as crawler_module
from . import job_store
from . import sync_contacts

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "outputs"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

router = APIRouter(prefix="/api/scraper", tags=["scraper"])

# Per-job asyncio Events to wake SSE consumers
_job_signals: dict[str, asyncio.Event] = {}
_active_jobs: set[str] = set()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class StartJobRequest(BaseModel):
    query: str
    mode: str = "all"          # "all" | "states" | "cities"
    states: list[str] = []
    cities: list[str] = []
    expected_types: list[str] = []  # e.g. ["Attorney", "Law firm"] — optional filter


# ---------------------------------------------------------------------------
# Region discovery routes
# ---------------------------------------------------------------------------

@router.get("/regions/states")
async def list_states():
    """Return all canonical state names present in the centers CSV."""
    return {"states": centers_module.CANONICAL_STATES}


@router.get("/regions/cities")
async def search_cities(q: str = Query(default="", max_length=100)):
    """Fuzzy-search anchor cities for autocomplete. Returns up to 20 results."""
    results = centers_module.search_cities(q)
    return {
        "cities": [
            {"name": c["name"], "state": c["state"], "lat": c["lat"], "lng": c["lng"]}
            for c in results
        ]
    }


@router.post("/regions/estimate")
async def estimate_tasks(req: StartJobRequest, current_user: dict = Depends(auth.get_current_user)):
    """
    Returns the estimated number of API calls for the given region selection.
    Also validates state/city inputs and returns any errors.
    Includes quota status for non-admin users.
    """
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    filtered_centers, errors = centers_module.get_centers_for_job(
        mode=req.mode,
        states=req.states,
        cities=req.cities,
    )

    task_count = centers_module.estimate_task_count(filtered_centers)
    is_admin = current_user.get("is_admin", False)
    quota_status = db.get_api_quota_status(current_user["user_id"], is_admin)

    # Check if user has enough quota
    can_proceed = True
    quota_message = ""
    if not is_admin:
        allowed, limit_message = db.check_daily_request_limit(
            user_id=current_user["user_id"],
            is_admin=False,
            estimated_requests=task_count,
        )
        can_proceed = allowed
        quota_message = limit_message if not allowed else ""

    return {
        "center_count": len(filtered_centers),
        "task_count": task_count,
        "errors": errors,
        "quota": quota_status,
        "can_proceed": can_proceed,
        "quota_message": quota_message,
    }


# ---------------------------------------------------------------------------
# Job routes
# ---------------------------------------------------------------------------

def _owns_job(job: dict[str, Any], current_user: dict[str, Any]) -> bool:
    if current_user.get("is_admin"):
        return True
    return job.get("user_id") == current_user["user_id"]


@router.get("/jobs")
async def list_scraper_jobs(current_user: dict = Depends(auth.get_current_user)):
    """List scraper jobs for current user (or all for admin)."""
    store = job_store.get_store()
    if current_user.get("is_admin"):
        jobs = store.list_jobs(job_type="scraper", limit=200)
    else:
        jobs = store.list_jobs(user_id=current_user["user_id"], job_type="scraper", limit=200)
    return {"jobs": jobs}


@router.post("/jobs")
async def start_job(
    req: StartJobRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query cannot be empty.")

    api_key = os.getenv("SCRAPER_TECH_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="SCRAPER_TECH_KEY is not configured on the server.")

    filtered_centers, errors = centers_module.get_centers_for_job(
        mode=req.mode,
        states=req.states,
        cities=req.cities,
    )

    if errors and not filtered_centers:
        raise HTTPException(status_code=400, detail="; ".join(errors))

    if not filtered_centers:
        raise HTTPException(status_code=400, detail="No geographic centers found for the selected region.")

    total_tasks = centers_module.estimate_task_count(filtered_centers)

    # Check daily API request limit for non-admin users
    is_admin = current_user.get("is_admin", False)
    allowed, limit_message = db.check_daily_request_limit(
        user_id=current_user["user_id"],
        is_admin=is_admin,
        estimated_requests=total_tasks,
    )
    if not allowed:
        raise HTTPException(status_code=429, detail=limit_message)

    job_id = str(uuid.uuid4())

    regions_payload = {
        "mode": req.mode,
        "states": req.states,
        "cities": req.cities,
    }

    store = job_store.get_store()
    store.create_scraper_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        query=req.query.strip(),
        regions=regions_payload,
        total_tasks=total_tasks,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    background_tasks.add_task(
        _run_job,
        job_id=job_id,
        user_id=current_user["user_id"],
        is_admin=is_admin,
        query=req.query.strip(),
        filtered_centers=filtered_centers,
        api_key=api_key,
        output_path=output_path,
        expected_types=req.expected_types or [],
    )

    return {
        "job_id": job_id,
        "total_tasks": total_tasks,
        "center_count": len(filtered_centers),
        "warnings": errors,
    }


@router.get("/jobs/{job_id}")
async def get_scraper_job(job_id: str, current_user: dict = Depends(auth.get_current_user)):
    """Get a scraper job by ID."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    return job_data


@router.get("/jobs/{job_id}/stream")
async def stream_scraper_job(
    job_id: str,
    token: Optional[str] = Query(default=None),
    current_user: Optional[dict] = Depends(auth.get_current_user_optional),
):
    """
    SSE stream of scraper job progress events.
    Supports reconnection: replays all stored events then streams live.
    """
    if current_user is None:
        if token:
            current_user = auth.decode_token(token)
        else:
            raise HTTPException(status_code=401, detail="Authentication required.")

    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    async def event_generator():
        sent = 0
        while True:
            new_events = store.get_events_from(job_id, sent)
            for event in new_events:
                sent += 1
                yield f"data: {json.dumps(event)}\n\n"

            current = store.get_job(job_id)
            if current and current["status"] in ("done", "failed"):
                final = {
                    "done": True,
                    "error": current.get("error"),
                    "total_tasks": current.get("total_tasks", 0),
                    "done_tasks": current.get("done_tasks", 0),
                    "result_count": current.get("result_count", 0),
                }
                yield f"data: {json.dumps(final)}\n\n"
                break

            sig = _job_signals.get(job_id)
            if sig:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(asyncio.ensure_future(_wait_event(sig))),
                        timeout=2.0,
                    )
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(2.0)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/jobs/{job_id}/download")
async def download_scraper_result(job_id: str, current_user: dict = Depends(auth.get_current_user)):
    """Download the full CSV output of a completed scraper job."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if job_data["status"] in ("queued", "running"):
        raise HTTPException(status_code=202, detail="Job not finished yet.")
    if job_data["status"] == "failed":
        raise HTTPException(status_code=500, detail=f"Job failed: {job_data.get('error')}")

    output_path = job_data.get("output_path")
    if not output_path or not Path(output_path).exists():
        raise HTTPException(status_code=404, detail="Output file not found.")

    query_slug = job_data.get("query", "results")[:30].replace(" ", "-").replace("/", "-")
    filename = f"{query_slug}_{job_id[:8]}.csv"

    return FileResponse(path=output_path, media_type="text/csv", filename=filename)


@router.get("/jobs/{job_id}/partial-download")
async def partial_download_scraper(job_id: str, current_user: dict = Depends(auth.get_current_user)):
    """
    Download partial CSV results from a running scraper job.
    Returns whatever data has been written so far.
    """
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    output_path = OUTPUT_DIR / f"{job_id}.csv"
    if not output_path.exists():
        raise HTTPException(status_code=404, detail="No data available yet.")

    query_slug = job_data.get("query", "partial")[:30].replace(" ", "-").replace("/", "-")
    filename = f"partial_{query_slug}_{job_id[:8]}.csv"

    return FileResponse(path=output_path, media_type="text/csv", filename=filename)


@router.post("/jobs/{job_id}/sync-to-contacts")
async def sync_scraper_job_to_contacts(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Sync scraper job results to the Contacts DB (leadsdatabase.cc).
    Uses place_id for deduplication — existing records are not replaced.
    """
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "scraper":
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if job_data["status"] != "done":
        raise HTTPException(status_code=400, detail="Job must be complete before syncing.")
    output_path = job_data.get("output_path")
    if not output_path or not Path(output_path).exists():
        raise HTTPException(status_code=404, detail="Output file not found.")

    try:
        result = sync_contacts.sync_job_to_contacts(Path(output_path))
        return {"ok": True, **result}
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------

async def _run_job(
    job_id: str,
    user_id: str,
    is_admin: bool,
    query: str,
    filtered_centers: list[dict[str, Any]],
    api_key: str,
    output_path: Path,
    expected_types: list[str] | None = None,
) -> None:
    store = job_store.get_store()
    store.set_running(job_id)
    seq = [0]
    requests_made = [0]  # Track actual API requests made

    async def on_progress(event: dict[str, Any]) -> None:
        store.append_event(job_id, seq[0], event)
        seq[0] += 1
        sig = _job_signals.get(job_id)
        if sig:
            sig.set()
            sig.clear()

        # Track API requests - each completed task = 1 API request
        # Only track for non-admin users
        if event.get("task_done") and not is_admin:
            requests_made[0] += 1
            # Record the request (record in batch every 10 requests to reduce DB writes)
            if requests_made[0] % 10 == 0:
                db.record_api_requests(user_id, 10)

    try:
        result_count = await crawler_module.run_crawl(
            job_id=job_id,
            query=query,
            centers=filtered_centers,
            api_key=api_key,
            output_path=output_path,
            on_progress=on_progress,
            expected_types=expected_types or [],
        )

        # Record any remaining requests (non-multiple of 10)
        if not is_admin and requests_made[0] % 10 != 0:
            db.record_api_requests(user_id, requests_made[0] % 10)

        store.set_done(job_id, str(output_path))
        logger.info("Scraper job %s done — %d unique results, %d API requests", job_id[:8], result_count, requests_made[0])

    except Exception as e:
        logger.exception("Scraper job %s failed: %s", job_id[:8], e)
        store.set_failed(job_id, str(e))

    finally:
        _active_jobs.discard(job_id)
        sig = _job_signals.pop(job_id, None)
        if sig:
            sig.set()


async def _wait_event(event: asyncio.Event) -> None:
    await event.wait()
    event.clear()


def cleanup_stale_jobs() -> None:
    """Mark jobs as failed if they were running when server restarted."""
    store = job_store.get_store()
    stale = store.get_stale_running_jobs()
    for job_id in stale:
        store.set_failed(job_id, "Server restarted while job was running.")
        logger.warning("Marked stale scraper job %s as failed", job_id)
