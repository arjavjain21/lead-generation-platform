"""API routes for Phone Number Enrichment."""

import asyncio
import csv
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, Query
from fastapi.responses import StreamingResponse

from shared.auth import get_current_user
from shared.db import get_db

from . import job_store, pipeline

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/phone-enrichment", tags=["Phone Enrichment"])

# In-memory event signals for SSE (job_id -> asyncio.Event)
_job_signals: dict[str, asyncio.Event] = {}

# Retained background tasks so they can be cancelled on user request (Fix 4).
# job_id -> asyncio.Task
_active_phone_jobs: dict[str, asyncio.Task] = {}

# In-memory cancel flags for fast checking by the pipeline loop (Fix 4).
# Mirrors enrichment/routes.py::_cancelled_jobs. Also persisted to the DB
# (job_store.set_cancelled) for cross-worker safety.
_cancelled_phone_jobs: set[str] = set()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _get_user_id(user: dict[str, Any]) -> str:
    """Extract user ID from user object."""
    if isinstance(user, dict):
        return user.get("user_id", "")
    return str(user)


async def _on_progress(job_id: str, event: dict[str, Any]) -> None:
    """Progress callback for pipeline - signals SSE wake-up."""
    job_store.append_event(job_id, event)
    if job_id in _job_signals:
        _job_signals[job_id].set()


def _detect_linkedin_column(columns: list[str]) -> Optional[str]:
    """Auto-detect the LinkedIn URL column."""
    # Common column names
    candidates = [
        "linkedin_url",
        "linkedinurl",
        "linkedin",
        "person_linkedin_url",
        "person_linkedinurl",
        "url",
        "profile_url",
        "profileurl",
    ]

    for col in columns:
        col_lower = col.lower().strip()
        if col_lower in candidates:
            return col
        # Check if column contains linkedin
        if "linkedin" in col_lower:
            return col

    return None


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@router.get("/jobs")
async def list_phone_jobs(
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """List all phone enrichment jobs for the current user."""
    user_id = _get_user_id(user)
    jobs = job_store.get_user_phone_jobs(user_id)

    # Convert to response format
    return {
        "jobs": [
            {
                "job_id": j["job_id"],
                "status": j["status"],
                "original_filename": j.get("original_filename", ""),
                "total": j.get("total", 0),
                "processed": j.get("processed", 0),
                "phones_found": j.get("phones_found", 0),
                "error": j.get("error"),
                "created_at": j["created_at"],
            }
            for j in jobs
        ]
    }


@router.post("/jobs")
async def create_phone_job(
    file: UploadFile = File(...),
    linkedin_col: str = Query(default=None, description="Column name containing LinkedIn URLs"),
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """
    Create a new phone enrichment job.

    Upload a CSV file and specify which column contains LinkedIn URLs.
    """
    user_id = _get_user_id(user)

    # Validate file type
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a CSV file")

    # Read and validate CSV
    content = await file.read()
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded")

    import io
    reader = csv.DictReader(io.StringIO(text))
    columns = reader.fieldnames or []

    if not columns:
        raise HTTPException(status_code=400, detail="CSV file is empty or invalid")

    # Detect or validate LinkedIn column
    if linkedin_col:
        if linkedin_col not in columns:
            raise HTTPException(status_code=400, detail=f"Column '{linkedin_col}' not found in CSV")
    else:
        detected = _detect_linkedin_column(columns)
        if not detected:
            raise HTTPException(
                status_code=400,
                detail=f"Could not auto-detect LinkedIn URL column. Please specify 'linkedin_col' parameter. Available columns: {', '.join(columns)}"
            )
        linkedin_col = detected

    # Count valid rows (rows with LinkedIn URL)
    rows = list(reader)
    total_rows = len(rows)
    valid_rows = sum(1 for r in rows if "linkedin.com" in str(r.get(linkedin_col, "")).lower())

    if valid_rows == 0:
        raise HTTPException(status_code=400, detail="No valid LinkedIn URLs found in the specified column")

    # Save uploaded file
    import uuid
    upload_dir = Path(__file__).parent.parent / "data" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)

    file_id = str(uuid.uuid4())
    input_path = upload_dir / f"{file_id}.csv"
    with open(input_path, "w", encoding="utf-8") as f:
        f.write(text)

    # Create job
    job_id = job_store.create_phone_enrichment_job(
        user_id=user_id,
        filename=str(input_path),
        original_filename=file.filename,
        linkedin_col=linkedin_col,
        total=total_rows,
    )

    # Start background processing — retain the task so the cancel endpoint can
    # revoke it (Fix 4).
    task = asyncio.create_task(_run_job_background(job_id, input_path, linkedin_col))
    _active_phone_jobs[job_id] = task

    return {
        "job_id": job_id,
        "status": "queued",
        "total": total_rows,
        "valid_urls": valid_rows,
        "linkedin_col": linkedin_col,
        "message": f"Job created. {valid_rows} LinkedIn URLs will be enriched.",
    }


@router.get("/jobs/{job_id}")
async def get_phone_job(
    job_id: str,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Get details of a phone enrichment job."""
    user_id = _get_user_id(user)
    job = job_store.get_phone_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Verify ownership
    if job.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    # Never leak the server-absolute output_path to the client (Fix 6). The
    # download endpoint already serves the file by job_id, so expose only a
    # boolean indicating whether an output file is present.
    raw_output = job.get("output_path")
    has_output = bool(raw_output) and Path(raw_output).exists()

    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "original_filename": job.get("original_filename", ""),
        "linkedin_col": job.get("linkedin_col", ""),
        "total": job.get("total", 0),
        "processed": job.get("processed", 0),
        "phones_found": job.get("phones_found", 0),
        "error": job.get("error"),
        "has_output": has_output,
        "created_at": job["created_at"],
        "updated_at": job.get("updated_at"),
    }


@router.get("/jobs/{job_id}/stream")
async def stream_phone_job(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    """
    SSE stream for job progress updates.
    """
    user_id = _get_user_id(user)

    # Verify job exists and belongs to user
    job = job_store.get_phone_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    # Create event for this job if not exists
    if job_id not in _job_signals:
        _job_signals[job_id] = asyncio.Event()

    return StreamingResponse(
        _sse_event_stream(job_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def _sse_event_stream(job_id: str):
    """Yield SSE ``data: <JSON>\\n\\n`` lines for a phone job (Fix 1).

    Every payload is ``json.dumps(...)`` (double-quoted JSON), one JSON object
    per ``data:`` line, so the frontend's ``JSON.parse(event.data)`` works. On a
    terminal status the stream flushes any pending events then emits an explicit
    ``{type:'status'}`` marker and closes. Heartbeats (on 30s idle) are also
    valid JSON.
    """
    last_seq = -1

    while True:
        # Check if job has reached a terminal state
        job = job_store.get_phone_job(job_id)
        if job and job["status"] in ("done", "failed", "cancelled", "abandoned"):
            # Flush any pending events before closing (each as a JSON object)
            for evt in job_store.get_events_since(job_id, last_seq):
                last_seq = max(last_seq, evt.get("seq", last_seq))
                yield f"data: {json.dumps(evt)}\n\n"
            # Explicit terminal marker so the client closes deterministically
            yield f"data: {json.dumps({'type': 'status', 'status': job['status'], 'job_id': job_id})}\n\n"
            break

        # Stream any new events (each one JSON-encoded, one object per line)
        for evt in job_store.get_events_since(job_id, last_seq):
            last_seq = max(last_seq, evt.get("seq", last_seq))
            yield f"data: {json.dumps(evt)}\n\n"

        # Wait for next signal or emit a heartbeat
        try:
            await asyncio.wait_for(_job_signals[job_id].wait(), timeout=30.0)
            _job_signals[job_id].clear()
        except asyncio.TimeoutError:
            # Heartbeat must also be valid JSON (Fix 1)
            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"


@router.get("/jobs/{job_id}/download")
async def download_phone_results(
    job_id: str,
    user: dict = Depends(get_current_user),
):
    """Download the enriched CSV file."""
    user_id = _get_user_id(user)

    job = job_store.get_phone_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")

    if job.get("status") != "done":
        raise HTTPException(status_code=400, detail="Job not completed yet")

    output_path = job.get("output_path")
    if not output_path or not Path(output_path).exists():
        raise HTTPException(status_code=404, detail="Output file not found")

    # Generate download filename
    original_name = job.get("original_filename", "enriched").replace(".csv", "")
    download_name = f"{original_name}_with_phones.csv"

    def iterfile():
        with open(output_path, "rb") as f:
            yield from f

    return StreamingResponse(
        iterfile(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{download_name}"',
        },
    )


# -----------------------------------------------------------------------------
# Background Processing
# -----------------------------------------------------------------------------

async def _run_job_background(job_id: str, input_path: Path, linkedin_col: str) -> None:
    """Run phone enrichment in background.

    Mirrors enrichment/routes.py::_run_linkedin_v2_job: sets an initial
    heartbeat, starts a 30s heartbeat task (so the startup reaper can tell a
    live job from a crashed one), and cleans up in finally.
    """
    try:
        # Set status to running
        job_store.update_job_status(job_id, "running")
        # Initial heartbeat so cleanup_stale_phone_jobs doesn't mark us abandoned
        try:
            job_store.heartbeat(job_id)
        except Exception as e:
            logger.warning("Initial heartbeat for %s failed: %s", job_id, e)

        # Start heartbeat task (updates last_heartbeat every 30s). Mirrors the
        # enrichment runner's heartbeat_loop exactly.
        async def heartbeat_loop():
            try:
                while True:
                    await asyncio.sleep(30)
                    try:
                        job_store.heartbeat(job_id)
                    except Exception as hb_err:
                        logger.warning("Heartbeat failed for %s: %s", job_id, hb_err)
            except asyncio.CancelledError:
                pass

        heartbeat_task = asyncio.create_task(heartbeat_loop())

        # Create progress callback
        async def on_progress(event: dict[str, Any]):
            await _on_progress(job_id, event)

        # Run pipeline (passing the shared cancel set so it stops promptly)
        await pipeline.run_phone_enrichment(
            job_id=job_id,
            input_path=input_path,
            linkedin_col=linkedin_col,
            on_progress=on_progress,
            cancelled_jobs=_cancelled_phone_jobs,
        )

    except Exception as e:
        logger.error(f"Phone enrichment job {job_id} failed: {e}")
        job_store.set_job_error(job_id, str(e))

    finally:
        # Cancel heartbeat task
        try:
            heartbeat_task.cancel()
        except NameError:
            # heartbeat_task was never created (failed before its definition)
            pass
        # Drop bookkeeping entries (mirrors enrichment finally block)
        _active_phone_jobs.pop(job_id, None)
        _cancelled_phone_jobs.discard(job_id)

        # Signal completion so any SSE listener wakes and sees terminal status
        if job_id in _job_signals:
            _job_signals[job_id].set()


# -----------------------------------------------------------------------------
# Cancellation (Fix 4) — mirrors enrichment/routes.py::cancel_enrichment_job
# -----------------------------------------------------------------------------

@router.post("/jobs/{job_id}/cancel")
async def cancel_phone_job(
    job_id: str,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """Cancel a running or queued phone enrichment job.

    Sets the cancel flag in both the in-memory set (fast path for the pipeline
    loop) and the database (status='cancelled', cross-worker safe), then revokes
    the retained asyncio task. The pipeline stops promptly between rows and the
    partial output file is retained on disk.
    """
    user_id = _get_user_id(user)
    job = job_store.get_phone_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Access denied")
    if job["status"] not in ("queued", "running"):
        raise HTTPException(status_code=400, detail="Only running or queued jobs can be cancelled")

    # In-memory flag (fast path checked by pipeline) + DB persistence (cross-worker)
    _cancelled_phone_jobs.add(job_id)
    job_store.set_cancelled(job_id)

    # Revoke the retained asyncio task
    task = _active_phone_jobs.get(job_id)
    if task and not task.done():
        task.cancel()

    # Wake any SSE listeners
    sig = _job_signals.pop(job_id, None)
    if sig:
        sig.set()

    logger.info("Phone job %s cancellation requested by user %s", job_id, user_id)
    return {
        "job_id": job_id,
        "status": "cancelled",
        "message": "Job has been cancelled. Partial results (if any) are retained on disk.",
    }


# -----------------------------------------------------------------------------
# Stale-job reaper (Fix 2) — mirrors enrichment/routes.py::cleanup_stale_jobs
# -----------------------------------------------------------------------------

def cleanup_stale_phone_jobs() -> None:
    """Mark phone jobs as abandoned if they were running when the server died.

    Uses heartbeat-based detection: a job is stale only if it has not received a
    heartbeat in the last 2 minutes (and has been alive > 3 minutes). This
    prevents false positives when the server restarts but the job is healthy.
    Called once at startup from main.py lifespan, right after the enrichment
    reaper.
    """
    stale = job_store.get_stale_running_phone_jobs()
    for jid in stale:
        job_store.set_abandoned(
            jid,
            "Phone job was abandoned: Server restarted or crashed while processing. "
            "The job was interrupted before completion. Please retry from the beginning.",
        )
        logger.warning("Marked stale phone job %s as abandoned on startup", jid)
