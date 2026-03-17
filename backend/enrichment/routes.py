"""
Domain Enrichment API routes.

This module provides all enrichment-related endpoints:
- CSV upload for enrichment
- Job management (create, list, get, stream, download)
- Partial downloads for running jobs (with incremental pipeline writes)
- Default cascade configuration
- List Building Tool endpoints (Flow 1, 2, 3)
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from shared import auth
from . import blitz_client
from . import job_store
from . import pipeline
from . import list_builder
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
import sync_contacts

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
OUTPUT_DIR = DATA_DIR / "outputs"
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

router = APIRouter(prefix="/api/enrichment", tags=["enrichment"])

# In-memory set of job_ids currently being actively processed
_active_jobs: set[str] = set()
# Per-job asyncio Event to wake SSE consumers
_job_signals: dict[str, asyncio.Event] = {}
# Set of jobs that have been cancelled by user
_cancelled_jobs: set[str] = set()

# ---------------------------------------------------------------------------
# Configuration: SMTP and cleanup settings
# ---------------------------------------------------------------------------

# SMTP Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "")
DEFAULT_RECIPIENT = os.getenv("DEFAULT_RECIPIENT", "arjav@eagleinfoservice.com")

# Cleanup settings
UPLOAD_RETENTION_DAYS = 7
OUTPUT_RETENTION_DAYS = 30
MAX_JOBS_PER_USER = 100


# ---------------------------------------------------------------------------
# Email notification function
# ---------------------------------------------------------------------------

async def send_job_notification(
    recipient_email: str,
    job_type: str,
    filename: str,
    status: str,
    total: int,
    processed: int,
    emails_found: int,
    error_message: Optional[str] = None
) -> None:
    """Send email notification when a job completes or fails."""
    if not SMTP_USER or not SENDER_EMAIL:
        logger.debug("SMTP not configured, skipping email notification")
        return

    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    subject = f"List Building Tool: {status.upper()} - {filename}"
    status_color = "#10b981" if status == "done" else "#ef4444"

    html_body = f"""
    <html>
    <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
            <h2 style="color: #1a315d;">List Building Tool - Job {status.upper()}</h2>
            <table style="width: 100%; border-collapse: collapse; margin-top: 20px;">
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Status:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb; color: {status_color}; font-weight: bold;">{status.upper()}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>File:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;">{filename}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Job Type:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;">{job_type}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Processed:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;">{processed} / {total}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Emails Found:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;">{emails_found}</td>
                </tr>
    """

    if error_message:
        html_body += f"""
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb;"><strong>Error:</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #e5e7eb; color: #ef4444;">{error_message}</td>
                </tr>
        """

    html_body += """
            </table>
            <p style="margin-top: 20px; color: #6b7280; font-size: 12px;">
                This is an automated notification from the List Building Tool.
            </p>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = recipient_email
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipient_email, msg.as_string())

        logger.info("Job notification email sent to %s", recipient_email)
    except Exception as e:
        logger.error("Failed to send job notification email: %s", e)


# ---------------------------------------------------------------------------
# Cleanup functions for old uploads and outputs
# ---------------------------------------------------------------------------

def cleanup_old_files() -> dict[str, int]:
    """Remove uploads older than 7 days and outputs older than 30 days."""
    import time

    now = time.time()
    upload_cutoff = now - (UPLOAD_RETENTION_DAYS * 24 * 60 * 60)
    output_cutoff = now - (OUTPUT_RETENTION_DAYS * 24 * 60 * 60)

    removed = {"uploads": 0, "outputs": 0}

    # Cleanup uploads
    if UPLOAD_DIR.exists():
        for f in UPLOAD_DIR.iterdir():
            if f.is_file():
                try:
                    if f.stat().st_mtime < upload_cutoff:
                        f.unlink()
                        removed["uploads"] += 1
                except Exception as e:
                    logger.warning("Failed to remove old upload %s: %s", f, e)

    # Cleanup outputs
    if OUTPUT_DIR.exists():
        for f in OUTPUT_DIR.iterdir():
            if f.is_file():
                try:
                    if f.stat().st_mtime < output_cutoff:
                        f.unlink()
                        removed["outputs"] += 1
                except Exception as e:
                    logger.warning("Failed to remove old output %s: %s", f, e)

    if removed["uploads"] > 0 or removed["outputs"] > 0:
        logger.info("Cleaned up old files: %s", removed)

    return removed


def enforce_job_limit(user_id: str) -> None:
    """Delete oldest jobs if user has more than MAX_JOBS_PER_USER."""
    store = job_store.get_store()
    jobs = store.list_enrichment_jobs(user_id=user_id, limit=1000)

    if len(jobs) >= MAX_JOBS_PER_USER:
        # Sort by created_at and delete oldest
        jobs_sorted = sorted(jobs, key=lambda j: j.get("created_at", ""))
        excess = len(jobs_sorted) - MAX_JOBS_PER_USER + 1

        for job in jobs_sorted[:excess]:
            try:
                store.delete_job(job["job_id"])
                logger.info("Deleted old job %s for user %s (enforcing limit)", job["job_id"], user_id)
            except Exception as e:
                logger.warning("Failed to delete old job %s: %s", job["job_id"], e)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class StartJobRequest(BaseModel):
    upload_id: str
    domain_col: str
    name_col: Optional[str] = None
    first_name_col: Optional[str] = None
    last_name_col: Optional[str] = None
    cascade: Optional[list[dict[str, Any]]] = None
    max_results: int = 5


class ChainJobRequest(BaseModel):
    """Request to chain enrichment from a scraper job output."""
    cascade: Optional[list[dict[str, Any]]] = None
    max_results: int = 5


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/default-cascade")
async def get_default_cascade():
    return {"cascade": blitz_client.DEFAULT_CASCADE}


@router.post("/upload")
async def upload_csv(
    file: UploadFile,
    current_user: dict = Depends(auth.get_current_user),
):
    """Accepts a CSV file, saves it persistently, returns upload_id and columns."""
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted.")

    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), nrows=5, skipinitialspace=True)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {e}")

    upload_id = str(uuid.uuid4())
    save_path = UPLOAD_DIR / f"{upload_id}.csv"
    save_path.write_bytes(content)

    # Save metadata alongside the CSV (original filename)
    metadata_path = UPLOAD_DIR / f"{upload_id}.metadata.json"
    metadata_path.write_text(json.dumps({"original_filename": file.filename}))

    preview = df.head(3).fillna("").astype(str).to_dict(orient="records")
    return {
        "upload_id": upload_id,
        "columns": list(df.columns),
        "preview": preview,
        "row_count": sum(1 for _ in io.StringIO(content.decode("utf-8", errors="replace"))) - 1,
        "filename": file.filename,
    }


def _owns_job(job: dict[str, Any], current_user: dict[str, Any]) -> bool:
    if current_user.get("is_admin"):
        return True
    return job.get("user_id") == current_user["user_id"]


@router.get("/jobs")
async def list_enrichment_jobs(current_user: dict = Depends(auth.get_current_user)):
    """List enrichment jobs for current user (or all for admin)."""
    store = job_store.get_store()
    if current_user.get("is_admin"):
        jobs = store.list_jobs(job_type="enrichment", limit=200)
    else:
        jobs = store.list_jobs(user_id=current_user["user_id"], job_type="enrichment", limit=200)

    # Enhance job display with user-friendly filenames
    for job in jobs:
        # Use original_filename as the primary filename for display
        if job.get("original_filename"):
            job["filename"] = job["original_filename"]
            job["display_filename"] = job["original_filename"]
        else:
            # Fallback for jobs created before this fix
            filename = job.get("filename", "")
            # If filename looks like a UUID, try to make it more user-friendly
            if filename and len(filename) == 36 and filename.count('-') == 4:
                # Generate a user-friendly name based on job ID
                friendly_name = f"uploaded_file_{job['job_id'][:8]}.csv"
                job["filename"] = friendly_name
                job["display_filename"] = friendly_name
            elif filename:
                # Use existing filename
                job["display_filename"] = f"{filename}.csv" if not filename.endswith('.csv') else filename
            else:
                job["filename"] = "Unknown.csv"
                job["display_filename"] = "Unknown.csv"

    return {"jobs": jobs}


@router.post("/jobs")
async def start_enrichment_job(
    req: StartJobRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """Starts a persistent enrichment job. Returns immediately with job_id."""
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    if req.domain_col not in df.columns:
        raise HTTPException(
            status_code=400, detail=f"Column '{req.domain_col}' not found in CSV."
        )

    rows = df.fillna("").astype(str).to_dict(orient="records")
    cascade = req.cascade if req.cascade else blitz_client.DEFAULT_CASCADE

    # Read metadata to get original filename
    metadata_path = UPLOAD_DIR / f"{req.upload_id}.metadata.json"
    original_filename = ""
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            original_filename = metadata.get("original_filename", "")
        except Exception as e:
            logger.warning("Failed to read metadata for %s: %s", req.upload_id, e)

    job_id = str(uuid.uuid4())
    store = job_store.get_store()

    # Convert cascade to JSON for storage
    cascade_json = json.dumps(cascade) if cascade else None

    # Enforce job limit before creating new job
    enforce_job_limit(current_user["user_id"])

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=str(req.upload_id),
        domain_col=req.domain_col,
        original_filename=original_filename,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade_config=cascade_json,
        max_results=req.max_results,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_job,
        job_id=job_id,
        rows=rows,
        domain_col=req.domain_col,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade=cascade,
        max_results=req.max_results,
        write_incremental=True,  # Enable incremental writes for partial downloads
    )

    return {"job_id": job_id, "total": len(rows)}


@router.get("/jobs/{job_id}")
async def get_enrichment_job(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """Get an enrichment job by ID."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    # Use original_filename for better display
    if job_data.get("original_filename"):
        job_data["filename"] = job_data["original_filename"]
        job_data["display_filename"] = job_data["original_filename"]
    else:
        # Fallback for old jobs
        filename = job_data.get("filename", "")
        if filename and len(filename) == 36 and filename.count('-') == 4:
            friendly_name = f"uploaded_file_{job_id[:8]}.csv"
            job_data["filename"] = friendly_name
            job_data["display_filename"] = friendly_name
        elif filename:
            job_data["display_filename"] = f"{filename}.csv" if not filename.endswith('.csv') else filename
        else:
            job_data["filename"] = "Unknown.csv"
            job_data["display_filename"] = "Unknown.csv"

    return job_data


@router.get("/jobs/{job_id}/stream")
async def stream_enrichment_job_progress(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """SSE stream of enrichment progress events with replay support."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
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
                    "total": current.get("total", 0),
                    "processed": current.get("processed", 0),
                    "emails_found": current.get("emails_found", 0),
                }
                yield f"data: {json.dumps(final)}\n\n"
                break

            sig = _job_signals.get(job_id)
            if sig:
                try:
                    await asyncio.wait_for(asyncio.shield(asyncio.ensure_future(_wait_event(sig))), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(2.0)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/jobs/{job_id}/download")
async def download_enrichment_result(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """Download the full CSV output of a completed enrichment job."""
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")
    if job_data["status"] in ("queued", "running"):
        raise HTTPException(status_code=202, detail="Job not finished yet.")

    # Check if job failed but has partial output available
    if job_data["status"] == "failed":
        output_path = job_data.get("output_path")
        error_msg = job_data.get("error", "")

        # If output_path is not in database, try the standard location
        if not output_path:
            output_path = OUTPUT_DIR / f"{job_id}.csv"

        # If failed but partial output exists and file is not empty
        if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
            # Allow download with a warning
            logger.info("Downloading partial results for failed job %s: %s", job_id, error_msg)
            # Continue to download below (don't raise exception)
        else:
            # No partial output available
            raise HTTPException(
                status_code=500,
                detail=f"Job failed: {error_msg}"
            )
    else:
        # For non-failed jobs, get output_path from database
        output_path = job_data.get("output_path")
        if not output_path or not Path(output_path).exists():
            raise HTTPException(status_code=404, detail="Output file not found.")
    return FileResponse(
        path=output_path,
        media_type="text/csv",
        filename=f"enriched_{job_id[:8]}.csv",
    )


@router.get("/jobs/{job_id}/partial-download")
async def partial_download_enrichment(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Download partial CSV results from a running enrichment job.
    Returns whatever enriched data has been written so far.
    """
    store = job_store.get_store()
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")
    if job_data.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Enrichment job not found.")
    if not _owns_job(job_data, current_user):
        raise HTTPException(status_code=403, detail="Access denied.")

    output_path = OUTPUT_DIR / f"{job_id}.csv"
    if not output_path.exists():
        raise HTTPException(status_code=404, detail="No enriched data yet.")

    return FileResponse(
        path=output_path,
        media_type="text/csv",
        filename=f"partial_enriched_{job_id[:8]}.csv",
    )


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------

async def _run_background_sync(job_id: str, output_path: Path) -> None:
    """
    Background task to sync enrichment results to Contacts DB.
    Runs asynchronously without blocking the API.
    """
    try:
        logger.info("Auto-syncing enrichment job %s to contacts DB (person records)", job_id)
        sync_result = sync_contacts.sync_enrichment_to_contacts(output_path)
        logger.info("Auto-sync complete for job %s: %s", job_id, sync_result)
    except Exception as sync_err:
        logger.error("Auto-sync failed for job %s: %s", job_id, sync_err)


async def _run_job(
    job_id: str,
    rows: list[dict[str, Any]],
    domain_col: str,
    name_col: Optional[str],
    first_name_col: Optional[str],
    last_name_col: Optional[str],
    cascade: list[dict[str, Any]],
    max_results: int,
    write_incremental: bool = False,
):
    store = job_store.get_store()
    store.set_running(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    async def on_progress(e: dict[str, Any]):
        # Get FRESH store instance for this thread
        # This fixes the progress counter bug where background tasks couldn't commit
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], e)
        seq[0] += 1
        sig = _job_signals.get(job_id)
        if sig:
            sig.set()
            sig.clear()

    try:
        output_rows = await pipeline.run_pipeline(
            rows=rows,
            domain_col=domain_col,
            name_col=name_col,
            first_name_col=first_name_col,
            last_name_col=last_name_col,
            cascade=cascade,
            max_results=max_results,
            on_progress=on_progress,
            write_incremental=write_incremental,
            output_path=output_path,
            cancelled_jobs=_cancelled_jobs,
            job_id=job_id,
        )

        # If not writing incrementally, write final output
        if not write_incremental:
            if output_rows:
                out_df = pd.DataFrame(output_rows)
                input_cols = [c for c in out_df.columns if c not in pipeline.ENRICHED_COLUMNS]
                ordered = input_cols + [c for c in pipeline.ENRICHED_COLUMNS if c in out_df.columns]
                out_df[ordered].to_csv(str(output_path), index=False)
            else:
                output_path.write_text("")

        store.set_done(job_id, str(output_path))
        logger.info("Enrichment job %s completed, %d output rows", job_id, len(output_rows))

        # Run auto-sync in the background without blocking the API
        # This prevents the refresh button from getting stuck
        asyncio.create_task(_run_background_sync(job_id, output_path))

        # Get job details for email notification
        job = store.get_enrichment_job(job_id)
        if job:
            await send_job_notification(
                recipient_email=DEFAULT_RECIPIENT,
                job_type="enrichment",
                filename=job.get("original_filename") or job.get("filename", "Unknown"),
                status="done",
                total=job.get("total", 0),
                processed=job.get("processed", 0),
                emails_found=job.get("emails_found", 0)
            )

    except RuntimeError as e:
        # Handle job cancellation
        if "was cancelled" in str(e):
            logger.info("Enrichment job %s was cancelled by user", job_id)
            # Job already marked as failed by cancel endpoint
            # Just ensure cleanup happens in finally block
            if output_path.exists():
                partial_size = output_path.stat().st_size
                if partial_size > 0:
                    logger.info("Cancelled job %s has partial output available: %d bytes", job_id, partial_size)
        else:
            # Other RuntimeErrors should be handled as normal failures
            logger.exception("Enrichment job %s failed with RuntimeError: %s", job_id, e)
            store.set_failed(job_id, f"Job failed: {str(e)}")
            # Send failure notification
            job = store.get_enrichment_job(job_id)
            if job:
                await send_job_notification(
                    recipient_email=DEFAULT_RECIPIENT,
                    job_type="enrichment",
                    filename=job.get("original_filename") or job.get("filename", "Unknown"),
                    status="failed",
                    total=job.get("total", 0),
                    processed=job.get("processed", 0),
                    emails_found=job.get("emails_found", 0),
                    error_message=str(e)
                )
    except Exception as e:
        logger.exception("Enrichment job %s failed: %s", job_id, e)

        # Provide user-friendly error message
        error_msg = str(e)
        error_lower = error_msg.lower()

        # Categorize error for better user feedback
        if "column" in error_lower and "not found" in error_lower:
            user_msg = f"Configuration error: The specified column was not found in the CSV file. {error_msg}"
        elif "authentication" in error_lower or "unauthorized" in error_lower:
            user_msg = "Authentication error. Please log in again."
        elif "timeout" in error_lower:
            user_msg = "Request timeout. The API took too long to respond. Please try again."
        elif "rate limit" in error_lower or "429" in error_msg:
            user_msg = "Rate limit exceeded. Please wait a few minutes and try again."
        else:
            user_msg = f"Job encountered an error: {error_msg}"

        # Check if we have partial output available
        if output_path.exists():
            partial_size = output_path.stat().st_size
            if partial_size > 0:
                user_msg += " (Partial results are available for download)"
                # Mark as done instead of failed so user can download partial results
                store.set_done(job_id, str(output_path))
                logger.warning("Enrichment job %s completed with errors, partial output available: %s", job_id, user_msg)
            else:
                store.set_failed(job_id, user_msg)
                # Send failure notification
                job = store.get_enrichment_job(job_id)
                if job:
                    await send_job_notification(
                        recipient_email=DEFAULT_RECIPIENT,
                        job_type="enrichment",
                        filename=job.get("original_filename") or job.get("filename", "Unknown"),
                        status="failed",
                        total=job.get("total", 0),
                        processed=job.get("processed", 0),
                        emails_found=job.get("emails_found", 0),
                        error_message=user_msg
                    )
        else:
            store.set_failed(job_id, user_msg)
            # Send failure notification
            job = store.get_enrichment_job(job_id)
            if job:
                await send_job_notification(
                    recipient_email=DEFAULT_RECIPIENT,
                    job_type="enrichment",
                    filename=job.get("original_filename") or job.get("filename", "Unknown"),
                    status="failed",
                    total=job.get("total", 0),
                    processed=job.get("processed", 0),
                    emails_found=job.get("emails_found", 0),
                    error_message=user_msg
                )

    finally:
        _active_jobs.discard(job_id)
        sig = _job_signals.pop(job_id, None)
        if sig:
            sig.set()


async def _wait_event(event: asyncio.Event):
    await event.wait()
    event.clear()


@router.post("/jobs/{job_id}/restart")
async def restart_enrichment_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Restart a failed enrichment job with the same configuration.

    Creates a new job using the same CSV file and configuration as the original.
    The original job_id is preserved in the parent_job_id field for tracking.
    """
    store = job_store.get_store()
    original_job = store.get_job(job_id)

    if not original_job:
        raise HTTPException(status_code=404, detail="Job not found")

    if original_job.get("job_type") != "enrichment":
        raise HTTPException(status_code=400, detail="Only enrichment jobs can be restarted")

    if not _owns_job(original_job, current_user):
        raise HTTPException(status_code=403, detail="Access denied")

    if original_job["status"] != "failed":
        raise HTTPException(status_code=400, detail="Only failed jobs can be restarted")

    # Read the original CSV file (kept in uploads/)
    upload_path = UPLOAD_DIR / f"{original_job['filename']}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Original CSV file not found")

    try:
        df = pd.read_csv(str(upload_path), skipinitialspace=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read original CSV: {e}")

    # Validate domain column exists
    domain_col = original_job.get('domain_col', '')
    if not domain_col or domain_col not in df.columns:
        raise HTTPException(status_code=400, detail=f"Domain column '{domain_col}' not found in CSV")

    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Parse cascade configuration from JSON
    cascade = None
    cascade_json = original_job.get('cascade_config')
    if cascade_json:
        try:
            cascade = json.loads(cascade_json)
        except Exception as e:
            logger.warning("Failed to parse cascade_config for job %s: %s", job_id, e)
            cascade = None

    if not cascade:
        cascade = blitz_client.DEFAULT_CASCADE

    # Create new job
    new_job_id = str(uuid.uuid4())
    store.create_enrichment_job(
        job_id=new_job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=original_job['filename'],
        domain_col=original_job['domain_col'],
        original_filename=original_job.get('original_filename', ''),
        parent_job_id=job_id,  # Track original job for restart chain
        name_col=original_job.get('name_col'),
        first_name_col=original_job.get('first_name_col'),
        last_name_col=original_job.get('last_name_col'),
        cascade_config=cascade_json,
        max_results=original_job.get('max_results', 5),
    )

    # Set up signals and background task
    _job_signals[new_job_id] = asyncio.Event()
    _active_jobs.add(new_job_id)

    background_tasks.add_task(
        _run_job,
        job_id=new_job_id,
        rows=rows,
        domain_col=original_job['domain_col'],
        name_col=original_job.get('name_col'),
        first_name_col=original_job.get('first_name_col'),
        last_name_col=original_job.get('last_name_col'),
        cascade=cascade,
        max_results=original_job.get('max_results', 5),
        write_incremental=True,
    )

    logger.info("Restarted enrichment job %s as new job %s", job_id, new_job_id)

    return {
        "job_id": new_job_id,
        "total": len(rows),
        "restarted_from": job_id,
    }


@router.post("/jobs/{job_id}/cancel")
async def cancel_enrichment_job(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Cancel a running or queued enrichment job.

    Marks the job as cancelled and removes it from active processing.
    Background task will check cancellation status and stop processing.
    """
    store = job_store.get_store()
    job = store.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("job_type") != "enrichment":
        raise HTTPException(status_code=400, detail="Only enrichment jobs can be cancelled")

    if not _owns_job(job, current_user):
        raise HTTPException(status_code=403, detail="Access denied")

    if job["status"] not in ("queued", "running"):
        raise HTTPException(status_code=400, detail="Only running or queued jobs can be cancelled")

    # Mark job as cancelled in database
    store.set_failed(job_id, "Job cancelled by user")

    # Add to cancelled set so background task knows to stop
    _cancelled_jobs.add(job_id)

    # Remove from active jobs set
    _active_jobs.discard(job_id)

    # Wake up any SSE listeners
    sig = _job_signals.pop(job_id, None)
    if sig:
        sig.set()

    logger.info("Enrichment job %s cancelled by user %s", job_id, current_user["user_id"])

    return {
        "job_id": job_id,
        "status": "cancelled",
        "message": "Job cancelled successfully"
    }


def cleanup_stale_jobs() -> None:
    """Mark jobs as failed if they were running when server restarted."""
    store = job_store.get_store()
    stale = store.get_stale_running_jobs()
    for job_id in stale:
        store.set_failed(job_id, "Server restarted while job was in progress.")
        logger.warning("Marked stale enrichment job %s as failed on startup", job_id)


# =============================================================================
# List Building Tool - New Endpoints for Flow 1, 2, 3
# =============================================================================

# --- Request Models ---

class CompanySearchRequest(BaseModel):
    """Request model for company search (Flow 2)."""
    name: Optional[str] = None
    industry: Optional[list[str]] = None
    employee_range: Optional[list[str]] = None
    company_type: Optional[list[str]] = None
    country_code: Optional[str] = None
    limit: int = 100
    offset: int = 0


class SearchAndEnrichRequest(BaseModel):
    """Request model for search + enrich (Flow 2)."""
    name: Optional[str] = None
    industry: Optional[list[str]] = None
    employee_range: Optional[list[str]] = None
    company_type: Optional[list[str]] = None
    country_code: Optional[str] = None
    max_decision_makers: int = 5
    include_generic_emails: bool = True


class LinkedInEnrichRequest(BaseModel):
    """Request model for LinkedIn enrichment (Flow 3)."""
    upload_id: str
    linkedin_col: str
    include_company: bool = True


# --- Flow 1: Domain Enrichment (Extended) ---

@router.post("/by-domains")
async def enrich_by_domains(
    req: StartJobRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 1: Domain → Generic Emails + Decision Makers

    Upload a CSV with domains and get:
    - Generic emails per domain
    - Up to 5 decision makers per company

    This extends the existing enrichment endpoint with additional options.
    """
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    if req.domain_col not in df.columns:
        raise HTTPException(
            status_code=400, detail=f"Column '{req.domain_col}' not found in CSV."
        )

    rows = df.fillna("").astype(str).to_dict(orient="records")
    cascade = req.cascade if req.cascade else blitz_client.DEFAULT_CASCADE

    # Read metadata
    metadata_path = UPLOAD_DIR / f"{req.upload_id}.metadata.json"
    original_filename = ""
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            original_filename = metadata.get("original_filename", "")
        except Exception:
            pass

    job_id = str(uuid.uuid4())
    store = job_store.get_store()
    cascade_json = json.dumps(cascade) if cascade else None

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=str(req.upload_id),
        domain_col=req.domain_col,
        original_filename=original_filename,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade_config=cascade_json,
        max_results=req.max_results,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_job,
        job_id=job_id,
        rows=rows,
        domain_col=req.domain_col,
        name_col=req.name_col,
        first_name_col=req.first_name_col,
        last_name_col=req.last_name_col,
        cascade=cascade,
        max_results=req.max_results,
        write_incremental=True,
    )

    return {"job_id": job_id, "total": len(rows), "flow": "domain_enrichment"}


# --- Flow 2: Company Search & Enrich ---

@router.post("/search/companies")
async def search_companies(
    req: CompanySearchRequest,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 2a: Search companies by criteria

    Search Blitz API for companies matching the given criteria.
    Returns list of companies with their LinkedIn URLs and details.
    """
    async with httpx.AsyncClient() as client:
        try:
            result = await blitz_client.company_search(
                client,
                name=req.name,
                industry=req.industry,
                employee_range=req.employee_range,
                company_type=req.company_type,
                country_code=req.country_code,
                limit=req.limit,
                offset=req.offset,
            )
            return {
                "count": result.get("count", 0),
                "total": result.get("total", 0),
                "results": result.get("results", []),
                "flow": "company_search",
            }
        except Exception as e:
            logger.error("Company search failed: %s", e)
            raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.post("/search/companies/enrich")
async def search_and_enrich(
    req: SearchAndEnrichRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 2b: Search companies + Enrich

    1. Search for companies matching criteria
    2. Enrich each company with decision makers and emails

    Returns a job_id for tracking the enrichment process.
    """
    # First, search for companies
    async with httpx.AsyncClient() as client:
        try:
            search_result = await blitz_client.company_search(
                client,
                name=req.name,
                industry=req.industry,
                employee_range=req.employee_range,
                company_type=req.company_type,
                country_code=req.country_code,
                limit=500,  # Max companies to enrich
                offset=0,
            )
        except Exception as e:
            logger.error("Company search failed: %s", e)
            raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

    companies = search_result.get("results", [])
    if not companies:
        raise HTTPException(status_code=400, detail="No companies found matching criteria.")

    # Create a job to track this enrichment
    job_id = str(uuid.uuid4())
    store = job_store.get_store()

    # Extract domains from companies (if available)
    domain_col = "domain"
    rows = []
    for company in companies:
        # Try to extract domain from LinkedIn URL or name
        domain = company.get("domain", "")
        if not domain and company.get("linkedin_url"):
            # Extract company name from LinkedIn URL as fallback
            name = company.get("name", "")
            if name:
                # Use lowercase, remove spaces as domain guess
                domain = name.lower().replace(" ", "") + ".com"
        if domain:
            row = {domain_col: domain}
            # Include original company data
            row["company_name"] = company.get("name", "")
            row["company_linkedin_url"] = company.get("linkedin_url", "")
            row["company_industry"] = company.get("industry", "")
            row["company_employee_count"] = str(company.get("employee_count", ""))
            rows.append(row)

    if not rows:
        raise HTTPException(status_code=400, detail="Could not extract domains from companies.")

    # Create enrichment job
    cascade_json = json.dumps(blitz_client.DEFAULT_CASCADE)

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=f"search_enrich_{job_id[:8]}",
        domain_col=domain_col,
        original_filename=f"search_enrich_{job_id[:8]}.csv",
        cascade_config=cascade_json,
        max_results=req.max_decision_makers,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_job,
        job_id=job_id,
        rows=rows,
        domain_col=domain_col,
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        cascade=blitz_client.DEFAULT_CASCADE,
        max_results=req.max_decision_makers,
        write_incremental=True,
    )

    return {
        "job_id": job_id,
        "total": len(rows),
        "companies_found": len(companies),
        "flow": "search_and_enrich",
    }


# --- Flow 3: LinkedIn Enrichment ---

@router.post("/by-linkedin")
async def enrich_by_linkedin(
    req: LinkedInEnrichRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Flow 3: LinkedIn URLs → Full Enrichment

    Upload a CSV with LinkedIn URLs and get fully enriched data:
    - Person details (name, title, company)
    - Work email
    - Phone (if available)
    - Company details
    """
    upload_path = UPLOAD_DIR / f"{req.upload_id}.csv"
    if not upload_path.exists():
        raise HTTPException(status_code=404, detail="Upload not found.")

    df = pd.read_csv(str(upload_path), skipinitialspace=True)
    if req.linkedin_col not in df.columns:
        raise HTTPException(
            status_code=400, detail=f"Column '{req.linkedin_col}' not found in CSV."
        )

    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Read metadata
    metadata_path = UPLOAD_DIR / f"{req.upload_id}.metadata.json"
    original_filename = ""
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text())
            original_filename = metadata.get("original_filename", "")
        except Exception:
            pass

    job_id = str(uuid.uuid4())
    store = job_store.get_store()

    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=len(rows),
        filename=str(req.upload_id),
        domain_col=req.linkedin_col,  # Using domain_col to store linkedin_col reference
        original_filename=original_filename,
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        cascade_config=None,
        max_results=1,
    )

    _job_signals[job_id] = asyncio.Event()
    _active_jobs.add(job_id)

    background_tasks.add_task(
        _run_linkedin_job,
        job_id=job_id,
        rows=rows,
        linkedin_col=req.linkedin_col,
        include_company=req.include_company,
    )

    return {"job_id": job_id, "total": len(rows), "flow": "linkedin_enrichment"}


async def _run_linkedin_job(
    job_id: str,
    rows: list[dict[str, Any]],
    linkedin_col: str,
    include_company: bool = True,
):
    """Background task to run LinkedIn enrichment job."""
    store = job_store.get_store()
    store.set_running(job_id)
    seq = [0]

    output_path = OUTPUT_DIR / f"{job_id}.csv"

    async def on_progress(e: dict[str, Any]):
        progress_store = job_store.get_store()
        progress_store.append_event(job_id, seq[0], e)
        seq[0] += 1
        sig = _job_signals.get(job_id)
        if sig:
            sig.set()
            sig.clear()

    try:
        output_rows = await list_builder.run_linkedin_enrichment(
            rows=rows,
            linkedin_col=linkedin_col,
            on_progress=on_progress,
        )

        if output_rows:
            out_df = pd.DataFrame(output_rows)
            out_df.to_csv(str(output_path), index=False)
        else:
            output_path.write_text("")

        store.set_done(job_id, str(output_path))
        logger.info("LinkedIn enrichment job %s completed, %d output rows", job_id, len(output_rows))

    except Exception as e:
        logger.exception("LinkedIn enrichment job %s failed: %s", job_id, e)
        if output_path.exists() and output_path.stat().st_size > 0:
            store.set_done(job_id, str(output_path))
        else:
            store.set_failed(job_id, str(e))

    finally:
        _active_jobs.discard(job_id)
        sig = _job_signals.pop(job_id, None)
        if sig:
            sig.set()


# --- Search Filter Options Endpoint ---

@router.get("/search/options")
async def get_search_options(_current_user: dict = Depends(auth.get_current_user)):
    """
    Get available search filter options for Flow 2.

    Returns normalized values for:
    - Industries
    - Employee ranges
    - Company types
    - Countries
    - Job levels
    - Job functions
    - Sales regions
    """
    return {
        "industries": [
            "Accounting", "Airlines and Aviation", "Animation", "Apparel and Fashion",
            "Architecture and Planning", "Automotive", "Banking", "Biotechnology",
            "Broadcast Media", "Computer Software", "Construction", "Defense and Space",
            "E-Learning", "Education Management", "Electrical/Electronic Manufacturing",
            "Entertainment", "Financial Services", "Food and Beverages",
            "Government Administration", "Health, Wellness and Fitness",
            "Hospital and Health Care", "Hospitality", "Information Technology and Services",
            "Insurance", "Internet", "Legal Services", "Logistics and Supply Chain",
            "Marketing and Advertising", "Mechanical or Industrial Engineering",
            "Medical Devices", "Music", "Non-Profit Organization Management",
            "Oil and Energy", "Pharmaceuticals", "Professional Training and Coaching",
            "Real Estate", "Restaurants", "Retail", "Security and Investigations",
            "Sports", "Staffing and Recruiting", "Telecommunications",
            "Venture Capital and Private Equity",
        ],
        "employee_ranges": [
            "1-10", "11-50", "51-200", "201-500",
            "501-1000", "1001-5000", "5001-10000", "10001+"
        ],
        "company_types": [
            "Educational", "Government Agency", "Nonprofit", "Partnership",
            "Privately Held", "Public Company", "Self-Employed"
        ],
        "countries": [
            {"code": "US", "name": "United States"},
            {"code": "GB", "name": "United Kingdom"},
            {"code": "CA", "name": "Canada"},
            {"code": "DE", "name": "Germany"},
            {"code": "FR", "name": "France"},
            {"code": "AU", "name": "Australia"},
            {"code": "NL", "name": "Netherlands"},
            {"code": "IN", "name": "India"},
            {"code": "JP", "name": "Japan"},
            {"code": "BR", "name": "Brazil"},
            {"code": "SG", "name": "Singapore"},
            {"code": "SE", "name": "Sweden"},
        ],
        "job_levels": [
            "C-Team", "VP", "Director", "Manager", "Staff", "Other"
        ],
        "job_functions": [
            "Advertising & Marketing", "Art, Culture and Creative Professionals",
            "Construction", "Customer/Client Service", "Education", "Engineering",
            "Finance & Accounting", "General Business & Management",
            "Healthcare & Human Services", "Human Resources", "Information Technology",
            "Legal", "Manufacturing & Production", "Operations",
            "Public Administration & Safety", "Purchasing", "Research & Development",
            "Sales & Business Development", "Science", "Supply Chain & Logistics",
            "Writing/Editing"
        ],
        "sales_regions": [
            {"code": "NORAM", "name": "North America"},
            {"code": "LATAM", "name": "Latin America"},
            {"code": "EMEA", "name": "Europe, Middle East, Africa"},
            {"code": "APAC", "name": "Asia-Pacific"},
        ],
    }
