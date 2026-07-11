"""
Unified Lead Generation Platform - Main FastAPI Application

Combines Google Maps Scraper and Domain Enrichment tools in one application
with a shared authentication system and unified job management.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import sqlite3
from dotenv import load_dotenv
from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from pydantic import BaseModel

from shared import auth, db
from shared.job_store_base import JobStoreBase
from scraper import routes as scraper_routes
from enrichment import routes as enrichment_routes
from enrichment import blitz_client
from enrichment import call_tracker
from enrichment import identifier_utils
from enrichment import job_store
from phone_enrichment import routes as phone_enrichment_routes

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
OUTPUT_DIR = DATA_DIR / "outputs"
DATA_DIR.mkdir(exist_ok=True)
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# CORS configuration
_raw_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173,http://localhost:3000")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()]

# Create FastAPI app
app = FastAPI(title="Lead Generation Platform")


@app.exception_handler(sqlite3.OperationalError)
async def sqlite_locked_handler(_request: Request, exc: sqlite3.OperationalError):
    """Return a clean 503 (with Retry-After) when the SQLite database is locked.

    The download UI surfaces this as a recoverable, retryable error rather
    than the cryptic 500 the user was previously seeing.
    """
    msg = str(exc).lower()
    if "locked" in msg or "busy" in msg:
        logger.warning("SQLite lock contention, returning 503: %s", exc)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "The platform database is briefly busy. Please retry in a few seconds.",
                "retry_after": 3,
            },
            headers={"Retry-After": "3"},
        )
    logger.exception("SQLite OperationalError: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc) or "Internal server error"},
    )


@app.exception_handler(Exception)
async def global_exception_handler(_request: Request, exc: Exception):
    """Ensure all unhandled exceptions return JSON, not plain text 'Internal Server Error'."""
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc) or "Internal server error"},
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include module routers
app.include_router(scraper_routes.router, tags=["scraper"])
app.include_router(enrichment_routes.router, tags=["enrichment"])
app.include_router(phone_enrichment_routes.router, tags=["phone_enrichment"])

# Create a shared router for common endpoints
shared_router = APIRouter(prefix="/api", tags=["shared"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    email: str
    password: str


class ChainJobRequest(BaseModel):
    """Request to chain enrichment from a scraper job."""
    cascade: Optional[list[dict[str, Any]]] = None
    max_results: int = 5
    # Optional list of provider names to use (e.g. ["contacts_db", "blitz", "smartprospect", "wizleads", "better_enrich"]).
    # If None, uses all enabled providers from enrichment/providers.py.
    providers: Optional[list[str]] = None
    # Pre-processing flags. Both default ON to preserve existing behavior.
    # normalize_domains=True: identifier_utils.normalize_domain() is applied
    # per row. False: the raw value flows to the provider.
    # dedupe_by_domain=True: input rows are collapsed by the domain column
    # before enrichment. False: every row is enriched.
    normalize_domains: bool = True
    dedupe_by_domain: bool = True


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    """Initialize database and clean up stale jobs. users must exist before jobs (FK)."""
    auth.init_auth_db()
    db.init_db()

    # Provider HTTP call tracker: creates schema + installs a global hook on
    # httpx.AsyncClient so every outbound enrichment call is recorded.
    # Best-effort, defensive — never blocks the underlying HTTP request.
    # The purge loop keeps storage bounded; the health loop self-monitors the
    # tracker itself so we detect any silent outage within ~1 hour.
    try:
        call_tracker.init()
        asyncio.create_task(_call_tracker_purge_loop())
        asyncio.create_task(_call_tracker_health_loop())
        logger.info("Started provider call tracker")
    except Exception as e:
        logger.warning("Failed to start call tracker: %s", e)

    # Restore in-memory job state from database (survives worker restarts)
    try:
        state = enrichment_routes.job_store.get_store().restore_job_state()
        enrichment_routes._cancelled_jobs.update(state.get("cancelled", set()))
        enrichment_routes._active_jobs.update(state.get("active", set()))
        logger.info("Restored job state: %d cancelled, %d active",
                    len(state.get("cancelled", set())), len(state.get("active", set())))
    except Exception as e:
        logger.warning("Failed to restore job state: %s", e)

    # Clean up stale scraper jobs
    scraper_routes.cleanup_stale_jobs()
    # Clean up stale enrichment jobs
    enrichment_routes.cleanup_stale_jobs()
    # Clean up old uploads (7 days) and outputs (30 days)
    enrichment_routes.cleanup_old_files()

    # Start the Contacts DB outbox retry background task. Drains any
    # contacts_write_outbox entries that hit transient failures (network,
    # rate limits, 5xx) and re-attempts the upsert. Permanent errors
    # (400, 404, 422) are marked failed and excluded from future retries.
    try:
        from enrichment.contacts_writer import retry_outbox_loop
        asyncio.create_task(retry_outbox_loop(interval_seconds=60))
        logger.info("Started contacts_write_outbox retry loop")
    except Exception as e:
        logger.warning("Failed to start outbox retry loop: %s", e)


async def _call_tracker_purge_loop() -> None:
    """Daily purge of provider_call_log rows older than 30 days.

    Best-effort: any failure is logged and the loop continues. Runs every
    24h, so steady-state disk usage stays bounded regardless of uptime.
    """
    while True:
        await asyncio.sleep(86400)  # 24h
        try:
            call_tracker.purge_old(days=30)
        except Exception as e:
            logger.warning("call_tracker purge failed: %s", e)


async def _call_tracker_health_loop() -> None:
    """Hourly self-check of the provider call tracker.

    Integrated into the app — runs as a background asyncio task started from
    startup(), dies with the process, restarts on next service start. Emits an
    INFO log line every hour with tracker stats, and a WARNING if structural
    issues are detected (hook not installed, table missing, query error).

    A row count of zero is NOT flagged as a warning — it can simply mean no
    provider HTTP traffic in the window. Operator watches the INFO trend.
    """
    # First check after 60s (give the app time to settle and traffic to start),
    # then every hour thereafter.
    await asyncio.sleep(60)
    while True:
        try:
            h = call_tracker.health_check()
            if not h.get("installed"):
                logger.warning(
                    "call_tracker NOT INSTALLED — hook missing, provider calls not being recorded"
                )
            elif not h.get("call_log_table_exists"):
                logger.warning(
                    "call_tracker table missing — schema init failed, no rows being recorded"
                )
            elif h.get("error"):
                logger.warning("call_tracker health_check error: %s", h["error"])
            else:
                logger.info(
                    "call_tracker healthy: call_log total=%d 1h=%d 24h=%d newest=%s | "
                    "email_ledger total=%d 1h=%d 24h=%d newest=%s",
                    h["call_log_total"], h["call_log_last_hour"], h["call_log_last_day"],
                    h["call_log_newest"],
                    h["email_ledger_total"], h["email_ledger_last_hour"],
                    h["email_ledger_last_day"], h["email_ledger_newest"],
                )
        except Exception as e:
            logger.warning("call_tracker health loop exception: %s", e)
        await asyncio.sleep(3600)  # 1h


# ---------------------------------------------------------------------------
# Shared Auth Routes
# ---------------------------------------------------------------------------

@shared_router.get("/health")
async def health():
    return {"status": "ok"}


@shared_router.post("/auth/login")
async def login(req: LoginRequest):
    """Authenticate with email + password. Returns JWT token on success."""
    email = req.email.strip().lower()
    user = auth.get_user_by_email(email)
    if user is None or not auth.verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    token = auth.create_token(user)
    return {
        "token": token,
        "user_id": user["user_id"],
        "email": user["email"],
        "is_admin": bool(user["is_admin"]),
    }


@shared_router.get("/auth/me")
async def me(current_user: dict = Depends(auth.get_current_user_with_api_key)):
    """Return the currently authenticated user's profile. Supports both JWT and API key authentication."""
    return {
        "user_id": current_user["user_id"],
        "email": current_user["email"],
        "is_admin": current_user.get("is_admin", False),
    }


@shared_router.post("/auth/refresh")
async def refresh_token(current_user: dict = Depends(auth.get_current_user)):
    """
    Refresh JWT token (must be authenticated with valid token).
    Returns a new token with updated expiration.
    """
    new_token = auth.create_token(current_user)
    return {
        "token": new_token,
        "user_id": current_user["user_id"],
        "email": current_user["email"],
        "is_admin": bool(current_user.get("is_admin", False)),
    }


# ---------------------------------------------------------------------------
# API Key Routes
# ---------------------------------------------------------------------------

class CreateApiKeyRequest(BaseModel):
    name: str


@shared_router.get("/api-keys")
async def list_api_keys(
    current_user: dict = Depends(auth.get_current_user),
    include_key: bool = False,
):
    """List all API keys for the current user. Set include_key=true to get plaintext keys."""
    keys = auth.get_api_keys(current_user["user_id"], include_key=include_key)
    return {"api_keys": keys}


@shared_router.post("/api-keys")
async def create_api_key(
    req: CreateApiKeyRequest,
    current_user: dict = Depends(auth.get_current_user),
):
    """Create a new API key. The key is shown only once - make sure to save it!"""
    result = auth.create_api_key(current_user["user_id"], req.name)
    return result


@shared_router.delete("/api-keys/{key_id}")
async def delete_api_key(
    key_id: str,
    current_user: dict = Depends(auth.get_current_user),
):
    """Delete (revoke) an API key."""
    deleted = auth.delete_api_key(key_id, current_user["user_id"])
    if not deleted:
        raise HTTPException(status_code=404, detail="API key not found.")
    return {"message": "API key deleted successfully."}


@shared_router.get("/quota")
async def get_api_quota(current_user: dict = Depends(auth.get_current_user)):
    """
    Return the current user's API quota status.
    Admins have unlimited quota (limit=None).
    Non-admins have 50,000 requests per day.
    """
    is_admin = current_user.get("is_admin", False)
    status = db.get_api_quota_status(current_user["user_id"], is_admin)
    return status


# ---------------------------------------------------------------------------
# Combined Jobs Routes
# ---------------------------------------------------------------------------

@shared_router.get("/jobs")
async def list_jobs(
    job_type: Optional[str] = None,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    List jobs with optional type filter.
    job_type can be 'scraper', 'enrichment', or None for all.
    """
    from shared.job_store_base import JobStoreBase

    store = JobStoreBase(db.get_db())

    if current_user.get("is_admin"):
        jobs = store.list_jobs(job_type=job_type, limit=200)
    else:
        jobs = store.list_jobs(user_id=current_user["user_id"], job_type=job_type, limit=200)

    # Enhance enrichment jobs with user-friendly filenames
    for job in jobs:
        if job.get("job_type") == "enrichment":
            if job.get("original_filename"):
                job["filename"] = job["original_filename"]
                job["display_filename"] = job["original_filename"]
            else:
                # Fallback for old jobs
                filename = job.get("filename", "")
                if filename and len(filename) == 36 and filename.count('-') == 4:
                    friendly_name = f"uploaded_file_{job['job_id'][:8]}.csv"
                    job["filename"] = friendly_name
                    job["display_filename"] = friendly_name
                elif filename:
                    job["display_filename"] = f"{filename}.csv" if not filename.endswith('.csv') else filename
                else:
                    job["filename"] = "Unknown.csv"
                    job["display_filename"] = "Unknown.csv"

    return {"jobs": jobs}


@shared_router.get("/jobs/{job_id}")
async def get_job(job_id: str, current_user: dict = Depends(auth.get_current_user)):
    """Get any job by ID (scraper or enrichment)."""
    from shared.job_store_base import JobStoreBase

    store = JobStoreBase(db.get_db())
    job_data = store.get_job(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found.")

    # Check ownership
    if not current_user.get("is_admin") and job_data.get("user_id") != current_user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    # Include child jobs if this is a scraper job
    if job_data.get("job_type") == "scraper":
        child_jobs = store.get_child_jobs(job_id)
        job_data["child_jobs"] = child_jobs

    # Enhance enrichment jobs with user-friendly filenames
    if job_data.get("job_type") == "enrichment":
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


@shared_router.post("/jobs/{scraper_job_id}/chain")
async def chain_to_enrichment(
    scraper_job_id: str,
    req: ChainJobRequest,
    background_tasks: BackgroundTasks,
    current_user: dict = Depends(auth.get_current_user),
):
    """
    Create an enrichment job from a completed scraper job's output.
    The scraper output CSV is used as the upload for enrichment.
    """
    from shared.job_store_base import JobStoreBase

    store = JobStoreBase(db.get_db())
    scraper_job = store.get_job(scraper_job_id)

    if not scraper_job:
        raise HTTPException(status_code=404, detail="Scraper job not found.")
    if scraper_job.get("job_type") != "scraper":
        raise HTTPException(status_code=400, detail="Job is not a scraper job.")
    if scraper_job["status"] != "done":
        raise HTTPException(status_code=400, detail="Scraper job must be complete before chaining.")

    # Check ownership
    if not current_user.get("is_admin") and scraper_job.get("user_id") != current_user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    output_path = scraper_job.get("output_path")
    if not output_path or not Path(output_path).exists():
        raise HTTPException(status_code=404, detail="Scraper output file not found.")

    # Read the scraper output CSV
    try:
        df = pd.read_csv(output_path, skipinitialspace=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read scraper output: {e}")

    # Check for website column
    if "website" not in df.columns:
        raise HTTPException(status_code=400, detail="Scraper output does not contain 'website' column.")

    # Prepare rows for enrichment
    rows = df.fillna("").astype(str).to_dict(orient="records")

    # Filter out rows without websites
    rows_with_domains = [r for r in rows if r.get("website", "").strip()]

    if not rows_with_domains:
        raise HTTPException(status_code=400, detail="No valid website domains found in scraper output.")

    # Pre-processing: dedupe by domain column (default ON). Counts
    # collapsed rows for transparency. Persists skipped raw values for
    # auditability. Runs BEFORE job creation so total reflects unique rows.
    if req.dedupe_by_domain:
        deduped_rows, deduped_count, skipped_domains = identifier_utils.dedupe_rows_by_domain(
            rows_with_domains, "website", req.normalize_domains
        )
    else:
        deduped_rows, deduped_count, skipped_domains = rows_with_domains, 0, []

    cascade = req.cascade if req.cascade else blitz_client.DEFAULT_CASCADE

    # Create enrichment job
    enrichment_job_id = str(uuid.uuid4())

    # Convert cascade to JSON for storage
    cascade_json = json.dumps(cascade) if cascade else None

    # Use the scraper job's search keyword for a user-friendly filename
    query_slug = scraper_job.get("query", "scraper")
    # Sanitize: lowercase, replace special chars with underscore, limit length
    query_slug = re.sub(r'[^a-z0-9]+', '_', query_slug.lower().strip())[:30].rstrip('_')
    filename = f"{query_slug}_{enrichment_job_id[:8]}.csv"

    enrichment_store = job_store.get_store()
    enrichment_store.create_enrichment_job(
        job_id=enrichment_job_id,
        user_id=current_user["user_id"],
        total=len(deduped_rows),
        filename=filename,
        domain_col="website",
        parent_job_id=scraper_job_id,
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        cascade_config=cascade_json,
        max_results=req.max_results,
        normalize_domains=req.normalize_domains,
        dedupe_by_domain=req.dedupe_by_domain,
        deduped_rows=deduped_count,
        dedupe_skipped_domains=json.dumps(skipped_domains),
    )

    # Set up signals and background task
    enrichment_routes._job_signals[enrichment_job_id] = asyncio.Event()
    enrichment_routes._active_jobs.add(enrichment_job_id)

    # Use _run_domain_enrich_job which supports all enabled providers
    # (Contacts DB + Blitz + Better Enrich) instead of _run_job which uses
    # the older pipeline with custom cascade. Pass req.providers if specified,
    # otherwise None to use all enabled providers from enrichment/providers.py.
    # Flags are passed through so the runner can gate normalize_domain() per row.
    background_tasks.add_task(
        enrichment_routes._run_domain_enrich_job,
        job_id=enrichment_job_id,
        rows=deduped_rows,
        domain_col="website",
        name_col=None,
        first_name_col=None,
        last_name_col=None,
        max_results=req.max_results,
        selected_providers=req.providers,  # None = all, or specific list
        normalize_domains=req.normalize_domains,
    )

    return {
        "job_id": enrichment_job_id,
        "total": len(deduped_rows),
        "parent_job_id": scraper_job_id,
        "deduped_count": deduped_count,
    }


# ---------------------------------------------------------------------------
# Admin Routes
# ---------------------------------------------------------------------------

admin_router = APIRouter(prefix="/api/admin", tags=["admin"])


@admin_router.get("/jobs")
async def admin_list_all_jobs(
    limit: int = 100,
    offset: int = 0,
    _admin: dict = Depends(auth.require_admin),
):
    """Admin-only: list all jobs across both types with user email attached."""
    from shared.job_store_base import JobStoreBase

    store = JobStoreBase(db.get_db())
    jobs = store.list_all_jobs_with_user(limit=limit, offset=offset)
    return {"jobs": jobs, "limit": limit, "offset": offset}


@admin_router.get("/users")
async def admin_list_users(_admin: dict = Depends(auth.require_admin)):
    """Admin-only: list all registered users."""
    users = auth.list_users()
    return {"users": users}


# Include shared and admin routers
app.include_router(shared_router)
app.include_router(admin_router)
