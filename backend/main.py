"""
Unified Lead Generation Platform - Main FastAPI Application

Combines Google Maps Scraper and Domain Enrichment tools in one application
with a shared authentication system and unified job management.
"""

from __future__ import annotations

import asyncio
import contextlib
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
from fastapi.exceptions import RequestValidationError
from fastapi.exception_handlers import (
    http_exception_handler,
    request_validation_exception_handler,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from shared import auth, db
from shared.job_store_base import JobStoreBase
from shared.mcp_auth import MCPAuthMiddleware
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

# ---------------------------------------------------------------------------
# Exception handlers (plain functions so they can be reused on the MCP sub-app)
# ---------------------------------------------------------------------------

async def sqlite_locked_handler(_request: Request, exc: sqlite3.OperationalError):
    """Return a clean 503 (with Retry-After) when the SQLite database is locked."""
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


async def global_exception_handler(_request: Request, exc: Exception):
    """Ensure all unhandled exceptions return JSON, not plain text 'Internal Server Error'."""
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc) or "Internal server error"},
    )


# ---------------------------------------------------------------------------
# /api/external/* error envelope — path-scoped so every other route keeps the
# default {"detail": ...} body byte-identical. External integrators get a
# structured {success:false, error:{code, message, ...}} contract instead.
# ---------------------------------------------------------------------------

async def external_error_handler(request: Request, exc: "ExternalError"):
    """scraper.external_helpers.ExternalError → envelope (or plain detail
    outside /api/external/*)."""
    if request.url.path.startswith("/api/external/"):
        headers = (
            {"Retry-After": str(exc.retry_after)}
            if exc.retry_after is not None else None
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={"success": False,
                     "error": {"status": exc.status_code, "code": exc.code,
                               "message": exc.message, **exc.extra},
                     "data": None, "meta": None},
            headers=headers,
        )
    # Raised outside the external namespace (shouldn't happen) — degrade to
    # the platform-standard detail shape.
    return JSONResponse(status_code=exc.status_code,
                        content={"detail": exc.message})


async def external_http_exception_handler(request: Request, exc: StarletteHTTPException):
    if request.url.path.startswith("/api/external/"):
        detail = (
            exc.detail
            if isinstance(exc.detail, dict)
            else {"code": "error", "message": str(exc.detail)}
        )
        headers = (
            {"Retry-After": str(detail["retry_after"])}
            if isinstance(detail, dict) and detail.get("retry_after") is not None
            else None
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={"success": False, "error": {"status": exc.status_code, **detail},
                     "data": None, "meta": None},
            headers=headers,
        )
    return await http_exception_handler(request, exc)


async def external_validation_exception_handler(request: Request, exc: RequestValidationError):
    if request.url.path.startswith("/api/external/"):
        return JSONResponse(
            status_code=422,
            content={"success": False,
                     "error": {"status": 422, "code": "validation_error",
                               "message": "Request validation failed.", "errors": exc.errors()},
                     "data": None, "meta": None},
        )
    return await request_validation_exception_handler(request, exc)


# ---------------------------------------------------------------------------
# MCP documentation oracle — atomic setup (Phase 1C)
# ---------------------------------------------------------------------------
# All-or-nothing pattern: if ANY step fails, _MCP_ENABLED stays False and
# /mcp is NOT mounted. This prevents the partial-failure vulnerability
# identified by the Phase 1C red-team review (mounted but unprotected).
# ---------------------------------------------------------------------------

_MCP_ENABLED = False
_mcp_app = None  # type: ignore

if os.environ.get("ENABLE_MCP_ORACLE", "true").lower() == "true":
    try:
        from mcp_oracle import mcp as _mcp_instance

        _mcp_app = _mcp_instance.streamable_http_app()

        # Defense in depth: register the same exception handlers on the
        # sub-app so MCP errors render as JSON (not plaintext 500).
        _mcp_app.add_exception_handler(Exception, global_exception_handler)
        _mcp_app.add_exception_handler(sqlite3.OperationalError, sqlite_locked_handler)

        # Auth middleware on the sub-app itself (stays protected even if
        # the mount path ever changes).
        _mcp_app.add_middleware(MCPAuthMiddleware)

        _MCP_ENABLED = True
        logger.info("MCP oracle configured successfully")
    except Exception as _mcp_exc:
        logger.warning("MCP oracle setup failed, will NOT mount: %s", _mcp_exc)
        _MCP_ENABLED = False
        _mcp_app = None


# ---------------------------------------------------------------------------
# Combined lifespan (Phase 1C): parent startup + MCP session manager
# ---------------------------------------------------------------------------

async def _run_parent_startup():
    """Existing startup logic — extracted verbatim from the old @app.on_event.

    Order matters: auth DB → main DB → call tracker → job state → cleanup → outbox.
    Hard failures (auth/db init, stale cleanup) propagate; soft failures
    (call tracker, job state, outbox) are caught and logged.
    """
    auth.init_auth_db()
    db.init_db()

    # Production-only boot actions (default on; pytest sets it false via
    # conftest). The startup reapers + background loops write to the REAL
    # jobs DB — a TestClient boot inside a pytest run against prod data
    # reaped 6 live scraper jobs on 2026-08-24 (09:37:12) with zero service-
    # log trace. Tests must never mutate production job state.
    reapers_enabled = os.environ.get("ENABLE_STARTUP_REAPERS", "true").lower() in ("1", "true", "yes")

    try:
        call_tracker.init()
        asyncio.create_task(_call_tracker_purge_loop())
        asyncio.create_task(_call_tracker_health_loop())
        logger.info("Started provider call tracker")
    except Exception as e:
        logger.warning("Failed to start call tracker: %s", e)

    try:
        asyncio.create_task(_maybe_prune_job_events())
        logger.info("Started job_events prune (file-gated, retention=%s days)", os.getenv("JOB_EVENTS_RETENTION_DAYS", "7"))
    except Exception as e:
        logger.warning("Failed to start job_events prune: %s", e)

    try:
        state = enrichment_routes.job_store.get_store().restore_job_state()
        enrichment_routes._cancelled_jobs.update(state.get("cancelled", set()))
        enrichment_routes._active_jobs.update(state.get("active", set()))
        logger.info("Restored job state: %d cancelled, %d active",
                    len(state.get("cancelled", set())), len(state.get("active", set())))
    except Exception as e:
        logger.warning("Failed to restore job state: %s", e)

    if not reapers_enabled:
        logger.info("ENABLE_STARTUP_REAPERS=false — skipping reapers, auto-resume, "
                    "dispatcher and outbox loop (test/CI boot)")
    else:
        scraper_routes.cleanup_stale_jobs()
        enrichment_routes.cleanup_stale_jobs()
        phone_enrichment_routes.cleanup_stale_phone_jobs()
        enrichment_routes.cleanup_old_files()

        # Auto-resume: any enrichment job the reaper just marked 'abandoned' is
        # re-queued through the restart path instead of waiting for a manual
        # click. Atomic claim + fresh-death window + restart cap make this safe
        # across the 4 workers. Soft failure only — must never block boot.
        try:
            from shared.auto_resume import maybe_auto_resume_abandoned_jobs
            asyncio.create_task(maybe_auto_resume_abandoned_jobs())
            logger.info("Started auto-resume watcher for abandoned jobs")
        except Exception as e:
            logger.warning("Failed to start auto-resume watcher: %s", e)

        # Scraper dispatcher (2026-08-24): claims queued jobs under a
        # platform-wide concurrency cap and re-launches stale-abandoned ones.
        # P1 (2026-09-02): the dispatcher now lives in the dedicated runner
        # process (runner_main.py / lead-gen-scraper-runner.service) so web
        # worker recycles/murders can never kill an in-flight job. Web
        # workers set ENABLE_SCRAPER_DISPATCHER=false and skip this block.
        # Default (unset) is true — preserves single-process local/dev runs.
        if os.environ.get("ENABLE_SCRAPER_DISPATCHER", "true").lower() not in ("1", "true", "yes"):
            logger.info("ENABLE_SCRAPER_DISPATCHER=false — web worker skips dispatcher (runner process owns it)")
        else:
            try:
                from scraper.dispatch import dispatch_loop, runtime_guard_loop
                from scraper.routes import _launch_claimed_job
                asyncio.create_task(dispatch_loop(_launch_claimed_job))
                asyncio.create_task(runtime_guard_loop())
                logger.info("Started scraper dispatcher + runtime guard")
            except Exception as e:
                logger.warning("Failed to start scraper dispatcher: %s", e)

        try:
            from enrichment.contacts_writer import retry_outbox_loop
            asyncio.create_task(retry_outbox_loop(interval_seconds=60))
            logger.info("Started contacts_write_outbox retry loop")
        except Exception as e:
            logger.warning("Failed to start outbox retry loop: %s", e)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Combined parent + MCP sub-app lifespan.

    Flow:
      1. Run parent startup (DB init, call tracker, job state, cleanup, outbox).
         If this fails, the worker fails to start — matching pre-MCP behavior.
      2. If parent startup succeeded AND _MCP_ENABLED, enter the MCP sub-app's
         lifespan via AsyncExitStack. This starts the StreamableHTTPSessionManager
         task group that MCP requests need.
      3. Yield control to the app.
      4. On shutdown (SIGTERM, SIGINT, exception): AsyncExitStack unwinds in
         reverse order — MCP session manager shuts down first, then we exit.
    """
    # Step 1: parent startup (may raise — propagated to gunicorn, worker restarts)
    await _run_parent_startup()
    logger.info("Parent startup complete")

    # Step 2: MCP lifespan (only if fully configured)
    if _MCP_ENABLED and _mcp_app is not None:
        try:
            async with contextlib.AsyncExitStack() as stack:
                await stack.enter_async_context(
                    _mcp_app.router.lifespan_context(_mcp_app)
                )
                logger.info("MCP session manager started")
                yield
        finally:
            logger.info("Lifespan shutdown complete (MCP session manager stopped)")
    else:
        # MCP not enabled — just yield
        logger.info("MCP not enabled, lifespan yields without MCP")
        yield
        logger.info("Lifespan shutdown complete")


# ---------------------------------------------------------------------------
# Create FastAPI app (with combined lifespan)
# ---------------------------------------------------------------------------

app = FastAPI(title="Lead Generation Platform", lifespan=lifespan)

# Register exception handlers on the parent app
app.add_exception_handler(sqlite3.OperationalError, sqlite_locked_handler)
app.add_exception_handler(Exception, global_exception_handler)
# Path-scoped /api/external/* error envelope (registered AFTER the global
# Exception handler so FastAPI's more-specific StarletteHTTPException /
# RequestValidationError dispatch still reaches these first).
app.add_exception_handler(StarletteHTTPException, external_http_exception_handler)
app.add_exception_handler(RequestValidationError, external_validation_exception_handler)

# ExternalError (raised by scraper/external_helpers.py impl_* functions) →
# envelope. Imported late to avoid circulars at module top.
from scraper.external_helpers import ExternalError  # noqa: E402
app.add_exception_handler(ExternalError, external_error_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Defense in depth: MCPAuthMiddleware on the parent self-scopes to /mcp/*
# (it passes through /api/* untouched). The sub-app ALSO has its own
# instance, so /mcp stays protected even if the mount path changes.
if _MCP_ENABLED:
    app.add_middleware(MCPAuthMiddleware)

# Include module routers
app.include_router(scraper_routes.router, tags=["scraper"])
app.include_router(enrichment_routes.router, tags=["enrichment"])
app.include_router(phone_enrichment_routes.router, tags=["phone_enrichment"])

# External scraper API (/api/external/scraper/*) — additive surface for
# API-key clients. Kill-switch: ENABLE_EXTERNAL_SCRAPER_API=false omits it.
if os.environ.get("ENABLE_EXTERNAL_SCRAPER_API", "true").lower() == "true":
    from scraper import external_routes as scraper_external_routes
    app.include_router(scraper_external_routes.router, tags=["external-scraper"])
    logger.info("External scraper API enabled at /api/external/scraper")

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
# Background task loops (used by lifespan via _run_parent_startup)
# ---------------------------------------------------------------------------


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


async def _maybe_prune_job_events() -> None:
    """Prune job_events at most once per JOB_EVENTS_PRUNE_MIN_INTERVAL (default
    1h), gated by a marker file so the 4 workers don't all prune at once.

    Runs once at startup as a background task — NOT a periodic loop — because
    gunicorn --max-requests recycles workers far faster than any multi-hour
    sleep would complete, so a periodic loop effectively never fires. Running
    at startup (on every boot) is what actually bounds job_events. Batched
    DELETE keeps each lock short; best-effort (failures logged, never raised)."""
    import time
    from pathlib import Path
    days = int(os.getenv("JOB_EVENTS_RETENTION_DAYS", "7"))
    min_interval = int(os.getenv("JOB_EVENTS_PRUNE_MIN_INTERVAL", "3600"))
    marker = Path(__file__).resolve().parent / "data" / ".last_job_events_prune"
    try:
        if marker.exists() and (time.time() - marker.stat().st_mtime) < min_interval:
            return  # another worker pruned recently
    except Exception:
        pass
    try:
        from shared.job_store_base import JobStoreBase
        store = JobStoreBase(db.get_db())
        removed = store.prune_old_job_events(days)
        try:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.touch()
        except Exception:
            pass
        if removed:
            logger.info("job_events prune: removed %d rows (retention=%dd)", removed, days)
    except Exception as e:
        logger.warning("job_events prune failed: %s", e)


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
    return {"status": "ok", "mcp_enabled": _MCP_ENABLED}


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
        source_type="google_maps_chain",
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

# ---------------------------------------------------------------------------
# Mount MCP documentation oracle (Phase 1C)
# ---------------------------------------------------------------------------
# Mounted AFTER all routers so it doesn't shadow any existing routes.
# The MCPAuthMiddleware on the parent self-scopes to /mcp — it does NOT
# enforce on /api/*. _mcp_app already has MCPAuthMiddleware registered
# directly on it (defense in depth).
# ---------------------------------------------------------------------------
if _MCP_ENABLED and _mcp_app is not None:
    try:
        app.mount("/mcp", _mcp_app)
        logger.info("MCP oracle mounted at /mcp")
    except Exception as e:
        logger.error("MCP mount failed, disabling: %s", e)
        _MCP_ENABLED = False
