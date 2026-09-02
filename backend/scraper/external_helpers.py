"""Shared implementation for the external scraper API surface.

Single code path used by BOTH the HTTP routes (scraper/external_routes.py,
``/api/external/scraper/*``) and the MCP action tools
(mcp_oracle/scraper_actions.py). Everything here reuses existing internals —
center resolution, task estimation, quota gates, the job store, the cache
signature formula, and the CSV row counter — so external callers get exactly
the behavior (and guardrails) of the UI pipeline.

All functions are synchronous: FastAPI runs ``def`` endpoints in its
threadpool, and the async MCP tools off-load these via
``anyio.to_thread.run_sync`` — blocking SQLite/CSV work never touches the
event loop (same rationale as the 2026-08-24 MCPAuthMiddleware off-loop fix).

Design notes (load-bearing):
- ``peek_cache`` is a READ-ONLY variant of ``cache.check_cache``: identical
  SELECT + cache_id formula, but no ``last_accessed_at``/``access_count``
  mutation and no ``cache_stats`` write. Estimates and cache previews must
  not distort analytics.
- Task counts are ALWAYS centers × 3 zooms ([10,11,12]) — this matches what
  ``POST /api/scraper/jobs`` stores and what the quota pre-check bills. The
  legacy ``/api/scraper/regions/estimate`` reports ×1 for zips mode; that
  inconsistency is deliberately NOT replicated here.
- ``center_ids`` order is preserved everywhere: it feeds
  ``generate_region_signature`` which is order-sensitive — reordering would
  silently change the cache identity.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field

from shared import db
from . import cache as cache_module
from . import centers as centers_module
from . import job_store
from .crawler import OUTPUT_COLS
from .routes import OUTPUT_DIR, _count_csv_data_rows, _owns_job

# Mirror of routes.py's download-gate set — statuses at which a job's results
# may be served. 'stopped' is legacy but still accepted by the existing
# download endpoint, so it is accepted here too.
TERMINAL_STATUSES = ("done", "failed", "abandoned", "cancelled", "stopped")
CANCELLABLE_STATUSES = ("queued", "running")
DEFAULT_ZOOMS = [10, 11, 12]

MAX_TASK_LIMIT_DEFAULT = 15_000
MAX_JOB_LIST_LIMIT = 200
MAX_RESULT_PAGE_LIMIT = 1_000
MAX_CACHE_PAGE_LIMIT = 1_000

RESULT_FIELDS: tuple[str, ...] = tuple(OUTPUT_COLS)

# Compact projection for MCP consumers (and any client that passes
# fields=compact). Small enough for an LLM context, big enough to act on.
COMPACT_FIELDS: tuple[str, ...] = (
    "name", "category_name", "full_address", "city", "city_state", "phone",
    "website", "rating", "review_count", "place_link", "place_id",
    "latitude", "longitude",
)


# ---------------------------------------------------------------------------
# Errors & response envelope
# ---------------------------------------------------------------------------

class ExternalError(Exception):
    """Structured error surfaced through the /api/external error envelope.

    ``code`` is a stable machine-readable token (see external_routes docs);
    ``retry_after`` (seconds) becomes a Retry-After header when set.
    """

    def __init__(
        self,
        code: str,
        message: str,
        status_code: int,
        retry_after: Optional[int] = None,
        **extra: Any,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retry_after = retry_after
        self.extra = extra


def to_http_exception(exc: ExternalError) -> HTTPException:
    """Convert ExternalError → HTTPException with a structured detail dict."""
    detail: dict[str, Any] = {"code": exc.code, "message": exc.message}
    if exc.retry_after is not None:
        detail["retry_after"] = exc.retry_after
    detail.update(exc.extra)
    headers = {}
    if exc.retry_after is not None:
        headers["Retry-After"] = str(exc.retry_after)
    return HTTPException(status_code=exc.status_code, detail=detail, headers=headers or None)


def envelope(
    data: Any,
    *,
    total: Optional[int] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> dict[str, Any]:
    """Uniform ApiResponse-style envelope: {success, data, error, meta}."""
    meta = None
    if total is not None or limit is not None or offset is not None:
        meta = {"total": total, "limit": limit, "offset": offset}
    return {"success": True, "data": data, "error": None, "meta": meta}


# ---------------------------------------------------------------------------
# Request models (bounded — callers cannot mint 5000-center jobs silently)
# ---------------------------------------------------------------------------

class ExternalScrapeRequest(BaseModel):
    query: str = Field(min_length=1, max_length=200)
    mode: str = "all"  # "all" | "states" | "cities" | "zips" | "centers"
    country: str = Field(default="us", max_length=2)
    states: list[str] = Field(default_factory=list, max_length=60)
    cities: list[str] = Field(default_factory=list, max_length=1_000)
    zips: list[str] = Field(default_factory=list, max_length=5_000)
    center_ids: list[str] = Field(default_factory=list, max_length=5_000)
    expected_types: list[str] = Field(default_factory=list, max_length=20)


class CreateJobRequest(ExternalScrapeRequest):
    prefer_cache: bool = True


def request_regions(req: ExternalScrapeRequest) -> dict[str, Any]:
    """Regions dict for cache signatures — byte-compatible with the UI path.

    Mirrors routes.py's ``start_job``/``check_scraper_cache`` payloads (mode,
    country, states, cities, zips, center_ids — center_ids order preserved).
    ``expected_types`` is intentionally absent: it has its own signature and
    must not leak into the region signature (2026-07-17 regression guard).
    """
    return {
        "mode": req.mode,
        "country": req.country,
        "states": req.states,
        "cities": req.cities,
        "zips": req.zips,
        "center_ids": req.center_ids,
    }


# ---------------------------------------------------------------------------
# Estimate
# ---------------------------------------------------------------------------

def compute_estimate(req: ExternalScrapeRequest) -> dict[str, Any]:
    """Resolve centers and compute the task count (ALWAYS centers × 3).

    Returns {centers, errors, center_count, task_count, task_basis}. Raises
    ExternalError(no_centers) when nothing resolves — matching the UI path's
    400, but with a structured code. Task basis matches what POST
    /api/scraper/jobs stores and what the quota pre-check bills (×3), NOT the
    legacy /regions/estimate zips ×1 quirk.
    """
    if not req.query.strip():
        raise ExternalError("no_query", "Query cannot be empty.", 400)

    try:
        filtered_centers, errors = centers_module.get_centers_for_job(
            mode=req.mode,
            country=req.country,
            states=req.states,
            cities=req.cities,
            zips=req.zips,
            center_ids=req.center_ids,
        )
    except sqlite3.OperationalError:
        raise ExternalError(
            "database_busy",
            "The platform database is briefly busy. Please retry in a few seconds.",
            503,
            retry_after=3,
        )

    if errors and not filtered_centers:
        raise ExternalError("no_centers", "; ".join(errors), 400)
    if not filtered_centers:
        raise ExternalError(
            "no_centers",
            "No geographic centers found for the selected region.",
            400,
        )

    task_count = centers_module.estimate_task_count(filtered_centers)
    return {
        "centers": filtered_centers,
        "errors": errors,
        "center_count": len(filtered_centers),
        "task_count": task_count,
        "task_basis": "centers_x_3_zooms",
    }


# ---------------------------------------------------------------------------
# Cache (read-only)
# ---------------------------------------------------------------------------

def peek_cache(
    query: str,
    regions: dict[str, Any],
    zooms: Optional[list[int]] = None,
    expected_types: Optional[list[str]] = None,
) -> Optional[dict[str, Any]]:
    """Read-only cache lookup — the non-mutating twin of check_cache.

    Identical cache_id formula (region/zoom/types signatures) and identical
    SELECT (active + unexpired), but NO last_accessed_at/access_count UPDATE
    and NO cache_stats write. External estimates and previews must not
    distort cache analytics.

    Also tolerant of regions arriving as a JSON string (same contract as
    check_cache via _normalize_regions).
    """
    conn = db.get_db()
    region_sig = cache_module.generate_region_signature(regions)
    zoom_sig = cache_module.generate_zoom_signature(zooms or DEFAULT_ZOOMS)
    types_sig = cache_module.generate_expected_types_signature(expected_types)
    cache_id = cache_module.generate_cache_id(query, region_sig, zoom_sig, types_sig)

    row = conn.execute(
        """
        SELECT * FROM scraped_cache
        WHERE cache_id = ?
        AND status = 'active'
        AND expires_at > ?
        """,
        (cache_id, datetime.now(timezone.utc).isoformat()),
    ).fetchone()
    return dict(row) if row else None


def cache_metadata(entry: dict[str, Any]) -> dict[str, Any]:
    """Cache entry → the metadata shape routes.py:546-558 returns (no rows)."""
    created = _parse_ts(entry.get("created_at"))
    expires = _parse_ts(entry.get("expires_at"))
    now = datetime.now(timezone.utc)
    days_old = (now - created).days if created else None
    days_remaining = (expires - now).days if expires else None
    return {
        "cached": True,
        "cache_id": entry.get("cache_id"),
        "query": entry.get("query"),
        "total_results": entry.get("total_results"),
        "is_partial": bool(entry.get("is_partial")),
        "percentage_complete": entry.get("percentage_complete", 100.0),
        "created_at": entry.get("created_at"),
        "days_old": days_old,
        "days_remaining": days_remaining,
        "expires_at": entry.get("expires_at"),
        "external_access_count": entry.get("access_count", 1),
    }


def cache_file(entry: dict[str, Any]) -> tuple[Optional[Path], bool]:
    """Resolve the entry's result file → (path, exists_and_nonempty).

    Closes the known gap where check_cache returns a hit whose file was
    deleted — the caller can expose file_available instead of a bare 404.
    """
    raw = entry.get("result_file_path")
    path = Path(raw) if raw else None
    if path is None:
        return None, False
    exists = path.exists() and path.stat().st_size > 0
    return path, exists


def is_full_cache_hit(entry: dict[str, Any]) -> bool:
    """Whether this entry can serve complete data instantly.

    A partial entry (cancelled job) or a missing/empty file cannot serve a
    complete dataset; fall through to a fresh job in that case.
    """
    if not entry or bool(entry.get("is_partial")):
        return False
    pct = entry.get("percentage_complete") or 0
    try:
        if float(pct) < 100.0:
            return False
    except (TypeError, ValueError):
        return False
    _, file_ok = cache_file(entry)
    return file_ok


# ---------------------------------------------------------------------------
# Results reader (CSV → paginated JSON rows)
# ---------------------------------------------------------------------------

def validate_fields(fields: Optional[list[str]]) -> Optional[list[str]]:
    """Validate a field projection against the CSV schema.

    None → None (caller default). The literal "compact" is expanded to
    COMPACT_FIELDS. Unknown names → ExternalError(invalid_fields) listing
    the valid set so callers can self-correct.
    """
    if fields is None:
        return None
    if fields == ["compact"]:
        return list(COMPACT_FIELDS)
    unknown = [f for f in fields if f not in RESULT_FIELDS]
    if unknown:
        raise ExternalError(
            "invalid_fields",
            f"Unknown result field(s): {', '.join(unknown)}.",
            400,
            valid_fields=list(RESULT_FIELDS),
        )
    return list(fields)


def read_csv_rows(
    path: Path,
    *,
    offset: int = 0,
    limit: int = 100,
    fields: Optional[list[str]] = None,
) -> tuple[list[dict[str, Any]], list[str], int]:
    """Stream a window of rows from the results CSV as JSON dicts.

    Skips `offset` data rows then collects up to `limit`, projecting each row
    to `fields` when given. Never loads the whole file. `total` comes from
    the (path,size,mtime_ns)-cached counter — the authoritative count.

    Returns (rows, fields_returned, total).
    """
    total = _count_csv_data_rows(path)
    rows: list[dict[str, Any]] = []
    if total == 0 or limit <= 0:
        return rows, fields or list(RESULT_FIELDS), total

    end = offset + limit
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < offset:
                continue
            if i >= end:
                break
            if fields:
                rows.append({k: row.get(k, "") for k in fields})
            else:
                rows.append(dict(row))
    return rows, fields or list(RESULT_FIELDS), total


# ---------------------------------------------------------------------------
# Guardrails & projection
# ---------------------------------------------------------------------------

def get_max_external_tasks() -> int:
    """MAX_EXTERNAL_SCRAPER_TASKS env (default 15,000). ≤0 disables the cap."""
    try:
        return int(os.getenv("MAX_EXTERNAL_SCRAPER_TASKS", str(MAX_TASK_LIMIT_DEFAULT)))
    except ValueError:
        return MAX_TASK_LIMIT_DEFAULT


def check_task_cap(total_tasks: int, is_admin: bool) -> None:
    """Reject non-admin externals creating jobs above the task cap (422).

    Admins are exempt (they're already quota-exempt). The error carries both
    the limit and the actual count so callers can narrow their geography.
    """
    limit = get_max_external_tasks()
    if limit <= 0 or is_admin:
        return
    if total_tasks > limit:
        raise ExternalError(
            "task_limit_exceeded",
            f"This scrape needs {total_tasks} tasks, above the external-API limit "
            f"of {limit} per job. Narrow the geography (fewer cities/states/zips) "
            "or contact an admin.",
            422,
            limit=limit,
            task_count=total_tasks,
        )


def quota_snapshot(user: dict[str, Any]) -> dict[str, Any]:
    """db.get_api_quota_status + the external task cap, as one block."""
    snapshot = db.get_api_quota_status(
        user_id=user["user_id"], is_admin=bool(user.get("is_admin", False))
    )
    return {**snapshot, "external_task_limit": get_max_external_tasks() or None}


def queue_position(job: dict[str, Any]) -> int:
    """0-based position among queued scraper jobs (by created_at). 0 = next."""
    if job.get("status") != "queued":
        return 0
    conn = db.get_db()
    row = conn.execute(
        """
        SELECT COUNT(*) AS n FROM jobs
        WHERE job_type = 'scraper' AND status = 'queued'
        AND created_at < ?
        """,
        (job.get("created_at"),),
    ).fetchone()
    return int(row["n"]) if row else 0


def suggested_poll_seconds(total_tasks: int) -> int:
    """Polling guidance by job size: 10s small / 30s medium / 60s large."""
    if total_tasks < 500:
        return 10
    if total_tasks < 5_000:
        return 30
    return 60


def job_links(job_id: str) -> dict[str, str]:
    """HAL-style links; `csv` points at the existing API-key-capable download."""
    return {
        "status": f"/api/external/scraper/jobs/{job_id}",
        "results": f"/api/external/scraper/jobs/{job_id}/results",
        "csv": f"/api/scraper/jobs/{job_id}/download",
        "cancel": f"/api/external/scraper/jobs/{job_id}/cancel",
    }


def _parse_regions(raw: Any) -> dict[str, Any]:
    """Job row's regions (JSON string from the DB) → dict; {} on garbage."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def _parse_ts(raw: Any) -> Optional[datetime]:
    if not raw or not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw.replace("+00:00", "")).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def pct_complete(done_tasks: int, total_tasks: int) -> float:
    """Task-based completion %, capped at 100 (resumed jobs can exceed)."""
    if not total_tasks:
        return 0.0
    return round(min(100.0, done_tasks / total_tasks * 100), 1)


def project_job(job: dict[str, Any], *, detail: bool = False) -> dict[str, Any]:
    """Curated external projection of a scraper job row.

    List view: identity + status + counters + file availability + links.
    Detail view adds progress/pct, rows_on_disk, error, queue position,
    timestamps, parsed regions, and poll guidance. NEVER the raw SELECT * —
    internal columns (user_id, output_path, enrichment columns) stay private.
    """
    job_id = job.get("job_id", "")
    total_tasks = int(job.get("total_tasks", 0) or 0)
    done_tasks = int(job.get("done_tasks", 0) or 0)

    out: dict[str, Any] = {
        "job_id": job_id,
        "query": job.get("query"),
        "display_name": job.get("display_name"),
        "status": job.get("status"),
        "total_tasks": total_tasks,
        "done_tasks": done_tasks,
        "result_count": int(job.get("result_count", 0) or 0),
        "file_available": _job_file_available(job),
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "links": job_links(job_id),
    }
    if not detail:
        return out

    csv_path = _resolve_job_csv(job)
    rows_on_disk = _count_csv_data_rows(csv_path) if csv_path else 0
    out.update({
        "progress": {
            "done_tasks": done_tasks,
            "total_tasks": total_tasks,
            "pct_complete": pct_complete(done_tasks, total_tasks),
        },
        "rows_on_disk": rows_on_disk,
        "error": job.get("error"),
        "queue_position": queue_position(job),
        "timestamps": {
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at"),
            "cancelled_at": job.get("cancelled_at"),
        },
        "regions": _parse_regions(job.get("regions")),
        "suggested_poll_seconds": suggested_poll_seconds(total_tasks),
    })
    return out


def _job_file_available(job: dict[str, Any]) -> bool:
    path = _resolve_job_csv(job)
    return bool(path and path.exists() and path.stat().st_size > 0)


def _resolve_job_csv(job: dict[str, Any]) -> Optional[Path]:
    """A job's results CSV: output_path if present, else OUTPUT_DIR/{id}.csv."""
    p = job.get("output_path")
    if p:
        return Path(p)
    jid = job.get("job_id")
    return OUTPUT_DIR / f"{jid}.csv" if jid else None


# ---------------------------------------------------------------------------
# impl_* — the single code path shared by HTTP routes and MCP tools
# ---------------------------------------------------------------------------

def _get_scraper_job_or_404(job_id: str) -> dict[str, Any]:
    """Fetch + type-check, mapping the store's errors to structured ones."""
    try:
        job = job_store.get_store().get_job(job_id)
    except sqlite3.OperationalError:
        raise ExternalError(
            "database_busy", "Database briefly busy. Retry shortly.", 503, retry_after=3
        )
    if not job:
        raise ExternalError("not_found", "Job not found.", 404)
    if job.get("job_type") != "scraper":
        raise ExternalError("not_found", "Scraper job not found.", 404)
    return job


def _require_owner(job: dict[str, Any], user: dict[str, Any]) -> None:
    if not _owns_job(job, user):
        raise ExternalError("access_denied", "Access denied.", 403)


def impl_estimate(user: dict[str, Any], req: ExternalScrapeRequest) -> dict[str, Any]:
    """Dry-run: centers/task count + quota status + read-only cache preview."""
    est = compute_estimate(req)
    entry = peek_cache(req.query, request_regions(req), DEFAULT_ZOOMS, req.expected_types)
    allowed, limit_message = db.check_daily_request_limit(
        user_id=user["user_id"],
        is_admin=bool(user.get("is_admin", False)),
        estimated_requests=est["task_count"],
    )
    return {
        "query": req.query.strip(),
        "country": req.country,
        "mode": req.mode,
        "center_count": est["center_count"],
        "task_count": est["task_count"],
        "task_basis": est["task_basis"],
        "warnings": est["errors"],
        "cache": cache_metadata(entry) if entry else {"cached": False},
        "quota": quota_snapshot(user),
        "can_proceed": bool(allowed),
        "quota_message": None if allowed else limit_message,
    }


def impl_cache_query(
    user: dict[str, Any],
    req: ExternalScrapeRequest,
    *,
    offset: int = 0,
    limit: int = 100,
    fields: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Cache query: full metadata + inline rows on a usable hit.

    A hit whose backing file is gone is still a hit (file_available=false,
    empty rows) — the caller decides whether to re-scrape. A miss returns
    the fresh-run task estimate as a hint.
    """
    entry = peek_cache(req.query, request_regions(req), DEFAULT_ZOOMS, req.expected_types)
    if not entry:
        est = compute_estimate(req)
        return {
            "cached": False,
            "fresh_task_estimate": est["task_count"],
            "hint": "POST /api/external/scraper/jobs to run a fresh scrape.",
        }

    metadata = cache_metadata(entry)
    path, file_ok = cache_file(entry)
    rows: list[dict[str, Any]] = []
    rows_available = 0
    fields_out: list[str] = fields or list(RESULT_FIELDS)
    if file_ok and path is not None:
        rows, fields_out, rows_available = read_csv_rows(
            path, offset=offset, limit=limit, fields=fields
        )
    return {
        **metadata,
        "file_available": file_ok,
        "rows_available": rows_available,
        "fields": fields_out,
        "rows": rows,
    }


def impl_create_job(
    user: dict[str, Any],
    req: CreateJobRequest,
    source: str = "API",
) -> dict[str, Any]:
    """Create a scraper job (or short-circuit on a full cache hit).

    Order of guards mirrors the UI's start_job plus the external cap:
    query/key checks → centers → cache short-circuit (prefer_cache) →
    task cap → daily quota pre-check → insert as 'queued'. Nothing else —
    the dispatcher owns execution; no events or signals are touched.
    """
    if not req.query.strip():
        raise ExternalError("no_query", "Query cannot be empty.", 400)

    est = compute_estimate(req)

    # Cache short-circuit: a full, unexpired hit serves the data with no job,
    # no quota consumption, and no scraper.tech cost. Partial hits / missing
    # files fall through to a fresh job. Deliberately BEFORE the
    # SCRAPER_TECH_KEY check — a cache hit scrapes nothing, so a missing key
    # must not block free data.
    if req.prefer_cache:
        entry = peek_cache(req.query, request_regions(req), DEFAULT_ZOOMS, req.expected_types)
        if entry and is_full_cache_hit(entry):
            path, _ = cache_file(entry)
            rows_available = _count_csv_data_rows(path) if path else 0
            return {
                "created": False,
                "served_from_cache": True,
                "cache": cache_metadata(entry),
                "rows_available": rows_available,
                "quota": quota_snapshot(user),
                "links": {
                    "cache_rows": "/api/external/scraper/cache",
                    "csv": f"/api/scraper/cache/download/{entry.get('cache_id')}",
                },
            }

    # Only a fresh scrape needs the scraper.tech credential (mirrors the UI's
    # start_job 500 path).
    if not os.getenv("SCRAPER_TECH_KEY", ""):
        raise ExternalError(
            "scraper_not_configured",
            "SCRAPER_TECH_KEY is not configured on the server.",
            500,
        )

    check_task_cap(est["task_count"], bool(user.get("is_admin", False)))

    is_admin = bool(user.get("is_admin", False))
    allowed, limit_message = db.check_daily_request_limit(
        user_id=user["user_id"],
        is_admin=is_admin,
        estimated_requests=est["task_count"],
    )
    if not allowed:
        quota = db.get_api_quota_status(user_id=user["user_id"], is_admin=is_admin)
        raise ExternalError(
            "quota_exceeded",
            limit_message,
            429,
            resets_at=quota.get("resets_at"),
        )

    job_id = str(uuid.uuid4())
    regions_payload = {
        **request_regions(req),
        # Persisted inside the regions blob (like start_job) so restart/resume
        # recover the type filter; stripped before any cache signature.
        "expected_types": req.expected_types or [],
    }
    store = job_store.get_store()
    try:
        store.create_scraper_job(
            job_id=job_id,
            user_id=user["user_id"],
            query=req.query.strip(),
            regions=regions_payload,
            total_tasks=est["task_count"],
            display_name=f"[{source}] {req.query.strip()[:60]}",
        )
    except sqlite3.OperationalError:
        raise ExternalError(
            "database_busy", "Database briefly busy. Retry shortly.", 503, retry_after=3
        )

    return {
        "created": True,
        "job_id": job_id,
        "status": "queued",
        "total_tasks": est["task_count"],
        "center_count": est["center_count"],
        "warnings": est["errors"],
        "display_name": f"[{source}] {req.query.strip()[:60]}",
        "quota": quota_snapshot(user),
        "suggested_poll_seconds": suggested_poll_seconds(est["task_count"]),
        "links": job_links(job_id),
    }


def impl_list_jobs(
    user: dict[str, Any],
    *,
    limit: int = 50,
    offset: int = 0,
    status: Optional[str] = None,
) -> dict[str, Any]:
    """List the caller's scraper jobs (admin sees all) with envelope meta."""
    store = job_store.get_store()
    if user.get("is_admin"):
        jobs = store.list_jobs(job_type="scraper", limit=limit, offset=offset, status=status)
    else:
        jobs = store.list_jobs(
            user_id=user["user_id"], job_type="scraper", limit=limit, offset=offset, status=status
        )
    return {
        "jobs": [project_job(j) for j in jobs],
        "total": len(jobs),
    }


def impl_job_status(user: dict[str, Any], job_id: str) -> dict[str, Any]:
    """Detailed status projection for one job."""
    job = _get_scraper_job_or_404(job_id)
    _require_owner(job, user)
    return project_job(job, detail=True)


def impl_job_results(
    user: dict[str, Any],
    job_id: str,
    *,
    offset: int = 0,
    limit: int = 100,
    fields: Optional[list[str]] = None,
) -> dict[str, Any]:
    """JSON rows from a TERMINAL job's CSV. 409 (not_ready) while running."""
    job = _get_scraper_job_or_404(job_id)
    _require_owner(job, user)

    if job.get("status") not in TERMINAL_STATUSES:
        total_tasks = int(job.get("total_tasks", 0) or 0)
        done_tasks = int(job.get("done_tasks", 0) or 0)
        raise ExternalError(
            "not_ready",
            f"Job is {job.get('status')} "
            f"({pct_complete(done_tasks, total_tasks)}% complete). "
            f"Poll GET /api/external/scraper/jobs/{job_id}.",
            409,
            retry_after=suggested_poll_seconds(total_tasks),
            job_status=job.get("status"),
        )

    path = _resolve_job_csv(job)
    if not path or not path.exists() or path.stat().st_size == 0:
        raise ExternalError(
            "results_not_available",
            "Job has no results file on disk.",
            404,
        )

    fields_out = validate_fields(fields)
    rows, returned_fields, total = read_csv_rows(
        path, offset=offset, limit=limit, fields=fields_out
    )
    return {
        "job_id": job_id,
        "status": job.get("status"),
        "total_rows": total,
        "fields": returned_fields,
        "rows": rows,
    }


def impl_cancel_job(user: dict[str, Any], job_id: str) -> dict[str, Any]:
    """Cancel via the shared core extracted from the UI's cancel handler."""
    from .routes import _cancel_scraper_job_core  # local import: P2 adds it

    job = _get_scraper_job_or_404(job_id)
    _require_owner(job, user)
    if job.get("status") not in CANCELLABLE_STATUSES:
        raise ExternalError(
            "not_cancellable",
            f"Only queued or running jobs can be cancelled (status: {job.get('status')}).",
            400,
            job_status=job.get("status"),
        )
    # The core performs its own fetch/ownership/status checks (defense in
    # depth) and maintains the in-process cancel/active sets + SSE wake.
    _cancel_scraper_job_core(job_id, user)
    return {
        "job_id": job_id,
        "status": "cancelled",
        "ok": True,
        "message": "Job cancelled successfully.",
    }


def impl_quota(user: dict[str, Any]) -> dict[str, Any]:
    """Quota snapshot for the external surface."""
    return quota_snapshot(user)
