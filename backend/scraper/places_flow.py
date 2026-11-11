"""Job runner for GetLeads "Instant US Database" pulls (scraper-page source).

A places job is a scraper job (job_type='scraper', source_type='getleads_places')
that never touches the scraper.tech crawler:

- Created DIRECTLY in status='running' (never 'queued') so the scraper
  dispatcher — which atomically claims queued jobs and would try to grid-scrape
  them — can never pick it up. The request handler spawns the driver task,
  exactly like enrichment jobs do.
- Phases (total_tasks=3): 1) fair-use precheck + free count, 2) async export
  on GetLeads (polled), 3) CSV download + normalized write.
- Output CSV is written to OUTPUT_DIR/{job_id}.csv with a literal `website`
  column, so the existing scraper→enrichment chain endpoint
  (POST /api/jobs/{id}/chain) works unchanged.
- Survives restarts: the export_id lives in the job's regions blob; on boot
  rearm_running_places_jobs() re-attaches pollers to running places jobs
  (cross-worker safe via a resume_claimed_at compare-and-set).
- Cancel-aware: the standard scraper cancel endpoint flips the DB status; the
  poll loop notices within one tick and stops (the remote export may still
  complete and bill its rows — GetLeads exports cannot be cancelled).
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from shared import db
from scraper import job_store, places_client
from scraper.places_client import PlacesError

logger = logging.getLogger(__name__)

SOURCE_TYPE = "getleads_places"
PHASE_TOTAL = 3

# OUTPUT_DIR is owned by scraper/routes.py (single source of truth for where
# job CSVs live; the download endpoints fall back to OUTPUT_DIR/{job_id}.csv).
from scraper.routes import OUTPUT_DIR  # noqa: E402

# Normalized output columns. `website` is REQUIRED — the chain-to-enrichment
# endpoint (main.py chain_to_enrichment) gates on a literal "website" column.
CSV_COLUMNS = [
    "name", "category", "phone", "website",
    "email", "email_type", "additional_emails",
    "street_address", "city", "state", "zip", "full_address",
    "rating", "review_count", "google_maps_url",
    "facebook", "instagram", "linkedin",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Job creation
# ---------------------------------------------------------------------------


def _insert_places_job(
    job_id: str,
    user_id: str,
    query_label: str,
    display_name: str,
    regions_payload: dict[str, Any],
    group_name: Optional[str],
) -> None:
    """Insert a places job row directly in status='running'.

    Deliberately bypasses JobStoreBase.create_job, which hard-codes
    status='queued' — a queued places row would be claimed by the scraper
    dispatcher and run as a scraper.tech grid job. Inserting as 'running'
    closes that race by construction. is_resumable=0 keeps the UI from
    offering scraper Resume (places jobs retry fresh instead).
    """
    store = job_store.get_store()
    now = _now()
    store.conn.execute(
        """INSERT INTO jobs (
               job_id, user_id, job_type, status, query, regions,
               total_tasks, done_tasks, result_count,
               source_type, is_resumable, group_name, display_name,
               created_at, updated_at, last_heartbeat
           ) VALUES (?, ?, 'scraper', 'running', ?, ?, ?, 0, 0, ?, 0, ?, ?, ?, ?, ?)""",
        (
            job_id, user_id, query_label,
            json.dumps(regions_payload),
            PHASE_TOTAL, SOURCE_TYPE,
            group_name, display_name,
            now, now, now,
        ),
    )
    store.conn.commit()


def create_and_launch(
    user: dict[str, Any],
    filters: dict[str, Any],
    group: Optional[str] = None,
) -> dict[str, Any]:
    """Validate + insert + spawn the driver. Returns the API response dict.

    Raises PlacesError (kill switch / key missing) or ValueError (filterless).
    """
    if not places_client.is_enabled():
        raise PlacesError("create", 503, "Instant US Database is disabled (ENABLE_GETLEADS_PLACES=false)")
    if not places_client.is_configured():
        raise PlacesError("create", 503, "GETLEADS_API_KEY is not configured")

    filters = places_client.sanitize_filters(filters)  # ValueError on filterless

    allowed, limit_message = db.check_daily_request_limit(
        user_id=user["user_id"],
        is_admin=user.get("is_admin", False),
        estimated_requests=1,
    )
    if not allowed:
        raise ValueError(limit_message)

    label = places_client.describe_filters(filters)
    job_id = str(uuid.uuid4())
    regions_payload = {
        "mode": "places",
        "country": "us",
        "filters": filters,
        "places": {"export_id": None, "expected": None},
    }
    _insert_places_job(
        job_id=job_id,
        user_id=user["user_id"],
        query_label=label,
        display_name=f"{label} · Instant DB",
        regions_payload=regions_payload,
        group_name=group,
    )
    asyncio.create_task(run_places_job(job_id))
    logger.info("Places job %s created by user %s (%s)", job_id, user.get("user_id"), label)
    return {"job_id": job_id, "status": "running", "total_tasks": PHASE_TOTAL}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


class _Emitter:
    """Sequenced append_event + heartbeat + SSE wake for one job."""

    def __init__(self, job_id: str):
        self.job_id = job_id
        self.seq = job_store.get_store().count_events(job_id)

    def emit(self, event: dict[str, Any]) -> None:
        store = job_store.get_store()
        store.append_event(self.job_id, self.seq, event)
        self.seq += 1
        store.heartbeat(self.job_id)
        self._wake()

    def _wake(self) -> None:
        # Best-effort: the SSE stream also polls every 2s, so a missed wake
        # only costs latency, never correctness.
        try:
            from scraper.routes import _job_signals

            sig = _job_signals.get(self.job_id)
            if sig:
                sig.set()
        except Exception:
            pass


def _fail(job_id: str, message: str) -> None:
    logger.warning("Places job %s failed: %s", job_id, message)
    job_store.get_store().set_failed(job_id, message)


def _save_places_state(job_id: str, **state: Any) -> None:
    """Merge keys into the job's regions blob 'places' sub-dict (survives
    restarts — the rearm path reads export_id from here)."""
    store = job_store.get_store()
    job = store.get_job(job_id)
    if not job:
        return
    try:
        regions = json.loads(job["regions"]) if job.get("regions") else {}
    except (ValueError, TypeError):
        regions = {}
    places_state = regions.get("places") or {}
    places_state.update(state)
    regions["places"] = places_state
    store.conn.execute(
        "UPDATE jobs SET regions=?, updated_at=? WHERE job_id=?",
        (json.dumps(regions), _now(), job_id),
    )
    store.conn.commit()


def _normalize_row(raw: dict[str, str]) -> dict[str, str]:
    """GetLeads CSV row → our canonical columns (missing fields → '')."""
    def g(key: str) -> str:
        return (raw.get(key) or "").strip()

    extra_emails = "; ".join(
        e for e in (g(f"email_{i}") for i in range(2, 9)) if e
    )
    street, city, state, zip_ = g("street_address"), g("city"), g("state"), g("zip")
    full_address = ", ".join(p for p in (street, city, state, zip_) if p)
    return {
        "name": g("business_name"),
        "category": g("category"),
        "phone": g("phone"),
        "website": g("website"),
        "email": g("primary_email"),
        "email_type": g("email_type"),
        "additional_emails": extra_emails,
        "street_address": street,
        "city": city,
        "state": state,
        "zip": zip_,
        "full_address": full_address,
        "rating": g("rating"),
        "review_count": g("review_count"),
        "google_maps_url": g("google_maps_url"),
        "facebook": g("facebook"),
        "instagram": g("instagram"),
        "linkedin": g("linkedin"),
    }


def _write_csv(job_id: str, rows: list[dict[str, str]]) -> Path:
    out_path = OUTPUT_DIR / f"{job_id}.csv"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = OUTPUT_DIR / f"{job_id}.csv.tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    os.replace(tmp_path, out_path)  # atomic — never a half-visible CSV
    return out_path


async def run_places_job(job_id: str, *, resume: bool = False) -> None:
    """Drive one places job to a terminal state. Never raises — every failure
    path lands in set_failed so the job card shows the reason."""
    emitter = _Emitter(job_id)
    store = job_store.get_store()
    job = store.get_job(job_id)
    if not job or job.get("source_type") != SOURCE_TYPE:
        logger.error("run_places_job: %s is not a places job", job_id)
        return

    try:
        regions = json.loads(job.get("regions") or "{}")
    except (ValueError, TypeError):
        regions = {}
    filters = regions.get("filters") or {}
    state = regions.get("places") or {}
    export_id = state.get("export_id")

    if store.is_job_cancelled_or_abandoned(job_id):
        return

    try:
        # ---- Phase 1: fair-use precheck + free count -----------------------
        if not resume or not export_id:
            emitter.emit({
                "phase": 1, "task_done": False, "new_results": 0,
                "status": "precheck", "message": "Checking daily budget…",
            })
            fu = await places_client.fair_use()
            daily_remaining = (fu or {}).get("daily", {}).get("remaining")
            resets_at = (fu or {}).get("daily", {}).get("resets_at")
            if daily_remaining is not None and daily_remaining <= 0:
                raise PlacesError(
                    "fair_use", 402,
                    f"GetLeads daily Google Maps budget is used up (resets {resets_at or 'at 00:00 UTC'}). "
                    "Try again after the reset.",
                )

            emitter.emit({
                "phase": 1, "task_done": False, "new_results": 0,
                "status": "counting", "message": "Counting matches (free)…",
            })
            total = await places_client.count_places(filters)
            _save_places_state(job_id, expected=total)
            emitter.emit({
                "phase": 1, "task_done": True, "new_results": 0,
                "status": "counted", "message": f"{total:,} businesses match.",
            })

            if total == 0:
                out_path = _write_csv(job_id, [])
                store.update_result_count(job_id, 0)
                emitter.emit({
                    "phase": 3, "task_done": True, "new_results": 0,
                    "status": "done", "message": "No matches — empty CSV ready.",
                })
                store._mark_done_and_cleanup(job_id, str(out_path))
                return

            if total > places_client.MAX_EXPORT_ROWS:
                raise PlacesError(
                    "size_cap", 422,
                    f"{total:,} businesses exceed the 25,000-per-export cap. "
                    "Narrow the filters (fewer states, or ZIP groups) and fetch in parts.",
                )

            if daily_remaining is not None and total > daily_remaining:
                emitter.emit({
                    "phase": 1, "task_done": True, "new_results": 0,
                    "status": "warn",
                    "message": f"Note: {total:,} requested but only {daily_remaining:,} rows left today — export may be trimmed.",
                })

        # ---- Phase 2: async export on GetLeads -----------------------------
        if not resume or not export_id:
            emitter.emit({
                "phase": 2, "task_done": False, "new_results": 0,
                "status": "exporting", "message": "Export started on GetLeads…",
            })
            export_id = await places_client.start_export(filters)
            _save_places_state(job_id, export_id=export_id)

        emitter.emit({
            "phase": 2, "task_done": False, "new_results": 0,
            "status": "exporting", "message": "Export running on GetLeads (polling)…",
        })
        export_url, rows_available = await _poll_until_done(job_id, emitter, str(export_id))
        emitter.emit({
            "phase": 2, "task_done": True, "new_results": 0,
            "status": "export_done", "message": "Export ready — downloading CSV…",
        })

        # ---- Phase 3: download + write --------------------------------------
        _, raw_rows = await places_client.download_export_csv(export_url)
        rows = [_normalize_row(r) for r in raw_rows]
        out_path = _write_csv(job_id, rows)
        with_emails = sum(1 for r in rows if r["email"])
        # Emit BEFORE the exact set: append_event increments result_count by
        # new_results, and update_result_count then pins the authoritative
        # total (the reverse order double-counts — caught by test).
        emitter.emit({
            "phase": 3, "task_done": True, "new_results": len(rows),
            "status": "done",
            "message": f"{len(rows):,} businesses ({with_emails:,} with email) saved.",
        })
        store.update_result_count(job_id, len(rows))
        store._mark_done_and_cleanup(job_id, str(out_path))
        logger.info("Places job %s done: %d rows (%d with email)",
                    job_id, len(rows), with_emails)

    except PlacesError as exc:
        emitter.emit({
            "phase": 0, "task_done": False, "new_results": 0,
            "status": "failed", "message": exc.detail,
        })
        _fail(job_id, exc.detail)
    except Exception as exc:  # never leave a places job hanging in 'running'
        logger.exception("Places job %s crashed", job_id)
        message = f"Unexpected error: {exc}"[:500]
        emitter.emit({
            "phase": 0, "task_done": False, "new_results": 0,
            "status": "failed", "message": message,
        })
        _fail(job_id, message)


async def _poll_until_done(
    job_id: str, emitter: _Emitter, export_id: str
) -> tuple[str, Optional[int]]:
    """Poll the export until completed/failed/timeout/cancel. Returns
    (export_url, rows_available)."""
    store = job_store.get_store()
    deadline = asyncio.get_event_loop().time() + places_client.EXPORT_TIMEOUT_S
    while True:
        if store.is_job_cancelled_or_abandoned(job_id):
            raise PlacesError(
                "cancelled", 0,
                "Cancelled — the remote export may still complete and use its rows.",
            )
        store.heartbeat(job_id)

        data = await places_client.poll_export(export_id)
        status = str(data.get("job_status") or data.get("status") or "").lower()
        if status == "completed":
            url = data.get("export_url")
            if not url:
                raise PlacesError("poll_export", 0, "completed export returned no export_url")
            return str(url), data.get("rows_exported")
        if status in ("failed", "error", "cancelled"):
            raise PlacesError("poll_export", 0, f"GetLeads export {status}: {str(data)[:200]}")

        if asyncio.get_event_loop().time() > deadline:
            raise PlacesError(
                "poll_export", 0,
                f"Export did not finish within {int(places_client.EXPORT_TIMEOUT_S)}s "
                f"(export_id {export_id}).",
            )
        await asyncio.sleep(places_client.EXPORT_POLL_INTERVAL_S)


# ---------------------------------------------------------------------------
# Restart re-arm (boot path — see main._run_parent_startup)
# ---------------------------------------------------------------------------


def _claim_for_rearm(job_id: str) -> bool:
    """Cross-worker CAS claim so exactly one of the 4 gunicorn workers re-arms
    each running places job. Uses the existing resume_claimed_at column with a
    10-minute stale override (a crashed claim shouldn't block re-arm forever)."""
    store = job_store.get_store()
    now = _now()
    cursor = store.conn.execute(
        """UPDATE jobs SET resume_claimed_at=?, updated_at=?
           WHERE job_id=? AND status='running'
             AND (resume_claimed_at IS NULL
                  OR datetime(resume_claimed_at) < datetime('now', '-10 minutes'))""",
        (now, now, job_id),
    )
    store.conn.commit()
    return cursor.rowcount > 0


def rearm_running_places_jobs() -> int:
    """Re-attach pollers to places jobs still 'running' after a restart.

    - export_id saved → resume polling (no re-spend).
    - no export_id → re-run from phase 1 (fresh export; the pre-restart
      export, if one had just started, completes remotely unbilled-to-us but
      counts against the shared fair-use budget — accepted, tiny window).
    """
    if not places_client.is_enabled():
        return 0
    store = job_store.get_store()
    rows = store.conn.execute(
        """SELECT job_id FROM jobs
           WHERE job_type='scraper' AND source_type=? AND status='running'""",
        (SOURCE_TYPE,),
    ).fetchall()
    rearmed = 0
    for row in rows:
        job_id = row["job_id"]
        if _claim_for_rearm(job_id):
            asyncio.create_task(run_places_job(job_id, resume=True))
            rearmed += 1
            logger.info("Re-armed places job %s after restart", job_id)
    return rearmed
