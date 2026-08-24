"""
Auto-resume of abandoned jobs (2026-08-20 enrichment incident; 2026-08-24 scraper).

When a gunicorn worker dies mid-job (WORKER TIMEOUT murder or max-requests
recycle), the in-worker asyncio job runner dies with it. The reaper marks the
job 'abandoned'. This module closes the loop: freshly-abandoned, resumable
jobs are atomically claimed and resumed through the same code paths as the
user-facing resume/restart endpoints (partial CSV carry-over + checkpoint
skipping — no data loss, no duplicate rows; the resume machinery is proven
in production).

Two independent flows:
  1. Enrichment (boot-time, since 2026-08-20): after the startup reaper runs,
     one worker claims each freshly-abandoned enrichment job and resumes it
     through ``_restart_job_core`` (auto=True).
  2. Scraper (runtime guard, 2026-08-24): the dispatcher's guard loop reaps
     stale-running scraper jobs every minute and immediately re-queues the
     freshly abandoned ones through the resume path (checkpoints preserved).
     Cap: SCRAPER_AUTO_RESUME_MAX_RESTARTS (default 2) counted on the ROOT
     job of the chain — i.e. after 2 automatic retries the job is left for a
     human, exactly as requested ("tried at least 2x before leaving it on
     abandoned").

Safety rails:
- ``try_claim_abandoned`` is an atomic compare-and-set on ``jobs.status``
  (running -> abandoned) so exactly one of the 4 gunicorn workers claims a job.
- Only jobs whose heartbeat went stale RECENTLY (default 24h) are considered,
  so ancient abandoned jobs from weeks ago are not suddenly resurrected.
- ``restart_count`` is capped per job type.
- ``AUTO_RESUME_ENABLED`` env kill-switch (default on).
- Everything is best-effort: an exception here must never block a worker.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from shared import db

logger = logging.getLogger(__name__)

# Jobs whose heartbeat went stale longer ago than this are NOT auto-resumed
# (they predate this boot cycle; resurrecting weeks-old jobs would surprise
# the user and burn provider credits).
AUTO_RESUME_MAX_STALE_AGE_HOURS = float(os.getenv("AUTO_RESUME_MAX_STALE_AGE_HOURS", "24"))

# Hard cap on enrichment restart_count — beyond this the job is presumed
# pathological and left for a human.
AUTO_RESUME_MAX_RESTARTS = int(os.getenv("AUTO_RESUME_MAX_RESTARTS", "10"))

# How long to wait after boot before resuming (lets the flood settle and the
# reaper finish marking everything first).
AUTO_RESUME_DELAY_SECONDS = float(os.getenv("AUTO_RESUME_DELAY_SECONDS", "15"))

# Scraper-specific: max automatic resume attempts per job CHAIN (counted on
# the root job via restart_count). After this, the job stays abandoned for a
# human decision. Default 2 = "try twice automatically, then give up".
SCRAPER_AUTO_RESUME_MAX_RESTARTS = int(os.getenv("SCRAPER_AUTO_RESUME_MAX_RESTARTS", "2"))

# How recently a scraper job must have been abandoned to auto-resume it
# (guards the runtime guard loop from resurrecting old rows when it starts).
SCRAPER_AUTO_RESUME_MAX_ABANDONED_AGE_MINUTES = float(
    os.getenv("SCRAPER_AUTO_RESUME_MAX_ABANDONED_AGE_MINUTES", "30")
)

AUTO_RESUME_ENABLED = os.getenv("AUTO_RESUME_ENABLED", "true").lower() in ("1", "true", "yes")


def try_claim_abandoned(job_id: str) -> bool:
    """Atomically transition a job from 'running' to 'abandoned' and claim it.

    Returns True if THIS caller won the claim (i.e. the row was still
    'running' and is now 'abandoned' with our marker), False otherwise.

    The claim doubles as the abandonment itself: the compare-and-set
    ``WHERE status='running'`` guarantees exactly one writer flips the state,
    even across processes/workers (SQLite serializes writers; the rowcount
    tells us if we were first).
    """
    conn = db.get_db()
    error_text = (
        "Job was interrupted by a worker restart. Auto-resume will restart "
        "it from its last checkpoint."
    )
    cursor = conn.execute(
        """UPDATE jobs
           SET status='abandoned',
               error=?,
               updated_at=?
           WHERE job_id=? AND status='running'""",
        (error_text, _utc_now_iso(), job_id),
    )
    conn.commit()
    return cursor.rowcount == 1


def get_recently_abandoned_resumable_jobs() -> list[dict[str, Any]]:
    """Enrichment jobs abandoned by THIS boot cycle's reaper that are safe to resume.

    Selection criteria (all must hold):
    - job_type='enrichment' and status='abandoned'
    - heartbeat went stale within AUTO_RESUME_MAX_STALE_AGE_HOURS (fresh death,
      not an ancient job)
    - restart_count below the cap
    - is_resumable flag set on the row
    """
    conn = db.get_db()
    rows = conn.execute(
        """SELECT job_id, user_id, restart_count, last_heartbeat
           FROM jobs
           WHERE job_type='enrichment'
             AND status='abandoned'
             AND is_resumable=1
             AND restart_count < ?
             AND last_heartbeat IS NOT NULL
             AND datetime(last_heartbeat) >= datetime('now', ?)""",
        (AUTO_RESUME_MAX_RESTARTS, f"-{int(AUTO_RESUME_MAX_STALE_AGE_HOURS)} hours"),
    ).fetchall()
    return [dict(r) for r in rows]


def get_recently_abandoned_scraper_jobs() -> list[dict[str, Any]]:
    """Freshly-abandoned scraper jobs eligible for automatic resume.

    Eligibility:
    - job_type='scraper', status='abandoned', is_resumable=1
    - abandoned within SCRAPER_AUTO_RESUME_MAX_ABANDONED_AGE_MINUTES
    - ROOT of its chain (no children of its own — a child means a resume
      already happened; if the child dies it becomes the new abandoned head)
    - root's restart_count below SCRAPER_AUTO_RESUME_MAX_RESTARTS
    """
    conn = db.get_db()
    rows = conn.execute(
        """SELECT j.job_id, j.user_id, j.restart_count, j.updated_at, j.last_heartbeat
           FROM jobs j
           WHERE j.job_type='scraper'
             AND j.status='abandoned'
             AND j.is_resumable=1
             AND j.restart_count < ?
             AND datetime(j.updated_at) >= datetime('now', ?)
             AND NOT EXISTS (
               SELECT 1 FROM jobs c WHERE c.parent_job_id = j.job_id
             )""",
        (
            SCRAPER_AUTO_RESUME_MAX_RESTARTS,
            f"-{int(SCRAPER_AUTO_RESUME_MAX_ABANDONED_AGE_MINUTES)} minutes",
        ),
    ).fetchall()
    return [dict(r) for r in rows]


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


async def resume_one(job_id: str, user_id: str) -> bool:
    """Resume a single abandoned ENRICHMENT job through the restart code path.

    Reuses ``enrichment.routes._restart_job_core``'s logic by invoking the
    function directly with a synthetic admin identity. This keeps a single
    source of truth for resume semantics (partial CSV carry-over, checkpoint
    space dedupe, cascade preservation).

    Returns True if a new job was created.
    """
    # Import inside the function: enrichment.routes imports shared.db and is
    # heavy; also avoids circular imports at module load.
    from enrichment import routes as enrichment_routes

    synthetic_user = {"user_id": user_id, "email": "auto-resume@system", "is_admin": True}

    try:
        result = await enrichment_routes._restart_job_core(
            job_id, current_user=synthetic_user, auto=True
        )
        logger.info(
            "Auto-resume: job %s resumed as %s (total=%s)",
            job_id, result.get("job_id"), result.get("total"),
        )
        return True
    except Exception as exc:
        logger.warning("Auto-resume failed for job %s: %s", job_id, exc)
        return False


def claim_scraper_resume_job(job_id: str) -> "str | None":
    """Atomically create the resume child for an abandoned scraper job.

    Everything — the eligibility re-check, the chain attempt-count bump on
    the ROOT job, and the child row INSERT — happens inside ONE
    ``BEGIN IMMEDIATE`` transaction on a dedicated connection. Concurrent
    claimants (4 workers' guard loops tick in the same second) block on the
    reserved lock; after commit they re-read, see the child, and bail. This
    closes both races the first two deploy iterations exposed:
      1. SELECT/UPDATE CAS on the thread-local conn read stale snapshots
         (no cross-process serialization) → N winners.
      2. Serializing only the counter bump still left the child INSERT
         outside the txn → rivals claimed in the gap after COMMIT.

    The child is created fully-formed (status='queued', regions copied
    verbatim, total/done counters from the chain root, result_count from the
    carried-over CSV). The parent's partial CSV is copied BEFORE the txn
    (deterministic child path); on any bail the copy is unlinked. The
    dispatcher claims the child within seconds and the standard
    checkpoint-aware runner does the work — exactly like a user-clicked
    resume, minus the HTTP layer.

    Returns the new child job_id, or None when this claimant lost / the job
    is ineligible (already has a child, chain at cap, not abandoned).
    """
    import sqlite3 as _sqlite3
    import uuid as _uuid
    from pathlib import Path as _Path
    from scraper.dispatch import safe_copy_csv

    output_dir = _Path(__file__).resolve().parent.parent / "data" / "outputs"

    conn = _sqlite3.connect(str(db.DB_PATH), timeout=30.0, isolation_level=None)
    conn.row_factory = _sqlite3.Row
    new_child_id = str(_uuid.uuid4())
    copied_csv = output_dir / f"{new_child_id}.csv"
    try:
        parent = conn.execute(
            """SELECT * FROM jobs
               WHERE job_id=? AND job_type='scraper' AND status='abandoned'
                 AND is_resumable=1""",
            (job_id,),
        ).fetchone()
        if not parent:
            return None

        # Carry over the partial CSV (truncated-tail repaired). Copied before
        # the txn under the child's deterministic path; unlinked on bail.
        src = None
        if parent["output_path"]:
            candidate = _Path(parent["output_path"])
            if candidate.exists():
                src = candidate
        if src is None:
            fallback = output_dir / f"{job_id}.csv"
            if fallback.exists():
                src = fallback
        carried_rows = 0
        if src is not None and safe_copy_csv(src, copied_csv) > 0:
            import csv as _csv
            try:
                with open(copied_csv, newline="", encoding="utf-8") as f:
                    carried_rows = sum(1 for _ in _csv.DictReader(f))
            except Exception:
                carried_rows = 0

        conn.execute("BEGIN IMMEDIATE")
        try:
            # Re-check under the write lock: another claimant may have won
            # while we copied the CSV.
            parent = conn.execute(
                """SELECT * FROM jobs
                   WHERE job_id=? AND job_type='scraper' AND status='abandoned'
                     AND is_resumable=1
                     AND NOT EXISTS (SELECT 1 FROM jobs c WHERE c.parent_job_id = jobs.job_id)""",
                (job_id,),
            ).fetchone()
            if not parent:
                conn.execute("ROLLBACK")
                copied_csv.unlink(missing_ok=True)
                return None

            # Walk to the chain root INSIDE the txn. The attempt budget lives
            # on the root: each generation's child starts restart_count=0, so
            # counting on the abandoned row would reset the cap every restart.
            root = parent
            hops = 0
            while root["parent_job_id"] and hops < 10:
                row = conn.execute(
                    "SELECT * FROM jobs WHERE job_id=?", (root["parent_job_id"],)
                ).fetchone()
                if not row:
                    break
                root = row
                hops += 1
            if int(root["restart_count"] or 0) >= SCRAPER_AUTO_RESUME_MAX_RESTARTS:
                conn.execute("ROLLBACK")
                copied_csv.unlink(missing_ok=True)
                logger.info(
                    "Scraper auto-resume: chain of %s at attempt cap (%d) — leaving abandoned",
                    job_id[:8], root["restart_count"],
                )
                return None

            conn.execute(
                "UPDATE jobs SET restart_count = restart_count + 1, updated_at=? WHERE job_id=?",
                (_utc_now_iso(), root["job_id"]),
            )
            now = _utc_now_iso()
            conn.execute(
                """INSERT INTO jobs (job_id, user_id, job_type, status, parent_job_id,
                     query, regions, total_tasks, done_tasks, result_count,
                     is_resumable, created_at, updated_at)
                   VALUES (?, ?, 'scraper', 'queued', ?, ?, ?, ?, ?, ?, 1, ?, ?)""",
                (
                    new_child_id, parent["user_id"], job_id,
                    parent["query"], parent["regions"],
                    int(root["total_tasks"] or 0), int(root["done_tasks"] or 0),
                    carried_rows, now, now,
                ),
            )
            conn.execute("COMMIT")
            logger.info(
                "Scraper auto-resume: %s -> new child %s (carried %d rows, chain attempt %d)",
                job_id[:8], new_child_id[:8], carried_rows, int(root["restart_count"] or 0) + 1,
            )
            return new_child_id
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            copied_csv.unlink(missing_ok=True)
            raise
    finally:
        conn.close()


async def resume_one_scraper(job_id: str, user_id: str) -> bool:
    """Resume a single abandoned SCRAPER job via claim_scraper_resume_job.

    Thin wrapper: the claim function creates the queued child atomically;
    the dispatcher picks it up. No HTTP-layer involvement.
    """
    try:
        child = claim_scraper_resume_job(job_id)
        if child is None:
            logger.info("Scraper auto-resume: skipped %s (lost race / ineligible / at cap)", job_id[:8])
            return False
        return True
    except Exception as exc:
        logger.warning("Scraper auto-resume failed for job %s: %s", job_id, exc)
        return False

    try:
        # Drive the same endpoint function the UI's "Resume (N tasks)" button
        # calls — single source of truth for resume semantics.
        result = await scraper_routes.resume_scraper_job(
            job_id,
            req=scraper_routes.ResumeJobRequest(include_previous=True),
            background_tasks=None,  # unused now — resume enqueues via dispatcher
            current_user=synthetic_user,
        )
        logger.info(
            "Scraper auto-resume: job %s -> %s (pending=%s)",
            job_id[:8], result.get("job_id", "?")[:8], result.get("pending_tasks"),
        )
        return True
    except HTTPException as http_exc:
        logger.warning(
            "Scraper auto-resume skipped for %s: %s", job_id[:8], http_exc.detail
        )
        return False
    except Exception as exc:
        logger.warning("Scraper auto-resume failed for job %s: %s", job_id, exc)
        return False


async def maybe_auto_resume_abandoned_jobs() -> None:
    """Entry point called from the FastAPI lifespan after the reaper ran.

    Best-effort: never raises. Claims and resumes at most the jobs found in
    ``get_recently_abandoned_resumable_jobs``; concurrency across the 4
    workers is resolved by the atomic claim.
    """
    if not AUTO_RESUME_ENABLED:
        logger.info("Auto-resume disabled via AUTO_RESUME_ENABLED")
        return

    try:
        await asyncio.sleep(AUTO_RESUME_DELAY_SECONDS)
        candidates = get_recently_abandoned_resumable_jobs()
        if not candidates:
            logger.info("Auto-resume: no freshly-abandoned resumable jobs found")
            return

        logger.info("Auto-resume: %d candidate job(s) after boot", len(candidates))
        for job in candidates:
            job_id = job["job_id"]
            # Only resume jobs nobody has resumed yet. ANY child (regardless
            # of its status) means a restart already happened — if the child
            # is done the work is complete, if running/queued it is in flight,
            # and if the child itself later dies, IT becomes the abandoned
            # head that gets auto-resumed. This prevents re-running chains
            # that already completed (e.g. the Aug 19-20 incident chain where
            # a manual restart finished the file).
            if _has_child_job(job_id):
                logger.info("Auto-resume: job %s already has a restart child, skipping", job_id)
                continue
            await resume_one(job_id, job["user_id"] or "")
    except Exception as exc:
        logger.warning("Auto-resume pass failed (non-fatal): %s", exc)


async def maybe_auto_resume_scraper_jobs() -> None:
    """Scraper counterpart of maybe_auto_resume_abandoned_jobs.

    Called by the dispatcher's runtime guard loop every GUARD_POLL_SECONDS and
    once at boot (after the boot reaper). Atomic in the sense that the resume
    creates a child with parent_job_id — a second caller sees the child via
    _has_child_job (baked into the candidate query) and skips. The
    restart_count bump happens BEFORE the child insert, so even a crash
    between the two only ever over-counts attempts (safe direction).
    """
    if not AUTO_RESUME_ENABLED:
        return
    try:
        candidates = get_recently_abandoned_scraper_jobs()
        if not candidates:
            return
        logger.info("Scraper auto-resume: %d candidate(s)", len(candidates))
        for job in candidates:
            await resume_one_scraper(job["job_id"], job["user_id"] or "")
    except Exception as exc:
        logger.warning("Scraper auto-resume pass failed (non-fatal): %s", exc)


def _has_child_job(job_id: str) -> bool:
    """True if this job has ANY restart-child, regardless of child status.

    A child means the resume decision already happened (manually or by a
    prior auto-resume pass). See maybe_auto_resume_abandoned_jobs.
    """
    conn = db.get_db()
    row = conn.execute(
        "SELECT 1 FROM jobs WHERE parent_job_id=? LIMIT 1",
        (job_id,),
    ).fetchone()
    return row is not None
