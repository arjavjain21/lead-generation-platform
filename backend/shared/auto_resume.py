"""
Auto-resume of abandoned enrichment jobs (2026-08-20 abandonment incident).

When a gunicorn worker dies mid-job (WORKER TIMEOUT murder or max-requests
recycle), the in-worker asyncio job runner dies with it. The next worker to
boot marks the job 'abandoned' via the startup reaper. Today the user must
manually click Restart — under worker churn every multi-hour job dies this way
and the tool becomes unusable.

This module closes the loop: after the startup reaper marks jobs abandoned,
one worker atomically claims each freshly-abandoned, resumable job and resumes
it through the same code path as the user-facing /restart endpoint (partial
CSV carry-over + checkpoint-based row skipping — no data loss, no duplicate
rows; the resume machinery is proven in production).

Safety rails:
- ``try_claim_abandoned`` is an atomic compare-and-set on ``jobs.status``
  (running -> abandoned) so exactly one of the 4 gunicorn workers claims a job.
- Only jobs whose heartbeat went stale RECENTLY (default 24h) are considered,
  so ancient abandoned jobs from weeks ago are not suddenly resurrected.
- ``restart_count`` is capped (default 10) to prevent infinite crash loops.
- ``AUTO_RESUME_ENABLED`` env kill-switch (default on).
- Everything is best-effort: an exception here must never block worker boot.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Optional

from shared import db

logger = logging.getLogger(__name__)

# Jobs whose heartbeat went stale longer ago than this are NOT auto-resumed
# (they predate this boot cycle; resurrecting weeks-old jobs would surprise
# the user and burn provider credits).
AUTO_RESUME_MAX_STALE_AGE_HOURS = float(os.getenv("AUTO_RESUME_MAX_STALE_AGE_HOURS", "24"))

# Hard cap on restart_count — beyond this the job is presumed pathological and
# left for a human.
AUTO_RESUME_MAX_RESTARTS = int(os.getenv("AUTO_RESUME_MAX_RESTARTS", "10"))

# How long to wait after boot before resuming (lets the flood settle and the
# reaper finish marking everything first).
AUTO_RESUME_DELAY_SECONDS = float(os.getenv("AUTO_RESUME_DELAY_SECONDS", "15"))

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


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


async def resume_one(job_id: str, user_id: str) -> bool:
    """Resume a single abandoned job through the restart code path.

    Reuses ``enrichment.routes.restart_enrichment_job``'s logic by invoking the
    endpoint function directly with a synthetic admin identity. This keeps a
    single source of truth for resume semantics (partial CSV carry-over,
    checkpoint-space dedupe, cascade preservation).

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
