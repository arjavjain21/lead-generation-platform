"""
Scraper job dispatcher — platform-wide concurrency cap (2026-08-24 incident).

Root cause of the repeated "abandoned jobs": gunicorn's round-robin can pile
several heavy scraper jobs (each 8 crawler workers × 3 zoom levels) onto ONE
process. Combined with the /enrich flood sharing the same 4 workers, the
worker hits the cgroup memory ceiling, the event loop stalls in swap-thrash,
heartbeats stop, and every job on that worker gets reaped as 'abandoned'.

Fix: jobs are created in status='queued' and a dispatcher claims them one at
a time, respecting TWO caps:
  - MAX_CONCURRENT_SCRAPER_JOBS   — platform-wide, across all 4 workers
  - SCRAPER_JOBS_PER_WORKER       — per process, so one worker can never
                                    hoard every slot (today's failure mode)

The claim is an atomic compare-and-set (UPDATE ... WHERE status='queued') —
SQLite serializes writers, so with 4 workers each running a dispatch loop,
exactly one wins per claim. Queued jobs hold no worker resources (no event,
no runner, no memory) so a deep backlog costs nothing.

Also hosts the runtime guard loop: reaps stale-running scraper jobs every
minute (heartbeat-aware, same rule as the boot reaper) and feeds freshly
abandoned ones to auto-resume. This closes the gap where a frozen worker's
jobs sat 'running' until the NEXT worker boot noticed them (up to ~30 min
under max-requests recycling).

Safety properties:
- A queued job can be cancelled normally (cancel endpoint accepts 'queued').
- The stale-reaper never touches queued jobs (running-only query) — a backlog
  can wait indefinitely without being falsely abandoned.
- If a worker dies between claim and launch, the job's heartbeat never starts,
  the runtime guard reaps it as stale-running, and auto-resume re-queues it.
- Best-effort everywhere: a dispatcher crash must never take down a worker.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Awaitable, Callable

from shared import db

logger = logging.getLogger(__name__)

# Platform-wide cap on concurrently RUNNING scraper jobs (all workers
# combined). 14 jobs at once was the freeze recipe; 6 keeps total in-flight
# crawl tasks near ~48 while leaving memory headroom for the /enrich flood
# sharing the same processes.
MAX_CONCURRENT_SCRAPER_JOBS = int(os.getenv("MAX_CONCURRENT_SCRAPER_JOBS", "6"))

# Per-worker cap. Without this, one worker's poll loop could claim every free
# slot in a burst and recreate the single-process pileup (2026-08-24).
SCRAPER_JOBS_PER_WORKER = int(os.getenv("SCRAPER_JOBS_PER_WORKER", "2"))

# How often each worker's dispatch loop looks for a free slot.
DISPATCH_POLL_SECONDS = float(os.getenv("SCRAPER_DISPATCH_POLL_SECONDS", "5"))

# How often the runtime guard reaps stale jobs + triggers auto-resume.
GUARD_POLL_SECONDS = float(os.getenv("SCRAPER_GUARD_POLL_SECONDS", "60"))


def count_running_scraper_jobs() -> int:
    """Number of scraper jobs currently in status='running' (all workers)."""
    conn = db.get_db()
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM jobs WHERE job_type='scraper' AND status='running'"
    ).fetchone()
    return int(row["n"]) if row else 0


def claim_next_queued_scraper_job() -> str | None:
    """Atomically claim the oldest queued scraper job (queued -> running).

    Returns the job_id claimed by THIS caller, or None if the platform cap is
    hit or no job is waiting. The compare-and-set on status guarantees that
    with N dispatch loops polling concurrently, each queued job is claimed by
    exactly one worker.

    Deliberately claims at most ONE job per call: both caps are re-checked on
    every call, so a burst of claims can never overshoot a limit even if a
    job finished and freed a slot between the count and the claim.
    """
    conn = db.get_db()
    if count_running_scraper_jobs() >= MAX_CONCURRENT_SCRAPER_JOBS:
        return None

    row = conn.execute(
        """SELECT job_id FROM jobs
           WHERE job_type='scraper' AND status='queued'
           ORDER BY created_at ASC LIMIT 1"""
    ).fetchone()
    if not row:
        return None

    job_id = row["job_id"]
    cursor = conn.execute(
        """UPDATE jobs SET status='running', updated_at=?
           WHERE job_id=? AND status='queued'""",
        (_now_iso(), job_id),
    )
    conn.commit()
    if cursor.rowcount != 1:
        # Another worker won the race — not an error.
        return None
    return job_id


def safe_copy_csv(src, dst) -> int:
    """Copy a partial-output CSV, repairing a truncated final line.

    A worker death mid-write can leave a partial last line (flushed without
    its newline). csv.DictReader would then mis-parse or duplicate it. This
    copies the file and, if it does not end with a newline, drops the tail.

    Returns the number of bytes written (0 if the source is missing/empty —
    caller treats that as "nothing to carry over").
    """
    import shutil

    if not src.exists() or src.stat().st_size == 0:
        return 0
    shutil.copyfile(src, dst)
    size = dst.stat().st_size
    if size == 0:
        return 0
    with open(dst, "rb") as f:
        f.seek(-1, os.SEEK_END)
        last = f.read(1)
    if last != b"\n":
        # Truncated tail — find the last newline and cut after it.
        with open(dst, "rb") as f:
            data = f.read()
        cut = data.rfind(b"\n")
        with open(dst, "wb") as f:
            f.write(data[: cut + 1] if cut >= 0 else b"")
        logger.info("safe_copy_csv: repaired truncated tail on %s", dst.name)
    return dst.stat().st_size


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


async def dispatch_loop(
    launch: Callable[[str], Awaitable[None]],
    poll_seconds: float = DISPATCH_POLL_SECONDS,
    per_worker_cap: int = SCRAPER_JOBS_PER_WORKER,
) -> None:
    """Per-worker dispatch loop: claim queued jobs and launch their runners.

    Started once per worker from main.py lifespan (guarded by the same env
    flag as the other boot-time loops so pytest boots never run it). Each
    claim is launched as an independent asyncio task so the loop keeps
    polling; the per-worker cap bounds how many this process hosts at once.
    """
    in_flight: set[asyncio.Task] = set()
    while True:
        try:
            in_flight = {t for t in in_flight if not t.done()}
            if len(in_flight) < per_worker_cap:
                job_id = claim_next_queued_scraper_job()
                if job_id:
                    logger.info(
                        "Dispatch: claimed queued scraper job %s (%d/%d on this worker)",
                        job_id, len(in_flight) + 1, per_worker_cap,
                    )
                    task = asyncio.create_task(launch(job_id))
                    in_flight.add(task)
        except asyncio.CancelledError:
            for t in in_flight:
                t.cancel()
            raise
        except Exception as exc:
            logger.warning("Dispatch tick failed (non-fatal): %s", exc)
        await asyncio.sleep(poll_seconds)


async def runtime_guard_loop(interval_seconds: float = GUARD_POLL_SECONDS) -> None:
    """Reap stale-running scraper jobs and auto-resume the freshly abandoned.

    The boot-time reaper only runs when a worker (re)starts; under steady
    state a frozen worker's jobs would sit 'running' until the next worker
    recycle (up to ~30 min). This loop applies the exact same heartbeat-aware
    rule every minute, then hands freshly-abandoned jobs to auto-resume —
    total detection-to-restart latency ~2 minutes, forever.

    Started per worker from main.py lifespan. Safe to run in all 4 workers:
    the reaper's UPDATE is idempotent (status guard) and auto-resume uses an
    atomic claim (see shared/auto_resume.py).
    """
    while True:
        try:
            from scraper import routes as scraper_routes
            from shared.auto_resume import maybe_auto_resume_scraper_jobs

            scraper_routes.cleanup_stale_jobs()
            await maybe_auto_resume_scraper_jobs()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Scraper guard tick failed (non-fatal): %s", exc)
        await asyncio.sleep(interval_seconds)
