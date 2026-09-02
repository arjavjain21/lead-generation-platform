"""
Scraper runner service entrypoint (P1, 2026-09-02).

WHY THIS EXISTS — the Sep-1 RCA: scraper jobs lived inside gunicorn web
workers. Those workers are routinely recycled (--max-requests) and
occasionally murdered (WORKER TIMEOUT notify-starvation), and every death
killed every in-flight job hosted by that worker. Auto-resume kept rescuing
them, but an evening of 3-10-minute recycles burned the 2-attempt budget and
chains landed 'abandoned'. Recovery machinery worked; the deaths were the
disease.

This process is the cure: a dedicated job runner that serves NO HTTP. It
owns the scraper dispatcher (queued->running claims) and the runtime guard
(reap stale + auto-resume). Web workers keep handling API/SSE/UI traffic —
and only that — so their recycles and murders can no longer touch running
jobs. Enrichment/phone runners stay in the web workers for now (their jobs
are shorter and already auto-resume; moving them is a later, separate step).

Design constraints honored:
- Single runner process: no claim races, no dispatcher duplication.
  (If HA is ever needed, the BEGIN IMMEDIATE claims already serialize.)
- ENABLE_SCRAPER_DISPATCHER env gates the dispatcher in BOTH processes:
  web workers set 'false' (they must not steal jobs), the runner sets
  'true'. Default when unset: 'true' — preserving the pre-P1 single-process
  behavior for local dev and any deployment that hasn't adopted the runner.
- The runner exits non-zero on fatal startup failure (Restart=always pulls
  it back up); every loop inside is best-effort and never exits the process.
- No new DB schema: everything is jobs-table status transitions that the
  existing dispatcher/guard/auto-resume code already performs.

Run:  python runner_main.py   (via lead-gen-scraper-runner.service)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("runner")


async def _runner_lifespan() -> None:
    """Boot sequence for the runner — mirrors main.py's, minus all HTTP/MCP.

    Order: DB init → job-state restore → stale reap (boot reaper) →
    auto-resume pass → dispatcher loop + runtime guard loop (forever).
    """
    from shared import auth, db

    auth.init_auth_db()
    db.init_db()
    logger.info("runner: databases initialized")

    # Restore enrichment cancel/active sets so is_job_cancelled() stays
    # cross-process consistent (scraper cancel checks are DB-based already).
    with contextlib.suppress(Exception):
        from enrichment import routes as enrichment_routes
        state = enrichment_routes.job_store.get_store().restore_job_state()
        enrichment_routes._cancelled_jobs.update(state.get("cancelled", set()))
        enrichment_routes._active_jobs.update(state.get("active", set()))
        logger.info(
            "runner: restored job state (%d cancelled, %d active)",
            len(state.get("cancelled", set())), len(state.get("active", set())),
        )

    # Boot reaper: mark anything left 'running' by a previous runner death.
    # Running-only, job-type-scoped — queued backlog is never reaped.
    with contextlib.suppress(Exception):
        from scraper import routes as scraper_routes
        scraper_routes.cleanup_stale_jobs()
        logger.info("runner: boot reap complete")

    # Auto-resume what the boot reaper just marked abandoned (delay lets
    # things settle first, mirroring the web-worker boot path).
    with contextlib.suppress(Exception):
        from shared.auto_resume import maybe_auto_resume_abandoned_jobs
        asyncio.create_task(maybe_auto_resume_abandoned_jobs())
        logger.info("runner: enrichment auto-resume watcher started")

    # The heart of the runner: claim queued scraper jobs and run them, plus
    # the 60s guard that reaps stale-running and feeds auto-resume.
    from scraper.dispatch import dispatch_loop, runtime_guard_loop
    from scraper.routes import _launch_claimed_job

    per_worker_cap = int(os.getenv("SCRAPER_JOBS_PER_WORKER", "2"))
    logger.info(
        "runner: dispatcher starting (per-process cap %d, platform cap %s)",
        per_worker_cap,
        os.getenv("MAX_CONCURRENT_SCRAPER_JOBS", "6"),
    )

    await asyncio.gather(
        dispatch_loop(_launch_claimed_job, per_worker_cap=per_worker_cap),
        runtime_guard_loop(),
    )


async def main() -> None:
    logger.info("runner: starting (pid %s)", os.getpid())
    stop = asyncio.Event()

    def _stop(signum, _frame):
        logger.info("runner: received signal %s — shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    runner_task = asyncio.create_task(_runner_lifespan())

    # Await either: fatal runner error, or SIGTERM/SIGINT.
    stop_task = asyncio.create_task(stop.wait())
    done, _pending = await asyncio.wait(
        {runner_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
    )

    if stop_task in done:
        logger.info("runner: stop requested — cancelling loops")
        runner_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner_task
        logger.info("runner: bye")
        return

    # runner_task finished without stop => it crashed. Log the cause, then
    # exit non-zero so systemd Restart=always re-launches a clean process.
    # (.result() would re-raise BEFORE this log line — swallow into a log
    # instead so journald shows why the runner died.)
    exc = runner_task.exception()
    if exc is not None:
        logger.error("runner: lifespan crashed — %s: %s", type(exc).__name__, exc,
                     exc_info=exc)
    else:
        logger.error("runner: lifespan ended unexpectedly (no exception) — exiting 1")
    raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
