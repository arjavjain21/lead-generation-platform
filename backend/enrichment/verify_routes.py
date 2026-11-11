"""Verify-Emails API surface (PR-1.1, 2026-11-11).

Three entry points, all under the enrichment router's auth
(JWT or X-API-Key), all read-only w.r.t. the contacts DB:

- POST /api/enrichment/verify          — inline for ≤25 emails, background
  job above (job_type='enrichment', source_type='verify'; SSE/progress/
  cancel/download all work through the generic jobs surface).
- POST /api/enrichment/verify/from-job/{job_id} — chunk selection from an
  enrichment job's output CSV (dm_email + optional retained catch-alls +
  company_email), deduped, limit/offset.
- GET  /api/enrichment/verify/results/{job_id} — summary rollup for the UI
  (also embedded in the generic job payload by PR-1.4).

CSV columns: original_email, email_norm, classification, verifier, code,
message, verified_at, cache_hit, source_job_id.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from shared import auth
from . import job_store
from . import verify_flow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/enrichment")

def _output_dir() -> Path:
    from . import routes as _routes  # lazy: avoid import cycle
    return Path(_routes.OUTPUT_DIR)


class VerifyRequest(BaseModel):
    emails: Optional[list[str]] = Field(default=None, max_length=verify_flow.BATCH_MAX_EMAILS)
    allow_catch_all: bool = False  # sendability policy only — never hides rows
    recheck: bool = False  # bypass the raw-facts cache (still stores fresh results)
    job_name: Optional[str] = None


@router.post("/verify")
async def verify_emails_endpoint(
    req: VerifyRequest,
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    emails_raw = [e for e in (req.emails or []) if str(e or "").strip()]
    if not emails_raw:
        raise HTTPException(status_code=400, detail="No usable email addresses supplied.")
    if len(emails_raw) > verify_flow.BATCH_MAX_EMAILS:
        raise HTTPException(
            status_code=422,
            detail=f"Maximum {verify_flow.BATCH_MAX_EMAILS} emails per batch.",
        )
    pairs, dropped = verify_flow.dedupe_emails(emails_raw)

    if len(pairs) <= verify_flow.SYNC_MAX_EMAILS:
        conn = job_store.get_store().conn
        rows: list[dict[str, Any]] = []

        def _collect(original, email_norm, classification, verifier, code,
                     message, verified_at, cache_hit):
            rows.append({
                "original_email": original, "email_norm": email_norm,
                "classification": classification, "verifier": verifier,
                "code": code, "message": (message or "")[:200],
                "verified_at": verified_at, "cache_hit": cache_hit,
            })

        summary = await verify_flow.verify_batch(
            pairs, allow_catch_all=req.allow_catch_all, recheck=req.recheck,
            conn=conn, on_result=_collect,
        )
        return {
            "mode": "inline",
            "invalid_inputs_dropped": dropped,
            "summary": summary,
            "results": rows,
        }

    return await _start_verify_job(
        pairs, dropped, req.allow_catch_all, req.recheck,
        req.job_name, current_user, source_job_id="",
    )


@router.post("/verify/from-job/{job_id}")
async def verify_from_job(
    job_id: str,
    limit: int = 2000,
    offset: int = 0,
    include_catch_all: bool = True,
    allow_catch_all: bool = False,
    recheck: bool = False,
    only_with_email: bool = True,
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    store = job_store.get_store()
    job = store.get_job(job_id)
    if not job or job.get("job_type") != "enrichment":
        raise HTTPException(status_code=404, detail="Job not found.")
    job_csv = _output_dir() / f"{job_id}.csv"
    partial = _output_dir() / f"{job_id}_partial.csv"
    path = job_csv if job_csv.exists() else (partial if partial.exists() else None)
    if path is None:
        raise HTTPException(status_code=409, detail="Job has no downloadable output yet.")

    emails = verify_flow.extract_job_emails(path, limit, offset, only_with_email, include_catch_all)
    if not emails:
        raise HTTPException(status_code=400, detail="No emails found in the requested chunk.")
    pairs, dropped = verify_flow.dedupe_emails(emails)
    return await _start_verify_job(
        pairs, dropped, allow_catch_all, recheck, None, current_user,
        source_job_id=job_id,
    )


@router.get("/verify/results/{job_id}")
async def verify_results(
    job_id: str,
    current_user: dict = Depends(auth.get_current_user_with_api_key),
):
    store = job_store.get_store()
    job = store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"job_id": job_id, "status": job.get("status"), "summary": _summary_from_job_row(store, job)}


# ---------------------------------------------------------------------------
# Background job plumbing (generic jobs surface: SSE / cancel / download)
# ---------------------------------------------------------------------------

async def _start_verify_job(
    pairs: list[tuple[str, str]],
    dropped: int,
    allow_catch_all: bool,
    recheck: bool,
    job_name: Optional[str],
    current_user: dict,
    source_job_id: str,
):
    from . import routes as enrichment_routes  # lazy: avoid import cycle

    store = job_store.get_store()
    job_id = _new_id()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    total = len(pairs)
    store.create_enrichment_job(
        job_id=job_id,
        user_id=current_user["user_id"],
        total=total,
        filename=f"verify_emails_{stamp}_{job_id[:8]}.csv",
        domain_col="email",
        original_filename=f"verify_emails_{stamp}_{job_id[:8]}.csv",
        parent_job_id=source_job_id or None,
        source_type="verify",
        display_name=(job_name or "Verify emails")[:120],
    )

    enrichment_routes._job_signals[job_id] = asyncio.Event()
    enrichment_routes._active_jobs.add(job_id)
    task = asyncio.create_task(
        _run_verify_job(
            job_id=job_id,
            pairs=pairs,
            allow_catch_all=allow_catch_all,
            recheck=recheck,
            source_job_id=source_job_id,
        )
    )
    _VERIFY_TASKS.add(task)
    task.add_done_callback(_VERIFY_TASKS.discard)
    return {
        "mode": "job",
        "job_id": job_id,
        "total": total,
        "invalid_inputs_dropped": dropped,
        "stream": f"/api/enrichment/jobs/{job_id}/stream",
    }


_VERIFY_TASKS: set[asyncio.Task] = set()


def _new_id() -> str:
    import uuid

    return str(uuid.uuid4())


def _summary_from_job_row(store: Any, job: dict) -> dict:
    """Rebuild the summary from the job's latest verify progress event (the
    runner appends cumulative counters with every result + a verify_done)."""
    fallback = {"total": int(job.get("total") or 0),
                "processed": int(job.get("processed") or 0)}
    try:
        events = store.get_events_from(job["job_id"], 0)
    except Exception:
        return fallback
    payload = None
    for body in events:  # get_events_from returns decoded payload dicts
        if isinstance(body, dict) and body.get("type") in ("verify_result", "verify_done"):
            payload = body
    if payload is None:
        return fallback
    return {k: payload.get(k, 0) for k in (
        "total", "processed", "verified", "catch_all", "invalid", "unknown",
        "cache_hits", "unavailable", "validated", "sendable")} | {"done": payload.get("type") == "verify_done"}


async def _run_verify_job(
    job_id: str,
    pairs: list[tuple[str, str]],
    allow_catch_all: bool,
    recheck: bool,
    source_job_id: str,
):
    """Mirror of the enrichment runner skeleton: running state, 30s
    heartbeat, incremental CSV, progress events, cancel -> partial."""
    from . import routes as enrichment_routes

    store = job_store.get_store()
    out_path = _output_dir() / f"{job_id}.csv"
    conn = store.conn

    store.set_running(job_id)
    counters = {"verified": 0, "catch_all": 0, "invalid": 0, "unknown": 0,
                "cache_hits": 0, "processed": 0, "unavailable": 0}
    seq = [0]

    def _append_event(payload: dict) -> None:
        try:
            store.append_event(job_id, seq[0], payload)
            seq[0] += 1
            sig = enrichment_routes._job_signals.get(job_id)
            if sig:
                sig.set()
                sig.clear()
        except Exception as ev_err:  # never break the run for an event
            logger.warning("verify job %s event append failed: %s", job_id, ev_err)

    def _check_cancelled() -> bool:
        return enrichment_routes._cancelled_jobs and job_id in enrichment_routes._cancelled_jobs

    fh = open(out_path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=verify_flow.VERIFY_CSV_COLUMNS, restval="")
    writer.writeheader()
    try:
        def on_result(original, email_norm, classification, verifier, code,
                      message, verified_at, cache_hit):
            writer.writerow({
                "original_email": original,
                "email_norm": email_norm,
                "classification": classification,
                "verifier": verifier,
                "code": code,
                "message": (message or "")[:200],
                "verified_at": verified_at,
                "cache_hit": "true" if cache_hit else "false",
                "source_job_id": source_job_id,
            })
            fh.flush()
            counters[classification] = counters.get(classification, 0) + 1
            counters["processed"] += 1
            if cache_hit:
                counters["cache_hits"] += 1
            _append_event({
                "type": "verify_result",
                "index": counters["processed"] - 1,  # row event (D-fix counter rule)
                "processed": counters["processed"],
                "total": len(pairs),
                **counters,
            })

        # Heartbeat wrapper: the batch engine pings the store between waves.
        async def _heartbeat_loop():
            while True:
                await asyncio.sleep(30)
                try:
                    store.set_heartbeat(job_id)
                except Exception:
                    pass

        hb = asyncio.create_task(_heartbeat_loop())
        try:
            summary = await verify_flow.verify_batch(
                pairs,
                allow_catch_all=allow_catch_all,
                recheck=recheck,
                conn=conn,
                on_result=on_result,
                check_cancelled=_check_cancelled,
                source_job_id=source_job_id,
            )
        finally:
            hb.cancel()
            try:
                store.set_heartbeat(job_id)
            except Exception:
                pass

        cancelled = _check_cancelled()
        fh.close()
        fh = None
        if cancelled and counters["processed"] < len(pairs):
            partial = _output_dir() / f"{job_id}_partial.csv"
            os.replace(out_path, partial)
            store.set_partial(job_id, str(partial))
        else:
            store.set_done(job_id, str(out_path))
        _append_event({"type": "verify_done", **counters,
                       "validated": counters["verified"] + counters["catch_all"] + counters["invalid"],
                       "sendable": counters["verified"] + (counters["catch_all"] if allow_catch_all else 0)})
    except Exception as exc:
        logger.exception("verify job %s failed: %s", job_id, exc)
        if fh is not None:
            fh.close()
        store.set_failed(job_id, str(exc))
    finally:
        enrichment_routes._job_signals.pop(job_id, None)
        enrichment_routes._active_jobs.discard(job_id)
