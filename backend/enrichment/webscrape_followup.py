"""Website-email followup: waterfall misses → webscrapedash scrape-emails API.

When a domain-based enrichment job finishes, every input domain that ended with
NO email from the waterfall is submitted (auto for eligible jobs, manual
backfill button for historical ones) to the scraper VPS's scrape-emails API.
That pipeline visits each site and extracts generic business emails
(info@, contact@, …). When the batch completes, a second CSV is built, the
job card gains a second download button, and email/Slack notifications fire.

Design pins (enrichment/tests/test_webscrape_followup.py):
* NEVER slows or breaks the waterfall: the auto hook is fire-and-forget with a
  blanket try/except; every entry point here is best-effort and logged.
* One followup per enrichment job, enforced by UNIQUE(parent_job_id).
* The miss domain list is snapshotted into ``domains_json`` at creation so the
  followup survives the parent CSV being cleaned up (30-day retention).
* Deterministic Idempotency-Keys (derived from parent job id + chunk index)
  make submit retries safe — their API replays to the same batch.
* Poller: single-owner across the 4 gunicorn workers via a DB lease row
  (webscrape_followup_poller); a dead holder's lease expires in LEASE_S.
* Completion is idempotent: the submitted/processing → finalizing transition
  is an atomic claim, so webhook push and poller tick can both fire safely.
* Notifications reuse enrichment.routes.send_job_notification (late import —
  routes imports this module) with recipients = global inbox + Slack relay +
  the job owner's account email.

Kill-switch: WEBCSCRAPE_FOLLOWUP_ENABLED=false disables auto-submit, the
poller, and the webhook; the manual backfill endpoint returns 503.

Schema (idempotent CREATE TABLE IF NOT EXISTS at first use, mirroring the
blitz_miss_store / call_tracker pattern; thread-local conns via shared.db):

    webscrape_followups(followup_id PK, parent_job_id UNIQUE, status, origin,
        created_by, domains_json, batches_json, domains_total, processed,
        emails_found, no_email_count, eta_min, csv_path, error,
        submit_attempts, created_at, updated_at, completed_at)
    webscrape_followup_poller(id=1, holder, expires_at)
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from enrichment.identifier_utils import normalize_domain
from enrichment import webscrape_dash_client as dash
from shared import db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_POLL_INTERVAL_S = 30
DEFAULT_LEASE_S = 90
#: Re-submit a pending_submit followup at most this often (unhappiness backoff).
SUBMIT_RETRY_MIN_S = 120
#: Consecutive submit failures before giving up entirely.
MAX_SUBMIT_ATTEMPTS = 20
#: A finalizing row older than this is considered crashed mid-finalize and is
#: re-claimed by the poller.
FINALIZING_STALE_MIN_S = 300
#: Chunking: their per-request hard cap.
CHUNK_SIZE = dash.MAX_WEBSITES_PER_BATCH
#: Sleep between chunk submits so we stay under their 20K domains/min key limit.
INTER_CHUNK_SLEEP_S = 31.0

#: row_status values that must NOT count as a miss (not definitive outcomes).
_NON_MISS_STATUSES = {"skipped", "skipped_no_domain", "error"}
#: Any email-bearing column — a row with any of these filled is NOT a miss.
_EMAIL_COLUMNS = ("dm_email", "company_email", "final_email")

STATUS_PENDING_SUBMIT = "pending_submit"
STATUS_SUBMITTED = "submitted"
STATUS_PROCESSING = "processing"
STATUS_FINALIZING = "finalizing"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

_ACTIVE_STATUSES = (STATUS_PENDING_SUBMIT, STATUS_SUBMITTED, STATUS_PROCESSING)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS webscrape_followups (
    followup_id    TEXT PRIMARY KEY,
    parent_job_id  TEXT NOT NULL UNIQUE,
    status         TEXT NOT NULL DEFAULT 'pending_submit',
    origin         TEXT NOT NULL DEFAULT 'auto',
    created_by     TEXT DEFAULT '',
    domains_json   TEXT NOT NULL DEFAULT '[]',
    batches_json   TEXT NOT NULL DEFAULT '[]',
    domains_total  INTEGER NOT NULL DEFAULT 0,
    processed      INTEGER NOT NULL DEFAULT 0,
    emails_found   INTEGER NOT NULL DEFAULT 0,
    no_email_count INTEGER NOT NULL DEFAULT 0,
    eta_min        INTEGER,
    csv_path       TEXT DEFAULT '',
    error          TEXT DEFAULT '',
    submit_attempts INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    completed_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_webscrape_followups_status
    ON webscrape_followups(status);
CREATE TABLE IF NOT EXISTS webscrape_followup_poller (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    holder TEXT,
    expires_at TEXT
);
"""

_schema_initialized = False

DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "outputs"


def _ensure_schema() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    conn = db.get_db()
    conn.executescript(_SCHEMA)
    conn.execute("INSERT OR IGNORE INTO webscrape_followup_poller (id) VALUES (1)")
    conn.commit()
    _schema_initialized = True


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _age_s(ts: str) -> float:
    try:
        parsed = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return 0.0
    return max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds())


def followup_enabled() -> bool:
    return os.getenv("WEBCSCRAPE_FOLLOWUP_ENABLED", "true").strip().lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Miss extraction
# ---------------------------------------------------------------------------


def extract_miss_domains(csv_path: Path) -> list[str]:
    """Distinct bare domains from an enrichment results CSV whose rows ended
    with no email in any waterfall column. Order preserved (submission order);
    invalid/noise domains dropped via normalize_domain."""
    misses: list[str] = []
    seen: set[str] = set()
    try:
        with open(csv_path, newline="", encoding="utf-8", errors="replace") as fh:
            for row in csv.DictReader(fh):
                status = (row.get("row_status") or "").strip()
                if status in _NON_MISS_STATUSES:
                    continue
                if any((row.get(col) or "").strip() for col in _EMAIL_COLUMNS):
                    continue
                domain = normalize_domain(row.get("input_domain"))
                if not domain or "." not in domain or domain in seen:
                    continue
                seen.add(domain)
                misses.append(domain)
    except OSError as exc:
        logger.warning("followup miss extraction failed for %s: %s", csv_path, exc)
        return []
    return misses


# ---------------------------------------------------------------------------
# Store helpers
# ---------------------------------------------------------------------------


def get_followup(parent_job_id: str) -> Optional[dict[str, Any]]:
    _ensure_schema()
    conn = db.get_db()
    row = conn.execute(
        "SELECT * FROM webscrape_followups WHERE parent_job_id=?", (parent_job_id,)
    ).fetchone()
    return dict(row) if row else None


def summaries_for_jobs(parent_job_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Batched summary for the jobs-list endpoint. Never raises."""
    if not parent_job_ids:
        return {}
    try:
        _ensure_schema()
        conn = db.get_db()
        placeholders = ",".join("?" * len(parent_job_ids))
        rows = conn.execute(
            f"SELECT parent_job_id, status, origin, domains_total, processed, "
            f"emails_found, no_email_count, eta_min, csv_path, error, created_at, "
            f"completed_at FROM webscrape_followups WHERE parent_job_id IN ({placeholders})",
            parent_job_ids,
        ).fetchall()
        return {r["parent_job_id"]: dict(r) for r in rows}
    except Exception as exc:  # noqa: BLE001 — augmentation must never break listing
        logger.warning("followup summaries failed: %s", exc)
        return {}


def _update(parent_job_id: str, **cols: Any) -> None:
    _ensure_schema()
    conn = db.get_db()
    sets = ", ".join(f"{k}=?" for k in cols)
    conn.execute(
        f"UPDATE webscrape_followups SET {sets}, updated_at=? WHERE parent_job_id=?",
        (*cols.values(), _now_iso(), parent_job_id),
    )
    conn.commit()


def _owner_email(parent_job_id: str) -> Optional[str]:
    """Account email of the enrichment job's owner (users table)."""
    try:
        from enrichment import job_store

        job = job_store.get_store().get_job(parent_job_id)
        user_id = (job or {}).get("user_id")
        if not user_id:
            return None
        row = db.get_db().execute(
            "SELECT email FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        return row["email"] if row else None
    except Exception as exc:  # noqa: BLE001 — notify must be best-effort
        logger.warning("followup owner-email lookup failed for %s: %s", parent_job_id, exc)
        return None


# ---------------------------------------------------------------------------
# Submission
# ---------------------------------------------------------------------------


def _chunk(domains: list[str]) -> list[list[str]]:
    return [domains[i : i + CHUNK_SIZE] for i in range(0, len(domains), CHUNK_SIZE)]


async def _submit_all(parent_job_id: str, domains: list[str]) -> list[dict[str, Any]]:
    """Submit every chunk; returns per-batch dicts. Raises on hard failure."""
    callback_url = _callback_url()
    batches: list[dict[str, Any]] = []
    client = dash.WebscrapeDashClient()
    for idx, chunk in enumerate(_chunk(domains)):
        if idx:
            await asyncio.sleep(INTER_CHUNK_SLEEP_S)
        response = await client.submit(
            chunk,
            client_tag=f"lgp:{parent_job_id[:8]}",
            idempotency_key=f"lgp-followup-{parent_job_id}-{idx}",
            callback_url=callback_url,
        )
        batches.append(
            {
                "job_id": response.get("job_id"),
                "total": int(response.get("total") or len(chunk)),
                "new": int(response.get("new") or 0),
                "requeued": int(response.get("requeued") or 0),
                "already_done": int(response.get("already_done") or 0),
                "invalid": int(response.get("invalid") or 0),
            }
        )
    return batches


def _callback_url() -> Optional[str]:
    """Batch-completion webhook target — only when the signing secret is set
    (poller-only mode otherwise)."""
    if not os.getenv("WEBSCRAPER_WEBHOOK_SECRET", "").strip():
        return None
    base = os.getenv("WEBCSCRAPE_CALLBACK_BASE", "https://listbuilding.eagleinfoservice.com")
    return f"{base.rstrip('/')}/api/enrichment/webscrape-followup/webhook"


async def estimate_for_job(csv_path: Path) -> dict[str, Any]:
    """Dry-run quote for the backfill dialog. Free; queues nothing."""
    domains = extract_miss_domains(csv_path)
    if not domains:
        return {"misses": 0}
    client = dash.WebscrapeDashClient()
    totals = {"new": 0, "requeued": 0, "already_done": 0, "invalid": 0, "billable": 0}
    eta_min = 0
    for chunk in _chunk(domains):
        quote = await client.estimate(chunk, client_tag="lgp-estimate")
        for key in totals:
            totals[key] += int(quote.get(key) or 0)
        eta_min = max(eta_min, int(quote.get("eta_min") or 0))
    return {"misses": len(domains), **totals, "eta_min": eta_min}


async def create_followup(
    parent_job_id: str, csv_path: Path, *, origin: str, created_by: str = ""
) -> dict[str, Any]:
    """Create (and try to submit) the followup for a job. Idempotent on
    parent_job_id — an existing row is returned with created=False."""
    _ensure_schema()
    existing = get_followup(parent_job_id)
    if existing:
        # Revive a terminal-but-unproductive followup so the UI Retry button
        # and a post-cancel change of heart both work; any other existing row
        # (active or done) is returned as-is.
        if existing["status"] in (STATUS_CANCELLED, STATUS_FAILED):
            _update(
                parent_job_id,
                status=STATUS_PENDING_SUBMIT,
                batches_json="[]",
                submit_attempts=0,
                error="",
                eta_min=None,
                completed_at=None,
            )
            await try_submit(parent_job_id)
            return {"created": True, "revived": True, "followup": _public_summary(get_followup(parent_job_id) or {})}
        return {"created": False, "followup": _public_summary(existing)}
    domains = extract_miss_domains(csv_path)
    if not domains:
        return {"created": False, "reason": "no_misses"}

    conn = db.get_db()
    now = _now_iso()
    try:
        conn.execute(
            "INSERT INTO webscrape_followups (followup_id, parent_job_id, status, origin,"
            " created_by, domains_json, domains_total, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                f"wf_{uuid.uuid4().hex[:12]}",
                parent_job_id,
                STATUS_PENDING_SUBMIT,
                origin,
                created_by,
                json.dumps(domains),
                len(domains),
                now,
                now,
            ),
        )
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — UNIQUE race or DB hiccup
        logger.warning("followup insert raced for %s: %s", parent_job_id, exc)
        return {"created": False, "followup": _public_summary(get_followup(parent_job_id) or {})}

    await try_submit(parent_job_id)
    return {"created": True, "followup": _public_summary(get_followup(parent_job_id) or {})}


async def try_submit(parent_job_id: str) -> bool:
    """Best-effort submit of a pending_submit row. True when submitted."""
    row = get_followup(parent_job_id)
    if not row or row["status"] != STATUS_PENDING_SUBMIT:
        return False
    attempts = int(row.get("submit_attempts") or 0)
    if attempts >= MAX_SUBMIT_ATTEMPTS:
        _update(parent_job_id, status=STATUS_FAILED, error="submit attempts exhausted")
        return False
    domains = json.loads(row.get("domains_json") or "[]")
    try:
        batches = await _submit_all(parent_job_id, domains)
        _update(
            parent_job_id,
            status=STATUS_SUBMITTED,
            batches_json=json.dumps(batches),
            submit_attempts=attempts + 1,
            error="",
        )
        logger.info(
            "followup submitted for %s: %d domain(s) in %d batch(es)",
            parent_job_id,
            len(domains),
            len(batches),
        )
        return True
    except dash.WebscrapeDashAuthError as exc:
        _update(parent_job_id, submit_attempts=attempts + 1, error=str(exc)[:500])
        logger.error("followup submit auth failure for %s: %s", parent_job_id, exc)
        return False
    except dash.WebscrapeDashError as exc:
        retry_after = getattr(exc, "retry_after_s", None)
        _update(parent_job_id, submit_attempts=attempts + 1, error=str(exc)[:500])
        logger.warning(
            "followup submit failed for %s (attempt %d, retry_after=%s): %s",
            parent_job_id,
            attempts + 1,
            retry_after,
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# Polling / finalization
# ---------------------------------------------------------------------------


async def _advance_followup(row: dict[str, Any]) -> None:
    """Move one followup toward done. All failures are logged, never raised."""
    parent_job_id = row["parent_job_id"]
    status = row["status"]
    try:
        if status == STATUS_PENDING_SUBMIT:
            if _age_s(row["updated_at"]) >= SUBMIT_RETRY_MIN_S:
                await try_submit(parent_job_id)
            return
        if status in (STATUS_SUBMITTED, STATUS_PROCESSING):
            await _poll_and_maybe_finalize(row)
            return
        if status == STATUS_FINALIZING and _age_s(row["updated_at"]) >= FINALIZING_STALE_MIN_S:
            await _finalize(parent_job_id)
    except Exception as exc:  # noqa: BLE001 — a bad row must not kill the tick
        logger.error("followup advance failed for %s: %s", parent_job_id, exc)
        _update(parent_job_id, error=str(exc)[:500])


async def _poll_and_maybe_finalize(row: dict[str, Any]) -> None:
    parent_job_id = row["parent_job_id"]
    batches = json.loads(row.get("batches_json") or "[]")
    client = dash.WebscrapeDashClient()
    processed = emails_found = done_batches = 0
    eta_min: Optional[int] = None
    for batch in batches:
        job_id = batch.get("job_id")
        if not job_id:
            continue
        # limit=1: we only need the status envelope here — full result rows
        # are paged once at finalize time (no_email counts come from there).
        status_doc = await client.get_job(job_id, limit=1)
        processed += int(status_doc.get("processed") or 0)
        emails_found += int(status_doc.get("emails_found") or 0)
        batch_eta = status_doc.get("eta_min")
        eta_min = max(eta_min or 0, int(batch_eta)) if batch_eta is not None else eta_min
        if status_doc.get("done"):
            done_batches += 1
    all_done = bool(batches) and done_batches == len(batches)
    # no_email_count deliberately NOT touched here — it is computed exactly
    # once at finalize from the full paged result set.
    if not all_done:
        _update(
            parent_job_id,
            status=STATUS_PROCESSING,
            processed=processed,
            emails_found=emails_found,
            eta_min=eta_min,
        )
        return
    _update(parent_job_id, processed=processed, emails_found=emails_found, eta_min=eta_min)
    await _finalize(parent_job_id)


def _claim_finalizing(parent_job_id: str) -> bool:
    """Atomic submitted/processing → finalizing claim (webhook + poller race)."""
    _ensure_schema()
    conn = db.get_db()
    cursor = conn.execute(
        "UPDATE webscrape_followups SET status=?, updated_at=? "
        "WHERE parent_job_id=? AND status IN (?,?)",
        (STATUS_FINALIZING, _now_iso(), parent_job_id, STATUS_SUBMITTED, STATUS_PROCESSING),
    )
    conn.commit()
    return cursor.rowcount > 0


async def _finalize(parent_job_id: str) -> None:
    """Collect all batch results, build the CSV, mark done, notify.
    Idempotent via the finalizing claim (or re-claim when stale)."""
    row = get_followup(parent_job_id)
    if not row:
        return
    if row["status"] not in (STATUS_FINALIZING, STATUS_SUBMITTED, STATUS_PROCESSING):
        return
    if row["status"] != STATUS_FINALIZING and not _claim_finalizing(parent_job_id):
        return
    if row["status"] == STATUS_FINALIZING and not _claim_stale_finalizing(parent_job_id):
        return

    batches = json.loads(row.get("batches_json") or "[]")
    client = dash.WebscrapeDashClient()
    results: list[dict[str, Any]] = []
    for batch in batches:
        job_id = batch.get("job_id")
        if job_id:
            results.extend(await client.collect_results(job_id))
    csv_path = _write_results_csv(parent_job_id, results)
    emails_found = sum(1 for r in results if r.get("found"))
    no_email = sum(1 for r in results if not r.get("found"))
    _update(
        parent_job_id,
        status=STATUS_DONE,
        csv_path=str(csv_path),
        emails_found=emails_found,
        no_email_count=no_email,
        processed=len(results),
        eta_min=None,
        completed_at=_now_iso(),
        error="",
    )
    logger.info(
        "followup done for %s: %d email(s) / %d result(s) -> %s",
        parent_job_id,
        emails_found,
        len(results),
        csv_path,
    )
    await _notify_done(parent_job_id, emails_found, len(results))


def _claim_stale_finalizing(parent_job_id: str) -> bool:
    """Re-claim a finalizing row only when stale (crashed mid-finalize)."""
    conn = db.get_db()
    cursor = conn.execute(
        "UPDATE webscrape_followups SET updated_at=? WHERE parent_job_id=? "
        "AND status=? AND updated_at < ?",
        (
            _now_iso(),
            parent_job_id,
            STATUS_FINALIZING,
            _iso_minus_seconds(FINALIZING_STALE_MIN_S),
        ),
    )
    conn.commit()
    return cursor.rowcount > 0


def _iso_minus_seconds(seconds: float) -> str:
    moment = datetime.now(timezone.utc).timestamp() - seconds
    return datetime.fromtimestamp(moment, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"


def _write_results_csv(parent_job_id: str, results: list[dict[str, Any]]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / f"{parent_job_id}_website_emails.csv"
    fields = [
        "domain",
        "email",
        "found",
        "email_type",
        "email_source",
        "business_name",
        "email_confidence",
        "result_status",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "domain": r.get("website", ""),
                    "email": r.get("email") or "",
                    "found": "true" if r.get("found") else "false",
                    "email_type": r.get("email_type") or "",
                    "email_source": r.get("email_source") or "",
                    "business_name": r.get("business_name") or "",
                    "email_confidence": r.get("confidence") if r.get("confidence") is not None else "",
                    "result_status": r.get("status") or "",
                }
            )
    return csv_path


async def _notify_done(parent_job_id: str, emails_found: int, total: int) -> None:
    try:
        from enrichment.routes import get_notification_recipients, send_job_notification

        recipients = list(dict.fromkeys(get_notification_recipients()))
        owner = _owner_email(parent_job_id)
        if owner:
            recipients = list(dict.fromkeys([*recipients, owner]))
        if not recipients:
            return
        await send_job_notification(
            recipients=recipients,
            job_type="website_email_scrape",
            filename=f"website_emails_{parent_job_id[:8]}.csv",
            status="done",
            total=total,
            processed=total,
            emails_found=emails_found,
        )
    except Exception as exc:  # noqa: BLE001 — notify must never fail finalize
        logger.warning("followup notification failed for %s: %s", parent_job_id, exc)


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


async def cancel_followup(parent_job_id: str) -> dict[str, Any]:
    row = get_followup(parent_job_id)
    if not row:
        return {"cancelled": False, "reason": "not_found"}
    if row["status"] in (STATUS_DONE, STATUS_CANCELLED, STATUS_FAILED):
        return {"cancelled": False, "reason": f"already_{row['status']}"}
    cancelled_remote = 0
    try:
        client = dash.WebscrapeDashClient()
        for batch in json.loads(row.get("batches_json") or "[]"):
            job_id = batch.get("job_id")
            if job_id:
                await client.cancel(job_id)
                cancelled_remote += 1
    except dash.WebscrapeDashError as exc:
        logger.warning("followup remote cancel failed for %s: %s", parent_job_id, exc)
    _update(parent_job_id, status=STATUS_CANCELLED, completed_at=_now_iso())
    return {"cancelled": True, "remote_batches_cancelled": cancelled_remote}


# ---------------------------------------------------------------------------
# Poller (lease-guarded; one active holder across all workers)
# ---------------------------------------------------------------------------


def _acquire_lease(worker_id: str, lease_s: int = DEFAULT_LEASE_S) -> bool:
    _ensure_schema()
    conn = db.get_db()
    now = datetime.now(timezone.utc)
    expires = datetime.fromtimestamp(now.timestamp() + lease_s, tz=timezone.utc)
    cursor = conn.execute(
        "UPDATE webscrape_followup_poller SET holder=?, expires_at=? "
        "WHERE id=1 AND (expires_at IS NULL OR expires_at < ?)",
        (
            worker_id,
            expires.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        ),
    )
    conn.commit()
    return cursor.rowcount > 0


async def poll_tick(worker_id: str) -> int:
    """One poller pass over active followups. Returns rows advanced."""
    if not followup_enabled() or not dash.is_configured():
        return 0
    if not _acquire_lease(worker_id):
        return 0
    _ensure_schema()
    poll_statuses = (*_ACTIVE_STATUSES, STATUS_FINALIZING)
    placeholders = ",".join("?" * len(poll_statuses))
    rows = db.get_db().execute(
        f"SELECT * FROM webscrape_followups WHERE status IN ({placeholders})",
        poll_statuses,
    ).fetchall()
    advanced = 0
    for row in rows:
        await _advance_followup(dict(row))
        advanced += 1
    return advanced


async def followup_poller_loop(interval_s: int = DEFAULT_POLL_INTERVAL_S) -> None:
    """Lifespan loop — see main.py registration. Never raises."""
    worker_id = f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
    logger.info("website-email followup poller started (%s)", worker_id)
    while True:
        try:
            await poll_tick(worker_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — loop must survive anything
            logger.error("followup poll tick failed: %s", exc)
        await asyncio.sleep(interval_s)


# ---------------------------------------------------------------------------
# Auto-submit hook (called from the enrichment runner's done path)
# ---------------------------------------------------------------------------


async def auto_submit_for_job(job_id: str, csv_path: Path) -> None:
    """Fire-and-forget auto-submit after a domain job completes. Never raises —
    a followup outage must not affect the enrichment job itself."""
    try:
        if not followup_enabled() or not dash.is_configured():
            return
        from enrichment import job_store

        job = job_store.get_store().get_job(job_id)
        if not job or job.get("job_type") != "enrichment":
            return
        if job.get("status") != "done":
            return
        if job.get("website_only"):
            return  # locked UX: website-only mode promises zero fresh scraping
        if (job.get("selected_providers") or "").strip() not in ("", "[]"):
            return  # restricted cascades produce misleading misses
        if get_followup(job_id):
            return
        if not Path(csv_path).exists():
            return
        result = await create_followup(
            job_id, Path(csv_path), origin="auto", created_by=job.get("user_id") or ""
        )
        logger.info("followup auto-submit for %s: %s", job_id, result.get("created"))
    except Exception as exc:  # noqa: BLE001 — see docstring
        logger.error("followup auto-submit failed for %s: %s", job_id, exc)


# ---------------------------------------------------------------------------
# Webhook (batch.completed push — poller is the backstop when secret unset)
# ---------------------------------------------------------------------------


def webhook_secret_configured() -> bool:
    return bool(os.getenv("WEBSCRAPER_WEBHOOK_SECRET", "").strip())


def verify_webhook_signature(raw_body: bytes, signature: str) -> bool:
    secret = os.getenv("WEBSCRAPER_WEBHOOK_SECRET", "").strip()
    if not secret or not signature:
        return False
    expected = "sha256=" + hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


async def handle_batch_webhook(raw_body: bytes, signature: str) -> dict[str, Any]:
    """Verify + react to a batch.completed push. Response is log-friendly."""
    if not followup_enabled():
        return {"ok": False, "reason": "disabled"}
    if not webhook_secret_configured():
        return {"ok": False, "reason": "webhook secret not configured"}
    if not verify_webhook_signature(raw_body, signature):
        return {"ok": False, "reason": "bad signature"}
    try:
        payload = json.loads(raw_body)
    except ValueError:
        return {"ok": False, "reason": "bad json"}
    batch_job_id = payload.get("job_id")
    if not batch_job_id:
        return {"ok": False, "reason": "no job_id"}
    _ensure_schema()
    rows = db.get_db().execute(
        f"SELECT * FROM webscrape_followups WHERE status IN (?,?)",
        (STATUS_SUBMITTED, STATUS_PROCESSING),
    ).fetchall()
    for row in (dict(r) for r in rows):
        batches = json.loads(row.get("batches_json") or "[]")
        if any(b.get("job_id") == batch_job_id for b in batches):
            asyncio.create_task(_advance_followup(dict(row)))
            return {"ok": True, "parent_job_id": row["parent_job_id"]}
    return {"ok": False, "reason": "no matching followup"}


# ---------------------------------------------------------------------------
# Public shaping
# ---------------------------------------------------------------------------


def _public_summary(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "parent_job_id",
        "status",
        "origin",
        "domains_total",
        "processed",
        "emails_found",
        "no_email_count",
        "eta_min",
        "csv_path",
        "error",
        "created_at",
        "completed_at",
    )
    return {k: row.get(k) for k in keys if k in row}
