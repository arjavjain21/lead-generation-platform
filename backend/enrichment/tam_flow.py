"""TAM (Total Addressable Market) by-People flow — job runner (new Flow 2).

Pages through Blitz ``POST /v2/company/tam-by-people`` with persona +
firmographic filters, flattens every returned company into one CSV row,
persists a live cursor for crash-resume diagnostics, and (optionally) chains
a Flow-1 domain-enrichment job over the discovered domains by reusing the
exact ``/flows/domain-enrich`` creation + execution code path.

Cost note: Blitz bills 1 FUP record per RESULT returned by tam-by-people
(an empty page costs 0), so ``max_companies`` is a hard budget ceiling —
the loop stops the moment the cap is reached, mid-page included, and never
asks for the next cursor afterwards.

Job plumbing: TAM jobs are plain ``job_type='enrichment'`` rows (never a new
job_type literal), so the generic enrichment SSE stream / jobs list /
download endpoints serve them unchanged. ``status`` transitions use the
existing literals only ('running' -> 'done' | 'partial' | 'failed').
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from enrichment import blitz_client
from enrichment import contacts_client
from enrichment import identifier_utils
from enrichment import job_store

logger = logging.getLogger(__name__)

# Output dir mirrors enrichment/routes.py's convention (backend/data/outputs).
# Duplicated here to avoid importing enrichment.routes at module top (it is
# imported lazily for the Flow-1 chain hand-off instead).
DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "outputs"

# CSV column contract for the TAM export (order is load-bearing: the
# incremental DictWriter is created from this once, per job).
TAM_CSV_COLUMNS: tuple[str, ...] = (
    "name",
    "domain",
    "website",
    "linkedin_url",
    "industry",
    "type",
    "size",
    "employees_on_linkedin",
    "followers",
    "founded_year",
    "hq_city",
    "hq_state",
    "hq_country_code",
    "hq_region",
    "revenue",
    "slogan",
    "employee_growth_1y",
    "matched_people",
)

# tam-by-people page size. One request per page, 1 FUP record per result.
TAM_PAGE_SIZE = 50

# Hard ceiling on max_companies — guards both Blitz FUP budget and job
# runtime. Requests above this are clamped here (the route model also caps).
TAM_MAX_COMPANIES = 10_000


def _env_int(name: str, default: int) -> int:
    """Read an int env var, falling back to ``default`` on absent/junk."""
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r — using default %d", name, raw, default)
        return default


# Hard page cap for the pagination loop. A cursor-cycling server bug (cursor
# never null, pages repeat forever) would otherwise loop endlessly while the
# 30 s heartbeat keeps the job 'running' and job_events grows unbounded.
# 400 pages x 50 rows = 2x the TAM_MAX_COMPANIES headroom, so it never fires
# on a legitimate run. Env-overridable (TAM_MAX_PAGES) for ops emergencies.
TAM_MAX_PAGES = _env_int("TAM_MAX_PAGES", 400)

# Break after this many CONSECUTIVE empty pages (server signalling exhaustion
# without a null cursor, or a soft failure loop). Blitz bills 0 for empty
# pages, but an unbounded request loop is still unacceptable.
TAM_EMPTY_PAGE_LIMIT = 5

# Marker stamped inside the job_state progress payload (see save_tam_progress).
TAM_PROGRESS_KIND = "tam_progress"

# Fire-and-forget chain tasks keep a strong reference here so the event
# loop's weak task refs cannot garbage-collect a live Flow-1 job mid-run.
_pending_chain_tasks: set[asyncio.Task] = set()


# ---------------------------------------------------------------------------
# Row flattening
# ---------------------------------------------------------------------------

def flatten_tam_company(entry: dict[str, Any]) -> dict[str, Any]:
    """Flatten one tam-by-people result entry into a TAM_CSV_COLUMNS row.

    ``entry`` is ``{"company": {...}, "matched_people": N}`` where company
    carries the Blitz company projection (see ``blitz_client.tam_by_people``).
    ``domain`` is NULLABLE in the API — never assume it. ``employee_growth``
    is a list of ``{percentage, timespan}``; the export keeps the first
    entry's percentage (or ``None``), per the column contract.
    """
    company = entry.get("company") or {}
    hq = company.get("hq") or {}
    growth = company.get("employee_growth") or []
    first_growth = growth[0] if isinstance(growth, list) and growth else {}
    return {
        "name": company.get("name"),
        "domain": company.get("domain"),
        "website": company.get("website"),
        "linkedin_url": company.get("linkedin_url"),
        "industry": company.get("industry"),
        "type": company.get("type"),
        "size": company.get("size"),
        "employees_on_linkedin": company.get("employees_on_linkedin"),
        "followers": company.get("followers"),
        "founded_year": company.get("founded_year"),
        "hq_city": hq.get("city"),
        "hq_state": hq.get("state"),
        "hq_country_code": hq.get("country_code"),
        "hq_region": hq.get("region"),
        "revenue": company.get("revenue"),
        "slogan": company.get("slogan"),
        "employee_growth_1y": first_growth.get("percentage") if first_growth else None,
        "matched_people": entry.get("matched_people"),
    }


# ---------------------------------------------------------------------------
# Cursor persistence (job_state)
# ---------------------------------------------------------------------------

def save_tam_progress(
    job_id: str,
    *,
    cursor: Optional[str],
    rows_written: int,
    pages: int,
) -> None:
    """Persist the live TAM cursor + row count into the shared ``job_state``
    table, one write per page.

    There is no clean precedent for free-form resumable state in this
    codebase: ``job_state.state`` is only ever read back for the exact
    literals ``'cancelled'`` / ``'active'`` (``restore_job_state``), so a
    JSON payload written there is inert to the boot restore and to the
    cancel endpoint (which removes the row outright). A future restart /
    resume path can read this row back via ``read_tam_progress``; every
    normal completion wipes it via ``remove_job_state`` in the runner's
    ``finally`` block.
    """
    store = job_store.get_store()
    payload = json.dumps(
        {
            "kind": TAM_PROGRESS_KIND,
            "cursor": cursor,
            "rows_written": rows_written,
            "pages": pages,
        }
    )
    store.save_job_state(job_id, payload)


def read_tam_progress(job_id: str) -> Optional[dict[str, Any]]:
    """Read back the TAM progress payload for ``job_id`` (or None).

    Returns the parsed dict only when the row exists AND carries the
    ``tam_progress`` kind marker — anything else (cancelled/active markers,
    corrupt JSON) reads as absent.
    """
    store = job_store.get_store()
    try:
        row = store.conn.execute(
            "SELECT state FROM job_state WHERE job_id=?", (job_id,)
        ).fetchone()
    except Exception as read_err:
        logger.warning("TAM progress read failed for %s: %s", job_id, read_err)
        return None
    if not row:
        return None
    try:
        data = json.loads(row["state"])
    except (json.JSONDecodeError, TypeError):
        return None
    if isinstance(data, dict) and data.get("kind") == TAM_PROGRESS_KIND:
        return data
    return None


# ---------------------------------------------------------------------------
# Flow-1 chain hand-off
# ---------------------------------------------------------------------------

def create_chained_enrichment_job(
    tam_job_id: str,
    user_id: str,
    companies: list[dict[str, Any]],
    *,
    titles: Optional[list[str]],
    max_decision_makers: int,
    providers: Optional[list[str]],
    exact_titles: bool = False,
) -> dict[str, Any]:
    """Create a Flow-1 domain-enrichment job over a TAM run's domains.

    Reuses the exact code path ``/flows/domain-enrich`` uses for job creation
    (``EnrichmentJobStore.create_enrichment_job``) and execution
    (``routes._run_domain_enrich_job``) — no upload, no duplicated CSV
    plumbing; rows are handed over in-memory the same way the scraper ->
    enrichment chain endpoint does. Rows are deduped by domain with
    normalization on (mirrors the Flow-1 pre-processing).

    Title handling: the stored ``cascade_config`` keeps the titles
    UNBRACKETED even when ``exact_titles`` is true — the chained job's local
    title gate matches include-titles literally, so bracketed "[CEO]" tokens
    would drop every person. Exact matching travels as the ``exact_titles``
    kwarg on ``_run_domain_enrich_job``, whose callee bracket-wraps at
    Blitz-call time (``blitz_find_people_prepass``).

    Returns ``{"chained_job_id": ..., "chained_total": N,
    "deduped_count": N}`` or ``{"chain_skipped": reason}``.
    """
    from enrichment import routes as enrichment_routes  # lazy: avoids import cycle

    chain_rows = [
        {"domain": row.get("domain") or "", "company_name": row.get("name") or ""}
        for row in companies
        if row.get("domain")
    ]
    if not chain_rows:
        return {"chain_skipped": "no_companies_with_domain"}

    deduped_rows, deduped_count, _skipped = identifier_utils.dedupe_rows_by_domain(
        chain_rows, "domain", True
    )

    # Store the cascade UNBRACKETED and forward ``exact_titles`` to the
    # Flow-1 runner instead (mirrors how /flows/domain-enrich persists plain
    # cascades + forwards the flag). The chained job's LOCAL title gate
    # (title_filter.person_matches_titles) matches include-titles literally,
    # so a stored "[CEO]" cascade rejected every person (100% drop).
    # ``run_domain_enrichment`` brackets the plain titles at Blitz-call time
    # (blitz_find_people_prepass -> bracket_exact), keeping server-side exact
    # matching intact.
    cascade_json = None
    if titles:
        cascade = enrichment_routes._titles_to_cascade(",".join(list(titles)))
        if cascade:
            cascade_json = json.dumps(cascade)

    chained_job_id = str(uuid.uuid4())
    store = job_store.get_store()
    store.create_enrichment_job(
        job_id=chained_job_id,
        user_id=user_id,
        total=len(deduped_rows),
        filename=f"tam_{tam_job_id[:8]}_{chained_job_id[:8]}.csv",
        domain_col="domain",
        original_filename=f"tam_{tam_job_id[:8]}.csv",
        parent_job_id=tam_job_id,
        cascade_config=cascade_json,
        max_results=max_decision_makers,
        selected_providers=providers,
        source_type="tam_chain",
    )

    # Same in-memory plumbing the flow endpoints use so the generic SSE /
    # cancel endpoints work for the chained job.
    enrichment_routes._job_signals[chained_job_id] = asyncio.Event()
    enrichment_routes._active_jobs.add(chained_job_id)

    task = asyncio.create_task(
        enrichment_routes._run_domain_enrich_job(
            job_id=chained_job_id,
            rows=deduped_rows,
            domain_col="domain",
            name_col="company_name",
            first_name_col=None,
            last_name_col=None,
            max_results=max_decision_makers,
            selected_providers=providers,
            exact_titles=exact_titles,
        )
    )
    _pending_chain_tasks.add(task)
    task.add_done_callback(_pending_chain_tasks.discard)

    logger.info(
        "TAM job %s chained Flow-1 job %s over %d domains (%d deduped away)",
        tam_job_id, chained_job_id, len(deduped_rows), deduped_count,
    )
    return {
        "chained_job_id": chained_job_id,
        "chained_total": len(deduped_rows),
        "deduped_count": deduped_count,
    }


# ---------------------------------------------------------------------------
# Contacts-DB backup
# ---------------------------------------------------------------------------

async def _backup_companies_to_contacts_db(
    http: httpx.AsyncClient, rows: list[dict[str, Any]]
) -> dict[str, int]:
    """Upsert every domain-bearing TAM company into the Contacts DB.

    Mirrors the write-back guarantee of the enrichment waterfall: new data
    the platform produces lands in the contacts database (leadsdatabase.cc),
    not only in a job CSV. The business upsert is domain-keyed, so
    domain-less companies are counted and skipped; duplicate domains are
    collapsed. Individual failures are logged and NEVER propagated — the
    TAM job's own status must not depend on backup health.
    """
    backed_up = 0
    skipped_no_domain = 0
    failed = 0
    seen_domains: set[str] = set()
    for row in rows:
        domain = (row.get("domain") or "").strip().lower()
        if not domain:
            skipped_no_domain += 1
            continue
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        try:
            await contacts_client.upsert_business_record_async(
                http,
                domain=domain,
                company_name=row.get("name") or "",
                company_website=row.get("website") or "",
                city=row.get("hq_city") or "",
                city_state=row.get("hq_state") or "",
            )
            backed_up += 1
        except Exception as upsert_err:  # best-effort by contract
            failed += 1
            logger.warning(
                "TAM company backup failed for %s: %s", domain, upsert_err
            )
    return {
        "companies_backed_up": backed_up,
        "companies_skipped_no_domain": skipped_no_domain,
        "companies_backup_failed": failed,
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

async def run_tam_flow(
    job_id: str,
    params: dict[str, Any],
    on_progress: Optional[Callable[[dict[str, Any]], Any]] = None,
) -> dict[str, Any]:
    """Run the TAM-by-People pagination loop for ``job_id``.

    Args:
        job_id: enrichment job row this run belongs to (already created and
            set 'running' by the route-level skeleton).
        params: validated request params:
            - company_filters / people_filters: whitelisted Blitz filter
              payloads (built by the route models — never raw client dicts).
            - max_companies: budget ceiling (clamped to TAM_MAX_COMPANIES).
            - create_enrichment_job: chain a Flow-1 job on completion.
            - user_id: job owner (used by the chain).
            - titles / max_decision_makers / providers / exact_titles: chain
              parameters for the Flow-1 job.
            - output_dir: optional output directory override (tests).
            - should_cancel: sync ``() -> bool`` checked between pages.
        on_progress: callback invoked once per page (SSE event payload).

    Returns:
        ``{"status": "done"|"cancelled", "companies_found": N, "pages": N,
        "csv_path": str, "capped": bool, "chained_job_id"? , "chain_skipped"?}``

    Cancel semantics: checked between pages; a cancelled run keeps whatever
        rows already flushed (partial CSV) and reports status 'cancelled'.

    Loop guards (safety): the pull hard-stops at ``TAM_MAX_PAGES`` pages
        (default 400, env ``TAM_MAX_PAGES``) and after ``TAM_EMPTY_PAGE_LIMIT``
        consecutive empty pages — both keep every row already written and
        leave ``capped`` true when a cursor was still outstanding.
    """
    company_filters: dict[str, Any] = dict(params.get("company_filters") or {})
    people_filters: dict[str, Any] = dict(params.get("people_filters") or {})
    max_companies = min(
        int(params.get("max_companies") or 1000), TAM_MAX_COMPANIES
    )
    output_dir = Path(params.get("output_dir") or OUTPUT_DIR)
    should_cancel: Optional[Callable[[], bool]] = params.get("should_cancel")

    output_path = output_dir / f"{job_id}.csv"
    rows: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    consecutive_empty_pages = 0
    cancelled = False

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_file = open(output_path, "w", newline="", encoding="utf-8")
    try:
        writer = csv.DictWriter(
            csv_file, fieldnames=list(TAM_CSV_COLUMNS), extrasaction="ignore"
        )
        writer.writeheader()
        csv_file.flush()

        async with httpx.AsyncClient() as http:
            while True:
                if should_cancel and should_cancel():
                    cancelled = True
                    logger.info(
                        "TAM job %s cancelled after %d pages (%d companies)",
                        job_id, pages, len(rows),
                    )
                    break

                if pages >= TAM_MAX_PAGES:
                    # Cursor-cycling server bug guard: without this the loop
                    # would fetch forever (heartbeat keeps the job alive,
                    # job_events grows unbounded). Keep everything written.
                    logger.warning(
                        "TAM job %s hit the %d-page hard cap (%d companies, "
                        "cursor still outstanding) — stopping the pull",
                        job_id, TAM_MAX_PAGES, len(rows),
                    )
                    break

                page = await blitz_client.tam_by_people(
                    http,
                    company_filters=company_filters,
                    people_filters=people_filters,
                    max_results=TAM_PAGE_SIZE,
                    cursor=cursor,
                )
                pages += 1

                entries = page.get("results") or []
                remaining = max_companies - len(rows)
                for entry in entries[:max(remaining, 0)]:
                    rows.append(flatten_tam_company(entry))
                    writer.writerow(rows[-1])
                # Per-page flush + fsync so a running job is live-downloadable
                # and a crash/cancel never loses completed pages (mirrors the
                # Flow-1 incremental writer).
                csv_file.flush()
                await asyncio.to_thread(os.fsync, csv_file.fileno())

                consecutive_empty_pages = consecutive_empty_pages + 1 if not entries else 0

                cursor = page.get("cursor") or None
                save_tam_progress(
                    job_id,
                    cursor=cursor,
                    rows_written=len(rows),
                    pages=pages,
                )
                # Live result-count on the job row so the Jobs page card
                # counts up while the pull runs (best-effort: a transient
                # SQLite hiccup must not kill a long pull).
                try:
                    job_store.get_store().update_result_count(job_id, len(rows))
                except Exception as count_err:
                    logger.debug(
                        "TAM per-page result-count update failed for %s: %s",
                        job_id, count_err,
                    )
                if on_progress:
                    try:
                        await _emit_progress(on_progress, {
                            "stage": "tam_page",
                            "page": pages,
                            "companies_found": len(rows),
                            "has_next_page": bool(cursor),
                            "message": (
                                f"TAM page {pages}: {len(rows)} companies so far"
                            ),
                        })
                    except Exception as prog_err:
                        logger.error(
                            "TAM progress callback failed for %s: %s",
                            job_id, prog_err,
                        )

                if cursor is None:
                    break
                if consecutive_empty_pages >= TAM_EMPTY_PAGE_LIMIT:
                    # Server keeps returning a cursor with zero results —
                    # treat as exhaustion instead of an unbounded request
                    # loop. Everything written so far is kept.
                    logger.warning(
                        "TAM job %s saw %d consecutive empty pages "
                        "(%d companies kept) — stopping the pull",
                        job_id, TAM_EMPTY_PAGE_LIMIT, len(rows),
                    )
                    break
                if len(rows) >= max_companies:
                    logger.info(
                        "TAM job %s hit max_companies cap (%d) after %d pages",
                        job_id, max_companies, pages,
                    )
                    break

            # Backup every discovered company into the Contacts DB — same
            # system of record the enrichment waterfall writes to. Runs for
            # cancelled pulls too: the rows already flushed are real data.
            backup_stats = await _backup_companies_to_contacts_db(http, rows)
            if on_progress:
                try:
                    await _emit_progress(on_progress, {
                        "stage": "tam_backup",
                        "message": (
                            f"Saved {backup_stats['companies_backed_up']} "
                            f"companies to the contacts database"
                        ),
                        **backup_stats,
                    })
                except Exception as prog_err:
                    logger.error(
                        "TAM backup progress callback failed for %s: %s",
                        job_id, prog_err,
                    )
    finally:
        csv_file.close()

    store = job_store.get_store()
    store.update_result_count(job_id, len(rows))

    summary: dict[str, Any] = {
        "status": "cancelled" if cancelled else "done",
        "companies_found": len(rows),
        "pages": pages,
        "csv_path": str(output_path),
        "capped": not cancelled and cursor is not None,
        **backup_stats,
    }

    if cancelled:
        return summary

    if params.get("create_enrichment_job"):
        try:
            chain_result = create_chained_enrichment_job(
                job_id,
                params.get("user_id") or "",
                rows,
                titles=params.get("titles"),
                max_decision_makers=int(params.get("max_decision_makers") or 5),
                providers=params.get("providers"),
                exact_titles=bool(params.get("exact_titles")),
            )
            return {**summary, **chain_result}
        except Exception as chain_err:
            # The TAM export itself is complete and safe; a chain failure must
            # not lose it. Surface the reason and keep status 'done'.
            logger.exception("TAM job %s chain creation failed: %s", job_id, chain_err)
            return {**summary, "chain_skipped": f"chain_error: {chain_err}"}

    return summary


async def _emit_progress(
    on_progress: Callable[[dict[str, Any]], Any],
    event: dict[str, Any],
) -> None:
    """Invoke a sync-or-async on_progress callback with one event."""
    result = on_progress(event)
    if asyncio.iscoroutine(result):
        await result
