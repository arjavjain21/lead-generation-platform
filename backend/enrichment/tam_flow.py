"""TAM (Total Addressable Market) by-People flow — job runner (new Flow 2).

Pages through Blitz ``POST /v2/company/tam-by-people`` with persona +
firmographic filters, flattens every returned company into one CSV row,
persists a live cursor for crash-resume diagnostics, and (optionally) chains
a Flow-1 domain-enrichment job over the discovered domains by reusing the
exact ``/flows/domain-enrich`` creation + execution code path.

Lookalike 2.0 (2026-09-18): when the request carries ``rank_seeds`` (seed
profiles) + ``query_plan`` (from ``lookalike.build_query_plan``), retrieval
FANS OUT — one Blitz query per seed industry plus one keywords-only query —
and every candidate row is scored deterministically against the seeds
(``tam_ranker.rank_rows``: trigram text similarity + industry + size +
niche keywords), sorted best-match first, and exported with trailing
``match_score`` / ``why_matched`` columns.

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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

from enrichment import blitz_client
from enrichment import tam_ranker
from enrichment import job_store
from enrichment.tam_chain import create_chained_enrichment_job  # noqa: F401  (re-export)
from enrichment.tam_legs import (  # noqa: F401  (re-exported legs)
    _backup_companies_to_contacts_db,
    _getleads_company_pull,
    _industry_to_keywords_on_422,
    _tam_validation_error,
)

logger = logging.getLogger(__name__)

# Output dir mirrors enrichment/routes.py's convention (backend/data/outputs).
# Duplicated here to avoid importing enrichment.routes at module top (it is
# imported lazily for the Flow-1 chain hand-off instead).
DATA_DIR = Path(__file__).parent.parent / "data"
OUTPUT_DIR = DATA_DIR / "outputs"

# CSV column contract for the TAM export (order is load-bearing: the
# incremental DictWriter is created from this once, per job). The two
# trailing columns are Lookalike 2.0 additions — ranked runs fill them,
# plain runs export them empty (flatten defaults None).
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
    "source",
    "match_score",
    "why_matched",
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

# GetLeads lookalike leg: hard credit cap per run (1 credit per contact
# returned). Env-overridable; the pull stops the moment it is hit.
GETLEADS_LOOKALIKE_MAX_CREDITS = _env_int("GETLEADS_LOOKALIKE_MAX_CREDITS", 500)

# Lookalike 2.0 fan-out ceilings: total candidates a ranked run may collect
# (scoring happens in memory, so this also bounds job memory) and the
# per-query-variant row cap.
LOOKALIKE_MAX_CANDIDATES = _env_int("LOOKALIKE_MAX_CANDIDATES", 2000)
FANOUT_PER_QUERY_CAP = 300


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
        "source": entry.get("source") or "blitz",
        # Lookalike 2.0 ranking columns — filled by tam_ranker.rank_rows in
        # ranked runs; None (empty CSV cell) everywhere else.
        "match_score": None,
        "why_matched": None,
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
# Flow-1 chain hand-off: see enrichment/tam_chain.py (create_chained_enrichment_job
# is re-exported above for callers/tests that still reference tam_flow.*).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Auxiliary legs (GetLeads pull, contacts-DB backup, 422 fallback) live in
# enrichment/tam_legs.py; re-exported under their original private names so
# callers/tests that reference tam_flow.* are unchanged.

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

@dataclass
class _PullShared:
    """State shared across every pull of one run (fan-out = several pulls).

    ``rows``/``pages``/``cursor`` are the run-wide counters the original
    single-loop owned; ``dedupe`` is only populated for fan-out runs — the
    same company legitimately appears in several industry queries and would
    otherwise be billed and exported twice per variant.
    """
    rows: list[dict[str, Any]] = field(default_factory=list)
    dedupe: Optional[set[str]] = None
    pages: int = 0
    cursor: Optional[str] = None
    cancelled: bool = False


def _row_dedupe_key(row: dict[str, Any]) -> Optional[str]:
    """Stable cross-variant identity: domain, else LinkedIn URL, else name."""
    domain = (row.get("domain") or "").strip().lower()
    if domain:
        return domain
    linkedin = (row.get("linkedin_url") or "").strip().lower()
    if linkedin:
        return linkedin
    name = (row.get("name") or "").strip().lower()
    return f"name:{name}" if name else None


def _gl_seen_keys(rows: list[dict[str, Any]]) -> set[str]:
    """Domain + name keys of already-collected rows, for GetLeads dedupe
    (its pull dedupes on ``domain or name:<name>`` keys)."""
    keys: set[str] = set()
    for row in rows:
        domain = (row.get("domain") or "").strip().lower()
        name = (row.get("name") or "").strip().lower()
        if domain:
            keys.add(domain)
        if name:
            keys.add(f"name:{name}")
    return keys


async def _blitz_pull(
    http: httpx.AsyncClient,
    *,
    job_id: str,
    company_filters: dict[str, Any],
    people_filters: dict[str, Any],
    budget: int,
    shared: _PullShared,
    exclude_domains: set[str],
    should_cancel: Optional[Callable[[], bool]],
    on_progress: Optional[Callable[[dict[str, Any]], Any]],
    write: bool,
    writer: csv.DictWriter,
    csv_file: Any,
) -> None:
    """One tam-by-people pagination loop — a single query variant.

    Appends up to ``budget`` new rows to ``shared.rows`` (deduped across
    variants when ``shared.dedupe`` is set), persists the cursor per page,
    and writes rows incrementally unless ``write`` is False (ranked runs
    buffer rows and write the scored, sorted export at the end). Every loop
    guard of the original inline loop is preserved: cancel check, the
    TAM_MAX_PAGES hard cap, the strict-enum 422 -> keywords fallback, and
    the consecutive-empty-page break.
    """
    filters = dict(company_filters)
    industry_fallback_used = False
    cursor: Optional[str] = None
    consecutive_empty_pages = 0
    added = 0

    while True:
        if should_cancel and should_cancel():
            shared.cancelled = True
            logger.info(
                "TAM job %s cancelled after %d pages (%d companies)",
                job_id, shared.pages, len(shared.rows),
            )
            break

        if shared.pages >= TAM_MAX_PAGES:
            # Cursor-cycling server bug guard: without this the loop
            # would fetch forever (heartbeat keeps the job alive,
            # job_events grows unbounded). Keep everything written.
            logger.warning(
                "TAM job %s hit the %d-page hard cap (%d companies, "
                "cursor still outstanding) — stopping the pull",
                job_id, TAM_MAX_PAGES, len(shared.rows),
            )
            break

        try:
            page = await blitz_client.tam_by_people(
                http,
                company_filters=filters,
                people_filters=people_filters,
                max_results=TAM_PAGE_SIZE,
                cursor=cursor,
            )
        except httpx.HTTPStatusError as http_err:
            status = getattr(http_err.response, "status_code", None)
            body = ""
            try:
                body = http_err.response.text or ""
            except Exception:
                pass
            if status == 422:
                rebuilt = _industry_to_keywords_on_422(
                    filters, body,
                    already_used=industry_fallback_used,
                )
                if rebuilt is not None:
                    industry_fallback_used = True
                    filters = rebuilt
                    logger.warning(
                        "TAM job %s: industry filter rejected by "
                        "Blitz's strict taxonomy — retrying as "
                        "keyword filter", job_id,
                    )
                    if on_progress:
                        try:
                            await _emit_progress(on_progress, {
                                "stage": "tam_filter_adjust",
                                "message": (
                                    "Industry filter converted to a "
                                    "keyword search (no exact match "
                                    "in the data provider's industry "
                                    "list) — continuing"
                                ),
                            })
                        except Exception:
                            pass
                    # Retry the same cursor; the rejected request
                    # did not count as a page (422 never bills).
                    continue
                raise _tam_validation_error(http_err) from http_err
            raise
        shared.pages += 1

        entries = page.get("results") or []
        remaining = budget - added
        for entry in entries[:max(remaining, 0)]:
            row = flatten_tam_company(entry)
            row_domain = (row.get("domain") or "").strip().lower()
            if row_domain and row_domain in exclude_domains:
                continue  # a seed company — never a lookalike result
            if shared.dedupe is not None:
                key = _row_dedupe_key(row)
                if key is None or key in shared.dedupe:
                    continue  # already collected by an earlier variant
                shared.dedupe.add(key)
            shared.rows.append(row)
            added += 1
            if write:
                writer.writerow(row)
        # Per-page flush + fsync so a running job is live-downloadable
        # and a crash/cancel never loses completed pages (mirrors the
        # Flow-1 incremental writer). Skipped for buffered (ranked) writes.
        if write:
            csv_file.flush()
            await asyncio.to_thread(os.fsync, csv_file.fileno())

        consecutive_empty_pages = consecutive_empty_pages + 1 if not entries else 0

        cursor = page.get("cursor") or None
        shared.cursor = cursor
        save_tam_progress(
            job_id,
            cursor=cursor,
            rows_written=len(shared.rows),
            pages=shared.pages,
        )
        # Live result-count on the job row so the Jobs page card
        # counts up while the pull runs (best-effort: a transient
        # SQLite hiccup must not kill a long pull).
        try:
            job_store.get_store().update_result_count(job_id, len(shared.rows))
        except Exception as count_err:
            logger.debug(
                "TAM per-page result-count update failed for %s: %s",
                job_id, count_err,
            )
        if on_progress:
            try:
                await _emit_progress(on_progress, {
                    "stage": "tam_page",
                    "page": shared.pages,
                    "companies_found": len(shared.rows),
                    "has_next_page": bool(cursor),
                    "message": (
                        f"TAM page {shared.pages}: {len(shared.rows)} companies so far"
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
                job_id, TAM_EMPTY_PAGE_LIMIT, len(shared.rows),
            )
            break
        if added >= budget:
            break


async def run_tam_flow(
    job_id: str,
    params: dict[str, Any],
    on_progress: Optional[Callable[[dict[str, Any]], Any]] = None,
) -> dict[str, Any]:
    """Run the TAM-by-People retrieval for ``job_id``.

    Args:
        job_id: enrichment job row this run belongs to (already created and
            set 'running' by the route-level skeleton).
        params: validated request params:
            - company_filters / people_filters: whitelisted Blitz filter
              payloads (built by the route models — never raw client dicts).
            - max_companies: budget ceiling (clamped to TAM_MAX_COMPANIES;
              fanned-out runs are additionally capped at
              LOOKALIKE_MAX_CANDIDATES).
            - rank_seeds / query_plan (Lookalike 2.0): seed profile dicts +
              the fan-out plan from ``lookalike.build_query_plan``. When
              present, retrieval runs one query per plan industry plus a
              keywords-only query, and the export is scored + sorted by
              ``tam_ranker.rank_rows`` (rows buffer in memory; the CSV is
              written once, best-match first).
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
        rows already flushed (partial CSV; ranked runs score + sort whatever
        was collected before the cancel) and reports status 'cancelled'.

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
    sources = [s for s in (params.get("sources") or ["blitz"]) if s in ("blitz", "getleads")] or ["blitz"]
    exclude_domains = set(params.get("exclude_domains") or [])

    rank_seeds = [s for s in (params.get("rank_seeds") or []) if isinstance(s, dict)]
    query_plan: dict[str, Any] = params.get("query_plan") or {}
    ranked = bool(rank_seeds)
    fanout = ranked or bool(query_plan)
    overall_budget = min(max_companies, LOOKALIKE_MAX_CANDIDATES) if fanout else max_companies

    output_path = output_dir / f"{job_id}.csv"
    rows: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    cancelled = False

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_file = open(output_path, "w", newline="", encoding="utf-8")
    try:
        writer = csv.DictWriter(
            csv_file, fieldnames=list(TAM_CSV_COLUMNS), extrasaction="ignore"
        )
        writer.writeheader()
        csv_file.flush()

        getleads_credits_used = 0
        backup_stats: dict[str, int] = {}
        async with httpx.AsyncClient() as http:
            shared = _PullShared(rows=rows, dedupe=set() if fanout else None)
            if "blitz" in sources:
                variants = (
                    tam_ranker.build_blitz_variants(company_filters, query_plan)
                    if fanout else []
                )
                if variants:
                    total_variants = len(variants)
                    for index, (label, variant_filters) in enumerate(variants, 1):
                        if shared.cancelled:
                            break
                        remaining_budget = overall_budget - len(shared.rows)
                        if remaining_budget <= 0:
                            break
                        budget = min(
                            FANOUT_PER_QUERY_CAP,
                            tam_ranker.split_fanout_budget(
                                remaining_budget, total_variants - index + 1
                            ),
                        )
                        if on_progress:
                            try:
                                await _emit_progress(on_progress, {
                                    "stage": "tam_fanout",
                                    "variant": index,
                                    "variants": total_variants,
                                    "message": (
                                        f"fan-out query {index}/{total_variants}: "
                                        f"{label}"
                                    ),
                                })
                            except Exception:
                                pass
                        await _blitz_pull(
                            http,
                            job_id=job_id,
                            company_filters=variant_filters,
                            people_filters=people_filters,
                            budget=budget,
                            shared=shared,
                            exclude_domains=exclude_domains,
                            should_cancel=should_cancel,
                            on_progress=on_progress,
                            write=not ranked,
                            writer=writer,
                            csv_file=csv_file,
                        )
                else:
                    await _blitz_pull(
                        http,
                        job_id=job_id,
                        company_filters=company_filters,
                        people_filters=people_filters,
                        budget=overall_budget,
                        shared=shared,
                        exclude_domains=exclude_domains,
                        should_cancel=should_cancel,
                        on_progress=on_progress,
                        write=not ranked,
                        writer=writer,
                        csv_file=csv_file,
                    )
            cancelled = shared.cancelled
            pages = shared.pages
            cursor = shared.cursor

            getleads_credits_used = 0
            if "getleads" in sources and len(shared.rows) < max_companies:
                gl_variants = (
                    tam_ranker.build_getleads_variants(
                        params.get("getleads_filters") or {}, query_plan
                    )
                    if fanout
                    else [("getleads", params.get("getleads_filters") or {})]
                )
                # An empty-filters variant is dropped: an untargeted GL pull
                # is 1 credit per random contact — the exact "random mix"
                # the lookalike guard exists to prevent.
                gl_variants = [(lbl, f) for lbl, f in gl_variants if f]
                remaining_credits = GETLEADS_LOOKALIKE_MAX_CREDITS
                total_gl = len(gl_variants)
                for index, (_label, gl_filters) in enumerate(gl_variants, 1):
                    if len(shared.rows) >= max_companies or remaining_credits <= 0:
                        break
                    remaining_needed = overall_budget - len(shared.rows)
                    needed = (
                        tam_ranker.split_fanout_budget(
                            remaining_needed, total_gl - index + 1
                        )
                        if fanout else remaining_needed
                    )
                    gl_rows, credits = await _getleads_company_pull(
                        http,
                        getleads_filters=gl_filters,
                        needed=needed,
                        existing_domains=_gl_seen_keys(shared.rows),
                        exclude_domains=exclude_domains,
                        credits_cap=remaining_credits,
                    )
                    for row in gl_rows:
                        shared.rows.append(row)
                        if not ranked:
                            writer.writerow(row)
                    remaining_credits -= credits
                    getleads_credits_used += credits
                    if on_progress and gl_rows:
                        try:
                            await _emit_progress(on_progress, {
                                "stage": "tam_getleads",
                                "message": (
                                    f"GetLeads leg: +{len(gl_rows)} companies "
                                    f"({getleads_credits_used} credits)"
                                ),
                                "getleads_companies": len(gl_rows),
                                "getleads_credits_used": getleads_credits_used,
                            })
                        except Exception:
                            pass
                if not ranked:
                    csv_file.flush()
            rows = shared.rows

            # Ranked export: score every buffered candidate against the
            # seeds, sort best-match first, write once. Runs for cancelled
            # pulls too — the buffered rows are real data and partial
            # downloads stay interpretable (scored, sorted).
            if ranked and rows:
                rows = tam_ranker.rank_rows(rows, rank_seeds, query_plan)
                for row in rows:
                    writer.writerow(row)
                csv_file.flush()
                await asyncio.to_thread(os.fsync, csv_file.fileno())
                try:
                    job_store.get_store().update_result_count(job_id, len(rows))
                except Exception as count_err:
                    logger.debug(
                        "TAM ranked result-count update failed for %s: %s",
                        job_id, count_err,
                    )
                if on_progress:
                    try:
                        await _emit_progress(on_progress, {
                            "stage": "tam_ranked",
                            "message": (
                                f"Ranked {len(rows)} companies by similarity "
                                f"to your examples"
                            ),
                            "companies_found": len(rows),
                        })
                    except Exception:
                        pass

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
        "sources": sources,
        "getleads_credits_used": getleads_credits_used if "getleads" in sources else 0,
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
                tam_display_name=params.get("display_name") or "",
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
