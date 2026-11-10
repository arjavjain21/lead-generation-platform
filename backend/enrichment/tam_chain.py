"""TAM -> Flow-1 (domain-enrichment) chain hand-off.

Extracted from ``tam_flow.py`` (Lookalike 2.0, 2026-09-18) to keep that
module under the file-size budget; ``tam_flow`` re-exports
``create_chained_enrichment_job`` so existing callers/tests are unchanged.

Reuses the exact code path ``/flows/domain-enrich`` uses for job creation
(``EnrichmentJobStore.create_enrichment_job``) and execution
(``routes._run_domain_enrich_job``) — no upload, no duplicated CSV plumbing;
rows are handed over in-memory the same way the scraper -> enrichment chain
endpoint does.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from enrichment import identifier_utils
from enrichment import job_store

logger = logging.getLogger(__name__)

# Fire-and-forget chain tasks keep a strong reference here so the event
# loop's weak task refs cannot garbage-collect a live Flow-1 job mid-run.
_pending_chain_tasks: set[asyncio.Task] = set()

# Column the chained Flow-1 job uses for TAM company LinkedIn pages. It is
# deliberately ``linkedin_url`` (NOT a bespoke name): the value is persisted
# as the job's ``linkedin_url_col`` — an existing jobs-table column that
# restart/resume restores automatically (routes.py create_enrichment_job +
# runner kwargs) — and ``list_builder.process_row``'s existing auto-detect
# routes any ``/company/`` URL found in that column straight to the
# company-URL orchestrator (the same path /flows/domain-enrich uses for
# company-URL rows). No new cascade code, no new column plumbing.
CHAIN_LINKEDIN_COL = "linkedin_url"


def _canonical_company_url(url: str) -> str:
    """Drop a leading ``www.`` host from a normalize_linkedin_url result.

    ``identifier_utils.normalize_linkedin_url`` lowercases host+path and
    strips query/trailing slash, but deliberately preserves ``www.`` — so
    ``www.linkedin.com/company/acme`` and ``linkedin.com/company/acme`` would
    be two different chain keys. Stripped HERE (chain-local) instead of in
    the shared helper: the shared normalizer feeds stored ``linkedin_norm``
    comparisons elsewhere and must keep its exact behavior.
    """
    if url.startswith("https://www.linkedin.com/"):
        return "https://linkedin.com/" + url[len("https://www.linkedin.com/"):]
    return url


def _company_url_from_tam_row(row: dict[str, Any]) -> str:
    """Canonical company-page LinkedIn URL for a TAM row ('' otherwise).

    Domain-less TAM rows can carry personal (/in/) or malformed URLs; only
    company-style pages (``/company/``, ``/school/``, ``/organization/`` per
    ``list_builder._is_company_linkedin_url``) can drive the Flow-1
    company-URL path, so anything else is treated as absent.
    """
    from enrichment import list_builder  # lazy: keeps module import light

    url = identifier_utils.normalize_linkedin_url(row.get("linkedin_url") or "")
    if not url:
        return ""
    if not list_builder._is_company_linkedin_url(url):
        return ""
    return _canonical_company_url(url)


def create_chained_enrichment_job(
    tam_job_id: str,
    user_id: str,
    companies: list[dict[str, Any]],
    *,
    titles: Optional[list[str]],
    max_decision_makers: int,
    providers: Optional[list[str]],
    exact_titles: bool = False,
    tam_display_name: str = "",
) -> dict[str, Any]:
    """Create a Flow-1 domain-enrichment job over a TAM run's companies.

    Rows are deduped by domain with normalization on (mirrors the Flow-1
    pre-processing). Since 2026-11-10 (PR-2.3), TAM companies that have NO
    domain but DO have a company LinkedIn page are chained too — they ride
    the exact company-URL path /flows/domain-enrich already uses: the URL
    travels in a ``linkedin_url`` column (persisted as the job's
    ``linkedin_url_col`` so restart/resume restores it; the jobs table has no
    company_linkedin_col) and ``list_builder.process_row``'s ``/company/``
    auto-detect routes the row to the company orchestrator. The provider
    waterfall itself is untouched.

    Cross-dedupe: a company already chained by domain (its ``linkedin_url``
    known from the same TAM run) is not chained again by URL, and duplicate
    company URLs among the domain-less rows collapse first-wins.

    Title handling: the stored ``cascade_config`` keeps the titles
    UNBRACKETED even when ``exact_titles`` is true — the chained job's local
    title gate matches include-titles literally, so bracketed "[CEO]" tokens
    would drop every person. Exact matching travels as the ``exact_titles``
    kwarg on ``_run_domain_enrich_job``, whose callee bracket-wraps at
    Blitz-call time (``blitz_find_people_prepass``).

    Returns ``{"chained_job_id": ..., "chained_total": N, "deduped_count": N,
    "chained_linkedin_rows": N}`` or ``{"chain_skipped": reason}`` (the
    legacy ``no_companies_with_domain`` literal is preserved for API/UI
    compatibility; it now means "no company had a domain OR a company
    LinkedIn URL").
    """
    from enrichment import routes as enrichment_routes  # lazy: avoids import cycle

    chain_rows = [
        {"domain": row.get("domain") or "", "company_name": row.get("name") or ""}
        for row in companies
        if row.get("domain")
    ]

    # LinkedIn-only companies (PR-2.3): collect the canonical company URLs
    # already represented by a domain-bearing row so the same company is
    # never chained twice, then first-wins-dedupe the domain-less rows.
    chained_urls = {
        _canonical_company_url(identifier_utils.normalize_linkedin_url(row.get("linkedin_url") or ""))
        for row in companies
        if row.get("domain")
    }
    chained_urls.discard("")
    linkedin_rows: list[dict[str, Any]] = []
    for row in companies:
        if row.get("domain"):
            continue  # domain rows are chained above — never twice
        url = _company_url_from_tam_row(row)
        if not url or url in chained_urls:
            continue
        linkedin_rows.append({
            "domain": "",
            CHAIN_LINKEDIN_COL: url,
            "company_name": row.get("name") or "",
        })
        chained_urls.add(url)

    if not chain_rows and not linkedin_rows:
        return {"chain_skipped": "no_companies_with_domain"}

    deduped_rows, deduped_count, _skipped = identifier_utils.dedupe_rows_by_domain(
        chain_rows, "domain", True
    )
    # Empty-domain rows pass through dedupe_rows_by_domain untouched by
    # design (its Notes: blank keys never collapse); the LinkedIn rows were
    # already URL-deduped above, so appending preserves both guarantees.
    deduped_rows = deduped_rows + linkedin_rows

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
    # Friendlier identity than the old tam_<id>_<id> pattern: the download
    # filename says what it is, display_name says where it came from.
    chain_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    store.create_enrichment_job(
        job_id=chained_job_id,
        user_id=user_id,
        total=len(deduped_rows),
        filename=f"find_companies_contacts_{chain_stamp}.csv",
        domain_col="domain",
        # Persisted so /restart + auto-resume restore the company-URL
        # routing for LinkedIn-only rows automatically (same mechanism the
        # upload flow uses; the jobs table has no company_linkedin_col).
        linkedin_url_col=CHAIN_LINKEDIN_COL,
        original_filename=f"find_companies_contacts_{chain_stamp}.csv",
        parent_job_id=tam_job_id,
        cascade_config=cascade_json,
        max_results=max_decision_makers,
        selected_providers=providers,
        source_type="tam_chain",
        display_name=(
            f"Contacts for TAM companies{(' — ' + tam_display_name) if tam_display_name else ''}"
        )[:120],
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
            linkedin_url_col=CHAIN_LINKEDIN_COL,
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
        "TAM job %s chained Flow-1 job %s over %d companies "
        "(%d domain + %d linkedin-only; %d domain deduped away)",
        tam_job_id, chained_job_id, len(deduped_rows),
        len(deduped_rows) - len(linkedin_rows), len(linkedin_rows), deduped_count,
    )
    return {
        "chained_job_id": chained_job_id,
        "chained_total": len(deduped_rows),
        "deduped_count": deduped_count,
        "chained_linkedin_rows": len(linkedin_rows),
    }
