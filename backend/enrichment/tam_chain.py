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
    """Create a Flow-1 domain-enrichment job over a TAM run's domains.

    Rows are deduped by domain with normalization on (mirrors the Flow-1
    pre-processing).

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
    # Friendlier identity than the old tam_<id>_<id> pattern: the download
    # filename says what it is, display_name says where it came from.
    chain_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    store.create_enrichment_job(
        job_id=chained_job_id,
        user_id=user_id,
        total=len(deduped_rows),
        filename=f"find_companies_contacts_{chain_stamp}.csv",
        domain_col="domain",
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
