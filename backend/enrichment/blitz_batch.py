"""Blitz Find People batch prepass — job-level DM discovery before the waterfall.

Pure library: NO job-store / DB imports. Callers own persistence (the miss
store), progress events, and the cancellation policy; this module only
orchestrates Blitz calls and returns plain maps the caller can feed into its
per-domain enrichment loop.
"""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Any, Callable, Optional

import httpx

from enrichment import blitz_client

logger = logging.getLogger(__name__)

# /v2/search/people accepts at most 50 company URLs per call.
_COMPANY_URLS_PER_CHUNK = 50

# Adaptive max_pages bounds (used when the caller does not pin a cap):
# enough pages that a full 50-company chunk can actually REACH the target
# (each page returns at most 50 results), with the legacy floor/ceiling.
_MAX_PAGES_FLOOR = 6
_MAX_PAGES_CEILING = 30


def _effective_max_pages(
    max_pages: Optional[int], target_per_company: int, chunk_size: int
) -> int:
    """Resolve the per-chunk pagination cap.

    An explicit ``max_pages`` is respected verbatim (callers/tests pinning a
    hard cap keep their semantics). The default (``None``) is adaptive:
    ``min(30, max(6, ceil(target * chunk / 50) + 1))`` — a target-10 job
    over a full 50-company chunk needs ~10 pages of up to 50 results to fill
    everyone; the old flat 6 silently truncated and under-filled targets > 5.
    """
    if max_pages is not None:
        return max_pages
    adaptive = math.ceil(target_per_company * chunk_size / _COMPANY_URLS_PER_CHUNK) + 1
    return min(_MAX_PAGES_CEILING, max(_MAX_PAGES_FLOOR, adaptive))


def _safe_record_miss(record_miss: Callable[[str, str], None], domain: str, kind: str) -> None:
    """Best-effort miss marker: never let a store failure break the prepass."""
    try:
        record_miss(domain, kind)
    except Exception as exc:  # noqa: BLE001 — store is best-effort by contract
        logger.warning("blitz prepass record_miss failed for domain (%s): %s", domain, exc)


async def blitz_find_people_prepass(
    http: httpx.AsyncClient,
    domains: list[str],
    *,
    is_recent_miss: Callable[[str], bool],
    record_miss: Callable[[str, str], None],
    title_include: list[str],
    title_exclude: list[str],
    exact_titles: bool,
    target_per_company: int = 5,
    max_pages: Optional[int] = None,
    domain_concurrency: int = 20,
    on_progress: Optional[Callable[[str], Any]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> dict[str, Any]:
    """Cover a job's domains in bulk before the per-domain waterfall runs.

    Replaces the per-domain Blitz waterfall (1 domain-to-linkedin + up to
    ``max_dms`` records/domain via waterfall-icp-keyword) with: 1
    domain-to-linkedin per non-miss domain (discovery lane), then 1
    /v2/search/people call per 50 companies (+cursor pages until every
    company reaches ``target_per_company`` or ``max_pages``), at 1 record
    per result. Domains with a stored recent miss (``is_recent_miss`` — TTL
    owned by the miss store) are never replayed: 0 Blitz records for them.

    FUP math (N fresh domains, all resolve, target 5, single page):
      waterfall path: N x (1 discovery + TITLE_SEARCH_POOL records/domain,
      pool >> 5 on fuzzy titles) vs
      prepass path:   N x 1 discovery + ceil(N/50) calls returning <= N x 5
      records (server-side exact titles shrink the fuzzy superset further).
    Miss replays cost 0 records until the store's TTL expires.

    Args:
        http: shared httpx.AsyncClient pointed at the Blitz host.
        domains: bare domains the job wants decision makers for.
        is_recent_miss: sync ``(domain) -> bool``; True = skip entirely
            (the miss store already paid for this answer).
        record_miss: sync best-effort ``(domain, kind) -> None``;
            kind is ``"company"`` (domain-to-linkedin found=false) or
            ``"contacts"`` (company resolved but zero matching people after
            a CONCLUSIVE pagination end — cursor exhausted, or every company
            at target). NEVER called for exception-path domains (a transient
            error must not poison the domain for the store's TTL) and never
            when the chunk's pagination was truncated at the page cap:
            matches may still exist past the last fetched page, so an empty
            result there is not a definitive miss.
        title_include/title_exclude: job-title filters for the people search.
        exact_titles: wrap ``title_include`` values in ``[...]`` via
            bracket_exact() for server-side exact matching (excludes stay
            fuzzy — an exclusion should stay broad).
        target_per_company: stop paginating once every company in the chunk
            has this many people.
        max_pages: hard cursor-pagination cap per chunk. None (default) is
            adaptive per chunk — ``min(30, max(6, ceil(target *
            chunk_size / 50) + 1))`` — so target>5 jobs page enough to
            actually fill their targets; an explicit int is respected
            verbatim.
        domain_concurrency: in-flight cap for domain-to-linkedin calls (a
            concurrency cap, NOT a rate limit — the discovery lane paces).
        on_progress: optional async ``callable(str)`` — one SSE event per
            chunk, ``"blitz find-people prepass chunk k/n"`` (best-effort).
        should_cancel: optional sync ``callable() -> bool`` checked between
            chunks; on True the prepass stops and unprocessed domains are
            simply absent from ``prepass_domains`` (callers fall back to the
            per-domain waterfall for them).

    Returns:
        {
          "company_url_by_domain": {domain: company_url} — only found companies,
          "persons_by_domain": {domain: [waterfall-flat rows]} — covered
              domains always have a key (empty list when nobody matched),
          "prepass_domains": set of domains the prepass CONCLUSIVELY covered
              (hit OR confirmed miss); everything else falls back to the
              per-domain waterfall,
          "skipped_miss": set of domains skipped via is_recent_miss (never
              touched, disjoint from prepass_domains),
        }
    """
    skipped_miss: set[str] = set()
    active: list[str] = []
    for domain in domains:
        if is_recent_miss(domain):
            skipped_miss.add(domain)
        else:
            active.append(domain)

    company_url_by_domain: dict[str, str] = {}
    persons_by_domain: dict[str, list[dict[str, Any]]] = {}
    prepass_domains: set[str] = set()

    # Phase 1: domains -> company LinkedIn URLs (discovery lane), bounded by
    # a semaphore. Per-domain exceptions leave the domain OUT of
    # prepass_domains so the caller falls back to the per-domain waterfall.
    semaphore = asyncio.Semaphore(max(1, domain_concurrency))

    async def _resolve(domain: str) -> None:
        async with semaphore:
            try:
                result = await blitz_client.domain_to_linkedin(http, domain)
            except Exception as exc:  # noqa: BLE001 — per-domain fallback, not a miss
                logger.warning(
                    "blitz prepass domain-to-linkedin failed for domain (%s): %s", domain, exc
                )
                return

            if not isinstance(result, dict) or not result.get("company_linkedin_url"):
                # Conclusive company miss — safe to store (Blitz answered: no page).
                _safe_record_miss(record_miss, domain, "company")
                prepass_domains.add(domain)
                persons_by_domain[domain] = []
                return
            company_url_by_domain[domain] = result["company_linkedin_url"]

    if active:
        await asyncio.gather(*(_resolve(domain) for domain in active))

    # Phase 2: chunk the resolved companies (sorted for deterministic chunk
    # boundaries + progress events) and batch-search people per chunk.
    items = sorted(company_url_by_domain.items())
    chunks = [
        items[i : i + _COMPANY_URLS_PER_CHUNK]
        for i in range(0, len(items), _COMPANY_URLS_PER_CHUNK)
    ]
    total_chunks = len(chunks)

    effective_include = (
        blitz_client.bracket_exact(title_include)
        if exact_titles and title_include
        else title_include
    )

    for index, chunk in enumerate(chunks, start=1):
        if should_cancel is not None and should_cancel():
            logger.info(
                "blitz find-people prepass cancelled before chunk %d/%d",
                index,
                total_chunks,
            )
            break

        if on_progress is not None:
            try:
                await on_progress(f"blitz find-people prepass chunk {index}/{total_chunks}")
            except Exception as exc:  # noqa: BLE001 — progress is best-effort
                logger.debug("blitz prepass progress callback failed: %s", exc)

        chunk_max_pages = _effective_max_pages(max_pages, target_per_company, len(chunk))
        # Out-param contract with find_people_batch: filled with
        # {"truncated": bool, "pages": int} on every non-exception path.
        chunk_stats: dict[str, Any] = {}
        try:
            grouped = await blitz_client.find_people_batch(
                http,
                [url for _domain, url in chunk],
                job_title_include=effective_include or None,
                job_title_exclude=title_exclude or None,
                target_per_company=target_per_company,
                max_pages=chunk_max_pages,
                stats=chunk_stats,
            )
        except Exception as exc:  # noqa: BLE001 — chunk-level fallback, NOT a miss
            # 429/5xx after retries or a network error: fall back to the
            # per-domain waterfall for every domain in this chunk. Do NOT
            # record misses — Blitz never conclusively answered.
            logger.warning(
                "blitz find-people prepass chunk %d/%d failed (%s): %d domains fall back to waterfall",
                index,
                total_chunks,
                exc,
                len(chunk),
            )
            continue

        # Pagination stopped at the page cap while the server still offered
        # a cursor: zero-person companies here are NOT a definitive answer —
        # matches may exist past the last fetched page. Recording a
        # 'contacts' miss anyway would poison the domain for the miss
        # store's TTL (default 30 days) on an unanswered question.
        truncated = bool(chunk_stats.get("truncated"))
        if truncated:
            logger.info(
                "blitz find-people prepass chunk %d/%d truncated at max_pages=%d "
                "(cursor pending after %d pages) — contacts misses suppressed for %d domains",
                index,
                total_chunks,
                chunk_max_pages,
                chunk_stats.get("pages", 0),
                len(chunk),
            )

        for domain, url in chunk:
            key = blitz_client._normalize_company_url(url)
            persons = grouped.get(key) or []
            persons_by_domain[domain] = persons
            if not persons and not truncated:
                # Company resolved, pagination ran to a CONCLUSIVE end, and
                # Blitz returned zero matching people: conclusive contacts
                # miss. Still covered — the per-domain code treats this as
                # "blitz found nobody" and proceeds to the GetLeads
                # decision-makers fallback.
                _safe_record_miss(record_miss, domain, "contacts")
            prepass_domains.add(domain)

    return {
        "company_url_by_domain": company_url_by_domain,
        "persons_by_domain": persons_by_domain,
        "prepass_domains": prepass_domains,
        "skipped_miss": skipped_miss,
    }
