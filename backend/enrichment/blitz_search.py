"""Blitz search-surface endpoints — /v2/search/people batch + TAM-by-People.

Extracted from ``blitz_client.py`` (which exceeded the 800-line guideline).
Everything here is RE-EXPORTED from ``blitz_client`` (see the re-export block
there), so all existing import paths keep working unchanged; import this
module directly only for new code.

Transport contract: these functions call the shared retry/transport plumbing
via ``blitz_client._post_with_retry`` resolved at CALL time (function-level
import), NOT a module-level ``from ... import``. That is load-bearing twice
over:
  1. it keeps ``blitz_client -> blitz_search`` a one-way import (no circular
     import, whichever module loads first), and
  2. tests and tooling that monkeypatch ``blitz_client._post_with_retry``
     keep governing these functions through the re-export.
"""

from __future__ import annotations

import logging
from typing import Any, Optional
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


def _normalize_company_url(url: str) -> str:
    """Normalize a company LinkedIn URL for grouping (the batch attribution key).

    ``domain-to-linkedin`` answers and ``experiences[].company_linkedin_url``
    frequently differ in purely cosmetic forms. If they do not collapse to
    ONE key, ``find_people_batch`` silently drops the person and the prepass
    then records a FALSE definitive ``'contacts'`` miss (30-day negative-cache
    poison). Collapsed forms:
      - host case           ``WWW.LinkedIn.com/...`` == ``www.linkedin.com/...``
      - www prefix          ``linkedin.com/...`` == ``www.linkedin.com/...``
      - /company/<slug> case ``/company/Acme`` == ``/company/acme`` (LinkedIn
                            slugs are case-insensitive identifiers)
      - trailing slashes    ``/company/acme/`` == ``/company/acme``
      - query/fragment      ``/company/acme?trk=...`` == ``/company/acme``
      - scheme              ``https://`` == ``http://`` (dropped entirely)

    Non-string input -> "".
    """
    if not isinstance(url, str):
        return ""
    trimmed = url.strip()
    # Strip query/fragment BEFORE parsing so schemeless inputs lose them too.
    for separator in ("?", "#"):
        trimmed = trimmed.split(separator, 1)[0]
    while trimmed.endswith("/"):
        trimmed = trimmed[:-1]
    parsed = urlparse(trimmed)
    if not parsed.netloc:
        # Schemeless "linkedin.com/company/x": re-parse with a // prefix so
        # the host is recognized instead of treating the whole URL as a path.
        parsed = urlparse(f"//{trimmed}")
    host = (parsed.netloc or "").lower()
    if not host:
        return trimmed.lower()
    if host.startswith("www."):
        host = host[len("www."):]
    path_parts = (parsed.path or "").split("/")
    if len(path_parts) >= 3 and path_parts[1].lower() == "company":
        path_parts[1] = "company"
        path_parts[2] = path_parts[2].lower()
    return f"{host}{'/'.join(path_parts)}"


def bracket_exact(titles: list[str]) -> list[str]:
    """Wrap each title in ``[...]`` for Blitz server-side EXACT title matching.

    Blitz's job-title filter treats a bare value as a fuzzy/contains match;
    ``[value]`` is exact (case- and accent-insensitive). Exact matching is
    the cheapest FUP lever for strict-title jobs: the server only returns
    people whose title IS one of the listed titles instead of a fuzzy
    superset the local title gate then discards.

    Pure function — returns a new list, never mutates the input.
    """
    return [f"[{title}]" for title in titles]


def _current_experience(person: dict[str, Any]) -> Optional[dict[str, Any]]:
    """First experiences[] entry with job_is_current=true, else None."""
    experiences = person.get("experiences")
    if not isinstance(experiences, list):
        return None
    for experience in experiences:
        if isinstance(experience, dict) and experience.get("job_is_current"):
            return experience
    return None


def _append_batch_person(
    grouped: dict[str, list[dict[str, Any]]],
    counts: dict[str, int],
    person: dict[str, Any],
) -> None:
    """Map one /v2/search/people result to a waterfall-flat row and append it
    under the person's CURRENT company (first experiences[] entry with
    job_is_current=true, normalized). Unattributable persons are skipped."""
    current = _current_experience(person)
    key = _normalize_company_url((current or {}).get("company_linkedin_url") or "")
    if not key or key not in grouped:
        logger.debug(
            "blitz find_people_batch: person without attributable current company skipped"
        )
        return

    first_name = person.get("first_name") or ""
    last_name = person.get("last_name") or ""
    full_name = person.get("full_name") or f"{first_name} {last_name}".strip()
    location = person.get("location") if isinstance(person.get("location"), dict) else {}
    counts[key] += 1
    grouped[key].append({
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "title": (current or {}).get("job_title") or "",
        "job_level": None,
        "linkedin_url": person.get("linkedin_url") or "",
        "email": None,
        "verified_email": None,
        "headline": person.get("headline") or "",
        "location_city": location.get("city") or "",
        "location_country": location.get("country_code") or "",
        "icp_tier": 1,
        "ranking": counts[key],
        "experiences": person.get("experiences") or [],
    })


async def find_people_batch(
    client: httpx.AsyncClient,
    company_linkedin_urls: list[str],
    *,
    job_title_include: Optional[list[str]] = None,
    job_title_exclude: Optional[list[str]] = None,
    job_levels: Optional[list[str]] = None,
    target_per_company: int = 5,
    max_pages: int = 6,
    stats: Optional[dict[str, Any]] = None,
) -> dict[str, list[dict[str, Any]]]:
    """POST /v2/search/people — batch decision-maker discovery across <=50 companies.

    One call (plus cursor pages) replaces N per-domain waterfall_icp_search
    calls. Companies are identified by ``company.linkedin_url`` (max 50 per
    call — chunk upstream); people filters follow the PeopleFilter contract:
    ``job_title.include`` / ``job_title.exclude`` (wrap values in ``[...]``
    via bracket_exact() for server-side exact matching) and ``job_level``.

    Cost note: 1 FUP record per result returned; an empty page costs 0
    records. Pagination stops as soon as (a) the response cursor is null
    (end of results), (b) EVERY requested company already has
    ``target_per_company`` people, or (c) ``max_pages`` pages were fetched.
    People already returned by a fetched page are always kept — records are
    billed when the page arrives, so truncating locally saves nothing.

    Each person is mapped to the WATERFALL-FLAT row shape consumed
    downstream (mirrors the rows list_builder._search_company_waterfall
    builds): first/last/full name, title (from the CURRENT experience — the
    first experiences[] entry with job_is_current=true), job_level None
    (resolved downstream), linkedin_url, email/verified_email None (the
    email cascade fills them), headline, location_city/location_country,
    icp_tier 1, per-company 1-based ranking, and the raw experiences[] list
    kept verbatim for downstream extraction (domains, past jobs). Persons
    whose current experience has no company_linkedin_url, or whose company
    is not among the requested URLs, cannot be attributed and are skipped.

    Args:
        stats: optional mutable out-param (kept as an out-param so the
            return shape stays stable for existing callers). When provided it
            is filled with ``{"truncated": bool, "pages": int}``:
            ``truncated`` is True ONLY when pagination stopped at the
            ``max_pages`` cap while the server still offered a cursor and at
            least one requested company was still below target — i.e. more
            matches MAY exist past the last fetched page, so an empty result
            for a company is NOT a definitive miss. A null cursor or every
            company reaching target is a definitive end (``truncated`` False).

    Returns:
        {normalized_company_url: [waterfall-flat rows]} — every requested
        URL (lowercase host, "www." stripped, /company/<slug> lowercased,
        trailing slash + query/fragment stripped) is a key, with an empty
        list when nobody matched.

    Errors: a 429/5xx that survives retries raises httpx.HTTPStatusError
    (network failures raise httpx.TransportError) straight out of
    _post_with_retry. Propagation is INTENTIONAL and unwrapped — the batch
    prepass (enrichment/blitz_batch.py) catches per-chunk and falls back to
    the per-domain waterfall for the chunk's domains.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    counts: dict[str, int] = {}
    for raw_url in company_linkedin_urls:
        key = _normalize_company_url(raw_url)
        if key and key not in grouped:
            grouped[key] = []
            counts[key] = 0
    if not grouped:
        if stats is not None:
            stats["truncated"] = False
            stats["pages"] = 0
        return grouped

    people_filter: dict[str, Any] = {}
    if job_title_include or job_title_exclude:
        title_filter: dict[str, Any] = {}
        if job_title_include:
            title_filter["include"] = job_title_include
        if job_title_exclude:
            title_filter["exclude"] = job_title_exclude
        people_filter["job_title"] = title_filter
    if job_levels:
        people_filter["job_level"] = job_levels

    # Late import: keeps blitz_client -> blitz_search one-way AND makes
    # `blitz_client._post_with_retry` monkeypatching govern this function.
    from enrichment import blitz_client

    cursor: Optional[str] = None
    pages = 0
    truncated = False
    while pages < max_pages:
        payload: dict[str, Any] = {
            "company": {"linkedin_url": company_linkedin_urls},
            "max_results": 50,
        }
        if people_filter:
            payload["people"] = people_filter
        if cursor is not None:
            payload["cursor"] = cursor

        # No try/except on purpose: retries exhausted -> the httpx error
        # propagates so blitz_batch can fall back per-chunk (see docstring).
        response = await blitz_client._post_with_retry(
            client,
            f"{blitz_client.BLITZ_BASE_URL}/v2/search/people",
            payload,
            timeout=60.0,
        )
        pages += 1

        for person in response.get("results") or []:
            if isinstance(person, dict):
                _append_batch_person(grouped, counts, person)

        cursor = response.get("cursor")
        if not cursor:
            # Server said "no more results" — definitive end.
            truncated = False
            break
        if all(count >= target_per_company for count in counts.values()):
            # Every company got its target — demand met, definitive end.
            truncated = False
            break
        if pages >= max_pages:
            # Page cap hit while the cursor still points at more results and
            # at least one company is below target: matches may exist past
            # the last fetched page — NOT a definitive end.
            truncated = True

    if stats is not None:
        stats["truncated"] = truncated
        stats["pages"] = pages

    return grouped


async def tam_by_people(
    client: httpx.AsyncClient,
    *,
    company_filters: dict[str, Any],
    people_filters: dict[str, Any],
    max_results: int = 50,
    cursor: Optional[str] = None,
) -> dict[str, Any]:
    """POST /v2/company/tam-by-people — persona + firmographic TAM search.

    Passthrough: ``company_filters`` and ``people_filters`` are the same
    filter surfaces /v2/search/people accepts (CompanyFilter / PeopleFilter
    dicts, e.g. ``{"employee_range": ["11-50"], "hq": {...}}`` and
    ``{"job_title": {"include": ["[CEO]"]}, "job_level": ["owner", "c_suite"]}``).

    Response: ``{"results": [...], "cursor": str | null}`` where each result
    is ``{"company": {...}, "matched_people": ...}`` and company carries
    {linkedin_url, linkedin_id (number), name, about, specialties[],
    industry, type, size, employees_on_linkedin, followers, founded_year,
    hq{city, state, country_code, country_name, region, continent},
    domain (NULLABLE — absent when Blitz cannot infer it; never assume it),
    website, slogan, revenue, employee_growth[{percentage, timespan}]}.
    ``cursor`` null means the last page.

    Cost: 1 FUP record per result returned; an empty page costs 0.
    """
    payload: dict[str, Any] = {
        "company": company_filters,
        "people": people_filters,
        "max_results": max_results,
    }
    if cursor is not None:
        payload["cursor"] = cursor

    # Late import: see module docstring (one-way import + patchability).
    from enrichment import blitz_client

    return await blitz_client._post_with_retry(
        client,
        f"{blitz_client.BLITZ_BASE_URL}/v2/company/tam-by-people",
        payload,
        timeout=60.0,
    )
