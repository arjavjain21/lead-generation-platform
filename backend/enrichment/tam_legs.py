"""Auxiliary TAM retrieval legs — GetLeads pull, contacts-DB backup, and
the Blitz strict-enum 422 fallback.

Extracted from ``tam_flow.py`` (Lookalike 2.0, 2026-09-18) to keep that
module under the file-size budget; ``tam_flow`` re-exports the private-named
helpers so existing callers/tests are unchanged.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from enrichment import contacts_client
from enrichment import getleads_client
from enrichment.lookalike import normalize_band

logger = logging.getLogger(__name__)

# 40 pages x 100 contacts is ample for a 500-company credit cap.
_GETLEADS_MAX_PAGES = 40


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
# Industry-enum fallback
# ---------------------------------------------------------------------------

def _industry_to_keywords_on_422(
    company_filters: dict[str, Any], body: str, *, already_used: bool
) -> Optional[dict[str, Any]]:
    """Rebuild company filters with ``industry`` moved into ``keywords``.

    Blitz's TAM ``industry`` filter is a STRICT ENUM (~700 exact taxonomy
    values like "Advertising Services"). The web UI's industry dropdown
    serves a different 43-value LinkedIn-style list, so any industry-picked
    TAM run 422s at the enum check. Rather than maintaining a second
    taxonomy copy, the failing industry selection is converted to the fuzzy
    ``keywords`` include filter (searches description/specialties/categories
    — semantically close for industry-style filtering).

    Returns the rebuilt filters when the fallback applies, else None:
    already attempted, industry not in the filters, or the 422 body does
    not look like an enum rejection (a different validation problem).
    """
    if already_used:
        return None
    if "industry" not in company_filters:
        return None
    if "Invalid option" not in body:
        return None
    rebuilt = dict(company_filters)
    industry = rebuilt.pop("industry") or {}
    include = list(industry.get("include") or [])
    exclude = list(industry.get("exclude") or [])
    if not include and not exclude:
        return None
    keywords = dict(rebuilt.get("keywords") or {})
    keywords["include"] = list(keywords.get("include") or []) + include
    keywords["exclude"] = list(keywords.get("exclude") or []) + exclude
    rebuilt["keywords"] = keywords
    return rebuilt


def _tam_validation_error(exc: httpx.HTTPStatusError) -> RuntimeError:
    """Readable error for a filter-validation 422 (Blitz's body lists the
    valid options — keep a snippet instead of the raw httpx text)."""
    snippet = ""
    try:
        snippet = (exc.response.text or "")[:300]
    except Exception:  # response already closed / streamed
        pass
    return RuntimeError(
        f"Blitz rejected the TAM filters (422 validation): {snippet}"
    )


# ---------------------------------------------------------------------------
# GetLeads lookalike leg
# ---------------------------------------------------------------------------

async def _getleads_company_pull(
    http: httpx.AsyncClient,
    *,
    getleads_filters: dict[str, Any],
    needed: int,
    existing_domains: set[str],
    exclude_domains: set[str],
    credits_cap: int,
) -> tuple[list[dict[str, Any]], int]:
    """Pull unique companies from GetLeads contact search (lookalike "getleads"/"both" sources).

    Contacts arrive with company fields riding on each row (org_company_name,
    org_domain — CAN be empty, org_industry_linkedin, employee_count_range,
    org_revenue_range); rows are deduped up to companies by domain-or-name.
    Stop conditions: enough unique companies, credits exhausted (1 credit per
    contact returned, accumulated from query_credits_used), has_more false,
    or the hard page cap. Returns (rows, credits_used). Never raises — a
    failed leg returns ([], 0) and the Blitz rows stand alone.
    """
    rows: list[dict[str, Any]] = []
    seen: set[str] = set(existing_domains) | set(exclude_domains)
    contact_counts: dict[str, int] = {}
    ordered: list[str] = []
    credits_used = 0
    offset = 0
    for _page in range(_GETLEADS_MAX_PAGES):
        if len(rows) >= needed or credits_used >= credits_cap:
            break
        page = await getleads_client.search_contacts_companies(
            http, limit=100, offset=offset, **getleads_filters,
        )
        contacts = page.get("contacts") or []
        credits_used += int(page.get("query_credits_used") or len(contacts))
        for contact in contacts:
            domain = (contact.get("org_domain") or "").strip().lower()
            name = (contact.get("org_company_name") or "").strip().lower()
            key = domain or f"name:{name}"
            if not key or key in seen:
                continue
            seen.add(key)
            ordered.append(key)
            contact_counts[key] = 1
            rows.append({
                "name": contact.get("org_company_name"),
                "domain": domain or None,
                "website": None,
                "linkedin_url": None,
                "industry": contact.get("org_industry_linkedin"),
                "type": None,
                "size": normalize_band(contact.get("employee_count_range")),
                "employees_on_linkedin": None,
                "followers": None,
                "founded_year": None,
                "hq_city": None, "hq_state": None,
                "hq_country_code": None, "hq_region": None,
                "revenue": contact.get("org_revenue_range"),
                "slogan": None,
                "employee_growth_1y": None,
                "matched_people": 1,
                "source": "getleads",
                "match_score": None,
                "why_matched": None,
            })
        if not page.get("has_more") or not contacts:
            break
        offset = page.get("next_offset") or (offset + len(contacts))
    for row, key in zip(rows, ordered):
        row["matched_people"] = contact_counts.get(key, 1)
    return rows, credits_used
