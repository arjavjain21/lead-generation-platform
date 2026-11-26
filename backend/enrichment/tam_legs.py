"""Auxiliary TAM retrieval legs — GetLeads pull, contacts-DB backup, and
the Blitz strict-enum 422 fallback.

Extracted from ``tam_flow.py`` (Lookalike 2.0, 2026-09-18) to keep that
module under the file-size budget; ``tam_flow`` re-exports the private-named
helpers so existing callers/tests are unchanged.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import httpx

from enrichment import contacts_client
from enrichment import getleads_client
from enrichment import leadocean_industries
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


# ---------------------------------------------------------------------------
# QuickEnrich + LeadOcean discovery legs (2026-11-11) - owner direction:
# shift TAM discovery from credit-limited Blitz/GetLeads onto unlimited QE
# and the 25M/mo LO search window.
# ---------------------------------------------------------------------------

def _kw_includes(keyword_filter: Any) -> list[str]:
    """Include values from a Blitz-style {include:[...], exclude:[...]} filter."""
    if isinstance(keyword_filter, dict):
        return [str(v) for v in (keyword_filter.get("include") or []) if v]
    return []


def _kw_excludes(keyword_filter: Any) -> list[str]:
    """Exclude values from a Blitz-style {include:[...], exclude:[...]} filter."""
    if isinstance(keyword_filter, dict):
        return [str(v) for v in (keyword_filter.get("exclude") or []) if v]
    return []


def _tam_filter_values(company_filters: dict[str, Any]) -> dict[str, list[str]]:
    """Normalize a Blitz CompanyFilter payload into flat include/exclude
    lists for the QE/LO legs (accepts BOTH the payload shape
    {industry:{include,exclude}} and the request shape
    {industry_include:[...], industry_exclude:[...]})."""
    def vals(key, alt, part):
        # payload shape first (dict), then flat request list
        kw = company_filters.get(key)
        if isinstance(kw, dict):
            items = kw.get(part) or []
        else:
            items = company_filters.get(alt) or [] if part == "include" else company_filters.get(alt) or []
        return list(dict.fromkeys(str(v) for v in items if v))

    out = {
        "industry": vals("industry", "industry_include", "include"),
        "industry_exclude": vals("industry", "industry_exclude", "exclude"),
        "keywords": vals("keywords", "keywords_include", "include"),
        "keywords_exclude": vals("keywords", "keywords_exclude", "exclude"),
        "name": vals("name", "name_include", "include"),
        "type": vals("type", "type_include", "include"),
    }
    hq = company_filters.get("hq") or {}
    out["country"] = [str(v) for v in (hq.get("country_code") or company_filters.get("hq_country_code") or []) if v]
    return out


def _kw_values(keyword_filter: Any) -> list[str]:
    """Extract include values from a Blitz-style {include:[...]} keyword filter."""
    if isinstance(keyword_filter, dict):
        return [str(v) for v in (keyword_filter.get("include") or []) if v]
    return []


QE_FINDER_MAX_ROWS = 2000
QE_FINDER_MAX_PAGES = 20
LO_SEARCH_MAX_ROWS = 1000
LO_SEARCH_MAX_PAGES = 10


def _env_int(name: str, default: int) -> int:
    """Env override read at CALL time (repo convention — see env_crashloop
    RCA: module-level int() froze test/ops overrides at import)."""
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _qe_company_from_record(rec: dict[str, Any]) -> dict[str, Any]:
    domain = (rec.get("final_email_domain") or rec.get("email_domain") or rec.get("url") or "").strip().lower()
    if domain and "." not in domain:
        domain = ""
    return {
        "name": rec.get("company_name") or "",
        "domain": domain,
        "linkedin_url": rec.get("linkedin_url") or "",
        "industry": rec.get("industry") or "",
        "size": rec.get("employee_count") or "",
        "revenue": rec.get("revenue") or "",
        "matched_people": 1,
        "source": "quickenrich",
    }


def _qe_company_from_contact(rec: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """(dedupe key, TAM row) from a contact-finder record."""
    domain = (rec.get("email_domain") or rec.get("company_url") or "").strip().lower()
    if domain and "." not in domain:
        domain = ""
    return domain or ("name:" + (rec.get("company_name") or "").strip().lower()), {
        "name": rec.get("company_name") or "",
        "domain": domain,
        "linkedin_url": rec.get("company_linkedin_url") or rec.get("company_linkedin") or "",
        "industry": rec.get("industry") or rec.get("company_industry") or "",
        "size": rec.get("company_employee_count") or rec.get("number_of_employees") or "",
        "revenue": rec.get("company_revenue") or "",
        "matched_people": 1,  # accumulated by the caller per key
        "source": "quickenrich",
    }


async def quickenrich_company_pull(
    http,
    company_filters: dict[str, Any],
    people_filters: dict[str, Any],
    needed: int,
    existing_keys: set,
    exclude_domains: set,
) -> tuple[list[dict[str, Any]], int]:
    """QuickEnrich discovery: contact-finder when people filters exist (true
    'companies that employ matching people' semantics, real match counts),
    else company-finder. Bounded by TAM_QUICKENRICH_MAX_ROWS/PAGES. Never
    raises - errors return ([], 0)."""
    from enrichment import quickenrich_client as qe

    if needed <= 0:
        return [], 0
    titles = list(people_filters.get("job_title_include")
                  or _kw_values(people_filters.get("job_title")) or [])
    fv = _tam_filter_values(company_filters)
    payload_common = {
        "industry": fv["industry"] or None,
        "country_code": fv["country"] or None,
        "employee_count": [b for b in (company_filters.get("employee_range") or []) if b] or None,
        "keywords": fv["keywords"] or None,
        "keywords_exclude": fv["keywords_exclude"] or None,
    }
    rows: list[dict[str, Any]] = []
    seen = set(existing_keys)
    pages = 0
    cursor = None
    match_counts: dict[str, int] = {}
    row_cap = min(needed, _env_int("TAM_QUICKENRICH_MAX_ROWS", QE_FINDER_MAX_ROWS))
    try:
        while len(rows) < row_cap and pages < _env_int("TAM_QUICKENRICH_MAX_PAGES", QE_FINDER_MAX_PAGES):
            pages += 1
            if titles:
                industry_vals = payload_common["industry"] or None
                result = await qe.contact_finder(
                    http, title=titles, cursor=cursor,
                    industry=industry_vals,
                    country_code=payload_common["country_code"],
                    employee_count=payload_common["employee_count"] or None,
                )
                if isinstance(result, dict) and result.get("error") == "validation":
                    result = await qe.contact_finder(
                        http, title=titles, cursor=cursor,
                        country_code=payload_common["country_code"],
                        employee_count=payload_common["employee_count"] or None,
                    )
                contacts = result.get("contacts") if isinstance(result, dict) else None
                if contacts is None:
                    break
                for c in contacts:
                    key, row = _qe_company_from_contact(c)
                    if not key or key in seen:
                        continue
                    if row["domain"] and row["domain"] in exclude_domains:
                        continue
                    seen.add(key)
                    match_counts[key] = match_counts.get(key, 0) + 1
                    rows.append(row)
                cursor = result.get("next_cursor")
            else:
                industry_vals = payload_common["industry"] or None
                kw_vals = payload_common["keywords"] or None
                result = await qe.company_finder(
                    http, cursor=cursor,
                    industry=industry_vals,
                    country_code=payload_common["country_code"],
                    employee_count=payload_common["employee_count"] or None,
                    keywords=kw_vals,
                    exclude_keywords=payload_common["keywords_exclude"],
                )
                if isinstance(result, dict) and result.get("error") == "validation":
                    # QE industry enum rejected the value(s) — retry with the
                    # industry terms as free-text keywords (mirrors the Blitz
                    # 422 industry→keywords fallback; the failed request does
                    # not count against results).
                    merged = list(dict.fromkeys((kw_vals or []) + (industry_vals or [])))
                    result = await qe.company_finder(
                        http, cursor=None,
                        country_code=payload_common["country_code"],
                        employee_count=payload_common["employee_count"] or None,
                        keywords=merged or None,
                        exclude_keywords=payload_common["keywords_exclude"],
                    )
                companies = result.get("companies") if isinstance(result, dict) else None
                if companies is None:
                    break
                for rec in companies:
                    row = _qe_company_from_record(rec)
                    key = row["domain"] or ("name:" + row["name"].strip().lower())
                    if not key or key in seen:
                        continue
                    if row["domain"] and row["domain"] in exclude_domains:
                        continue
                    seen.add(key)
                    rows.append(row)
                cursor = result.get("next_cursor")
            if not cursor or not result.get("has_more", bool(cursor)):
                if not cursor:
                    break
    except Exception as exc:
        logger.warning("QuickEnrich TAM pull failed: %s", exc)
        return rows[:row_cap], pages
    return rows[:row_cap], pages


async def leadocean_company_pull(
    http,
    company_filters: dict[str, Any],
    needed: int,
    existing_keys: set,
    exclude_domains: set,
) -> tuple[list[dict[str, Any]], int]:
    """LeadOcean /v1/companies/search discovery - 1 search record per row on
    the 25M/mo window, bounded by TAM_LEADOCEAN_MAX_ROWS/PAGES. Never
    raises."""
    from enrichment import leadocean_client as lo

    if needed <= 0:
        return [], 0
    fv = _tam_filter_values(company_filters)
    industries = fv["industry"]
    # Only validated enum values pass through (invalid labels silently drop —
    # an empty industry list simply means "not filtered by industry").
    industries = [i for i in industries if leadocean_industries.is_valid_industry(i)]
    params: dict[str, Any] = {}
    if industries:
        params["industry"] = industries
    if fv["industry_exclude"]:
        params["excludeIndustry"] = fv["industry_exclude"]
    if fv["type"]:
        params["companyType"] = fv["type"]
    if company_filters.get("employee_range"):
        params["employeeRange"] = list(company_filters["employee_range"])
    if fv["country"]:
        params["hqCountry"] = fv["country"]
    if fv["keywords"]:
        params["keywords"] = fv["keywords"]
    if fv["keywords_exclude"]:
        params["excludeKeywords"] = fv["keywords_exclude"]
    if fv["name"]:
        params["q"] = " ".join(fv["name"][:3])
    if not params:
        return [], 0  # untargeted pull = burn search records; refuse

    rows: list[dict[str, Any]] = []
    seen = set(existing_keys)
    pages = 0
    cursor = None
    lo_cap = min(needed, _env_int("TAM_LEADOCEAN_MAX_ROWS", LO_SEARCH_MAX_ROWS))
    try:
        while len(rows) < lo_cap and pages < _env_int("TAM_LEADOCEAN_MAX_PAGES", LO_SEARCH_MAX_PAGES):
            pages += 1
            result = await lo.search_companies(http, params, cursor=cursor)
            companies = result.get("companies") if isinstance(result, dict) else None
            if companies is None:
                break
            for row in companies:
                key = row.get("domain") or ("name:" + (row.get("name") or "").strip().lower())
                if not key or key in seen:
                    continue
                if row.get("domain") and row["domain"] in exclude_domains:
                    continue
                seen.add(key)
                row["matched_people"] = 1
                row["source"] = "leadocean"
                # Fill the TAM geo column from the normalized country (the
                # finder returns it as `country`; the CSV schema speaks
                # hq_country_code — blank cells were hiding the filter's work).
                if row.get("country") and not row.get("hq_country_code"):
                    row["hq_country_code"] = row.pop("country")
                rows.append(row)
            cursor = result.get("next_cursor")
            if not cursor:
                break
    except Exception as exc:
        logger.warning("LeadOcean TAM pull failed: %s", exc)
        return rows[:lo_cap], pages
    return rows[:lo_cap], pages
