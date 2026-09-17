"""Lookalike seed resolution + profile synthesis for the Find Companies page.

"Start from example companies": the user pastes 3–5 companies (domains or
LinkedIn company URLs); we profile each one, synthesize the traits they share,
and shape a standard TAM-by-People search around those traits.

Resolution order per seed (free first, paid last):
  1. GetLeads contact search by domain — 1 credit; returns org name,
     LinkedIn-taxonomy industry, size band, revenue range, about text.
     (Domain seeds only.)
  2. Blitz company enrichment — ~1 FUP record; returns industry, size band,
     specialties, HQ, domain. Used for LinkedIn-URL seeds directly, and as
     the fallback when GetLeads cannot profile a domain seed.

Synthesis is DETERMINISTIC and explainable (no LLM): majority industry,
specialty keywords appearing in >=2 seeds (top 6), median size band, HQ
country only when shared by a majority. The route returns these as
suggestions; the UI renders them as removable chips and submits the final
filters explicitly — the machine proposes, the human approves.

Band formats differ by source and are normalized HERE: GetLeads
"11 to 50" <-> Blitz "11-50"; both use "10001+".
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any, Optional

import httpx

from enrichment import blitz_client
from enrichment import blitz_search
from enrichment import getleads_client

logger = logging.getLogger(__name__)

MAX_SEEDS = 5

# LinkedIn size bands, smallest to largest — used for median-band synthesis.
_SIZE_BANDS = [
    "1-10", "11-50", "51-200", "201-500", "501-1000",
    "1001-5000", "5001-10000", "10001+",
]


def normalize_band(raw: Optional[str]) -> Optional[str]:
    """Normalize a size band to Blitz's '11-50' form.

    GetLeads uses '11 to 50'; unknown shapes pass through unchanged (the
    TAM industry-enum fallback pattern does not apply here — employee_range
    is a fixed enum, so callers should drop bands not in _SIZE_BANDS)."""
    if not raw or not str(raw).strip():
        return None
    band = str(raw).strip().replace(" to ", "-").replace(" ", "")
    return band if band in _SIZE_BANDS else None


def _band_index(band: Optional[str]) -> Optional[int]:
    try:
        return _SIZE_BANDS.index(band or "")
    except ValueError:
        return None


def parse_seed(raw: str) -> dict[str, str]:
    """Classify one seed input as a bare domain or a LinkedIn company URL."""
    value = (raw or "").strip().lower().rstrip("/")
    if not value:
        return {"kind": "invalid", "value": ""}
    if "linkedin.com/company/" in value:
        return {"kind": "linkedin_url", "value": value}
    # strip scheme/path from bare domains
    domain = value.split("://", 1)[-1].split("/", 1)[0].removeprefix("www.")
    if domain and "." in domain and " " not in domain:
        return {"kind": "domain", "value": domain}
    return {"kind": "invalid", "value": raw.strip()}


async def resolve_seed(
    http: httpx.AsyncClient, parsed: dict[str, str]
) -> dict[str, Any]:
    """Profile one seed. Never raises — unresolvable seeds come back with
    resolved=False so the caller can warn and continue."""
    result: dict[str, Any] = {
        "input": parsed["value"], "kind": parsed["kind"],
        "resolved": False, "source": None, "name": None, "domain": None,
        "linkedin_url": None, "industry": None, "size_band": None,
        "specialties": [], "country": None,
    }
    try:
        if parsed["kind"] == "domain":
            result["domain"] = parsed["value"]
            gl = await getleads_client.search_contacts_companies(
                http, domains=[parsed["value"]], limit=1,
            )
            contact = (gl.get("contacts") or [None])[0]
            if contact:
                result.update({
                    "resolved": True, "source": "getleads",
                    "name": contact.get("org_company_name"),
                    "industry": contact.get("org_industry_linkedin"),
                    "size_band": normalize_band(contact.get("employee_count_range")),
                    "linkedin_url": contact.get("org_linkedin_url"),
                })
                return result
        elif parsed["kind"] == "linkedin_url":
            result["linkedin_url"] = parsed["value"]
        else:
            return result

        # GetLeads missed (or URL seed) -> Blitz company enrichment.
        company_url = result.get("linkedin_url") or parsed.get("value")
        if parsed["kind"] == "domain":
            d2l = await blitz_client.domain_to_linkedin(http, parsed["value"])
            company_url = (d2l or {}).get("company_linkedin_url")
            result["linkedin_url"] = company_url
        if not company_url:
            return result
        enrich = await blitz_search.company_enrich(http, company_url)
        company = enrich.get("company") or {}
        if not company:
            return result
        result.update({
            "resolved": True, "source": "blitz",
            "name": company.get("name"),
            "domain": company.get("domain") or result.get("domain"),
            "linkedin_url": company.get("linkedin_url") or company_url,
            "industry": company.get("industry"),
            "size_band": normalize_band(company.get("size")),
            "specialties": [s for s in (company.get("specialties") or []) if s][:12],
            "country": ((company.get("hq") or {}).get("country_code")),
        })
    except Exception as exc:
        logger.warning("Lookalike seed resolution failed for %s: %s",
                       parsed.get("value"), exc)
    return result


def synthesize_profile(seeds: list[dict[str, Any]]) -> dict[str, Any]:
    """Deterministic trait synthesis across resolved seeds.

    Returns chips + a human summary. Rules:
      - industries: majority (>= ceil(n_resolved/2)) shared industry, top 2
      - keywords:   specialties appearing in >=2 seeds, top 6 by frequency
      - size_band:  the median resolved band
      - countries:  HQ country only when shared by a majority
      - conflicting_profiles: True when no industry AND no keywords survive
        (the UI warns the run will be broad).
    """
    resolved = [s for s in seeds if s.get("resolved")]
    # Single-seed runs (very common: "lookalikes of THIS company") take that
    # seed's own traits; multi-seed runs require sharing (>=2 / majority).
    min_share = 1 if len(resolved) <= 1 else 2
    industries = Counter(
        s["industry"] for s in resolved if s.get("industry")
    )
    shared_industries = [
        ind for ind, n in industries.most_common(2)
        if n >= max(min_share, -(-len(resolved) // 2))
    ]
    specialty_counts: Counter = Counter()
    for s in resolved:
        for spec in set(s.get("specialties") or []):
            specialty_counts[spec.strip().lower()] += 1
    if len(resolved) == 1:
        keywords = [
            spec.strip().lower()
            for spec in (resolved[0].get("specialties") or [])[:6]
        ]
    else:
        keywords = [
            kw for kw, n in specialty_counts.most_common(12) if n >= 2
        ][:6]

    band_idxs = sorted(
        i for i in (_band_index(s.get("size_band")) for s in resolved)
        if i is not None
    )
    size_band = _SIZE_BANDS[band_idxs[len(band_idxs) // 2]] if band_idxs else None

    countries = Counter(s.get("country") for s in resolved if s.get("country"))
    shared_countries = [
        c for c, n in countries.most_common(2)
        if n >= max(min_share, -(-len(resolved) // 2))
    ]

    return {
        "industries": shared_industries,
        "keywords": keywords,
        "size_band": size_band,
        "countries": shared_countries,
        "conflicting_profiles": bool(resolved) and not shared_industries and not keywords,
        "seeds_resolved": len(resolved),
        "seeds_total": len(seeds),
    }


def seeds_display_list(raw_seeds: list[str]) -> str:
    """'a.com, b.com +1' style label for job display names."""
    parts = [str(s)[:30] for s in raw_seeds[:2]]
    extra = len(raw_seeds) - 2
    label = ", ".join(parts)
    if extra > 0:
        label += f" +{extra}"
    return label[:80]


def to_getleads_band(band: Optional[str]) -> Optional[str]:
    """Inverse of normalize_band: '11-50' -> '11 to 50' (GetLeads filter form)."""
    if not band:
        return None
    if band == "10001+":
        return band
    return band.replace("-", " to ") if "-" in band else band
