"""Safe company-name resolution (PR-2.2, 2026-11-11).

Turns a name-only row's company name into a domain — CONSERVATIVELY. The
cardinal rule: an ambiguous name NEVER auto-joins a company; it is returned
as a reviewable candidate list so a human decides (the whole point of the
review directive "ambiguous matches must be reviewable and must not
autojoin").

Sources (in order):
1. Internal Contacts DB company search (FREE, 75 RPS lane) — primary.
2. Blitz /v2/search/companies by name (1 credit per 5 results, env-gated
   OFF by default: ENABLE_NAME_RESOLVER_BLITZ) — optional paid fallback.

Confidence:
  high      exactly one exact-normalized-name match
  medium    multiple matches but exactly one shares the country hint
  ambiguous multiple matches, no discriminator (NEVER auto-joined)
  none      zero matches / source unavailable

Kill switch ENABLE_NAME_RESOLVER (default false) arms the whole module —
callers treat 'none' as name_unresolved when it is off. Results cached in
the name_resolution table (TTL 14d), keyed (name_norm, country_hint).
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from . import identifier_utils

logger = logging.getLogger(__name__)

TTL_DAYS = int(os.getenv("NAME_RESOLVER_TTL_DAYS", "14"))

# Legal suffixes stripped for name normalization (lowercase, order-free).
_SUFFIXES = {
    "inc", "inc.", "llc", "l.l.c", "ltd", "ltd.", "limited", "plc", "gmbh",
    "ag", "sa", "s.a.", "nv", "bv", "pvt", "pvt.", "pte", "pte.", "co",
    "co.", "corp", "corp.", "corporation", "company", "holdings",
    "group", "the",
}


def normalize_company_name(name: str) -> str:
    """Lowercase, punctuation-collapsed, legal-suffix-stripped company name."""
    cleaned = re.sub(r"[^\w\s]", " ", str(name or "").lower())
    tokens = [t for t in cleaned.split() if t]
    while tokens and tokens[-1] in _SUFFIXES:
        tokens.pop()
    while tokens and tokens[0] in _SUFFIXES:
        tokens.pop(0)
    return " ".join(tokens)


def resolver_enabled() -> bool:
    return os.environ.get("ENABLE_NAME_RESOLVER", "false").lower() == "true"


def blitz_fallback_enabled() -> bool:
    return os.environ.get("ENABLE_NAME_RESOLVER_BLITZ", "false").lower() == "true"


@dataclass
class ResolveResult:
    status: str  # high | medium | ambiguous | none
    domain: str = ""
    company_linkedin_url: str = ""
    country: str = ""
    candidates: list[dict[str, Any]] = field(default_factory=list)
    source: str = ""


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS name_resolution (
            name_norm     TEXT NOT NULL,
            country_hint  TEXT NOT NULL DEFAULT '',
            result_json   TEXT NOT NULL,
            resolved_at   TEXT NOT NULL,
            PRIMARY KEY (name_norm, country_hint)
        )
        """
    )
    conn.commit()


def _cache_get(conn: sqlite3.Connection, name_norm: str, country_hint: str) -> Optional[ResolveResult]:
    import json

    try:
        row = conn.execute(
            "SELECT result_json, resolved_at FROM name_resolution "
            "WHERE name_norm=? AND country_hint=?",
            (name_norm, country_hint),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    try:
        resolved_at = datetime.fromisoformat(row[1])
        if resolved_at.tzinfo is None:
            resolved_at = resolved_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - resolved_at > timedelta(days=TTL_DAYS):
            return None
        return ResolveResult(**json.loads(row[0]))
    except (ValueError, TypeError):
        return None


def _cache_put(conn: sqlite3.Connection, name_norm: str, country_hint: str, result: ResolveResult) -> None:
    import json

    try:
        conn.execute(
            "INSERT OR REPLACE INTO name_resolution "
            "(name_norm, country_hint, result_json, resolved_at) VALUES (?, ?, ?, ?)",
            (name_norm, country_hint, json.dumps(result.__dict__),
             datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    except sqlite3.Error as cache_err:
        logger.debug("name_resolution cache write failed: %s", cache_err)


def _extract_domain(candidate: dict[str, Any]) -> str:
    for key in ("website", "domain", "website_norm", "company_domain"):
        val = identifier_utils.normalize_domain(candidate.get(key) or "")
        if val:
            return val
    return ""


async def resolve_company_name(
    contacts_http,
    name: str,
    *,
    country_hint: str = "",
    conn: Optional[sqlite3.Connection] = None,
    blitz_http=None,
) -> ResolveResult:
    """Resolve a company name to a domain. NEVER auto-joins an ambiguous name.

    ``contacts_http``/``blitz_http`` are the shared httpx clients. ``conn``
    is the jobs.db connection for the cache table.
    """
    name_norm = normalize_company_name(name)
    if not name_norm:
        return ResolveResult(status="none")
    if not resolver_enabled():
        return ResolveResult(status="none")
    country_hint = (country_hint or "").strip().lower()

    if conn is not None:
        _ensure_table(conn)
        cached = _cache_get(conn, name_norm, country_hint)
        if cached is not None:
            return cached

    result = await _resolve_uncached(contacts_http, name, name_norm, country_hint, blitz_http)

    if conn is not None:
        _cache_put(conn, name_norm, country_hint, result)
    return result


def _score(candidate_name: str, name_norm: str) -> float:
    cand = normalize_company_name(candidate_name)
    if not cand:
        return 0.0
    if cand == name_norm:
        return 1.0
    if cand.startswith(name_norm) or name_norm.startswith(cand):
        return 0.7
    return 0.0


async def _resolve_uncached(contacts_http, name, name_norm, country_hint, blitz_http) -> ResolveResult:
    from . import contacts_client

    candidates: list[dict[str, Any]] = []
    source = "contacts_db"

    # Source 1: internal contacts DB (free).
    try:
        found = await contacts_client.search_companies_by_name(contacts_http, name, limit=10)
        candidates = [
            {
                "name": c.get("name", ""),
                "domain": _extract_domain(c),
                "linkedin_url": c.get("linkedin_url", ""),
                "country": str(c.get("country", "") or "").lower(),
            }
            for c in (found or [])
        ]
    except Exception as exc:
        logger.info("name resolver contacts_db search failed for %r: %s", name, exc)

    # Source 2: Blitz name search — env-gated paid fallback.
    if not candidates and blitz_fallback_enabled() and blitz_http is not None:
        from . import blitz_client

        source = "contacts_db+blitz"
        try:
            res = await blitz_client.company_search(blitz_http, name=name, limit=10)
            candidates = [
                {
                    "name": c.get("name", ""),
                    "domain": identifier_utils.normalize_domain(c.get("domain") or ""),
                    "linkedin_url": c.get("linkedin_url", ""),
                    "country": str(c.get("country_code", "") or "").lower(),
                }
                for c in (res.get("results") or [])
            ]
        except Exception as exc:
            logger.info("name resolver blitz fallback failed for %r: %s", name, exc)

    scored = [
        {**c, "score": _score(c["name"], name_norm)}
        for c in candidates
    ]
    strong = [c for c in scored if c["score"] >= 0.7 and (c["domain"] or c["linkedin_url"])]
    strong.sort(key=lambda c: -c["score"])

    if len(strong) == 1:
        c = strong[0]
        return ResolveResult(status="high", domain=c["domain"],
                             company_linkedin_url=c["linkedin_url"],
                             country=c["country"], candidates=strong, source=source)
    if len(strong) > 1:
        exact = [c for c in strong if c["score"] == 1.0]
        pool = exact if len(exact) >= 2 else strong
        if country_hint:
            matching = [c for c in pool if c["country"] == country_hint]
            if len(matching) == 1:
                c = matching[0]
                return ResolveResult(status="medium", domain=c["domain"],
                                     company_linkedin_url=c["linkedin_url"],
                                     country=c["country"], candidates=pool, source=source)
        # AMBIGUOUS — reviewable candidates, never auto-joined.
        return ResolveResult(status="ambiguous", candidates=pool, source=source)
    return ResolveResult(status="none", candidates=scored, source=source)
