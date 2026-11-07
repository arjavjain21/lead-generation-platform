"""
LeadOcean API client — work-email waterfall tier 4 (api.leadocean.io).

LeadOcean (Agency · UGC plan, 2026-11-06): $247/mo, 1M enrichments/day,
25M search records/month, 200 RPS account-wide, 5 keys (one used).
Position: AFTER quickenrich, BEFORE getleads — the team's own test report
(docs/LEADOCEAN_PROVIDER_TEST_REPORT.md) measured 82% email yield on the
exact cohort this position sees (people QuickEnrich failed on).

Live-verified contract (probe session 2026-11-06, ~6 records):
- Auth: ``x-api-key`` header. Key carries scopes [enrich, search].
- ``GET /v2/people/email/work?linkedin_url=`` → ``{success, data: V2Email | null}``
  where V2Email = {email, status, verified_batch_date}. ``data: null`` =
  profile exists but no work email. HTTP 404 = no profile at all.
- ``GET /v2/people/email/personal?linkedin_url=`` → ``{success, data: [{email,
  verified_batch_date} | ...]}`` — RANKED list, NO status field (consumer
  domains are never vendor-verified; supplier-asserted only). Live proof of
  the fallback's value: bill gates → work null, 3 personal addresses.
- No X-Quota headers on healthy responses (only x-source/x-request-id) —
  quota truth is the free ``/v1/usage`` + ``/v1/account`` endpoints; throttle
  state surfaces as ``meta.notice`` on responses once past a window.
- Billing: 1 record per lookup WHETHER OR NOT IT FINDS ANYTHING. Misses pay
  → the miss store (leadocean_guards, 7d TTL) is budget protection.
- Daily wall: past 1M/day the API throttles ~400x rather than erroring —
  never wait it out; a lazy usage check (once per 15 min per process) arms
  the cross-worker cooldown until resetsAt so every worker skips instantly.

Status policy (email_status, probe+schema verified):
- ``verified`` (their verifier probe-sent, business addresses only) → accept,
  verified_hint "yes".
- ``catch_all`` / ``untested`` / ``unknown`` / ``risky`` / ``derived`` →
  accept with hint "unknown" — the mailtester gate at the call site decides
  (identical to Blitz unverified emails).
- ``invalid`` / ``role`` / ``abuse`` / ``spam_trap`` (LEADOCEAN_REJECT_STATUS)
  → the work address is discarded and the PERSONAL fallback runs (a bad work
  address says nothing about the person's personal mailbox).
- Personal addresses carry no status → always verified_hint "unknown"
  (mailtester-gated at call sites).

House return-shape convention (see moltsets/quickenrich clients):
- found → dict (found: True, email, email_type, status, verified_hint, ...)
- clean miss / cooldown / lane-busy skip / rejected-grade → None
- terminal account errors → falsy ``pipeline._ProviderError``
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any, Optional

import httpx

from . import leadocean_guards
from . import leadocean_industries
from . import pipeline  # For _ProviderError (terminal-error signaling)
from shared import rate_limiter
from shared.env_utils import env_float

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.getenv("LEADOCEAN_BASE_URL", "https://api.leadocean.io").rstrip("/")
API_KEY = os.getenv("LEADOCEAN_API_KEY", "")

# 200 RPS is ACCOUNT-wide (owner decision 2026-11-06: single key, assume the
# cap spans keys). Run at 75% so other consumers of the same account (e.g.
# the arjav-hermes key seen on /v1/account) keep headroom.
_RATE_LIMIT_RPS = env_float("LEADOCEAN_RATE_LIMIT_RPS", "150")
_REFILL_PER_SEC = float(_RATE_LIMIT_RPS)
# ⚠ capacity >= 1.0 or the shared limiter livelocks (grants need a whole
# token; found live 2026-10-14 on moltsets at 30 RPM).
_CAPACITY = max(1.0, env_float("LEADOCEAN_RATE_LIMIT_BURST", "25"))

_MAX_RETRIES = 2
_BASE_BACKOFF = 0.5
_MAX_BACKOFF = 5.0
_REQUEST_TIMEOUT = 15.0

# Bounded-wait skip: a saturated lane SKIPS this person's LeadOcean attempt
# (no miss marker — retried on a later pass) instead of parking on the rate
# limiter while holding a pipeline concurrency slot.
_MAX_QUEUE_WAIT = env_float("LEADOCEAN_MAX_QUEUE_WAIT", "10.0")

# Lazy free-usage check cadence: at most one /v1/usage per process per window.
_USAGE_CHECK_INTERVAL_S = 900.0  # 15 minutes

# ---- Phase 2: domain→people discovery lane (search quota, NOT enrich) ----
# GET /v1/companies/{domain}/people bills 1 SEARCH record per returned row
# against the 25M/month window (live-verified 2026-11-07: pace.search moved
# 2 → 127 during probing). The row cap bounds worst-case spend per domain;
# page size 50 is the largest the vendor accepted in probing.
_DISCOVERY_PAGE_SIZE = max(1, min(100, int(env_float("LEADOCEAN_DISCOVERY_PAGE_SIZE", "50"))))
_DISCOVERY_MAX_ROWS = max(_DISCOVERY_PAGE_SIZE, int(env_float("LEADOCEAN_DISCOVERY_MAX_ROWS", "100")))
_DISCOVERY_MAX_PAGES = max(1, (_DISCOVERY_MAX_ROWS // _DISCOVERY_PAGE_SIZE) + 1)


def _discovery_enabled() -> bool:
    """Fine-grained kill switch for the discovery lane only (tier-4 email
    enrichment stays up when this is off) — ENABLE_BLITZ_FIND_PEOPLE_BATCH
    pattern."""
    return os.environ.get("ENABLE_LEADOCEAN_DISCOVERY", "true").lower() == "true"


def _is_enabled() -> bool:
    return os.environ.get("ENABLE_LEADOCEAN", "true").lower() == "true"


def _rejected_statuses() -> frozenset[str]:
    raw = os.getenv("LEADOCEAN_REJECT_STATUS", "invalid,role,abuse,spam_trap")
    return frozenset(s.strip().lower() for s in raw.split(",") if s.strip())


def _provider_error(method: str, error_type: str, message: str):
    return pipeline._ProviderError(
        provider="leadocean",
        method=method,
        error_type=error_type,
        message=f"LeadOcean: {message}",
    )


async def _acquire_rate_limit(max_queue_wait: Optional[float] = None) -> bool:
    """Draw from the shared cross-process token bucket (bounded-wait variant).

    Returns False only in bounded mode when the wait budget is exceeded —
    a SKIP, never a definitive miss.
    """
    deadline = time.monotonic() + max_queue_wait if max_queue_wait is not None else None
    while True:
        wait = await asyncio.to_thread(
            rate_limiter.acquire_token, "leadocean", _REFILL_PER_SEC, _CAPACITY
        )
        if wait <= 0:
            return True
        if deadline is not None:
            now = time.monotonic()
            if now + wait > deadline:
                return False
            wait = min(wait, deadline - now)
            if wait <= 0:
                return False
        await asyncio.sleep(wait)


def _headers() -> dict[str, str]:
    return {"x-api-key": API_KEY, "Accept": "application/json"}


async def _get_json(
    client: httpx.AsyncClient,
    path: str,
    params: dict[str, Any],
    method_label: str,
    max_queue_wait: Optional[float] = None,
) -> tuple[Optional[int], Any]:
    """GET with retries. Returns (status_code, parsed_body).

    Terminal conditions return a falsy _ProviderError as the BODY with a
    None status — callers distinguish via the marker below.
    """
    if not API_KEY:
        return None, _provider_error(method_label, "no_api_key", "LEADOCEAN_API_KEY not configured")

    for attempt in range(_MAX_RETRIES + 1):
        granted = await _acquire_rate_limit(max_queue_wait=max_queue_wait)
        if not granted:
            return None, _provider_error(
                method_label, "lane_busy",
                "rate-lane queue exceeded the wait budget — skipping to protect pipeline latency",
            )
        try:
            resp = await client.get(
                f"{BASE_URL}{path}", params=params, headers=_headers(),
                timeout=_REQUEST_TIMEOUT,
            )
        except httpx.HTTPError as exc:
            if attempt >= _MAX_RETRIES:
                return None, _provider_error(method_label, "network_error", f"network error: {exc}")
            await asyncio.sleep(random.uniform(0.0, min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))))
            continue

        if resp.status_code == 429:
            retry_after: Optional[float] = None
            raw = resp.headers.get("Retry-After")
            try:
                retry_after = float(raw) if raw is not None else None
            except ValueError:
                retry_after = None
            if attempt >= _MAX_RETRIES:
                # Sustained 429 at 150 RPS means account-level contention —
                # arm a short cooldown so all workers back off.
                leadocean_guards.set_cooldown(
                    "rate_limited", "sustained 429s", retry_after=retry_after or 120.0
                )
                return None, _provider_error(method_label, "rate_limited", "sustained 429s")
            await asyncio.sleep(min(retry_after or _BACKOFF_DELAY(), _MAX_BACKOFF))
            continue

        if resp.status_code == 402:
            # Past a window and refused (paid plans throttle rather than 402,
            # but handle both) — skip until the daily reset.
            leadocean_guards.set_daily_exhausted_cooldown()
            return None, _provider_error(method_label, "quota_exhausted", "402 payment required")

        if resp.status_code == 403:
            return None, _provider_error(method_label, "forbidden", "key missing required scope")

        try:
            body = resp.json()
        except ValueError:
            body = {}
        return resp.status_code, body

    return None, _provider_error(method_label, "retries_exhausted", "retries exhausted")


def _BACKOFF_DELAY() -> float:
    return random.uniform(0.0, _MAX_BACKOFF)


def _notice(body: Any) -> Optional[dict[str, Any]]:
    """meta.notice rides on responses while the account is past a window."""
    if isinstance(body, dict):
        notice = (body.get("meta") or {}).get("notice")
        if isinstance(notice, dict):
            return notice
    return None


# ---------------------------------------------------------------------------
# Lazy usage check (free) — arms the daily-wall cooldown
# ---------------------------------------------------------------------------

_last_usage_check: float = 0.0


async def maybe_check_usage(client: httpx.AsyncClient) -> None:
    """At most one free /v1/usage per process per 15 min.

    Detects a spent daily enrichment window (the wall throttles instead of
    erroring, so this is the ONLY proactive detection) and arms the
    cross-worker cooldown until resetsAt. Phase 2 (2026-11-07): also watches
    the 25M/month SEARCH window the same way. Best-effort; never raises; does
    not consume the rate lane budget materially (free endpoint, still
    rate-limit-acquired for politeness).
    """
    global _last_usage_check
    now = time.monotonic()
    if now - _last_usage_check < _USAGE_CHECK_INTERVAL_S:
        return
    _last_usage_check = now
    snapshot = await get_usage(client)
    if not isinstance(snapshot, dict):
        return
    try:
        pace = snapshot.get("pace") or {}
        enrich_windows = pace.get("enrich") or []
        for window in enrich_windows:
            limit = window.get("limit")
            used = window.get("used")
            if isinstance(limit, (int, float)) and isinstance(used, (int, float)):
                if limit > 0 and used >= limit:
                    leadocean_guards.set_daily_exhausted_cooldown(
                        resets_at=window.get("resetsAt"),
                    )
                    break
        search_windows = pace.get("search") or []
        for window in search_windows:
            limit = window.get("limit")
            used = window.get("used")
            if isinstance(limit, (int, float)) and isinstance(used, (int, float)):
                if limit > 0 and used >= limit:
                    leadocean_guards.set_search_exhausted_cooldown(
                        resets_at=window.get("resetsAt"),
                    )
                    break
    except Exception:
        logger.debug("leadocean: usage-window parsing failed", exc_info=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def find_person_email(
    client: httpx.AsyncClient,
    *,
    linkedin_url: str,
) -> Optional[dict[str, Any]]:
    """Find a person's best email on LeadOcean — LinkedIn-URL keyed.

    Flow (one or two records per attempt):
      1. ``/v2/people/email/work`` — status-bearing work address.
      2. Personal fallback (owner decision 2026-11-06): when work is
         absent OR its status is rejected-grade, ``/v2/people/email/personal``
         returns a ranked supplier-asserted list — take the first.
         Live-justified: bill gates → work null + 3 personal addresses.

    Guard wiring (owned here, call sites stay thin): cooldown → instant
    skip; recent miss → instant skip; definitive miss (404 no-profile, or
    no work AND no personal) → miss marker + None. A rejected work status
    does NOT mark a miss while personal still has a chance, and vice versa.

    Returns ``{"found": True, "email", "email_type" ("work"|"personal"),
    "status", "verified_batch_date", "verified_hint"}`` or None. Terminal
    errors surface as a falsy ``_ProviderError``.
    """
    if not linkedin_url:
        return None
    key = leadocean_guards.person_key(linkedin_url)

    if not _is_enabled():
        return _provider_error("find_person_email", "disabled", "kill switch active")
    if not API_KEY:
        return _provider_error("find_person_email", "no_api_key", "LEADOCEAN_API_KEY not configured")

    cooldown = leadocean_guards.get_active_cooldown()
    if cooldown is not None:
        logger.debug(
            "leadocean: skipping %s — cooldown active (%s, %ds left)",
            key, cooldown["code"], cooldown["remaining_seconds"],
        )
        return None
    if leadocean_guards.is_recent_person_miss(key):
        return None

    # Lazy free usage check — arms the daily-wall cooldown when spent.
    await maybe_check_usage(client)
    if leadocean_guards.get_active_cooldown() is not None:
        return None

    # ---- 1. work email ----
    status, body = await _get_json(
        client, "/v2/people/email/work",
        {"linkedin_url": linkedin_url}, "email_work",
        max_queue_wait=_MAX_QUEUE_WAIT,
    )
    if status is None:  # terminal error (lane_busy/network/429-storm/402)
        err = body
        if getattr(err, "error_type", "") == "lane_busy":
            logger.debug("leadocean: lane busy — skipping %s this pass", key)
            return None
        return err

    notice = _notice(body)
    if notice:
        leadocean_guards.set_cooldown(
            str(notice.get("code") or "throttled"),
            str(notice.get("message") or "account throttled"),
            until_ts=notice.get("resetsAt"),
        )

    work_email, work_status, work_date = "", "", ""
    if status == 200 and isinstance(body, dict):
        data = body.get("data")
        if isinstance(data, dict):
            work_email = str(data.get("email") or "").strip()
            work_status = str(data.get("status") or "").strip().lower()
            work_date = str(data.get("verified_batch_date") or "")

    work_usable = bool(work_email) and work_status not in _rejected_statuses()
    if work_usable:
        return {
            "found": True,
            "email": work_email,
            "email_type": "work",
            "status": work_status or "unknown",
            "verified_batch_date": work_date,
            "verified_hint": "yes" if work_status == "verified" else "unknown",
        }

    # ---- 2. personal fallback ----
    # Runs on: 404 is SKIPPED (no profile → no personal either); work null;
    # work status rejected-grade.
    if status == 404:
        leadocean_guards.record_person_miss(key, "email")
        return None

    status2, body2 = await _get_json(
        client, "/v2/people/email/personal",
        {"linkedin_url": linkedin_url}, "email_personal",
        max_queue_wait=_MAX_QUEUE_WAIT,
    )
    if status2 is None:
        err = body2
        if getattr(err, "error_type", "") == "lane_busy":
            return None
        return err

    personal = []
    if status2 == 200 and isinstance(body2, dict) and isinstance(body2.get("data"), list):
        personal = [p for p in body2["data"] if isinstance(p, dict) and p.get("email")]

    if personal:
        first = personal[0]
        return {
            "found": True,
            "email": str(first["email"]).strip(),
            "email_type": "personal",
            # Personal endpoints publish NO status (consumer domains are
            # never vendor-verified) — always mailtester-gated downstream.
            "status": "",
            "verified_batch_date": str(first.get("verified_batch_date") or ""),
            "verified_hint": "unknown",
        }

    # No work (or rejected) AND no personal — definitive miss.
    leadocean_guards.record_person_miss(key, "email")
    return None


async def search_company_people(
    client: httpx.AsyncClient,
    domain: str,
    *,
    max_people: int = 25,
) -> Optional[dict[str, Any]]:
    """Discover people at a company by bare domain — LeadOcean search lane.

    Phase 2 (2026-11-07). ``GET /v1/companies/{domain}/people`` bills 1 SEARCH
    record per returned row against the 25M/month window (NOT the 1M/day
    enrich wall), so the platform only calls this as the TERMINAL discovery
    tier — when contacts_db, QuickEnrich dataset-search, the Blitz waterfall
    and the GetLeads DM fallback have all come up empty.

    Live-verified contract (2026-11-07): cursor pagination via
    ``meta.nextCursor`` (no offset), ``limit`` param honored (default 25),
    NO server-side filters — title/has_email filtering is client-side. HTTP
    404 = "No people for this company" (clean miss, cached 7d).

    Only ``has_email=true`` rows are returned: rows carry no addresses, and a
    false row is a guaranteed miss on the email waterfall (which would still
    pay 1 enrich record per attempt). The row is billed either way — the
    filter stops downstream spend, not the search record.

    Returns ``{"people": [flat dicts], "rows_seen": int, "pages": int}``,
    ``None`` on clean miss / cooldown / lane-busy skip, or a falsy
    ``_ProviderError`` on terminal errors. ICP title gating is NOT done here
    (pipeline-helper concern, same split as every other provider).
    """
    if not domain:
        return None
    if not _is_enabled():
        return _provider_error("search_company_people", "disabled", "kill switch active")
    if not _discovery_enabled():
        return None  # lane off is a silent skip, not an error
    if not API_KEY:
        return _provider_error(
            "search_company_people", "no_api_key", "LEADOCEAN_API_KEY not configured"
        )

    dkey = leadocean_guards.domain_key(domain)
    cooldown = leadocean_guards.get_active_cooldown()
    if cooldown is not None:
        logger.debug(
            "leadocean: discovery skip %s — cooldown active (%s)", domain, cooldown["code"]
        )
        return None
    if dkey and leadocean_guards.is_recent_person_miss(dkey):
        return None

    # Lazy free usage check — arms the daily-enrich AND search-window cooldowns.
    await maybe_check_usage(client)
    if leadocean_guards.get_active_cooldown() is not None:
        return None

    target = max(1, int(max_people))
    people: list[dict[str, Any]] = []
    rows_seen = 0
    pages = 0
    cursor: Optional[str] = None
    while pages < _DISCOVERY_MAX_PAGES and rows_seen < _DISCOVERY_MAX_ROWS:
        params: dict[str, Any] = {"limit": _DISCOVERY_PAGE_SIZE}
        if cursor:
            params["cursor"] = cursor
        status, body = await _get_json(
            client, f"/v1/companies/{domain}/people",
            params, "company_people",
            max_queue_wait=_MAX_QUEUE_WAIT,
        )
        if status is None:
            err = body
            if getattr(err, "error_type", "") == "lane_busy":
                logger.debug("leadocean: discovery lane busy — skipping %s this pass", domain)
                return None
            if getattr(err, "error_type", "") == "quota_exhausted":
                # _get_json already armed the daily-enrich label; the true
                # reset comes from the next lazy /v1/usage (15-min cadence),
                # which arms the monthly search cooldown with resetsAt.
                leadocean_guards.set_search_exhausted_cooldown()
            return err

        notice = _notice(body)
        if notice:
            leadocean_guards.set_cooldown(
                str(notice.get("code") or "throttled"),
                str(notice.get("message") or "account throttled"),
                until_ts=notice.get("resetsAt"),
            )
            return None

        if status == 404:
            # Definitive: vendor knows no people for this company.
            if dkey:
                leadocean_guards.record_person_miss(dkey, "discovery")
            return None

        rows = []
        next_cursor = ""
        if status == 200 and isinstance(body, dict):
            data = body.get("data")
            if isinstance(data, list):
                rows = [r for r in data if isinstance(r, dict)]
            meta = body.get("meta")
            if isinstance(meta, dict):
                next_cursor = str(meta.get("nextCursor") or "")

        pages += 1
        rows_seen += len(rows)
        for row in rows:
            if len(people) >= target:
                break
            parsed = _parse_person_row(row)
            if parsed.get("linkedin_url") and parsed.get("has_email"):
                people.append(parsed)

        if not next_cursor or len(people) >= target or not rows:
            break
        cursor = next_cursor

    if not people:
        # Rows existed but none had an email — a definitive discovery miss
        # for the email funnel (do not mark when we merely hit the row cap).
        if rows_seen and rows_seen < _DISCOVERY_MAX_ROWS and dkey:
            leadocean_guards.record_person_miss(dkey, "discovery")
        return None
    return {"people": people, "rows_seen": rows_seen, "pages": pages}


def _parse_person_row(row: dict[str, Any]) -> dict[str, Any]:
    """Flatten one leadocean.person.v1 row (live-probed shape, 2026-11-07)."""
    profile = row.get("profile_data") or {}
    contact = row.get("contact_data") or {}
    experiences = contact.get("contact_current_experiences") or []
    exp = experiences[0] if experiences and isinstance(experiences[0], dict) else {}
    seniority = exp.get("job_seniority") or {}
    ids = (row.get("meta") or {}).get("source_ids") or {}
    address = profile.get("profile_address") or {}
    company_links = exp.get("company_social_links") or {}
    company_linkedin = company_links.get("linkedin") or {}
    return {
        "full_name": str(profile.get("profile_full_name") or ""),
        "first_name": str(profile.get("profile_first_name") or ""),
        "last_name": str(profile.get("profile_last_name") or ""),
        "headline": str(profile.get("profile_headline") or ""),
        "linkedin_url": str(profile.get("profile_url") or ""),
        "title": str(exp.get("job_title") or ""),
        "job_level": str(seniority.get("job_level") or ""),
        "has_email": bool(contact.get("has_email")),
        "email_status": str(contact.get("email_status") or ""),
        "email_types": list(contact.get("email_types") or []),
        "has_phone": bool(contact.get("has_phone")),
        "person_id": str(ids.get("person_id") or ""),
        "company_name": str(exp.get("company_name") or ""),
        "company_domain": str(exp.get("company_domain") or ""),
        "company_industry": str(exp.get("company_industry") or ""),
        "company_linkedin_url": str(company_linkedin.get("url") or ""),
        "city": str(address.get("city") or ""),
        "country_code": str(address.get("country_code") or ""),
    }


async def count_companies(
    client: httpx.AsyncClient,
    industries: list[str],
) -> Optional[dict[str, Any]]:
    """FREE company count by industry: GET /v1/companies/count.

    No quota lane (live-verified 2026-11-07 — repeated probing left
    pace.search untouched by count calls). Repeatable ``industry`` params are
    OR-combined server-side. Industries are validated against the stored
    532-value enum BEFORE the vendor is hit; an invalid value returns a
    falsy ``_ProviderError("invalid_industry")`` naming the offenders.
    """
    if not _is_enabled() or not API_KEY:
        return _provider_error("count_companies", "disabled", "kill switch active or no key")
    wanted = [str(i).strip() for i in (industries or []) if str(i).strip()]
    if not wanted:
        return _provider_error("count_companies", "invalid_industry", "no industry values supplied")
    invalid = [i for i in wanted if not leadocean_industries.is_valid_industry(i)]
    if invalid:
        return _provider_error(
            "count_companies", "invalid_industry",
            "not LeadOcean industry values: " + ", ".join(invalid[:5]),
        )
    status, body = await _get_json(
        client, "/v1/companies/count",
        {"industry": wanted}, "count_companies",
    )
    if status is None:
        return body
    data = body.get("data") if isinstance(body, dict) else None
    if isinstance(data, dict):
        return data
    return None


async def get_usage(client: httpx.AsyncClient) -> Optional[dict[str, Any]]:
    """Free quota gauge: GET /v1/usage. Ledgered; returns parsed data or error."""
    if not _is_enabled() or not API_KEY:
        return _provider_error("get_usage", "disabled", "kill switch active or no key")
    status, body = await _get_json(client, "/v1/usage", {}, "get_usage")
    if status is None:
        return body
    data = body.get("data") if isinstance(body, dict) else None
    if isinstance(data, dict):
        leadocean_guards.record_usage("usage", data)
        return data
    return None
