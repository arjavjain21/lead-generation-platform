"""
MoltSets API client — person-email waterfall tier (api.moltsets.com).

MoltSets (2026-10-14, $97/mo plan: unlimited tokens, 15k records/5h,
75k records/week, 7.5k searches/5h, personal email included, no phone).
Position in the cascade (set 2026-10-14): 6th — after blitz, before
better_enrich and wizleads. Only reached when contacts_db, smartprospect,
quickenrich, getleads, and blitz have ALL missed the person, so volume is
residual by construction.

Endpoints used (v1 scope — person email only, NO domain discovery):
- ``linkedin_to_best_email`` (linkedin_url) — highest-coverage single
  lookup; "best" falls back to the personal email when no business email
  exists (personal included in the plan).
- ``search_business_email_by_name`` (first_name + last_name +
  company_domain) — the name+domain path.
- ``get_usage`` (account) — observability only.

API contract quirks encoded here (SDK: docs/moltsets-sdk/):
- Auth: ``Authorization: Bearer <key>`` + a NON-EMPTY ``User-Agent`` (the
  API rejects requests without one).
- A miss is HTTP 200 + ``status: "not_found"`` — NEVER HTTP 404.
- Errors are HTTP 4xx/5xx with ``error.code``:
  * ``rate_limited`` / ``internal_error`` — short-transient, retried twice.
  * ``fair_use_limit_exceeded`` / ``fair_use_requests_exceeded`` — the 5h /
    weekly fair-use window is spent. NOT retried: the client arms the
    cross-worker cooldown (moltsets_guards) and returns a falsy
    ``_ProviderError`` so every worker skips MoltSets instantly until the
    window resets (owner requirement: "if we have hit the 5h limit we just
    move on").
  * ``plan_restricted`` / ``insufficient_tokens`` / ``insufficient_phone_tokens``
    / ``invalid_input`` — terminal, no retry.
- Every 2xx carries ``metadata.fair_use`` — parsed into the
  ``moltsets_usage`` ledger for GET /api/enrichment/moltsets/usage.
- Emails carry a ``risk_score`` grade: A (validated deliverable), B (known
  send), C (catch-all/uncertain), D (hard negative — bounce/complaint/
  spam-trap), F (no signal). Default policy rejects only D
  (MOLTSETS_REJECT_RISK); A/B surface as verified, C/F as unknown
  (mailtester gates those at the call sites, same as Blitz unverified).

Rate limiting: one SHARED cross-process token bucket
(``MOLTSETS_RATE_LIMIT_RPM``, default 30) via shared/rate_limiter.py — all
4 gunicorn workers draw from one SQLite-backed bucket (SmartProspect
pattern). 30 RPM sits under the sustained 15k-records/5h (=50/min) cap;
the fair-use cooldown absorbs the rest.

House return-shape convention (see quickenrich_client):
- found -> dict (``found: True``, email, risk_score, ...)
- clean miss / cooldown active / risk-rejected -> ``None``
- terminal auth/credit/fair-use/HTTP errors -> ``_ProviderError`` (falsy)
  distinguished by ``pipeline._is_provider_error``.

The client owns the guard wiring: it checks the cooldown + person miss
store BEFORE the HTTP call (miss = zero-latency skip) and records
definitive misses + fair-use metadata itself, keeping call sites thin.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Any, Optional

import httpx

from . import moltsets_guards
from . import pipeline  # For _ProviderError (terminal-error signaling)
from shared import rate_limiter
from shared.env_utils import env_float

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.getenv(
    "MOLTSETS_BASE_URL",
    "https://api.moltsets.com/api/v1/tools",
).rstrip("/")

API_KEY = os.getenv("MOLTSETS_API_KEY", "")

# MoltSets requires a non-empty User-Agent on every request.
_USER_AGENT = os.getenv(
    "MOLTSETS_USER_AGENT",
    "listbuilding-waterfall/1.0 (+https://listbuilding.eagleinfoservice.com)",
)

# Shared cross-process limiter — one bucket across all gunicorn workers.
# ⚠ CAPACITY MUST STAY >= 1.0: shared.rate_limiter only GRANTS when the
# bucket holds a full token (``refilled >= 1.0``) and the refill is capped
# by ``min(capacity, ...)`` — a sub-1.0 capacity can never accumulate a
# whole token and _acquire_rate_limit livelocks (found live 2026-10-14:
# capacity 0.5 at 30 RPM hung every request ~forever). The SUSTAINED rate
# is still 30/min via refill; capacity 1 just permits a burst of one.
_RATE_LIMIT_RPM = env_float("MOLTSETS_RATE_LIMIT_RPM", "30")
_REFILL_PER_SEC = _RATE_LIMIT_RPM / 60.0
_CAPACITY = max(1.0, env_float("MOLTSETS_RATE_LIMIT_BURST", "1.0"))

_MAX_RETRIES = 2            # only for rate_limited / internal_error
_BASE_BACKOFF = 0.5
_MAX_BACKOFF = 5.0
_REQUEST_TIMEOUT = 20.0

# Error codes that mean a fair-use window (5h records / weekly / 5h
# searches) is exhausted — arm the cooldown, never retry here.
_FAIR_USE_CODES = frozenset(
    {"fair_use_limit_exceeded", "fair_use_requests_exceeded"}
)

# Risk grades we surface as verified without further checks. C (catch-all)
# and F (no signal) surface as "unknown" so the mailtester gate at the
# call site decides — identical to how Blitz unverified emails flow.
_VERIFIED_RISK_GRADES = frozenset({"A", "B"})


def _is_enabled() -> bool:
    """Return True unless the MOLTSETS kill switch is active."""
    return os.environ.get("ENABLE_MOLTSETS", "true").lower() == "true"


def _rejected_risk_grades() -> frozenset[str]:
    """Grades never returned to the cascade (MOLTSETS_REJECT_RISK, def 'D')."""
    raw = os.getenv("MOLTSETS_REJECT_RISK", "D")
    grades = {g.strip().upper() for g in raw.split(",") if g.strip()}
    return frozenset(grades)


async def _acquire_rate_limit() -> None:
    """Draw from the shared cross-process token bucket.

    Loops until a token is GRANTED (SmartProspect pattern): concurrent
    denials compute similar waits and would all proceed together after
    sleeping, bursting past the account limit. Only a grant consumes a
    token.
    """
    while True:
        wait = await asyncio.to_thread(
            rate_limiter.acquire_token, "moltsets", _REFILL_PER_SEC, _CAPACITY
        )
        if wait <= 0:
            return
        await asyncio.sleep(wait)


def _backoff_delay(attempt: int) -> float:
    cap = min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0.0, cap)


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": _USER_AGENT,
    }


def _provider_error(method: str, error_type: str, message: str):
    """Terminal error -> falsy _ProviderError (cascade continues past it)."""
    return pipeline._ProviderError(
        provider="moltsets",
        method=method,
        error_type=error_type,
        message=f"MoltSets: {message}",
    )


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


def _extract_email_fields(results: Any) -> dict[str, Any]:
    """Pull (email, risk_score, email_type, identity) out of a results block.

    MoltSets result shapes vary per endpoint (dict, or dict with nested
    ``results``/``person``/``profile``). Mirrors the SDK's defensive
    ``_iter_dicts`` walk: scan every dict in the tree for the first
    non-empty value of each field family.
    """
    blocks: list[dict] = []

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            blocks.append(value)
            for child_key in ("results", "data", "person", "profile"):
                _walk(value.get(child_key))
        elif isinstance(value, list):
            for item in value:
                _walk(item)

    _walk(results)

    def _first(keys: tuple[str, ...]) -> Any:
        for block in blocks:
            for key in keys:
                candidate = block.get(key)
                if candidate not in (None, "", [], {}):
                    return candidate
        return None

    email = _first(("email", "business_email", "personal_email"))
    risk = _first(
        ("risk_score", "business_email_risk_score", "personal_email_risk_score")
    )
    email_type = _first(("email_type", "type"))
    linkedin = _first(("linkedin_url", "linkedin"))
    return {
        "email": str(email).strip() if email else "",
        "risk_score": str(risk).strip().upper() if risk else "",
        "email_type": str(email_type) if email_type else "",
        "first_name": str(_first(("first_name", "firstName")) or ""),
        "last_name": str(_first(("last_name", "lastName")) or ""),
        "full_name": str(_first(("full_name", "name")) or ""),
        "linkedin_url": str(linkedin) if linkedin else "",
    }


def _parse_response(body: dict[str, Any]) -> tuple[bool, Any, dict[str, Any], Optional[dict[str, Any]]]:
    """Split a 2xx body into (found, results, metadata, error_block).

    ``status`` defaults to ``ok`` when absent (SDK behavior). A 200 can
    still carry an ``error`` block (partial batch) — surfaced, not raised.
    """
    status = str(body.get("status", "ok") or "ok").lower()
    found = status == "ok"
    return (
        found,
        body.get("results"),
        body.get("metadata") or {},
        body.get("error") if isinstance(body.get("error"), dict) else None,
    )


# ---------------------------------------------------------------------------
# Core request loop
# ---------------------------------------------------------------------------


async def _post_tool(
    client: httpx.AsyncClient,
    method: str,
    payload: dict[str, Any],
) -> tuple[bool, Any, dict[str, Any]]:
    """POST {BASE_URL}/{tool} with retries + guard wiring.

    Returns ``(found, results, metadata)``. Raises nothing: terminal
    conditions come back as a falsy _ProviderError tuple slot — callers
    check ``type is pipeline._ProviderError`` via the marker below.

    Return contract:
        (True, results, metadata)                    -> data found
        (False, results_or_None, metadata)           -> clean not_found
        (_ERROR_MARKER, _ProviderError, {})          -> terminal error
    """
    if not _is_enabled():
        return _ERROR_MARKER, _provider_error(method, "disabled", "kill switch active"), {}
    if not API_KEY:
        return _ERROR_MARKER, _provider_error(method, "no_api_key", "MOLTSETS_API_KEY not configured"), {}

    url = f"{BASE_URL}/{method}"
    last_error: Optional[str] = None

    for attempt in range(_MAX_RETRIES + 1):
        await _acquire_rate_limit()
        try:
            resp = await client.post(
                url, json=payload, headers=_headers(), timeout=_REQUEST_TIMEOUT
            )
        except httpx.HTTPError as exc:
            last_error = f"network error: {exc}"
            if attempt >= _MAX_RETRIES:
                return _ERROR_MARKER, _provider_error(method, "network_error", last_error), {}
            await asyncio.sleep(_backoff_delay(attempt))
            continue

        try:
            body = resp.json()
        except ValueError:
            body = {}

        if 200 <= resp.status_code < 300:
            found, results, metadata, partial_error = _parse_response(body)
            if metadata.get("fair_use"):
                moltsets_guards.record_fair_use("response", metadata["fair_use"])
            if partial_error:
                code = str(partial_error.get("code") or "")
                if code in _FAIR_USE_CODES:
                    _arm_cooldown_from_error(partial_error)
                    return _ERROR_MARKER, _provider_error(
                        method, code, str(partial_error.get("message") or "fair use window exhausted")
                    ), {}
                logger.warning(
                    "moltsets: %s returned partial error block: %s", method, partial_error
                )
            return found, results, metadata

        # HTTP error — parse error.code / retry_after.
        error = body.get("error") if isinstance(body.get("error"), dict) else {}
        code = str(error.get("code") or f"http_{resp.status_code}")
        message = str(error.get("message") or resp.text or "request failed")[:400]

        if code in _FAIR_USE_CODES:
            _arm_cooldown_from_error({**error, "_status": resp.status_code})
            return _ERROR_MARKER, _provider_error(method, code, message), {}

        retryable = code in ("rate_limited", "internal_error")
        if not retryable or attempt >= _MAX_RETRIES:
            return _ERROR_MARKER, _provider_error(method, code, message), {}

        retry_after: Optional[float] = None
        raw_retry = (body.get("metadata") or {}).get("retry_after") or resp.headers.get("Retry-After")
        try:
            retry_after = float(raw_retry) if raw_retry is not None else None
        except (TypeError, ValueError):
            retry_after = None
        await asyncio.sleep(
            min(retry_after, _MAX_BACKOFF) if retry_after else _backoff_delay(attempt)
        )

    return _ERROR_MARKER, _provider_error(method, "retries_exhausted", last_error or "retries exhausted"), {}


# Sentinel distinguishing the error tuple slot from a clean miss.
_ERROR_MARKER = object()


def _arm_cooldown_from_error(error: dict[str, Any]) -> None:
    """Arm the cross-worker cooldown from a fair_use error payload."""
    code = str(error.get("code") or "fair_use_limit_exceeded")
    message = str(error.get("message") or "fair use window exhausted")
    retry_after: Optional[float] = None
    try:
        retry_after = float(error.get("retry_after")) if error.get("retry_after") is not None else None
    except (TypeError, ValueError):
        retry_after = None
    moltsets_guards.set_cooldown(code, message, retry_after=retry_after)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def find_person_email(
    client: httpx.AsyncClient,
    *,
    linkedin_url: str = "",
    first_name: str = "",
    last_name: str = "",
    company_domain: str = "",
) -> Optional[dict[str, Any]]:
    """Find a person's best email on MoltSets — ONE request per attempt.

    Picks the endpoint by available identity: ``linkedin_to_best_email``
    when a LinkedIn URL exists (highest coverage, personal-email fallback),
    else ``search_business_email_by_name`` (needs first + last + domain).
    The SDK recipe's longer chains (search_linkedin_profile ->
    linkedin_to_best_email) are deliberately NOT followed — with 7.5k
    searches/5h we spend exactly one request per person per attempt.

    Guard wiring (owned here, call sites stay thin):
    - active fair-use cooldown -> ``None`` immediately (no HTTP, no marker)
    - recent definitive miss for this person -> ``None`` immediately
    - clean not_found -> miss marker recorded, ``None`` returned
    - risk grade in MOLTSETS_REJECT_RISK (default D) -> ``None``, NO
      marker (a bad best-email today is not proof of a permanent miss)

    Returns ``{"found": True, "email", "risk_score", "email_type",
    "first_name", "last_name", "full_name", "linkedin_url",
    "verified_hint", "source_endpoint"}`` or ``None``. Terminal account
    errors surface as a falsy ``_ProviderError`` (falsy like ``None``; the
    pipeline distinguishes via ``_is_provider_error``).
    """
    key = moltsets_guards.person_key(
        linkedin_url=linkedin_url,
        first_name=first_name,
        last_name=last_name,
        domain=company_domain,
    )
    kind = "linkedin" if linkedin_url else "name_domain"

    if not linkedin_url and not (first_name and last_name and company_domain):
        return None

    cooldown = moltsets_guards.get_active_cooldown()
    if cooldown is not None:
        logger.debug(
            "moltsets: skipping %s — cooldown active (%s, %ds left)",
            key or "lookup", cooldown["code"], cooldown["remaining_seconds"],
        )
        return None
    if key and moltsets_guards.is_recent_person_miss(key):
        return None

    if linkedin_url:
        tool = "linkedin_to_best_email"
        payload = {"linkedin_url": linkedin_url}
    else:
        tool = "search_business_email_by_name"
        payload = {
            "first_name": first_name,
            "last_name": last_name,
            "company_domain": company_domain,
        }

    found, results, _ = await _post_tool(client, tool, payload)
    if found is _ERROR_MARKER:
        return results  # the _ProviderError object
    if not found:
        if key:
            moltsets_guards.record_person_miss(key, kind)
        return None

    fields = _extract_email_fields(results)
    email = fields["email"]
    if not email:
        # ok-status with no email anywhere — treat as a miss.
        if key:
            moltsets_guards.record_person_miss(key, kind)
        return None
    if fields["risk_score"] and fields["risk_score"] in _rejected_risk_grades():
        logger.info(
            "moltsets: rejecting %s risk grade %s for %s",
            email, fields["risk_score"], key or "lookup",
        )
        return None

    verified_hint = (
        "yes" if fields["risk_score"] in _VERIFIED_RISK_GRADES else "unknown"
    )
    return {
        "found": True,
        "email": email,
        "risk_score": fields["risk_score"],
        "email_type": fields["email_type"],
        "first_name": fields["first_name"],
        "last_name": fields["last_name"],
        "full_name": fields["full_name"],
        "linkedin_url": fields["linkedin_url"] or linkedin_url,
        "verified_hint": verified_hint,
        "source_endpoint": tool,
    }


async def get_usage(
    client: httpx.AsyncClient,
    period: str = "today",
) -> Optional[dict[str, Any]]:
    """Fetch account usage (period: today|week|month|billing_cycle).

    Observability only — used by GET /api/enrichment/moltsets/usage. The
    snapshot is ledgered via moltsets_guards.record_fair_use. Returns the
    parsed results dict, a falsy _ProviderError, or None on clean miss.
    """
    found, results, _ = await _post_tool(client, "get_usage", {"period": period})
    if found is _ERROR_MARKER:
        return results
    if isinstance(results, dict):
        moltsets_guards.record_fair_use("get_usage", results)
    return results if found else None
