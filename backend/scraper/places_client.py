"""GetLeads Google Maps places database client (US local-business listings).

Backs the "Instant US Database" data source on the Google Maps Scraper page
(2026-11-11): a pre-indexed 20.5M-listing US database served by GetLeads with

- FREE lookups: category search, per-state city names, match counts
- fair-use-metered rows: async CSV exports (1 row of the account's
  google_maps fair-use budget per exported business — 50K/day, 500K/month
  on the Unlimited plan; the budget is ACCOUNT-WIDE, shared with anything
  else hitting the GetLeads places API).

Shares GETLEADS_API_KEY and the account-wide ~100 RPM limiter bucket with
the enrichment getleads_client (one account, one request budget). This is a
SCRAPER DATA SOURCE, not an enrichment cascade provider — it returns
businesses, not contacts, and never touches contacts_writer.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import os
import time
from typing import Any, Optional

import httpx

from shared import rate_limiter
from shared.env_utils import env_float

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (mirrors enrichment/getleads_client.py — same account)
# ---------------------------------------------------------------------------

BASE_URL = os.getenv("GETLEADS_BASE_URL", "https://app.getleads.io")
API_KEY = os.getenv("GETLEADS_API_KEY", "")

# Account limit is 100 req/min GLOBAL across all gunicorn workers AND across
# both GetLeads clients (enrichment + places). Same shared bucket name
# "getleads" + same env knobs means both clients draw one budget correctly.
_RATE_LIMIT_RPM = env_float("GETLEADS_RATE_LIMIT_RPM", "95")
_REFILL_PER_SEC = _RATE_LIMIT_RPM / 60.0
_CAPACITY = env_float("GETLEADS_RATE_LIMIT_BURST", _REFILL_PER_SEC)

_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0
_REQUEST_TIMEOUT = 60.0

# Server-side hard cap on a single CSV export (docs: "an export contains at
# most 25,000 rows; for larger results, use disjoint segments").
MAX_EXPORT_ROWS = 25_000

# Async-export lifecycle knobs (consumed by places_flow).
EXPORT_POLL_INTERVAL_S = env_float("GETLEADS_PLACES_POLL_S", "5")
EXPORT_TIMEOUT_S = env_float("GETLEADS_PLACES_EXPORT_TIMEOUT_S", "1800")

_CATEGORIES_PATH = "/api/v1/places/categories"
_COUNT_PATH = "/api/v1/places/search/count"
_EXPORT_PATH = "/api/v1/places/search/export"
_FAIR_USE_PATH = "/api/v1/usage/fair-use"

# Filter keys the GetLeads places API accepts that we plumb through from the
# UI. Anything else in the request payload is dropped here (defense in depth
# alongside the pydantic route models).
_ALLOWED_FILTER_KEYS = (
    "categories",
    "category_contains",
    "business_name",
    "exclude_categories",
    "exclude_category_contains",
    "states",
    "zips",
    "has_email",
    "has_phone",
    "has_website",
)
_BUSINESS_FILTER_KEYS = ("categories", "category_contains", "business_name")
_GEO_FILTER_KEYS = ("states", "zips")

# In-process category cache: the autocomplete hits this endpoint on every
# keystroke-debounce, and the lookup is free — cache purely for snappiness.
_CATEGORY_CACHE_TTL_S = 600.0
_CATEGORY_CACHE_MAX = 200
_category_cache: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}


class PlacesError(Exception):
    """GetLeads places API call failed after retries (or was rejected)."""

    def __init__(self, method: str, status_code: int, detail: str):
        self.method = method
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"{method} failed ({status_code}): {detail}")


def is_enabled() -> bool:
    """Kill switch for the places surface (default on; env ENABLE_GETLEADS_PLACES)."""
    return os.environ.get("ENABLE_GETLEADS_PLACES", "true").lower() == "true"


def is_configured() -> bool:
    """True when the shared GetLeads key is present (places uses the same key)."""
    return bool(API_KEY)


# ---------------------------------------------------------------------------
# Rate limit (shared cross-process token bucket — see shared/rate_limiter.py)
# ---------------------------------------------------------------------------


async def _acquire_rate_limit() -> None:
    """Same grant-loop discipline as enrichment getleads_client: only a granted
    token admits a call, so concurrent denials can't burst past the cap."""
    while True:
        wait = await asyncio.to_thread(
            rate_limiter.acquire_token, "getleads", _REFILL_PER_SEC, _CAPACITY
        )
        if wait <= 0:
            return
        await asyncio.sleep(wait)


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def _normalize_zip(raw: str) -> str:
    """ZIP+4 → 5-digit ZIP (the API reduces it too; normalizing keeps counts
    and cache keys stable on our side)."""
    z = raw.strip()
    return z.split("-")[0][:5] if z else z


def sanitize_filters(payload: dict[str, Any]) -> dict[str, Any]:
    """Whitelist + clean a filter payload for the places API.

    Raises ValueError when the result would carry no business AND no geo
    filter — the API rejects filterless requests, so fail before spending a
    rate-limit token.
    """
    cleaned: dict[str, Any] = {}
    for key in _ALLOWED_FILTER_KEYS:
        value = payload.get(key)
        if value is None or value == "" or value == []:
            continue
        if key in ("categories", "category_contains", "exclude_categories",
                   "exclude_category_contains", "states"):
            cleaned[key] = [str(v).strip() for v in value if str(v).strip()]
            if not cleaned[key]:
                del cleaned[key]
        elif key == "zips":
            zips = [_normalize_zip(str(v)) for v in value if str(v).strip()]
            zips = [z for z in zips if z]
            if zips:
                cleaned[key] = zips
        elif key == "business_name":
            name = str(value).strip()
            if name:
                cleaned[key] = name
        else:  # booleans (has_email / has_phone / has_website)
            cleaned[key] = bool(value)

    has_business = any(k in cleaned for k in _BUSINESS_FILTER_KEYS)
    has_geo = any(k in cleaned for k in _GEO_FILTER_KEYS)
    if not has_business and not has_geo:
        raise ValueError(
            "At least one business filter (category / business name) or "
            "location filter (states / zips) is required."
        )
    return cleaned


def describe_filters(filters: dict[str, Any]) -> str:
    """Human label for the job card (e.g. 'Roofing contractor · TX')."""
    label = ""
    if filters.get("categories"):
        label = ", ".join(filters["categories"][:2])
    elif filters.get("category_contains"):
        label = ", ".join(filters["category_contains"][:2])
    elif filters.get("business_name"):
        label = filters["business_name"]
    if filters.get("states"):
        label += " · " + ", ".join(filters["states"][:3])
    elif filters.get("zips"):
        label += f" · {len(filters['zips'])} ZIPs"
    return label.strip(" ·") or "US businesses"


# ---------------------------------------------------------------------------
# HTTP core
# ---------------------------------------------------------------------------


def _should_retry(status_code: int) -> bool:
    """Retry 429 + 5xx only — same policy as every provider client."""
    return status_code == 429 or (500 <= status_code < 600)


def _backoff_delay(attempt: int) -> float:
    import random

    return min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt) * (0.5 + random.random()))


async def _request(
    method: str,
    path: str,
    *,
    params: Optional[dict[str, Any]] = None,
    json_body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """One authed JSON call with retries. Raises PlacesError on final failure."""
    if not is_enabled():
        raise PlacesError(method, 503, "Places kill switch active (ENABLE_GETLEADS_PLACES=false)")
    if not API_KEY:
        raise PlacesError(method, 503, "GETLEADS_API_KEY is not configured")

    url = f"{BASE_URL}{path}"
    last_error: Optional[PlacesError] = None
    for attempt in range(_MAX_RETRIES + 1):
        await _acquire_rate_limit()
        try:
            async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT) as client:
                resp = await client.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers={"Authorization": f"Bearer {API_KEY}"},
                )
        except httpx.HTTPError as exc:
            last_error = PlacesError(method, 0, f"network error: {exc}")
        else:
            if 200 <= resp.status_code < 300:
                try:
                    return resp.json()
                except ValueError:
                    last_error = PlacesError(method, resp.status_code, "non-JSON response")
            elif resp.status_code == 402:
                # Fair-use/credit wall — retrying only burns the account
                # limiter; surface it verbatim for a clean user message.
                raise PlacesError(method, 402, _extract_detail(resp))
            elif _should_retry(resp.status_code):
                last_error = PlacesError(method, resp.status_code, _extract_detail(resp))
            else:
                raise PlacesError(method, resp.status_code, _extract_detail(resp))

        if attempt < _MAX_RETRIES:
            await asyncio.sleep(_backoff_delay(attempt))

    raise last_error or PlacesError(method, 0, "request failed")


def _extract_detail(resp: httpx.Response) -> str:
    try:
        body = resp.json()
        for key in ("detail", "error", "message"):
            if isinstance(body, dict) and body.get(key):
                return str(body[key])[:300]
        return str(body)[:300]
    except Exception:
        return resp.text[:300] if resp.text else f"HTTP {resp.status_code}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def search_categories(q: str = "", page: int = 1) -> dict[str, Any]:
    """FREE category-name lookup (autocomplete). Returns the API dict with at
    least {categories, total, has_more}."""
    cache_key = (q.strip().lower(), page)
    now = time.monotonic()
    cached = _category_cache.get(cache_key)
    if cached and now - cached[0] < _CATEGORY_CACHE_TTL_S:
        return cached[1]

    params: dict[str, Any] = {"page": page}
    if q.strip():
        params["q"] = q.strip()
    data = await _request("GET", _CATEGORIES_PATH, params=params)

    if len(_category_cache) >= _CATEGORY_CACHE_MAX:
        _category_cache.clear()
    _category_cache[cache_key] = (now, data)
    return data


async def count_places(filters: dict[str, Any]) -> int:
    """FREE match count for the given filters (0 credits, sizes the pull)."""
    data = await _request("POST", _COUNT_PATH, json_body=filters)
    return int(data.get("total_matching") or 0)


async def start_export(filters: dict[str, Any]) -> str:
    """Kick off an async CSV export; returns export_id for polling."""
    data = await _request("POST", _EXPORT_PATH, json_body=filters)
    export_id = data.get("export_id")
    if not export_id:
        raise PlacesError("start_export", 0, f"no export_id in response: {str(data)[:200]}")
    return str(export_id)


async def poll_export(export_id: str) -> dict[str, Any]:
    """Poll export status. Completed payloads carry export_url + rows_exported;
    shape varies defensively (job_status: pending|processing|completed|failed)."""
    return await _request("GET", f"{_EXPORT_PATH}/{export_id}")


async def download_export_csv(export_url: str) -> tuple[list[str], list[dict[str, str]]]:
    """Download a completed export's CSV (presigned URL, no auth header) and
    parse it. Returns (fieldnames, rows-as-dicts). Raises PlacesError on
    download/parse failure — the URL expires, so call promptly on completion."""
    try:
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            # The API's export_url 302-redirects to the actual file (live E2E
            # 2026-11-12) — httpx does NOT follow redirects by default and
            # raise_for_status() errors on the 302 itself.
            resp = await client.get(export_url, follow_redirects=True)
            resp.raise_for_status()
            text = resp.text
    except httpx.HTTPError as exc:
        raise PlacesError("download_export_csv", 0, f"download failed: {exc}") from exc

    try:
        reader = csv.DictReader(io.StringIO(text))
        fieldnames = list(reader.fieldnames or [])
        rows = [
            {k: (v or "").strip() for k, v in row.items() if k is not None}
            for row in reader
        ]
    except csv.Error as exc:
        raise PlacesError("download_export_csv", 0, f"CSV parse failed: {exc}") from exc
    return fieldnames, rows


async def fair_use() -> Optional[dict[str, Any]]:
    """Normalized google_maps fair-use gauge, or None when the gauge can't be
    read (treated as 'unknown — proceed' by callers; the export itself is the
    hard wall)."""
    try:
        data = await _request("GET", _FAIR_USE_PATH)
    except PlacesError as exc:
        logger.warning("places fair-use gauge unavailable: %s", exc)
        return None
    gm = data.get("google_maps")
    if not isinstance(gm, dict):
        return None

    def _bucket(key: str) -> dict[str, Any]:
        b = gm.get(key) or {}
        return {
            "cap": b.get("cap"),
            "used": b.get("used"),
            "remaining": b.get("remaining"),
            "resets_at": b.get("resets_at"),
        }

    return {"daily": _bucket("daily"), "monthly": _bucket("monthly")}
