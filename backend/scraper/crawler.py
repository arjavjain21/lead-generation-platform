"""
Async Google Maps scraper using the scraper.tech API.

API endpoint: GET https://api.scraper.tech/searchmaps.php
Auth header:  Scraper-key: <api_key>
Params:       query, lat, lng, zoom, limit, country, lang, offset

For each job:
  - Iterates over filtered centers × [10, 11, 12] zooms
  - 8 concurrent workers (asyncio.Semaphore)
  - In-memory deduplication by place_id (fallback: hash of name+website+address)
  - Haversine radius filtering
  - Progress reported via on_progress callback
  - Returns path to output CSV
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import math
import os
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Coroutine

import httpx

logger = logging.getLogger(__name__)

API_URL = "https://api.scraper.tech/searchmaps.php"
DEFAULT_ZOOMS = [10, 11, 12]
MAX_RETRIES = 3
BACKOFF_BASE = 1.5
REQUEST_TIMEOUT = 30
CONCURRENCY = 8

OUTPUT_COLS = [
    "dedupe_key", "query", "center_name", "center_state", "center_lat", "center_lng", "zoom",
    "place_id", "business_id", "name", "category_name", "full_address", "city", "city_state",
    "latitude", "longitude", "distance_km", "rating", "review_count",
    "website", "phone", "types", "price_level", "timezone", "working_hours",
    "is_claimed", "verified", "is_permanently_closed", "is_temporarily_closed",
    "place_link", "photo_count", "first_photo_url", "inserted_at",
]


# ---------------------------------------------------------------------------
# Pure helpers (no I/O)
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlng / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def choose_radius_km(query: str) -> float:
    q = query.strip().lower()
    city_heavy = {
        "doctor", "dentist", "attorney", "lawyer", "accountant", "cpa",
        "insurance agency", "marketing agency", "advertising agency",
        "software company", "public relations firm", "chiropractor",
    }
    broader_local = {
        "plumber", "electrician", "roofer", "hvac contractor",
        "pest control service", "locksmith", "towing service",
    }
    sparse_rural = {"campground", "rv park", "marina", "ranch"}

    if q in city_heavy:
        return 100.0
    if q in broader_local:
        return 125.0
    if q in sparse_rural:
        return 175.0
    return 100.0


def parse_types(item: dict[str, Any]) -> list[str]:
    types = item.get("types")
    if isinstance(types, list):
        return [str(t).strip().lower() for t in types if str(t).strip()]
    return []


def matches_expected_types(item: dict[str, Any], expected_types: list[str]) -> bool:
    if not expected_types:
        return True
    item_types = set(parse_types(item))
    expected = {t.strip().lower() for t in expected_types if t.strip()}
    return len(item_types.intersection(expected)) > 0


def dedupe_key(item: dict[str, Any]) -> str:
    place_id = (item.get("place_id") or "").strip()
    if place_id:
        return f"place_id::{place_id.lower()}"
    raw = "|".join([
        (item.get("name") or "").strip().lower(),
        (item.get("website") or "").strip().lower(),
        (item.get("full_address") or "").strip().lower(),
    ])
    return "fallback::" + hashlib.md5(raw.encode()).hexdigest()


def flatten_item(
    item: dict[str, Any],
    center: dict[str, Any],
    zoom: int,
    query: str,
    radius_km: float,
) -> dict[str, Any] | None:
    """Flatten + radius-filter one API result. Returns None if outside radius."""
    try:
        lat = float(item.get("latitude") or 0)
        lng = float(item.get("longitude") or 0)
    except (TypeError, ValueError):
        return None

    if lat == 0 and lng == 0:
        return None

    dist = haversine_km(center["lat"], center["lng"], lat, lng)
    if dist > radius_km:
        return None

    city_raw = str(item.get("city") or "")
    parts = city_raw.rsplit(",", 1)
    city_name = parts[0].strip() if parts else city_raw
    city_state = parts[1].strip() if len(parts) == 2 else ""

    types_raw = item.get("types")
    types_str = " | ".join(str(t) for t in types_raw) if isinstance(types_raw, list) else ""

    photos = item.get("photos") or []
    first_photo = photos[0]["src"] if photos and isinstance(photos[0], dict) else ""

    working_hours = item.get("working_hours")

    return {
        "dedupe_key": dedupe_key(item),
        "query": query,
        "center_name": center["name"],
        "center_state": center["state"],
        "center_lat": center["lat"],
        "center_lng": center["lng"],
        "zoom": zoom,
        "place_id": (item.get("place_id") or "").strip(),
        "business_id": (item.get("business_id") or "").strip(),
        "name": (item.get("name") or "").strip(),
        "category_name": (item.get("categoryName") or "").strip(),
        "full_address": (item.get("full_address") or "").strip(),
        "city": city_name,
        "city_state": city_state,
        "latitude": lat,
        "longitude": lng,
        "distance_km": round(dist, 2),
        "rating": str(item.get("rating") or ""),
        "review_count": str(item.get("review_count") or ""),
        "website": (item.get("website") or "").strip(),
        "phone": (item.get("phone_number") or "").strip(),
        "types": types_str,
        "price_level": str(item.get("price_level") or ""),
        "timezone": (item.get("timezone") or "").strip(),
        "working_hours": json.dumps(working_hours, ensure_ascii=False) if working_hours else "",
        "is_claimed": item.get("is_claimed"),
        "verified": item.get("verified"),
        "is_permanently_closed": item.get("is_permanently_closed"),
        "is_temporarily_closed": item.get("is_temporarily_closed"),
        "place_link": (item.get("place_link") or "").strip(),
        "photo_count": len(photos),
        "first_photo_url": first_photo,
        "inserted_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Async API caller
# ---------------------------------------------------------------------------

def _should_retry(status_code: int) -> bool:
    """Determine if a request should be retried based on status code."""
    return status_code == 429 or status_code >= 500


def _backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Return seconds to wait before the next attempt."""
    if retry_after is not None:
        return min(retry_after, 30.0)
    # Exponential backoff with jitter
    cap = min(30.0, BACKOFF_BASE * (2 ** attempt))
    return BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1.0)


async def _fetch_one(
    client: httpx.AsyncClient,
    query: str,
    center: dict[str, Any],
    zoom: int,
    semaphore: asyncio.Semaphore,
) -> list[dict[str, Any]]:
    """Call the API once with retry/backoff. Returns raw item list."""
    params = {
        "query": query,
        "limit": "1000",
        "country": center.get("country", "us"),
        "lang": "en",
        "lat": str(center["lat"]),
        "lng": str(center["lng"]),
        "offset": "0",
        "zoom": str(zoom),
    }

    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            async with semaphore:
                resp = await client.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)

            # Check for retryable status codes
            if _should_retry(resp.status_code):
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)

                if attempt < MAX_RETRIES:
                    logger.warning(
                        "Scraper API rate limited (429) or server error (%d) for %s zoom=%d "
                        "(attempt %d/%d), retrying in %.1fs",
                        resp.status_code, center["name"], zoom, attempt + 1, MAX_RETRIES + 1, delay,
                    )
                    await asyncio.sleep(delay)
                    continue

                # Exhausted retries
                resp.raise_for_status()

            # Success or non-retryable error
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data") or []
            if not isinstance(items, list):
                raise ValueError(f"Unexpected payload shape: {type(items)}")
            return items

        except httpx.HTTPStatusError as e:
            # Don't retry client errors (4xx except 429)
            if e.response.status_code < 500 and e.response.status_code != 429:
                raise
            last_error = e
            if attempt < MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Scraper API HTTP error (attempt %d/%d) for %s zoom=%d: %s — retrying in %.1fs",
                    attempt + 1, MAX_RETRIES + 1, center["name"], zoom, e, delay,
                )
                await asyncio.sleep(delay)

        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Scraper API error (attempt %d/%d) for %s zoom=%d: %s — retrying in %.1fs",
                    attempt + 1, MAX_RETRIES + 1, center["name"], zoom, e, delay,
                )
                await asyncio.sleep(delay)

    raise last_error or RuntimeError("Unknown error")


# ---------------------------------------------------------------------------
# Main crawl function
# ---------------------------------------------------------------------------

ProgressCallback = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


async def run_crawl(
    job_id: str,
    query: str,
    centers: list[dict[str, Any]],
    api_key: str,
    output_path: Path,
    on_progress: ProgressCallback,
    zooms: list[int] | None = None,
    expected_types: list[str] | None = None,
    cancelled_jobs: Optional[set[str]] = None,
) -> int:
    """
    Run the full crawl for a job. Writes results to output_path (CSV).
    Calls on_progress for each completed task.
    Returns total unique results written.
    """
    if zooms is None:
        zooms = DEFAULT_ZOOMS

    radius_km = choose_radius_km(query)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    seen_keys: dict[str, bool] = {}

    tasks: list[tuple[dict[str, Any], int]] = [
        (center, zoom)
        for center in centers
        for zoom in zooms
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers = {"Scraper-key": api_key}

    async with httpx.AsyncClient(headers=headers) as client:
        with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=OUTPUT_COLS, extrasaction="ignore")
            writer.writeheader()
            write_lock = asyncio.Lock()

            async def process_task(center: dict[str, Any], zoom: int, task_seq: int) -> int:
                # Check if job was cancelled
                if cancelled_jobs and job_id in cancelled_jobs:
                    logger.info("[job %s] Job cancelled, stopping at task %d/%d", job_id[:8], task_seq + 1, len(tasks))
                    raise RuntimeError(f"Job {job_id} was cancelled")

                new_results = 0
                try:
                    items = await _fetch_one(client, query, center, zoom, semaphore)
                    rows_to_write: list[dict[str, Any]] = []

                    for item in items:
                        flat = flatten_item(item, center, zoom, query, radius_km)
                        if flat is None:
                            continue
                        if expected_types and not matches_expected_types(item, expected_types):
                            continue
                        key = flat["dedupe_key"]
                        if key not in seen_keys:
                            seen_keys[key] = True
                            rows_to_write.append(flat)

                    if rows_to_write:
                        async with write_lock:
                            writer.writerows(rows_to_write)
                            csv_file.flush()

                    new_results = len(rows_to_write)
                    logger.info(
                        "[job %s] task %d/%d: %s zoom=%d → %d new results",
                        job_id[:8], task_seq + 1, len(tasks), center["name"], zoom, new_results,
                    )

                except Exception as e:
                    logger.error(
                        "[job %s] task %d/%d FAILED: %s zoom=%d — %s",
                        job_id[:8], task_seq + 1, len(tasks), center["name"], zoom, e,
                    )

                await on_progress({
                    "task_seq": task_seq,
                    "total_tasks": len(tasks),
                    "center_name": center["name"],
                    "center_state": center["state"],
                    "zoom": zoom,
                    "new_results": new_results,
                    "total_results": len(seen_keys),
                    "task_done": True,
                })

                return new_results

            coros = [
                process_task(center, zoom, i)
                for i, (center, zoom) in enumerate(tasks)
            ]
            await asyncio.gather(*coros)

    return len(seen_keys)
