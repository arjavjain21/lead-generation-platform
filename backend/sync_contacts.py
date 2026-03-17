"""
Sync job results to Contacts DB (leadsdatabase.cc).

Uses place_id for deduplication — records already synced are skipped.
Includes retry logic with exponential backoff for reliability.
"""

from __future__ import annotations

import csv
import logging
import os
import random
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

CONTACTS_API_BASE = os.getenv("CONTACTS_API_BASE_URL", "https://leadsdatabase.cc").rstrip("/")
BLOCKLIST_DOMAINS = {
    "facebook.com", "fb.com", "google.com", "google.co", "googleapis.com",
    "wikipedia.org", "yelp.com", "linkedin.com", "instagram.com",
    "twitter.com", "x.com", "youtube.com", "maps.google.com",
    "biz.google.com", "plus.google.com", "goo.gl",
    "apple.com", "bing.com", "yahoo.com", "wix.com", "squarespace.com",
    "godaddy.com", "weebly.com",
}

DB_LOCK = threading.Lock()
DATA_DIR = Path(__file__).parent / "data"
SYNC_STATE_PATH = DATA_DIR / "contacts_sync_state.db"

# Retry configuration
_MAX_RETRIES = 3
_BASE_BACKOFF = 2.0
_MAX_BACKOFF = 30.0


def _headers() -> dict[str, str]:
    token = os.getenv("CONTACTS_API_TOKEN", "")
    if not token:
        raise RuntimeError("CONTACTS_API_TOKEN environment variable is not set")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _backoff_delay(attempt: int, retry_after: Optional[float] = None) -> float:
    """Return seconds to wait before the next attempt."""
    if retry_after is not None:
        return min(retry_after, _MAX_BACKOFF)
    # Exponential backoff with jitter
    cap = min(_MAX_BACKOFF, _BASE_BACKOFF * (2 ** attempt))
    return random.uniform(0, cap)


def _init_sync_state() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(SYNC_STATE_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS synced_place_ids (
            place_id TEXT PRIMARY KEY,
            synced_at TEXT NOT NULL
        );
        """
    )
    conn.commit()
    return conn


def _load_synced(conn: sqlite3.Connection) -> set[str]:
    with DB_LOCK:
        rows = conn.execute("SELECT place_id FROM synced_place_ids").fetchall()
    return {r[0] for r in rows}


def _mark_synced(conn: sqlite3.Connection, place_id: str) -> None:
    with DB_LOCK:
        conn.execute(
            "INSERT OR IGNORE INTO synced_place_ids (place_id, synced_at) VALUES (?, ?)",
            (place_id, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()


def _extract_domain(url: str) -> Optional[str]:
    if not url or not url.strip():
        return None
    raw = url.strip()
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    try:
        host = (urlparse(raw).hostname or "").lower().removeprefix("www.")
        return host if host else None
    except Exception:
        return None


def _is_blocked(domain: str) -> bool:
    if not domain:
        return True
    parts = domain.split(".")
    for i in range(len(parts) - 1):
        if ".".join(parts[i:]) in BLOCKLIST_DOMAINS:
            return True
    return False


def _should_retry(status_code: int) -> bool:
    """Determine if a request should be retried based on status code."""
    return status_code == 429 or status_code >= 500


def _upsert_one(row: dict[str, Any], retries: int = 3) -> bool:
    website = (row.get("website") or "").strip()
    domain = _extract_domain(website)
    if not domain or _is_blocked(domain):
        return False

    name = (row.get("name") or "").strip() or "Unknown"
    company_website = website if website.startswith(("http://", "https://")) else f"https://{domain}"

    payload = {
        "email": f"contact@{domain}",
        "domain": domain,
        "full_name": name,
        "company_name": name,
        "company_domain": domain,
        "company_website": company_website,
    }

    for attempt in range(retries):
        try:
            resp = requests.post(
                f"{CONTACTS_API_BASE}/v1/persons/upsert",
                headers=_headers(),
                json=payload,
                timeout=30,
            )

            # Check if we should retry this error
            if _should_retry(resp.status_code):
                retry_after_raw = resp.headers.get("Retry-After")
                retry_after = float(retry_after_raw) if retry_after_raw else None
                delay = _backoff_delay(attempt, retry_after)

                if attempt < retries - 1:
                    logger.warning(
                        "Contacts API sync returned %d for %s (attempt %d/%d), retrying in %.1fs",
                        resp.status_code, domain, attempt + 1, retries, delay,
                    )
                    time.sleep(delay)
                    continue

                # Exhausted retries
                logger.error("Contacts API sync returned %d for %s, exhausted retries", resp.status_code, domain)
                return False

            # Success
            resp.raise_for_status()
            return True

        except requests.RequestException as e:
            if attempt < retries - 1:
                delay = _backoff_delay(attempt)
                logger.warning(
                    "Contacts API sync error for %s (attempt %d/%d): %s - retrying in %.1fs",
                    domain, attempt + 1, retries, e, delay,
                )
                time.sleep(delay)
            else:
                logger.error("Contacts API sync failed for %s (%s): %s", name, domain, e)
                return False

    return False


def sync_job_to_contacts(output_path: Path) -> dict[str, int]:
    """
    Sync a job's output CSV to the Contacts DB.
    Returns {synced, skipped, failed}.
    """
    token = os.getenv("CONTACTS_API_TOKEN", "")
    if not token:
        raise RuntimeError("CONTACTS_API_TOKEN environment variable is not set")

    if not output_path.exists():
        raise FileNotFoundError(f"Output file not found: {output_path}")

    conn = _init_sync_state()
    synced_ids = _load_synced(conn)

    rows: list[dict[str, Any]] = []
    with open(output_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))

    to_sync: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        pid = (row.get("place_id") or "").strip()
        if not pid or pid in synced_ids or pid in seen:
            continue
        seen.add(pid)
        to_sync.append(row)

    result = {"synced": 0, "skipped": len(rows) - len(to_sync), "failed": 0}
    for row in to_sync:
        pid = row.get("place_id", "")
        if _upsert_one(row):
            _mark_synced(conn, pid)
            result["synced"] += 1
        else:
            result["failed"] += 1

    conn.close()
    return result


def sync_enrichment_to_contacts(output_path: Path) -> dict[str, int]:
    """
    Sync an enrichment job's output CSV to the Contacts DB as person records.
    Syncs by email address - updates existing records or creates new ones.
    Returns {synced, skipped, failed}.
    """
    token = os.getenv("CONTACTS_API_TOKEN", "")
    if not token:
        raise RuntimeError("CONTACTS_API_TOKEN environment variable is not set")

    if not output_path.exists():
        raise FileNotFoundError(f"Output file not found: {output_path}")

    rows: list[dict[str, Any]] = []
    with open(output_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))

    result = {"synced": 0, "skipped": 0, "failed": 0}

    # Deduplicate by email within this file
    seen_emails: set[str] = set()
    unique_rows: list[dict[str, Any]] = []
    for row in rows:
        email = (row.get("dm_email") or "").strip().lower()
        if email and email not in seen_emails and email != "no_email" and "@" in email:
            seen_emails.add(email)
            unique_rows.append(row)
    rows = unique_rows

    for row in rows:
        # Get email - required for person sync
        email = (row.get("dm_email") or "").strip()
        if not email or email == "no_email" or "@" not in email:
            result["skipped"] += 1
            continue

        # Extract domain from email if not provided
        domain = (row.get("domain") or "").strip()
        if not domain and "@" in email:
            domain = email.split("@")[1].strip().lower()

        if not domain:
            result["skipped"] += 1
            continue

        # Build person payload
        payload = {
            "email": email,
            "domain": domain,
            "full_name": (row.get("dm_full_name") or "").strip(),
            "first_name": (row.get("dm_first_name") or "").strip(),
            "last_name": (row.get("dm_last_name") or "").strip(),
            "linkedin_url": (row.get("dm_linkedin_url") or "").strip(),
            "title": (row.get("dm_title") or "").strip(),
            "company_domain": domain,
        }

        # Remove empty fields
        payload = {k: v for k, v in payload.items() if v}

        try:
            resp = requests.post(
                f"{CONTACTS_API_BASE}/v1/persons/upsert",
                headers=_headers(),
                json=payload,
                timeout=30,
            )
            if resp.status_code in (200, 201):
                result["synced"] += 1
            elif resp.status_code == 400 and "duplicate" in resp.text.lower():
                # Already exists - count as synced (it's already in the DB)
                result["synced"] += 1
            else:
                logger.warning(f"Failed to sync person {email}: {resp.status_code} {resp.text}")
                result["failed"] += 1
        except Exception as e:
            logger.error(f"Error syncing person {email}: {e}")
            result["failed"] += 1

    return result
