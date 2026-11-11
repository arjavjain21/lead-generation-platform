"""Standalone email-verification job engine (PR-1.1, 2026-11-11).

Stage 3 of the TAM → People → Verify workflow: verify a pasted/uploaded list
or a chunk of an enrichment job's emails through MailTester, as a BACKGROUND
job (job_type='enrichment', source_type='verify') reusing the platform's job
store, SSE/progress events, cancellation, heartbeat, incremental CSV writer
and generic download endpoint. NO enrichment-cascade calls — the only
external touch is ``mailtester_client.verify_email``.

Raw-facts cache (policy-independent): every definitive result is stored in
``verify_results`` keyed by (email_norm, verifier) with a TTL; sendability
policy (allow_catch_all) is applied at READ time, never baked into the
stored row. Catch-all (``mb``) is a DISTINCT classification — never
"verified": it proves the domain accepts all mail, not that this person's
mailbox exists. Verification is READ-ONLY: no contacts-DB writes, no
poisoning, no campaign/blocklist effects.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Optional

from . import mailtester_client

logger = logging.getLogger(__name__)

VERIFIER_NAME = "mailtester"

# Classifications (read-time policy applied on top of the raw code):
#   ok  -> "verified"       (deliverable; accepted for sending)
#   mb  -> "catch_all"      (domain accepts all mail — person unconfirmed;
#                            reportable-and-retained when allow_catch_all,
#                            always visible in the CSV regardless)
#   ko  -> "invalid"        (dead mailbox)
#   unavailable/other -> "unknown" (validator outage — fail-open evidence)
CLASS_VERIFIED = "verified"
CLASS_CATCH_ALL = "catch_all"
CLASS_INVALID = "invalid"
CLASS_UNKNOWN = "unknown"

VERIFY_CSV_COLUMNS = [
    "original_email",
    "email_norm",
    "classification",
    "verifier",
    "code",
    "message",
    "verified_at",
    "cache_hit",
    "source_job_id",
]

# Bounded knobs. Concurrency 8 sits safely under mailtester's process-wide
# 10 RPS limiter; batches above the sync ceiling always run as jobs.
SYNC_MAX_EMAILS = 25
BATCH_MAX_EMAILS = 50_000
VERIFY_CONCURRENCY = 8
_RESULT_TTL_DAYS = int(os.getenv("VERIFY_RESULT_TTL_DAYS", "30"))

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def normalize_email(email: str) -> str:
    """Lowercased, whitespace-stripped email ('' when not email-shaped)."""
    cleaned = str(email or "").strip().lower()
    # Accept "Name <a@b.com>" pastes via parseaddr.
    if "<" in cleaned and ">" in cleaned:
        cleaned = parseaddr(cleaned)[1].lower()
    return cleaned if _EMAIL_RE.match(cleaned) else ""


def classify(code: str, allow_catch_all: bool) -> tuple[str, bool]:
    """Map a raw verifier code to (classification, counted_as_deliverable).

    Read-time policy: ``allow_catch_all`` only changes whether a catch-all
    row is COUNTED as sendable in the summary — the classification itself
    stays ``catch_all`` either way (an mb address is never "verified").
    """
    if code == "ok":
        return CLASS_VERIFIED, True
    if code == "mb":
        return CLASS_CATCH_ALL, bool(allow_catch_all)
    if code == "ko":
        return CLASS_INVALID, False
    return CLASS_UNKNOWN, False


# ---------------------------------------------------------------------------
# Raw-facts cache (jobs.db table, guarded-create — call_tracker pattern)
# ---------------------------------------------------------------------------

def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS verify_results (
            email_norm   TEXT NOT NULL,
            verifier     TEXT NOT NULL,
            code         TEXT NOT NULL,
            message      TEXT,
            verified_at  TEXT NOT NULL,
            source_job_id TEXT,
            PRIMARY KEY (email_norm, verifier)
        )
        """
    )
    conn.commit()


def _get_cached(conn: sqlite3.Connection, email_norm: str) -> Optional[dict[str, Any]]:
    """A definitive cached row inside the TTL, else None. Policy-free."""
    try:
        row = conn.execute(
            "SELECT code, message, verified_at FROM verify_results "
            "WHERE email_norm=? AND verifier=?",
            (email_norm, VERIFIER_NAME),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    try:
        verified_at = datetime.fromisoformat(row[2])
        if verified_at.tzinfo is None:
            verified_at = verified_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) - verified_at > timedelta(days=_RESULT_TTL_DAYS):
            return None
    except ValueError:
        return None
    return {"code": row[0], "message": row[1] or "", "verified_at": row[2]}


def _store_result(
    conn: sqlite3.Connection,
    email_norm: str,
    code: str,
    message: str,
    source_job_id: str,
) -> str:
    """Persist a definitive raw result (never 'unavailable'). Returns verified_at."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO verify_results "
        "(email_norm, verifier, code, message, verified_at, source_job_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (email_norm, VERIFIER_NAME, code, message, now, source_job_id),
    )
    conn.commit()
    return now


# ---------------------------------------------------------------------------
# Input assembly
# ---------------------------------------------------------------------------

def dedupe_emails(raw: list[str]) -> tuple[list[tuple[str, str]], int]:
    """Normalize + dedupe, first occurrence wins.

    Returns ([(original, email_norm)], invalid_dropped) in input order.
    Non-email-shaped entries are dropped and counted (the caller surfaces
    them), never silently treated as verified.
    """
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    dropped = 0
    for raw_email in raw:
        original = str(raw_email or "").strip()
        norm = normalize_email(original)
        if not norm:
            dropped += 1
            continue
        if norm in seen:
            continue
        seen.add(norm)
        out.append((original or norm, norm))
    return out, dropped


def extract_job_emails(job_csv: Path, limit: int, offset: int,
                       only_with_email: bool, include_catch_all: bool) -> list[str]:
    """Pull emails from an enrichment job's output CSV (dm_email first, then
    the retained dm_email_catch_all column, then company_email)."""
    wanted: list[str] = []
    with open(job_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for col in (
                ["dm_email"] + (["dm_email_catch_all"] if include_catch_all else []) + ["company_email"]
            ):
                val = (row.get(col) or "").strip()
                if col == "dm_email_catch_all" and ";" in val:
                    # Multiple retained candidates are ';'-joined — verify each.
                    wanted.extend(v.strip() for v in val.split(";") if v.strip())
                elif val:
                    wanted.append(val)
            if only_with_email and not any(
                (row.get(c) or "").strip() for c in ("dm_email", "company_email")
            ):
                continue
    return wanted[offset: offset + limit]


# ---------------------------------------------------------------------------
# The engine (sync path ≤25, background job above)
# ---------------------------------------------------------------------------

async def verify_batch(
    emails: list[tuple[str, str]],
    *,
    allow_catch_all: bool,
    recheck: bool,
    conn: sqlite3.Connection,
    on_result=None,
    check_cancelled=None,
    source_job_id: str = "",
) -> dict[str, Any]:
    """Verify normalized emails; returns the summary block.

    ``on_result(original, email_norm, classification, verifier, code,
    message, verified_at, cache_hit)`` fires per email (the job runner
    streams it to the CSV + progress counters; the sync path ignores it).
    ``check_cancelled()`` -> True aborts promptly (already-done work stands).
    """
    _ensure_tables(conn)
    counters = {
        "total": len(emails),
        "verified": 0,
        "catch_all": 0,
        "invalid": 0,
        "unknown": 0,
        "cache_hits": 0,
        "unavailable": 0,
    }

    async with asyncio.Semaphore(VERIFY_CONCURRENCY):
        async def _one(original: str, email_norm: str) -> None:
            cached = None if recheck else _get_cached(conn, email_norm)
            if cached is not None:
                code, message, verified_at = cached["code"], cached["message"], cached["verified_at"]
                cache_hit = True
                counters["cache_hits"] += 1
            else:
                cache_hit = False
                try:
                    result = await mailtester_client.verify_email(_shared_client(), original)
                    code, message = result.get("code", ""), result.get("message", "")
                except RuntimeError:
                    # Validator outage / breaker open / missing key — fail-open
                    # evidence state, never cached, never "invalid".
                    code, message = "unavailable", "validator unavailable"
                if code in ("ok", "mb", "ko"):
                    verified_at = _store_result(conn, email_norm, code, message, source_job_id)
                else:
                    verified_at = datetime.now(timezone.utc).isoformat()
                    counters["unavailable"] += 1
            classification, _deliverable = classify(code, allow_catch_all)
            counters[classification] = counters.get(classification, 0) + 1
            if on_result is not None:
                on_result(original, email_norm, classification, VERIFIER_NAME,
                          code, message, verified_at, cache_hit)

        tasks = []
        for original, email_norm in emails:
            if check_cancelled is not None and check_cancelled():
                break
            tasks.append(_one(original, email_norm))
        if tasks:
            await asyncio.gather(*tasks)

    # Precise sendability denominator: definitive verdicts only (exclude
    # unavailable) — reconciliation math must never count outages as votes.
    return {
        **counters,
        "validated": counters["verified"] + counters["catch_all"] + counters["invalid"],
        "sendable": counters["verified"] + (counters["catch_all"] if allow_catch_all else 0),
        "allow_catch_all": allow_catch_all,
        "verifier": VERIFIER_NAME,
    }


# ---------------------------------------------------------------------------
# Shared httpx client (mailtester takes an AsyncClient; responses never read
# here beyond verify_email's own handling)
# ---------------------------------------------------------------------------

_shared_http: Any = None


def _shared_client() -> Any:
    global _shared_http
    if _shared_http is None:
        import httpx

        class _NullResponse:
            """verify_email only needs .get()/params plumbing on OUR side; the
            real client is passed through untouched — this shim exists purely
            to satisfy type checkers and tests that swap it."""

        _shared_http = httpx.AsyncClient(timeout=30.0)
    return _shared_http


async def close_shared_client() -> None:
    global _shared_http
    if _shared_http is not None:
        try:
            await _shared_http.aclose()
        except Exception:
            pass
        _shared_http = None
