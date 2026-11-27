"""Per-user activity audit trail (2026-11-26).

Answers "who did what": every audited action — non-GET ``/api/*`` mutations,
download-shaped GETs, logins (success + failure) and admin operations — lands
in the ``user_activity_log`` table (schema in ``shared/db.py::init_db``).

Write doctrine (non-negotiable, copied from the ``last_used_at`` debounce RCA
2026-08-24 in shared/auth.py): per-request SQLite writes can stall the event
loop for the full busy_timeout under write contention. Therefore:

- ``audit(..., sync=False)`` (the default) only does ``queue.put_nowait`` —
  never blocks, never raises. A single daemon writer thread per gunicorn
  worker drains the queue in batches (≤ ``_BATCH_MAX`` rows, one
  ``executemany`` + one commit per flush). The request path cannot feel the
  write lock.
- ``sync=True`` inserts inline on the caller's thread — reserved for the
  login path ONLY (~5/day), where the lockout counter must see the row
  immediately or a brute-forcer gets free retries during the 1s flush window.
- On any SQLite error the writer recycles its connection (``db.recycle_db``,
  the 2026-10/11 wedge doctrine) and drops the batch — the audit trail must
  never take a request down with it.

Volume is bounded by construction: the middleware only audits mutations and
download GETs, never the ~226K/day automated GET pollers. Worst case is a few
thousand rows/day (<3 MB); ``prune_old`` keeps retention at
``AUDIT_RETENTION_DAYS`` (default 90) via the boot-time prune task in main.py.

Kill switch: ``ENABLE_AUDIT_LOG=false`` disables both the module and the
middleware registration (main.py) without touching anything else.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from . import db

logger = logging.getLogger(__name__)

AUDIT_ENABLED = os.getenv("ENABLE_AUDIT_LOG", "true").lower() in ("1", "true", "yes")
AUDIT_RETENTION_DAYS = int(os.getenv("AUDIT_RETENTION_DAYS", "90"))

_FLUSH_INTERVAL_S = 1.0   # max latency of a queued row
_BATCH_MAX = 200          # rows per executemany
_QUEUE_MAX = 10_000       # ~hours of burst headroom; drop-on-full beyond that
_META_MAX_CHARS = 2048    # meta JSON is truncated, never unbounded
_DROP_WARN_INTERVAL_S = 60.0  # rate-limit the drop warning

_queue: "queue.Queue[tuple]" = queue.Queue(maxsize=_QUEUE_MAX)
_stop_event = threading.Event()
_thread: Optional[threading.Thread] = None
_start_lock = threading.Lock()
_dropped_rows = 0
_last_drop_warn = 0.0

_INSERT_SQL = (
    "INSERT INTO user_activity_log "
    "(ts, user_id, email, key_id, action, method, path, resource_id, status, ip, meta) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_for(
    action: str,
    method: Optional[str],
    path: Optional[str],
    user: Optional[dict[str, Any]],
    status: Optional[int],
    ip: Optional[str],
    resource_id: Optional[str],
    meta: Optional[dict[str, Any]],
    email_override: Optional[str],
) -> tuple:
    user = user or {}
    meta_text = None
    if meta:
        try:
            meta_text = json.dumps(meta, default=str)[:_META_MAX_CHARS]
        except (TypeError, ValueError):
            meta_text = '{"note": "unserializable meta"}'
    return (
        _now_iso(),
        user.get("user_id"),
        email_override or user.get("email"),
        user.get("key_id"),
        action,
        method,
        path,
        resource_id,
        status,
        ip,
        meta_text,
    )


def _warn_dropped(count: int, reason: str) -> None:
    """Rate-limited warning so a wedged writer can't flood the journal."""
    global _dropped_rows, _last_drop_warn
    _dropped_rows += count
    now = time.monotonic()
    if now - _last_drop_warn >= _DROP_WARN_INTERVAL_S:
        _last_drop_warn = now
        logger.warning(
            "audit rows dropped (total %d, latest %d rows): %s",
            _dropped_rows, count, reason,
        )


def _write_rows(rows: list[tuple]) -> None:
    """One executemany + one commit. On SQLite trouble: recycle this
    thread's connection and drop the batch — never propagate."""
    if not rows:
        return
    try:
        conn = db.get_db()
        conn.executemany(_INSERT_SQL, rows)
        conn.commit()
    except sqlite3.Error as exc:
        db.recycle_db(reason=f"audit insert failed: {exc}")
        _warn_dropped(len(rows), str(exc))


def _writer_loop() -> None:
    """Daemon writer: block up to _FLUSH_INTERVAL_S, then drain up to
    _BATCH_MAX rows in one transaction. Exits when stop() is set AND the
    queue is empty (final drain included)."""
    while not (_stop_event.is_set() and _queue.empty()):
        try:
            first = _queue.get(timeout=_FLUSH_INTERVAL_S)
        except queue.Empty:
            continue
        batch = [first]
        while len(batch) < _BATCH_MAX:
            try:
                batch.append(_queue.get_nowait())
            except queue.Empty:
                break
        _write_rows(batch)


def _ensure_started() -> None:
    """Lazily start the writer if audit rows arrive before main.py's
    audit.start() (import-order edge in tests). Idempotent + thread-safe."""
    global _thread
    if _thread is not None and _thread.is_alive():
        return
    with _start_lock:
        if _thread is None or not _thread.is_alive():
            _stop_event.clear()
            _thread = threading.Thread(
                target=_writer_loop, name="audit-writer", daemon=True
            )
            _thread.start()


def start() -> None:
    """Start the writer thread (called from main.py startup). Idempotent."""
    if not AUDIT_ENABLED:
        return
    _ensure_started()


def stop(timeout: float = 2.0) -> None:
    """Signal the writer, give it `timeout` seconds to flush + join.
    Called from the lifespan shutdown path."""
    _stop_event.set()
    thread = _thread
    if thread is not None and thread.is_alive():
        thread.join(timeout=timeout)


def audit(
    action: str,
    *,
    method: Optional[str] = None,
    path: Optional[str] = None,
    user: Optional[dict[str, Any]] = None,
    status: Optional[int] = None,
    ip: Optional[str] = None,
    resource_id: Optional[str] = None,
    meta: Optional[dict[str, Any]] = None,
    email: Optional[str] = None,
    sync: bool = False,
) -> None:
    """Record one action. Never raises.

    ``user`` is the resolved identity dict (JWT payload or API-key user);
    ``email`` overrides/sets the email column for anonymous attempts
    (login_failed carries the attempted address). ``sync=True`` inserts
    inline — login/lockout path only, see module docstring.
    """
    if not AUDIT_ENABLED:
        return
    row = _row_for(action, method, path, user, status, ip, resource_id, meta, email)
    if sync:
        _write_rows([row])
        return
    try:
        _ensure_started()
        _queue.put_nowait(row)
    except queue.Full:
        _warn_dropped(1, "queue full")
    except Exception as exc:  # belt and suspenders — audit must never raise
        _warn_dropped(1, f"unexpected enqueue error: {exc}")


# ---------------------------------------------------------------------------
# Reads (admin activity feed + login lockout)
# ---------------------------------------------------------------------------

def query_activity(
    email: Optional[str] = None,
    action: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Filter the audit log for the admin feed. Returns (rows, total)."""
    conditions: list[str] = []
    params: list[Any] = []
    if email:
        conditions.append("email = ?")
        params.append(email.strip().lower())
    if action:
        conditions.append("action = ?")
        params.append(action)
    if since:
        conditions.append("ts >= ?")
        params.append(since)
    if until:
        conditions.append("ts <= ?")
        params.append(until)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    conn = db.get_db()
    total = conn.execute(
        f"SELECT COUNT(*) FROM user_activity_log {where}", params
    ).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM user_activity_log {where} ORDER BY id DESC LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()
    return [dict(r) for r in rows], int(total)


def count_recent(
    action: str,
    email: Optional[str] = None,
    ip: Optional[str] = None,
    window_min: int = 15,
) -> int:
    """Count matching rows inside the trailing window. Used by the login
    lockout: distinct rows matching EITHER the email OR the source IP
    (UNION of two index-served selects — a plain OR would table-scan)."""
    if not email and not ip:
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=window_min)).isoformat()
    selects = []
    params: list[Any] = []
    if email:
        selects.append("SELECT id FROM user_activity_log WHERE action = ? AND ts >= ? AND email = ?")
        params.extend([action, cutoff, email.strip().lower()])
    if ip:
        selects.append("SELECT id FROM user_activity_log WHERE action = ? AND ts >= ? AND ip = ?")
        params.extend([action, cutoff, ip])
    union = " UNION ".join(selects)
    try:
        row = db.get_db().execute(
            f"SELECT COUNT(*) FROM ({union})", params
        ).fetchone()
        return int(row[0]) if row else 0
    except sqlite3.Error as exc:
        logger.warning("audit count_recent failed (allowing login): %s", exc)
        return 0


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------

def prune_old(days: int = AUDIT_RETENTION_DAYS, batch_size: int = 5000) -> int:
    """Delete rows older than `days`, in bounded batches (pattern:
    call_tracker.purge_old). Called at boot from main.py, marker-file deduped
    across the 4 gunicorn workers."""
    if not AUDIT_ENABLED:
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    conn = db.get_db()
    deleted = 0
    while True:
        ids = [
            r[0]
            for r in conn.execute(
                "SELECT id FROM user_activity_log WHERE ts < ? ORDER BY id LIMIT ?",
                (cutoff, batch_size),
            ).fetchall()
        ]
        if not ids:
            break
        conn.execute(
            f"DELETE FROM user_activity_log WHERE id IN ({','.join('?' * len(ids))})",
            ids,
        )
        conn.commit()
        deleted += len(ids)
    if deleted:
        logger.info("audit log pruned: %d rows older than %d days", deleted, days)
    return deleted


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------

def client_ip(request: Any) -> str:
    """Best-effort client IP. gunicorn runs WITHOUT --proxy-headers, so
    request.client.host is always 127.0.0.1 behind nginx — the rightmost
    X-Forwarded-For entry (the one OUR nginx appended) is the real peer.
    Rightmost is spoof-resistant: a client lying in XFF only affects entries
    to its left."""
    host = request.client.host if request.client else ""
    if host in ("127.0.0.1", "::1", "localhost"):
        xff = request.headers.get("x-forwarded-for", "")
        if xff:
            return xff.split(",")[-1].strip()
    return host or "unknown"
