#!/usr/bin/env python3
"""
BetterEnrich usage monitor — tracks credit balance + API call health and alerts
on abnormal usage (key death, high burn rate, low balance, 401 storms).

Designed to run every 15 min from better-enrich-usage-monitor.timer (systemd).
Self-contained: reads config from backend/.env, writes readings to a dedicated
SQLite table, alerts to the platform alerts.log and (for CRITICAL states) email.

Exit code is always 0 — a monitor must never fail its unit; all errors are
logged as alerts instead.

Tables / files it touches (all additive, no app schema changes):
    jobs.db: better_enrich_credits   (created here, INSERT-only)
    backend/data/be_monitor_state.json  (alert dedupe timestamps)
    /var/www/lead-generation-platform/alerts.log  (append)

Env knobs (backend/.env):
    BE_ALERT_CPH         burn-rate alert threshold, credits/hour (default 250)
    BE_ALERT_FLOOR       low-balance alert threshold, credits    (default 1000)
    BE_ALERT_ERROR_PCT   1h error-share alert threshold, percent (default 50)
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import sqlite3
import sys
import time
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

import httpx

# ---------------------------------------------------------------------------
# Paths & config
# ---------------------------------------------------------------------------
BACKEND_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BACKEND_DIR.parent
DB_PATH = BACKEND_DIR / "data" / "jobs.db"
STATE_PATH = BACKEND_DIR / "data" / "be_monitor_state.json"
ALERT_LOG = REPO_ROOT / "alerts.log"

# Load .env (lightweight — no python-dotenv dependency for the timer path).
ENV = {}
ENV_FILE = BACKEND_DIR / ".env"
if ENV_FILE.exists():
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            ENV[key.strip()] = value.strip()

API_KEY = ENV.get("BETTER_ENRICH_API_KEY", "")
BASE_URL = ENV.get("BETTER_ENRICH_BASE_URL", "https://app.betterenrich.com")

BURN_CPH = float(ENV.get("BE_ALERT_CPH", "250"))
BALANCE_FLOOR = float(ENV.get("BE_ALERT_FLOOR", "1000"))
ERROR_PCT = float(ENV.get("BE_ALERT_ERROR_PCT", "50"))
CREDIT_PRICE = 0.024  # $/credit, Professional pack rate (pricing page 2026-09)

SMTP_SERVER = ENV.get("SMTP_SERVER", "")
SMTP_PORT = int(ENV.get("SMTP_PORT", "465") or 465)
SMTP_USER = ENV.get("SMTP_USER", "")
SMTP_PASSWORD = ENV.get("SMTP_PASSWORD", "")
SENDER = ENV.get("SENDER_EMAIL", SMTP_USER)
RECIPIENT = ENV.get("DEFAULT_RECIPIENT", "")

# Re-notify intervals per alert kind (seconds) so sustained states don't spam.
RENOTIFY = {
    "key_down": 6 * 3600,
    "burn_high": 6 * 3600,
    "balance_low": 12 * 3600,
    "error_storm": 2 * 3600,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s be-usage-monitor %(levelname)s %(message)s",
)
log = logging.getLogger("be_monitor")

# ---------------------------------------------------------------------------
# Alert delivery
# ---------------------------------------------------------------------------


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _log_alert(severity: str, message: str) -> None:
    """Append to the platform alerts.log using monitor.sh's format."""
    try:
        with ALERT_LOG.open("a") as fh:
            fh.write(f"[{severity}] [{_now_utc()}] be-usage: {message}\n")
    except OSError as exc:
        log.error("cannot write alerts.log: %s", exc)


def _send_email(subject: str, body: str) -> None:
    """Best-effort CRITICAL email; failures are logged, never raised."""
    if not all([SMTP_SERVER, SMTP_USER, SMTP_PASSWORD, SENDER, RECIPIENT]):
        log.info("email not configured, skipping alert email")
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = SENDER
        msg["To"] = RECIPIENT
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, timeout=20) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SENDER, [RECIPIENT], msg.as_string())
        log.info("alert email sent: %s", subject)
    except Exception as exc:
        log.error("alert email failed: %s", exc)


def _load_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text())
    except (OSError, ValueError):
        return {"last_notified": {}, "key_state": "unknown"}


def _save_state(state: dict) -> None:
    try:
        STATE_PATH.write_text(json.dumps(state, indent=1))
    except OSError as exc:
        log.error("cannot write state file: %s", exc)


def notify(kind: str, severity: str, message: str, state: dict) -> None:
    """Log + email an alert, deduped by kind using RENOTIFY intervals."""
    now = time.time()
    last = state.get("last_notified", {}).get(kind, 0)
    if now - last < RENOTIFY.get(kind, 3600):
        return
    _log_alert(severity, message)
    if severity == "CRITICAL":
        _send_email(f"[BetterEnrich] {kind}: {severity}", message)
    state.setdefault("last_notified", {})[kind] = now


# ---------------------------------------------------------------------------
# Data gathering
# ---------------------------------------------------------------------------


def fetch_credits() -> tuple[Optional[dict], Optional[int]]:
    """Return (parsed credits, http status). Parsed is None on any failure."""
    if not API_KEY:
        return None, None
    try:
        resp = httpx.get(
            f"{BASE_URL}/api/v1/credits",
            headers={"Authorization": API_KEY},
            timeout=15.0,
        )
    except httpx.HTTPError as exc:
        log.error("credits request transport error: %s", exc)
        return None, None
    if resp.status_code != 200:
        return None, resp.status_code
    try:
        data = resp.json()
    except ValueError:
        log.error("credits response not JSON")
        return None, resp.status_code
    return (
        {
            "onetime": _as_float(data.get("onetimeCredit")),
            "subscription": _as_float(data.get("subscriptionCredit")),
            "total": _as_float(data.get("totalCredit")),
        },
        resp.status_code,
    )


def _as_float(value) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def call_stats_last_hour() -> dict:
    """BE call/email stats from provider_call_log + ledger for the last hour."""
    since = datetime.fromtimestamp(time.time() - 3600, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M"
    )
    stats = {"calls": 0, "ok": 0, "auth_err": 0, "rate_limited": 0, "emails": 0}
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        try:
            row = conn.execute(
                """SELECT COUNT(*),
                          SUM(status BETWEEN 200 AND 299),
                          SUM(status IN (401, 402, 403)),
                          SUM(status = 429)
                   FROM provider_call_log
                   WHERE provider='better_enrich' AND ts >= ?""",
                (since,),
            ).fetchone()
            stats["calls"], stats["ok"], stats["auth_err"], stats["rate_limited"] = (
                row[0] or 0,
                row[1] or 0,
                row[2] or 0,
                row[3] or 0,
            )
            stats["emails"] = conn.execute(
                "SELECT COUNT(*) FROM provider_email_ledger "
                "WHERE provider='better_enrich' AND ts >= ?",
                (since,),
            ).fetchone()[0]
        finally:
            conn.close()
    except sqlite3.Error as exc:
        log.error("jobs.db stats query failed: %s", exc)
    return stats


def record_reading(credits: dict, stats: dict) -> None:
    """INSERT the reading into better_enrich_credits (table auto-created)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS better_enrich_credits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                onetime_credit REAL,
                subscription_credit REAL,
                total_credit REAL,
                calls_1h INTEGER,
                ok_1h INTEGER,
                auth_err_1h INTEGER,
                rate_limited_1h INTEGER,
                emails_1h INTEGER
            )"""
        )
        conn.execute(
            """INSERT INTO better_enrich_credits
               (ts, onetime_credit, subscription_credit, total_credit,
                calls_1h, ok_1h, auth_err_1h, rate_limited_1h, emails_1h)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                ts,
                credits.get("onetime"),
                credits.get("subscription"),
                credits.get("total"),
                stats["calls"],
                stats["ok"],
                stats["auth_err"],
                stats["rate_limited"],
                stats["emails"],
            ),
        )
        conn.commit()
    finally:
        conn.close()


def previous_reading() -> Optional[dict]:
    """Most recent recorded reading at least 20 minutes old (for burn rate)."""
    cutoff = time.time() - 20 * 60
    cutoff_ts = datetime.fromtimestamp(cutoff, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            row = conn.execute(
                """SELECT ts, total_credit FROM better_enrich_credits
                   WHERE total_credit IS NOT NULL AND ts <= ?
                   ORDER BY id DESC LIMIT 1""",
                (cutoff_ts,),
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        log.error("previous reading query failed: %s", exc)
        return None
    if not row:
        return None
    try:
        prev_time = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return {"ts": prev_time, "total": float(row[1])}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    state = _load_state()
    credits, status = fetch_credits()
    stats = call_stats_last_hour()

    # --- Key health -----------------------------------------------------
    if credits is not None:
        if state.get("key_state") != "up":
            _log_alert(
                "INFO",
                f"key is UP (credits endpoint 200, total={credits.get('total')})",
            )
        state["key_state"] = "up"
    else:
        if state.get("key_state") != "down":
            notify(
                "key_down",
                "CRITICAL",
                f"BETTER_ENRICH KEY DOWN: credits endpoint returned "
                f"{status if status else 'transport error'} — all enrichment "
                f"via BetterEnrich is failing. Rotate the key in backend/.env "
                f"and restart the service.",
                state,
            )
        state["key_state"] = "down"

    if credits is None:
        record_reading({}, stats)
        _save_state(state)
        return 0

    record_reading(credits, stats)

    # --- Burn rate --------------------------------------------------------
    prev = previous_reading()
    if prev and credits.get("total") is not None:
        elapsed_h = (
            datetime.now(timezone.utc) - prev["ts"]
        ).total_seconds() / 3600
        if elapsed_h > 0:
            burn = (prev["total"] - credits["total"]) / elapsed_h
            if burn < 0:
                _log_alert(
                    "INFO",
                    f"credits added: +{-burn:.0f} credits "
                    f"(total now {credits['total']:.0f})",
                )
            elif burn > BURN_CPH:
                per_day = burn * 24 * CREDIT_PRICE
                notify(
                    "burn_high",
                    "WARNING",
                    f"BURN HIGH: {burn:.0f} credits/h "
                    f"(~${per_day:.0f}/day at ${CREDIT_PRICE}/cr) over last "
                    f"{elapsed_h:.1f}h; threshold {BURN_CPH:.0f} cr/h. "
                    f"1h stats: {stats['calls']} calls / {stats['emails']} emails.",
                    state,
                )
            else:
                log.info(
                    "burn %.1f cr/h, total=%.0f, 1h: %d calls %d emails",
                    burn,
                    credits["total"],
                    stats["calls"],
                    stats["emails"],
                )

    # --- Low balance ------------------------------------------------------
    total = credits.get("total")
    if total is not None and total < BALANCE_FLOOR:
        notify(
            "balance_low",
            "CRITICAL",
            f"BALANCE LOW: {total:.0f} credits remaining "
            f"(floor {BALANCE_FLOOR:.0f}). Top up or pause BetterEnrich to "
            f"avoid mid-job exhaustion (403s).",
            state,
        )

    # --- Error storm (key effectively dead even if credits endpoint OK) ---
    if stats["calls"] >= 50 and stats["auth_err"] * 100 > ERROR_PCT * stats["calls"]:
        notify(
            "error_storm",
            "CRITICAL",
            f"ERROR STORM: {stats['auth_err']}/{stats['calls']} BetterEnrich "
            f"calls in the last hour returned 401/402/403. Service is not "
            f"being delivered while this continues.",
            state,
        )

    _save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
