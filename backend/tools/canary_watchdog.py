#!/usr/bin/env python3
"""Canary watchdog for the Email Finder terminal tier.

Runs every 5 minutes via systemd timer. Reads four metric sources, applies
the approved auto-disable thresholds with minimum sample counts, and on any
trip writes the persistent halt file checked by
providers.email_finder_rollout_allows() — instantly disabling the tier for
every worker, surviving restarts, cleared only by an operator.

Thresholds (approved 2026-11-21):
  1. Any contacts-DB row with source_name='email_finder' not verified  -> HALT (zero tolerance)
  2. email_finder call latency P95 > 60 s                             -> HALT (min 10 calls)
  3. MailTester proxy queue-wait P95 > 2 s (all clients, shared)      -> HALT (min 50 samples)
  4. email_finder provider-call error rate > 5%                       -> HALT (min 20 calls)

Window: last 30 minutes. Exit codes: 0 healthy, 3 halted (threshold),
4 halted (metric source unreadable — fail closed).
"""

import sqlite3
import subprocess
import sys
import time
from pathlib import Path

HALT_FILE = Path("/mnt/disk/email-finder/canary_halt")
HALT_LOG = Path("/mnt/disk/email-finder/canary_halt.log")
PROXY_DB = "/var/lib/mailtester-proxy/mailtester_proxy.db"
JOBS_DB = "/var/www/lead-generation-platform/backend/data/jobs.db"
WINDOW_MIN = 30

# Metric source failures fail CLOSED: a watchdog that can't read its
# metrics must not let the canary run unsupervised.
UNREADABLE_IS_HALT = True


def halt(reason: str, detail: str = "") -> int:
    if not HALT_FILE.exists():
        HALT_FILE.write_text(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {reason} {detail}\n")
    with open(HALT_LOG, "a") as f:
        f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} HALT reason={reason} {detail}\n")
    print(f"HALT: {reason} {detail}")
    return 3


def pct(values, p):
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(p / 100 * len(ordered) + 0.5) - 1))
    return ordered[idx]


def check_contacts_verified() -> tuple[bool, str]:
    """Threshold 1: any unverified email_finder row in Contacts DB."""
    sql = ("SELECT COUNT(*) FROM core.email "
           "WHERE source_name='email_finder' AND COALESCE(is_verified, false) IS NOT TRUE")
    try:
        out = subprocess.run(
            ["sudo", "-u", "postgres", "psql", "-p", "5432", "-d", "contacts", "-A", "-t", "-c", sql],
            capture_output=True, text=True, timeout=60,
        )
        if out.returncode != 0:
            return False, f"contacts query failed: {out.stderr.strip()[:120]}"
        n = int(out.stdout.strip() or "0")
        if n > 0:
            return False, f"{n} unverified email_finder rows in core.email"
        return True, "0 unverified rows"
    except Exception as e:
        return False, f"contacts query error: {e}"


FINDER_METRICS = "http://127.0.0.1:8910/metrics"
FINDER_HEALTH = "http://127.0.0.1:8910/health"
UNREACHABLE_STATE = Path("/mnt/disk/email-finder/canary_unreachable_count")


def check_finder_metrics() -> tuple[bool, str, dict]:
    """Threshold 2 from the finder service's own /metrics (real per-call
    durations on the exact path the platform uses) + service-reachability
    tracking (3 consecutive unreachable runs = halt: the tier is dead
    weight and masking miss traffic)."""
    import json as _json
    try:
        import urllib.request
        with urllib.request.urlopen(FINDER_HEALTH, timeout=10) as h:
            h.read()
        with urllib.request.urlopen(FINDER_METRICS, timeout=10) as r:
            m = _json.loads(r.read())
    except Exception as e:
        count = int((UNREACHABLE_STATE.read_text() or "0").strip() or 0) + 1 if UNREACHABLE_STATE.exists() else 1
        UNREACHABLE_STATE.write_text(str(count))
        if count >= 3:
            return False, f"finder unreachable {count} consecutive runs ({e})", {}
        return True, f"finder unreachable run {count}/3 (warning) ({e})", {}

    UNREACHABLE_STATE.write_text("0")
    n = m.get("recent_requests", 0)
    p95_s = m.get("p95_seconds", 0.0)
    msg = f"finder {n} req/30min, p95 {p95_s}s, errors {m.get('recent_errors', 0)}"
    if n >= 10 and p95_s > 60:
        return False, f"finder latency P95 {p95_s}s > 60s ({n} req)", m
    return True, msg, m


def check_platform_calls() -> tuple[bool, str]:
    """Threshold 4 from provider_call_log (platform-side error view)."""
    cutoff = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - WINDOW_MIN * 60))
    try:
        con = sqlite3.connect(f"file:{JOBS_DB}?mode=ro", uri=True, timeout=30)
        rows = con.execute(
            "SELECT status FROM provider_call_log "
            "WHERE provider='email_finder' AND ts >= ?",
            (cutoff,),
        ).fetchall()
        con.close()
    except Exception as e:
        return False, f"jobs.db read error: {e}"
    calls = len(rows)
    errors = sum(1 for (status,) in rows if (status or 0) >= 400)
    if calls >= 20 and errors / calls > 0.05:
        return False, f"error rate {100*errors/calls:.1f}% > 5% ({calls} calls)"
    return True, f"{calls} platform calls, {errors} errors"


def check_proxy_queue() -> tuple[bool, str]:
    """Threshold 3: proxy queue-wait P95 across ALL clients (shared budget)."""
    try:
        con = sqlite3.connect(f"file:{PROXY_DB}?mode=ro", uri=True, timeout=30)
        rows = con.execute(
            "SELECT queue_wait_ms FROM validation_logs "
            "WHERE created_at >= datetime('now', ?)",
            (f"-{WINDOW_MIN} minutes",),
        ).fetchall()
        con.close()
    except Exception as e:
        return False, f"proxy db read error: {e}"
    if len(rows) < 50:
        return True, f"proxy samples {len(rows)} < 50 min (skip)"
    p95 = pct([float(r[0] or 0) for r in rows], 95)
    if p95 > 2_000:
        return False, f"proxy queue-wait P95 {p95:.0f}ms > 2s ({len(rows)} samples)"
    return True, f"proxy queue p95 {p95:.0f}ms ({len(rows)} samples)"


def main() -> int:
    if HALT_FILE.exists():
        # Already halted: keep state, log the heartbeat, stay down.
        with open(HALT_LOG, "a") as f:
            f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} already-halted, staying down\n")
        print("already halted")
        return 3

    ok1, msg1 = check_contacts_verified()
    if not ok1:
        code = halt("unverified-writeback" if "rows" in msg1 else "metrics-unreadable", msg1)
        return code if "rows" in msg1 else 4
    okp, msgp = check_proxy_queue()
    okm, msgm, _ = check_finder_metrics()
    okc, msgc = check_platform_calls()

    print(f"[watchdog] contacts: {msg1} | proxy: {msgp} | finder: {msgm} | platform: {msgc}")

    if not okm:
        return halt("finder-unhealthy", msgm)
    if not okp:
        return halt("proxy-queue-latency", msgp)
    if not okc:
        return halt("email-finder-calls", msgc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
