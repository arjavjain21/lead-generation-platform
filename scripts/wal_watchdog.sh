#!/usr/bin/env bash
# WAL watchdog for jobs.db — prevents the 2026-10-16 outage class.
#
# Failure mode this contains: a long-lived SQLite read snapshot (thread-pool
# reader / SSE-adjacent path) blocks WAL checkpoints. With heavy job writes
# the WAL then grows unbounded (observed 26 GB -> root 100% -> box wedge ->
# hard reboot). journal_size_limit and wal_autocheckpoint CANNOT bound the
# WAL while a checkpoint is pinned, so this watchdog detects and remediates:
#
#   WAL > 1 GB  -> attempt wal_checkpoint(TRUNCATE) (no-op if busy)
#   WAL > 3 GB or root > 92% -> checkpoint + Slack WARNING
#   WAL > 8 GB or root < 5% free -> checkpoint; if STILL large, stop both
#       app services (jobs auto-resume on boot), fold the WAL, restart the
#       services, Slack CRITICAL. This is the emergency recipe from the
#       2026-10-15/16 incidents, automated.
#
# Runs every 5 minutes via lead-gen-wal-watchdog.timer. Safe to run manually.
set -u

DB="/var/www/lead-generation-platform/backend/data/jobs.db"
WAL="${DB}-wal"
ENV_FILE="/var/www/lead-generation-platform/backend/.env"
WARN_GB=3
CRIT_GB=8
ROOT_PCT_WARN=92
ROOT_FREE_CRIT=5
STATE_FILE="/var/tmp/wal_watchdog_state"

wal_bytes() { [ -f "$WAL" ] && stat -c%s "$WAL" 2>/dev/null || echo 0; }
gb() { awk -v b="$1" 'BEGIN{printf "%.1f", b/1024/1024/1024}'; }
root_pct() { df -P / | awk 'NR==2{gsub("%","",$5); print $5}'; }
root_free_pct() { df -P / | awk 'NR==2{gsub("%","",$5); print 100-$5}'; }

slack() {  # slack "LEVEL" "message"
    local token channel
    token=$(grep -oP '^SLACK_BOT_TOKEN=\K.*' "$ENV_FILE" 2>/dev/null)
    channel=$(grep -oP '^SLACK_CHANNEL=\K.*' "$ENV_FILE" 2>/dev/null)
    [ -n "$token" ] && [ -n "$channel" ] || return 0
    curl -s -m 10 -X POST -H "Authorization: Bearer ${token}" \
        -d "channel=${channel}" \
        --data-urlencode "text=*[hyperke-vps WAL watchdog] $1* $2" \
        "https://slack.com/api/chat.postMessage" >/dev/null 2>&1 || true
}

attempt_checkpoint() {
    sqlite3 "$DB" "PRAGMA busy_timeout=30000; PRAGMA wal_checkpoint(TRUNCATE);" 2>/dev/null | head -1
}

# --- cooldown: never auto-restart more often than every 30 min ---
last_crit=0
[ -f "$STATE_FILE" ] && last_crit=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
now=$(date +%s)

before=$(wal_bytes); before_gb=$(gb "$before"); rpct=$(root_pct); rfree=$(root_free_pct)
if [ "$before" -lt $((1024*1024*1024)) ] && [ "$rpct" -lt "$ROOT_PCT_WARN" ]; then
    exit 0   # healthy — no logs, no noise
fi

ck=$(attempt_checkpoint)
after=$(wal_bytes); after_gb=$(gb "$after")

if [ "$after" -lt $((1024*1024*1024)) ] && [ "$rpct" -lt "$ROOT_PCT_WARN" ]; then
    [ "$before" -ge $((3*1024*1024*1024)) ] && slack "RESOLVED" \
        "jobs.db WAL folded ${before_gb}GB -> ${after_gb}GB (checkpoint: ${ck:-n/a}). Root ${rpct}% used."
    exit 0
fi

# Still large after checkpoint attempt (pinned) or disk pressure high.
if [ "$after" -gt $((CRIT_GB*1024*1024*1024)) ] || [ "$rfree" -lt "$ROOT_FREE_CRIT" ]; then
    if [ $((now - last_crit)) -gt 1800 ]; then
        echo "$now" > "$STATE_FILE"
        slack "CRITICAL — auto-remediating" \
"jobs.db WAL pinned at ${after_gb}GB (checkpoint busy), root ${rpct}% used / ${rfree}% free.
Stopping app services to release the reader pin, folding WAL, restarting. Jobs auto-resume from checkpoints."
        systemctl stop lead-generation-platform.service lead-gen-scraper-runner.service
        sleep 3
        attempt_checkpoint
        systemctl start lead-generation-platform.service lead-gen-scraper-runner.service
        final=$(wal_bytes); final_gb=$(gb "$final")
        slack "CRITICAL — remediation done" \
            "WAL ${after_gb}GB -> ${final_gb}GB after fold+restart. Services restarting; jobs auto-resume. Root now $(root_pct)% used."
    else
        slack "CRITICAL" \
            "WAL still ${after_gb}GB, root ${rpct}% used (cooldown active — manual recipe: stop both lead-gen services, fold WAL, start, resume)."
    fi
    exit 1
fi

slack "WARNING" \
    "jobs.db WAL ${after_gb}GB after checkpoint attempt (busy/pinned), root ${rpct}% used. Will keep retrying every 5 min; auto-remediation at ${CRIT_GB}GB or <${ROOT_FREE_CRIT}% free."
exit 0
