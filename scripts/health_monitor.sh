#!/usr/bin/env bash
# Health monitor -> Slack (2026-10-16, Track A/A11).
#
# Checks every 5 min (lead-gen-health-monitor.timer):
#   root disk > 92% | MemAvailable < 10% | swap > 60%
#   PG 5432 / 5433 down | key services inactive
# Each failing check alerts to Slack at most once per hour (state files in
# /var/tmp/lgp_monitor_state); a check that recovers sends one RESOLVED.
# The jobs.db WAL has its own dedicated watchdog (wal_watchdog.sh).
set -u

ENV_FILE="/var/www/lead-generation-platform/backend/.env"
STATE_DIR="/var/tmp/lgp_monitor_state"
mkdir -p "$STATE_DIR"

slack() {  # slack "LEVEL" "msg"
    local token channel
    token=$(grep -oP '^SLACK_BOT_TOKEN=\K.*' "$ENV_FILE" 2>/dev/null)
    channel=$(grep -oP '^SLACK_CHANNEL=\K.*' "$ENV_FILE" 2>/dev/null)
    [ -n "$token" ] && [ -n "$channel" ] || return 0
    curl -s -m 10 -X POST -H "Authorization: Bearer ${token}" \
        -d "channel=${channel}" \
        --data-urlencode "text=*[hyperke-vps health] ${1}* ${2}" \
        "https://slack.com/api/chat.postMessage" >/dev/null 2>&1 || true
}

# check <name> <is_bad_now:0/1> <level> <msg>
declare -A LAST
check() {
    local name="$1" bad="$2" level="$3" msg="$4" now epoch
    now=$(date +%s)
    if [ "$bad" = "1" ]; then
        if [ -z "${LAST[$name]:-}" ] || [ $((now - ${LAST[$name]:-0})) -gt 3600 ]; then
            slack "$level" "$msg"
            echo "$now" > "$STATE_DIR/$name"
        fi
    else
        if [ -f "$STATE_DIR/$name" ]; then
            rm -f "$STATE_DIR/$name"
            slack "RESOLVED" "$name back to normal. $msg"
        fi
    fi
}

# --- disk (root AND /mnt/disk — 2026-10-18: backup dumps pushed /mnt/disk
#     to 90% and the I/O spike caused false pg_5433_down alerts) ---
rpct=$(df -P / | awk 'NR==2{gsub("%","",$5); print $5}')
[ "$rpct" -gt 92 ] && check disk "1" "WARNING" "root ${rpct}% used." \
                  || check disk "0" "OK" "root ${rpct}% used."
mpct=$(df -P /mnt/disk | awk 'NR==2{gsub("%","",$5); print $5}')
[ "$mpct" -gt 90 ] && check mntdisk "1" "WARNING" "/mnt/disk ${mpct}% used (backups/PG-5433 disk)." \
                  || check mntdisk "0" "OK" "/mnt/disk ${mpct}% used."

# --- memory ---
memtotal=$(awk '/MemTotal/{print $2}' /proc/meminfo)
memavail=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
memavail_pct=$(( memavail * 100 / memtotal ))
[ "$memavail_pct" -lt 10 ] && check mem "1" "WARNING" "MemAvailable ${memavail_pct}% — pressure/swap-thrash risk." \
                        || check mem "0" "OK" "MemAvailable ${memavail_pct}%."

swaptotal=$(awk '/SwapTotal/{print $2}' /proc/meminfo)
swapfree=$(awk '/SwapFree/{print $2}' /proc/meminfo)
if [ "$swaptotal" -gt 0 ]; then
    swapused_pct=$(( (swaptotal - swapfree) * 100 / swaptotal ))
    [ "$swapused_pct" -gt 60 ] && check swap "1" "WARNING" "swap ${swapused_pct}% in use." \
                          || check swap "0" "OK" "swap ${swapused_pct}% used."
fi

# --- postgres (10s timeout each: under I/O pressure a hang-free probe
#     matters more than a fast one) ---
pg_isready -q -t 10 -p 5432 && check pg5432 "0" "OK" "PG 5432 up." \
                           || check pg5432 "1" "CRITICAL" "PG 5432 (contacts DB) NOT accepting connections."
pg_isready -q -t 10 -p 5433 && check pg5433 "0" "OK" "PG 5433 up." \
                           || check pg5433 "1" "WARNING" "PG 5433 (lead_gen mirror) NOT accepting connections (10s timeout)."

# --- key services ---
for svc in lead-generation-platform.service lead-gen-scraper-runner.service contacts-api.service nginx.service; do
    short=$(echo "$svc" | tr '.-' '__')
    if systemctl is-active --quiet "$svc"; then
        check "svc_$short" "0" "OK" "$svc active."
    else
        check "svc_$short" "1" "CRITICAL" "$svc is NOT active."
    fi
done

exit 0
