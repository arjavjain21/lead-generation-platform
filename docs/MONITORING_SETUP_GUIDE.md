# Monitoring System Setup Guide

## Overview

A comprehensive monitoring system has been deployed for the Lead Generation Platform that automatically checks:
- Disk space usage (CRITICAL at 95%, WARNING at 90%)
- Failed jobs in the last 24 hours
- Stale running jobs (>6 hours)
- API errors in service logs
- Database health
- Service health

## What Was Deployed

### 1. Monitor Script
**File:** `/var/www/lead-generation-platform/monitor.sh`

A comprehensive bash script that checks all system health metrics and logs warnings/critical issues.

### 2. Systemd Timer
**Files:**
- `/etc/systemd/system/lead-generation-platform-monitor.service` - Service definition
- `/etc/systemd/system/lead-generation-platform-monitor.timer` - Timer definition

**Schedule:** Runs every 15 minutes
**Logs:**
- `/var/www/lead-generation-platform/monitoring.log` - All checks
- `/var/www/lead-generation-platform/alerts.log` - Warnings and critical alerts only

### 3. Log Output
```
[INFO] [2026-03-10 05:13:46] Checking service health...
[INFO] [2026-03-10 05:13:46] Service health OK
[CRITICAL] [2026-03-10 05:13:46] CRITICAL: Disk space at 95%
[WARNING] [2026-03-10 05:13:46] WARNING: 3 failed jobs in last 24 hours
```

## Current Status

### Critical Issue Detected: Disk Space at 95%

The monitoring script immediately detected a **CRITICAL disk space issue**:

```
CRITICAL: Disk space at 95% (threshold: 95%)
Available: ~10.5 GB
```

**IMMEDIATE ACTION REQUIRED:**

### Disk Cleanup Commands

Run these commands to free up disk space:

```bash
# 1. Check what's using space
du -sh /var/www/lead-generation-platform/backend/data/*
du -sh /var/www/lead-generation-platform/backend/data/uploads/*
du -sh /var/www/lead-generation-platform/backend/data/outputs/*

# 2. Remove old uploads (>30 days)
find /var/www/lead-generation-platform/backend/data/uploads/ -name "*.csv" -mtime +30 -delete

# 3. Remove old outputs (>30 days)
find /var/www/lead-generation-platform/backend/data/outputs/ -name "*.csv" -mtime +30 -delete

# 4. Check large log files
find /var/www/lead-generation-platform -name "*.log" -size +100M -ls

# 5. Run WAL checkpoint on database
sqlite3 /var/www/lead-generation-platform/backend/data/jobs.db "PRAGMA wal_checkpoint(TRUNCATE);"

# 6. Verify disk space after cleanup
df -h /
```

### Estimated Space Recovery

Based on current files:
- **Uploads**: Likely 1-2 GB of old CSV files
- **Outputs**: Likely 2-4 GB of old job outputs
- **WAL checkpoint**: Will free ~3 MB of WAL file
- **Total expected recovery**: 3-6 GB (should bring usage down to ~90-92%)

## Monitoring Thresholds

### Disk Space
- **WARNING**: 90% disk usage
- **CRITICAL**: 95% disk usage

### Failed Jobs
- **WARNING**: More than 5 failed jobs in last 24 hours
- Shows recent failures with error messages

### Stale Jobs
- **WARNING**: Jobs marked "running" for over 6 hours
- Indicates crashed/abandoned jobs

### API Errors
- **WARNING**: More than 50 API errors in 15 minutes
- Tracks 422, 500 errors, and Blitz API failures

## Monitoring Commands

### Manual Monitoring Check

```bash
# Run full monitoring check
/var/www/lead-generation-platform/monitor.sh

# Run in quiet mode (only show warnings/critical)
/var/www/lead-generation-platform/monitor.sh --quiet

# Check only disk space
/var/www/lead-generation-platform/monitor.sh --disk-only

# Check only jobs
/var/www/lead-generation-platform/monitor.sh --jobs-only
```

### View Monitoring Logs

```bash
# View all monitoring activity
tail -f /var/www/lead-generation-platform/monitoring.log

# View only warnings and critical alerts
tail -f /var/www/lead-generation-platform/alerts.log

# View recent alerts
tail -50 /var/www/lead-generation-platform/alerts.log
```

### Systemd Timer Management

```bash
# Check timer status
systemctl status lead-generation-platform-monitor.timer

# View timer logs
journalctl -u lead-generation-platform-monitor.service -f

# Manually trigger monitoring run
systemctl start lead-generation-platform-monitor.service

# Disable monitoring
sudo systemctl disable lead-generation-platform-monitor.timer
sudo systemctl stop lead-generation-platform-monitor.timer

# Enable monitoring
sudo systemctl enable lead-generation-platform-monitor.timer
sudo systemctl start lead-generation-platform-monitor.timer
```

## Setting Up Email Alerts

### Option 1: Simple Mail Forwarding

1. **Install mailutils:**
   ```bash
   sudo apt-get install mailutils
   ```

2. **Configure email forwarding in crontab:**
   ```bash
   crontab -e
   ```

3. **Add this line:**
   ```
   */15 * * * * /var/www/lead-generation-platform/monitor.sh --quiet 2>&1 | mail -s "Lead Gen Platform Alert" admin@example.com
   ```

### Option 2: Using AWS SNS (Recommended for Cloud)

```bash
# Install AWS CLI
sudo apt-get install awscli

# Configure AWS credentials
aws configure

# Publish alerts to SNS topic
*/15 * * * * /var/www/lead-generation-platform/monitor.sh --quiet 2>&1 | while read line; do aws sns publish --topic-arn arn:aws:sns:region:account:topicname --message "$line" --subject "Lead Gen Alert"; done
```

### Option 3: Slack Integration

```bash
# Install slack-cli
npm install -g slack-cli

# Configure webhook
slack configure

# Send alerts to Slack
*/15 * * * * /var/www/lead-generation-platform/monitor.sh --quiet 2>&1 | while read line; do slack send "$line"; done
```

## Customizing Thresholds

Edit `/var/www/lead-generation-platform/monitor.sh` to adjust thresholds:

```bash
# Lines 15-19
DISK_WARNING_THRESHOLD=90        # Warn at 90% disk usage
DISK_CRITICAL_THRESHOLD=95       # Critical at 95% disk usage
FAILED_JOBS_THRESHOLD=5          # Alert if more than 5 failed jobs
API_ERROR_THRESHOLD=50           # Alert if more than 50 API errors in 15min
WAL_SIZE_THRESHOLD=10485760      # 10 MB - checkpoint if WAL larger
```

## Monitoring Dashboard

To create a simple monitoring dashboard, use this script:

```bash
#!/bin/bash
# Simple monitoring dashboard - run with: watch -n 30 ./dashboard.sh

clear
echo "=========================================="
echo " Lead Generation Platform - Status"
echo "=========================================="
date
echo ""

# Service status
echo "Service Status:"
systemctl is-active lead-generation-platform.service && echo "  ✅ Running" || echo "  ❌ Stopped"
echo ""

# Disk space
echo "Disk Space:"
df -h / | awk 'NR==2 {printf "  Used: %s/%s (%s)\n  Available: %s\n", $3, $2, $5, $4}'
echo ""

# Recent jobs
echo "Recent Jobs:"
sqlite3 /var/www/lead-generation-platform/backend/data/jobs.db "
SELECT status, COUNT(*) as count
FROM jobs
WHERE created_at >= datetime('now', '-1 hour')
GROUP BY status;
" | while read status count; do
  if [ "$status" = "running" ]; then
    echo "  🔄 Running: $count"
  elif [ "$status" = "done" ]; then
    echo "  ✅ Done: $count"
  elif [ "$status" = "failed" ]; then
    echo "  ❌ Failed: $count"
  fi
done
echo ""

# API errors (last 15 min)
echo "API Errors (15 min):"
errors=$(journalctl -u lead-generation-platform.service --since "15 minutes ago" --no-pager | grep -c -E "(422|500)" || echo "0")
echo "  Total: $errors"
echo ""

echo "=========================================="
echo "Last monitoring check:"
tail -1 /var/www/lead-generation-platform/monitoring.log | grep -o '\[.*\]'
echo "=========================================="
```

## Automated Cleanup (Recommended)

Add this to crontab to automatically clean up old files:

```bash
# Run cleanup daily at 3 AM
0 3 * * * find /var/www/lead-generation-platform/backend/data/uploads/ -name "*.csv" -mtime +30 -delete
0 3 * * * find /var/www/lead-generation-platform/backend/data/outputs/ -name "*.csv" -mtime +30 -delete
0 3 * * * sqlite3 /var/www/lead-generation-platform/backend/data/jobs.db "PRAGMA wal_checkpoint(TRUNCATE);"
```

## Troubleshooting

### Monitoring Script Not Running

```bash
# Check if timer is active
systemctl status lead-generation-platform-monitor.timer

# Check last run time
systemctl list-timers | grep monitor

# View service logs
journalctl -u lead-generation-platform-monitor.service -n 50
```

### False Alerts

If you're getting false alerts:

1. **Check disk calculation:**
   ```bash
   df -h /
   ```

2. **Verify job status:**
   ```bash
   sqlite3 backend/data/jobs.db "SELECT status, COUNT(*) FROM jobs GROUP BY status;"
   ```

3. **Check journalctl:**
   ```bash
   journalctl -u lead-generation-platform.service --since "15 minutes ago" | grep -E "(422|500)"
   ```

### High CPU Usage from Monitoring

If monitoring script uses too much CPU:

1. **Reduce frequency:** Edit timer to run every 30 minutes instead of 15
2. **Reduce journalctl scope:** Check only last 5 minutes instead of 15
3. **Optimize database queries:** Add indexes to jobs table

## Next Steps

### Immediate (Today)
- [x] Deploy monitoring script
- [x] Set up systemd timer
- [ ] **Clean up disk space** (CRITICAL - at 95%)
- [ ] Set up email alerts

### Short Term (This Week)
- [ ] Monitor alerts for 48 hours
- [ ] Tune thresholds based on actual usage
- [ ] Set up automated daily cleanup
- [ ] Create monitoring dashboard

### Long Term (This Month)
- [ ] Integrate with Grafana/Prometheus for better visualization
- [ ] Add SMS/Slack alerts for critical issues
- [ ] Implement predictive monitoring (disk space trends)
- [ ] Add performance metrics (API response times, job duration)

## Files Created/Modified

- ✅ `/var/www/lead-generation-platform/monitor.sh` (NEW)
- ✅ `/etc/systemd/system/lead-generation-platform-monitor.service` (NEW)
- ✅ `/etc/systemd/system/lead-generation-platform-monitor.timer` (NEW)
- ✅ `/var/www/lead-generation-platform/monitoring.log` (AUTO-CREATED)
- ✅ `/var/www/lead-generation-platform/alerts.log` (AUTO-CREATED)

---

**Date:** March 10, 2026
**Status:** ✅ Monitoring system deployed and active
**Critical Issue:** 🚨 Disk space at 95% - immediate cleanup required
**Timer:** Running every 15 minutes
**Logs:** /var/www/lead-generation-platform/monitoring.log
