#!/bin/bash

###############################################################################
# Lead Generation Platform - Comprehensive Monitoring Script
#
# Monitors:
# - Disk space usage
# - Failed jobs in database
# - API errors in logs
# - Service health
# - Database WAL checkpoint needed
#
# Usage: ./monitor.sh [options]
#   --quiet     Only output if issues found
#   --disk-only Only check disk space
#   --jobs-only Only check job status
#
# Cron setup:
#   */15 * * * * /var/www/lead-generation-platform/monitor.sh --quiet | mail -s "Monitor Alert" admin@example.com
###############################################################################

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
LOG_FILE="$SCRIPT_DIR/monitoring.log"
ALERT_LOG="$SCRIPT_DIR/alerts.log"

# Thresholds
DISK_WARNING_THRESHOLD=90        # Warn at 90% disk usage
DISK_CRITICAL_THRESHOLD=95       # Critical at 95% disk usage
FAILED_JOBS_THRESHOLD=5          # Alert if more than 5 failed jobs
API_ERROR_THRESHOLD=50           # Alert if more than 50 API errors in 15min
WAL_SIZE_THRESHOLD=10485760      # 10 MB - checkpoint if WAL larger

# Colors for terminal output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

QUIET_MODE=false
CHECK_DISK_ONLY=false
CHECK_JOBS_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --quiet)
      QUIET_MODE=true
      shift
      ;;
    --disk-only)
      CHECK_DISK_ONLY=true
      shift
      ;;
    --jobs-only)
      CHECK_JOBS_ONLY=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Logging functions
log_info() {
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[INFO] [$timestamp] $1" >> "$LOG_FILE"
  if [ "$QUIET_MODE" = false ]; then
    echo -e "${GREEN}[INFO]${NC} $1"
  fi
}

log_warning() {
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[WARNING] [$timestamp] $1" >> "$ALERT_LOG"
  if [ "$QUIET_MODE" = false ]; then
    echo -e "${YELLOW}[WARNING]${NC} $1"
  fi
  # Always print warnings in quiet mode (that's the point)
  if [ "$QUIET_MODE" = true ]; then
    echo "[WARNING] $1"
  fi
}

log_critical() {
  local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[CRITICAL] [$timestamp] $1" >> "$ALERT_LOG"
  if [ "$QUIET_MODE" = false ]; then
    echo -e "${RED}[CRITICAL]${NC} $1"
  fi
  # Always print critical in quiet mode
  if [ "$QUIET_MODE" = true ]; then
    echo "[CRITICAL] $1"
  fi
}

###############################################################################
# Disk Space Monitoring
###############################################################################

check_disk_space() {
  log_info "Checking disk space..."

  # Get disk usage for root partition
  local disk_usage=$(df / | awk 'NR==2 {print $5}' | sed 's/%//')
  local available_gb=$(df / | awk 'NR==2 {print $4}')
  local total_gb=$(df / | awk 'NR==2 {print $2}')
  local used_gb=$(df / | awk 'NR==2 {print $3}')

  log_info "Disk usage: ${disk_usage}% (${used_gb}GB used of ${total_gb}GB total, ${available_gb}GB available)"

  if [ "$disk_usage" -ge "$DISK_CRITICAL_THRESHOLD" ]; then
    log_critical "CRITICAL: Disk space at ${disk_usage}% (threshold: ${DISK_CRITICAL_THRESHOLD}%)"
    log_critical "Available: ${available_gb}GB"
    log_critical "Action required immediately!"

    # Suggest cleanup commands
    log_critical "Cleanup commands:"
    log_critical "  find $BACKEND_DIR/data/uploads/ -name '*.csv' -mtime +30 -delete"
    log_critical "  find $BACKEND_DIR/data/outputs/ -name '*.csv' -mtime +30 -delete"
    log_critical "  sqlite3 $BACKEND_DIR/data/jobs.db 'PRAGMA wal_checkpoint(TRUNCATE);'"

    return 2
  elif [ "$disk_usage" -ge "$DISK_WARNING_THRESHOLD" ]; then
    log_warning "WARNING: Disk space at ${disk_usage}% (threshold: ${DISK_WARNING_THRESHOLD}%)"
    log_warning "Available: ${available_gb}GB"
    log_warning "Consider cleaning up old files"

    return 1
  fi

  log_info "Disk space OK"
  return 0
}

###############################################################################
# Database Monitoring
###############################################################################

check_database() {
  log_info "Checking database..."

  local db_path="$BACKEND_DIR/data/jobs.db"
  local wal_path="$db_path-wal"

  if [ ! -f "$db_path" ]; then
    log_warning "Database file not found: $db_path"
    return 1
  fi

  # Check database size
  local db_size=$(stat -f%z "$db_path" 2>/dev/null || stat -c%s "$db_path" 2>/dev/null)
  local db_size_mb=$((db_size / 1024 / 1024))
  log_info "Database size: ${db_size_mb}MB"

  # Check WAL size
  if [ -f "$wal_path" ]; then
    local wal_size=$(stat -f%z "$wal_path" 2>/dev/null || stat -c%s "$wal_path" 2>/dev/null)
    local wal_size_mb=$((wal_size / 1024 / 1024))
    log_info "WAL size: ${wal_size_mb}MB"

    if [ "$wal_size" -gt "$WAL_SIZE_THRESHOLD" ]; then
      log_warning "WAL file is large (${wal_size_mb}MB). Consider checkpointing:"
      log_warning "  sqlite3 $db_path 'PRAGMA wal_checkpoint(TRUNCATE);'"
    fi
  fi

  return 0
}

###############################################################################
# Job Monitoring
###############################################################################

check_failed_jobs() {
  log_info "Checking for failed jobs..."

  local failed_count=$(sqlite3 "$BACKEND_DIR/data/jobs.db" "
    SELECT COUNT(*) FROM jobs
    WHERE status = 'failed'
    AND created_at >= datetime('now', '-24 hours');
  " 2>/dev/null || echo "0")

  log_info "Failed jobs in last 24 hours: $failed_count"

  if [ "$failed_count" -gt "$FAILED_JOBS_THRESHOLD" ]; then
    log_warning "WARNING: $failed_count failed jobs in last 24 hours (threshold: $FAILED_JOBS_THRESHOLD)"

    # Show recent failures
    local recent_failures=$(sqlite3 "$BACKEND_DIR/data/jobs.db" "
      SELECT job_id, job_type, error, created_at
      FROM jobs
      WHERE status = 'failed'
      AND created_at >= datetime('now', '-24 hours')
      ORDER BY created_at DESC
      LIMIT 5;
    " 2>/dev/null)

    if [ -n "$recent_failures" ]; then
      log_warning "Recent failed jobs:"
      echo "$recent_failures" | while IFS='|' read -r job_id job_type error created_at; do
        log_warning "  [$job_type] $job_id: $error ($created_at)"
      done
    fi

    return 1
  fi

  log_info "Failed jobs check OK"
  return 0
}

check_stale_jobs() {
  log_info "Checking for stale running jobs..."

  local stale_count=$(sqlite3 "$BACKEND_DIR/data/jobs.db" "
    SELECT COUNT(*) FROM jobs
    WHERE status = 'running'
    AND created_at < datetime('now', '-6 hours');
  " 2>/dev/null || echo "0")

  log_info "Stale running jobs: $stale_count"

  if [ "$stale_count" -gt 0 ]; then
    log_warning "WARNING: $stale_count jobs marked as 'running' for over 6 hours"

    local stale_jobs=$(sqlite3 "$BACKEND_DIR/data/jobs.db" "
      SELECT job_id, job_type, created_at
      FROM jobs
      WHERE status = 'running'
      AND created_at < datetime('now', '-6 hours')
      ORDER BY created_at;
    " 2>/dev/null)

    if [ -n "$stale_jobs" ]; then
      log_warning "Stale jobs:"
      echo "$stale_jobs" | while IFS='|' read -r job_id job_type created_at; do
        log_warning "  [$job_type] $job_id (started: $created_at)"
      done
    fi

    return 1
  fi

  log_info "Stale jobs check OK"
  return 0
}

###############################################################################
# API Error Monitoring
###############################################################################

check_api_errors() {
  log_info "Checking for API errors in logs..."

  # Count 422 errors (Contacts DB validation errors)
  local error_422=$(journalctl -u lead-generation-platform.service --since "15 minutes ago" --no-pager 2>/dev/null | grep -c "422" || true)
  if [ -z "$error_422" ]; then error_422=0; fi

  # Count 500 errors (server errors)
  local error_500=$(journalctl -u lead-generation-platform.service --since "15 minutes ago" --no-pager 2>/dev/null | grep -c "500" || true)
  if [ -z "$error_500" ]; then error_500=0; fi

  # Count Blitz API errors
  local blitz_errors=$(journalctl -u lead-generation-platform.service --since "15 minutes ago" --no-pager 2>/dev/null | grep -i "blitz" | grep -i -c "error\|fail" || true)
  if [ -z "$blitz_errors" ]; then blitz_errors=0; fi

  log_info "API errors in last 15 minutes:"
  log_info "  422 errors: $error_422"
  log_info "  500 errors: $error_500"
  log_info "  Blitz errors: $blitz_errors"

  local total_errors=$((error_422 + error_500 + blitz_errors))

  if [ "$total_errors" -gt "$API_ERROR_THRESHOLD" ]; then
    log_warning "WARNING: $total_errors API errors in last 15 minutes (threshold: $API_ERROR_THRESHOLD)"

    # Show recent errors
    log_warning "Recent errors:"
    journalctl -u lead-generation-platform.service --since "15 minutes ago" --no-pager | grep -E "(422|500|Blitz.*Error)" | tail -20 | while read -r line; do
      log_warning "  $line"
    done

    return 1
  fi

  log_info "API error check OK"
  return 0
}

###############################################################################
# Service Health Monitoring
###############################################################################

check_service_health() {
  log_info "Checking service health..."

  # Check if service is running
  if ! systemctl is-active --quiet lead-generation-platform.service; then
    log_critical "CRITICAL: Service is not running!"
    log_critical "Attempt restart: sudo systemctl restart lead-generation-platform.service"
    return 2
  fi

  # Check health endpoint
  local health_status=$(curl -s http://localhost:8765/api/health | jq -r '.status' 2>/dev/null || echo "error")

  if [ "$health_status" != "ok" ]; then
    log_critical "CRITICAL: Health check returned: $health_status"
    return 2
  fi

  # Check service is listening on port 8765
  if ! ss -tlnp | grep -q ":8765"; then
    log_critical "CRITICAL: Service not listening on port 8765"
    return 2
  fi

  log_info "Service health OK"
  return 0
}

###############################################################################
# Main Monitoring Loop
###############################################################################

main() {
  local exit_code=0

  # Create log files if they don't exist
  touch "$LOG_FILE" "$ALERT_LOG"

  log_info "=========================================="
  log_info "Starting monitoring check"
  log_info "=========================================="

  # Always check service health first
  if [ "$CHECK_JOBS_ONLY" = false ]; then
    check_service_health || exit_code=$?
  fi

  # Check disk space
  if [ "$CHECK_JOBS_ONLY" = false ]; then
    check_disk_space || exit_code=$?
  fi

  # Check database
  if [ "$CHECK_JOBS_ONLY" = false ]; then
    check_database || exit_code=$?
  fi

  # Check failed jobs
  if [ "$CHECK_DISK_ONLY" = false ]; then
    check_failed_jobs || exit_code=$?
  fi

  # Check stale jobs
  if [ "$CHECK_DISK_ONLY" = false ]; then
    check_stale_jobs || exit_code=$?
  fi

  # Check API errors
  if [ "$CHECK_DISK_ONLY" = false ]; then
    check_api_errors || exit_code=$?
  fi

  log_info "=========================================="
  log_info "Monitoring check complete"
  log_info "=========================================="

  if [ "$exit_code" -eq 0 ]; then
    log_info "All checks passed"
  elif [ "$exit_code" -eq 1 ]; then
    log_warning "Some warnings detected"
  else
    log_critical "Critical issues detected"
  fi

  exit $exit_code
}

# Run main function
main "$@"
