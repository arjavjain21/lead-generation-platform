#!/bin/bash
# Health check and auto-recovery for Lead Generation Platform

LOG_FILE="/var/www/lead-generation-platform/health-check.log"
API_URL="http://localhost:8765/api/health"
SERVICE_NAME="lead-generation-platform.service"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Check if service is running
if ! systemctl is-active --quiet "$SERVICE_NAME"; then
    log "❌ Service is not running. Attempting to restart..."
    sudo systemctl start "$SERVICE_NAME"
    sleep 5
    
    if systemctl is-active --quiet "$SERVICE_NAME"; then
        log "✅ Service restarted successfully"
    else
        log "❌ Failed to restart service!"
        exit 1
    fi
fi

# Check API health
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL" || echo "000")

if [ "$HTTP_CODE" = "200" ]; then
    log "✅ Health check passed"
    exit 0
else
    log "⚠️  API returned HTTP $HTTP_CODE, restarting service..."
    sudo systemctl restart "$SERVICE_NAME"
    sleep 5
    
    HTTP_CODE_AFTER=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL" || echo "000")
    if [ "$HTTP_CODE_AFTER" = "200" ]; then
        log "✅ Service recovered after restart"
    else
        log "❌ Service still unhealthy after restart"
        exit 1
    fi
fi
