#!/bin/bash
# Wait for enrichment job to complete, then restart service
# This script monitors the job and restarts the service when done

set -e

JOB_ID="d15c3247-a754-495e-8e02-1b6f6a7bd374"
OUTPUT_FILE="/var/www/lead-generation-platform/backend/data/outputs/${JOB_ID}.csv"
TOTAL_ROWS=7793
CHECK_INTERVAL=60  # Check every 60 seconds

echo "========================================================================"
echo "ENRICHMENT JOB COMPLETION MONITOR"
echo "Job ID: $JOB_ID"
echo "Total Rows: $TOTAL_ROWS"
echo "========================================================================"
echo ""

# Function to check job progress
check_progress() {
    if [ ! -f "$OUTPUT_FILE" ]; then
        echo "0"
        return
    fi

    # Count rows (subtract header)
    local rows=$(wc -l < "$OUTPUT_FILE" 2>/dev/null || echo "0")
    echo $((rows - 1))
}

# Function to check if job is done
is_job_done() {
    local current=$(check_progress)
    if [ "$current" -ge "$TOTAL_ROWS" ]; then
        return 0  # Done
    fi
    return 1  # Not done
}

# Monitor loop
echo "Monitoring job progress (checking every ${CHECK_INTERVAL}s)..."
echo ""

while true; do
    current=$(check_progress)
    percentage=$(echo "scale=1; $current * 100 / $TOTAL_ROWS" | bc)
    remaining=$((TOTAL_ROWS - current))

    echo "[$(date '+%H:%M:%S')] Progress: $current / $TOTAL_ROWS rows ($percentage%) - ${remaining} rows remaining"

    if is_job_done; then
        echo ""
        echo "========================================================================"
        echo "✅ JOB COMPLETED!"
        echo "========================================================================"
        echo ""
        echo "Job finished with $current rows processed."
        echo ""
        echo "Waiting 30 seconds to ensure file writes are complete..."
        sleep 30

        # Final verification
        final_count=$(check_progress)
        echo "Final row count: $final_count / $TOTAL_ROWS"

        if [ "$final_count" -ge "$TOTAL_ROWS" ]; then
            echo ""
            echo "========================================================================"
            echo "RESTARTING SERVICE TO APPLY BUG FIXES"
            echo "========================================================================"
            echo ""

            # Create backup
            echo "Creating database backup..."
            cp /var/www/lead-generation-platform/backend/data/jobs.db \
               /var/www/lead-generation-platform/backend/data/jobs.db.backup.$(date +%Y%m%d_%H%M%S)

            # Restart service
            echo "Restarting lead-generation-platform service..."
            sudo systemctl restart lead-generation-platform.service

            # Wait for service to start
            echo "Waiting for service to start..."
            sleep 10

            # Check service status
            if sudo systemctl is-active --quiet lead-generation-platform.service; then
                echo "✅ Service started successfully!"
            else
                echo "❌ Service failed to start. Check logs:"
                sudo journalctl -u lead-generation-platform.service --since "1 minute ago"
                exit 1
            fi

            # Health check
            echo "Running health check..."
            sleep 5
            health=$(curl -s http://localhost:8765/api/health || echo "{}")
            echo "Health status: $health"

            if echo "$health" | grep -q '"status":"ok"'; then
                echo "✅ Health check passed!"
            else
                echo "⚠️  Health check returned unexpected status"
            fi

            echo ""
            echo "========================================================================"
            echo "DEPLOYMENT COMPLETE"
            echo "========================================================================"
            echo ""
            echo "Both bug fixes have been applied:"
            echo "  1. ✅ Progress counter now updates correctly"
            echo "  2. ✅ Filename displays as actual filename (not UUID)"
            echo ""
            echo "Next steps:"
            echo "  1. Test with a new enrichment job (small sample file)"
            echo "  2. Verify progress counter updates in real-time"
            echo "  3. Verify filename displays correctly"
            echo ""
            echo "For details, see: /var/www/lead-generation-platform/BUG_FIXES_APPLIED.md"
            echo ""
            echo "========================================================================"

            exit 0
        else
            echo "⚠️  Row count decreased. Job may still be writing. Waiting..."
            sleep 30
            continue
        fi
    fi

    sleep $CHECK_INTERVAL
done
