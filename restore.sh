#!/bin/bash
# Restore script for Lead Generation Platform

if [ -z "$1" ]; then
    echo "Usage: $0 <backup_file.tar.gz>"
    echo ""
    echo "Available backups:"
    ls -lh /var/www/backups/lead-generation-platform/daily/*.tar.gz 2>/dev/null || echo "No daily backups found"
    ls -lh /var/www/backups/lead-generation-platform/weekly/*.tar.gz 2>/dev/null || echo "No weekly backups found"
    exit 1
fi

BACKUP_FILE="$1"
DATA_DIR="/var/www/lead-generation-platform/backend/data"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Error: Backup file not found: $BACKUP_FILE"
    exit 1
fi

echo "=== WARNING: This will replace current data! ==="
echo "Backup to restore: $BACKUP_FILE"
echo "Data directory: $DATA_DIR"
read -p "Continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    exit 0
fi

# Stop service
echo "Stopping service..."
sudo systemctl stop lead-generation-platform.service

# Backup current data (just in case)
echo "Backing up current data before restore..."
CURRENT_BACKUP="/tmp/before_restore_$(date +%Y%m%d_%H%M%S).tar.gz"
tar -czf "$CURRENT_BACKUP" -C /var/www/lead-generation-platform backend/data
echo "Current data backed up to: $CURRENT_BACKUP"

# Extract backup
echo "Restoring from backup..."
tar -xzf "$BACKUP_FILE" -C /var/www/lead-generation-platform

# Start service
echo "Starting service..."
sudo systemctl start lead-generation-platform.service

echo "Restore completed!"
