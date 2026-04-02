#!/bin/bash
# Backup script for Lead Generation Platform
# Backs up database, output files, and uploads

set -e

BACKUP_DIR="/var/www/backups/lead-generation-platform"
DATA_DIR="/var/www/lead-generation-platform/backend/data"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
KEEP_DAILY=7
KEEP_WEEKLY=4
KEEP_MONTHLY=3

# Log file
LOG_FILE="/var/www/lead-generation-platform/backup.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "===== Backup started at $(date) ====="

# 1. Backup database
echo "Backing up database..."
sqlite3 "$DATA_DIR/jobs.db" ".backup '$BACKUP_DIR/daily/jobs_db_$TIMESTAMP.db'"
chmod 640 "$BACKUP_DIR/daily/jobs_db_$TIMESTAMP.db"

# 2. Backup output files
echo "Backing up output files..."
tar -czf "$BACKUP_DIR/daily/outputs_$TIMESTAMP.tar.gz" -C "$DATA_DIR" outputs/

# 3. Backup uploads
echo "Backing up uploads..."
tar -czf "$BACKUP_DIR/daily/uploads_$TIMESTAMP.tar.gz" -C "$DATA_DIR" uploads/

# 4. Create a combined backup
echo "Creating combined backup..."
tar -czf "$BACKUP_DIR/daily/full_backup_$TIMESTAMP.tar.gz" -C /var/www/lead-generation-platform backend/data
chmod 640 "$BACKUP_DIR/daily/full_backup_$TIMESTAMP.tar.gz"

# 5. Clean old daily backups (keep last N days)
echo "Cleaning old daily backups..."
ls -t "$BACKUP_DIR/daily"/full_backup_*.tar.gz 2>/dev/null | tail -n +$((KEEP_DAILY + 1)) | xargs -r rm --

# 6. Weekly rotation (Sundays)
if [ $(date +%u) -eq 7 ]; then
    echo "Creating weekly backup..."
    cp "$BACKUP_DIR/daily/full_backup_$TIMESTAMP.tar.gz" "$BACKUP_DIR/weekly/weekly_$(date +%Y Week %U).tar.gz"
    ls -t "$BACKUP_DIR/weekly"/weekly_*.tar.gz 2>/dev/null | tail -n +$((KEEP_WEEKLY + 1)) | xargs -r rm --
fi

# 7. Monthly rotation (1st of month)
if [ $(date +%d) -eq 01 ]; then
    echo "Creating monthly backup..."
    cp "$BACKUP_DIR/daily/full_backup_$TIMESTAMP.tar.gz" "$BACKUP_DIR/monthly/monthly_$(date +%Y-%m).tar.gz"
    ls -t "$BACKUP_DIR/monthly"/monthly_*.tar.gz 2>/dev/null | tail -n +$((KEEP_MONTHLY + 1)) | xargs -r rm --
fi

# 8. Report sizes
echo "Backup sizes:"
du -sh "$BACKUP_DIR/daily/"*
du -sh "$BACKUP_DIR"/weekly/* 2>/dev/null || echo "No weekly backups yet"
du -sh "$BACKUP_DIR"/monthly/* 2>/dev/null || echo "No monthly backups yet"

echo "===== Backup completed at $(date) ====="
echo ""
