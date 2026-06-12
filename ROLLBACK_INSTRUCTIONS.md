ROLLBACK INSTRUCTIONS - ROUTING ITERATION 2 DEPLOYMENT
======================================

IF THE DEPLOYMENT GOES HAYWIRE:

### Immediate Rollback Commit:
git reset --hard 2f5105a5b77e408ada255802e702a1b6e646eeda

### Service Restart:
sudo systemctl restart lead-generation-platform.service
sudo systemctl status lead-generation-platform.service --no-pager

### Optional: Database Rollback (only if absolutely needed):
# From the backup location
sqlite3 /var/www/backups/lead-generation-platform/daily/pre_routing_deploy_20260612_153605.db \
  ".restore /var/www/lead-generation-platform/backend/data/jobs.db"

### Final Verify:
curl -s http://localhost:8765/api/health

---

ROLLBACK TIMESTAMP: 2026-06-12_153605
BACKUP LOCATION: /var/www/backups/lead-generation-platform/daily/pre_routing_deploy_20260612_153605.db