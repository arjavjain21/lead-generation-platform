ROLLBACK INSTRUCTIONS - ENRICHMENT AUDIT TRAIL DEPLOYMENT
===========================================================

Deployed: 2026-06-12 16:48 UTC
Commit:    6f8f842 (feat: enrichment audit trail — per-row provider attempts and no-email reasons)
Tag:       pre-audit-trail (commit 2c72434)

IF THE DEPLOYMENT GOES HAYWIRE:

### Immediate Rollback:
git reset --hard pre-audit-trail
sudo systemctl restart lead-generation-platform.service
sudo systemctl status lead-generation-platform.service --no-pager

### Or step back to the prior deploy (a08412a):
git reset --hard a08412a
sudo systemctl restart lead-generation-platform.service

### Final Verify:
curl -s http://localhost:8765/api/health
sudo journalctl -u lead-generation-platform.service -n 50 --no-pager

---

PRIOR DEPLOYMENT: ROUTING ITERATION 2
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