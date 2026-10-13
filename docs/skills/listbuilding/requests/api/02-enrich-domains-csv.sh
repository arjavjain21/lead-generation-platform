#!/usr/bin/env bash
# UI equivalent: "Domains" page — upload a CSV of domains, get decision makers.
# (Flow 1 — also handles company-LinkedIn-URL rows via the empty-domain padding
# trick; person LinkedIn URLs must go to 03 instead.)
# Edit CSV path, DOMAIN_COL and TITLES below.
set -euo pipefail

KEY=$(cat ~/.claude/.listbuilding-api-key)
BASE="https://listbuilding.eagleinfoservice.com"
CSV="./leads.csv"                     # must have a header row
DOMAIN_COL="domain"                   # column with domains/websites (may contain full URLs)
TITLES="CEO,Founder,Owner,VP,Director" # leave unset to use the default 3-tier cascade

# 1) Upload
UPLOAD=$(curl -s -X POST $BASE/api/enrichment/upload -H "X-API-Key: $KEY" -F "file=@$CSV")
UPLOAD_ID=$(echo "$UPLOAD" | python3 -c 'import json,sys; print(json.load(sys.stdin)["upload_id"])')
echo "upload_id: $UPLOAD_ID"

# 2) Create the Flow-1 job (strict_titles defaults ON = local ICP title gate)
JOB=$(curl -s -X POST $BASE/api/enrichment/flows/domain-enrich -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d '{
    "upload_id": "'"$UPLOAD_ID"'",
    "domain_col": "'"$DOMAIN_COL"'",
    "max_results": 5,
    "titles": "'"$TITLES"'"
  }')
JOB_ID=$(echo "$JOB" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
echo "job_id: $JOB_ID"

# 3) Poll (success status is DONE, not 'completed')
while :; do
  S=$(curl -s $BASE/api/enrichment/jobs/$JOB_ID -H "X-API-Key: $KEY" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["status"], d.get("processed"), "/", d.get("total"), "| emails:", d.get("emails_found"))')
  echo "poll: $S"
  echo "$S" | grep -qE '^(done|failed|partial|cancelled|abandoned) ' && break
  sleep 10
done

# 4) Download the result CSV (inputs + dm_* columns)
curl -s -H "X-API-Key: $KEY" "$BASE/api/enrichment/jobs/$JOB_ID/download" -o "./enriched_$(date +%Y%m%d).csv"
echo "saved: ./enriched_$(date +%Y%m%d).csv"

# NOTE: no-email domains are auto-submitted to the free website-email scraper
# (~51% fill) ONLY if the job has NO providers allowlist — a second CSV appears
# on the job later (see 08-jobs-lifecycle.sh).
