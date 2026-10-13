#!/usr/bin/env bash
# UI equivalent: "LinkedIn" page — bulk CSV of person LinkedIn URLs.
# Cascade per URL: contacts_db -> quickenrich -> getleads (batch pre-pass 100/chunk) -> blitz.
# Person URLs only (/in/...); company URLs (/company/...) work better via 02 with padding.
set -euo pipefail

KEY=$(cat ~/.claude/.listbuilding-api-key)
BASE="https://listbuilding.eagleinfoservice.com"
CSV="./linkedin_leads.csv"
PERSON_COL="linkedin_url"             # column containing linkedin.com/in/... values

UPLOAD=$(curl -s -X POST $BASE/api/enrichment/upload -H "X-API-Key: $KEY" -F "file=@$CSV")
UPLOAD_ID=$(echo "$UPLOAD" | python3 -c 'import json,sys; print(json.load(sys.stdin)["upload_id"])')

JOB=$(curl -s -X POST $BASE/api/enrichment/by-linkedin-v2 -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d '{
    "upload_id": "'"$UPLOAD_ID"'",
    "personal_linkedin_col": "'"$PERSON_COL"'",
    "include_company": true,
    "max_dms": 5
  }')
JOB_ID=$(echo "$JOB" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
echo "job_id: $JOB_ID"

while :; do
  S=$(curl -s $BASE/api/enrichment/jobs/$JOB_ID -H "X-API-Key: $KEY" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["status"], d.get("processed"), "/", d.get("total"), "| emails:", d.get("emails_found"))')
  echo "poll: $S"
  echo "$S" | grep -qE '^(done|failed|partial|cancelled|abandoned) ' && break
  sleep 10
done

curl -s -H "X-API-Key: $KEY" "$BASE/api/enrichment/jobs/$JOB_ID/download" -o "./linkedin_enriched_$(date +%Y%m%d).csv"
echo "saved: ./linkedin_enriched_$(date +%Y%m%d).csv"
