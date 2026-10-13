#!/usr/bin/env bash
# UI equivalent: "Scrape Google Maps" page — new scrape job.
# Flow: FREE cache-check -> dry-run estimate -> create job -> poll -> fetch rows.
# Edit QUERY / CITIES / COUNTRY below. Never skip steps 1-2: identical cached
# queries are free; estimates show real cost (centers x 3 tasks) before you spend.
set -euo pipefail

KEY=$(cat ~/.claude/.listbuilding-api-key)
BASE="https://listbuilding.eagleinfoservice.com"
QUERY="dentist"                       # business type, like the UI query box
MODE="cities"                         # cities | states | zips (US) | centers | all
CITIES='["Austin","Dallas"]'          # JSON array
COUNTRY="us"

# 1) FREE cache check (90-day cache; a hit = rows returned inline, no job, no cost)
curl -s -X POST $BASE/api/external/scraper/cache -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"query":"'"$QUERY"'","mode":"'"$MODE"'","cities":'"$CITIES"',"country":"'"$COUNTRY"'"}'
echo

# 2) Dry-run estimate (task_count = centers x 3 zooms; read before creating)
curl -s -X POST $BASE/api/external/scraper/estimate -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"query":"'"$QUERY"'","mode":"'"$MODE"'","cities":'"$CITIES"',"country":"'"$COUNTRY"'"}' \
  | python3 -m json.tool
echo

# 3) Create the job (prefer_cache=true default: a FULL cache hit returns
#    created:false + rows inline — no job row, no quota, no cost)
CREATE=$(curl -s -X POST $BASE/api/external/scraper/jobs -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"query":"'"$QUERY"'","mode":"'"$MODE"'","cities":'"$CITIES"',"country":"'"$COUNTRY"'","prefer_cache":true}')
echo "$CREATE" | python3 -m json.tool
JOB_ID=$(echo "$CREATE" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["job_id"])' \
  || echo "")
[ -z "$JOB_ID" ] && { echo "no job created (likely full cache hit above)"; exit 0; }

# 4) Poll until terminal (done|failed|cancelled|abandoned)
while :; do
  STATUS=$(curl -s $BASE/api/external/scraper/jobs/$JOB_ID -H "X-API-Key: $KEY")
  S=$(echo "$STATUS" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print(d["status"], d.get("progress",{}).get("pct_complete","?"), d.get("rows_on_disk","?"))')
  echo "poll: $S"
  echo "$S" | grep -qE '^(done|failed|cancelled|abandoned)' && break
  sleep 15
done

# 5) Fetch results (compact = 13 LLM-friendly fields; paginate with offset)
curl -s "$BASE/api/external/scraper/jobs/$JOB_ID/results?limit=1000&offset=0&fields=compact" \
  -H "X-API-Key: $KEY" > "./${QUERY}_${COUNTRY}_results.json"
echo "rows saved: ./${QUERY}_${COUNTRY}_results.json"
# CSV instead:  curl -s -H "X-API-Key: $KEY" $BASE/api/scraper/jobs/$JOB_ID/download -o results.csv
