#!/usr/bin/env bash
# UI equivalent: "Jobs" page buttons — everything you do on a job card.
# Works for enrichment jobs (and scraper jobs via the /api/external/scraper twin).
# Set JOB_ID, then comment in the block you need.
set -euo pipefail

KEY=$(cat ~/.claude/.listbuilding-api-key)
BASE="https://listbuilding.eagleinfoservice.com"
JOB_ID="<paste-job-id>"

# --- Status (statuses: queued running done failed partial cancelled abandoned) ---
curl -s $BASE/api/enrichment/jobs/$JOB_ID -H "X-API-Key: $KEY" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["status"], d.get("processed"), "/", d.get("total"), "| emails:", d.get("emails_found"))'

# --- List MY jobs with filters (admin keys see ALL users' jobs) ---
curl -s "$BASE/api/enrichment/jobs?status=running&file_ready=true" -H "X-API-Key: $KEY" | python3 -m json.tool | head -40

# --- Download result CSV (202 while still running) ---
curl -s -H "X-API-Key: $KEY" "$BASE/api/enrichment/jobs/$JOB_ID/download" -o "./job_result.csv"

# --- In-progress partial CSV (safe while running) ---
curl -s -H "X-API-Key: $KEY" "$BASE/api/enrichment/jobs/$JOB_ID/partial-download" -o "./job_partial.csv"

# --- Huge files: 10K-row shards ---
curl -s -H "X-API-Key: $KEY" "$BASE/api/enrichment/jobs/$JOB_ID/shards"

# --- Resume eligibility + TRUE resume (creates a NEW child job; progress preserved) ---
curl -s -H "X-API-Key: $KEY" "$BASE/api/enrichment/jobs/$JOB_ID/resume-info"
# restart may take a minute on big CSVs (long client timeouts still often land —
# check for a child row with parent_job_id=JOB_ID before retrying):
curl -s -X POST -m 300 "$BASE/api/enrichment/jobs/$JOB_ID/restart" -H "X-API-Key: $KEY"

# --- Cancel (partial results preserved; ALWAYS cancel before asking for a service restart) ---
curl -s -X POST "$BASE/api/enrichment/jobs/$JOB_ID/cancel" -H "X-API-Key: $KEY"

# --- 2nd CSV: website-scraped emails for the job's no-email domains (free rescue;
#     appears some time after job completion; needs a ?token= JWT — ask the owner
#     or use the button on the job card in the UI) ---
echo "webscrape followup: GET $BASE/api/enrichment/jobs/$JOB_ID/webscrape-followup/download?token=<JWT>"

# --- Scraper job equivalents (status/results/cancel) ---
# curl -s $BASE/api/external/scraper/jobs/<id> -H "X-API-Key: $KEY"
# curl -s "$BASE/api/external/scraper/jobs/<id>/results?limit=1000&offset=0&fields=compact" -H "X-API-Key: $KEY"
# curl -s -X POST $BASE/api/external/scraper/jobs/<id>/cancel -H "X-API-Key: $KEY"
