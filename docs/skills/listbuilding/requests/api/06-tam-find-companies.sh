#!/usr/bin/env bash
# UI equivalent: "Find Companies (TAM)" page — describe a persona + firmographics,
# get a deduplicated company list (with matched_people counts) as a job CSV.
# PAID: 1 Blitz record per company RETURNED (empty page free) — max_companies is
# your budget ceiling. At least ONE filter is required (400 otherwise).
# Optional: chain straight into a Flow-1 enrichment job over the discovered domains.
set -euo pipefail

KEY=$(cat ~/.claude/.listbuilding-api-key)
BASE="https://listbuilding.eagleinfoservice.com"

JOB=$(curl -s -X POST $BASE/api/enrichment/flows/tam -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d '{
    "company": {
      "industry_include": ["Software"],
      "employee_range": ["51-200"],
      "hq_country_code": ["US"]
    },
    "people": {
      "job_title_include": ["VP Engineering"],
      "job_levels": ["VP"],
      "min_per_company": 1
    },
    "max_companies": 500,
    "create_enrichment_job": true,
    "titles": "VP Engineering",
    "exact_titles": true
  }')
JOB_ID=$(echo "$JOB" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
echo "job_id: $JOB_ID  (total in response = budget ceiling, not result count)"

while :; do
  S=$(curl -s $BASE/api/enrichment/jobs/$JOB_ID -H "X-API-Key: $KEY" \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["status"], d.get("processed"), "/", d.get("total"))')
  echo "poll: $S"
  echo "$S" | grep -qE '^(done|failed|partial|cancelled|abandoned) ' && break
  sleep 15
done

curl -s -H "X-API-Key: $KEY" "$BASE/api/enrichment/jobs/$JOB_ID/download" -o "./tam_companies_$(date +%Y%m%d).csv"
echo "saved: ./tam_companies_$(date +%Y%m%d).csv"
# Columns: name, domain, website, linkedin_url, industry, type, size,
# employees_on_linkedin, followers, founded_year, hq_*, revenue, slogan,
# employee_growth_1y, matched_people. Every company is auto-saved to the contacts DB.
