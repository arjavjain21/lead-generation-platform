#!/usr/bin/env bash
# UI equivalent: company search — AND the "name resolver" step when your CSV has
# only company names. Returns company LinkedIn URLs (results have NO domain —
# feed the linkedin_url into 02-enrich-domains-csv.sh via the empty-domain
# padding trick, or into 03).
# Cost: ~1 Blitz credit per 5 results (empty results are free). <=5 concurrent.
set -euo pipefail

KEY=$(cat ~/.claude/.listbuilding-api-key)
BASE="https://listbuilding.eagleinfoservice.com"

# By name (+ optional country filter)
curl -s -X POST $BASE/api/enrichment/search/companies -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Spring Dental","country_code":"US","limit":5}' | python3 -m json.tool

# By ICP filters (industry / size)
curl -s -X POST $BASE/api/enrichment/search/companies -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"industry":["Software"],"employee_range":["51-200"],"country_code":"US","limit":50}' | python3 -m json.tool

# Auto-pick rule: near-exact name match, or CSV-city matches exactly one hq.city.
# Ambiguous (<=25): confirm with the user; (>25): best match + match_confidence=low.
