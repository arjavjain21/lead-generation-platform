#!/usr/bin/env bash
# UI equivalent: "People" (Find People) page — search the contacts DB directly.
# FREE (no provider credits). 30M+ persons; results come with emails attached.
set -euo pipefail

KEY=$(cat ~/.claude/.listbuilding-api-key)
BASE="https://listbuilding.eagleinfoservice.com"

# Search by title keyword + country + must-have-email + lead universe
curl -s -X POST $BASE/api/enrichment/search/employees -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d '{
    "title_keywords": "founder",
    "geo_country": ["United States"],
    "has_email": true,
    "universe": "saas",
    "limit": 50,
    "offset": 0
  }' | python3 -m json.tool

# Other filters: seniority[] (C-Team|VP|Director|Manager), function[]
# (Sales|Marketing|Engineering), industry[], name_contains.
# universe must be one of: local_business | b2b_agency | saas | ecom (else 422).
# Enumerations for every filter:  curl -s $BASE/api/enrichment/search/options -H "X-API-Key: $KEY"
# Paginate with offset; total is null on keyword searches (count unknown).
