#!/usr/bin/env bash
# UI equivalent: quick-lookup box on the Home page — one domain or person.
# Modes (auto-detected): domain only | linkedin_url only | domain+full_name (enhanced).
set -euo pipefail

KEY=$(cat ~/.claude/.listbuilding-api-key)
BASE="https://listbuilding.eagleinfoservice.com"

# --- FREE pass first ($0, contacts DB only) ---
curl -s -X POST $BASE/api/enrichment/enrich -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"domain":"acme.com","selected_providers":["contacts_db"]}' | python3 -m json.tool

# --- FULL cascade (paid tiers kick in only where the free pass missed) ---
curl -s -X POST $BASE/api/enrichment/enrich -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"domain":"acme.com","max_results":3,"titles":"CEO,Founder,VP"}' | python3 -m json.tool

# --- Specific person by LinkedIn URL (contacts_db -> quickenrich -> getleads -> blitz) ---
curl -s -X POST $BASE/api/enrichment/enrich -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"linkedin_url":"https://www.linkedin.com/in/someone"}' | python3 -m json.tool

# Notes:
# - Every successful call ALSO syncs the contact to the platform contacts DB.
# - company_name alone is NOT accepted — resolve it first (07-company-search-name-resolver.sh).
# - Restrictors are XOR: selected_providers OR force_provider, never both (400).
