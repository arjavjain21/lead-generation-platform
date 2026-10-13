# MCP requests — docs oracle + scraper action tools

Ask these through any registered MCP client (see `SETUP.md`). The oracle is the
**live truth source** — when static docs (including the skill files!) lag behind
a provider change, the oracle is authoritative.

## Docs oracle (read-only questions)

| Ask | Use when |
|---|---|
| `find_endpoint_for_intent("bulk enrich a CSV of domains")` | You know WHAT you want, not which endpoint |
| `get_endpoint_details(method="POST", path="/api/enrichment/flows/domain-enrich")` | Exact body fields / response shape / auth for one endpoint |
| `explain_error_code("429")` | A request failed and you want the platform-specific meaning + fix |
| `explain_response_field("email_source")` | A response field is unclear |
| `list_current_providers()` | **Live cascade order + who's enabled** (better than `GET /providers`, which returns membership only) |

Example conversation turns:
- "Ask listbuilding: explain_error_code 422"
- "Use find_endpoint_for_intent for 'find companies by persona and location'"

## Scraper action tools (drive scraping without REST)

All async; identity comes from the API key used at registration.

| Tool | What it does | Notes |
|---|---|---|
| `scrape_local_businesses` | Start a Google Maps scrape | **dry_run defaults to true** — returns estimate only; pass `dry_run: false` to actually create. Mirror of REST: cache-check → estimate → create |
| `check_scrape_cache` | Free cache lookup | Do this before any create |
| `get_scrape_job_status` | Poll a job | Terminal: done/failed/cancelled/abandoned |
| `get_scrape_job_results` | Fetch rows | Paginated; compact fields available |
| `cancel_scrape_job` | Cancel | Partial rows preserved |

Example: *"Use the listbuilding MCP tool scrape_local_businesses with query
'dentist', cities ['Austin'], country 'us', dry_run true — then tell me the
task_count and cache status."*

## Typical chains

1. **Scrape → emails (agency bread & butter):**
   `check_scrape_cache` (free) → `scrape_local_businesses` dry_run → real run →
   `get_scrape_job_results` (keep `website` rows) → REST Flow 1 on the websites
   (`api/02-enrich-domains-csv.sh`) → free website-email rescue on misses.
2. **"Which endpoint do I need?"** → `find_endpoint_for_intent` →
   `get_endpoint_details` → copy the body shape into the matching `api/*.sh`.
