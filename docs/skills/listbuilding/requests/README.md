# Ready-to-use requests — find the API/MCP equivalent of your UI flow

Every file here maps to a page you already use in the web app
(https://listbuilding.eagleinfoservice.com). Same actions, runnable from any
script, cron, or agent — no browser needed.

## Setup (once)

```bash
install -m 600 /dev/stdin ~/.claude/.listbuilding-api-key <<< "lgp_YOUR_KEY"   # from the platform owner
```

Every API script reads the key from that file. Run scripts with `bash <file>`
(after a git clone they are not executable bit-set).

## UI page → request file

| Web app page (what you click today) | API script | MCP |
|---|---|---|
| **Scrape Google Maps** (new scrape) | `api/01-scrape-google-maps.sh` | `mcp/oracle-and-tools.md` § scraper action tools |
| **Domains** (upload CSV → decision makers) | `api/02-enrich-domains-csv.sh` | — |
| **LinkedIn** (bulk LinkedIn URL CSV) | `api/03-enrich-linkedin-urls.sh` | — |
| **Home quick-lookup box** (one domain/person) | `api/04-single-lookup.sh` | — |
| **People** (Find People search) | `api/05-find-people-search.sh` | — |
| **Find Companies (TAM)** | `api/06-tam-find-companies.sh` | — |
| Company search by name (the resolver) | `api/07-company-search-name-resolver.sh` | — |
| **Jobs** page (status / download / resume / cancel / 2nd email CSV) | `api/08-jobs-lifecycle.sh` | `mcp/oracle-and-tools.md` § get_scrape_job_status/results |
| Ask "which endpoint / why did it fail?" | — | `mcp/oracle-and-tools.md` § docs oracle |
| Register the MCP server | — | `mcp/SETUP.md` |

## Rules

- Never hardcode the key — scripts read it from the key file.
- Scraper flow ALWAYS: cache-check → estimate → create (never skip the first two).
- Final pass of any enrichment run must be UNRESTRICTED (no `providers` allowlist)
  or the free website-email rescue is skipped.
