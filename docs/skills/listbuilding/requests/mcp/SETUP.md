# MCP setup — one-time registration

The platform hosts a **docs oracle + scraper action tools** MCP server at
`https://listbuilding.eagleinfoservice.com/mcp/`. Register it once per machine:

## Claude Code

```bash
claude mcp add listbuilding --transport http \
  --url https://listbuilding.eagleinfoservice.com/mcp/ \
  --header "X-API-Key: $(cat ~/.claude/.listbuilding-api-key)"
```

## Generic MCP client (streamable HTTP)

- **URL:** `https://listbuilding.eagleinfoservice.com/mcp/`
- **Header:** `X-API-Key: <your key>` (Bearer also accepted)
- **Auth note:** the middleware self-scopes to `/mcp*`; the same platform API key works.

## What you get

- **Docs oracle** (read-only): live endpoint catalog, provider cascade, error-code
  explanations, response-field docs — see `oracle-and-tools.md`.
- **5 scraper action tools**: `scrape_local_businesses` (dry-run by default),
  `get_scrape_job_status`, `get_scrape_job_results`, `check_scrape_cache`,
  `cancel_scrape_job` — the same engine the external REST API drives.

## Verify

After registering, ask your client: `list_current_providers` — it should return
the live cascade (contacts_db → smartprospect → quickenrich → getleads → blitz →
wizleads). If a provider is missing/disabled, trust this over any static doc.
