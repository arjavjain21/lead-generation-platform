# ListBuilding MCP — Setup Guide for Claude Code

- **Updated:** 2026-07-16
- **MCP Server:** `https://listbuilding.eagleinfoservice.com/mcp/`
- **Status:** Production-ready (verified end-to-end)

---

## What This Is

The ListBuilding MCP (Model Context Protocol) server connects Claude Code directly to the ListBuilding platform's API documentation. Once connected, Claude Code can:

- **Discover** the right API endpoint for any goal ("I want to enrich a domain")
- **Read** live API documentation, OpenAPI specs, and provider status
- **Explain** response fields, error codes, and enrichment modes
- **Validate** request payloads before you send them
- **Guide** you through common workflows with step-by-step templates

The MCP is a **documentation oracle** — it tells you WHICH endpoint to use and HOW, but it does not make API calls itself. You (or your code) make the actual API calls using the guidance the MCP provides.

---

## Prerequisites

1. **A ListBuilding account** at [https://listbuilding.eagleinfoservice.com/](https://listbuilding.eagleinfoservice.com/)
2. **An API key** — generate one in the app:
   - Log in → sidebar → **API Keys** → Create New Key
   - Your key starts with `lgp_` and looks like: `lgp_DK79a_il2RKzmvL_BJdiCXSsNZkhZLQ2dvRlxzn4EIc`
   - API keys do not expire
3. **Claude Code** installed on your machine ([installation guide](https://docs.anthropic.com/en/docs/claude-code))

---

## Installation

### Method A: Via Claude Code settings file (recommended)

**Step 1:** Open your Claude Code MCP configuration. The file location depends on your OS:

| OS | Path |
|---|---|
| macOS | `~/.claude/settings.json` |
| Linux | `~/.claude/settings.json` |
| Windows | `%USERPROFILE%\.claude\settings.json` |

If the file or directory doesn't exist, create it.

**Step 2:** Add the ListBuilding MCP server:

```json
{
  "mcpServers": {
    "listbuilding": {
      "url": "https://listbuilding.eagleinfoservice.com/mcp/",
      "headers": {
        "X-API-Key": "lgp_YOUR_API_KEY_HERE"
      }
    }
  }
}
```

Replace `lgp_YOUR_API_KEY_HERE` with your actual API key from Step 1 of Prerequisites.

**Step 3:** Restart Claude Code (close and reopen the terminal/app).

### Method B: Via Claude Code CLI

Run this in your terminal:

```bash
claude mcp add listbuilding \
  --transport http \
  --url https://listbuilding.eagleinfoservice.com/mcp/ \
  --header "X-API-Key: lgp_YOUR_API_KEY_HERE"
```

Restart Claude Code after adding.

---

## Verification

After restarting Claude Code, verify the MCP is connected by asking:

> *"Read the health://status resource from the listbuilding MCP."*

Claude Code should respond with `ok`.

If you get an error, see the [Troubleshooting](#troubleshooting) section below.

---

## What's Available

### Resources (7) — Live, auto-synced documentation

Resources are data the MCP serves on demand. They always reflect the current state of the platform.

| Resource URI | What it returns |
|---|---|
| `health://status` | Server health check — returns `"ok"` |
| `openapi://full` | Complete OpenAPI 3.1 spec (all 70 endpoints, every parameter) |
| `openapi://endpoints` | Condensed endpoint index — one line per route |
| `docs://clay-guide` | Clay enrichment step-by-step guide |
| `docs://api-reference` | Full platform API reference (enrichment + phone + scraper + auth) |
| `docs://changelog` | What changed and when |
| `providers://status` | Live provider cascade — which providers are enabled right now |
| `schemas://{model_name}` | JSON schema for any request model (e.g., `schemas://UnifiedEnrichRequest`) |

### Tools (8 docs + 5 actions) — Ask questions, get answers, run scrapes

Tools are functions Claude Code can call. The first 8 are the **read-only docs oracle**; the last 5 are
**scraper action tools** (added 2026-08-30) that actually drive the Google Maps scraper pipeline — same
guardrails as the HTTP API (your key's ownership, 50K/day quota, per-job task cap), gated server-side by
`ENABLE_MCP_SCRAPER_TOOLS`.

| Tool | What it answers / does |
|---|---|
| `find_endpoint_for_intent` | "I want to enrich a domain" → best endpoints ranked by relevance |
| `get_endpoint_details` | Full parameters, response shape, error codes for one endpoint |
| `explain_response_field` | "What does `email_source: smartprospect_email` mean?" |
| `explain_error_code` | "I got a 429 error" → troubleshooting steps |
| `list_current_providers` | Live cascade table — which providers are enabled, their rate limits |
| `get_quota_guide` | Quota limits, reset schedule, cost-reduction tips |
| `compare_enrichment_modes` | Difference between `domain_only`, `linkedin_only`, `enhanced` |
| `validate_request` | "Is this JSON payload valid for `/api/enrichment/enrich`?" |
| `scrape_local_businesses` | ⚡ **Action.** Estimate (`dry_run=true` is the DEFAULT — nothing is created) or create (`dry_run=false`) a Google Maps scrape job. `prefer_cache=true` serves free cached data when available |
| `get_scrape_job_status` | ⚡ **Action.** Poll a job: progress %, rows on disk, queue position, links |
| `get_scrape_job_results` | ⚡ **Action.** JSON rows from a finished job (≤200/call, compact fields by default) |
| `check_scrape_cache` | ⚡ **Action.** Is this exact scrape already cached (free, instant)? Includes sample rows |
| `cancel_scrape_job` | ⚡ **Action.** Cancel a queued/running job; partial results are kept |

> **Cost safety:** `scrape_local_businesses` defaults to `dry_run=true`, so a model can't accidentally
> commit thousands of paid scraper.tech tasks — the dry-run response shows the task count and quota impact,
> and you explicitly set `dry_run=false` to proceed. Non-admin jobs above `MAX_EXTERNAL_SCRAPER_TASKS`
> (default 15,000 tasks ≈ 5,000 centers) are rejected with a clear error.

### Prompts (6) — Guided templates for common tasks

Prompts are pre-built conversation starters. Ask Claude Code to use them:

| Prompt | When to use |
|---|---|
| `enrich_a_company` | "Use the enrich_a_company prompt for google.com" |
| `troubleshoot_zero_emails` | "Use troubleshoot_zero_emails for job abc123" |
| `setup_clay_integration` | "Use setup_clay_integration prompt" |
| `choose_providers_for_budget` | "Use choose_providers_for_budget for free tier only" |
| `compare_enrichment_vs_scraper` | "Use compare_enrichment_vs_scraper prompt" |
| `recover_abandoned_job` | "Use recover_abandoned_job for job xyz789" |

---

## Example Usage

Once connected, just talk to Claude Code naturally. Here are real examples:

### Example 1: "Which endpoint do I use?"

> **You:** "I have a list of company domains and I want to find decision-maker emails for each one. Which API endpoint should I use?"

Claude Code will call `find_endpoint_for_intent`, then tell you:
- Use `POST /api/enrichment/flows/domain-enrich` for bulk CSV processing
- Or `POST /api/enrichment/enrich` for single-row lookups
- Include the exact JSON payload and curl example

### Example 2: "What does this field mean?"

> **You:** "I got `email_source: smartprospect_email` in my response. What does that mean?"

Claude Code will call `explain_response_field` and explain that SmartProspect found the email, what SmartProspect is, and how it fits in the cascade.

### Example 3: "Why did I get an error?"

> **You:** "I got HTTP 429 when trying to start a scraper job. What happened?"

Claude Code will call `explain_error_code` and tell you:
- 429 = daily quota exceeded (50K for non-admin)
- How to check remaining quota via `GET /api/quota`
- Tips for reducing quota usage

### Example 4: "Validate my request"

> **You:** "Validate this payload for /api/enrichment/enrich: {\"domain\": \"acme.com\", \"selected_providers\": [\"contacts_db\"]}"

Claude Code will call `validate_request` and confirm whether the payload matches the schema.

### Example 5: "Show me the docs"

> **You:** "Read the API reference from the listbuilding MCP."

Claude Code will read `docs://api-reference` and summarize or search it for you.

---

## Authentication

The MCP server accepts two authentication methods:

### API Key (recommended for MCP)
```
Header: X-API-Key: lgp_your_key_here
```
- Keys start with `lgp_` and never expire
- Generate at: **Account → API Keys** in the web UI
- Works for: single-row enrichment (`/api/enrichment/enrich`), all MCP endpoints, scraper downloads/resume

### JWT Bearer Token (alternative)
```
Header: Authorization: Bearer your_jwt_token
```
- 7-day expiry — refresh via `POST /api/auth/refresh`
- Required for: CSV upload, bulk job creation, job management endpoints
- Not needed for MCP (API key is sufficient)

### Where each method works

| Action | API Key | JWT |
|---|:---:|:---:|
| **MCP server access** | Yes | Yes |
| Single-row enrichment (`/enrich`) | Yes | Yes |
| Scraper downloads + resume | Yes | Yes |
| CSV upload + bulk jobs | No | **Yes only** |
| API key management | No | **Yes only** |

**For MCP usage, API key is all you need.**

---

## Provider Cascade

The enrichment cascade checks providers in this order, stopping at the first one that returns a valid email:

| # | Provider | Cost | Rate Limit | Purpose |
|---|---|---|---|---|
| 1 | Contacts DB | **Free** | 75 RPS | Internal database |
| 2 | Blitz | Paid | 25 RPS | LinkedIn enrichment |
| 3 | SmartProspect | Paid | 30 RPS | SmartLead Find Emails |
| 4 | WizLeads | Paid | 10 RPS | Verified email |
| 5 | BetterEnrich | Paid | 10 RPS | Person + company email |

Ask Claude Code: *"List current providers"* to see the live status.

---

## Troubleshooting

### MCP not connecting

1. **Verify your API key is valid:**
   ```bash
   curl -s https://listbuilding.eagleinfoservice.com/api/health \
     -H "X-API-Key: lgp_YOUR_KEY"
   ```
   Should return `{"status":"ok","mcp_enabled":true}`

2. **Check the MCP endpoint directly:**
   ```bash
   curl -s -X POST https://listbuilding.eagleinfoservice.com/mcp/ \
     -H "Content-Type: application/json" \
     -H "Accept: application/json, text/event-stream" \
     -H "X-API-Key: lgp_YOUR_KEY" \
     -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}'
   ```
   Should return a JSON-RPC response with `"serverInfo":{"name":"listbuilding-docs-oracle"}`.

3. **Check settings.json syntax:** JSON must be valid (no trailing commas, proper quotes).

4. **Restart Claude Code completely** — not just reload, fully close and reopen.

### "401 Authentication required" error

- Your API key may be invalid or revoked
- Check for extra spaces or newlines in the key
- Generate a new key at **Account → API Keys**

### Claude Code doesn't see the MCP tools

- Ensure the MCP server entry is under `"mcpServers"` (plural, camelCase)
- The key name (`"listbuilding"`) can be anything — it's just an identifier
- Check Claude Code logs for MCP connection errors

### Tools return "not found" or empty results

- The MCP server reads from live docs files — if the docs are missing from the server, resources will return "not found"
- Ask Claude Code to read `health://status` first — if that returns "ok", the server is working
- If `openapi://full` is empty, the service may need a restart

---

## FAQ

**Q: Does the MCP make API calls for me?**
No. The MCP is a documentation oracle — it tells you WHICH endpoint to use and HOW, but you (or your code) make the actual API calls. This is by design for security and cost control.

**Q: How often does the documentation update?**
Automatically. The MCP reads the live OpenAPI spec from the running FastAPI app. When a new endpoint is added and the service restarts, the MCP serves the new spec on the next query. The markdown docs (`docs://clay-guide`, `docs://api-reference`) are read from the server's filesystem — update the files and the next query gets the new content.

**Q: Can I use the MCP from other tools (Cursor, VS Code, etc.)?**
Yes. Any MCP-compatible client can connect using the same URL and API key. The MCP server uses the standard streamable HTTP transport.

**Q: Is there a daily quota for MCP usage?**
No. MCP queries are read-only metadata operations — they do not count against the 50K daily API request quota. Only actual API calls (enrichment, scraping, etc.) count.

**Q: What if I need to make actual API calls from Claude Code?**
Use Claude Code's `Bash` tool to run `curl` commands against the API. The MCP gives you the exact endpoint, parameters, and examples — then you execute the call yourself.

**Q: Can multiple team members use the same MCP server?**
Yes. Each person uses their own API key. The MCP server handles concurrent connections (4 gunicorn workers, stateless mode).

---

## Quick Reference Card

| Need | Ask Claude Code |
|---|---|
| Find the right endpoint | "Find the endpoint for [your goal]" |
| Get endpoint details | "Get details for POST /api/enrichment/enrich" |
| Explain a field | "Explain the email_source field" |
| Debug an error | "Explain error 401" or "Explain error 'selected_providers must be non-empty'" |
| Check providers | "List current providers" |
| Check quota | "Get the quota guide" |
| Compare modes | "Compare enrichment modes" |
| Validate payload | "Validate this for /api/enrichment/enrich: {your JSON}" |
| Read the docs | "Read the api-reference resource" |
| Setup guide | "Use the setup_clay_integration prompt" |
| Budget help | "Use the choose_providers_for_budget prompt for free tier" |
| Troubleshoot 0 emails | "Use the troubleshoot_zero_emails prompt for job [id]" |

---

## Support

- **Platform issues:** arjav@eagleinfoservice.com
- **MCP endpoint:** `https://listbuilding.eagleinfoservice.com/mcp/`
- **Health check:** `https://listbuilding.eagleinfoservice.com/api/health`
- **API reference (full):** Read the `docs://api-reference` resource via MCP
