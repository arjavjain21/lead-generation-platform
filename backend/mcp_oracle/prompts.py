"""Phase 4: Guided MCP Prompt Templates.

Prompts are reusable conversation starters that AI agents invoke when they
need structured guidance for a common task. Each prompt returns a
pre-formatted message that includes context, instructions, and references
to the relevant resources/tools.

Unlike tools (which return data), prompts return a *plan of action* —
they tell the agent which resources to read, which tools to call, and
how to interpret the results.
"""

from __future__ import annotations

from .server import mcp


@mcp.prompt()
def enrich_a_company(domain: str) -> str:
    """Guide for enriching a company domain with decision-maker contacts.

    Args:
        domain: The company domain to enrich (e.g., "google.com").
    """
    return (
        f"# Enriching {domain}\n\n"
        "Follow these steps:\n\n"
        "1. **Check available providers** — call `list_current_providers` "
        "to see which providers are enabled right now.\n\n"
        "2. **Call the enrichment API**:\n"
        "```\n"
        f"POST /api/enrichment/enrich\n"
        f'{{"domain": "{domain}", "max_results": 5}}\n'
        "```\n"
        "Use `X-API-Key: lgp_...` header for authentication.\n\n"
        "3. **Interpret the response**:\n"
        "- `contacts` array contains the decision makers found\n"
        "- `email_source` on each contact shows which provider found the email\n"
        "- `data_sources` shows where company/contacts/emails came from\n"
        "- `sync_to_contacts_db` confirms the data was written back automatically\n\n"
        "4. **If no contacts found**, try:\n"
        "- Adding `full_name` for a specific person (switches to enhanced mode)\n"
        "- Adding `titles: \"CEO,CTO,VP\"` for title-based filtering\n"
        "- Using `force_provider: \"blitz\"` to test Blitz specifically\n\n"
        "Use `explain_response_field` if any field in the response is unclear.\n"
        "Use `compare_enrichment_modes` to understand the input modes."
    )


@mcp.prompt()
def troubleshoot_zero_emails(job_id: str) -> str:
    """Diagnose why an enrichment job returned 0 emails.

    Args:
        job_id: The enrichment job ID (8+ character prefix is enough).
    """
    return (
        f"# Troubleshooting Job {job_id} — 0 Emails\n\n"
        "Follow this diagnostic checklist:\n\n"
        "## Step 1: Check job status\n"
        f"```\n"
        f"GET /api/enrichment/jobs/{job_id}\n"
        f"```\n"
        "Look at:\n"
        "- `status`: should be `done`. If `failed`, check the `error` field.\n"
        "- `processed` vs `total`: if `processed=0`, the cascade never ran "
        "(likely a bug — check service logs).\n"
        "- `used_providers`: should list all 5 providers. If only "
        "`contacts_db` is listed, paid providers were skipped.\n\n"
        "## Step 2: Check if domains are valid\n"
        "Download the input CSV and verify domains have a `.` (e.g., "
        "`google.com`, not `google`). Invalid domains are silently skipped.\n\n"
        "## Step 3: Try a single-row test\n"
        "Pick one domain from the CSV and test it directly:\n"
        "```\n"
        "POST /api/enrichment/enrich\n"
        '{"domain": "test-domain.com", "debug": true}\n'
        "```\n"
        "The `debug=true` flag returns detailed routing info including "
        "`providers_called` and `providers_skipped`.\n\n"
        "## Step 4: Force specific providers\n"
        "Test each provider individually:\n"
        '```\n'
        '{"domain": "test.com", "force_provider": "blitz"}\n'
        '{"domain": "test.com", "force_provider": "smartprospect"}\n'
        '```\n\n'
        "## Step 5: Check provider health\n"
        "Call `list_current_providers` to see if any are disabled.\n"
        "Read `providers://status` resource for live cascade state.\n\n"
        "## Step 6: Check the UI warning\n"
        "If the job shows a yellow ⚠ banner, it was flagged as a "
        "potential pipeline failure. Click Retry to re-run.\n\n"
        "Use `explain_error_code` with the specific error message for "
        "targeted troubleshooting."
    )


@mcp.prompt()
def setup_clay_integration() -> str:
    """Step-by-step guide for setting up the enrichment API in Clay."""
    return (
        "# Setting Up Clay Integration\n\n"
        "## Prerequisites\n"
        "1. A ListBuilding account at https://listbuilding.eagleinfoservice.com/\n"
        "2. An API key (Account → API Keys → Create New Key)\n"
        "   Keys start with `lgp_` and do not expire.\n\n"
        "## Step 1: Add the enrichment card\n"
        "In Clay, search for the enrichment card that calls "
        "`POST /api/enrichment/enrich`. If using a custom HTTP cell:\n"
        "- URL: `https://listbuilding.eagleinfoservice.com/api/enrichment/enrich`\n"
        "- Method: POST\n"
        "- Headers: `X-API-Key: lgp_YOUR_KEY`, `Content-Type: application/json`\n\n"
        "## Step 2: Map input fields\n"
        "Map ONLY the fields you have:\n"
        "- **Domain only**: `{\"domain\": \"{{row.domain}}\"}`\n"
        "- **Domain + Name**: `{\"domain\": \"{{row.domain}}\", \"full_name\": \"{{row.name}}\"}`\n"
        "- **LinkedIn only**: `{\"linkedin_url\": \"{{row.linkedin}}\"}`\n\n"
        "## Step 3: Optional — limit providers\n"
        "To reduce costs, add `selected_providers`:\n"
        "```json\n"
        '{"domain": "{{row.domain}}", "selected_providers": ["contacts_db", "smartprospect"]}\n'
        "```\n"
        "This skips Blitz, WizLeads, and BetterEnrich.\n\n"
        "## Step 4: Map output fields\n"
        "The response has `contacts[0].email`, `contacts[0].full_name`, "
        "`contacts[0].title`, etc. Map these to Clay columns.\n\n"
        "## Step 5: Test on 10 rows first\n"
        "Run on a small sample to verify mapping before full enrichment.\n\n"
        "Read `docs://clay-guide` for the complete step-by-step guide with "
        "screenshots and troubleshooting."
    )


@mcp.prompt()
def choose_providers_for_budget(budget: str = "minimize cost") -> str:
    """Choose the right provider combination for your budget.

    Args:
        budget: Your budget goal (e.g., "free only", "minimize cost",
                "best quality regardless of cost").
    """
    return (
        f"# Provider Selection for Budget: {budget}\n\n"
        "## Provider Cost Structure\n\n"
        "| Provider | Cost | Rate | Best for |\n"
        "|----------|------|------|----------|\n"
        "| Contacts DB | **Free** | 75 RPS | Internal DB — always check first |\n"
        "| Blitz | Paid | 25 RPS | LinkedIn-based enrichment |\n"
        "| SmartProspect | Paid | 30 RPS | Self-verifying, batch-capable |\n"
        "| WizLeads | Paid | 10 RPS | Catch-all verified |\n"
        "| BetterEnrich | Paid | 10 RPS | Person + company email |\n\n"
        "## Recommended Configurations\n\n"
        "### Free tier only (zero paid API cost)\n"
        "```json\n"
        '{"selected_providers": ["contacts_db"]}\n'
        "```\n"
        "Only checks internal DB. Best for re-running against domains "
        "you've enriched before (data accumulates over time).\n\n"
        "### Cost-conscious (1 paid provider)\n"
        "```json\n"
        '{"selected_providers": ["contacts_db", "smartprospect"]}\n'
        "```\n"
        "Free first, then SmartProspect only. SmartProspect is self-verifying "
        "and batch-capable (30 RPS), giving the best cost/quality ratio.\n\n"
        "### Balanced (2 paid providers)\n"
        "```json\n"
        '{"selected_providers": ["contacts_db", "blitz", "smartprospect"]}\n'
        "```\n"
        "Free + Blitz (LinkedIn data) + SmartProspect (email verification).\n"
        "Skips WizLeads and BetterEnrich (lower hit rate, higher cost).\n\n"
        "### Full cascade (all providers)\n"
        "```json\n"
        '{"selected_providers": ["contacts_db", "blitz", "smartprospect", "wizleads", "better_enrich"]}\n'
        "```\n"
        "Or simply omit `selected_providers` entirely — the default uses all.\n\n"
        "## Cost-saving tips\n"
        "- Contacts DB is always free and always runs first — it accumulates "
        "data from every enrichment, so your hit rate improves over time\n"
        "- SmartProspect supports batch (up to 10 per call) — more efficient "
        "than single-call providers\n"
        "- Use `normalize_domains: true` on CSV uploads to avoid wasting "
        "calls on malformed URLs\n"
        "- Deduplicate input CSVs (built-in feature) to avoid re-enriching "
        "the same domain\n\n"
        "Call `list_current_providers` to see the live cascade, or read "
        "`providers://status` for the full status table."
    )


@mcp.prompt()
def compare_enrichment_vs_scraper() -> str:
    """Decide whether to use the Enrichment API or the Google Maps Scraper."""
    return (
        "# Enrichment API vs Google Maps Scraper — Which to Use?\n\n"
        "## Quick Decision\n\n"
        "| You have... | Use this | Why |\n"
        "|-------------|----------|-----|\n"
        "| Company domains | **Enrichment API** | Finds decision-maker emails |\n"
        "| Industry + location | **Scraper** | Finds businesses on Google Maps |\n"
        "| LinkedIn profiles | **Enrichment API** | Enriches specific people |\n"
        "| Nothing (cold start) | **Scraper → Enrichment** | Scrape first, then enrich |\n\n"
        "## Scraper: Google Maps Business Discovery\n"
        "- Input: query (e.g., \"dentist\") + country + cities\n"
        "- Output: business name, address, phone, website, rating, reviews\n"
        "- Rate: ~8 concurrent workers, 50K daily quota\n"
        "- Best for: building a list of businesses in a geographic area\n\n"
        "## Enrichment: Decision-Maker Contact Finding\n"
        "- Input: domain (company website) OR LinkedIn URL\n"
        "- Output: person name, title, email, LinkedIn, phone\n"
        "- Rate: 25-75 RPS per provider, cascade runs 5 providers\n"
        "- Best for: finding WHO to contact at a company you already know\n\n"
        "## Chaining: Scraper → Enrichment\n"
        "1. Run scraper: `POST /api/scraper/jobs` with query=\"dentist\", country=\"us\"\n"
        "2. Wait for completion (watch via SSE or poll status)\n"
        "3. Chain to enrichment: `POST /api/jobs/{scraper_id}/chain`\n"
        "   This reads the scraper's website column and creates an enrichment job\n"
        "4. The enrichment job processes each scraped domain through the cascade\n\n"
        "## Phone Enrichment (bonus)\n"
        "- Input: LinkedIn profile URLs\n"
        "- Output: phone numbers (US-focused)\n"
        "- Use AFTER enrichment (enrichment finds the LinkedIn URL, phone adds the number)\n\n"
        "Use `find_endpoint_for_intent` with your specific goal for endpoint-level guidance."
    )


@mcp.prompt()
def recover_abandoned_job(job_id: str) -> str:
    """Recover a job that was abandoned (server restart, crash, etc.).

    Args:
        job_id: The abandoned job ID.
    """
    return (
        f"# Recovering Abandoned Job {job_id}\n\n"
        "Abandoned jobs happen when the server restarts mid-processing "
        "(deploy, crash, OOM). The job's status is `abandoned` and "
        "partial results may exist.\n\n"
        "## Step 1: Check if there's partial output\n"
        f"```\n"
        f"GET /api/enrichment/jobs/{job_id}/partial-download\n"
        f"```\n"
        "If this returns a CSV, you have partial results. The number of "
        "rows tells you how far the job got.\n\n"
        "## Step 2: Restart the job (for enrichment)\n"
        f"```\n"
        f"POST /api/enrichment/jobs/{job_id}/restart\n"
        f"```\n"
        "This creates a NEW job with a new `job_id`. The old job's "
        "input CSV is reused. Returns the new `job_id`.\n\n"
        "## Step 3: Resume (for scraper — skip completed tasks)\n"
        "If the abandoned job is a scraper:\n"
        f"```\n"
        f"GET /api/scraper/jobs/{job_id}/resume-info\n"
        f"```\n"
        "Check `can_resume` and `checkpoint_count`. If resumable:\n"
        f"```\n"
        f"POST /api/scraper/jobs/{job_id}/resume\n"
        f'{{"include_previous": true}}\n'
        f"```\n"
        "This creates a new job that SKIPS already-completed tasks "
        "and copies the prior partial CSV into the output.\n\n"
        "## Step 4: Monitor progress\n"
        "Watch via SSE: `GET /api/enrichment/jobs/{{new_job_id}}/stream`\n"
        "Or poll: `GET /api/enrichment/jobs/{{new_job_id}}`\n\n"
        "## Prevention\n"
        "- Jobs with `last_heartbeat` within 2 minutes are NOT considered stale\n"
        "- The service runs cleanup_stale_jobs on startup to mark abandoned jobs\n"
        "- Use `selected_providers: [\"contacts_db\"]` for faster, cheaper "
        "re-runs of large jobs"
    )
