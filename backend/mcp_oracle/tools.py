"""Phase 3: Intent-driven MCP Discovery Tools.

Tools are callable functions that AI agents invoke to answer questions.
Unlike resources (which return raw data), tools process data and return
formatted, contextual responses.

All tools are read-only. None make HTTP calls, none have side effects.
They read from the same live sources as the Phase 2 resources (OpenAPI,
docs folder, provider config) and add intelligence on top.
"""

from __future__ import annotations

import json
from pathlib import Path

from .server import mcp

_DOCS_DIR = Path(__file__).resolve().parent.parent.parent / "docs"


# ---------------------------------------------------------------------------
# Intent → endpoint matching
# ---------------------------------------------------------------------------

# Keyword map: maps user intent keywords to endpoint paths.
# Each keyword adds a "relevance score" to the matching endpoints.
_INTENT_KEYWORDS: dict[str, list[str]] = {
    # Enrichment — single row
    "enrich": ["/api/enrichment/enrich"],
    "email": ["/api/enrichment/enrich"],
    "contact": ["/api/enrichment/enrich"],
    "decision maker": ["/api/enrichment/enrich"],
    "domain": ["/api/enrichment/enrich", "/api/enrichment/flows/domain-enrich"],
    "linkedin": ["/api/enrichment/enrich", "/api/enrichment/by-linkedin-v2"],
    "person": ["/api/enrichment/enrich"],

    # Enrichment — bulk/CSV
    "bulk": ["/api/enrichment/flows/domain-enrich", "/api/enrichment/by-linkedin-v2"],
    "csv": ["/api/enrichment/upload", "/api/enrichment/flows/domain-enrich"],
    "upload": ["/api/enrichment/upload"],
    "batch": ["/api/enrichment/flows/domain-enrich"],
    "list": ["/api/enrichment/flows/domain-enrich"],

    # Phone enrichment
    "phone": ["/api/phone-enrichment/jobs"],
    "call": ["/api/phone-enrichment/jobs"],
    "number": ["/api/phone-enrichment/jobs"],

    # Scraper
    "scrape": ["/api/scraper/jobs"],
    "business": ["/api/scraper/jobs"],
    "google maps": ["/api/scraper/jobs"],
    "places": ["/api/scraper/jobs"],
    "restaurant": ["/api/scraper/jobs"],
    "store": ["/api/scraper/jobs"],

    # Job management
    "job status": ["/api/enrichment/jobs/{job_id}", "/api/scraper/jobs/{job_id}"],
    "download": ["/api/enrichment/jobs/{job_id}/download", "/api/scraper/jobs/{job_id}/download"],
    "cancel": ["/api/enrichment/jobs/{job_id}/cancel", "/api/scraper/jobs/{job_id}/cancel"],
    "resume": ["/api/scraper/jobs/{job_id}/resume"],
    "restart": ["/api/enrichment/jobs/{job_id}/restart", "/api/scraper/jobs/{job_id}/restart"],

    # Auth
    "auth": ["/api/auth/login", "/api/auth/me"],
    "login": ["/api/auth/login"],
    "api key": ["/api/api-keys"],
    "token": ["/api/auth/login", "/api/auth/refresh"],
    "quota": ["/api/quota"],

    # Provider selection
    "provider": ["/api/enrichment/providers"],
    "cascade": ["/api/enrichment/providers"],
    "selected_providers": ["/api/enrichment/enrich"],
    "force_provider": ["/api/enrichment/enrich"],

    # Company search
    "search company": ["/api/enrichment/search/companies"],
    "company search": ["/api/enrichment/search/companies"],

    # People search / Find People (lead-universe filtering)
    "find people": ["/api/enrichment/search/employees"],
    "people search": ["/api/enrichment/search/employees"],
    "search people": ["/api/enrichment/search/employees"],
    "search employees": ["/api/enrichment/search/employees"],
    "leads by": ["/api/enrichment/search/employees"],
    "universe": ["/api/enrichment/search/employees"],
    "saas leads": ["/api/enrichment/search/employees"],
    "b2b leads": ["/api/enrichment/search/employees"],
    "local leads": ["/api/enrichment/search/employees"],
    "ecommerce leads": ["/api/enrichment/search/employees"],
}


def _score_endpoints(intent: str) -> list[tuple[str, int]]:
    """Score each endpoint based on keyword matches. Returns sorted (path, score)."""
    intent_lower = intent.lower()
    scores: dict[str, int] = {}

    for keyword, paths in _INTENT_KEYWORDS.items():
        if keyword in intent_lower:
            for path in paths:
                scores[path] = scores.get(path, 0) + 1

    # Also do fuzzy matching against the OpenAPI path + summary
    from main import app
    spec = app.openapi()
    for path, methods in spec.get("paths", {}).items():
        for method, details in methods.items():
            if method.startswith("x-"):
                continue
            summary = (details.get("summary") or "").lower()
            desc = (details.get("description") or "").lower()
            combined = f"{method} {path} {summary} {desc}"
            for word in intent_lower.split():
                if len(word) > 2 and word in combined:
                    key = f"{method.upper()} {path}"
                    scores[key] = scores.get(key, 0) + 1

    return sorted(scores.items(), key=lambda x: -x[1])


@mcp.tool()
def find_endpoint_for_intent(intent: str) -> str:
    """Find the right API endpoint(s) for a given goal.

    Describe what you want to accomplish in plain English. This tool
    searches all endpoints and returns the best matches with their
    documentation.

    Examples:
        - "enrich a domain with decision makers"
        - "find phone numbers for LinkedIn profiles"
        - "scrape coffee shops in New York"
        - "check my job status"
        - "create an API key"
        - "limit which providers run"

    Args:
        intent: What you want to do, in plain English.
    """
    from main import app

    scored = _score_endpoints(intent)
    if not scored:
        return (
            f"No endpoints matched '{intent}'. Try different keywords.\n\n"
            "Use the `openapi://endpoints` resource to see all available endpoints."
        )

    spec = app.openapi()
    lines = [f"# Best matches for: \"{intent}\"", ""]
    for key, score in scored[:5]:
        # Parse "METHOD /path" format or just "/path"
        parts = key.split(" ", 1)
        if len(parts) == 2:
            method, path = parts
        else:
            method, path = "GET", key

        path_data = spec.get("paths", {}).get(path, {})
        details = path_data.get(method.lower(), {})
        summary = details.get("summary", "")
        tags = details.get("tags", [])

        lines.append(f"### {method} `{path}`")
        lines.append(f"**{summary}**")
        if tags:
            lines.append(f"_Tags: {', '.join(tags)}_")
        lines.append(f"_Relevance score: {score}_")
        lines.append("")

    if len(scored) > 5:
        lines.append(f"_...and {len(scored) - 5} more matches (lower relevance)_")

    return "\n".join(lines)


@mcp.tool()
def get_endpoint_details(method: str, path: str) -> str:
    """Get full details for a specific endpoint: parameters, response, errors.

    Returns the complete OpenAPI operation object for the given endpoint,
    formatted as readable markdown. Includes:
    - All parameters (query, path, header, body)
    - Request body schema
    - Response shapes
    - Error codes

    Args:
        method: HTTP method (GET, POST, PUT, DELETE).
        path: Endpoint path (e.g., "/api/enrichment/enrich").
    """
    from main import app

    spec = app.openapi()
    method_lower = method.lower()
    path_data = spec.get("paths", {}).get(path, {})

    if method_lower not in path_data:
        available = ", ".join(path_data.keys()) if path_data else "none"
        return (
            f"Endpoint {method.upper()} {path} not found.\n\n"
            f"Available methods for this path: {available}\n\n"
            f"Use `find_endpoint_for_intent` to search by goal."
        )

    op = path_data[method_lower]
    lines = [f"# {method.upper()} {path}", ""]

    if op.get("summary"):
        lines.append(f"**{op['summary']}**")
        lines.append("")
    if op.get("description"):
        lines.append(op["description"])
        lines.append("")

    # Parameters
    params = op.get("parameters", [])
    if params:
        lines.append("## Parameters")
        lines.append("")
        lines.append("| Name | In | Type | Required | Description |")
        lines.append("|------|-----|------|----------|-------------|")
        for p in params:
            name = p.get("name", "")
            loc = p.get("in", "")
            req = "✅" if p.get("required") else ""
            ptype = p.get("schema", {}).get("type", "")
            desc = (p.get("description") or "").replace("\n", " ")[:80]
            lines.append(f"| `{name}` | {loc} | {ptype} | {req} | {desc} |")
        lines.append("")

    # Request body
    body = op.get("requestBody")
    if body:
        lines.append("## Request Body")
        lines.append("")
        content = body.get("content", {})
        for ctype, cdata in content.items():
            lines.append(f"**Content-Type:** `{ctype}`")
            schema_ref = cdata.get("schema", {}).get("$ref", "")
            if schema_ref:
                model_name = schema_ref.split("/")[-1]
                lines.append(f"Schema: `{model_name}` — use `schemas://{model_name}` for full details")
            lines.append("")

    # Responses
    responses = op.get("responses", {})
    if responses:
        lines.append("## Responses")
        lines.append("")
        for code, resp in responses.items():
            desc = resp.get("description", "")
            lines.append(f"- **{code}:** {desc}")

    return "\n".join(lines)


@mcp.tool()
def explain_response_field(field_name: str) -> str:
    """Explain what a response field means and which providers populate it.

    Useful when you see a field in an API response and want to understand
    its meaning, valid values, and data source.

    Common fields:
    - ``email_source`` — which provider found the email
    - ``data_sources`` — which provider was used for each data type
    - ``sync_to_contacts_db`` — write-back status
    - ``row_status`` — per-row processing result
    - ``mode`` — enrichment mode detected from inputs

    Args:
        field_name: The field name from the API response (e.g., "email_source").
    """
    field_lower = field_name.lower()

    EXPLANATIONS = {
        "email_source": (
            "**`email_source`** tells you which provider in the cascade found the email.\n\n"
            "Valid values:\n"
            "- `contacts_db_email`, `contacts_db_name`, `contacts_db_linkedin` — found in internal Contacts DB (free)\n"
            "- `blitz_email`, `blitz_linkedin` — found via Blitz API (paid)\n"
            "- `getleads_email` — found via GetLeads (paid)\n"
            "- `smartprospect_email` — found via SmartProspect (paid)\n"
            "- `wizleads_email` — found via WizLeads (paid)\n"
            "- `better_enrich`, `better_enrich_company_email` — found via BetterEnrich (paid)\n"
            "- `not_found` — cascade ran but no provider returned an email\n"
            "- empty string — row never entered the cascade (no LinkedIn URL found)"
        ),
        "data_sources": (
            "**`data_sources`** is an object with three keys showing the source for each data type:\n\n"
            "- `company_linkedin`: where the company LinkedIn URL came from (`contacts_db`, `blitz`, `not_found`)\n"
            "- `contacts`: where the decision-maker contacts came from\n"
            "- `emails`: where the email addresses came from\n\n"
            "Each value is either a provider name or `not_found`."
        ),
        "sync_to_contacts_db": (
            "**`sync_to_contacts_db`** shows the status of automatic write-back to the internal Contacts DB.\n\n"
            "- `status`: `success`, `no_contacts_to_sync`, `failed`, `partial`\n"
            "- `records_synced`: number of contacts written\n"
            "- `records_skipped`: number skipped (duplicates, invalid)\n"
            "- `records_failed`: number that failed\n\n"
            "Write-back happens automatically — you never need to manually sync."
        ),
        "row_status": (
            "**`row_status`** is the per-row processing result in bulk CSV jobs.\n\n"
            "- `enriched`: email found successfully\n"
            "- `no_linkedin`: no company LinkedIn URL found (cascade didn't run)\n"
            "- `no_contacts`: LinkedIn found but no decision makers\n"
            "- `not_found`: cascade ran but no provider returned an email\n"
            "- `error`: processing error\n"
            "- `skipped`: row skipped (empty domain, etc.)"
        ),
        "mode": (
            "**`mode`** is the enrichment mode auto-detected from inputs:\n\n"
            "- `domain_only`: only domain provided → get all decision makers\n"
            "- `linkedin_only`: only LinkedIn URL (no domain) → enrich specific person\n"
            "- `company_linkedin_only`: only company LinkedIn URL → decision makers via company URL\n"
            "- `enhanced`: domain + person info → company contacts + specific person"
        ),
        "selected_providers": (
            "**`selected_providers`** is the user-specified allowlist of providers.\n\n"
            "When set, the cascade only uses providers in this list. "
            "`contacts_db` is always allowed (mandatory first step).\n\n"
            "Mutually exclusive with `force_provider`. Cannot be empty."
        ),
    }

    if field_lower in EXPLANATIONS:
        return EXPLANATIONS[field_lower]

    # Try partial match
    for key, explanation in EXPLANATIONS.items():
        if field_lower in key or key in field_lower:
            return explanation

    return (
        f"No specific explanation for `{field_name}`.\n\n"
        "Try these common fields: `email_source`, `data_sources`, "
        "`sync_to_contacts_db`, `row_status`, `mode`, `selected_providers`.\n\n"
        "Or use `schemas://UnifiedEnrichRequest` to see all request fields, "
        "or read the `docs://api-reference` resource for the full response shape."
    )


@mcp.tool()
def explain_error_code(error_message: str) -> str:
    """Get troubleshooting steps for an API error.

    Pass the error message or HTTP status code you received, and this tool
    returns the likely cause and recommended fix.

    Args:
        error_message: The error detail string or HTTP status code.
                       Examples: "401", "Invalid token", "selected_providers
                       must be a non-empty list", "database is briefly busy"
    """
    msg_lower = error_message.lower().strip()

    # 401
    if "401" in msg_lower or "unauthorized" in msg_lower or "invalid token" in msg_lower:
        return (
            "## HTTP 401 — Authentication Failed\n\n"
            "**Likely causes:**\n"
            "- Missing or expired JWT token\n"
            "- Invalid API key\n"
            "- Using API key on an endpoint that requires JWT (bulk/flow endpoints)\n\n"
            "**Fixes:**\n"
            "- For single-row enrichment: use `X-API-Key: lgp_...` header\n"
            "- For bulk/CSV upload endpoints: use `Authorization: Bearer <jwt>` (JWT only)\n"
            "- Refresh expired JWT: `POST /api/auth/refresh`\n"
            "- Generate new API key: Account → API Keys in the UI"
        )

    # 403
    if "403" in msg_lower or "forbidden" in msg_lower or "access denied" in msg_lower:
        return (
            "## HTTP 403 — Forbidden\n\n"
            "**Likely causes:**\n"
            "- Trying to access another user's job\n"
            "- Non-admin accessing admin-only endpoints\n\n"
            "**Fix:** Ensure you own the resource or have admin privileges."
        )

    # 404
    if "404" in msg_lower or "not found" in msg_lower:
        return (
            "## HTTP 404 — Not Found\n\n"
            "**Likely causes:**\n"
            "- Wrong job_id or upload_id\n"
            "- Job output file deleted (old jobs)\n"
            "- Typo in endpoint path\n\n"
            "**Fix:** Verify the ID with `GET /api/enrichment/jobs` or "
            "`GET /api/scraper/jobs`."
        )

    # 429
    if "429" in msg_lower or "quota" in msg_lower or "rate limit" in msg_lower:
        return (
            "## HTTP 429 — Quota Exceeded\n\n"
            "**Cause:** You've exceeded the 50,000 daily request limit (non-admin).\n\n"
            "**Fixes:**\n"
            "- Wait until midnight UTC (quota resets daily)\n"
            "- Check current usage: `GET /api/quota`\n"
            "- Contact admin for quota increase\n"
            "- Use `selected_providers: [\"contacts_db\"]` to reduce API calls"
        )

    # selected_providers validation errors
    if "selected_providers" in msg_lower:
        if "mutually exclusive" in msg_lower:
            return (
                "## selected_providers + force_provider Conflict\n\n"
                "You cannot set both `force_provider` AND `selected_providers`.\n\n"
                "**Fix:** Pick one:\n"
                "- `force_provider: \"blitz\"` — force a single provider\n"
                "- `selected_providers: [\"contacts_db\", \"smartprospect\"]` — allowlist a subset"
            )
        if "non-empty" in msg_lower or "empty" in msg_lower:
            return (
                "## Empty selected_providers List\n\n"
                "`selected_providers` must contain at least one provider.\n\n"
                "**Valid values:** `contacts_db`, `blitz`, `getleads`, `smartprospect`, "
                "`wizleads`, `better_enrich`"
            )
        if "invalid" in msg_lower:
            return (
                "## Unknown Provider Name\n\n"
                "One of your `selected_providers` values is not a valid provider name.\n\n"
                "**Valid values:** `contacts_db`, `blitz`, `getleads`, `smartprospect`, "
                "`wizleads`, `better_enrich`\n\n"
                "Check for typos and case sensitivity (all lowercase)."
            )

    # 503
    if "503" in msg_lower or "busy" in msg_lower or "database" in msg_lower:
        return (
            "## HTTP 503 — Database Busy\n\n"
            "**Cause:** SQLite database is briefly locked (high concurrency).\n\n"
            "**Fix:** Retry in 3 seconds. The response includes a "
            "`Retry-After: 3` header. This is transient and safe to retry."
        )

    # 0 emails
    if "0 email" in msg_lower or "no email" in msg_lower or "zero email" in msg_lower:
        return (
            "## Zero Emails from a Job\n\n"
            "**If the job status is `done` with 0 emails:**\n"
            "1. Check if `processed` is also 0 — if so, the cascade never ran (bug)\n"
            "2. Check `used_providers` — if only `contacts_db` is listed, "
            "other providers may have been skipped\n"
            "3. Try `force_provider: \"blitz\"` or `force_provider: \"smartprospect\"` "
            "to test individual providers\n"
            "4. Check service logs: `journalctl -u lead-generation-platform.service`\n\n"
            "**If the job shows the yellow ⚠ warning banner:**\n"
            "This is a known safety feature. Click Retry to re-run."
        )

    return (
        f"No specific troubleshooting guide for: \"{error_message}\"\n\n"
        "Common error codes: 401 (auth), 403 (forbidden), 404 (not found), "
        "429 (quota), 503 (database busy).\n\n"
        "Read `docs://api-reference` → Error Code Reference section for the full list."
    )


@mcp.tool()
def list_current_providers() -> str:
    """List the current provider cascade with live enabled/disabled status.

    Returns the cascade order, which providers are enabled, their rate
    limits, and which are free vs paid. This is live data — reflects
    the current running state, not cached documentation.
    """
    from enrichment import providers as prov

    cascade = [
        (1, "contacts_db", "75 RPS", "Free"),
        (2, "blitz", "25 RPS", "Paid"),
        (3, "getleads", "100/min*", "Paid"),
        (4, "smartprospect", "30 RPS", "Paid"),
        (5, "wizleads", "10 RPS", "Paid"),
        (6, "better_enrich", "10 RPS", "Paid"),
    ]

    lines = [
        "# Current Provider Cascade (Live)",
        "",
        "| # | Provider | Status | Rate | Cost |",
        "|---|----------|--------|------|------|",
    ]
    for pos, name, rps, cost in cascade:
        enabled = "✅" if prov.is_provider_enabled(name) else "❌"
        lines.append(f"| {pos} | `{name}` | {enabled} | {rps} | {cost} |")

    lines.extend([
        "",
        "The cascade stops at the first provider that returns a valid email.",
        "Use `selected_providers` to restrict the cascade to a subset.",
        "`contacts_db` is always allowed even if not listed.",
    ])
    return "\n".join(lines)


@mcp.tool()
def get_quota_guide() -> str:
    """Explain the daily quota model and current limits.

    Returns information about:
    - Daily request limit (50K non-admin, unlimited admin)
    - Reset schedule (midnight UTC)
    - How to check remaining quota
    - Which endpoints count against quota
    """
    return (
        "# Daily Quota Guide\n\n"
        "## Limits\n\n"
        "| User type | Daily limit | Reset |\n"
        "|-----------|-------------|-------|\n"
        "| Non-admin | 50,000 requests/day | Midnight UTC (calendar day) |\n"
        "| Admin | Unlimited | — |\n\n"
        "## Check your quota\n\n"
        "```bash\n"
        "curl https://listbuilding.eagleinfoservice.com/api/quota \\\n"
        "  -H \"Authorization: Bearer YOUR_JWT\"\n"
        "```\n\n"
        "Returns:\n"
        "```json\n"
        "{\"limit\": 50000, \"used\": 12345, \"remaining\": 37655, "
        "\"resets_at\": \"2026-07-17T00:00:00Z\", \"is_admin\": false}\n"
        "```\n\n"
        "## What counts against quota\n\n"
        "- Scraper job creation (heaviest — each task costs ~10 units)\n"
        "- Enrichment job creation\n"
        "- Single-row enrichment via `/api/enrichment/enrich`\n\n"
        "## How to reduce quota usage\n\n"
        "- Use `selected_providers: [\"contacts_db\"]` for free-tier-only enrichment\n"
        "- Use cached scraper results (90-day cache, no quota cost)\n"
        "- Deduplicate input CSVs before uploading (built-in feature)"
    )


@mcp.tool()
def compare_enrichment_modes() -> str:
    """Explain the difference between enrichment input modes.

    The API auto-detects the mode based on which inputs you provide.
    This tool explains each mode and when to use it.
    """
    return (
        "# Enrichment Input Modes\n\n"
        "The API auto-detects the mode from your inputs:\n\n"
        "| Mode | Trigger | Behavior |\n"
        "|------|---------|----------|\n"
        "| `domain_only` | Only `domain` | Get ALL decision makers (cascade) |\n"
        "| `linkedin_only` | Only `linkedin_url` (no domain) | Enrich specific person |\n"
        "| `company_linkedin_only` | Only `company_linkedin_url` | Decision makers via company URL |\n"
        "| `enhanced` | `domain` + (`full_name` or `linkedin_url`) | Company contacts + specific person |\n\n"
        "## When to use each\n\n"
        "- **domain_only**: Best for discovering who works at a company\n"
        "- **linkedin_only**: Best when you have a person's LinkedIn but don't know their company\n"
        "- **enhanced**: Best for finding a SPECIFIC person at a known company\n\n"
        "## Provider cascade per mode\n\n"
        "- `domain_only` and `enhanced`: Full cascade "
        "(Contacts DB → Blitz → GetLeads → SmartProspect → WizLeads → BetterEnrich)\n"
        "- `linkedin_only`: LinkedIn cascade "
        "(Contacts DB → Blitz → GetLeads from-linkedin fallback; "
        "SmartProspect/WizLeads need name+domain so they are not in this arm)\n\n"
        "## Important\n\n"
        "In `enhanced` mode, the API looks for the SPECIFIC person only. "
        "If not found, it returns 0 contacts — it does NOT fall back to "
        "the domain cascade."
    )


@mcp.tool()
def validate_request(endpoint: str, payload_json: str) -> str:
    """Validate a JSON request payload against the endpoint's schema.

    Checks whether your JSON payload matches the expected schema for the
    given endpoint. Returns a list of any issues found (missing required
    fields, wrong types, unknown fields).

    Args:
        endpoint: The API path (e.g., "/api/enrichment/enrich").
        payload_json: Your request body as a JSON string.
    """
    import json as _json

    try:
        payload = _json.loads(payload_json)
    except _json.JSONDecodeError as e:
        return f"❌ Invalid JSON: {e}"

    from enrichment import routes as enr

    schema_map = {
        "/api/enrichment/enrich": enr.UnifiedEnrichRequest,
        "/api/enrichment/jobs": enr.StartJobRequest,
        "/api/enrichment/flows/domain-enrich": enr.ProviderToggleRequest,
    }

    model = schema_map.get(endpoint)
    if not model:
        return (
            f"No schema mapping for `{endpoint}`.\n\n"
            f"Supported endpoints: {', '.join(schema_map.keys())}"
        )

    fields = model.model_json_schema().get("properties", {})
    required = set(model.model_json_schema().get("required", []))

    issues: list[str] = []

    # Check required fields
    for req in required:
        if req not in payload:
            issues.append(f"❌ Missing required field: `{req}`")

    # Check for unknown fields
    for key in payload:
        if key not in fields:
            issues.append(f"⚠️ Unknown field: `{key}` (not in schema)")

    # Check field types
    for key, value in payload.items():
        if key not in fields:
            continue
        expected_type = fields[key].get("type", "")
        if expected_type == "string" and not isinstance(value, str):
            if value is not None:
                issues.append(f"❌ `{key}` should be string, got {type(value).__name__}")
        elif expected_type == "integer" and not isinstance(value, int):
            if value is not None:
                issues.append(f"❌ `{key}` should be integer, got {type(value).__name__}")
        elif expected_type == "array" and not isinstance(value, list):
            if value is not None:
                issues.append(f"❌ `{key}` should be array, got {type(value).__name__}")

    if not issues:
        return f"✅ Payload for `{endpoint}` looks valid.\n\nAll fields match the schema."

    lines = [f"# Validation Issues for `{endpoint}`", ""]
    for issue in issues:
        lines.append(issue)
    lines.append("")
    lines.append(f"**Schema:** Use `schemas://{model.__name__}` for full field reference.")
    return "\n".join(lines)
