# ListBuilding Platform — Full API Reference

- **Updated on:** 2026-07-16
- **Updated by:** @Arjav Jain
- **Scope:** Every user-facing endpoint on https://listbuilding.eagleinfoservice.com/
- **Base URL:** `https://listbuilding.eagleinfoservice.com`

---

## Overview

The ListBuilding platform exposes four user-facing API families:

1. **Enrichment API** — find decision-maker emails for companies and people
2. **Phone Enrichment API** — find phone numbers for LinkedIn profiles
3. **Google Maps Scraper API** — scrape business listings by country and query
4. **Auth + API Keys API** — manage login tokens and long-lived API keys

All endpoints accept and return JSON (with the exception of CSV upload/download endpoints, which use multipart and file responses).

Public health check: `GET /api/health` (no auth) returns `{"status": "ok"}`.

---

## Authentication

Two authentication methods are supported. **They are not interchangeable across all endpoints** — see the matrix below.

### Option A: API Key (recommended for integrations)

API keys are long-lived tokens formatted as `lgp_<43 characters>`. They do not expire. Generate one in the app under **Account → API Keys**, or via the API:

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/api-keys \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-integration-key"}'
```

Use the returned key in one of two ways:

```bash
# Preferred
-H "X-API-Key: lgp_YOUR_API_KEY"

# Also accepted (treated as API key, not JWT)
-H "Authorization: Bearer lgp_YOUR_API_KEY"
```

Revoke a key with `DELETE /api/api-keys/{key_id}` (requires JWT, not another API key).

### Option B: JWT Bearer token

```bash
# Login to get a token (7-day expiry)
curl -X POST https://listbuilding.eagleinfoservice.com/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "your@email.com", "password": "yourpassword"}'
# → {"token": "...", "user_id": "...", "email": "...", "is_admin": false}

# Refresh an existing token (resets the 7-day clock)
curl -X POST https://listbuilding.eagleinfoservice.com/api/auth/refresh \
  -H "Authorization: Bearer YOUR_CURRENT_JWT"

# Use the token
-H "Authorization: Bearer YOUR_JWT_TOKEN"
```

JWT expiry: **7 days**.

### Auth matrix — which method works where

| Endpoint category | JWT Bearer | API Key |
| --- | :---: | :---: |
| `POST /api/enrichment/enrich` (single row) | ✅ | ✅ |
| `GET /api/enrichment/enrich` (single row) | ✅ | ✅ |
| `GET /api/enrichment/enrich/{domain}` (legacy single row) | ✅ | ✅ |
| `GET /api/enrichment/providers` | ✅ | ✅ |
| `GET /api/enrichment/stats/sources` | ✅ | ✅ |
| `GET /api/auth/me` | ✅ | ✅ |
| Scraper cache + download + resume endpoints | ✅ | ✅ |
| **`POST /api/enrichment/upload` (CSV upload)** | ✅ | ❌ JWT only |
| **All `/api/enrichment/flows/*` endpoints** | ✅ | ❌ JWT only |
| **All `/api/enrichment/by-*` endpoints** | ✅ | ❌ JWT only |
| **All `/api/enrichment/jobs/*` endpoints** | ✅ | ❌ JWT only |
| **All `/api/phone-enrichment/*` endpoints** | ✅ | ❌ JWT only |
| **`POST /api/scraper/jobs` (scraper job creation)** | ✅ | ❌ JWT only |
| **`/api/api-keys/*` (key management)** | ✅ | ❌ JWT only |

**Rule of thumb:** API keys work for read-only / single-row lookups (e.g., from Clay HTTP cells). Anything that creates or modifies a job requires JWT.

### Self-registration

There is no public signup endpoint. New users are created by an administrator via the in-app UI. If you need an account, contact the platform owner.

---

## Section A: Enrichment API

### A.1 Single-row enrichment: `POST /api/enrichment/enrich`

The primary endpoint for finding contact data for one company or person.

#### Request body parameters

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `domain` | string | Yes* | — | Company domain (e.g., `google.com`). Must contain a `.`. |
| `linkedin_url` | string | Yes* | — | Personal LinkedIn URL (e.g., `https://linkedin.com/in/johndoe`). Auto-rerouted to `company_linkedin_url` if a `/company/` URL is passed. |
| `company_linkedin_url` | string | No | — | Company LinkedIn URL (e.g., `https://linkedin.com/company/acme`). |
| `full_name` | string | No | — | Full name of target person. |
| `first_name` | string | No | — | First name (alternative to `full_name`). |
| `last_name` | string | No | — | Last name. |
| `phone` | string | No | — | Phone number hint. |
| `company_name` | string | No | — | Company name hint. |
| `existing_email` | string | No | — | Existing email hint (skips email resolution). |
| `max_results` | integer | No | 5 | Max contacts to return (1–10). |
| `titles` | string | No | — | Comma-separated titles filter (e.g., `"CEO,CTO,VP"`). Max 50. |
| `strict_titles` | boolean | No | true | Strict local title gate: drop contacts that don't match `titles`/`cascade` after provider matching (see A.6). `false` = keep provider fuzzy matches. |
| `cascade` | array of objects | No | — | Advanced: list of title-filter dicts (see A.7). |
| `force_provider` | string | No | — | Force a single provider. One of: `contacts_db`, `blitz`, `smartprospect`, `wizleads`, `better_enrich`. Mutually exclusive with `selected_providers`. |
| `selected_providers` | array of strings | No | — | Restrict cascade to a subset (see A.6). Mutually exclusive with `force_provider`. |

\*At least one of `domain`, `linkedin_url`, or `company_linkedin_url` is required.

#### Input modes (auto-detected)

| Input | Mode | Behavior |
| --- | --- | --- |
| `domain` only | `domain_only` | All decision makers (cascade) |
| `linkedin_url` only (no domain) | `linkedin_only` | Specific person via cascade |
| `company_linkedin_url` only (no domain) | `company_linkedin_only` | Decision makers via company LinkedIn URL |
| `domain` + (`full_name` or `linkedin_url`) | `enhanced` | Company contacts + specific person |

#### Example: domain only

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "X-API-Key: lgp_YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"domain": "google.com"}'
```

#### Example: domain + full name

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "X-API-Key: lgp_YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"domain": "google.com", "full_name": "John Doe"}'
```

#### Example: LinkedIn URL only

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "X-API-Key: lgp_YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"linkedin_url": "https://linkedin.com/in/johndoe"}'
```

#### Response shape

```json
{
  "domain": "google.com",
  "mode": "domain_only",
  "company_linkedin_url": "https://linkedin.com/company/google",
  "contacts": [
    {
      "full_name": "John Doe",
      "first_name": "John",
      "last_name": "Doe",
      "title": "VP of Sales",
      "email": "john@google.com",
      "linkedin_url": "https://linkedin.com/in/johndoe",
      "headline": "VP of Sales at Google",
      "location_city": "San Francisco",
      "location_country": "US",
      "icp_tier": 1,
      "email_source": "blitz_email"
    }
  ],
  "contact_count": 1,
  "data_sources": {
    "company_linkedin": "contacts_db",
    "contacts": "blitz",
    "emails": "blitz_email"
  },
  "sync_to_contacts_db": {
    "status": "success",
    "records_synced": 1,
    "records_skipped": 0,
    "records_failed": 0
  },
  "seg_classification": "direct_google",
  "seg_provider": "Google"
}
```

**SEG fields (flag-gated, added 2026-08-25).** When the `ENABLE_SEG_CLASSIFICATION` env flag is on, the response carries two additional **top-level** keys classifying the domain's mail server (never inside `contacts[*]`):

| Field | Type | Values |
| --- | --- | --- |
| `seg_classification` | string | `external_seg` \| `direct_google` \| `direct_microsoft` \| `other_or_unknown` \| `no_email`, or `""` if the domain could not be classified |
| `seg_provider` | string | Named provider, e.g. `SEG: Proofpoint`, `SEG: Mimecast`, `Microsoft`, `Google`, `Other / Unknown`, `No Email (no MX)`, `Invalid Domain`, `Free Webmail (gmail.com)` |

When the flag is **off** the keys are **absent** (not null). Guidance for senders: `external_seg` = behind a secure email gateway → warm/deprioritize; `no_email` = undeliverable → exclude; `direct_google`/`direct_microsoft` = standard sending rules.

#### Email source values

The `email_source` field on each contact tells you which provider found the email:

| `email_source` | Provider |
| --- | --- |
| `contacts_db_email`, `contacts_db_name`, `contacts_db_linkedin`, `contacts_db_contacts` | Contacts DB (free) |
| `blitz_email`, `blitz_linkedin` | Blitz |
| `smartprospect_email` | SmartProspect |
| `wizleads_email` | WizLeads |
| `better_enrich`, `better_enrich_company_email` | BetterEnrich |
| `not_found` | No provider found an email |

### A.2 Single-row enrichment via GET

Identical behavior to POST, but parameters are passed as query strings.

```bash
curl -G https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "X-API-Key: lgp_YOUR_KEY" \
  --data-urlencode "domain=google.com" \
  --data-urlencode "titles=CEO,CTO"
```

For `selected_providers` on GET, pass it as a comma-separated string:
```
?selected_providers=contacts_db,smartprospect
```

Same response shape as POST, including the flag-gated top-level `seg_classification` / `seg_provider` keys (see A.1).

### A.3 Legacy: `GET /api/enrichment/enrich/{domain}`

Older path-style single-domain lookup. Accepts `max_results`, `cascade_json`, `force_provider` as query params. Prefer the POST/GET form above for new integrations. Also returns the flag-gated top-level `seg_classification` / `seg_provider` keys when `ENABLE_SEG_CLASSIFICATION` is on (same semantics as A.1).

**By-company Contacts DB lookup (added 2026-07-21).** When the `ENABLE_COMPANY_LOOKUP` env flag is `true` (set via the `lead-generation-platform.service` systemd drop-in), this endpoint additionally returns **every person filed under the company in the Contacts DB** — not only those whose email matches the lookup domain — with emails **preserved as stored** (no mailtester re-validation, so `.mil`/`.gov` emails are no longer dropped as "No MX"). These rows carry `email_source: "contacts_db"`, `validation_status: "preserved"`.

- **Additive only** — the normal provider cascade (Contacts DB → Blitz → SmartProspect → WizLeads → BetterEnrich) runs unchanged; by-company rows are merged in afterward, so the contact count never drops below the cascade-only result.
- **Skipped when `force_provider` is set** — forcing one provider returns only that provider's path.
- A same-name cascade contact is replaced by the by-company version **only if its email was stripped/empty**; a validated cascade email is never overwritten.
- Backed by a new Contacts API endpoint: `GET /v1/company/persons/by-domain?domain=&limit=&source=` (optional `source`, e.g. `outscraper`, filters by provenance).
- **Disable:** remove (or set `=false` in) `/etc/systemd/system/lead-generation-platform.service.d/enable-company-lookup.conf` and restart the service.

> The bulk list-building flow (`POST /api/enrichment/flows/domain-enrich`) **also** uses this by-company path (Phase 1B, 2026-07-21): when the flag is on, the output CSV additionally includes every Contacts DB person filed under each input company, tagged `dm_email_source="contacts_db_by_company"` (emails preserved, additive to the normal cascade results).

**Phase 2 (2026-07-22) — `source` filter:** all enrichment entry points (`POST /enrich`, `GET /enrich/{domain}`, `POST /flows/domain-enrich`) accept an optional `source` param. Set `source="outscraper"` to narrow the by-company lookup to outscraper-sourced (Google Maps) contacts only; omit/`null` = all sources (default, no change). Backed by `company_persons_by_domain?source=...`. The UI exposes this as the "Outscraper (Google Maps)" checkbox in the Data Sources block. (Known gap: `GET /enrich` query form accepts `source` but has no by-company merge, so it's a no-op there — Phase 1c.)

**Website-only mode (2026-08-27) — `website_only` flag on `POST /flows/domain-enrich`:** when `true`, the job reads **exclusively** from the `website_scrape` cohort that the nightly sync imports from webscraper.eagleinfoservice.com into the Contacts DB. Zero paid providers run (no Blitz/GetLeads/SmartProspect/WizLeads/BetterEnrich), no company-email fallback, no mailtester — emails are returned as stored, tagged `dm_email_source="contacts_db_by_company"`. Title filters still apply. `providers`, `source`, and company-LinkedIn columns are ignored in this mode (company-URL rows fall back to the domain-keyed lookup). Data freshness is up to ~24h + one failed sync — query `GET /api/enrichment/website-scrape/status` for the as-of watermark, last-run telemetry, and a `synced_within_hours` flag (48h threshold). New website scraping is **not** triggered by this mode; submit new domains at webscraper.eagleinfoservice.com and they appear after the next nightly sync. The flag persists on the job row, so restarts/resumes keep the mode.

**Phase 3 (2026-07-22) — crash-safe jobs (`ENABLE_INCREMENTAL_PERSISTENCE`):** when this flag is on, a cancelled or crashed enrichment job persists its partial results — a partial CSV at `data/outputs/<job_id>.csv` plus a collector drain to the Contacts DB — and ends with `status="partial"` instead of losing all in-memory data. The partial CSV is downloadable via the usual `/jobs/{id}/download`. Default off; enabled in production via the `enable-incremental-persistence.conf` systemd drop-in.

### A.4 Provider cascade order

When resolving emails, the system queries providers in this order, stopping at the first one that returns a valid email:

| # | Provider | Rate Limit | Cost | Purpose |
| --- | --- | --- | --- | --- |
| 1 | Contacts DB | 75 RPS | Free | Internal database lookup |
| 2 | Blitz | 25 RPS | Paid | LinkedIn-based enrichment with title cascade |
| 3 | SmartProspect | 30 RPS | Paid | Self-verifying person-email finder |
| 4 | WizLeads | 10 RPS | Paid | Catch-all verified email |
| 5 | BetterEnrich | 10 RPS | Paid | Person + company email fallback |

> **SEG classification is not a cascade provider.** It is a side-channel DNS-over-HTTPS (MX) domain lookup that runs alongside the cascade; it does not count as a provider call for job counters and never affects which cascade step wins. Its DoH lookups do appear in `provider_call_log` under the `seg` provider key for observability.

### A.5 `selected_providers` (new 2026-07-13)

Restricts the cascade to a subset of providers.

**Valid names:** `contacts_db`, `blitz`, `smartprospect`, `wizleads`, `better_enrich`

**Rules:**
- `contacts_db` is always allowed even if not in your list (mandatory first step)
- Mutually exclusive with `force_provider` (HTTP 400 if both are set)
- Empty list rejected (HTTP 400)
- Unknown provider names rejected (HTTP 400)

**Example — free tier only:**
```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "X-API-Key: lgp_YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"domain": "acme.com", "selected_providers": ["contacts_db"]}'
```

**Example — Contacts DB + SmartProspect only:**
```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "X-API-Key: lgp_YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"domain": "acme.com", "selected_providers": ["contacts_db", "smartprospect"]}'
```

### A.6 Title filtering

#### Simple titles filter
```
?titles=CEO,CTO,VP
```
Converts to a single-tier cascade requesting only people with those titles.

#### Local strict-title gate (2026-08-26)
Provider-side title matching is fuzzy — e.g. with `include_headline_search: true`,
a "President" filter matched "Vice President of Product Management", "Chief Revenue
Officer", etc. (a production job returned 77% off-ICP contacts this way). The
platform now **re-applies your titles locally after every discovery step** (Internal
DB, Blitz waterfall, generic fallbacks) and drops non-matching contacts *before*
email resolution.

Gate semantics:
- A title matches when ALL its words match (order-insensitive, synonym-aware:
  `VP` = `Vice President`, `Founder` = `Co-Founder`, `CEO` = `Chief Executive Officer`…).
- Compound negations are respected: "President" does **not** match
  "Vice/Deputy/Past President…".
- Junior excludes (`assistant`, `intern`, `junior`, `associate`) are overridden by
  senior signals — "Associate Director" is kept, "Sales Associate" is dropped.
- Structured signals from the Internal DB (seniority, function) count as matches.
- The gate is **inert when no titles are supplied** (default cascade requests are
  never filtered by it).

To opt out (keep provider fuzzy matches for higher volume):
```json
{ "titles": "CEO,CTO", "strict_titles": false }
```
`strict_titles` (default `true`) is accepted on `/enrich`, `/flows/domain-enrich`,
and `/by-domains`. The opt-out is persisted with the job, so resume/restart keeps it.

#### Custom cascade (advanced)
```json
{
  "cascade": [
    {
      "include_title": ["CEO", "Founder", "Owner"],
      "exclude_title": ["assistant", "intern", "junior"],
      "location": ["WORLD"],
      "include_headline_search": false
    },
    {
      "include_title": ["VP", "Director"],
      "exclude_title": ["assistant"],
      "location": ["US"],
      "include_headline_search": true
    }
  ]
}
```

**Cascade properties:**
- `include_title`: Array of job titles to include
- `exclude_title`: Array of titles to exclude
- `location`: Geographic filter (e.g., `["WORLD"]`, `["US"]`, `["UK"]`)
- `include_headline_search`: Search LinkedIn headlines in addition to titles

#### Default cascade (no filter)
If no `titles` or `cascade` is provided, the default 3-tier cascade is used:
1. **Tier 1:** Owner, CEO, Founder, Co-Founder, President
2. **Tier 2:** CMO, VP Marketing, VP Sales, Chief Revenue Officer
3. **Tier 3:** Director of Marketing, Director of Sales, Head of Marketing, Head of Sales

Fetch the current default via `GET /api/enrichment/default-cascade` (no auth).

---

## Section B: Bulk / CSV Enrichment

All endpoints in this section require **JWT Bearer authentication** (API keys are not accepted).

> **Crash-safety model (2026-07-22+).** Enrichment jobs now write their output CSV **incrementally** (batch-by-batch as each row group finishes) and push contacts to the Contacts DB incrementally via the outbox. A cancel or worker crash mid-run therefore never loses already-completed rows — the partial CSV on disk and the drained Contacts DB rows both survive. Such jobs end with `status="partial"` (instead of `failed` with total data loss) and are fully resumable: `POST /jobs/{job_id}/restart` reads per-row checkpoints, skips completed rows, and carries the prior partial CSV forward so the resumed job produces one complete file (see B.11, B.13). Controlled by the `ENABLE_INCREMENTAL_PERSISTENCE` flag (on in production via the `enable-incremental-persistence.conf` systemd drop-in).

### B.1 Upload a CSV: `POST /api/enrichment/upload`

Step 1 of any bulk flow. Accepts a CSV file, returns an `upload_id` you use to start a job.

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/upload \
  -H "Authorization: Bearer YOUR_JWT" \
  -F "file=@domains.csv"
```

**Response:**
```json
{
  "upload_id": "8ec32b12-fb17-4a5a-b8d2-6142f59df480",
  "columns": ["domain", "company_name", "..."],
  "preview": [{}, {}, {}],
  "row_count": 4619,
  "filename": "8ec32b12-fb17-4a5a-b8d2-6142f59df480"
}
```

### B.2 Flow 1 — Domain CSV enrichment: `POST /api/enrichment/flows/domain-enrich`

The modern endpoint for bulk domain-CSV enrichment (used by the current UI).

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/flows/domain-enrich \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "upload_id": "8ec32b12-fb17-4a5a-b8d2-6142f59df480",
    "domain_col": "domain",
    "max_results": 5,
    "providers": ["contacts_db", "blitz", "smartprospect", "wizleads", "better_enrich"],
    "titles": "CEO,CTO",
    "strict_titles": true,
    "normalize_domains": true,
    "dedupe_by_domain": true
  }'
```

**Request body parameters:**

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `upload_id` | string | Yes | — | From B.1 upload response. |
| `domain_col` | string | Yes | — | Name of the domain column in your CSV. |
| `name_col` | string | No | — | Column with full names. |
| `first_name_col` | string | No | — | Column with first names. |
| `last_name_col` | string | No | — | Column with last names. |
| `linkedin_url_col` | string | No | — | Column with personal LinkedIn URLs. |
| `company_linkedin_col` | string | No | — | Column with company LinkedIn URLs. |
| `phone_col` | string | No | — | Column with phone numbers. |
| `company_name_col` | string | No | — | Column with company names. |
| `existing_email_col` | string | No | — | Column with existing emails. |
| `titles` | string | No | — | Comma-separated titles filter. |
| `strict_titles` | boolean | No | true | Strict local title gate: drop contacts that don't match `titles` after provider matching (see A.6). `false` = keep provider fuzzy matches (higher volume, lower precision). |
| `max_results` | integer | No | 5 | Max contacts per domain. |
| `providers` | array of strings | No | all enabled | Provider allowlist (same semantics as `selected_providers` on the single-row endpoint). |
| `normalize_domains` | boolean | No | true | Normalize raw URLs to bare domains. |
| `dedupe_by_domain` | boolean | No | true | Drop duplicate-domain rows before processing. |

**Response:**
```json
{
  "job_id": "114e9ec3-51b8-445e-8fbc-417cba1fc516",
  "total": 4619,
  "flow": "domain_enrichment",
  "deduped_count": 4619
}
```

### B.3 Flow 3 — LinkedIn CSV enrichment: `POST /api/enrichment/by-linkedin-v2`

Bulk enrichment from a CSV of LinkedIn URLs (personal and/or company).

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/by-linkedin-v2 \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "upload_id": "...",
    "personal_linkedin_col": "linkedin_url",
    "company_linkedin_col": "company_linkedin_url",
    "max_results": 5
  }'
```

At least one of `personal_linkedin_col` or `company_linkedin_col` is required.

### B.4 Flow 2 — Company search: `POST /api/enrichment/search/companies`

Search Blitz for companies matching criteria, returns matching companies (no contacts yet).

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/search/companies \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "acme",
    "industry": ["Software"],
    "employee_range": ["50-200"],
    "country": ["US"]
  }'
```

Use `GET /api/enrichment/search/options` to discover allowed values for each filter.

### B.5 Search options: `GET /api/enrichment/search/options`

Returns allowed values for company-search filters: `industries`, `employee_ranges`, `company_types`, `countries`, `job_levels`, `job_functions`, `sales_regions`.

### B.6 List jobs: `GET /api/enrichment/jobs`

Returns the caller's enrichment jobs (admin sees all). Limit 200.

```bash
curl https://listbuilding.eagleinfoservice.com/api/enrichment/jobs \
  -H "Authorization: Bearer YOUR_JWT"
```

### B.7 Job status: `GET /api/enrichment/jobs/{job_id}`

```bash
curl https://listbuilding.eagleinfoservice.com/api/enrichment/jobs/114e9ec3-... \
  -H "Authorization: Bearer YOUR_JWT"
```

**Response includes:** `status` (`queued` | `running` | `done` | `failed` | `cancelled` | `abandoned` | `partial`), `processed`, `total`, `emails_found`, `used_providers`, `selected_providers`, `error`.

### B.8 SSE progress stream: `GET /api/enrichment/jobs/{job_id}/stream`

Server-sent events stream of progress updates. Use to render live progress bars.

### B.9 Download final CSV: `GET /api/enrichment/jobs/{job_id}/download`

Returns the enriched CSV for a finished job. Works for `done`, `partial`, and `failed` jobs (for `failed`, only if a non-empty partial output exists on disk — otherwise returns HTTP 500 with the failure message). Returns HTTP 202 with `"Job not finished yet."` only when the job is still `queued` or `running`.

| Job status | Behavior |
| --- | --- |
| `done` | Full CSV, filename `enriched_<job_id>.csv` |
| `partial` | Partial CSV written before the cancel/crash, filename `partial_<name>_<job_id>.csv` |
| `failed` (with partial output on disk) | Partial CSV, served with a log warning |
| `failed` (no partial output) | HTTP 500 with the original failure message |
| `queued` / `running` | HTTP 202 `"Job not finished yet."` |

For live in-progress previews see **B.14 `/recover-partial`**; for chunked downloads of very large jobs see **B.16 `/shards`**.

**SEG columns (flag-gated, added 2026-08-25).** When `ENABLE_SEG_CLASSIFICATION` is on, every enrichment job CSV (Flow 1 `domain-enrich`, Flow 3 `by-linkedin-v2` and legacy `by-linkedin`, legacy `by-domains`) ends with two trailing columns:

| Column | Description |
| --- | --- |
| `seg_classification` | Domain's mail-server verdict — `external_seg`, `direct_google`, `direct_microsoft`, `other_or_unknown`, or `no_email` (blank while the flag is off) |
| `seg_provider` | Named provider behind the verdict, e.g. `SEG: Proofpoint`, `Microsoft`, `Google` (blank while the flag is off) |

Sender guidance: `external_seg` → warm/deprioritize (secure email gateway filters cold mail aggressively); `no_email` → exclude (undeliverable); `direct_google`/`direct_microsoft` → standard sending rules.

### B.10 Download partial CSV: `GET /api/enrichment/jobs/{job_id}/partial-download`

Returns whatever rows have been written so far while the job is still running. Useful for early inspection on long jobs. (For a status-guard-free variant that also works after cancel/failure, prefer **B.14 `/recover-partial`**.)

### B.11 Restart job (true resume): `POST /api/enrichment/jobs/{job_id}/restart`

Restarts a `failed`, `abandoned`, `cancelled`, **or** `partial` enrichment job. This is a **true resume**, not a from-scratch retry:

1. Reads the original CSV and the prior job's per-row checkpoints.
2. Skips rows whose checkpoints are already complete.
3. **Carries the previous partial CSV into the new job** as `prepend_rows`, so the resumed job's output is one complete file (prior completed rows + newly processed rows).
4. **Preserves the prior partial** on disk, renamed to `<original_job_id>_partial.csv` and registered as the original job's `partial_output_path` (the UI's "Download Partial" button reads this), so the pre-resume snapshot stays downloadable.
5. Re-applies the original `selected_providers`, cascade config, normalize/dedupe flags, and column mapping — no body required.

Returns a new `job_id` and the resume bookkeeping:

```json
{
  "job_id": "<new job_id>",
  "total": 4619,
  "restarted_from": "<original job_id>",
  "deduped_count": 4619
}
```

| Field | Meaning |
| --- | --- |
| `job_id` | New job carrying the resume forward (original is preserved) |
| `total` | Row count in the **new** job (after dedupe of remaining unprocessed rows) |
| `restarted_from` | The original `job_id` you POSTed to |
| `deduped_count` | Rows dropped by domain dedupe on the re-read CSV |

The UI's "Resume" button calls **B.13 `/resume-info`** first to show checkpoint counts, then POSTs here. Returns HTTP 409 if a restart for the same job is already active.

### B.12 Cancel job: `POST /api/enrichment/jobs/{job_id}/cancel`

Cancel a `queued` or `running` job. Already-completed rows remain on disk and the job ends in `partial` status (under the crash-safety model), so it is immediately resumable via B.11.

### B.13 Resume info: `GET /api/enrichment/jobs/{job_id}/resume-info`

Auth required (JWT). Returns resume eligibility + partial-CSV status — exactly the shape the UI "Resume" button reads before deciding to POST to `/restart`.

```bash
curl https://listbuilding.eagleinfoservice.com/api/enrichment/jobs/$JOB_ID/resume-info \
  -H "Authorization: Bearer YOUR_JWT"
```

**Response:**
```json
{
  "filename": "domains.csv",
  "status": "partial",
  "total": 4619,
  "checkpoint_count": 3120,
  "partial_csv_exists": true,
  "partial_csv_rows": 3118,
  "emails_found": 2847,
  "unprocessed": 1499,
  "can_resume": true
}
```

| Field | Meaning |
| --- | --- |
| `filename` | Original uploaded filename (or internal name) |
| `status` | Current job status |
| `total` | Total rows in the original job |
| `checkpoint_count` | Rows with a completed checkpoint on disk |
| `partial_csv_exists` | True if a non-empty `<job_id>.csv` is on disk |
| `partial_csv_rows` | Data rows in that partial CSV (header excluded) |
| `emails_found` | Counter from the job record |
| `unprocessed` | `max(0, total - checkpoint_count)` — rows a resume would still process |
| `can_resume` | True if there are checkpoints **or** a partial CSV (i.e. a resume has something to carry forward) |

### B.14 Recover partial CSV: `GET /api/enrichment/jobs/{job_id}/recover-partial`

Auth required (JWT). **No status guard** — works for `running`, `partial`, `failed`, `cancelled`, and `abandoned` jobs. Streams whatever partial CSV exists as a `text/csv` attachment. The UI's "Download Partial" button calls this. Returns HTTP 404 `"No partial file available yet."` if no non-empty partial file exists.

```bash
curl https://listbuilding.eagleinfoservice.com/api/enrichment/jobs/$JOB_ID/recover-partial \
  -H "Authorization: Bearer YOUR_JWT" \
  -o partial.csv
```

Candidate paths checked in order: recorded `partial_output_path` → recorded `output_path` → live `data/outputs/<job_id>.csv` → renamed `data/outputs/<job_id>_partial.csv`. The first non-empty match is served; downloaded filename is `partial_<original_filename>_<job_id>.csv`. This endpoint complements `/download` (B.9), which is the right choice for finished jobs.

### B.15 List download shards: `GET /api/enrichment/jobs/{job_id}/shards`

Auth required (JWT). Lists virtual 10,000-row download shards for a job's live CSV. **Works while the job is still running** — each shard reports how many of its rows are already on disk and becomes downloadable (via B.16) as rows land.

```bash
curl https://listbuilding.eagleinfoservice.com/api/enrichment/jobs/$JOB_ID/shards \
  -H "Authorization: Bearer YOUR_JWT"
```

**Response:**
```json
{
  "job_id": "114e9ec3-51b8-445e-8fbc-417cba1fc516",
  "shard_size": 10000,
  "rows_on_disk": 23107,
  "total": 46190,
  "shards": [
    { "shard": 0, "start_row": 0,     "end_row": 10000, "rows_available": 10000, "complete": true  },
    { "shard": 1, "start_row": 10000, "end_row": 20000, "rows_available": 10000, "complete": true  },
    { "shard": 2, "start_row": 20000, "end_row": 30000, "rows_available": 3107,  "complete": false },
    { "shard": 3, "start_row": 30000, "end_row": 40000, "rows_available": 0,     "complete": false },
    { "shard": 4, "start_row": 40000, "end_row": 46190, "rows_available": 0,     "complete": false }
  ]
}
```

| Field | Meaning |
| --- | --- |
| `shard_size` | Fixed at 10,000 rows |
| `rows_on_disk` | Live count of data rows in the CSV right now |
| `total` | Job's declared total row count (used to size the shard list when larger than `rows_on_disk`) |
| `shards[].shard` | 0-based shard index (use as the `{shard}` path param in B.16) |
| `shards[].start_row` / `end_row` | Inclusive row range this shard covers |
| `shards[].rows_available` | How many of this shard's rows are already on disk |
| `shards[].complete` | True once `rows_available` covers the full shard range |

### B.16 Download one shard: `GET /api/enrichment/jobs/{job_id}/shard/{shard}`

Auth required (JWT). **No status guard** — works for `running`, `done`, `partial`, `failed`, `cancelled`, and `abandoned`. Streams one 10,000-row shard of the live CSV as a `text/csv` attachment. Reads the file sequentially and never loads the whole thing into memory, so it is safe for 100K+ row jobs. Returns HTTP 400 on a negative shard index and HTTP 404 `"No data written yet."` if the CSV does not exist or is empty.

```bash
curl https://listbuilding.eagleinfoservice.com/api/enrichment/jobs/$JOB_ID/shard/2 \
  -H "Authorization: Bearer YOUR_JWT" \
  -o shard_2.csv
```

Use B.15 first to discover available shard indices and how many rows each contains. Downloaded filename pattern: `shard_<index>_<original_filename>_<job_id>.csv`. Each shard is self-contained (includes the CSV header), so shards can be downloaded independently and in any order.

### B.17 Source stats: `GET /api/enrichment/stats/sources`

```bash
curl -G https://listbuilding.eagleinfoservice.com/api/enrichment/stats/sources \
  -H "Authorization: Bearer YOUR_JWT" \
  --data-urlencode "start_date=2026-07-01T00:00:00" \
  --data-urlencode "end_date=2026-07-31T23:59:59"
```

Returns per-provider email-extraction counts for the date range. Non-admins see only their own stats; admins see global.

### B.18 Legacy endpoints

These endpoints still work but are deprecated. Prefer the modern equivalents above.

| Endpoint | Modern equivalent |
| --- | --- |
| `POST /api/enrichment/by-domains` | `POST /api/enrichment/flows/domain-enrich` |
| `POST /api/enrichment/by-linkedin` | `POST /api/enrichment/by-linkedin-v2` |
| `POST /api/enrichment/search/companies/enrich` | `POST /api/enrichment/flows/domain-enrich` after extracting domains |
| `POST /api/enrichment/jobs` (with `StartJobRequest`) | `POST /api/enrichment/flows/domain-enrich` |

### B.19 Find People — direct people search: `POST /api/enrichment/search/employees`

Search the internal contacts database (~8.7M leads) directly by role / function / location / industry, and **filter by lead universe** (Local business / B2B-Agency / SaaS / E-commerce). This is the engine behind the **Find People** page in the UI. Requires **JWT Bearer authentication**.

**Request body** (`EmployeeSearchRequest`, JSON — all fields optional):

| Field | Type | Example | Notes |
| --- | --- | --- | --- |
| `universe` | string | `saas` | Lead type — see below. Omit for all leads. |
| `seniority` | list[string] | `["vp","cxo"]` | Seniority levels |
| `function` | list[string] | `["sales","engineering"]` | Job functions |
| `geo_country` | list[string] | `["United States"]` | Canonical country names |
| `industry` | list[string] | `["software and tech platforms"]` | Exact industry text |
| `title_keywords` | string | `founder` | Matched in title/headline |
| `name_contains` | string | `Kenneth` | Substring on name |
| `has_email` | boolean | `true` | `true` = only leads with an email |
| `limit` | int | `50` | Page size (1–200) |
| `offset` | int | `0` | Pagination offset |

**Lead universes** (the `universe` filter):

| Value | Means | Approx. size |
| --- | --- | --- |
| `local_business` | Physical / local services | ~2.87M |
| `b2b_agency` | Agencies, consulting, manufacturing, finance, media | ~1.72M |
| `saas` | Software / SaaS companies | ~0.59M |
| `ecom` | DTC / retail / consumer brands | ~0.28M |
| *(omit)* | All leads (incl. ~3.3M unclassified) | ~8.77M |

New leads are auto-classified into a universe on write-back, so the buckets stay current without manual runs. Full rules: see `docs/LEAD_UNIVERSE_CLASSIFICATION.md`.

**Example** (JWT auth):
```bash
curl -s -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/search/employees" \
  -H "Authorization: Bearer $JWT" -H "Content-Type: application/json" \
  -d '{"universe":"saas","seniority":["vp"],"geo_country":["United States"],"has_email":true,"limit":50}'
```

**Response** (one best email per person — prefers primary, non-generic, verified):
```json
{"total": null, "limit": 50, "offset": 0, "people": [
  {"person_id":"...","full_name":"Ken Hejduk","headline":"...","seniority":"head",
   "geo_country":"United States","geo_state":"California","lead_universe":"saas",
   "title":"...","company_name":"Levelpath","industry":"information technology & services",
   "email":"ken.hejduk@levelpath.com","is_verified":true,"rating":null,"local_category":null,
   "seg_classification":"external_seg","seg_provider":"SEG: Proofpoint"}
]}
```

**SEG fields on people rows (flag-gated, added 2026-08-25).** When `ENABLE_SEG_CLASSIFICATION` is on, every row in `people[]` additionally carries `seg_classification` (one of `external_seg` / `direct_google` / `direct_microsoft` / `other_or_unknown` / `no_email`, or `""`) and `seg_provider` (named provider label). The verdict is derived from the person's email domain; rows without an email get `""` for both. When the flag is off the keys are absent.

> **Underlying endpoint (API-key access):** the same search is available directly on the Contacts DB API at `GET https://leadsdatabase.cc/v1/people/search?universe=saas&...` (Bearer `CONTACTS_API_TOKEN`); identical params as a query string.

---

## Section C: Phone Enrichment API

Find phone numbers for LinkedIn profiles via the Blitz Direct Phone API. All endpoints require **JWT Bearer authentication** (API keys not accepted).

### C.1 Upload CSV + start job: `POST /api/phone-enrichment/jobs`

```bash
curl -X POST "https://listbuilding.eagleinfoservice.com/api/phone-enrichment/jobs?linkedin_col=linkedin_url" \
  -H "Authorization: Bearer YOUR_JWT" \
  -F "file=@linkedin_profiles.csv"
```

**Query parameter:** `linkedin_col` (optional) — column name containing LinkedIn URLs. If omitted, auto-detected. Recognized column names: `linkedin_url`, `linkedinurl`, `linkedin`, `person_linkedin_url`, `person_linkedinurl`, `url`, `profile_url`, `profileurl` — or any column whose name contains "linkedin".

**CSV requirement:** At least one row's value must contain `linkedin.com`. Otherwise HTTP 400.

**Response:**
```json
{
  "job_id": "...",
  "status": "queued",
  "total": 1500,
  "valid_urls": 1487,
  "linkedin_col": "linkedin_url",
  "message": "..."
}
```

### C.2 List jobs: `GET /api/phone-enrichment/jobs`

Returns the caller's phone-enrichment jobs.

### C.3 Job status: `GET /api/phone-enrichment/jobs/{job_id}`

Returns `status`, counters, output path. Status values: `queued`, `running`, `done`, `failed`.

### C.4 SSE progress stream: `GET /api/phone-enrichment/jobs/{job_id}/stream`

Server-sent events with heartbeat every 30 seconds. Note: phone enrichment SSE supports **header auth only** (no `?token=` query fallback, unlike the scraper).

### C.5 Download enriched CSV: `GET /api/phone-enrichment/jobs/{job_id}/download`

Available once status is `done`. Output CSV preserves all original columns and appends:

| Column | Type | Description |
| --- | --- | --- |
| `phone_number` | string | The phone number found, or empty if none. |
| `phone_found` | string | `"true"` or `"false"`. |

Download filename pattern: `{original_filename}_with_phones.csv`.

### C.6 Rate limit

Phone enrichment has its own rate limit: **5 requests/second** with 5 concurrent workers. No cancel, restart, resume, or partial-download endpoints exist for phone jobs — if a job hangs, contact support.

---

## Section D: Google Maps Scraper API

Scrape business listings from Google Maps by country, query, and geographic mode. Most endpoints require **JWT Bearer authentication**. Cache and resume endpoints also accept **API keys**.

### D.1 Supported countries (19)

The scraper currently supports these 19 countries (ISO 2-letter codes):

| Code | Country | Code | Country |
| --- | --- | --- | --- |
| `us` | United States | `nl` | Netherlands |
| `gb` | United Kingdom | `be` | Belgium |
| `ie` | Ireland | `pl` | Poland |
| `au` | Australia | `se` | Sweden |
| `ca` | Canada | `dk` | Denmark |
| `de` | Germany | `at` | Austria |
| `fr` | France | `ch` | Switzerland |
| `es` | Spain | `pt` | Portugal |
| `it` | Italy | `no` | Norway |
| | | `nz` | New Zealand |

List dynamically via `GET /api/scraper/regions/countries` (no auth).

### D.2 Region discovery endpoints (all no-auth, read-only)

| Endpoint | Purpose |
| --- | --- |
| `GET /api/scraper/regions/countries` | List of supported countries `{code, name}` |
| `GET /api/scraper/regions/centers?country=us` | All centers for a country `{name, state, lat, lng}` |
| `GET /api/scraper/regions/states` | Canonical US state names (USA only) |
| `GET /api/scraper/regions/cities?country=us&q=...` | Fuzzy city search (max 20 results) |
| `GET /api/scraper/regions/cities?country=us` (no q) | All cities (US: ~29,546; others: anchor cities) |
| `POST /api/scraper/regions/parse-zip-csv` | Upload CSV of US 5-digit zip codes, validate |
| `POST /api/scraper/regions/parse-uk-postcode-csv` | Upload CSV of UK postcodes |
| `POST /api/scraper/regions/parse-ca-postal-csv` | Upload CSV of Canada postal codes |
| `POST /api/scraper/regions/cities-to-zips` | Map city names → zip/postal codes |
| `GET /api/scraper/regions/download-template?type=us_cities` | Download template CSV (`us_cities`, `us_zips`, `uk_postcodes`, `ca_postal_codes`) |
| `POST /api/scraper/regions/estimate` | Estimate task count + quota check for a planned job (JWT required) |

### D.3 Create a scraper job: `POST /api/scraper/jobs`

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/scraper/jobs \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "coffee shop",
    "mode": "cities",
    "country": "us",
    "cities": ["New York", "Los Angeles", "Chicago"],
    "expected_types": []
  }'
```

**Request body parameters:**

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `query` | string | required | Search query (e.g., `"coffee shop"`, `"dentist"`) |
| `mode` | string | `"all"` | One of `"all"`, `"states"`, `"cities"`, `"zips"` |
| `country` | string | `"us"` | ISO 2-letter country code |
| `states` | array of strings | `[]` | US state names (mode=`states`) |
| `cities` | array of strings | `[]` | City names (mode=`cities`) |
| `zips` | array of strings | `[]` | US zips, UK postcodes, or CA postal codes (mode=`zips`) |
| `center_ids` | array of strings | `[]` | Center name filter (non-US) |
| `expected_types` | array of strings | `[]` | Expected place types |

**Response:** `{job_id, total_tasks, center_count, warnings}`.

**Note:** Each job targets a single country. To scrape multiple countries, create separate jobs.

### D.4 Other job endpoints

| Method + Path | Auth | Description |
| --- | --- | --- |
| `GET /api/scraper/jobs` | JWT | List caller's jobs (admin sees all, limit 200) |
| `GET /api/scraper/jobs/{id}` | JWT | Get one job |
| `GET /api/scraper/jobs/{id}/stream` | JWT via header **or** `?token=...` query | SSE progress stream |
| `GET /api/scraper/jobs/{id}/download` | JWT **or** API key | Full CSV for finished/stopped/failed/cancelled/abandoned jobs |
| `GET /api/scraper/jobs/{id}/partial-download` | JWT **or** API key | CSV of rows collected so far while job is still running |
| `POST /api/scraper/jobs/{id}/cancel` | JWT | Cancel a queued or running job |
| `POST /api/scraper/jobs/{id}/restart` | JWT | Restart a failed/abandoned/cancelled/stopped job from scratch (new `job_id`) |

### D.5 Resume feature (skip completed tasks)

| Method + Path | Auth | Description |
| --- | --- | --- |
| `GET /api/scraper/jobs/{id}/resume-info` | JWT **or** API key | Returns `{can_resume, checkpoint_count, total_tasks, completion_pct, is_resumable}` |
| `POST /api/scraper/jobs/{id}/resume` | JWT **or** API key | Resume from checkpoints. Body: `{include_previous: true}` — copies the prior partial CSV into the new job so you end up with one combined file. Returns new `job_id`. |

**Resume vs Restart:** Resume skips already-completed tasks and continues from where the prior job stopped. Restart re-runs everything from scratch.

### D.6 Scraper → Enrichment chain: `POST /api/jobs/{scraper_job_id}/chain`

Hand off a completed scraper job's results to the enrichment pipeline.

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/jobs/SCRAPER_JOB_ID/chain \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{"providers": ["contacts_db", "blitz", "smartprospect"]}'
```

Reads the scraper output CSV (requires a `website` column), filters valid domains, creates an enrichment job with `parent_job_id` set to the scraper job.

### D.7 Cache feature (avoid re-scraping identical queries)

The scraper caches results for 90 days. Identical queries return cached results instantly without consuming your daily quota.

| Method + Path | Auth | Description |
| --- | --- | --- |
| `POST /api/scraper/cache/check` | JWT **or** API key | Check if an identical query was run before. Returns cache metadata if hit. |
| `GET /api/scraper/cache/download/{cache_id}` | JWT **or** API key | Download a cached result CSV. |
| `POST /api/scraper/cache/subset-count` | JWT | Return row count for a geographic subset of cached results. |

**Cache key inputs:** normalized query, region signature (mode + country + states/cities/zips), zoom signature (`[10, 11, 12]`), and expected-types signature.

### D.8 Scraper output CSV columns

Each row in the output CSV represents one scraped business:

```
dedupe_key, query, center_name, center_state, center_lat, center_lng, zoom,
place_id, business_id, name, category_name, full_address, city, city_state,
latitude, longitude, distance_km, rating, review_count, website, phone, types,
price_level, timezone, working_hours, is_claimed, verified,
is_permanently_closed, is_temporarily_closed, place_link, photo_count,
first_photo_url, inserted_at, center_id
```

Download filename pattern: `{query}_{total_tasks}_centers_{result_count}_results_{status}.csv`. Partial downloads append `_INCOMPLETE`.

---

## Section E: Helper Endpoints

### E.1 Health check: `GET /api/health` (no auth)

```json
{"status": "ok"}
```

### E.2 List enabled providers: `GET /api/enrichment/providers`

Returns the dynamic list of currently-enabled providers. Useful for building UIs that adapt to provider availability.

```json
{"providers": ["contacts_db", "blitz", "smartprospect", "wizleads", "better_enrich"]}
```

### E.3 Default title cascade: `GET /api/enrichment/default-cascade` (no auth)

Returns the 3-tier default Blitz cascade (Tier 1: Owner/CEO/Founder; Tier 2: C-level; Tier 3: Director-level). Useful when constructing custom `cascade` payloads.

### E.4 Daily quota: `GET /api/quota` (JWT)

```bash
curl https://listbuilding.eagleinfoservice.com/api/quota \
  -H "Authorization: Bearer YOUR_JWT"
```

```json
{
  "limit": 50000,
  "used": 12345,
  "remaining": 37655,
  "resets_at": "2026-07-17T00:00:00Z",
  "is_admin": false
}
```

---

## Section F: Rate Limits & Quotas

### F.1 Daily request quota

| User type | Daily quota | Reset time |
| --- | --- | --- |
| Non-admin | **50,000 requests/day** | Midnight UTC (calendar-day reset) |
| Admin | Unlimited | — |

The quota applies to scraper-job creation, scraper restarts, and scraper estimate calls. Single-row enrichment (`POST /api/enrichment/enrich`) calls also count.

### F.2 Quota-exceeded response

HTTP **429** with a human-readable message. No `Retry-After` or `X-RateLimit-*` headers are attached.

### F.3 SQLite lock contention

When the database is briefly locked (rare), the API returns HTTP **503** with `Retry-After: 3` (seconds).

### F.4 Per-provider rate limits (throughput ceilings)

These are the per-provider ceilings the platform respects when calling upstream APIs. Total throughput for any single job is bounded by these.

| Provider | Rate | Concurrency |
| --- | --- | --- |
| Contacts DB | 75 requests/sec | 25 concurrent domain lookups |
| Blitz | 25 requests/sec | 25 concurrent |
| SmartProspect | 30 requests/sec (account-level 2000 req/min) | batch up to 10 per call |
| WizLeads | 10 requests/sec | 15 concurrent email lookups |
| BetterEnrich | 10 requests/sec | shared with company-email fallback |
| Phone Enrichment | 5 requests/sec | 5 concurrent |
| Scraper | 8 concurrent workers | per-job |

### F.5 Practical throughput

For a bulk CSV enrichment job with no contention, expect **30–110 rows per minute** depending on hit rate (rows that miss all providers cost ~5–10s each; rows that hit Contacts DB cost <1s each).

---

## Section G: UI Features Summary

The platform's UI lives at `https://listbuilding.eagleinfoservice.com/`. Sidebar pages:

| Page | Purpose |
| --- | --- |
| **Home** | Welcome screen with flow chooser cards |
| **Google Maps Scraper** | Build a scraper job (query, country, mode, providers) |
| **Upload Domains** | CSV upload + column mapping + provider selection for enrichment |
| **Company Search** | Search companies by criteria, then enrich matching ones |
| **LinkedIn Enrich** | CSV of LinkedIn URLs → work emails |
| **Phone Enrichment** | CSV of LinkedIn URLs → US phone numbers |
| **Enrichment Jobs** | List of enrichment jobs with filter tabs and per-job actions |
| **API Keys** | Create / list / delete API keys |
| **Help & Guide** | In-app documentation |

### Job filter tabs

- **Enrichment Jobs:** All, Running, Starting, Done, Partial, Failed, Abandoned, Cancelled
- **Scraper Jobs:** All, Done, Running, Failed, Abandoned, Cancelled

### Per-job actions

- **Download** — get final CSV
- **Resume** — restart a stopped/abandoned/failed job from checkpoints (scraper) or from scratch (enrichment)
- **Retry** — restart from scratch
- **Download Partial** — get rows collected so far while job is still running
- **Cancel** — stop a queued or running job

### Modals

- **Cache modal** — shown when starting a scraper job that matches a cached query. Offers "Use Cached Results" (instant, free) or "Scrape Fresh".
- **Resume modal** — shown when restarting a job. Shows checkpoint counts and includes a "Include previous results" checkbox.
- **Chain to Enrichment modal** — shown after a scraper job completes. Lets you pick providers and hand off results to enrichment.

### 0-email warning banner (new 2026-07-13)

Any enrichment job that completes with `emails_found = 0` on a non-empty input shows a prominent yellow banner:

> ⚠ 0 emails found on a N-row input. Output CSV is likely empty. Retry the job or contact support.

The card also gets an orange left-border. The backend now marks such jobs as `failed` (not `done`) when the failure is detected at the row level — so the API `status` field and the UI banner agree.

### Provider selection UI

On the Upload Domains page, the provider checkboxes let you pick which providers run for that job:
- **Contacts DB** is always on and locked (mandatory first step)
- **Blitz**, **SmartProspect**, **WizLeads**, **BetterEnrich** are toggleable

This UI maps to the `providers` field in `ProviderToggleRequest` (same semantics as `selected_providers` on the single-row endpoint).

---

## Error Code Reference

### HTTP 400 — Bad Request

Common cases and their exact messages:

| Trigger | Message |
| --- | --- |
| Bad domain | `Invalid domain format` |
| Missing identifier | `Either 'domain', 'linkedin_url', or 'company_linkedin_url' must be provided` |
| `force_provider` + `selected_providers` both set | `force_provider and selected_providers are mutually exclusive. Pick one: force_provider='blitz' (single) or selected_providers=['contacts_db','smartprospect'] (subset).` |
| Empty `selected_providers` list | `selected_providers must be a non-empty list. Valid providers: ['better_enrich', 'blitz', 'contacts_db', 'smartprospect', 'wizleads']` |
| Unknown provider name | `Invalid provider(s) in selected_providers: ['fake_provider']. Valid: ['better_enrich', 'blitz', 'contacts_db', 'smartprospect', 'wizleads']` |
| Bad titles value | `Titles cannot be empty` |
| Too many titles | `Maximum 50 titles allowed. Contact support for bulk operations.` |
| Non-CSV upload | `Only CSV files are accepted.` |
| Missing column | `Column '<col>' not found in CSV.` |
| Bad cascade JSON | `Could not parse CSV: ...` or `Invalid cascade JSON` |

### HTTP 401 — Unauthorized

`Invalid token.` or `Authentication required. Provide either JWT token or X-API-Key header.`

### HTTP 403 — Forbidden

`Access denied.` — returned when a non-owner non-admin tries to access another user's job.

### HTTP 404 — Not Found

`Job not found.` / `Upload not found.` / `Output file not found.` / `No enriched data yet.`

### HTTP 409 — Conflict

`A restart for this job is already in progress (job_id: ..., status: ...). Please wait for it to complete or cancel it first.`

### HTTP 429 — Quota Exceeded

Plain-text message about daily quota. No `Retry-After` header.

### HTTP 500 — Internal Server Error

`Could not read original CSV: ...` / `Search failed: ...` / `Job failed: <error_msg>`

### HTTP 503 — Service Unavailable

`The platform database is briefly busy. Please retry in a few seconds.` — with `Retry-After: 3` header.

---

## Changelog

| Date | Change |
| --- | --- |
| 2026-07-24 | Documented crash-safety model + resume/recovery endpoints: `/resume-info` (B.13), `/recover-partial` (B.14), `/shards` (B.15), `/shard/{shard}` (B.16). Updated `/download` (B.9) and `/restart` (B.11) to reflect partial/failed support and true-resume behavior. |
| 2026-07-16 | Full API reference published. Added: phone enrichment, scraper, helper endpoints, auth matrix, rate limits, UI features, error reference. |
| 2026-07-13 | `selected_providers` parameter added to `/api/enrichment/enrich`. Fail-loud guards prevent silent 0-email jobs. UI warning banner added. |
| 2026-07-08 | SmartProspect (SmartLead Find Emails) added as 3rd provider. |
| 2026-07-06 | Prospeo disabled end-to-end. |
| 2026-04-15 | Original SOP published (Clay step-by-step guide only). |
