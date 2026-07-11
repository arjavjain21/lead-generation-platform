# List Building API — End-to-End Documentation

- **Document date:** 2026-07-05
- **Base URL:** `https://listbuilding.eagleinfoservice.com`
- **Local backend:** `http://localhost:8765`
- **Service:** `lead-generation-platform.service` (FastAPI + SQLite + PostgreSQL)
- **In-API mirror:** `GET /api/enrichment/flows/help` returns this same content as JSON
- **Status:** Production-live, all endpoints below are reachable on the current deploy

---

## 1. What this API is

- A unified lead-generation platform that turns a list of **domains** or **LinkedIn URLs** into enriched contact records.
- Each record contains decision-maker names, titles, verified work emails, phone numbers, LinkedIn URLs, and company metadata.
- It runs a **provider cascade** — Contacts DB (free, internal) first, then Blitz, then smartprospect, then WizLeads, then BetterEnrich — stopping at the first provider that returns a contact.
- Three user-facing **flows** plus a single-shot `/enrich` endpoint for ad-hoc lookups.
- All bulk work runs as a **background job** with SSE progress streaming and CSV download.

---

## 2. Authentication

- **Two credential types are accepted** depending on endpoint:
  - **JWT bearer token** — `Authorization: Bearer <token>` — obtained from `POST /api/auth/login`.
  - **API key** — `X-API-Key: <key>` **or** `Authorization: Bearer <key>` — created from `POST /api/api-keys`.
- **JWT-only endpoints** (CSV upload, job create/list/cancel/restart, downloads): API key is NOT accepted.
- **JWT-or-API-key endpoints** (single `/enrich`, search, providers, stats, `/flows/help`): either credential works.
- Tokens expire in **7 days**; API keys do not expire unless revoked.
- Login payload:

```json
{ "email": "you@company.com", "password": "your-password" }
```

- Login response shape:

```json
{ "access_token": "...", "token_type": "bearer", "user": { "email": "...", "is_admin": false } }
```

- Users are created via CLI only (`backend/create_user.py`) — there is no public signup endpoint.

---

## 3. Provider cascade (how enrichment actually works)

- Provider order is fixed by configuration in `backend/enrichment/providers.py`:

| # | Provider | Rate | Status | Role |
|---|----------|------|--------|------|
| 1 | `contacts_db` | 75 RPS | enabled | Internal PostgreSQL DB — always tried first, free |
| 2 | `blitz` | 25 RPS | enabled | LinkedIn-based enrichment with title cascade |
| 3 | `smartprospect` | 30 RPS | enabled | SmartLead Find Emails — self-verifying person-email finder, batch up to 10. Gates on `firstName`+`lastName`+`domain` (decoupled from Blitz). `verification_status="Valid"` → `dm_email_verified="yes"` (skips MailTester). `status="Invalid"` discarded, cascade falls through. Kill switch: `ENABLE_SMARTPROSPECT=false`. |
| 4 | `wizleads` | 10 RPS | enabled | Catch-all verified email enrichment |
| 5 | `better_enrich` | 10 RPS | enabled | Person + company email (final fallback) |
| 6 | `prospeo` | n/a | **disabled** | Code present but end-to-end off (`ENABLE_PROSPEO=false` + `providers.py` flag) |

- **Cascade behavior:** stop on first provider that returns a usable contact — later providers are skipped for that row.
- **Title cascade** (when no custom titles given) — used by Blitz:
  - Tier 1 — Owner, CEO, Founder, Co-Founder, President
  - Tier 2 — CMO, CTO, COO, VP-level
  - Tier 3 — Director-level (Director of Marketing, Director of Sales, etc.)
- **Custom titles** — pass `titles: "dentist,orthodontist,dmd"` (max 50, comma-separated) for fuzzy matching against LinkedIn headlines.
- **Forced provider** — single-shot `/enrich` accepts `force_provider` to bypass the cascade.
- **Domain normalization** — raw URLs like `https://mesterh-service.de/?utm_source=gmb` are normalized to `mesterh-service.de` before any provider call.
- **Email verification** — Contacts DB emails are run through MailTester (`validation.hyperke.org` proxy); invalid emails are marked and the cascade continues.

---

## 4. Endpoint quick-reference

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/api/enrichment/flows/help` | none | **This documentation as JSON** |
| GET | `/api/enrichment/providers` | JWT or key | List enabled providers |
| GET | `/api/enrichment/default-cascade` | none | Default 3-tier title cascade |
| GET | `/api/enrichment/search/options` | JWT or key | Industries, employee ranges, countries, etc. |
| GET | `/api/enrichment/stats/sources` | JWT or key | Email-source aggregation |
| GET | `/api/enrichment/enrich/{domain}` | JWT or key | Quick single-domain lookup |
| GET | `/api/enrichment/enrich` | JWT or key | Unified single lookup (GET form) |
| POST | `/api/enrichment/enrich` | JWT or key | Unified single lookup + DB sync |
| POST | `/api/enrichment/upload` | **JWT only** | Upload CSV, get `upload_id` |
| POST | `/api/enrichment/jobs` | **JWT only** | Start basic enrichment job (legacy) |
| GET | `/api/enrichment/jobs` | **JWT only** | List my jobs (admin sees all) |
| GET | `/api/enrichment/jobs/{id}` | **JWT only** | Job status + config |
| GET | `/api/enrichment/jobs/{id}/stream` | **JWT only** | SSE progress stream |
| GET | `/api/enrichment/jobs/{id}/download` | **JWT only** | Full result CSV |
| GET | `/api/enrichment/jobs/{id}/partial-download` | **JWT only** | In-progress CSV (running jobs) |
| POST | `/api/enrichment/jobs/{id}/restart` | **JWT only** | Restart failed/abandoned job (dedupes) |
| POST | `/api/enrichment/jobs/{id}/cancel` | **JWT only** | Cancel a running/queued job |
| POST | `/api/enrichment/flows/domain-enrich` | **JWT only** | **Flow 1 — recommended domain pipeline** |
| POST | `/api/enrichment/search/companies` | JWT or key | **Flow 2** — search companies by criteria |
| POST | `/api/enrichment/search/companies/enrich` | JWT or key | Flow 2 legacy — search + enrich in one call |
| POST | `/api/enrichment/by-linkedin-v2` | **JWT only** | **Flow 3 — recommended LinkedIn pipeline** |
| POST | `/api/enrichment/by-linkedin` | **JWT only** | Flow 3 legacy — personal URLs only |
| POST | `/api/enrichment/by-domains` | **JWT only** | Legacy alias for basic job creation |

---

## 5. Flow 1 — Domain enrichment (the primary use case)

- **Endpoint:** `POST /api/enrichment/flows/domain-enrich`
- **Auth:** JWT only
- **Content-Type:** `application/json`
- **Body schema** (`ProviderToggleRequest`):

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `upload_id` | string | yes | — | From `POST /api/enrichment/upload` |
| `domain_col` | string | yes | — | Column name holding the domain |
| `name_col` | string | no | null | Full-name column (boosts match rate) |
| `first_name_col` | string | no | null | First-name column |
| `last_name_col` | string | no | null | Last-name column |
| `linkedin_url_col` | string | no | null | Per-row LinkedIn URL hint |
| `phone_col` | string | no | null | Per-row phone hint |
| `company_name_col` | string | no | null | Company-name hint |
| `existing_email_col` | string | no | null | Known email hint |
| `max_results` | int | no | `5` | Decision makers returned per domain |
| `providers` | string[] | no | null | Subset of `["blitz","smartprospect","wizleads","better_enrich"]`. `contacts_db` always runs first and cannot be disabled. |
| `titles` | string | no | null | Comma-separated fuzzy titles, e.g. `"dentist,orthodontist,dmd"`. Max 50. |
| `normalize_domains` | bool | no | `true` | Strip `https://`, `www.`, query params before lookup |
| `dedupe_by_domain` | bool | no | `true` | Drop duplicate domains before processing |

- **Response (200):**

```json
{ "job_id": "9c1f...e7", "total": 482 }
```

- **Concurrency:** 25 domains processed in parallel.
- **Pre-processing:** when `dedupe_by_domain=true`, duplicates are removed before job creation and the new `total` reflects unique rows.
- **Validation errors:**
  - `404` — `Upload not found.` (wrong/missing `upload_id`)
  - `400` — `Column '<name>' not found in CSV.`
  - `400` — `Titles cannot be empty`
  - `400` — `Maximum 50 titles allowed. Contact support for bulk operations.`
  - `400` — `Invalid providers: [...]. Valid: ['contacts_db','blitz','smartprospect','wizleads','better_enrich']`

### Flow 1 input CSV — minimum viable file

```csv
domain
acme.com
globex.com
initech.com
```

### Flow 1 input CSV — maximum coverage

```csv
domain,first_name,last_name,linkedin_url,company_name,existing_email
acme.com,Jane,Doe,https://linkedin.com/in/janedoe,Acme Inc,jane@acme.com
```

### Flow 1 — minimal call

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/flows/domain-enrich \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"upload_id":"<uuid>","domain_col":"domain"}'
```

### Flow 1 — dental vertical with custom titles

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/flows/domain-enrich \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "upload_id":"<uuid>",
    "domain_col":"domain",
    "titles":"dentist,orthodontist,dmd,dds",
    "max_results":3,
    "providers":["blitz","better_enrich"]
  }'
```

---

## 6. Flow 2 — Company search

- **Endpoint:** `POST /api/enrichment/search/companies` (returns matching companies only)
- **Endpoint:** `POST /api/enrichment/search/companies/enrich` (search + start enrichment job — legacy)
- **Auth:** JWT or API key
- **Body schema** (`CompanySearchRequest`):

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `name` | string | no | null | Company name (substring match) |
| `industry` | string[] | no | null | e.g. `["Computer Software","SaaS"]` — see `/search/options` |
| `employee_range` | string[] | no | null | e.g. `["11-50","51-200"]` |
| `company_type` | string[] | no | null | `Privately Held`, `Public Company`, etc. |
| `country_code` | string | no | null | ISO 3166-1 alpha-2 (`US`, `GB`, `DE`, …) |
| `limit` | int | no | `100` | Max results per page |
| `offset` | int | no | `0` | Pagination offset |

- **`/search/companies` response:**

```json
{ "count": 50, "total": 432, "results": [{"domain":"...","linkedin_url":"...","name":"..."}] }
```

- **`/search/companies/enrich` body** (`SearchAndEnrichRequest`) — adds `max_decision_makers: int = 5` and `include_generic_emails: bool = true`. Returns `{ "job_id":"...","total":50,"companies_found":50 }`.
- **Discover allowed values** for industry / employee range / country via `GET /api/enrichment/search/options`.

### Flow 2 — find SaaS companies in the US

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/search/companies \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"industry":["Computer Software"],"employee_range":["51-200"],"country_code":"US","limit":50}'
```

---

## 7. Flow 3 — LinkedIn enrichment

- **Recommended endpoint:** `POST /api/enrichment/by-linkedin-v2` — handles personal AND/OR company LinkedIn URLs in one job.
- **Legacy endpoint:** `POST /api/enrichment/by-linkedin` — personal URLs only, kept for backward compatibility.
- **Auth:** JWT only
- **Body schema** (`LinkedInV2Request`):

| Field | Type | Required | Default | Notes |
|-------|------|----------|---------|-------|
| `upload_id` | string | yes | — | From `POST /api/enrichment/upload` |
| `personal_linkedin_col` | string | no | null | Column with `linkedin.com/in/...` URLs |
| `company_linkedin_col` | string | no | null | Column with `linkedin.com/company/...` URLs |
| `max_dms` | int | no | `5` | Decision makers per company |
| `include_company` | bool | no | `true` | Pull company metadata (industry, size) |

- **Constraint:** at least one of `personal_linkedin_col` or `company_linkedin_col` must be supplied and must exist in the CSV.
- **Response (200):**

```json
{ "job_id":"...","total":1000,"flow":"linkedin_v2_enrichment" }
```

- **Legacy** `LinkedInEnrichRequest` body is `{ "upload_id":"...","linkedin_col":"...","include_company":true }`.

### Flow 3 input CSV

```csv
personal_linkedin,company_linkedin
https://linkedin.com/in/janedoe,https://linkedin.com/company/acme
https://linkedin.com/in/johndoe,
```

### Flow 3 — start job

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/by-linkedin-v2 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"upload_id":"<uuid>","personal_linkedin_col":"personal_linkedin","company_linkedin_col":"company_linkedin","max_dms":5}'
```

---

## 8. Single-shot enrichment — `/enrich`

- **GET** `/api/enrichment/enrich/{domain}` — quickest path for a single domain.
- **GET/POST** `/api/enrichment/enrich` — unified; auto-detects mode from inputs.
- **Auth:** JWT or API key
- **Mode auto-detection:**

| Inputs | Mode | Behavior |
|--------|------|----------|
| `domain` only | `domain_only` | Full cascade, all decision makers |
| `linkedin_url` only | `linkedin_only` | Specific person via cascade |
| `domain` + (`full_name` *or* `linkedin_url`) | `enhanced` | Looks for that specific person only; 0 results if not found (no fallback) |

- **Common parameters** (query for GET, body for POST):

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `domain` | string | — | e.g. `google.com` |
| `linkedin_url` | string | — | `https://linkedin.com/in/...` |
| `full_name` | string | — | `"John Doe"` |
| `first_name` | string | — | Use with `last_name` |
| `last_name` | string | — | Use with `first_name` |
| `phone` | string | — | Optional hint |
| `company_name` | string | — | Optional hint |
| `existing_email` | string | — | Optional hint |
| `max_results` | int | `5` | 1–10 |
| `titles` | string | null | Comma-separated, e.g. `"CEO,CTO"` |
| `cascade` | object[] | null | Advanced — overrides `titles`; same shape as `/default-cascade` |
| `cascade_json` | string | null | URL-encoded JSON of `cascade` (GET only) |
| `force_provider` | string | null | One of `contacts_db`, `blitz`, `smartprospect`, `wizleads`, `better_enrich` |
| `debug` | bool | `false` | POST/GET `/enrich` only — adds a `routing` block |

- **Response shape (200):**

```json
{
  "domain": "google.com",
  "mode": "domain_only",
  "company_linkedin_url": "https://linkedin.com/company/google",
  "contacts": [
    {
      "full_name": "Jane Doe",
      "first_name": "Jane",
      "last_name": "Doe",
      "title": "VP of Sales",
      "email": "jane@google.com",
      "linkedin_url": "https://linkedin.com/in/janedoe",
      "headline": "VP of Sales at Google",
      "location_city": "Mountain View",
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
  }
}
```

- `data_sources.*` may also be `"not_found"` when no provider returned data.
- The POST call **syncs** every found contact into the internal Contacts DB; the GET call does not sync.

### Quick single-domain lookup

```bash
curl "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich/acme.com?max_results=3" \
  -H "Authorization: Bearer $TOKEN"
```

### Single person via LinkedIn

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"linkedin_url":"https://linkedin.com/in/janedoe"}'
```

---

## 9. CSV upload — `/upload`

- **Endpoint:** `POST /api/enrichment/upload`
- **Auth:** JWT only
- **Content-Type:** `multipart/form-data`
- **Form field:** `file` (must end in `.csv`)
- **Response (200):**

```json
{
  "upload_id": "9f1e...c3",
  "columns": ["domain","first_name","last_name"],
  "preview": [{"domain":"acme.com","first_name":"Jane","last_name":"Doe"}],
  "row_count": 482,
  "filename": "leads.csv"
}
```

- The CSV is stored at `backend/data/uploads/<upload_id>.csv` plus a sibling `<upload_id>.metadata.json` recording the original filename.
- `upload_id` is then passed to Flow 1, Flow 3, or `/jobs`.

### Upload example

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@leads.csv"
```

---

## 10. Job lifecycle

### Create (legacy basic)

- `POST /api/enrichment/jobs` accepts `StartJobRequest`:

| Field | Type | Default | Notes |
|-------|------|---------|-------|
| `upload_id` | string | — | Required |
| `domain_col` | string | — | Required |
| `name_col` / `first_name_col` / `last_name_col` | string | null | Optional hints |
| `linkedin_url_col` / `phone_col` / `company_name_col` / `existing_email_col` | string | null | Optional hints |
| `cascade` | object[] | null | Custom cascade (advanced) |
| `max_results` | int | `5` | Per-domain decision makers |
| `force_provider` | string | null | `contacts_db` / `blitz` / `smartprospect` / `wizleads` / `better_enrich` |
| `validate_email` | bool | `true` | Run MailTester on results |

- **Recommended:** prefer Flow 1 (`/flows/domain-enrich`) over `/jobs` — it adds provider selection, fuzzy titles, and per-row dedupe.

### List

- `GET /api/enrichment/jobs` returns `{ "jobs":[{...}] }` — own jobs for users, all jobs for admins.
- Each job object contains: `job_id`, `status`, `total`, `processed`, `emails_found`, `filename`, `original_filename`, `created_at`, `completed_at`, plus the full creation config.

### Status

- `GET /api/enrichment/jobs/{job_id}` returns the full job record including `status`, `error`, and absolute `output_file` path.
- Status values: `queued`, `running`, `completed`, `failed`, `cancelled`, `partial`.

### Stream (SSE)

- `GET /api/enrichment/jobs/{job_id}/stream` returns `text/event-stream`.
- Events include replay (prior history) and live progress until `status` reaches `completed` / `failed` / `cancelled`.
- Frontend uses `EventSource`; server closes the stream when the job finishes.

### Download

- `GET /api/enrichment/jobs/{job_id}/download` — full enriched CSV once the job is `completed` (also works for `failed`/`partial`).
- `GET /api/enrichment/jobs/{job_id}/partial-download` — whatever has been written so far for `running` jobs.

### Restart

- `POST /api/enrichment/jobs/{job_id}/restart` returns in <1s and spawns a new job in the background.
- Behaviour:
  - Re-reads the original uploaded CSV.
  - Reads the prior job's output and skips rows already processed.
  - Dedupes unprocessed rows.
  - Persists `restarted_from` on the new job.
- Response (200):

```json
{ "job_id":"<new-uuid>", "total":432, "restarted_from":"<old-uuid>", "deduped_count":50 }
```

### Cancel

- `POST /api/enrichment/jobs/{job_id}/cancel` flips status to `cancelled`.
- Already-processed rows remain available via `/download` or `/partial-download`.
- Response (200):

```json
{ "job_id":"...", "status":"cancelled", "message":"Job cancellation requested. Partial results will be preserved." }
```

---

## 11. Output CSV columns (Flow 1 + Flow 3)

- The enriched CSV keeps all input columns and appends these (one row per decision maker):

| Column | Meaning |
|--------|---------|
| `company_linkedin_url` | Resolved company LinkedIn URL |
| `company_name` | Normalized company name |
| `company_industry` | Industry (e.g. `Computer Software`) |
| `company_employee_count` | Employee range |
| `dm_first_name` / `dm_last_name` / `dm_full_name` | Decision-maker names |
| `dm_title` | Raw title from provider |
| `dm_job_level` / `dm_job_function` | Classified level/function |
| `dm_linkedin_url` | Personal LinkedIn URL |
| `dm_email` | Work email |
| `dm_email_source` | Provider that produced the email (`contacts_db`, `blitz_email`, `smartprospect_email`, `wizleads`, `better_enrich`) |
| `dm_email_verified` | `valid` / `invalid` / `unknown` |
| `mailtester_code` / `mailtester_message` | MailTester verification result |
| `dm_phone` | Phone (if found) |
| `dm_headline` | LinkedIn headline |
| `dm_location_city` / `dm_location_country` | Location |
| `dm_icp_tier` | 1 / 2 / 3 |
| `row_status` | `enriched` / `not_found` / `skipped` / `error` |
| `input_domain` / `input_full_name` / `input_linkedin_url` | Echo of inputs |
| `normalized_linkedin_url` | Cleaned LinkedIn URL |
| `source_path` | Which flow produced this row |
| `provider_attempts` | JSON list of providers tried |
| `providers_called` / `providers_skipped` | Summary |
| `final_email` / `final_email_level` | Resolved best email + verification level |

---

## 12. Metadata endpoints

### Providers

- `GET /api/enrichment/providers` → `{ "providers":["contacts_db","blitz","smartprospect","wizleads","better_enrich"] }` (reflects `ENABLED_PROVIDERS`).

### Default cascade

- `GET /api/enrichment/default-cascade` → `{ "cascade":[{...},{...},{...}] }` (no auth required).
- Each tier has `include_title`, `exclude_title`, `location` (`["WORLD"]`), and `include_headline_search`.

### Search options

- `GET /api/enrichment/search/options` → industries, employee ranges, company types, countries, job levels, job functions, sales regions.

### Stats

- `GET /api/enrichment/stats/sources?start_date=2026-06-01&end_date=2026-07-05` →
```json
{ "totals":{"contacts_db":12340,"blitz_email":4321}, "grand_total":16661, "breakdown":{...} }
```
- Admins see all users; non-admins see only their own.

---

## 13. The `/help` endpoint

- **Path:** `GET /api/enrichment/flows/help`
- **Auth:** none (intentionally public, like `/default-cascade`)
- **Response shape:**

```json
{
  "generated_at": "2026-07-05",
  "base_url": "https://listbuilding.eagleinfoservice.com",
  "auth": { ... },
  "providers": { ... },
  "endpoints": [
    { "method":"POST", "path":"/api/enrichment/flows/domain-enrich",
      "auth":"jwt", "summary":"...", "body_fields":[...], "response":{...} },
    ...
  ],
  "examples": { "flow1_minimal": "...", "single_enrich": "..." }
}
```

- Returns the same content as this document, structured for programmatic discovery.

---

## 14. Error conventions

- All errors follow FastAPI's `{"detail":"..."}` shape.
- HTTP codes in use:
  - `200` — success
  - `400` — bad request (bad CSV, bad column, invalid provider, too many titles)
  - `401` — missing/invalid token or API key
  - `403` — authenticated but not allowed (e.g. accessing another user's job)
  - `404` — upload/job/domain not found
  - `429` — daily API quota exceeded (non-admin only; 50K/day)
  - `500` — internal error (logged to `journalctl -u lead-generation-platform.service`)
  - `503` — SQLite database locked (auto-retry with `Retry-After` header)

---

## 15. Rate limits & quotas

- **No per-user throttle on the enrichment API itself.**
- Upstream provider rates are managed internally with sliding windows + exponential backoff.
- Non-admin users are subject to a **50,000 requests/day** quota tracked in `daily_api_requests`.
- Admin users are unlimited.

---

## 16. End-to-end example — Flow 1 from scratch

```bash
# 1) Log in
TOKEN=$(curl -s -X POST https://listbuilding.eagleinfoservice.com/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"you@company.com","password":"..."}' | jq -r .access_token)

# 2) Upload CSV
UPLOAD=$(curl -s -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/upload \
  -H "Authorization: Bearer $TOKEN" -F "file=@leads.csv")
UPLOAD_ID=$(echo $UPLOAD | jq -r .upload_id)

# 3) Start Flow 1
JOB=$(curl -s -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/flows/domain-enrich \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"upload_id\":\"$UPLOAD_ID\",\"domain_col\":\"domain\",\"titles\":\"dentist,orthodontist,dmd\"}")
JOB_ID=$(echo $JOB | jq -r .job_id)

# 4) Poll status (or open SSE stream)
curl -s "https://listbuilding.eagleinfoservice.com/api/enrichment/jobs/$JOB_ID" \
  -H "Authorization: Bearer $TOKEN"

# 5) Download results
curl -s "https://listbuilding.eagleinfoservice.com/api/enrichment/jobs/$JOB_ID/download" \
  -H "Authorization: Bearer $TOKEN" -o enriched.csv
```

---

## 17. Support & troubleshooting

- **Service logs:** `journalctl -u lead-generation-platform.service -f`
- **Process health:** `curl http://localhost:8765/api/health` → `{"status":"ok"}`
- **DB lock:** `cd backend && sqlite3 data/jobs.db "PRAGMA wal_checkpoint(TRUNCATE);"`
- **PostgreSQL companion:** `sudo -u postgres psql -p 5433 lead_gen`
- **Contact:** arjav@eagleinfoservice.com

---

**Document version:** 1.0 — 2026-07-05
**Covers:** every enrichment endpoint live on this date, including the new `/api/enrichment/flows/help`.
