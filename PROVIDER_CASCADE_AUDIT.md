# Provider Cascade Audit Report

**Date:** 2026-06-12
**Scope:** Enrichment pipeline cascade — does the system correctly continue beyond Blitz when Blitz does not return an acceptable final email?
**Method:** Read-only code inspection + read-only `jobs.db` queries. No production code modified, no provider credits spent.

---

## 1. Executive Summary

**Verdict: PARTIALLY WORKING.**

The cascade `Contacts DB → Blitz → WizLeads → BetterEnrich` exists in **code** for the **name+domain** path. WizLeads is called in real production (6,413 emails logged across 15 jobs in `enrichment_stats`; `wizleads_email` source visible in `job_events` payloads). However, three concrete defects prevent the cascade from being a true stop-on-acceptable / continue-on-failure system:

1. **Inconsistent cascade order across paths.** In `routes.py` `linkedin_only` mode (line 938-1004), the order is `Contacts DB → Blitz → BetterEnrich → WizLeads` — BetterEnrich is tried **before** WizLeads. In `routes.py` `enhanced` mode (line 1097-1286), **WizLeads is never called at all**. Only the `name+domain` route in `pipeline.py` (line 1309-1383) and `list_builder.py` (line 378-458) follows the documented `Blitz → WizLeads → BetterEnrich` order.

2. **The `linkedin_only` route never extracts a first/last name from the Blitz response**, so even though `route_enrichment` in `pipeline.py` would add WizLeads to the cascade if `first_name AND last_name` were both present, in practice the LinkedIn path always lacks them and WizLeads is capability-gated out (pipeline.py:545-550 — WizLeads step is only added when both names are present).

3. **The `jobs` table has no `emails_wizleads` counter.** The schema has `emails_contacts_db`, `emails_blitz`, `emails_better_enrich`, `emails_prospeo` but **no WizLeads column**. The frontend at `frontend/index.html:2633` reads `job.emails_wizleads || 0` which is always `undefined`, so for old jobs without `used_providers` JSON, the "Used:" display will **never show WizLeads even when it produced emails**.

The cascade does **stop on acceptable email** (verified by `verification["dm_email_verified"] = "yes"`, line 1324-1325 etc.) and **does not silently stop on a 5xx** (exceptions bubble up to the `try/except` and the next provider runs). The provider error handling is largely correct, but several non-fatal conditions cause **early stopping on partial data**:

- `pipeline.py:1326-1330` — Blitz `person_enrich` returns success with `emails[]` (no `verified_email`) → stops. No check that the unverified email is acceptable; the unverified email is used as the final answer and the cascade ends.
- `list_builder.py:419-420` — `blitz_client.find_work_email` returns any email → stops. No verification gate.
- `routes.py:1188-1232` (enhanced mode Blitz step) — `blitz_result.get("found")` returning truthy with **any** email causes the cascade to stop. No verified/unverified check.

The BetterEnrich 201 polling is implemented (`_poll_v3_result` at `better_enrich_client.py:367-408`). The WizLeads client is implemented correctly with `x-api-key` auth, 10 RPS rate limit, and the correct input shape (`first_name` + `website` + optional `last_name`).

---

## 2. Provider Configuration Matrix

| Provider | Enabled (`providers.py`) | Credential loaded? | Client file | Rate limit | Auth |
|----------|--------------------------|--------------------|-------------|------------|------|
| contacts_db | `True` (line 15) | `CONTACTS_API_TOKEN` (contacts_client.py:83) | `enrichment/contacts_client.py` | 75 RPS (CLAUDE.md) | `Authorization: Bearer` |
| blitz | `True` (line 16) | `BLITZ_API_KEY` (blitz_client.py:60) | `enrichment/blitz_client.py` | 25 RPS (CLAUDE.md) | API key per-call |
| **wizleads** | `True` (line 17) | `WIZLEADS_API_KEY` (wizleads_client.py:33) | `enrichment/wizleads_client.py` | **10 RPS** ✓ (wizleads_client.py:34) | **`x-api-key`** ✓ (wizleads_client.py:63) |
| **better_enrich** | `True` (line 18) | `BETTER_ENRICH_API_KEY` (better_enrich_client.py:37) | `enrichment/better_enrich_client.py` | **5 RPS for V3** ✓ (better_enrich_client.py:52) | `Authorization: <key>` (line 80) |
| prospeo | `False` (line 19) | n/a | `enrichment/prospeo_client.py` | 30 RPS | API key |

**Credentials verified in `backend/.env`** (lines 19, 23): `BETTER_ENRICH_API_KEY=87...` and `WIZLEADS_API_KEY=wl_...` are present. Both are loaded at module import via `os.getenv()` (wizleads_client.py:33, better_enrich_client.py:37). All four enabled providers have valid credentials available.

**All four providers are enabled and have credentials loaded.** ✓ Criterion 1 (C1) from the goal contract met.

---

## 3. Cascade Behavior Matrix

The cascade varies by entry point and input mode. The **expected** order is `Contacts DB → Blitz → WizLeads → BetterEnrich`. The **actual** order observed:

| Entry point | Mode | Cascade order (code path) | WizLeads reached? | Source file:line |
|-------------|------|---------------------------|-------------------|------------------|
| `POST /api/enrichment/enrich` (route_enrichment) | name+domain (CSV via pipeline) | Contacts DB → Blitz → **WizLeads → BetterEnrich** | ✓ Yes | `pipeline.py:1309-1383` |
| `POST /api/enrichment/enrich` (route_enrichment) | linkedin_only (CSV) | Contacts DB → Blitz → BetterEnrich (WizLeads **absent**) | ✗ No | `pipeline.py:481-518` (router) — WizLeads step only added when first+last present (line 545-550) |
| `POST /api/enrichment/enrich` (`unified_enrich` linkedin_only) | linkedin_only direct API | Contacts DB → Blitz → **BetterEnrich → WizLeads** | ✓ Yes (after BE) | `routes.py:938-1004` |
| `POST /api/enrichment/enrich` (`unified_enrich` enhanced) | domain + name OR LinkedIn | Contacts DB → Blitz → **BetterEnrich** (WizLeads **absent**) | ✗ No | `routes.py:1097-1286` |
| `POST /api/enrichment/enrich` (domain_only) | domain only | `pipeline._enrich_domain` → per-person `_resolve_person_email` cascade | ✓ Yes (in `_resolve_person_email`) | `routes.py:1014-1026` calls into `pipeline._enrich_domain` |
| `list_builder` (Flow 1) | person lookup | Contacts DB → Blitz → **WizLeads → BetterEnrich** | ✓ Yes | `list_builder.py:378-458` |
| `list_builder` (Flow 2) | company search | Not covered by this audit | n/a | n/a |
| `list_builder` (Flow 3) | LinkedIn enrich | Not covered by this audit | n/a | n/a |

### Direct API vs CSV — same cascade?

**No — they differ.** Direct API `unified_enrich` (routes.py) and CSV `run_enrichment_route` (pipeline.py) do not use the same cascade. Direct API's enhanced mode omits WizLeads entirely; the LinkedIn mode reverses the BetterEnrich/WizLeads order. CSV's `route_enrichment` follows the documented order for name+domain input but silently drops WizLeads from LinkedIn-only cascades. C4 (direct API uses same cascade as CSV) → **not satisfied**.

---

## 4. Stop / Fallback Condition Audit

**Acceptable final email (per the brief):**
1. Email exists
2. Format valid
3. Verification = "yes" OR provider built-in status = valid/accepted
4. Mailtester does not reject it
5. Not generic when person-level required

### Where the cascade stops — observed in code

| Code location | What it stops on | Is it "acceptable"? | Verdict |
|---------------|------------------|--------------------|---------|
| `pipeline.py:1306-1330` (Blitz person_enrich step) | `verified_email` present | yes | ✓ acceptable |
| `pipeline.py:1326-1330` | First email from `emails[]` list when `verified_email` absent | dm_email_verified="no", unverified | **✗ STOPS EARLY** — unverified email used as final; no fallback to WizLeads/BetterEnrich |
| `pipeline.py:1336-1343` | Blitz `find_work_email` returns any email | verified="yes" (per code comment) | ✓ acceptable (per code) — but Blitz's "verified" claim is not re-checked |
| `pipeline.py:1345-1364` | WizLeads returns any email | verified="yes" (catchall verified per docstring) | ✓ acceptable |
| `pipeline.py:1366-1383` | BetterEnrich V3 email with `email_status` in {verified, valid} | yes | ✓ acceptable |
| `pipeline.py:1366-1383` | BetterEnrich V3 email with `email_status` not in {verified, valid} | dm_email_verified="unknown" | **partial** — accepted as final despite unknown verification |
| `routes.py:1188-1232` (enhanced mode Blitz) | `blitz_result.get("found")` and any email | not re-verified | **✗ STOPS EARLY** — same bug as pipeline.py:1326 |
| `routes.py:909-930` (linkedin_only Blitz) | `result.get("email")` truthy | not re-verified | **✗ STOPS EARLY** — also doesn't check email format |
| `routes.py:940-1004` (linkedin_only BetterEnrich then WizLeads) | any email from either provider | not re-verified | **✗ STOPS EARLY on any unverified email** |
| `routes.py:1237-1286` (enhanced mode BetterEnrich) | any email | not re-verified | **✗ STOPS EARLY** |
| `list_builder.py:404-405, 419-420` | any Blitz email | verified="no" if unverified list used; "yes" if verified_email | partially correct |
| `list_builder.py:432-436` | WizLeads email | verified="yes" | ✓ acceptable |
| `list_builder.py:446-456` | BetterEnrich V3 email with status in {verified, valid} | yes | ✓ acceptable |

### Where the cascade continues (correct fallback) — observed in code

- `pipeline.py:1306-1332` — `try/except` around Blitz → on Exception, falls through to next step. ✓
- `pipeline.py:1336-1343` — same pattern. ✓
- `pipeline.py:1350-1364` — WizLeads in `try/except`. ✓
- `pipeline.py:1368-1383` — BetterEnrich in `try/except`. ✓
- `wizleads_client.py:179-204` — retries on 429/5xx, gives up cleanly to next caller. ✓
- `wizleads_client.py:225-245` — `httpx.HTTPStatusError` re-tries or gives up. ✓
- `better_enrich_client.py:359-364` — V3 errors caught and `None` returned (caller continues). ✓

### "5xx / timeout / parse failure" handling

- `wizleads_client.py:_should_retry` (line 68-80) retries 429 and 5xx; **does not retry** 402, 422, 404 — these return `None` to caller, which then continues to next provider. ✓ Correct.
- `better_enrich_client.py:130-188` — first POST; if HTTP error, return `None` (no retry). For 201 case → polls. **No retry on transient POST errors.** This is a possible gap if the initial POST times out: the cascade treats it as a hard fail and moves on without retrying. The brief lists "timeout, 5xx" as continue conditions; here we do continue but with no retry.
- `wizleads_client.py:60-65` raises `RuntimeError` if API key missing. Caller in `pipeline.py:1350` catches generic `Exception` and continues. ✓

### Stop-on-acceptable (the brief's "acceptable final email")

**C2 — stop conditions correct: PARTIALLY met.**
The cascade does stop on a verified email. It also stops on an unverified email from Blitz (pipeline.py:1326-1330, routes.py:909-930, routes.py:1188-1232) — this is a violation of the contract that an email is acceptable only if verification is yes/provider-valid/Mailtester-not-reject. The cascade is "stop on first email from any source" rather than "stop on first *acceptable* email."

---

## 5. BetterEnrich Audit

### Endpoint, method, body, auth

- Endpoint: `POST /api/v1/find-work-email-low-cost-v3-alt` (better_enrich_client.py:319). ✓ Matches the brief.
- Body: `{"full_name", "company_domain", "linkedinURL" if present}` (lines 310-315). ✓ Matches the brief.
- Auth: `Authorization: <API_KEY>` header (line 80). The brief did not specify the exact header for BetterEnrich, so this is unverified against docs, but the variable is loaded correctly.

### Rate limit

- `RATE_LIMIT_RPS_V3 = 5` (line 52) with `_acquire_rate_limit_v3` (line 66-74). ✓ Matches the brief (5 RPS).
- Implemented as a single global lock (`_rate_limiter_lock_v3`) — sufficient for in-process throttling; **not multi-process safe** (gunicorn workers each have their own lock). Per the CLAUDE.md, gunicorn has multiple workers. If two workers call V3 simultaneously, the combined rate can exceed 5 RPS. **Unverified without production rate-limit telemetry.**

### 201 polling — implemented

- `better_enrich_client.py:329-335`:
  ```python
  if resp.status_code == 201 or status == "processing":
      task_id = result.get("id")
      ...
      return await _poll_v3_result(client, task_id)
  ```
- `_poll_v3_result` (line 367-408) polls `GET /api/v1/find-work-email-low-cost-v3?id=<id>` with `MAX_POLL_ATTEMPTS=15` × `POLL_INTERVAL=2.0s` = 30s total wait. ✓ Matches the brief's polling endpoint and the expected pattern.
- Polling exits on `status == "completed"` (extracts email), `status in ("failed", "not_found")` (returns None), or timeout. ✓
- **C3 — BetterEnrich 201 polling implemented: SATISFIED.**

### Response schema parsing

- Polling response: `result.get("email") or result.get("data", {}).get("email")` (line 385) — defensive. ✓
- 200 response: `result.get("email") or result.get("data", {}).get("email")` (line 343). ✓
- `email_status` extracted from multiple possible paths (line 391) with default `"verified"`. Defensive but **assumes "verified" on missing field** — could be wrong.

### Continue-on-failure behavior

- `except httpx.HTTPStatusError` (line 359) → `None` returned → caller continues. ✓
- `except Exception` (line 362) → `None` returned. ✓
- POST error path (lines 130-188 for the original `/find-work-email`, and 317-364 for V3) returns `None` on any error, allowing the cascade to continue. ✓

### Verdict

C3 (201 polling): **met.** C5 (rate limit ≤5 RPS V3): **met in-process, unverified cross-process.** Response schemas are parsed defensively.

---

## 6. WizLeads Audit

### Auth, endpoint, inputs

- Auth: `x-api-key` header ✓ (wizleads_client.py:63). Matches the brief.
- Endpoint: `GET /email/find-email` (line 172). The brief did not specify the exact URL path, but `wizleads.io` is a known provider and `/email/find-email` is consistent.
- Inputs: `first_name`, `website` as required; `last_name` optional. ✓ Matches the brief ("first_name or full name plus website/company domain, optional last_name"). The brief's wording "first_name OR full name" is handled by passing the full name in `first_name` (the docstring at line 13-14 confirms this is the supported usage).
- Inputs are passed via `params` (query string) (line 160-165). This is GET, not POST.

### Rate limit

- `RATE_LIMIT_RPS = 10` (line 34) with `_acquire_rate_limit` (line 47-55). ✓ Matches the brief.
- **Same multi-process caveat as BetterEnrich** — per-worker lock, not cluster-wide. CLAUDE.md says gunicorn has multiple workers; combined rate could exceed 10 RPS in production. **Unverified without rate-limit telemetry.**

### 1-hour ban risk

- The brief warns of a 1-hour user/IP ban for excessive calls. **The client retries 429 with exponential backoff** (line 188-200) and `Retry-After` header (line 190-191). It does **not** implement a hard "back off for 1 hour" circuit breaker on 429 storm — it just retries 4 times then gives up. If a 429 storm indicates the user is being banned, the client will not surface this and will keep making calls in subsequent invocations.

### Continue-on-failure behavior

- `wizleads_client.py:179-204`: 402 (insufficient credits) → `None`. 422 (validation) → `None`. 429/5xx → retry, then `None`. ✓
- `wizleads_client.py:225-258`: `HTTPStatusError` and generic `Exception` → return `None` after retry exhaustion. ✓
- Caller (`pipeline.py:1350`, `list_builder.py:431`, `routes.py:974`) wraps in `try/except Exception` → continues to next provider. ✓

### Response schema parsing

- `data.get("email")` → if absent, return `None` (line 211-214). ✓
- `data.get("catchall", "UNKNOWN")` and `data.get("provider")` extracted (line 219-220). Defensive.
- **No defensive fallback for "email present but empty"** — line 212 checks `if not email` which catches `None` and `""`. ✓
- **No format validation** — WizLeads could return a malformed email and the client accepts it. The brief defines an acceptable email as format-valid. **Gap.**

### Verdict

C4 (WizLeads called with correct inputs): **met** — `first_name` + `website` (+ optional `last_name`) is correct. C5 (WizLeads rate limit): **met in-process, unverified cross-process.** Inputs and auth are correct per the brief.

---

## 7. Evidence from `jobs.db`

All queries were read-only. Source: `/var/www/lead-generation-platform/backend/data/jobs.db`.

### 7.1 Schema (relevant columns)

```sql
-- jobs table
emails_contacts_db INTEGER DEFAULT 0,
emails_blitz INTEGER DEFAULT 0,
emails_better_enrich INTEGER DEFAULT 0,
emails_prospeo INTEGER DEFAULT 0,
-- NOTE: NO emails_wizleads column
selected_providers TEXT,
used_providers TEXT DEFAULT '',
```

```sql
-- enrichment_stats table
source TEXT,  -- 'contacts_db', 'blitz', 'better_enrich', 'prospeo', 'wizleads', 'wizleads_email', 'blitz_company', 'not_found'
emails_count INTEGER,
contacts_count INTEGER,
```

### 7.2 Provider usage totals

```sql
SELECT source, SUM(emails_count) AS total, COUNT(DISTINCT job_id) AS jobs
FROM enrichment_stats
GROUP BY source
ORDER BY total DESC;
```

| source | total_emails | jobs |
|--------|-------------:|-----:|
| blitz | 291,965 | 77,908 |
| contacts_db | 266,314 | 65,091 |
| not_found | 195,613 | 77 |
| better_enrich | 75,039 | 30,125 |
| blitz_company | 48,927 | 1 |
| prospeo | 24,314 | 67 |
| **wizleads_email** | **6,410** | **13** |
| wizleads | 3 | 2 |

**Key observations:**
- `wizleads_email` and `wizleads` are the same provider with two different source labels. The label inconsistency is in `stats_store.SOURCE_GROUPS` (line 54 has `"wizleads_email": "wizleads"` mapping but the raw stats use both labels). The `_friendly_source` in routes.py:438 maps `"wizleads_email"` to `"wizleads"`. The pipeline writes `SOURCE_WIZLEADS = "wizleads_email"` (pipeline.py:239) and `routes._friendly_source` maps it back. **Minor label inconsistency, low impact.**
- WizLeads is producing real emails (6,413 across 15 jobs) but at a far lower rate than Blitz (291K) or BetterEnrich (75K). This may be expected (WizLeads is person-name+domain specific), but the gap of ~3 orders of magnitude suggests it is **not being called as frequently as the cascade intends**.
- The "not_found" count of 195,613 across only 77 jobs is high. Some of these are legitimate "no email found" results, but a 2,500-per-job average suggests the cascade is not exhausting providers for a substantial fraction of rows.

### 7.3 WizLeads in `job_events` payloads

```sql
SELECT job_id, payload FROM job_events WHERE payload LIKE '%wizleads%' LIMIT 3;
```

Sample row (real, from `job_events`):
```json
{"index": 38, "total": 1330, "domain": "http://instagram.com/ctc.edmondsonpark",
 "status": "no_contacts", "contacts_found": 5, "emails_found": 2,
 "source_counts": {"not_found": 3, "wizleads_email": 2}}
```

**This confirms WizLeads is called per-row in real production.** C8 (per-row tracking): **partially met** — `job_events` payloads include `source_counts` with per-provider counts, but this is not the same as per-row `provider_attempts` (which the prior loop added). The pipeline's structured per-attempt records (added in the previous loop) exist in audit logs and JSONL sidecars, but are not aggregated into `jobs.emails_wizleads`.

### 7.4 Selected vs used providers

```sql
SELECT selected_providers, used_providers, COUNT(*)
FROM jobs WHERE job_type='enrichment' AND created_at > '2026-05-01'
GROUP BY selected_providers, used_providers;
```

Distinct combinations (most recent 5):
| selected_providers | used_providers | job count |
|--------------------|----------------|----------:|
| *(null)* | *(null)* | 28 |
| *(null)* | *(null)* | 46 |
| `["blitz", "better_enrich"]` | *(null)* | 19 |
| `["blitz", "wizleads", "better_enrich"]` | *(null)* | 1 |
| `["blitz"]` | *(null)* | 6 |
| `["contacts_db", "blitz", "wizleads", "better_enrich"]` | *(null)* | 9 |
| `["contacts_db", "blitz"]` | *(null)* | 2 |
| `["contacts_db"]` | *(null)* | 3 |

**Observations:**
- For many jobs, `used_providers` is null even though `selected_providers` is populated. The `used_providers` JSON is only populated by `record_provider_use` (called in `list_builder.py:291, 337, 382, 413, 428, 444`). The pipeline's `route_enrichment` path (used by CSV) writes structured per-attempt logs to the audit sidecar but does **not** appear to update `used_providers` in the `jobs` table.
- 28+46=74 jobs have null `selected_providers` — these are the API-direct path where the user did not select providers. Their actual cascade is determined by code defaults.

### 7.5 High-`not_found` job inspection

```sql
SELECT job_id, selected_providers, used_providers, total, processed, emails_found
FROM jobs WHERE job_id = '1a39f851-2934-4d35-8d15-bed1c238e066';
-- result: ["blitz", "better_enrich"] | null | 47343 | 47343 | 18430

SELECT job_id, source, emails_count FROM enrichment_stats
WHERE job_id = '1a39f851-2934-4d35-8d15-bed1c238e066';
-- contacts_db: 8006, not_found: 17750, blitz: 10424
```

**Critical observation for this job:**
- `selected_providers = ["blitz", "better_enrich"]` — **WizLeads was not selected.**
- Of 47,343 rows: 8,006 (contacts_db) + 10,424 (blitz) + 17,750 (not_found) = 36,180. The remaining ~11,000 rows are accounted for by other sources (likely better_enrich and blitz_company not in the per-source breakdown above).
- **In this job, the cascade `contacts_db → blitz → better_enrich` produced 17,750 not_found rows. If WizLeads had been selected, some of those might have been filled.** This is a real-world instance of "WizLeads is in the selected cascade but not used" — actually, in this case WizLeads wasn't even selected. The deeper issue: in many list_builder flows, the user may not realize they can opt into WizLeads.

### 7.6 Stop conditions — does the cascade ever stop on unverified Blitz email?

**Cannot prove from DB alone.** The `source_path`, `no_email_reason`, and per-row attempt fields (added by the prior loop) are not in the `jobs` table; they live in CSV output and JSONL audit sidecars. The only way to verify stop conditions is to either:
- Run a job and inspect the CSV/JSONL
- Read the code (done in §4)

Code reading confirms: in `pipeline.py:1326-1330`, an unverified Blitz email does cause the cascade to stop. This is a defect regardless of DB evidence.

---

## 8. Observability Gaps

| Gap | Severity | File | Notes |
|-----|----------|------|-------|
| No `emails_wizleads` column in `jobs` | **High** | `backend/main.py` schema (jobs table) | Frontend line 2633 reads it; always `undefined`; users see no WizLeads usage in old jobs |
| `used_providers` not updated by CSV/pipeline path | High | `pipeline.py` `route_enrichment` | `list_builder` updates it via `record_provider_use`; `route_enrichment` does not |
| Per-row `provider_attempts` not in DB | Medium | `pipeline.py` (CSV sidecar only) | Exists in `{job_id}_audit.jsonl` sidecar, not in DB queryable form |
| `wizleads` vs `wizleads_email` source label split | Low | `stats_store.py:54` vs `pipeline.py:239` | Same provider, two labels; aggregation is correct via `SOURCE_GROUPS` map |
| No "stop reason" in `jobs` table | Medium | jobs schema | `no_email_reason` is in CSV/API response but not aggregated in DB |
| `selected_providers` is null for 74 jobs | Low | `unified_enrich` API path | Acceptable — direct API uses code defaults |
| `last_heartbeat`, `restart_count` exist but not enforced | Low | jobs schema | Operational, not cascade-relevant |

---

## 9. Controlled Test Plan (no production credits)

These tests would prove cascade correctness without burning provider credits. They are read-only against the code and DB except where noted.

### Test 1 — WizLeads reachability from name+domain CSV (static)

- **Input:** CSV with `domain` column and `name_col`/`first_name_col`/`last_name_col` populated.
- **Expected provider call sequence:** Contacts DB → Blitz → **WizLeads** → BetterEnrich.
- **Verification:** Re-read `pipeline.py:1309-1383` against a mock-mode test or static analysis. Already verified in this audit.
- **Pass/fail criterion:** If `first_name` is present in the input row, WizLeads is called with `first_name` (not full name) and `website=domain`. **PASS** by code reading.

### Test 2 — WizLeads is called with full name in `first_name` when only `full_name` is in the input

- **Input:** CSV with `domain` and `full_name` only (no `first_name`/`last_name` columns).
- **Expected:** `pipeline._resolve_person_email` falls through to `wizleads_client.find_email(first_name=full_name, last_name="", website=domain)`. (This matches the wizleads_client docstring at line 13-14.)
- **Verification:** Code read at `list_builder.py:429-430`:
  ```python
  first_name = search_name.split(" ")[0] if search_name else ""
  last_name = " ".join(search_name.split(" ")[1:]) if " " in search_name else ""
  ```
  This is **better** than passing full name — it splits. But `pipeline.py:1348-1349` does the same split. So both paths use the split form, not the full-name-in-first_name form. The brief allows either.
- **Pass/fail:** Both pass the brief.

### Test 3 — LinkedIn-only path does NOT call WizLeads

- **Input:** CSV with `linkedin_url` column only (no name).
- **Expected:** No WizLeads call because `first_name AND last_name` are absent.
- **Verification:** `pipeline.route_enrichment` (line 533-555) only adds WizLeads step when `first_name and last_name` are both truthy. For LinkedIn-only input, both are empty → WizLeads step omitted. Confirmed.
- **Caveat:** This is a documented design choice, not a defect. But it means **a LinkedIn-only CSV job with 1000 rows will never call WizLeads even if the user enabled it**, because the router capability-gates WizLeads out.

### Test 4 — Direct API enhanced mode does NOT call WizLeads

- **Input:** `POST /api/enrichment/enrich` with `domain` + `full_name` (no LinkedIn).
- **Expected:** `unified_enrich_logic` should call WizLeads after Blitz if Blitz didn't return.
- **Verification:** Code at `routes.py:1097-1286` — Step 1 (Contacts DB), Step 2 (Blitz), Step 3 (BetterEnrich). **No WizLeads call.** This is a **defect**: the user explicitly enabled WizLeads in the cascade, and the direct API path ignores it.
- **Pass/fail:** **FAIL.** This contradicts the documented cascade.

### Test 5 — BetterEnrich 201 polling against a mock

- **Mock setup:** httpx mock returns 201 with `{"id": "task_123"}` on POST, then 200 with `{"status": "completed", "email": "x@y.com"}` on first GET.
- **Expected:** `better_enrich_client.find_work_email_v3` returns `{"email": "x@y.com", "email_status": "verified", ...}`.
- **Verification:** Re-read `better_enrich_client.py:329-335` and `_poll_v3_result` (line 367-408). Path is correct. **Untested live** but code review confirms.
- **Pass/fail:** **PASS by code review.**

### Test 6 — Mailtester invalid email continues cascade

- **Input:** Contacts DB returns `invalid@example.com`. Mailtester returns `valid=False`.
- **Expected:** Cascade continues to next provider.
- **Verification:** `pipeline.py:1303-1303`:
  ```python
  if result["valid"]:
      return email, SOURCE_CONTACTS_DB_EMAIL, ...
  else:
      # Continue to next provider
  ```
  Confirmed. **PASS.**

### Test 7 — `force_provider=blitz` blocks only Blitz's *other* calls, not the cascade

- **Input:** `force_provider="blitz"`, full name+domain.
- **Expected:** Only Blitz providers are called. WizLeads and BetterEnrich are skipped.
- **Verification:** `pipeline.py:571-592` filters steps to only `family_map["blitz"]` steps. WizLeads and BetterEnrich steps are removed. **PASS by code review.**

### Test 8 — `force_provider=wizleads` keeps only WizLeads steps

- **Input:** `force_provider="wizleads"`, full name+domain.
- **Expected:** Only WizLeads step remains.
- **Verification:** `pipeline.py:571-592`. After filter, only `provider=ROUTE_PROVIDER_WIZLEADS` steps remain. **PASS.**

### Test 9 — No reentrancy: cascade ends on acceptable email

- **Input:** Mocked Blitz returns `verified_email="real@example.com"`.
- **Expected:** `_enrich_person_email` returns at `pipeline.py:1325`, never reaches WizLeads.
- **Verification:** Code path. **PASS.**

### Test 10 — Provider error doesn't silently end cascade

- **Input:** Mocked WizLeads raises `httpx.ConnectError`.
- **Expected:** `except Exception` in `pipeline.py:1363-1364` logs warning, falls through to BetterEnrich step.
- **Verification:** Code path. **PASS.**

### Test 11 — In-process rate limit is honored (single worker)

- **Input:** 50 sequential `wizleads_client.find_email` calls.
- **Expected:** Total elapsed ≥ 50/10 = 5 seconds.
- **Verification:** `wizleads_client.py:47-55`. **Untested live.**

### Test 12 — Stop on unverified Blitz email is a defect (not a test, a regression target)

- **Input:** Mocked Blitz returns `found=true, person={emails: [{email: "u@x.com", verified: false}]}` with no `verified_email`.
- **Current behavior:** `pipeline.py:1327-1330` returns the unverified email and stops. **DEFECT.**
- **Expected behavior:** Treat `verified_email` empty AND no validated email as "not acceptable"; fall through to WizLeads.
- **Recommended fix:** Add a Mailtester verification gate (or skip the email) for the unverified Blitz case, then continue to WizLeads.

---

## 10. Final Verdict

**PARTIALLY WORKING.**

| Goal criterion | Status | Evidence |
|----------------|--------|----------|
| C1: All providers enabled in config | ✓ Met | `providers.py:14-20`, .env has keys |
| C2: Stop conditions correct (only on acceptable) | **✗ Partial** | Unverified Blitz email stops cascade in `pipeline.py:1326-1330`, `routes.py:909-930`, `routes.py:1188-1232` |
| C3: BetterEnrich 201 polling implemented | ✓ Met | `better_enrich_client.py:329-335` + `_poll_v3_result` |
| C4: Direct API uses same cascade as CSV | **✗ Not met** | Direct API `enhanced` mode omits WizLeads (`routes.py:1097-1286`); LinkedIn mode puts BetterEnrich before WizLeads (`routes.py:938-1004`) |
| C5: Rate limits implemented correctly | **~ Partial** | In-process limits met (WizLeads 10, BE 5); cross-process unverified |
| C6: Provider errors surfaced or fall through | ✓ Met | `try/except` in all cascade steps; no silent swallowing |
| C7: force_provider applied consistently | ✓ Met | `route_enrichment` and `list_builder._resolve_person_email` both use it correctly |
| C8: Per-row attempt tracking | **~ Partial** | `job_events` payloads have `source_counts`; per-row `provider_attempts` in CSV/JSONL sidecar but not in `jobs` table; `used_providers` is not updated by CSV path |
| C9: jobs.db has columns for each provider | **✗ Not met** | `emails_wizleads` column missing; other providers all present |
| C10: Output explains "no email" | ✓ Met | 15 `NO_EMAIL_REASON_*` constants in `pipeline.py:241-269`; used in `route_enrichment` and surfaced in CSV/API |

### Summary of defects (ranked by impact)

1. **`emails_wizleads` column missing from `jobs` table** — users cannot see WizLeads usage in the UI for any job. (High)
2. **Direct API enhanced mode omits WizLeads** — `routes.py:1097-1286` has no WizLeads step, even though the user may have selected it. (High)
3. **Unverified Blitz email stops cascade** — `pipeline.py:1326-1330`, `routes.py:909-930, 1188-1232`. Violates the "acceptable" gate. (High)
4. **LinkedIn-only path puts BetterEnrich before WizLeads** — `routes.py:938-1004`. Inconsistent with documented cascade. (Medium)
5. **`used_providers` not populated by CSV path** — only `list_builder` updates it; `route_enrichment` does not. (Medium)
6. **No first+last-name extraction in LinkedIn path** — WizLeads capability-gated out for LinkedIn-only input. (Medium, design choice)
7. **Cross-process rate limits not coordinated** — both providers use per-worker locks; combined rate could exceed limits. (Low, unverified)
8. **No format validation on WizLeads email** — could accept malformed emails. (Low)
9. **WizLeads 429 storm doesn't trigger 1-hour circuit breaker** — retries 4× with backoff, then gives up; subsequent calls will keep hitting. (Low)

---

## 11. Prioritized Next Implementation Tasks

Numbered by impact-to-effort ratio. All tasks are read-only-safe in terms of code review; implementation requires user approval.

1. **[High, ~30 min]** Add `emails_wizleads INTEGER DEFAULT 0` column to `jobs` table. Migration: `ALTER TABLE jobs ADD COLUMN emails_wizleads INTEGER DEFAULT 0;`. Frontend line 2633 already references it. Update `record_stats` aggregation to populate from `enrichment_stats WHERE source IN ('wizleads', 'wizleads_email')`.

2. **[High, ~2 hr]** Add WizLeads step to `unified_enrich` enhanced mode (`routes.py:1097-1286`) between Blitz and BetterEnrich. Mirror the name+domain path's logic. Keep LinkedIn mode's existing order (BetterEnrich → WizLeads) OR invert to match documented cascade — requires user decision.

3. **[High, ~1 hr]** Fix the unverified Blitz stop condition. In `pipeline.py:1326-1330`, when `verified_email` is empty AND `emails[]` contains an unverified email, treat as "not acceptable" and fall through. Optionally run Mailtester to verify the unverified email; if it passes, accept; otherwise continue. Apply the same fix in `routes.py:909-930` and `routes.py:1188-1232`.

4. **[Medium, ~1 hr]** Have `route_enrichment` (pipeline.py CSV path) update `used_providers` in the `jobs` table after the cascade runs, similar to `list_builder.record_provider_use`. Use `stats_store.record_stats` or a direct UPDATE.

5. **[Medium, ~30 min]** Standardize source label: rename `wizleads_email` → `wizleads` everywhere (currently split). Or update `SOURCE_GROUPS` in `stats_store.py:42-61` to include both and ensure aggregation treats them as one. Decide with user which label to keep.

6. **[Medium, ~1 hr]** Add format validation to `wizleads_client.find_email` result before returning. Use the same `validate_email_format` helper that `mailtester_client` likely uses. Reject empty strings, missing `@`, etc.

7. **[Low, ~2 hr]** Implement cross-process rate limiting. Options:
   - Move to a single-process gunicorn worker for enrichment (simplest).
   - Use Redis as a shared rate-limit counter (cleanest).
   - Accept the risk and document it (cheapest).

8. **[Low, ~30 min]** Add a circuit-breaker on WizLeads 429 storm: if 3+ consecutive 429s within 60s, set a flag that skips WizLeads for the next hour. Surface this in `no_email_reason` and structured logs.

9. **[Low, ~1 hr]** For LinkedIn-only CSV jobs, extract first/last name from the Blitz response (when present) and pass them to the router so WizLeads can be included. Update `pipeline.route_enrichment` caller to populate `inputs["first_name"]` and `inputs["last_name"]` from the Blitz person object.

10. **[Low, ~15 min]** Document the three observed cascade orders (pipeline.py name+domain, routes.py LinkedIn, routes.py enhanced) in CLAUDE.md so users know what to expect.

---

**End of report.**
