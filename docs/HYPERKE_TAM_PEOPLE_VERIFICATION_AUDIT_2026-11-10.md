# Hyperke TAM, People Finder, and Email Verification — Read-Only Audit Report

**Audit date:** 10 November 2026
**Audited system:** ListBuilding platform — https://listbuilding.eagleinfoservice.com
**Primary repo:** `/var/www/lead-generation-platform` (git, branch `feat/moltsets-cascade`, commit `384e33b`, identical to `master`; working tree clean apart from untracked artifacts)
**Brief:** `docs/Hyperke_TAM_People_Finder_Audit_Brief_2026-11-10.md`
**Mode:** Read-only. No code changes, no jobs started, no provider calls placed, no data mutated. All database access was `sqlite3 -readonly` SELECTs against `backend/data/jobs.db` and one public unauthenticated `GET https://leadsdatabase.cc/stats.json`.

---

## 1. Executive verdict

**What exists today (confirmed deployed and observed working):**

1. **Stage 1 (Account TAM)** — substantially exists. The **Find Companies (TAM)** page (`/#/search`) drives `POST /api/enrichment/flows/tam`: a persona-filtered, cursor-paginated company discovery over **Blitz** (`/v2/company/tam-by-people`, 1 FUP record per returned company, empty page free), with a **GetLeads** top-up leg (500-credit cap, weaker filter parity), a **lookalike mode** (3–5 seed companies → ranked similar companies, match_score 0–100), a 21-column CSV export, automatic write-back of every domain-bearing company to the contacts DB, and optional one-click chaining into Flow-1 contact enrichment. 84 production TAM jobs to date; runs of 500–4,045 companies observed `done`.
2. **Stage 2 (company → people + emails)** — exists **but not on the page the brief assumes**. The `/#/people` ("Find People") page is an **internal-DB-only person search** (title/country/seniority/function/industry/universe filters; ≤200 rows; no company identifier input of any kind). The actual company-scoped people finder is the **Upload Domains page (Flow 1, `/api/enrichment/flows/domain-enrich`)** and its LinkedIn sibling (**Flow 3**), which run the full 9-provider discovery+email waterfall as background jobs at production scale (a 14,415-domain job completed 2026-11-09). Flow 1 accepts `domain` (required column) plus optional company-LinkedIn/person-LinkedIn/name columns, with company-LinkedIn taking precedence over domain at row level — matching the brief's stated priority for the two identifiers it supports. **`company_name` is not a discovery key** (name-only rows are skipped).
3. **Stage 3 (email verification)** — **partially exists, embedded only.** MailTester (self-hosted proxy `validation.hyperke.org/ninja`) verifies contacts-DB and unverified-provider emails inline during enrichment (181K calls in the last 7 days), producing `ok` / `mb` (catch-all) / `ko` codes. Production policy `MAILTESTER_ACCEPT_CODES=ok` **drops catch-all emails by default**. There is **no standalone verification endpoint, page, job type, or batch-upload flow** — an operator cannot today verify a selected subset of an exported list.

**What is missing / broken for the proposed 90% workflow:**

- No UI path accepts a mixed company list (LinkedIn-only / domain-only / name-only rows) in one job; Flow 1 hard-requires a `domain` column, and the TAM chain hands off **only domain-bearing rows** (LinkedIn-only TAM results are silently excluded from chaining).
- `/#/people` cannot scope by company and cannot trigger enrichment — it is a read-only internal-DB query, mislabeled by the brief's assumption as the Stage-2 workhorse.
- No standalone verification surface; catch-all semantics exist (`mb` → `valid_catch_all` only on the QuickEnrich direct endpoint via `allow_catch_all`) but are not exposed anywhere an operator can reach for arbitrary lists.
- No person-location filter in Stage 2 (exists only as a TAM people-filter on the Blitz leg).
- Positive verification results are **not persisted** to the contacts DB (only `mark_email_invalid` negatives are), so re-runs re-verify and re-bill attention on the same addresses.
- Operator-facing docs are materially stale (Help page lists 6 sources and says BetterEnrich/Prospeo are disabled; `/flows/help` cascade table omits LeadOcean and MoltSets; the People page banner says "6.6M+ leads" vs. 30.8M actual persons).
- No signals/discovery playbook exists in the repo; directory scraping, Apollo, DiscoLike are not integrated (Outscraper exists as VPS services + a data-source tag only; Clay is a *client* of this API, never a dependency).

**Feasibility verdict:** the three-step process is **feasible with a modest, reuse-heavy extension** (estimated P0 ≈ 8–14 person-days). The heavy machinery — provider waterfall, job system, SSE progress, crash-safe resume, CSV exports, contacts write-back, verification client — is built, deployed, and battle-tested at 10K+-row scale. The gaps are input flexibility, one new company-scoped search surface, one new verification job, and count/coverage surfacing.

**Top recommendation (detail in §10):** **Option A — "Minimal extension"**: keep TAM + Flow 1 + Jobs as the spine; (1) make Flow 1 identifier-flexible, (2) add a company-scoped people-search mode to `/#/people` that rides the existing waterfall, (3) add a standalone verification job + small tab reusing `mailtester_client`. Defer any project-shell UI (Option B) until the backend pieces prove out. **Confidence: high** (all load-bearing claims verified in code at cited lines, with production DB evidence).

---

## 2. Scope, access, and evidence

### 2.1 Repositories, branches, deployments

| Component | Location | Version / state | Evidence |
|---|---|---|---|
| ListBuilding platform (backend + frontend) | `/var/www/lead-generation-platform` | branch `feat/moltsets-cascade` @ `384e33b` (2026-11-08), == `master`; tree clean | E-001 |
| Backend service | systemd `lead-generation-platform.service`, gunicorn 4× uvicorn workers, port 8765, `--timeout 600` | **active**, `/api/health` → `{"status":"ok","mcp_enabled":true}` (2026-11-10) | E-001, E-035 |
| Scraper job runner | systemd `lead-gen-scraper-runner.service` | active (separate service — restart isolation) | E-001 |
| Contacts DB API (leadsdatabase.cc) | `/opt/contacts_api`, systemd `contacts-api.service`, port 8080 behind nginx | active; `GET /stats.json` OK 2026-11-10 | E-027 |
| LeadOcean/Blitz/etc. provider APIs | external | not probed (no-cost policy); status inferred from code + call logs | E-028 |
| Outscraper services (api/export/sync) | systemd units on this VPS | active but belong to **another codebase** (`/opt/outscraper_export` et al.) — not called by this repo | E-031 |
| Website-scrape sync service | `lead-gen-website-scrape-sync.service` | unit `failed` but sync is env-disabled (`WEBSITE_SCRAPE_SYNC_ENABLED=false`) — dormant by design | E-031 |
| Hyperke-adjacent dirs (not audited) | `/opt/hyperke-dashboard`, `/opt/hyperke-inbox-watch`, `/opt/mailtester-proxy`, `/opt/search-broker`, `/home/ubuntu/hyperke-ai-toolkit` | **not verified** — out of audit scope; smallest access request if needed: read on `/opt/mailtester-proxy` (upstream verifier truth) | — |

### 2.2 Safe checks performed (all read-only)

- `git log/branch/status`, `git show` of specific commits (strict-titles default history).
- `systemctl is-active` / `list-units` (read-only), `curl /api/health`, `curl leadsdatabase.cc/stats.json` (public).
- `sqlite3 -readonly` on `backend/data/jobs.db`: job counts by type/status, last 15 enrichment jobs, TAM job history, 7-day `provider_call_log` volumes, `blitz_fair_use` meter, `provider_email_quality_daily` sample, table inventory.
- `grep`/`sed` code reading only; `backend/.env` read for **key names and non-secret policy values only** (kill-switch booleans, rate limits, `MAILTESTER_ACCEPT_CODES=ok`). No secret values are reproduced in this report. One pre-existing security finding is flagged redacted (E-022).
- No pytest run (suite exists: 595 test functions / 43 files under `backend/enrichment/tests` + `backend/tests`; running it is non-mutating but was unnecessary for the audit and is covered in §12).

### 2.3 Evidence register

Statuses: **D** = confirmed deployed/observed in production data; **C** = confirmed in code at cited lines; **DOC** = documented only; **NV** = not verified.

| ID | Claim | Status | Evidence pointer |
|---|---|---|---|
| E-001 | Deployment truth: branch `feat/moltsets-cascade` @ `384e33b` == `master`, clean tree; backend + contacts-api + scraper-runner active; health OK | D | `git log -1`; `systemctl is-active`×3; `curl localhost:8765/api/health` (2026-11-10) |
| E-002 | Frontend is a single-file vanilla HTML/JS app, hash-routed; pages: home, scraper, domains, search(TAM), people, linkedin, phones, jobs, explorer(flag-gated), apikeys, help | C | `frontend/index.html:1168-1182` (nav), `3146-3194` (router) |
| E-003 | `/#/people` inputs: title keywords, country, universe, seniority, function, industry, name-contains, has-email; limit 1–200 | C | `frontend/index.html:2078-2146`, payload build `5192-5200` |
| E-004 | `/#/people` → `POST /api/enrichment/search/employees` → internal contacts DB only, free; response has `total`, `people[]` with one best email + SEG fields | C | `frontend/index.html:5205`; `backend/enrichment/routes.py:7653-7692`; `contacts_client.py:312-353` |
| E-005 | People-page banner claims "6.6M+ leads"; actual `persons.total` = 30,848,494 (2026-11-10) | D | `frontend/index.html:2090`; `leadsdatabase.cc/stats.json` |
| E-006 | `search_people` supports **no company identifier** filter (no domain / company name / company LinkedIn) | C | `contacts_client.py:312-353` (params list) |
| E-007 | Flow-1 UI: domain CSV required; optional name/first/last/LinkedIn/phone/company-name/existing-email columns (auto-detected); titles ≤50; strict-titles checkbox **checked by default**; provider checkboxes; max DMs 3/5/10 | C | `frontend/index.html:1616-1826`, auto-detect `3576-3601`, payload `3728-3752` |
| E-008 | Strict-titles default ON is **deliberate**: `3808501` set OFF, then `b404c0e` ("strict-title-match checkbox now defaults ON — root cause of title-filter leaks") set it back ON; HEAD still checked | D | `git log -S 'id="domain_strict_titles" checked'`; `frontend/index.html:1751` |
| E-009 | Flow-1 API: `domain_col` **required** (400 if column missing); `company_linkedin_col`, `linkedin_url_col`, `company_name_col`, `strict_titles: bool = True`, `titles`, `max_results`, `providers`, `exact_titles`, `include_phone`, `recent_hires_only`, `website_only`, `source` | C | `routes.py:7104-7197` (model), `7221-7229` (400 check), `7252-7253` (strict marker) |
| E-010 | Row-routing precedence: explicit `company_linkedin_col` → auto-detected `/company/` URL in `linkedin_url_col` → domain cascade; **company URL wins over domain**; no-domain-no-URL row → `STATUS_SKIPPED`; `company_name` alone never triggers discovery | C | `list_builder.py:2607-2682` (`2635` company branch precedes `2659` skip) |
| E-011 | Company-URL orchestrator: Blitz title-waterfall + local strict-title gate (since 2026-10-06); GetLeads DM rescue when waterfall empty + Blitz breaker open surfaces `breaker_open` on the row | C | `list_builder.py:3824-3990` (gate `3924-3944`, rescue `3946-3969`) |
| E-012 | Unified `/api/enrichment/enrich` auto-reroutes `/company/`↔`/in/` URLs between fields; 4 modes: `domain_only`, `linkedin_only`, `company_linkedin_only`, `enhanced` | C | `routes.py:2301-2338` |
| E-013 | Live person-email cascade (both executors, mirrored): contacts_db(name+domain) → contacts_db(linkedin) → smartprospect → quickenrich → **leadocean** → getleads → blitz(person_enrich) → blitz(email-from-linkedin) → **moltsets** → better_enrich → wizleads → contacts_db(input name) | C | `pipeline.py:2630-3195` (step comments at 2694/2758/2818/2873/2945/3010/3057/3124/3156); mirror `list_builder.py:627-1088` |
| E-014 | Production kill-switches: `ENABLE_BETTER_ENRICH=false` (better_enrich **off** in prod despite `providers.py:20` True); all others true (`ENABLE_QUICKENRICH/LEADOCEAN/MOLTSETS/GETLEADS/SMARTPROSPECT`) | D | `backend/.env` (non-secret booleans, 2026-11-10); `providers.py:15-60` |
| E-015 | `/flows/help` self-doc cascade table is stale: omits leadocean + moltsets, labels better_enrich "DISABLED since 2026-09-29", orders getleads before blitz without LO/M | C | `routes.py:939-948` vs E-013 |
| E-016 | Domain-only DM-discovery arm: no-LI rescue QE-dataset → GL-DM → LO-domain-people (terminal); with-LI: contacts_db Step 2 → QE injection → Blitz prepass+waterfall (title-gated) → GL Step 2.4 → LO Step 2.4b | C | `pipeline.py:4114-4239` (no-LI), `4241-4533` (Step 2→2.4b) |
| E-017 | TAM flow: Blitz `tam-by-people` primary (50/page cursor, `max_companies` ≤10,000, 1 FUP record/company, empty page free); GetLeads top-up (500-credit cap, 40 pages ×100 contacts, 1 credit/**contact**, skipped when a company-type filter is set); lookalike mode (≤5 seeds, ranked, ≤2,000 candidates, ≤300/query fan-out); dedupe key domain→linkedin→name; stop rules (400 pages, 5 empty pages, budget) | C | `tam_flow.py:471-773` + constants `89-131`; `blitz_search.py:278-320`; `tam_legs.py:129-197`, `:23`; `tam_routes.py:421-436` |
| E-018 | TAM CSV: 21 columns incl. `domain` (nullable), `website`, `linkedin_url`, `name`, `matched_people`, `source`, `match_score`, `why_matched`; GetLeads rows sparse and `matched_people` hard-coded 1 | C | `tam_flow.py:64-86`, `138-175`; `tam_legs.py:170-196` |
| E-019 | LeadOcean free industry counts exist (`GET /api/enrichment/leadocean/count?industry=` repeatable OR; `/leadocean/industries` 532-value enum) — counts/facets only, no company lists; not surfaced in any UI | C | `routes.py:8540-8601`; `leadocean_client.py:581-613`; `leadocean_industries.py` |
| E-020 | QuickEnrich dataset-search caps: `QUICKENRICH_DATASET_MAX_PAGES=2` (prod env), 20/page ⇒ ≤40 persons/domain | D | `quickenrich_client.py:102, 589-705`; `backend/.env` |
| E-021 | LeadOcean domain→people caps: page 50, max rows 100/domain (`LEADOCEAN_DISCOVERY_MAX_ROWS`); kill switch default on | C | `leadocean_client.py:99-108` |
| E-022 | MailTester: `GET https://validation.hyperke.org/ninja`; codes `ok`/`mb`/`ko`; prod accept policy `MAILTESTER_ACCEPT_CODES=ok` ⇒ **catch-all `mb` rejected by default**; 10 RPS client limit; circuit breaker fails open; 120 s memo cache; **hardcoded fallback API key in source at `mailtester_client.py:91` (value redacted — rotate)** | D | `mailtester_client.py:28-109,151-153,226-228`; `backend/.env` |
| E-023 | Status mapping: `ok`→`valid_ok`, `mb`→`invalid`, `ko`→`invalid`, else `unknown`; raw code preserved on row | C | `routes.py:1580-1591` |
| E-024 | `allow_catch_all` exists **only** on `/api/enrichment/quickenrich` (POST+GET): keeps `mb` as `valid_catch_all`; validation summary block `{validated, accepted, dropped_policy_rejected, dropped_invalid, unavailable}` | C | `routes.py:4278-4402` (param `4306`, gate `4375-4386`) |
| E-025 | **No standalone verification surface**: no `/verify*` route, no UI page/tab, no `verify` job_type (`job_type ∈ {scraper, enrichment, phone_enrichment}`) | C | route inventory `routes.py`; `shared/db.py:108`; frontend page list E-002 |
| E-026 | Contacts-DB persistence: upsert body's `email_verified`/`mailtester_code` are **silently ignored server-side**; only `mark_email_invalid` (`verification_status:"invalid"` → `core.email.is_verified=false`) persists — positives never persisted | C | `contacts_writer.py:408-418,528-536`; `contacts_client.py:624-727` |
| E-027 | Contacts DB scale (2026-11-10): 30,848,494 persons; 27,229,827 emails (24.5M work / 305K personal); **only 24,017 flagged verified**; 2.72M phones; 9,194,379 companies | D | `leadsdatabase.cc/stats.json` |
| E-028 | 7-day provider call volume (jobs.db `provider_call_log`): contacts_db 3.50M; quickenrich 1.37M; blitz 1.25M; smartprospect 752K; getleads 459K; seg 369K; leadocean 252K; **mailtester 181K**; moltsets 74K; wizleads 24K | D | read-only SQL, 2026-11-10 |
| E-029 | Blitz FUP meter: `/v2/enrichment/email` lane at 4,931,843 records used (snapshot 2026-11-09; plan 15M/mo) | D | `blitz_fair_use` table row |
| E-030 | Production job scale: 14,415-row domain job `done` 2026-11-09 (3,156 emails); 1,402 done / 102 partial / 54 failed enrichment jobs total; 84 `tam_flow` jobs | D | read-only SQL on `jobs` |
| E-031 | External-tool truth: **Clay** = client of our API only (guides; no Clay client code); **Outscraper** = contacts-DB source tag + VPS services in another codebase; **Apollo** = data provenance vocabulary only; **DiscoLike** = absent; **Prospeo** = disabled end-to-end (4 layers); no signals/discovery playbook file exists in repo | C | grep inventory + `providers.py:25`; docs agent sweep 2026-11-10 |
| E-032 | TAM UI exposes MX-provider filters, recent-hires, min-matches, company-type checkboxes, sources, chain-enrich; does **not** expose `exact_titles`/`providers` (API-only) | C | `frontend/index.html:1904-2074`; `tam_routes.py:221-255` |
| E-033 | Flow 1 has **no person-location filter** (UI or API); Blitz cascade entries default `location:["WORLD"]`; TAM people-filter supports `location_country_code` (Blitz leg only) | C | `routes.py:7104-7197` (no field); `blitz_client.py:378-406`; `tam_routes.py:189` |
| E-034 | Strict-title gate: synonym-aware local gate over every discovery path; default-cascade (no user titles) exempt; contacts-DB candidate pool widened to `max(max_results, TITLE_SEARCH_POOL=12)` | C | `title_filter.py:31-37,280-334`; `pipeline.py:4100-4111` |
| E-035 | Concurrency/infra: Flow-1 `DOMAIN_CONCURRENCY=25`, `LINKEDIN_CONCURRENCY=15`; gunicorn 4 workers, `--timeout 600`, `max-requests 50000` | C | `list_builder.py:163-165`; systemd unit |
| E-036 | Persistent per-run email cache on `/mnt/disk/enrichment-email-cache/{run}.db` keyed (domain, name), stores `mailtester_code`, no TTL — same-cache reruns skip provider calls | C | `email_cache.py:1-55` |
| E-037 | Verification defaults: `validate_email=True` on unified `/enrich`, legacy `/jobs`, pipeline, list_builder flows; **false** on quickenrich direct (`validate_emails` opt-in); Flow-1 call site relies on default True | C | `routes.py:1630-1633, 2147, 4290-4306`; `pipeline.py:2422,3868,4883`; `list_builder.py:635,2349,3380` |
| E-038 | Provider self-verification trust: GetLeads only `VALID`→yes; SmartProspect `Valid`→yes; QE dated→yes; Blitz `verified_email`→yes else MailTester-gated; LO `verified`→yes, `catch_all`→unknown-gated; MoltSets A/B→yes, C/F→gated, D dropped; **WizLeads always yes** | C | verification sweep (getleads_client.py:231-232,329-330; pipeline.py:1302-1334,1580-1596,1617-1637,3185-3188; leadocean_client.py:28-38; moltsets_client.py:35-39) |
| E-039 | Job counters: `total`, `processed`, `emails_found`, per-provider `emails_*`, `title_gate_dropped`; **no per-job verification-outcome aggregation**; UI shows Total/Processed/Emails | C | `jobs` schema; `frontend/index.html:1831-1835, 6983` |
| E-040 | GetLeads company search: no ownership-type field (`entity_types` 400s); 1 credit per returned contact; TAM leg capped 40 pages | C | `getleads_client.py:1185-1269` (note 1205-1208); `tam_legs.py:23` |
| E-041 | Docs staleness: Help page "Last Updated 2026-07-05", lists 6 sources, "BetterEnrich/Prospeo disabled"; API-reference header 2026-10-13 vs changelog rows 2026-11-06/07; A.1 email_source table missing `leadocean_email` | C | `frontend/index.html:2496-2706`; `docs/ListBuilding_Platform_Full_API_Reference_2026-07-16.md` |
| E-042 | MCP oracle `providers://status` is **live** code-backed (calls `is_provider_enabled` at request time, includes leadocean) though its footer prose says "order set 2026-10-10" | C | `mcp_oracle/resources.py` |
| E-043 | Jobs-page Source filter lacks `tam_flow`/`tam_chain` options (TAM runs visible under All only) | C | `frontend/index.html:2478-2485` |
| E-044 | Test suite: 595 test functions across 43 files (backend); frontend `tests/` pins served-HTML structure; conftest sets `ENABLE_MCP_ORACLE=false` | C | `find/grep` count 2026-11-10 |
| E-045 | Known code-vs-doc nits: mailtester docstring says valid if ok **or mb** (stale vs policy env); Blitz `domain_catch_all` parsed but never consumed (dead output); `mb` is dropped without contacts-DB marking (only `ko` poisons) | C | `mailtester_client.py:131`; `blitz_client.py:320-346`; `routes.py:4264-4267` |
| E-046 | Miss/negative caches live: `blitz_domain_miss` 30d, `leadocean_miss` 7d, `moltsets_miss` 14d + cross-worker cooldown tables — replays skip spent providers | D | jobs.db table list + guard modules |
| E-047 | `search/companies` legacy Blitz company search still mounted (1 credit/5 results, ≤1000 rows) alongside TAM — duplicate discovery surface | C | `routes.py:7619-7650`; `blitz_client.py:416-482` |
| E-048 | No FindX interface exists; the only mention of "FindX" is the audit brief itself | C | repo-wide grep 2026-11-10 |

### 2.4 Inaccessible / not verified

- Provider account dashboards (Blitz FUP totals, GetLeads credits, QE plan gauges) — would need credentials; the platform's own meters were used instead (E-029).
- `/opt/mailtester-proxy` internals (upstream verifier contract beyond `ok/mb/ko`) — read access would firm up catch-all provability claims in §7.
- GetLeads hosted MCP fair-use gauge — intentionally not called (different product surface; zero-cost but unnecessary).
- No end-to-end billable probe was run; all "observed working" claims rest on production DB state, not new requests.

---

## 3. Current architecture

### 3.1 Component inventory

```
Operator browser
  └── frontend/index.html  (single-file app, nginx no-cache, hash routes)
        #/search  → POST /api/enrichment/flows/tam          (Stage 1)
        #/domains → POST /api/enrichment/upload + /flows/domain-enrich   (Stage 2 workhorse, "Flow 1")
        #/linkedin→ POST /flows/by-linkedin-v2              (Stage 2, LinkedIn-URL inputs, "Flow 3")
        #/people  → POST /api/enrichment/search/employees   (internal-DB person search ONLY)
        #/jobs    → GET /api/enrichment/jobs, SSE /jobs/{id}/stream, /download, /shards
FastAPI backend (gunicorn ×4, port 8765, jobs.db = cross-worker truth)
  ├── enrichment/routes.py (~8.8K lines) — endpoints, job runners, SSE
  ├── enrichment/pipeline.py + list_builder.py — the cascade executors
  ├── enrichment/tam_flow.py + tam_routes.py + tam_legs.py + tam_ranker.py + lookalike.py — Stage 1
  ├── provider clients: contacts_client (internal DB, 75 RPS) · smartprospect · quickenrich
  │   · leadocean · getleads · blitz(+blitz_search) · moltsets · better_enrich(env-off) · wizleads · prospeo(off)
  ├── mailtester_client.py — the only SMTP-grade verifier (inline)
  ├── contacts_writer.py — the single write-back path → leadsdatabase.cc (outbox, idempotent)
  └── call_tracker.py — provider_call_log + email ledger + quality rollups
SQLite jobs.db (~4.4 GB, WAL): jobs, job_events, checkpoints, miss/cooldown tables, ledgers
External contacts DB (leadsdatabase.cc = contacts-api on this VPS, PG): 30.8M persons / 9.2M companies
```

### 3.2 End-to-end flows

**Stage 1 — account TAM (as built):**

```
ICP filters (company: name/industry/type/size/revenue/keywords/founded/followers/HQ/MX;
             people: titles include/exclude, job levels, min matches, person country, recent-hire)
   or 3–5 seed companies (lookalike, ranked)
        │
        ▼
POST /flows/tam ──► job (source_type=tam_flow) ──► Blitz tam-by-people (cursor, 50/page, ≤10k cos)
        │                                              + GetLeads top-up leg (≤500 credits, weaker filters)
        │                                              + lookalike fan-out (≤2000 candidates, ≤300/query)
        ▼
21-col CSV (name, domain, website, linkedin_url, industry, type, size, …, matched_people,
            source, match_score, why_matched)
        ├──► every domain-bearing row upserted to contacts DB (auto, free)
        └──► optional chain → Flow-1 job (DOMAIN-BEARING ROWS ONLY) → contacts+emails CSV
```

**Stage 2 — company → people + email waterfall (as built):**

```
CSV upload (#/domains or #/linkedin) ──► column mapping (domain required on Flow 1)
        │  per row: company-LinkedIn (explicit col or /company/ autodetect)  >  domain  >  [name-only ⇒ SKIPPED]
        ▼
Discovery per company (domain arm):
  contacts_db(by-company) → QE dataset-search (≤2 pages×20) → Blitz find-people prepass +
  title waterfall (cursor, user titles, strict gate) → GetLeads DM fallback → LeadOcean
  domain→people (terminal, ≤100 rows)      [no-LI rescue: QE → GL → LO]
Per person email waterfall (11 steps, stop on first hit):
  contacts_db(name+domain) → contacts_db(linkedin) → smartprospect → quickenrich →
  leadocean → getleads → blitz person_enrich → blitz email-from-linkedin → moltsets →
  better_enrich(prod-off) → wizleads → contacts_db(input-name)
  + inline MailTester gate on contacts_db & unverified provider emails (fail-open)
        ▼
60+-col CSV (dm_* identity/title/location/linkedin/email + dm_email_source +
  dm_email_verified + mailtester_code/message + source_path + provider_attempts +
  provider_errors + no_email_reason + company_email fallback + SEG/MX + gate counters)
  + contacts_writer sync → leadsdatabase.cc (person/company split, outbox, idempotent)
```

**Stage 3 — verification (as built):** *inline only* — MailTester runs inside Stage 2 per candidate email; `mb` (catch-all) dropped by default policy; results stamped on CSV rows; `ko` poisons the contacts-DB row; positives not persisted; no operator-selectable batch, no re-check, no standalone export.

---

## 4. Live capability and provider matrix

Production cascade (code E-013 + env E-014): `contacts_db → smartprospect → quickenrich → leadocean → getleads → blitz → moltsets → wizleads` (better_enrich code-present but env-off; prospeo off).

| Source | Company discovery | Person search (per company) | Email finding | Verification | Caps (live) | Cost basis | Integration status |
|---|---|---|---|---|---|---|---|
| **Internal contacts DB** (leadsdatabase.cc) | ✗ (lookup/write only; no discovery endpoint) | ✓ by domain (`company_contacts_enriched`, pool 12 w/ titles) + 30.8M-person free search (`/search/employees`) | ✓ stored emails (one best/person) | re-validates via MailTester on read (default) | 75 RPS | free | deployed, heaviest user (3.5M calls/7d) |
| **SmartProspect** | ✗ | ✗ | ✓ person (name+domain, batch ≤10) | self: `Valid`→yes | shared 1900 RPM (env 1400) | plan credits | deployed (752K calls/7d) |
| **QuickEnrich** | ✗ (dataset-search is per-domain) | ✓ dataset-search (domain+titles, has_email) | ✓ person (linkedin or name+domain) + phones | dated email → yes | dataset ≤2 pages ×20/domain (env); 5800/5800 RPM lanes | credit on returned employee-with-email; empty page free | deployed (1.37M calls/7d) |
| **LeadOcean** | counts only (532 industries, free) — no lists | ✓ domain→people (terminal tier) | ✓ work+personal by LinkedIn URL | `verified`→yes; `catch_all`→unknown; rejects invalid/role/abuse/spam_trap | discovery ≤100 rows/domain; 150 RPS; 1M/day wall → cooldown skip | enrich: 1 rec/miss included; discovery: 1 search rec/row (25M/mo) | deployed (252K calls/7d) |
| **GetLeads** | ✓ TAM top-up leg (weak filters, no ownership type) + lookalike seed fallback | ✓ decision-makers domain fallback | ✓ person (name+domain, batch 100) + from-LinkedIn | `VALID`→yes; `CATCH_ALL`→unknown (kept) | TAM leg ≤500 credits/run, 40 pages ×100; ~10k/min batch | 1 credit/contact returned (TAM), plan credits (enrich) | deployed (459K calls/7d) |
| **Blitz** | ✓ **primary** TAM (richest filters incl. MX, recent-hire, matched-people counts) | ✓ title waterfall + find-people prepass (50 cos/call) | ✓ person + email-from-LinkedIn | `verified_email`→yes; else MailTester-gated | 25 RPS default lane (env); FUP 15M/mo — meter at 4.93M (E-029) | search: 1 rec/result (empty free); email/phone: 1 rec/call (miss bills); person/d2l: success-only | deployed (1.25M calls/7d) |
| **MoltSets** | ✗ | ✗ | ✓ person best-email incl. personal | grades A/B→yes; C/F→gated; D dropped | 30 RPM; 15k/5h + 75k/wk windows → cooldown skip | plan records | deployed (74K calls/7d) |
| **BetterEnrich** | ✗ | ✗ | ✓ person + company-email fallback | `email_status` verified/valid→yes | 10/5 RPS | credits (~0 at times) | **env-OFF in prod** (E-014) |
| **WizLeads** | ✗ | ✗ | ✓ person (terminal) | **always stamped yes** (vendor catch-all verify) | 10 RPS | 3 credits/email (doc) | deployed (24K calls/7d) |
| **MailTester** (self-hosted proxy) | — | — | — | ✓ the only SMTP prober; ok/mb/ko | 10 RPS; 30s timeout; breaker fail-open | self-hosted (no per-call cost) | deployed (181K calls/7d) |
| **Prospeo** | ✗ | ✗ | ✗ | ✗ | — | — | disabled end-to-end (4 layers) |
| **Outscraper / Apollo / DiscoLike / Clay** | ✗ (source tag / provenance only / absent / client-only) | ✗ | ✗ | ✗ | — | — | see E-031 |

Key matrix conclusions: **only Blitz (+ weakly GetLeads) can discover companies**; **all of QE/GL/LO/Blitz + internal DB can find people per company** (domain- or LinkedIn-keyed); **only MailTester verifies SMTP-level**, with provider self-attestations layered above it.

---

## 5. Stage 1 audit — account TAM generation & qualification

### 5.1 What an operator can do today

- Describe an ICP via company filters (name, industry, type, size band or numeric headcount, revenue, keywords, founded year, min LinkedIn followers, HQ country/continent/sales-region, MX-provider include/exclude) **and** persona filters (titles include/exclude, job levels, min matches per company, person country, recent-hires-only), then run — Blitz `tam-by-people` returns companies **that employ matching people**, each with a true `matched_people` count (E-017).
- Or give 3–5 example companies → "Analyze examples" (free job-less profile synthesis) → run lookalikes; results ranked by trigram/industry/size/keyword similarity with `match_score` + `why_matched` (E-017).
- Watch live progress (per-page SSE events, incremental CSV flush for unranked runs), download the 21-column CSV, and optionally chain straight into contact enrichment (E-017, E-018).
- Every domain-bearing result is auto-upserted to the contacts DB — TAM building also grows the private DB (E-017).

### 5.2 Counts, dedupe, and identity resolution

- **Count semantics:** `result_count` = companies returned (≤ `max_companies`, hard budget ceiling 1–10,000, default 500 in UI). `matched_people` per row is real for Blitz, hard-coded 1 for GetLeads rows (E-018) — sorting the merged CSV by `matched_people` mixes semantics across sources.
- **Dedupe:** cross-source key = `domain → linkedin_url → name:<name>`; seeds excluded; GL leg seeds its dedupe set from Blitz rows (E-017). Same-name/different-company collisions collapse only when both rows lack domain+linkedin.
- **Capped/unknown coverage is knowable for Blitz:** the run summary carries `capped` (cursor outstanding) and page/stop-rule counters; the UI does **not** currently surface "capped" prominently to the operator (runs table shows count + status only, E-017 frontend lines 5131-5167).
- **Free pre-counts:** LeadOcean industry counts exist (E-019) but use a 532-value taxonomy that differs from Blitz's ~700-value enum and the UI's 43-value dropdown — a count there does **not** predict a Blitz TAM yield. Honest TAM sizing today = run Blitz TAM with `max_companies` as the budget and read the result (empty = free).

### 5.3 Exceptions (Atishay's qualification) — current status

| Route | Status | Evidence |
|---|---|---|
| Lookalikes | **Built** (Blitz+GL legs, ranked mode) — functionally the DiscoLike-shaped route | E-017 |
| Directory scraping / Google-search sourcing | **Not integrated** — Google Maps scraping exists in the *scraper* product (separate page), Outscraper is a data-source tag only; no directory-scrape → TAM pipeline | E-031 |
| Public/industry datasets | Manual/CSV-only (an operator can upload any company CSV to Flow 1) | E-009 |
| Apollo / Prospeo / Clay / DiscoLike | Not integrated / disabled / client-only / absent | E-031 |
| Signals playbook | **Does not exist in the repo** (only the brief mentions it) | E-031 |

### 5.4 Stage 1 → Stage 2 handoff contract (measured against the brief)

- TAM CSV exports `domain`, `linkedin_url`, `name` — all three Stage-2 identifiers present (E-018). Domains are normalized by Blitz (`flatten_tam_company`), LinkedIn URLs canonical.
- **Gap G1 (high):** the chain-enrich consumes **only domain-bearing rows**; TAM rows with LinkedIn URL but no domain are silently excluded from the automated handoff (tam_chain.py:61-67). Operators must manually split those into Flow 3.
- **Gap G2 (medium):** no qualification/QA step between TAM and people-finding — the chain fires immediately when ticked; there is no approval gate or per-company selection UI.

---

## 6. Stage 2 audit — company-scoped people search & email enrichment

### 6.1 The `/#/people` page vs the brief's assumption

The brief assumes `/#/people` takes accounts and finds contacts across resources using titles + locations + a company identifier. **Reality (E-003/E-004/E-006): it is an internal-DB-only search with no company scoping, no enrichment trigger, no job, ≤200 rows, and a CSV limited to the rows shown.** It is useful as a free person-mining tool (30.8M persons; SEG badges; universe filters) but it is not the Stage-2 engine. The engine is Flow 1 (`/#/domains`) / Flow 3 (`/#/linkedin`) / the unified `/enrich` API.

### 6.2 Input contract (Flow 1, the production path)

| Field | UI | API | Notes / gaps |
|---|---|---|---|
| `domain` | required column | required column (400 if absent) | **LinkedIn-only or name-only CSVs cannot be submitted** (E-009) |
| `company_linkedin_col` | auto-detected via person-LinkedIn column only | explicit field | company-URL rows short-circuit to the company orchestrator; **takes precedence over domain** (E-010) — matches brief priority for these two |
| `linkedin_url_col` (person) | auto-detected | field | `/company/` values auto-routed to the company path with a visible warning (E-007 `3603-3626`) |
| `company_name_col` | auto-detected | field | **carried to output + used for company enrichment, never a discovery key** — name-only rows → `SKIPPED` (E-010) |
| `titles` (≤50) | free text | string, fuzzy, synonym-aware | strict local gate **ON by default** (E-007/E-008/E-009; API default `strict_titles=True`; escape hatch `strict_titles=false` marks the cascade) |
| person location | **absent** | **absent** | Blitz cascade supports per-tier `location` but nothing exposes it (E-033) |
| max DMs | 3/5/10 | int | per-company cap; contacts-DB pool widened to ≥12 when titles set (E-034) |
| providers | 9 checkboxes (all default on) | allowlist | contacts_db locked on; global kill-switch beats allowlist (E-014) |
| pre-processing | normalize domains, dedupe by domain (both default on) | flags | dedupe happens pre-job so `total` = unique rows (E-009) |

**Conflict semantics:** if a row carries a company LinkedIn URL *and* a domain that disagree, the company URL wins and the domain is still used for email resolution — there is **no conflict detection/flag** (E-010). The unified `/enrich` is more forgiving (auto-reroutes `/company/`↔`/in/` between fields, E-012) but also does not flag conflicts.

### 6.3 Discovery & waterfall (verified order, both executors mirrored — E-013/E-016)

Domain arm discovery: contacts_db by-company → QE dataset-search (≤2 pages×20) → Blitz prepass + title waterfall (user titles, strict gate, recent-hire injectable) → GetLeads DM fallback → LeadOcean domain→people terminal (≤100 rows). No-LinkedIn rescue: QE → GL → LO, then a `no_linkedin` row carrying `no_email_reason` (incl. `contacts_db_unavailable` when the breaker was open — partial-failure visibility, E-016).

Person email waterfall (stop at first hit): contacts_db(name+domain) → contacts_db(linkedin) → smartprospect → quickenrich → leadocean → getleads → blitz(person) → blitz(linkedin→email) → moltsets → better_enrich(**prod-off**) → wizleads → contacts_db(input-name). Misses negative-cached (Blitz 30d / LO 7d / MoltSets 14d) so reruns don't re-bill (E-046).

### 6.4 Bulk execution, reliability, scale (observed)

- Background job model with 30s heartbeats, atomic counters via `append_event`, SSE progress, cancel → `partial` CSV, crash-safe resume from per-domain checkpoints, shards + partial downloads (E-030, E-035, endpoint list).
- Concurrency 25 domains / 15 person-emails; a 14,415-domain job completed 2026-11-09; 1.7K-row jobs complete in minutes (E-030).
- Per-row failure isolation: `provider_errors` JSON cell + `no_email_reason`; a provider outage degrades to other tiers (multiple 2026-10/11 RCAs fixed silent-zero paths — e.g. contacts_db breaker now surfaces `contacts_db_unavailable`, Blitz breaker-open stamped, E-011/E-016).
- Interactive `/enrich` has a 20s cascade budget (`ENRICH_REQUEST_BUDGET_S=20`) returning partial results — background jobs are exempt (E-014 env).

### 6.5 Output & counts

- 60+-column CSV (full contract E-009/E-013 column list at `list_builder.py:240-310`): identity, title, location, LinkedIn, `dm_email` + `dm_email_source` + `dm_email_verified` + `mailtester_code/message`, `source_path`, `provider_attempts(_json)`, `providers_called/skipped`, `provider_errors`, `no_email_reason`, company-email fallback block, SEG + `dm_mx_provider`, `dm_previous_companies/titles`.
- Job counters: total/processed/emails_found + per-provider email counts + `title_gate_dropped` (strict-gate drops) (E-039). **Missing:** matched-people vs in-ICP split beyond the gate counter; per-job verification-outcome rollup (ok/mb/ko counts) — those exist only per-row in the CSV.
- Every enriched contact syncs to leadsdatabase.cc through `contacts_writer` (outbox, retry, idempotent by normalized keys) — reruns do not create duplicate persons (E-026 context; `linkedin_norm_uq` race fixed 2026-09-12).

### 6.6 Stage-2 gaps ranked

| # | Gap | Severity | Evidence |
|---|---|---|---|
| G3 | No single surface accepts mixed identifier lists; LinkedIn-only/name-only rows unusable in Flow 1 | **High** | E-009, E-010 |
| G4 | `/#/people` cannot scope by company nor trigger enrichment — the brief's named Stage-2 page is not Stage-2 capable | **High** | E-003, E-004, E-006 |
| G5 | No person-location filter anywhere in Stage 2 | Medium | E-033 |
| G6 | No conflict flagging when identifiers disagree (company URL silently wins) | Medium | E-010, E-012 |
| G7 | Coverage caps not surfaced in UI (QE 40/person cap, LO 100/domain, waterfall max_dms, contacts pool 12) — CSV rows just stop | Medium | E-020, E-021, E-034 |
| G8 | Strict titles ON by default narrows recall; operators wanting volume must know to uncheck | Medium (deliberate) | E-007, E-008 |
| G9 | Provider-order docs stale in 3 operator-visible places (`/flows/help`, Help page, People banner) | Low (trust) | E-015, E-041, E-005 |

---

## 7. Stage 3 audit — normal & catch-all email verification

### 7.1 What "normal" and "catch-all" mean in this system (evidence-based)

They are **result categories of one verifier, not two modes**: MailTester's proxy returns `code ∈ {ok, mb, ko}`. `ok` = deliverable ("normal"); **`mb` = the catch-all/accept-all category** (routes.py:4294 literally documents "mb (catch-all)"); `ko` = dead. Under production policy `MAILTESTER_ACCEPT_CODES=ok` (E-022), `mb` is **rejected (dropped) by default** — mapped to `invalid` in API responses (E-023) — while the QuickEnrich direct endpoint alone exposes `allow_catch_all=True` to keep `mb` rows labeled `valid_catch_all` (E-024). There is no separate second-pass verifier and no disposable/role classifier (role addresses exist only as LeadOcean vendor-side rejects, E-022/E-038).

### 7.2 Status taxonomy (live)

| Surface | Statuses | Evidence |
|---|---|---|
| MailTester raw | `ok` / `mb` / `ko` / `unavailable` (local, outage) / `""` | E-022 |
| CSV row | `dm_email_verified` yes/no/unknown; `mailtester_code`; `mailtester_message`; `final_email_verification_source` ∈ mailtester/provider_self | E-013 columns |
| Unified API | `validation_status` ∈ valid_ok / invalid / unknown (mb **and** ko both → invalid; raw code kept) | E-023 |
| QuickEnrich endpoint | valid_ok / **valid_catch_all** / invalid / unknown + summary counters | E-024 |
| Provider self-attestation | per-provider (GL VALID-only; SP Valid; QE dated; LO verified; MoltSets A/B; WizLeads always-yes) | E-038 |
| Scoreboard (`/stats/email-quality`) | VALID / CATCH_ALL / INVALID / UNKNOWN rollups (parsed for GL+SP only; others UNKNOWN) | call_tracker; routes.py:8681-8737 |
| Contacts DB | `core.email.is_verified` (only ever set **false** via `mark_email_invalid` on `ko`); Explorer facet `email_verification_result` incl. `catch_all` | E-026 |

### 7.3 What verification does today (and doesn't)

- Runs **automatically inline** on Flow-1/legacy/unified enrichment (`validate_email=True` default) for contacts-DB + unverified provider emails; **fails open** on outage (email kept, `unavailable`); 120s in-memory memo prevents intra-request duplicates; per-run disk cache stores codes (E-022, E-036, E-037).
- `ko` poisons the contacts-DB row (`is_verified=false`) so future cascades skip it; `mb` is dropped-but-not-poisoned so it can resurface under a lenient policy (E-045).
- **Cannot do any of:** operator-selected subset verification, arbitrary list upload/paste, re-check later, batch resume, a status-bearing verification-only export, or catch-all risk labeling at scale. No endpoint, page, or job type exists (E-025).
- **Catch-all provability:** with the current single verifier, a catch-all address is *classifiable* (`mb`) but not individually provable — SMTP-level proof for accept-all domains requires per-address techniques (panel probing / MX+pattern checks) the platform does not have. The UI should therefore present catch-all as **"deliverable-unknown — send with caps/suppression rules"**, never "safe".
- **Cost angle:** 181K MailTester calls/7d are all inline; because positives aren't persisted (E-026), repeat jobs re-verify the same contacts-DB emails — a verification-result cache keyed by email is the single highest-leverage efficiency fix.

### 7.4 Fit for a standalone screen

Everything needed exists as building blocks: `mailtester_client.verify_email` (rate-limited, breaker, memo), `_map_validation_status`, `allow_catch_all`/`valid_catch_all` semantics, the job framework (jobs/job_events/SSE/cancel/download), CSV upload+download plumbing, and the email ledger for audit. Missing: a `verify` job_type + endpoint, email-keyed result persistence, concurrency above the 10 RPS single-process lock (a batch verifier must shard or raise the limit), an export schema (email, status, code, verifier, verified_at), and a UI tab. A tab on the existing People/tools area or a small dedicated page are equally feasible — the job system makes both cheap (§10).

---

## 8. Requirement-to-evidence gap matrix

| Brief requirement | Status today | Proof | Missing work | Severity | Component |
|---|---|---|---|---|---|
| Stage 1: ICP → qualified company list w/ provenance | **Supported** (Blitz TAM + GL + lookalike) | E-017, E-030 | Coverage-cap surfacing; GL matched_people=1 fix | Low/Med | tam_flow |
| Stage 1: discovery-source breadth (directories, DiscoLike, Apollo…) | **Partial** — lookalike built; rest manual/absent | E-031 | Optional manual-import SOP; no code need | Low (exception path) | docs |
| Stage 1→2 handoff with all three identifiers | **Partial** — CSV carries domain+linkedin+name; chain drops non-domain rows | E-018, tam_chain.py:61-67 | Chain LinkedIn-only rows via company orchestrator; name-only via name→domain resolution | **High** | tam_chain, list_builder |
| Stage 2: bulk companies, mixed identifiers, one job | **Absent** (domain col mandatory; name-only skipped) | E-009, E-010 | Identifier-flexible row admission + precedence incl. name fallback + conflict flag | **High** | routes, list_builder |
| Stage 2: `/#/people` as the workhorse | **Absent** (internal-DB search only) | E-003–E-006 | Company-scoped search mode riding existing waterfall | **High** | frontend, routes |
| Identifier priority LinkedIn > domain > name w/ corroboration | **Partial**: LinkedIn>domain enforced; name never used; no conflict alerts | E-010, E-012 | Name→domain resolution (contacts-DB company-by-name / GL company enrich) + conflict detection | Med | list_builder |
| Title families vs strict; aliases; negative titles | **Supported** (fuzzy+synonyms+excludes; strict gate default ON; escape hatch) | E-007/E-008/E-034 | None (default is a policy choice to confirm) | Low | title_filter |
| Optional person location filter | **Absent** in Stage 2 (TAM-only) | E-033 | Expose cascade `location` / QE-GL equivalents where supported | Med | routes, frontend |
| Full-recall person discovery past page caps | **Partial** — cursor waterfall yes; QE ≤40, LO ≤100 caps; UI doesn't flag caps | E-020, E-021 | Cap transparency + optional multi-pass | Med | UI |
| Email waterfall incl. personal fallback + provenance | **Supported** (9 tiers, per-row source, LO/MoltSets personal fallbacks) | E-013, E-016 | — | — | pipeline |
| Inline validity handling incl. catch-all | **Supported** (ok/mb/ko; mb default-dropped; `allow_catch_all` QE-only) | E-022–E-024 | Operator-facing policy toggle | Med | routes |
| Bulk job scale 10k+ rows, progress, cancel, resume, partial export | **Supported** (observed 14.4K) | E-030, E-035 | — | — | job system |
| Stage-2 rerun idempotency (no dupes/double-bill) | **Supported** (input dedupe, miss caches, writer idempotency, per-run email cache) | E-036, E-046, E-026 | Cross-run verification cache | Low | pipeline |
| Provider partial failure visibility | **Supported** (provider_errors, no_email_reason, breaker stamps) | E-011, E-016 | Job-level "degraded" banner | Low | UI |
| Counts reconciliation (companies→people→emails) | **Partial** — totals exist; matched-vs-in-ICP split only via gate counter; no verification rollup | E-039 | Stage-wise count summary on job cards | Med | routes, UI |
| Stage 3: verify a selected batch (normal + catch-all) w/ status export | **Absent** | E-025 | Verify job + endpoint + tab + email-keyed cache | **High** | new |
| Catch-all sendability policy | **Partial** — semantics exist (`valid_catch_all`) but default drops; no operator guidance | E-022–E-024 | Policy default decision + UI labels | Med | product |
| Auth/roles/quota reuse for new surfaces | **Supported** (JWT+API-key dep, 50K/day quota, admin gates) | E-001, routes.py:920-936 | None | — | shared |
| No Clay dependency | **Confirmed** — Clay is only a client | E-031 | — | — | — |

---

## 9. Proposed operator experience (product-level, no implementation)

**Journey (reuses existing pages; new pieces marked ✦):**

1. **Accounts (existing TAM page).** Operator enters ICP filters or seeds → previewed estimate is skipped (no free cross-provider count exists — show "budget = max companies, empty is free") → run → TAM run card shows companies found, `capped` flag if the cursor had more, CSV download. ✦ Card gains "Continue to Find People" which creates the Stage-2 job from **all** rows (domain rows via domain path, LinkedIn rows via company path — no more silent drop).
2. **People + emails (existing Upload Domains page, extended).** Paste/upload the account list (mixed `domain` / `company_linkedin` / `company_name` columns — ✦ admission relaxed). Set titles (families, with strict toggle default per policy), ✦ optional person location, max contacts/company, providers. Row-level identifier resolution shown in a preview strip: "812 by domain · 143 by company URL · 45 name-only (resolving…)" with ✦ conflict flags where identifiers disagree. Start → job card (existing SSE) shows Total / Processed / Companies matched / Contacts found / Emails found / ✦ gate drops / ✦ verification outcomes (ok/mb/ko/unavailable) — reconciling with the CSV on download.
3. **Verify selected (✦ new tab "Verify Emails").** Paste emails or "verify this job's emails" with a row-count selector (e.g. 2,000 of 20,000) + catch-all policy choice (drop / keep-flagged). Job model identical to enrichment jobs (progress, cancel, resume, dedupe against email-keyed cache, re-check-later). Export columns: `email, status(valid_ok|valid_catch_all|invalid|unknown), verifier(mailtester), code, message, verified_at, first_seen` — plus per-domain rollup for suppression decisions.
4. **QA + export.** Existing CSV contract extended with the verification rollup; SEG/MX columns already support deliverability triage; the Explorer remains the deep-dive surface for the internal DB.

**Illustrative Stage-2 output schema (already 90% the live contract — `list_builder.py:240-310`):** `company_linkedin_url, company_name, company_industry, company_employee_count, dm_full_name, dm_title, dm_job_level, dm_linkedin_url, dm_location_city/country, dm_email, dm_email_source, dm_email_verified, mailtester_code, dm_mx_provider, seg_classification, source_path, provider_attempts, provider_errors, no_email_reason, row_status` (+ input echoes). ✦ additions: `company_identifier_used (domain|company_linkedin|name_resolved)`, `identifier_conflict (bool)`.

**Failure states to design for (mostly existing):** provider breaker-open (row reason + job banner), validator outage (fail-open + `unavailable` counter), cancel mid-run (partial CSV), rerun (cache hits shown as "reused"), capped discovery (per-company "more available" marker where a provider cap truncated).

---

## 10. Architecture options compared

| Criterion | **A. Minimal extension** (TAM + Flow-1 flexibility + People-page company mode + Verify tab) | **B. Guided 3-step project shell** (new project entity, stage-wise wizard UI) | **C. Backend-first unification** (new APIs/jobs first, thin UI later) |
|---|---|---|---|
| Dev effort | **S–M** (~3–5 wks) | M–L (~6–10 wks) | M (~4–6 wks to same UI date) |
| Code reuse | **Very high** — every new piece plugs into existing jobs/cascade/writer | Medium — new state model + UI, reuses cascade only | High backend reuse, but operator value delayed |
| Operational complexity | **Unchanged** (same services, same job types + 1) | New project state to operate/debug | Two releases before operators feel it |
| Scale performance | Proven at 14K rows | Same engine, so equal | Same |
| Risk | **Low** — additive endpoints + UI tabs | Medium — new surface, new failure modes, docs/UI sync burden | Low tech risk, high delivery risk |
| Maintenance | One more tab + endpoint family | Project shell forever | — |
| Time-to-value | **~2 wks for Stage 3 alone** | Longest | Medium |
| Testability | Existing fixtures extend naturally | Needs new E2E flows | Same as A |

**Selected: Option A** (with Option C's sequencing inside it — build the verify endpoint and identifier-flexible admission backend-first, then wire the two UI surfaces). Rationale: every load-bearing engine is already deployed at scale; the only **absent** capability (Stage 3) is a thin job around an existing client; Option B's project shell adds state and UI without unlocking any capability the Jobs-page chaining doesn't already give, and this codebase's single-file frontend + jobs-list UX is exactly the pattern operators already know. Option B becomes attractive later **only if** clients need saved projects/client-separation — currently a question, not a requirement (Q-3).

---

## 11. Prioritized implementation backlog

**P0 — unblock the 90% workflow (bug-fix-adjacent + thin features)**

| Item | Scope | Depends on | Risk | Acceptance test | Effort |
|---|---|---|---|---|---|
| P0-1 Standalone verification job (feature) | `POST /api/enrichment/verify` (upload/paste/list, `allow_catch_all` policy, email-keyed result table + cache reuse, `job_type='enrichment'` w/ `source_type='verify'` to avoid new literal… or new type per rule #4 — **decide Q-1**), export CSV with status+provenance, UI tab | mailtester_client (exists) | Low (read-mostly; only `ko` writes poison) | Upload 100 emails fixture → statuses match unit-mocked codes; rerun = 100 cache hits, 0 new calls | M (4–6 pd) |
| P0-2 Identifier-flexible Flow 1 (bug-fix class) | Accept CSVs without `domain` col: company-LinkedIn col alone (company orchestrator), name-only rows via name→domain resolution (contacts-DB company lookup + GL company-enrich fallback); `company_identifier_used` + `identifier_conflict` columns | E-010 path (exists) | Medium (resolution quality) | 3-fixture CSV (LI-only / name-only / mixed-conflict) → all rows processed or explicitly flagged; conflict rows surfaced not silently joined | M (4–6 pd) |
| P0-3 TAM chain non-domain rows (bug fix) | Chain LinkedIn-bearing rows through the company path instead of dropping | P0-2 pattern | Low | TAM fixture with 2 domain + 2 LI-only rows → chained job processes 4 | S (1–2 pd) |
| P0-4 Verify-tab UI + People-page "company mode" input | ✦ Stage-3 tab; ✦ `/#/people` gains optional company-identifier box that switches to the waterfall job path (or a pointer card to Upload Domains — **decide Q-2**) | P0-1/P0-2 | Low | Playwright/pinned-HTML tests (repo pattern) | S–M (3–4 pd) |
| P0-5 Rotate mailtester key + remove hardcoded fallback (security fix) | env-only key; rotate at proxy | — | Low | grep clean; verification still 200 | S (<1 pd) |

**P1 — quality, transparency, efficiency**

- P1-1 Verification-outcome rollups on job cards (`ok/mb/ko/unavailable` counters via append_event) + matched-vs-gate-drop split (M, 2–3 pd).
- P1-2 Coverage-cap surfacing: per-company "capped" markers (QE 40 / LO 100 / waterfall max) + TAM `capped` flag in runs table (S–M, 2 pd).
- P1-3 Person-location filter in Flow 1 (Blitz cascade `location` + GL/QE where supported; honest "not supported by source" hints) (M, 3 pd).
- P1-4 Persist positive verification to contacts DB (`verification_status: valid|catch_all` on upsert — server-side support confirmed for `invalid`, verify others) → stops re-billing MailTester on reruns (M, 3 pd + contacts-api change).
- P1-5 GL TAM leg `matched_people` real counts or explicit `source_semantics` column (S, 1 pd).
- P1-6 Docs sync: `/flows/help` cascade table, Help page sources, People banner count, Jobs-page TAM source filter (S, 1 pd).

**P2 — exceptions & scale niceties**

- P2-1 Free TAM pre-count bridging (map LO industries → Blitz enums, or Blitz dry-run probe) (M).
- P2-2 Conflict corroboration (domain↔LinkedIn cross-check via contacts-DB company records) (M).
- P2-3 Verification batching above 10 RPS (shard lock or raise proxy limit) for 20K+ chunks (S–M).
- P2-4 Project-shell UI (Option B) — only if Q-3 answers "yes" (L).

Rollout phases: **Phase 1** = P0-5 + P0-1..P0-3 behind existing tests + one canary job; **Phase 2** = P0-4 + P1-1/P1-2/P1-6; **Phase 3** = P1-3..P1-5, P2 as demand dictates.

---

## 12. Verification / test plan (for the later implementation — nothing costly run during this audit)

**Fixtures (repo pattern — `backend/enrichment/tests/`, 595 tests):**
- CSV fixtures: domain-only / company-LI-only / name-only / mixed-with-conflict / invalid-URL / rebrand-redirect rows; person fixtures with title synonyms and excluded titles; duplicate persons across two providers; accept-all domain; known-valid; known-dead.
- MailTester mock: 200 `{ok|mb|ko}`, 4xx, 5xx, timeout paths (extend `test_mailtester_timeout.py`).
- Identifier-precedence unit tests against `_detect_linkedin_url_type` + row routing (E-010).

**Contract tests:** verify endpoint (statuses incl. `valid_catch_all`, summary block parity with quickenrich's), new CSV columns, chain-of-4 acceptance (P0-3), rerun idempotency (assert zero new provider calls via call-log fixture).

**Non-billable dry runs (this repo's established habit):** `pytest -k "tam or verify or identifier"`; MCP oracle schema lookups; `GET /flows/help` diff.

**Authorized paid probes (request approval, with estimates):** one 50-company Blitz TAM probe (~50 FUP records ≈ negligible vs 10M headroom on the 15M plan); one 25-domain QE dataset batch (≤25 credits); ~100 MailTester verifications via the new endpoint against a synthetic domain list (self-hosted, no vendor cost). Each is reversible and bounded.

**Performance/QA gates:** 5K-row mixed-identifier job completes < 30 min with correct per-stage counts; verification of 2,000 emails ≤ 4 min at 10 RPS with cache warm-hit rerun < 10 s; SSE/cancel/resume paths exercised; no NEW failures beyond the ~7 known pre-existing.

**Go/no-go:** Stage-3 endpoint + tab live and reconciling counts → go Phase 2; identifier-flexible Flow 1 passing the 3-fixture matrix → go chain change; any regression in `VALID_PROVIDERS` parity test or writer idempotency → stop.

---

## 13. Open questions and recommended defaults (ranked by impact)

| # | Question | Why it matters | Options | Recommended default |
|---|---|---|---|---|
| Q-1 | New `job_type='verify'` literal vs `source_type='verify'` on enrichment jobs? | Rule #4 forbids changing job literals; adding one is allowed but touches reapers/SSE/UI filters | (a) new type (clean) (b) source_type reuse (zero-risk) | **(b) source_type on `enrichment`** — ships fastest, no reaper surface changes |
| Q-2 | Extend `/#/people` with a company-scoped mode, or keep it internal-DB-only and route company work to Upload Domains? | Determines where operators live; brief assumed People page is the engine | (a) company mode on People (b) pointer card + improved Upload Domains (c) both | **(a)** — matches the brief's stated UX; the waterfall runner is reused either way |
| Q-3 | Do clients need per-client project separation / saved TAMs (Option B trigger)? | Decides P2-4; affects data model now vs later | (a) single shared workspace (today) (b) client tags on jobs (c) full projects | **(b) lightweight client tag on jobs** — defer projects |
| Q-4 | Catch-all sendability default: keep dropping `mb`, or keep-and-flag? | Directly changes deliverable counts clients see (mb volume is material) | (a) drop (today) (b) keep+flag in exports (c) per-run toggle | **(c) per-run toggle, default keep+flag** on the new verify surface; enrichment cascade unchanged |
| Q-5 | Is strict-titles-ON the right default for the new company mode? | Recall vs precision tradeoff; b404c0e deliberately chose precision | (a) ON (today) (b) OFF for recall-max "ALL people" | **(a) ON default with visible toggle** — preserve the leak fix, let operators opt wide |
| Q-6 | Person-location filter: required for launch? | Blitz supports it; QE/GL mostly don't — partial coverage may confuse | (a) Blitz-only w/ caveat (b) omit | **(a) Blitz-only + caveat chip** |
| Q-7 | Should positive verification persist to contacts DB (P1-4 needs a contacts-api change)? | Cuts repeat MailTester load; touches the other codebase | (a) yes (b) local cache only | **(b) local email-keyed cache first**, contacts-api change as P1 |
| Q-8 | Who operates and at what volume/day? (assumed: 1–3 AMs, ≤5 jobs/day, ≤20K contacts/job) | Sizing for verify batching + quotas | confirm or correct | Proceed on assumption; revisit if >5× |

Questions 1–10 from the brief §8 that the code already answers (account-TAM definition reachable-set, ALL-people semantics, compulsory CSV fields, personal-email handling — LO/MoltSets personal fallbacks exist and are returned flagged; UI has no exclude toggle) are answered in §§5–7 and not re-asked.

---

## 14. Bottom line

**Deployable quickly (≈2–3 weeks, P0):** a standalone email-verification tab (the only genuinely missing stage), identifier-flexible company intake so one mixed list flows through one job, TAM→people chaining that stops dropping LinkedIn-only accounts, and the stale-copy/doc fixes. Every one of these rides machinery that is already deployed and proven at 14K-row scale, with tests, resume, cancel, and write-back idempotency already in place.

**Remains manual/exceptional (by design, matching Atishay's qualification):** exotic TAM sourcing — directories, Google-search harvesting, Apollo/DiscoLike-style lookalikes beyond the built-in Blitz/GL lookalike mode, signal-based discovery. None of these are integrated, and none need to be for the default path; the platform's escape hatch is "bring any company CSV" plus the external scraper API and MCP tools for bespoke work. A signals playbook should be **written** (it does not exist), but it is an SOP artifact, not code.

**Conditions for realistically covering ~90% of list-building cases:** (1) the client's ICP maps onto Blitz's TAM filter surface (industry/size/geo/persona) or 3–5 seed companies exist; (2) accounts carry — or resolve to — a domain or company LinkedIn URL (name-only lists need P0-2's resolver and will still yield lower coverage); (3) the contact goal is decision-makers by title family rather than literally all employees (per-provider caps make "ALL people" a recall objective, not a guarantee — QE ≤40/domain, LO ≤100/domain, waterfall ≤ max DMs); (4) catch-all tolerance is decided per client (Q-4) since default policy currently drops those addresses; (5) Clay stays optional on both sides — it is a client of this API today and nothing in the default pipeline depends on it.

---

### Contradictory/stale references on record (for the reviewer)

1. `/flows/help` cascade table (routes.py:939-948) omits LeadOcean/MoltSets and mislabels BetterEnrich — vs live code E-013/E-014. (E-015)
2. Help page "Last Updated 2026-07-05" — 6-source list, "BetterEnrich/Prospeo disabled" — vs 7-active-source reality. (E-041)
3. People-page "6.6M+ leads" vs 30.8M persons. (E-005)
4. mailtester docstring "valid if ok or mb" vs env policy `ok`. (E-045)
5. API-reference header date 2026-10-13 vs changelog rows 2026-11-06/07; A.1 missing `leadocean_email`. (E-041)
6. MCP oracle `providers://status` footer "order set 2026-10-10" vs its own live 9-tier table. (E-042)
7. The memory-index phrasing "UI strict checkbox shipped unchecked → gate off" is easy to misread as the *current* state; the underlying note is correct that `b404c0e` flipped the default to **checked (gate ON)**, which is HEAD's behavior today. (E-008)

*End of report. Prepared 2026-11-10; all timestamps UTC. No production systems were modified during this audit.*
