---
name: listbuilding
description: Complete lead-generation toolkit for the ListBuilding platform (listbuilding.eagleinfoservice.com) — the full enrichment waterfall (contacts_db → smartprospect → quickenrich → getleads → blitz → wizleads) for decision-maker emails from ANY input (domains, person/company LinkedIn URLs, company names, business name+address, or a mixed CSV), PLUS the Google Maps scraper for local-business data across 30 countries, PLUS TAM company discovery, ICP people search, and the free website-email rescue. Use when the user wants "emails for these companies", "find decision makers", "enrich this CSV/lead list", "who to contact at X", "scrape Google Maps", "find dentists/plumbers/cafes in X", "local business leads by city/country", or any end-to-end list building (scrape → enrich → emails → phones).
---

# ListBuilding — Complete Lead Generation & Enrichment Skill

One skill for the whole agency pipeline: **scrape local businesses → enrich their
websites → decision-maker emails → optional phones**, from any machine, with one API key.

> **Copy-paste requests live in `requests/`** (next to this file): `requests/api/`
> holds one ready-to-run script per **UI page** (scrape / domains / linkedin /
> people / TAM / single lookup / jobs), and `requests/mcp/` holds MCP setup +
> oracle/tool examples. Start at `requests/README.md` — it maps every web-app
> page to its API/MCP equivalent.

> **State verified against live platform code + config: 2026-10-13** (post
> QuickEnrich cascade reorder of 2026-10-10). When live endpoints disagree with this
> doc, **live wins** — see "Live truth sources" at the bottom.

---

## Part 0 — Authentication (API key, no login)

```bash
KEY=$(cat ~/.claude/.listbuilding-api-key 2>/dev/null || cat ~/.claude/.listbuilding-enrichment-api-key)
# team VPS: /hyperke-clients/.claude/.listbuilding-api-key (shared)
BASE="https://listbuilding.eagleinfoservice.com"
```

Use on **every** request: `-H "X-API-Key: $KEY"` (also accepted as `Authorization: Bearer $KEY`).

- **Every `/api/enrichment/*` endpoint accepts the API key** — upload, flows, jobs, downloads,
  search — EXCEPT the SSE stream (`GET /jobs/{id}/stream`, JWT-only). **Poll `GET /jobs/{id}` instead.**
- **Every `/api/external/scraper/*` endpoint accepts the API key.** Scraper CSV downloads
  (`GET /api/scraper/jobs/{id}/download`) accept it too.
- JWT login (`POST /api/auth/login`) works but is unnecessary; its response field is `token`.
- Admin-tier keys: no daily quota; job listings show ALL users' jobs — track the `job_id` YOUR
  create-call returned. Non-admin keys see only their own jobs.
- Key missing? Request from the platform owner. One-time setup:
  `install -m 600 /dev/stdin ~/.claude/.listbuilding-api-key <<< "lgp_THE_KEY"`
- **Phone enrichment (`/api/phone-enrichment/*`) is a separate JWT-only UI surface** — not on
  the API-key surface. Phones reach API consumers two ways: the paid `include_phone` flag on
  enrichment jobs (Blitz Direct Phone, US-only) and phone columns already stored in the
  contacts DB (≈395K numbers backfilled 2026-10-11).

---

# Part 1 — THE WATERFALL (the complete cascade)

Every enrichment request walks a cascade of providers. **The cascade stops at the first
provider that returns a usable contact** for a row; later providers are skipped for that row.
`contacts_db` (the platform's own 30M+ person database) is always first and always free.

## The three arms (order set 2026-10-10, owner-locked)

```
PERSON (name + domain, or domain+name hints):
  contacts_db → smartprospect → quickenrich → getleads → blitz → wizleads

LINKEDIN URL (person profile):
  contacts_db → quickenrich(linkedin) → getleads(from-linkedin) → blitz(pe_by_li) → blitz(find_work_email)

DOMAIN ONLY (discover decision makers):
  contacts_db → quickenrich(dataset-search) → blitz(title waterfall) → getleads(decision-makers)
  └─ misses can continue to: website-email rescue (free, automatic) on qualifying jobs
```

- `better_enrich` (disabled 2026-09-29, credit incident) and `prospeo` (disabled) are off
  end-to-end. **The global kill switch beats `force_provider`/`selected_providers`** — forcing
  a disabled provider returns `no_email_reason="provider_disabled"`, not a vendor error.
- Resumed/restarted jobs created BEFORE 2026-10-10 inherit their original provider selection
  (no QuickEnrich). Only newly created jobs include it.

## Provider-by-provider (rates, cost model, quirks)

| # | Provider | Rate limit | Cost model | Quirks you must know |
|---|----------|-----------|------------|----------------------|
| 1 | `contacts_db` | 75 RPS | **Free** (internal PG, 30M+ persons) | Emails run through MailTester; invalid → cascade continues to paid tiers. Source labels `contacts_db_*`. |
| 2 | `smartprospect` | 1900 RPM shared cross-process (account cap 2000/min) | Paid, credit on delivered email; batch ≤10 | Needs firstName+lastName+domain (or LinkedIn). Self-verifying: trust `verification_status="Valid"` only. Batch pre-pass on Flow-1 jobs. |
| 3 | `quickenrich` | Two SHARED cross-process lanes: person-search 5800 RPM + dataset-search 5800 RPM since 2026-10-13 (GTM Unlimited/max plan, unlimited leads — 6000 RPM cap is GLOBAL across endpoints, both lanes share it; code defaults 900/280 = Standard-safe) | Paid; employee-search credits on match only; dataset-search credits per returned employee-with-email (empty page free) | Newest provider (2026-10-10). Person email via LinkedIn URL or name+company URL. Domain DMs via dataset-search (title-filtered, has_email). Provider-side quirks (title param ≤255 chars, boolean flags `1`/`0`) are handled by the platform client — don't work around them. |
| 4 | `getleads` | batch 100 (plan ~10k/min); unlimited-plan fair-use budget (UTC calendar-day + monthly reset) | Paid credits per record; from-linkedin fallback in the LinkedIn arm; `lookup_decision_makers` domain fallback (C-Team/VP/Director) when the whole domain arm finds nobody | Batch pre-pass of 100/chunk on Flow-3. Decision-makers fallback costs 1 credit/record — fires only after contacts_db + QE + Blitz all miss. |
| 5 | `blitz` | 50 RPS per endpoint (legacy plan), per-lane client caps; **FUP 15M records/month** — check `GET /api/enrichment/blitz/fair-use` before big runs | **Search endpoints bill 1 record per RETURNED result (empty = free); `/v2/enrichment/email` + `/v2/enrichment/phone` bill per CALL (miss still bills); `/v2/enrichment/person` + domain-to-linkedin bill on success only; errors never bill** | LinkedIn-based enrichment with the 3-tier title waterfall. `/person` endpoint is plan-dead. >5-domain Flow-1 jobs get a free-ish batch prepass (`/v2/search/people`, 50 companies/call). 30-day domain-miss negative cache — definitive misses skip Blitz on replay. Phone bundle (`include_phone`) = its own 50-RPS lane, **does NOT drain the 15M FUP pool**, miss = $0 but call still bills. |
| 6 | `wizleads` | 10 RPS | Paid | Last active tier, catch-all verified email. Weakest yield (52% of its calls historically 429) — it's the "one more try" tier. |
| — | `better_enrich` | — | — | **Disabled 2026-09-29.** Don't select or force it. |
| — | `prospeo` | — | — | **Disabled end-to-end.** Not a valid selection. |

## The title ICP gate (why some contacts disappear)

- Flow-1/domain jobs default `strict_titles: true` — a **local gate drops off-ICP contacts**
  (≈77% of off-title Blitz leak eliminated). Provider fuzzy matches get re-filtered locally,
  so what lands in your CSV matches your titles.
- `titles` (≤50, comma-separated) fuzzy-matches LinkedIn headlines. `exact_titles: true`
  switches to server-side exact `[CEO]` matching on Blitz — **cheaper** (bills fewer returned
  records); use when titles are precise role names.
- Default 3-tier cascade if you pass nothing: Owner/CEO/Founder/Co-Founder/President →
  C-level/VP → Director/Head-of X (`GET /api/enrichment/default-cascade`).
- Company-LinkedIn-URL paths are gated too (fixed 2026-10-06) — same title discipline on every arm.
- Off-ICP contacts dropped by the gate are flagged `title_gate_dropped` in job telemetry.

## Controlling the cascade per request

| Want | Field | Notes |
|------|-------|-------|
| Free pass only | `selected_providers: ["contacts_db"]` | Always run before paid; re-run paid only for misses |
| Single provider | `force_provider: "quickenrich"` | e.g. smoke-test one tier |
| Allowlist | `selected_providers: [...]` | `contacts_db` always allowed even if omitted; XOR with force (400 if both) |
| Full cascade | *(omit both)* | **Required on the FINAL pass of any big run** — provider-restricted jobs are excluded from the free website-email rescue |

**Never select a provider that live state says is off** — check "Live truth sources" below.

---

# Part 2 — The Universal CSV runbook (point it at ANY leads file)

**Default mode for "here's a CSV, get me emails."** Profile the file, route each row down its
strongest path, ask at most ONE round of questions, run, merge everything into ONE output CSV.

## Phase A — Understand the ask (30 seconds)

1. **Titles/ICP**: if unstated, ask ONCE: *"No titles specified — use the default 3-tier
   cascade (Owner/CEO/Founder → C-level/VP → Director/Head of X), or give me your titles?"*
   "Just get me contacts" → default. Never invent titles silently.
2. **Phones**: only if asked — `include_phone` is **paid per call even on miss, US-only**. Ask first.
3. **Scale**: note row count for Phase D expectations.

## Phase B — Profile the file

Read header + ~20 sample rows (>5K rows: random sample). Classify columns by header synonyms
AND value shapes:

| Role | Typical headers | Value shape |
|---|---|---|
| `domain` | domain, website, url, site, web, homepage | `acme.com` or any URL (Flow 1 normalizes) |
| `person_li` | linkedin, linkedin url, profile url | contains `linkedin.com/in/` |
| `company_li` | company linkedin, company url | contains `linkedin.com/company/` (also `/school/`) |
| names | first/last/full name, contact name | plain names |
| `company_name` | company, business, organization | plain names |
| `city`/`address` | city, town, address, street, state, zip, country | plain text |
| `existing_email` | email | has `@` — already-have; don't re-find |

**One `linkedin` column can hold BOTH `/in/` and `/company/` values — classify per ROW.**

**Row bucketing** (strongest identifier wins; B5 beats B1 — a specific person beats a domain;
B1 beats B2):

| Bucket | Row has |
|---|---|
| **B5 person-li** | person LinkedIn URL (highest priority) |
| **B1 domain** | domain/website value |
| **B2 company-li** | company LinkedIn URL, no domain |
| **B3 place** | company name + city/address, no domain/URL (local-biz file) |
| **B4 name** | company name only |
| junk | none — skip and report |

Report bucket counts to the user before running: *"3,120 rows: 1,900 domains · 700 person
LinkedIn · 220 company LinkedIn · 300 names (with city) · 12 junk"*.

## Phase C — Route each bucket

| Bucket | Route | How |
|---|---|---|
| B1 domain | **Flow 1** `/flows/domain-enrich` | `domain_col` + hint cols (`name_col`/`first_name_col`/`last_name_col`, `company_name_col`, `existing_email_col`). **Do NOT pass `company_linkedin_col` here** — a row with BOTH domain and company-URL short-circuits to the (weaker) company-URL path and skips the richer domain cascade. |
| B2 company-li | **Flow 1 + padding trick** | Flow 1 requires `domain_col` to *name a real column*, but company-URL rows work with **empty domain cells** (the company-URL branch runs before the empty-domain skip and honors your `titles` + strict gate). Create the bucket CSV with an empty `domain` column + `company_linkedin_col`. Prefer this over `/by-linkedin-v2` for title-controlled company runs — v2's company branch runs the default cascade with no local title gate. |
| B5 person-li | **Flow 3** `/by-linkedin-v2` | `personal_linkedin_col` + `include_company: true`. Keep domains as a plain extra column (echoed through). |
| B3 place | **Place match → B1** | Recipe below → website domain → run as B1 (its website-email rescue finds the `info@` addresses local-biz files usually want). Misses fall to B4. |
| B4 name | **Flow 2 resolve → B2** | Recipe below → company LinkedIn URL → B2 padded route. |

Titles from Phase A go on the Flow-1 jobs. Split buckets into separate CSVs (each carrying
`_lb_row_id` from Phase E), upload each once, one job per bucket.

### Name-resolution recipe (B4 — company names only)

Resolve each **unique** name (dedupe first) to a company LinkedIn URL:

```bash
curl -s -X POST $BASE/api/enrichment/search/companies -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"Spring Dental","country_code":"US","limit":5}'
# → {"count","total","results":[{"linkedin_url","name","industry","employee_count",
#    "company_type","hq":{"country_code","city"}}]}
```

- **No `domain` in results** — the `linkedin_url` is the payload; feed it to the B2 route.
- **Auto-pick** on near-exact name match, or when the CSV has a city and exactly one result's
  `hq.city` matches. Ambiguous ≤25 → show top-2 per name in ONE confirmation round; more →
  best-scored match + `match_confidence=low` in the output.
- Cost ≈ 1 Blitz credit per 5 results (empty free). ≤5 concurrent; honor 429 `Retry-After: 3`.
  Cache resolved names → URLs in a scratch file as you go.

### Place-match recipe (B3 — business name + address)

**Local mirror (free, instant) — VPS access only:**

```bash
sudo pg_lsclusters | grep lead-gen    # must report "online" for 5433
sudo -u postgres psql -p 5433 lead_gen -c "
SELECT DISTINCT ON (place_id) name, city, full_address, gmaps_email,
       split_part(regexp_replace(website, '^https?://', ''), '/', 1) AS bare_domain
FROM scraped_places
WHERE lower(city) = lower('<city>')
  AND lower(name) LIKE '%<normalized name>%'
  AND website IS NOT NULL AND website <> ''
ORDER BY place_id, (gmaps_email IS NOT NULL) DESC, inserted_at DESC NULLS LAST
LIMIT 10;"
```

- ~3.4M+ rows (platform scrapes + Outscraper import + nightly mirror of every new scraper
  campaign). Normalize first: lowercase, strip punctuation and inc/llc/ltd/corp/co/llp/plc.
  Pick the row whose address tokens match; `bare_domain` feeds Flow 1; `gmaps_email` already
  carries a contact email for ~12% of rows.
- Cluster down? (It does NOT auto-start on reboot.) On the VPS:
  `sudo pg_ctlcluster --skip-systemctl-redirect 16 lead-gen start`. Off-VPS: place match is
  unavailable → fall back to B4 with `country_code`.
- **Paid fallback of last resort** (ask user first): one tiny Google-Maps scrape job per
  unresolved business (`query` = business name, one city) via the scraper flow in Part 4 —
  cache-check first, estimate before scale.

## Phase D — Expectations for big files (inform, never block)

1. **Pilot first**: one bucket's first ~100 rows (or 100-row random sample) as its own job.
2. **Extrapolate**: *"pilot: 100 rows → 42 emails in 8 min ⇒ full 12K ≈ 16 h, ~5K emails"*.
   Then continue — no re-ask.
3. Rough reference (2026-10; pilot always beats these): large domain jobs ~10–15 rows/min →
   10K domains ≈ 11–16 h. LinkedIn buckets faster (batched 100/chunk). Name resolution
   ≈ 1–2 calls + ~0.2 credits per unique name.
4. **Blitz FUP check before big paid runs**: `GET /api/enrichment/blitz/fair-use` → relay
   remaining budget (search bills per returned result; email/phone per call).
5. **Website-email rescue (free, automatic)**: when an unrestricted domain-flow job finishes,
   no-email domains auto-submit to the website-email scraper (~51% historical fill) and a
   second CSV appears on the job later. Requires: NOT `website_only`, NO `providers` allowlist.
   **So the final pass of any big run is always unrestricted.**

## Phase E — Merge into ONE CSV + yield report

- Add `_lb_row_id` (0..n-1) to the original file before splitting; every bucket CSV carries it.
- Final = all bucket outputs concatenated, sorted by `_lb_row_id`: original columns first,
  then standard enrichment columns (Part 3), plus `enrich_path` (`domain`/`company_li`/
  `person_li`/`place_match`/`name_resolved`) and `match_confidence` (`high`/`low` on
  name-resolved rows).
- Flow 1 emits **one row per contact** (not per input row). Report row-level + unique-email
  stats, per-bucket yield, top `no_email_reason` values, followup-job status.
- Dedupe-flag same person across buckets (match email/LinkedIn) — keep both rows, note overlap.
- Save `<input>_enriched_YYYYMMDD.csv`; never overwrite input; file paths only in chat.

---

# Part 3 — Enrichment endpoints & flows

## Quick reference

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/enrichment/providers` | Which providers are ON (membership — NOT cascade order) |
| `GET` | `/api/enrichment/flows/help` | Self-documentation JSON (no auth; its cascade section may lag — trust Part 1) |
| `GET` | `/api/enrichment/default-cascade` | Default 3-tier titles (no auth) |
| `POST` | `/api/enrichment/enrich` | Single lookup (mode auto-detection) |
| `GET` | `/api/enrichment/enrich/{domain}` | Quick single-domain lookup |
| `POST` | `/api/enrichment/upload` | Upload CSV → `upload_id` |
| `POST` | `/api/enrichment/flows/domain-enrich` | **Flow 1** — bulk domain enrichment (+ company-LI via padding) |
| `POST` | `/api/enrichment/by-linkedin-v2` | **Flow 3** — bulk LinkedIn enrichment (person URLs) |
| `POST` | `/api/enrichment/search/companies` | **Flow 2** — company search by name/ICP (the name-resolver) |
| `POST` | `/api/enrichment/flows/tam` | **TAM by People** — persona+firmographic company discovery (paid) |
| `POST` | `/api/enrichment/search/employees` | **Find People** — contacts-DB search (free) |
| `GET` | `/api/enrichment/search/options` | Filter enumerations |
| `GET` | `/api/enrichment/blitz/fair-use` | Blitz FUP meter (check before big runs) |
| `GET` | `/api/enrichment/jobs` | List jobs (status/search/date_from/date_to/file_ready/source) |
| `GET` | `/api/enrichment/jobs/{id}` | **Status polling** |
| `GET` | `/api/enrichment/jobs/{id}/download` | Result CSV (**202 while running**) |
| `GET` | `/api/enrichment/jobs/{id}/partial-download` | In-progress CSV |
| `GET` | `/api/enrichment/jobs/{id}/shards` + `/shard/{n}` | 10K-row shards for huge files |
| `GET` | `/api/enrichment/jobs/{id}/resume-info` | Recovery eligibility |
| `POST` | `/api/enrichment/jobs/{id}/restart` | True resume (checkpoints, skips done rows) |
| `POST` | `/api/enrichment/jobs/{id}/cancel` | Cancel (partial preserved) |
| `GET` | `/api/enrichment/jobs/{id}/webscrape-followup/download?token=` | 2nd CSV: website-scraped emails |
| `GET` | `/api/enrichment/website-scrape/status` | `website_only` cohort freshness |
| `GET` | `/api/enrichment/stats/sources` | Email-source stats |

## Flow 1 — Domain enrichment (`/flows/domain-enrich`) — the bulk workhorse

```bash
UPLOAD=$(curl -s -X POST $BASE/api/enrichment/upload -H "X-API-Key: $KEY" -F "file=@leads.csv")
UPLOAD_ID=$(echo "$UPLOAD" | jq -r .upload_id)

JOB=$(curl -s -X POST $BASE/api/enrichment/flows/domain-enrich -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d '{
    "upload_id": "'"$UPLOAD_ID"'",
    "domain_col": "domain",
    "max_results": 5,
    "titles": "CEO,Founder,VP,Director"
  }')
JOB_ID=$(echo "$JOB" | jq -r .job_id)   # → {"job_id","total","flow","deduped_count"}
```

Body fields: `upload_id`, `domain_col` (required — must name a real column; **cells may be
empty for company-URL rows**) · hint cols `name_col`/`first_name_col`/`last_name_col`/
`linkedin_url_col` (auto-detects `/company/` values per row; person URLs here are echoed NOT
enriched — route those to Flow 3)/`company_linkedin_col`/`phone_col`/`company_name_col`/
`existing_email_col` · `max_results` (5) · `providers` (allowlist — **don't set on final
passes**) · `titles` (≤50, fuzzy vs headlines) · `strict_titles` (**true**) ·
`normalize_domains` (true) · `dedupe_by_domain` (true) · `website_only` (false — zero paid
providers, only the nightly webscraper cohort) · `source` · `lead_universe` · `exact_titles`
(false — cheaper server-side exact match on Blitz) · `include_phone` (false — **paid per call
even on miss**, US-only) · `phone_for_all` (false — one call per eligible row; more cost).

## Flow 3 — LinkedIn URLs (`/by-linkedin-v2`)

At least one of `personal_linkedin_col` / `company_linkedin_col`. `max_dms` (5),
`include_company` (true), `include_phone` (false). Person URLs run the LinkedIn arm
(contacts_db → quickenrich → getleads batch pre-pass 100/chunk → blitz). The company column
runs the default cascade **with no local title gate** — title-controlled company runs should
use Flow 1 + padding instead.

## Flow 2 — Company search (`/search/companies`)

ICP discovery AND the name-resolver: `{"name":"Acme","industry":[…],"employee_range":["51-200"],
"country_code":"US","limit":50}` → results carry `linkedin_url` (**no domain**) → feed into
Flow 1 (padded) or TAM-chained enrichment.

## TAM by People (`/flows/tam`) — paid, 1 Blitz record/company

Persona + firmographics → deduplicated company CSV (`name, domain, website, linkedin_url,
industry, type, size, employees_on_linkedin, followers, founded_year, hq_*, revenue, slogan,
employee_growth_1y, matched_people`). ≥1 filter required (400 otherwise). Unknown keys /
bad `job_levels` → 422.

```bash
JOB=$(curl -s -X POST $BASE/api/enrichment/flows/tam -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' -d '{
    "company": {"industry_include":["Software"],"employee_range":["51-200"],"hq_country_code":["US"]},
    "people": {"job_title_include":["VP Engineering"],"job_levels":["VP"],"min_per_company":1},
    "max_companies": 500,
    "create_enrichment_job": true, "titles": "VP Engineering", "exact_titles": true
  }')   # → {"job_id","flow":"tam","total"}  (total = budget ceiling, not result count)
```

- Filters: `company{linkedin_url≤50, name/industry/type/keywords_include|exclude,
  employee_range, employee_count_min/max, min_linkedin_followers, revenue_min/max,
  founded_year_min/max, hq_country_code/continent/sales_region}` · `people{
  job_title_include/exclude, job_levels (C-Team|VP|Director|Manager|Staff|Other),
  min_per_company 0-25, location_country_code}`.
- `max_companies` (default 1000, cap 10000) = budget, enforced mid-page; 50/page cursor.
- `create_enrichment_job: true` chains a Flow-1 job over discovered domains
  (`source_type='tam_chain'`). Poll/cancel/download via the normal job endpoints.
- UI equivalent: "Find Companies (TAM)" page — prefer it for humans. Every company returned
  is auto-saved to the contacts DB.

## Find People (`/search/employees`) — free

```bash
curl -s -X POST $BASE/api/enrichment/search/employees -H "X-API-Key: $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"title_keywords":"founder","geo_country":["United States"],"has_email":true,"universe":"saas","limit":50}'
```

Filters: `seniority[]`, `function[]`, `geo_country[]`, `industry[]`, `title_keywords`,
`name_contains`, `has_email`, `universe` (`local_business|b2b_agency|saas|ecom`, else 422),
`limit`, `offset`. Returns `{"total","limit","offset","people":[…]}` (`total` null on keyword
searches). Enumerations: `GET /search/options`.

## Single-row (`/enrich`) — mode auto-detection

| Inputs | Mode | Behavior |
|--------|------|----------|
| `domain` only | `domain_only` | Full domain cascade, all decision makers |
| `linkedin_url` only | `linkedin_only` | Person: contacts_db → quickenrich → getleads → blitz |
| `company_linkedin_url` only | `company_linkedin_only` | Decision makers via company URL (title-gated) |
| `domain` + (`full_name` or `linkedin_url`) | `enhanced` | Person first; **falls back to domain contacts if not found** |

`/company/` URL in `linkedin_url` (and vice-versa) is auto-rerouted. `company_name` alone is
NOT accepted — resolve via Flow 2 first.

```bash
# free pass first ($0)
curl -s -X POST $BASE/api/enrichment/enrich -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"domain":"acme.com","selected_providers":["contacts_db"]}'

# full cascade
curl -s -X POST $BASE/api/enrichment/enrich -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"domain":"acme.com","max_results":3,"titles":"CEO,Founder,VP"}'
```

Response: `{domain, mode, company_linkedin_url, contacts[{full_name,title,email,linkedin_url,
headline,location_city/country,icp_tier,email_source,validation_status,email_verified,
verification_message}], contact_count, data_sources{...}, sync_to_contacts_db{status,records_*}}`.
`email_source`: `contacts_db_*`, `smartprospect_email`, `quickenrich_email`, `getleads_email`,
`blitz_*`, `wizleads_email`, `not_found`. **Every successful call syncs to the platform
contacts DB** — don't re-enrich what's synced.

## Job lifecycle (all flows)

1. **Upload** (multipart, .csv) → `upload_id`
2. **Create job** → note YOUR `job_id` (admin keys see everyone's jobs in listings)
3. **Poll** every 2–5 s: `curl -s $BASE/api/enrichment/jobs/$JOB_ID -H "X-API-Key: $KEY" |
   jq '{status,total,processed,emails_found}'` — statuses `queued, running, done, failed,
   partial, cancelled, abandoned` (success = **`done`**; progress = `processed/total`)
4. **Download**: 202 `{"detail":"Job not finished yet."}` while running; partial CSV on
   `partial`/`cancelled`; `/shards` + `/shard/{n}` for huge files
5. **Recover**: `resume-info` → `can_resume`; `restart` = true resume (skips done rows,
   carries partial CSV). Cancel preserves partials. Re-upload on failure is the wrong move.

Looping single rows (≤~100 domains): ≤5–8 concurrent, honor 429. Above that use a CSV job.

## Output CSV columns

Input columns preserved; appended: `company_linkedin_url/company_name/company_industry/
company_employee_count`, `dm_first_name…dm_full_name`, `dm_title`, `dm_job_level/function`,
`dm_linkedin_url/headline/location_*`, `dm_email/dm_email_source`, `dm_email_verified/
mailtester_code/mailtester_message`, `dm_phone`, `dm_icp_tier`, `row_status` (enriched/
no_linkedin/no_contacts/not_found/error/skipped), `input_*` echoes, `provider_attempts/
providers_called/providers_skipped`, `final_email/level`, `seg_classification/provider`,
`dm_previous_companies`/`dm_previous_titles` (`" | "`-joined non-current roles, 500-char cap).
The universal merge adds `enrich_path` + `match_confidence`.

---

# Part 4 — THE SCRAPER (Google Maps local-business data)

Pull name/phone/website/address/rating/reviews for any business type across **30 countries**.
Works server-to-server. **The flow is ALWAYS: cache-check → estimate → create → poll → fetch.**

## Step 1 — FREE cache check (never skip)

```bash
curl -s -X POST $BASE/api/external/scraper/cache \
  -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"<BUSINESS TYPE>","mode":"cities","cities":["<CITY>"],"country":"us"}'
```

`cached:true` → **rows are right there** (paginate with `?offset=&limit=&fields=compact`).
Only create a job if the cache looks stale/insufficient. Cache lives 90 days.

## Step 2 — Dry-run estimate (cost/size preview, no job)

```bash
curl -s -X POST $BASE/api/external/scraper/estimate \
  -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"dentist","mode":"cities","cities":["Austin"],"country":"us"}'
# → {"center_count":N,"task_count":N*3,"task_basis":"centers_x_3_zooms",
#    "warnings":[],"cache":{...}|{"cached":false},"quota":{...},"can_proceed":true}
```

**1 task = 1 center×zoom call; every center costs ×3 (zooms 10/11/12).** Read `task_count`
before assuming job size. Run this before anything bigger than a handful of cities.

## Step 3 — Create the job

```bash
curl -s -X POST $BASE/api/external/scraper/jobs \
  -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"query":"<BUSINESS TYPE>","mode":"cities","cities":["<CITY>"],"country":"us","prefer_cache":true}'
```

- `prefer_cache: true` (default): a **full** unexpired cache hit short-circuits — response has
  `created:false, served_from_cache:true, rows_available` + a direct CSV link. **No job row,
  no quota, no cost.** Partial hits fall through to a fresh job.
- Otherwise note `job_id`, `suggested_poll_seconds`, `links` from `data`.

## Step 4 — Poll until terminal

```bash
curl -s $BASE/api/external/scraper/jobs/<JOB_ID> -H "X-API-Key: $KEY"
```

Loop every `suggested_poll_seconds` until `data.status` ∈ `done|failed|cancelled|abandoned`.
`data.progress.pct_complete`, `data.rows_on_disk` show progress. (No SSE — poll.)

## Step 5 — Fetch results

```bash
curl -s "$BASE/api/external/scraper/jobs/<JOB_ID>/results?limit=1000&offset=0&fields=compact" \
  -H "X-API-Key: $KEY"
```

Paginate with `offset`. CSV instead: `curl -s -H "X-API-Key: $KEY" \
$BASE/api/scraper/jobs/<JOB_ID>/download -o results.csv`. 409 `not_ready` on results = still
running → poll status, wait `retry_after`.

## Geography modes

| Mode | Fields | Notes |
|---|---|---|
| `cities` (recommended) | `cities:["Austin"]` | Fuzzy-matched; ~8 centers/city → ×3 tasks each |
| `states` | `states:["Texas"]` | |
| `zips` | `zips:["78701"]` | **US only**, 5-digit |
| `centers` | `center_ids:["Berlin"]` | Named centers (non-US too) — **order matters for cache hits** |
| `all` | — | Whole country — HUGE (US ≈ 88,638 tasks); never without explicit user ask |

**30 countries:** us gb ie au ca · de fr es it nl be pl se dk at ch pt no nz ·
mx br ar co cl pe ve ec bo py uy · id (Indonesia, 288 centers).
Discover geography: `GET $BASE/api/scraper/regions/countries`, `.../centers?country=de`,
`.../cities?q=<term>&country=us` (unauthenticated).

**Many cities at once**: ONE job with `cities:[...]` (≤1000), not N jobs.

## Result fields

`fields=compact` (13, LLM-friendly): name, category_name, full_address, city, city_state,
phone, website, rating, review_count, place_link, place_id, latitude, longitude.
Or cherry-pick: `fields=name,phone,website`. Omit for all 34. All values are strings.

## Scraper → enrichment (using the data)

The website column is the bridge to emails:

1. Fetch results (`fields=website,name,full_address,phone,...`).
2. Filter rows with a non-empty `website` (typically ~60–75%).
3. Save `{name, website}` as a CSV → **Flow 1** (`domain_col: "website"`) → decision-maker
   emails + auto write-back to the contacts DB.
4. Rows whose domain finds no email get the **free website-email rescue** automatically
   (unrestricted jobs only) — a second CSV with scraped `info@`-style addresses later.
5. Local-business rows usually want `info@` anyway — consider `website_only: true` (zero paid
   providers) or a `contacts_db`-only first pass for cost-free coverage.

**UI-only extras** (JWT surface, not the API-key surface): scraper→enrichment one-click
chaining on job cards, and **campaign buckets** (`group` label → grouped jobs render as one
folder card with a merged-CSV download, `GET /api/scraper/jobs/group/{name}/download`).
API-key users replicate chaining with steps 1–3 above.

## Scraper cost & rate awareness

- Admin keys have no daily quota, but big jobs still cost real scraper.tech money —
  estimate first, cache-check always.
- Non-admin keys: 50K/day task quota (`GET /api/external/scraper/quota` shows usage);
  per-job task cap may apply (422 with the cap in the message).
- nginx: `/api/external/*` ≈10 r/s burst 20 per IP; shared team IPs share the bucket —
  on 429 back off and honor `Retry-After`.

---

# Part 5 — Errors & rate limits (both surfaces)

Bodies are `{"detail":"…"}` (external surface wraps in `{success,data,error,meta}`).

| Code | Meaning / fix |
|------|---------------|
| 401 | bad/missing key |
| 403 | not your job (non-admin key) |
| 404 | bad id |
| 400 | bad column name · >50 titles · both restrictors set · chain on non-done job |
| 409 | scraper results not ready → poll status |
| 422 | invalid `universe` · TAM unknown filter key / bad `job_levels` · non-admin task cap |
| 429 | rate-limited — enrichment `/enrich` burst 50, `/api/enrichment/*` burst 40, `/api/external/*` burst 20 per IP; wait `Retry-After: 3` |
| 503 | SQLite busy — retry ~3 s |
| 202 | CSV download before job done |

**No daily quota on enrichment** — provider credits are the only meter. Scraper side has the
50K/day task quota for non-admin keys.

## Common mistakes

- Polling for `completed` — the value is **`done`** · Calling `/stream` with a key — poll instead
- Both restrictors set — 400 · Assuming enhanced mode = person-only — it falls back to domain
- Re-upload on failure — use `/restart` · Overwriting the input CSV — always save a new file
- Passing `company_linkedin_col` on rows that HAVE domains — silently downgrades to the weaker path
- Setting a `providers` allowlist on the final pass — kills the free website-email rescue
- Enriching person URLs via Flow 1's `linkedin_url_col` — echoed, not enriched; use Flow 3
- Creating scraper jobs without cache-check + estimate — identical queries waste real money
- `mode:"all"` country-wide jobs without an explicit user ask
- Selecting `better_enrich` or `prospeo` — both disabled; forcing returns `provider_disabled`
- Trusting `GET /providers` response order as cascade order — it's membership only (Part 1 = order)

---

# Part 6 — Live truth sources (when this doc looks stale)

1. **Cascade order**: this skill's Part 1, else the **ListBuilding MCP oracle**:
   ```bash
   claude mcp add listbuilding --transport http \
     --url https://listbuilding.eagleinfoservice.com/mcp/ \
     --header "X-API-Key: $(cat ~/.claude/.listbuilding-api-key)"
   ```
   Ask: `list_current_providers()`, `find_endpoint_for_intent("…")`, `get_endpoint_details(method, path)`,
   `explain_error_code("429")`, `explain_response_field("email_source")`. Also hosts scraper
   action tools (`scrape_local_businesses` dry-run default, job status/results, cache check, cancel).
2. **Provider membership**: `GET /api/enrichment/providers` (which are on — order not meaningful).
3. **Self-doc**: `GET /api/enrichment/flows/help` (endpoint shapes reliable; its embedded
   cascade list can lag behind reorders — Part 1 wins).
4. **Blitz budget**: `GET /api/enrichment/blitz/fair-use`.
5. **Platform owner** — keys, unexpected behavior. If live endpoints disagree with this file,
   live wins — and get this skill updated.

## Maintenance note (platform owner)

After any provider add/remove/reorder or surface change: re-verify Part 1 against
`enrichment/pipeline.py` `route_enrichment`, update this file, the two machine-local skills
(`listbuilding-enrichment`, `listbuilding-scraper` in `~/.claude/skills/`), the team repo copy
(`lead-list-enricher` via `scripts/sync-to-team-repo.sh`), and the platform API reference.

## Rules

- NEVER print the raw key; always `$KEY` from the key file. Never embed keys in files that
  leave your machine — this skill ships WITHOUT any key.
- ALWAYS free-first (`contacts_db` pass before paid); results to files, never dumped into chat.
- Don't re-enrich synced contacts; cancel runaway jobs (`POST /jobs/{id}/cancel`).
- Universal mode: ask titles ONCE, pilot before big files, one merged CSV with `enrich_path`.
- Never route to a provider that live state says is off; never skip the scraper cache-check.
