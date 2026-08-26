# Website-Scrape Data Integration — Implementation Plan

**Status:** Approved design, not yet implemented
**Date:** 2026-08-26
**Owner:** Lead Generation Platform
**Source:** `webscraper.eagleinfoservice.com` scraping VPS (`ubuntu@144.217.94.180`)
**Target:** Local Contacts DB (`contacts` on port 5432, exposed as `leadsdatabase.cc`)

---

## 0. Executive Summary

A nightly incremental sync imports curated website-scraped contact data (~2.44M domains,
~1.23M emails, ~75K named contacts) from the web-scraping VPS into the Contacts DB, tagged
`website_scrape`. After import, this data flows to users through the **existing** cascade —
Contacts DB is already the first, free step of every enrichment — so no cascade provider
changes are needed. A "Website data only" toggle lets users restrict a run to this source
and skip all paid providers.

**Measured overlap (2026-08-26, read-only analysis):**

| Metric | Value |
|---|---|
| Website-scrape domains | 2,439,584 |
| Contacts-DB domains (`core.company.website_norm`) | 6,412,687 |
| **Domain overlap** | **1,106,046** (45.3% of ws, 17.2% of contacts-db) |
| **Net-new domains** to Contacts DB | **1,333,538** (54.7% of ws) |
| Curated (`own_domain`) emails | 254,524 distinct |
| Curated-email overlap (20K sample) | ~36% → est. ~163K net-new emails |
| Named contacts (`metadata.email_contacts`) | ~75,553 rows |

**No 5433 cluster, no mirror table.** The contacts DB is the single destination; the sync
is a direct curate-and-push pipeline over SSH. (5433 stays off — it has a history of
errors and is not needed for this design.)

---

## 1. Goals & Non-Goals

### Goals

1. **Daily incremental import** of website-scraped contacts into the Contacts DB with a
   stable source tag (`website_scrape`), without live connections to the scraping VPS
   during user requests.
2. **Cascade integration for free**: website-scrape data participates in every normal
   enrichment run through the existing Contacts DB first step.
3. **"Website data only" mode**: a UI toggle that restricts a run to website-scrape data
   only (no paid providers, no fresh scraping) — with honest, plain-language expectations.
4. **Provenance preserved**: every imported row carries its source, so users can
   include/exclude it and analytics can attribute it.
5. **Quality floor at import**: junk email classes filtered before push; unverified
   status recorded; no title gating (titles are too sparse to gate on).

### Non-Goals

- Re-scraping or on-demand scraping from the lead-gen platform. New scraping happens only
  at `webscraper.eagleinfoservice.com`.
- A new cascade provider (7th slot). Explicitly rejected: contacts_db already serves this
  role; adding a provider would require ~30 wiring points across three mirrored
  `VALID_PROVIDERS` sets.
- Reviving the 5433 `lead_gen` cluster or maintaining a local mirror/staging DB.
- Importing operational columns (`http_status`, `worker_id`, retries, timing).
- Modifying the scraping VPS or its database in any way (read-only consumer).

---

## 2. Architecture

```
webscraper.eagleinfoservice.com VPS (144.217.94.180)
│  email_enrichment (2.44M domains) + gmaps_places (2.45M rows)
│
│  ① NIGHTLY SYNC — systemd timer, ~03:30 local, over SSH
│     watermark = last completed_at seen; pulls only newer rows
│     throttle: small batches, read-only DB user
▼
CURATE ON THE FLY (streaming, in the sync process)
│  - email classes: keep own_domain + role_service; drop freemail,
│    off_domain, placeholder, vendor_*, artifact, pubsec_mismatch
│  - email_shared_nd cap (default 20) — kill hosting/aggregator junk
│  - domain normalization identical to platform (normalize_domain)
▼
PUSH to Contacts DB — via contacts API / sanctioned upsert path
│  company upsert (domain, name, phone, city/state/country, rating,
│                  reviews, gmaps_url, gmaps_types → custom_fields)
│  + email upsert(s) (unverified, source=website_scrape, type, confidence)
│  + person upsert for named email_contacts {e,n,t}
│  idempotent: re-running the sync or backfill never creates duplicates
▼
CONTACTS DB (leadsdatabase.cc) — single source of truth
│
├── ② Normal cascade: contacts_db is already step 1 → website data
│   flows into every run automatically, source-labeled
│
└── ③ "Website data only" toggle (Domain Enrichment tab):
    filtered lookup (source=website_scrape) — no paid providers called,
    no fresh scraping submitted
```

### Sync state

Lives in the platform's `jobs.db` (one row in a small `website_scrape_sync_state` table:
`last_completed_at` watermark, `last_run_at`, `last_run_status`, `rows_pulled`,
`rows_pushed`, `skipped_junk`, `errors`). This is the only local footprint of the sync;
there is no separate database and no data at rest in the platform.

---

## 3. Phase-by-Phase Implementation

### Phase 0 — Prerequisites & access (est. 0.5 day)

- [ ] Set up SSH key auth to `ubuntu@144.217.94.180` (password works today; key is
      cleaner and scriptable). Add host alias `webscraper-vps` to `~/.ssh/config`.
- [ ] On the scraping VPS: create a read-only Postgres user (SELECT-only on
      `email_enrichment` + `gmaps_places`). Never expose its Postgres to the internet.
- [ ] Rotate the VPS password after key auth is working (a password sat in this chat).
- **Exit criteria:** `ssh webscraper-vps` works keyless; read-only user can run the
  watermark query.

### Phase 1 — Curate-and-push sync service (est. 1–1.5 days)

- [ ] New module `backend/enrichment/website_scrape_sync.py`:
  - `run_sync()` — pull rows newer than watermark, curate, push, update watermark.
  - Curation rules in a dedicated `CurationPolicy` dataclass (testable in isolation).
  - Pushes via the **contacts API** (HTTP upserts, reusing the `contacts_writer` pattern),
    never direct SQL into the `contacts` DB from this platform — same rule as every
    other write path.
  - Batched (500 rows), throttled (~40 rows/s, the measured safe upsert rate), with
    per-batch checkpointing so a mid-run failure resumes from the last batch.
  - Remote DB access over SSH (subprocess psql), never a network-exposed Postgres
    connection.
  - Runs outside the web workers (standalone systemd service + timer), so worker memory
    limits and the event loop are untouched.
- [ ] Watermark table in `jobs.db`, initialized at startup.
- [ ] First run = the backfill. See §4 volume estimate — re-estimate from dry-run output
      before the live run; schedule overnight.
- [ ] systemd unit + timer (`lead-gen-website-scrape-sync.timer`, daily ~03:30) with
      `flock` to prevent overlapping runs, plus an admin-only manual trigger endpoint.
- [ ] Env config: `WEBSITE_SCRAPE_SYNC_ENABLED`, `WEBSITE_SCRAPE_SYNC_HOST`,
      `WEBSITE_SCRAPE_SHARED_ND_CAP`, `WEBSITE_SCRAPE_BATCH_SIZE`.

### Phase 1b — Safe-by-default rollout (part of Phase 1)

- [ ] Everything ships disabled (`WEBSITE_SCRAPE_SYNC_ENABLED=false`). Nothing runs
      until the flag flips, so master is never at risk.
- [ ] `--dry-run` mode reports what would be pushed (counts per class) without pushing —
      used for validation before the first live push.
- **Exit criteria:** backfill complete; a second full run is a no-op (idempotency
  proof); nightly timer active and quiet.

### Phase 2 — Serving: cascade + website-only lookup (est. 1 day)

- [ ] **Normal cascade:** zero code change. Website-scrape data surfaces through the
      existing contacts_db step, source-labeled, participating in existing
      verified/unverified ordering. (Verify with tests; the change is literally
      "nothing".)
- Cascade behavior on overlap: when both website-scrape and provider contacts exist for
  a domain, both are returned, each source-labeled — website-scraped ones additionally
  marked unverified. Verified results keep ranking above scraped ones, so new data can
  only add, never demote. The existing source include/exclude toggle covers filtering.
- [ ] **Website-only mode:** extend the by-company lookup used by Flow 1 with a
      `source=website_scrape` filter (same pattern as the existing `outscraper` source
      filter param). When active: skip all paid providers, no cascade fallback.
  - Implementation note: `contacts_client.company_persons_by_domain` already supports
    `source` / `exclude_source` params — the work is plumbing the flag from the job
    request through `list_builder.run_domain_enrichment` to the by-company lookup, and
    ensuring the fallback cascade is skipped.
  - [ ] Lookups in website-only mode are read-only (no write-back of looked-up rows);
        write-back of user job results follows existing job behavior unchanged.
  - [ ] Title filter: **no title gate** on website-scrape data at import or serve time
        (titles too sparse). If a user enters target titles in a run, existing
        fuzzy/strict behavior applies to all sources equally (user-controlled).
- **Exit criteria:** a Flow 1 run with the website-only flag returns only
  website_scrape-sourced contacts and makes zero paid-provider calls (verified via
  `provider_call_log`).

### Phase 2b — Find People page (small, same day)

- [ ] Add `website_scrape` to the Find People source filter options (the `universe`
      filter pattern), so users can browse this cohort directly.

### Phase 3 — Frontend UI (est. 1 day)

In the Domain Enrichment tab, next to provider checkboxes (pattern: existing
`provider_outscraper` toggle):

- [ ] Toggle 1 — **"Website-scraped data"** (include/exclude website data in normal
      runs; default checked=include). Same semantics as the Outscraper toggle.
- [ ] Toggle 2 — **"Website data only"** (exclusive mode; default off). When on:
  - The run reads only existing database data (up to 24h stale). Tooltip:
    *"Reads only data already collected by the website scraper (up to 24h old). Does
    not start new website scraping. To get new websites scraped, submit them at
    webscraper.eagleinfoservice.com — results appear here after the next daily sync."*
  - [ ] Show **"data as of \<date\>"** (from the sync-state watermark) on the toggle.
  - [ ] Grey out the provider checkboxes — making it visually obvious that no paid
        providers will run.
  - [ ] Website-only mode + strict title search allowed, with the honest caveat that
        website-scrape titles are sparse; results may be fewer.
  - [ ] Mutual exclusivity: website-only overrides include/exclude toggle 1;
        conflicts with `force_provider`/`selected_providers` → 400 (existing
        validation pattern).
- [ ] Update the file-ready/segment counts help text if the mode changes counts.

### Phase 4 — Observability & docs (est. 0.5 day)

- [ ] Sync run logs to journald + the sync-state table (rows pulled/pushed/skipped/
      errors). `provider_call_log` unaffected (no new provider).
- [ ] `monitor.sh` additions: sync-age check (stale watermark > 48h → alert) + contacts
      DB disk-growth watch (root disk alert at 85% — verify monitor.sh's existing
      threshold wiring while there).
- [ ] Docs: `CLAUDE.md` env vars + integration section;
      `docs/ListBuilding_Platform_Full_API_Reference_*.md` (website-only mode param,
      behavior, caveats); `backend/api-docs/API_DOCUMENTATION.md` + `openapi.json`
      (`website_only: bool`, default false, on Flow 1).
- [ ] MCP oracle: add `website_scrape` to valid source values in `mcp_oracle/tools.py`
      + `resources.py`; update the `selected_providers`/source examples in `prompts.py`.

### Phase 5 — First backfill & go-live (est. 0.5 day + run time)

- [ ] Dry-run report → review counts per class (own_domain ~255K, role_service ~432K,
      plus named contacts ~75K + company-only rows; exact figures re-measured then).
- [ ] Flip `WEBSITE_SCRAPE_SYNC_ENABLED=true`.
- [ ] Backfill run overnight. Monitor via journalctl + sync-state table.
- [ ] Post-backfill verification queries (see §6).
- [ ] Enable UI toggles (default: include=on, only-mode=off).
- [ ] Watch for 48h: contacts DB disk growth, API latency, breaker trips, 429s from
      the contacts API.
- **Exit criteria:** sync age < 48h; website-only runs make zero paid calls; no dupes
  (email-keyed upserts only); disk growth within budget.

---

## 4. Data Mapping (website scraper → Contacts DB)

| Website scraper field | Contacts DB destination | Notes |
|---|---|---|
| `email_enrichment.domain` | `core.company.website_norm` + `website` | normalized identically to platform (`normalize_domain`) |
| `business_name` / `page_title` | `core.company.name` | prefer `business_name`; fallback `page_title` |
| `email` + `email_type`/`email_class`/`email_confidence` | `core.email` | unverified, `source_name='website_scrape'`, type mapping below |
| `metadata.email_contacts[] {e,n,t}` | `core.person` + `core.email` + `core.source_row` | named contact; title stored if present (display-only, no gating) |
| `metadata.phone` | `core.company.phone_e164` (company level) or person phone | E.164 normalization attempted; store raw if unparseable |
| gmaps `address/city/state/postal_code/country` | `core.company` location fields | |
| gmaps `rating`, `reviews_count`, `gmaps_types`, `google_maps_url` | company `custom_fields` JSON | no schema surgery; structured in JSON |
| `industry` / `industry_raw` | auto → `lead_universe` via existing classifier (`classify_industry`) | feeds Lead Universe filters |
| `http_status`, `worker_id`, `attempt_count`, `processing_time_ms`, queue machinery | **not imported** | operational, no lead value |

**Email type mapping:**

| scraper `email_type` | contacts `type` |
|---|---|
| `domain_named` | `work` |
| `domain_generic` | `work` + `is_generic_email=true` + `generic_reason` |
| `freemail` | `personal` (filtered out by curation) |
| `off_domain_visible` | `other`/null + excluded unless own-domain visible |

### Curation policy (import filter)

Import when ALL hold:

- status is `completed` (or `no_email` for company-only import — no email rows)
- `email_class IN ('own_domain', 'role_service')` — role_service means generic role
  addresses like info@; importable as generic work email, flagged generic
- `email_shared_nd <= 20` (default cap, env-tunable)
- email passes syntax validation + not in role-junk list (abuse@, postmaster@, etc.)
- domain passes `normalize_domain` (deep URLs stripped) + not in blocklist

**No-email domains:** imported as company records (phone/city/rating still valuable),
no email rows. Emails that fail curation are counted, not imported.

### Backfill volume estimate (re-verify from dry-run)

~1.2M curated email rows (own_domain ~255K + role_service ~432K + named contacts ~75K)
plus company upserts for up to ~2.4M domains (~1.33M net-new). At ~40 rows/s, email
pushes are ~5–7h; adding full company upserts could push the total toward ~17h
worst-case. Company upserts are cheaper and batchable — the dry-run report gives the
real number. Disk: ~+2–4 GB net on root (contacts DB 63 GB → ~66–67 GB; 50 GB free).

### Duplicate handling (both DBs have data for a domain)

| Situation | Behavior |
|---|---|
| Same email exists | Idempotent update (freshness), source tag preserved; never duplicate rows |
| New email for known person/company | Additional email on that person/company, tagged `website_scrape`, unverified |
| Genuinely new person | New person record, tagged `website_scrape` |
| Same person found by different sources | One person record; emails sit side by side, each with its own source + verified status |

Serving behavior: a domain lookup returns **all** contacts, source-labeled. Verified
emails rank above unverified scraped ones. The include/exclude toggle (existing
`source` param pattern) filters by source. New data adds; it never demotes or
overwrites provider data.

### Non-import rows

- All `no_email` domains → company-only records (firmographics only).
- `browser_queued` / `processing` / `pending` rows → skipped; the watermark stamps on
  terminal `completed_at` only, so in-flight rows naturally re-pull on a later sync
  once terminal. (281K browser_queued rows are future supply.)

---

## 5. Edge Cases & Constraints

1. **Generic-heavy data.** role_service (info@) is ~432K rows — imported as generic work
   emails, flagged `is_generic_email` — they serve "any email at this business" use
   cases, not decision-maker hunting. own_domain named emails (~255K, ~64% net-new) are
   the higher-quality layer.
2. **Unverified by default.** All imported emails marked unverified; verified ranking
   protects users. A mailtester verification pass is possible later without re-import
   (separate effort).
3. **Staleness honesty.** Data as of watermark; toggle shows "as of" date. A failed
   sync night = stale data + visible sync-age alert, not silent failure.
4. **Contacts DB saturation risk.** The 2026-08-06 incident: when the contacts cluster
   saturates, our circuit breaker floods. Mitigations: nightly quiet-hour run, 40 rows/s
   throttle, small batches, standalone service outside web workers.
5. **Access rules.** Sync connects to the scraping VPS **only** via SSH; the platform
   never holds a network Postgres connection to it, and never at user-request time.
   Contacts writes go through the sanctioned API, never direct SQL. Watermark + sync
   state in jobs.db (tiny).
6. **Freshness of browser_queued.** 281K rows are in browser_queued — future supply;
   pulled once terminal. Keep the browser queue running on the scraper side; it is the
   growth path for this dataset.
7. **No fresh scraping from platform.** UI copy and tooltips make clear: website-only
   mode reads existing data only; new scraping happens at
   webscraper.eagleinfoservice.com. The UI states what each option does and what output
   to expect.
8. **Domain normalization.** Shared `normalize_domain` from the platform, applied at
   import (scraper domain → website_norm) and lookup. A past bug (18 emails from 96K
   rows) was exactly a normalization mismatch — one shared function, tested.
9. **Event-loop safety.** All sync DB I/O in the standalone service (not web workers);
   website-only lookups reuse the existing thread-pooled contacts lookup path (same as
   the outscraper source filter — proven safe).
10. **Title gate: none.** Titles too sparse in this dataset; gating would make contacts
    unusable. Titles stored display-only.
11. **Quota/credits.** Website-only runs make zero paid calls — good for users ("free
    mode").
12. **Rollback.** Kill-switch off stops the sync. Imported rows are identifiable by the
    `website_scrape` source tag and removable with a one-line delete if ever needed;
    re-running the sync restores them. UI toggles ship dark (default off), enabled by
    decision.

---

## 5b. Security & Secrets

- Rotate the scraping VPS password (it appeared in this chat) — Phase 0.
- SSH key auth only for the sync service; password auth can be disabled for that user
  once keys work.
- Read-only DB user on the scraping VPS; never expose its Postgres to the internet.
- Never write direct SQL to the local contacts DB from this platform — sanctioned API
  only.
- Secrets in `backend/.env` only (existing convention); never committed. This doc
  stores no credentials.

---

## 5c. Storage & memory summary

| Item | Cost | Where | Verdict |
|---|---|---|---|
| Sync state table in jobs.db | ~few KB | root (jobs.db) | trivial |
| Contacts DB growth | +2–4 GB (est.) | root disk (63 GB → ~66–67 GB) | fine, watch item |
| jobs.db growth | ~0 | root | no new pressure |
| RAM in web workers | ~0 | workers capped 3 GB | no change |
| Sync service RAM | ~100–300 MB | standalone service | fine |

Root disk alert at 85% (currently 75%); contacts DB growth is the one to watch.

---

## 6. Verification & Testing

### Pre-backfill (dry-run)

- Dry-run prints: total rows considered, per-class keep/skip counts, sample rows.
- `--limit 1000` end-to-end trial run (pull → curate → push → watermark) before the
  full backfill.
- Curation + normalization unit tests green.

### Post-backfill verification queries (contacts DB)

```sql
-- 1. Website-scrape rows now present
SELECT COUNT(*) FROM core.email WHERE source_name='website_scrape';
SELECT COUNT(*) FROM core.source_row WHERE source_name='website_scrape';
```

```sql
-- 2. Idempotency proof: re-run the sync; rows_pulled>0 but zero new email rows
```

3. **Paid-call isolation (the core promise of the mode):** Flow 1 website-only run →
   `provider_call_log` shows 0 blitz/smartprospect/wizleads/better_enrich/getleads
   calls for that job_id.

### Unit tests

- `CurationPolicy` (all junk classes, shared_nd cap, syntax, blocklist).
- Domain normalization alignment (scraper domain → `normalize_domain` → website_norm).
- Watermark advancement (terminal-only stamping).
- Website-only lookup filter (source=website_scrape, cascade skipped).
- Idempotent upsert (same payload twice → INSERTED/UPDATED/SKIPPED, no dupes).
- UI (manual): toggle states, greyed-out providers, "as of" date render.

### E2E sanity

- Flow 1 website-only run on a known sampled domain (e.g. thrushlaw.com or wcchs.net):
  contacts returned are website_scrape-labeled; zero paid calls logged.

### Success metrics

- Sync runs nightly; sync age < 48h; rows pushed per night proportional to scraping
  throughput (monitor trend).
- Website-only runs: 0 paid calls. Include/exclude toggle behaves as specified.
- Contacts DB disk growth ≤ 5 GB; root disk stays < 85%.
- 429/breaker trips attributable to the sync ≈ 0 (nightly window, throttled).

---

## 7. Rollout Sequence (summary)

| # | Step | Est. |
|---|---|---|
| 1 | Phase 0: access (SSH key, read-only user, rotate password) | 0.5d |
| 2 | Phase 1 + 1b: sync module, state table, dry-run, backfill, timer (flag-off shipping) | 1–1.5d |
| 3 | Phase 2 + 2b: serving (website-only lookup plumbing, Find People source) | 1d |
| 4 | Phase 3: UI toggles + copy + as-of date | 1d |
| 5 | Phase 4: observability + docs + MCP oracle | 0.5d |
| 6 | Phase 5: go-live (enable flag, toggles, monitor) | 0.5d + overnight run |

**Estimated total: 4–5 working days** implementation + unattended overnight backfill
(may span two nights if company upsertes push the total past ~8h).

---

## 8. Open Items (non-blocking)

1. Exact field-fit against the contacts API's upsert field contract (e.g., does the
   company upsert accept `custom_fields`? If not, long-tail gmaps fields need a home) —
   verify during implementation against the contacts-api project's own conventions.
2. Whether `role_service` generic emails should be imported at all — default is
   import-as-generic; final call at dry-run review.
3. Mailtester verification pass on imported emails (future, separate effort).
4. Contacts API rate-limit headroom during backfill (75 RPS shared; sync uses
   ~40 rows/s; 429s handled by outbox-style retry — reuse the pattern, verify exact
   reuse vs reimplement).
5. Whether Find People needs a separate "website scrape" facet beyond the source filter
   (subsumed by source filter; decide at UI time).
6. Periodic review of the shared_nd cap (default 20) against junk rates.

---

## Appendix A — Measured source DB stats (2026-08-26, read-only via SSH)

- status: completed 1,151,177 / no_email 1,006,461 / browser_queued 281,835 /
  processing 111
- email_class (email non-null): role_service 431,718 / freemail 325,562 / own_domain
  255,057 / off_domain 144,887 / vendor_signature 58,168 / role_technical 3,828 /
  (blank) 3,139 / pubsec_mismatch 2,833 / vendor_platform 861 / artifact 507 /
  placeholder 181
- gmaps_places: 2,446,671 rows
- Domains: 2,439,584 distinct (email_enrichment)
- Overlap vs contacts-db: 1,106,046 shared domains (45.3% of ws); net-new 1,333,538
- own_domain distinct emails: 254,524; 20K-sample overlap vs contacts-db emails: 7,213
  (~36%) → est. net-new ~163K

Figures drift as scraping continues — re-verify at implementation time.
