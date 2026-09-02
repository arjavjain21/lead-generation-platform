# Contacts DB Search-Performance Plan — 2026-08-31

**Scope:** speed and reliability of user-facing search, without regressions and without data
loss. **Verdict from the architectural assessment (2026-08-31): Typesense is NOT being
implemented.** The work below optimizes PostgreSQL in place, in the contacts-api project.

**Why (one paragraph).** User-facing search on listbuilding.eagleinfoservice.com is a
pass-through: `POST /api/enrichment/search/employees` → leadsdatabase.cc
`/v1/people/search`, implemented in `/opt/contacts_api/app/search_endpoints.py` as
`ILIKE '%term%'` + `~*` regex over core.person / core.company / core.decision_makers.
Measured 1.2–5.9 s (contacts-api's own `OPTIMIZATION_PLAN_2026-08-5-23.md`). Meanwhile
~3.5 GB of trigram indexes exist and are nearly unused (`idx_person_headline_trgm`
1,814 MB / 25 scans; `person_full_name_trgm_idx` 996 MB / 55 scans;
`idx_company_name_trgm` 0 scans on Aug-23 doc, 7 today — stats reset is young). The
planner isn't picking them up; the fix is diagnosis + ANALYZE + surgical rewrites — not a
new engine. All work is in **contacts-api** (its repo, its DB on 5432, its service). The
lead-gen repo gets **zero code changes**.

---

## Ground truth verified live (2026-08-31, read-only)

| Fact | Value | Source |
|---|---|---|
| MV refresh query | running 99.8% CPU during assessment (PID 1431555, lock_timeout 10s, statement_timeout 600s) | pg_stat_activity |
| pg_trgm extension | installed | pg_extension |
| Git state of /opt/contacts_api | branch `fix/company-website-norm-unique`, **ahead 17, uncommitted WIP in migrations/017** | git status |
| Contacts-api service | uvicorn 4 workers, port 8080, `contacts-api.service` | start.sh + skill |
| DB roles/limits | `api_app` role, statement_timeout 5s, idle_in_transaction 5s; PG16, shared_buffers 4 GB | OPTIMIZATION_PLAN |
| Timers on the box touching contacts DB | contacts-db-idle-reaper (*/5), outscraper-contacts-sync (hourly-ish), contacts-api-cleanup (daily 00:00), contacts-dashboard-stats (daily 01:33) | systemctl list-timers |

---

## Non-negotiable safety rules (bind every subagent to these)

1. **Lead-gen platform is OFF-LIMITS for changes.** It only benefits passively (fewer
   contacts_db circuit-breaker trips, faster Find People). Never touch jobs.db, never
   restart its service, never modify nginx for this work.
2. **contacts-api git discipline:** work on a NEW branch off current prod state; the
   uncommitted WIP in migrations/017 is not ours to discard — snapshot, never clobber.
3. **Index DDL:** only `CREATE INDEX CONCURRENTLY` / `DROP INDEX CONCURRENTLY` /
   `REINDEX CONCURRENTLY`. Never plain CREATE/DROP on core tables. A failed concurrent
   build leaves an INVALID index — detect and DROP it before retrying.
4. **Disk guard:** / is at 77% (47 GB free). Abort any phase if free space < 25 GB.
5. **No semantics change:** never replace `ILIKE` with `%`/`similarity()` operators (same
   matches, different ranking semantics); never change response JSON shape. Parity is
   proven by golden-corpus checksums (P0.1) or the change doesn't ship.
6. **Windows:** no index build/reindex while the MV refresh is running, during the */5
   idle-reaper, during the lead-gen nightly website-scrape sync window, or while any
   long transaction is open. Check `pg_stat_activity` before and during.
7. rules#7 (no .env secrets), contacts-api skill rules (contactsapi user, backups before
   changes, UUID `::text` casting pattern, normalized columns) all apply.
8. **Every phase ends with the golden corpus re-run.** No phase is "done" on intention —
   only on measured, checksum-verified results.

---

## Phases → tasks (IDs are the TaskList entries)

### P0 — Baseline & safety net (read-only + prep)
- **#1 Golden baseline corpus.** ~12 queries spanning people/companies/DM search with
  facets, title keywords (broad + narrow), name_contains, domain. Capture EXPLAIN
  (ANALYZE, BUFFERS) + 3-run wall latency + ordered-PK result checksums. This is THE
  regression oracle. Output: docs/baseline_20260831/.
- **#2 DDL snapshot + git hygiene + calendar.** pg_dump schema-only (recreate-DDL for
  anything we drop), git snapshot of WIP + rollback tag, MV-refresh/timer window map,
  disk-guard threshold formalized.

### P1 — Diagnosis before treatment (read-only)
- **#3 Index-vs-predicate diagnosis.** For each trigram index: exact indexdef, operator
  class, expression match to predicate, and per-query EXPLAIN verdict on WHY the planner
  skips it (stale stats vs GIN recheck cost vs <3-char terms like VP/IT/HR which trigram
  cannot serve). Verdict taxonomy: "ANALYZE fixes", "planner is right, seq scan cheaper",
  "needs different structure". Also: supersession map for 0-scan indexes.
- **#4 pg_stat_statements targeting.** Top-20 by total/mean exec time, intersected with
  endpoint shapes — ensures we optimize queries real traffic runs, not hypothetical ones.

### P2 — Database operations (online, gated on P1 verdicts)
- **#5 ANALYZE core tables.** Cheapest possible win attempt; re-run corpus; if plans
  flip, later phases shrink.
- **#6 Build justified indexes CONCURRENTLY.** One per window. Leading candidate:
  `decision_makers.job_title` trigram (no index backs its ILIKE; DM search 2.8–5.9 s;
  618K rows → minutes to build). Optional person facet-covering index if query-mix
  justifies.
- **#7 Drop confirmed-dead indexes.** Only after ≥7-day observation window of
  pg_stat_user_indexes (young stats counter). company_name_idx (459MB/0),
  idx_person_state (271MB/0), idx_website twin if key-analysis clears it. Recreate DDL
  pre-saved. Expected reclaim ~1–1.5 GB.
- **#8 REINDEX CONCURRENTLY worst bloat.** person heap 8.5 GB vs total 18 GB. One per
  window, off-peak, disk-guard armed, done AFTER #7 frees space.

### P3 — Code changes (contacts-api repo, new branch)
- **#9 website suffix-LIKE → website_norm equality.** `LOWER(c.website) LIKE '%domain'`
  → `c.website_norm = $n` (normalized). Proven-hot indexed column (14M+ scans). Parity
  gate: 50-domain sample, old-vs-new result sets must match exactly.
- **#10 COUNT-skip policy + stable pagination tiebreak.** Extend the existing
  total-elision policy to the next-slowest shapes; add stable tiebreak to ORDER BY
  (their migrations/011 documents the non-deterministic-tie hazard). API shape unchanged.
- **#11 Latency middleware + parity test gate.** X-Process-Time header + slow-query
  journald logging (WARN >500ms, rate-capped). Then the gate: full pytest green + golden
  corpus checksums identical + JSON contract unchanged.

### P4 — Deploy & verify
- **#12 Staged deploy + 24h watch.** Backups per skill convention → commit/push/tag →
  restart contacts-api → health + corpus → 24h watch (journald, pg_stat_statements
  deltas, lead-gen breaker-trip counts). Rollback <5 min (tag checkout + backup restore;
  index ops roll back independently via DROP / saved DDL).

### P5 — Close-out
- **#13 Outcome doc + ADR + docs.** Before/after latency table, built/dropped/reindexed
  inventory, planner verdicts, Typesense-rejected ADR cross-ref, residual backlog
  (short-acronym search, title-token column, keyset pagination).

---

## Dependency graph

```
#1 baseline ─┬─> #3 diagnosis ─┬─> #5 ANALYZE ──> re-baseline check
             │                 ├─> #6 build idx  (needs #3 verdicts + #2 windows)
             │                 └─> #7 drop idx   (needs #3 map + 7-day obs)
#2 safety net┴─────────────────┴─> #8 reindex     (needs #7 first, disk guard)
#3 + #4 ──> #9/#10/#11 code (parity gates) ──> #12 deploy ──> #13 docs
```

---

## Success criteria

| Metric | Baseline | Target |
|---|---|---|
| People search (title broad) | multi-second / 5s-timeout risk | < 500 ms p95 |
| Companies search by name | 1.2–1.4 s | < 200 ms |
| DM search | 2.8–5.9 s | < 500 ms |
| Golden-corpus checksums | — | identical (unless documented tie-reorder) |
| Data loss | — | zero (indexes are derivable; no table rewrites) |
| Lead-gen repo changes | — | zero |
| Disk floor | 47 GB free | never < 25 GB at any phase boundary |
```
