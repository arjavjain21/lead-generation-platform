# Loop: Fix Provider Cascade Defects

Status: running

## Goal contract

**OUTCOME:** Provider cascade defects from `PROVIDER_CASCADE_AUDIT.md` are fixed so the enrichment system:
- Uses Contacts DB → Blitz → WizLeads → BetterEnrich in direct API enhanced mode.
- Does not stop on unverified Blitz emails (continues to WizLeads/BetterEnrich).
- Tracks WizLeads usage in `jobs.db` (new `emails_wizleads` column) and frontend job stats.
- Updates `used_providers` in CSV/pipeline jobs.
- Preserves BetterEnrich 201 polling and verified-Blitz stop behavior.
- Preserves `force_provider` behavior.
- Normalizes `wizleads`/`wizleads_email` source labels to canonical `wizleads`.

**SUCCESS CRITERIA (binary, in priority order):**

1. **Direct API enhanced mode uses Contacts DB → Blitz → WizLeads → BetterEnrich** — `routes.py` `unified_enrich_logic` `enhanced` path calls WizLeads between Blitz and BetterEnrich when input is eligible (full_name/first_name + domain).
2. **Unverified Blitz emails do not stop the cascade** — In `pipeline.py:1326-1330`, `routes.py:909-930`, `routes.py:1188-1232`, if Blitz returns emails[] without `verified_email` and no other acceptance condition is met, the cascade continues to next provider.
3. **`emails_wizleads` column exists and is populated** — `jobs.emails_wizleads INTEGER DEFAULT 0`; aggregation includes `source IN ('wizleads','wizleads_email')`; frontend renders the value.
4. **CSV/pipeline jobs populate `used_providers`** — `route_enrichment` (or its caller) writes attempted providers to `jobs.used_providers` after the cascade.
5. **Verified Blitz email still stops the cascade** — If `verified_email` is present, the cascade returns without calling WizLeads or BetterEnrich.
6. **BetterEnrich 201 polling still works** — The polling code path is unchanged or improved without breaking 201→polled-completion behavior.
7. **force_provider behavior preserved** — `force_provider=blitz` calls only Blitz; `force_provider=contacts_db` skips paid providers; `force_provider=wizleads` calls only WizLeads when eligible.
8. **Source label normalization** — `wizleads` and `wizleads_email` aggregated under canonical `wizleads`; historical rows still display correctly.
9. **Tests pass** — All new controlled tests in `backend/enrichment/tests/test_cascade_fixes.py` pass when run with mocks (no production provider credits used).

**VERIFICATION:**
- `cd /var/www/lead-generation-platform/backend && python -m pytest enrichment/tests/test_cascade_fixes.py -v` (new test file).
- Read affected files at specific line ranges to confirm code change.
- Inspect `jobs.db` schema: `.schema jobs` should show `emails_wizleads`.
- `python -c "import sqlite3; conn=sqlite3.connect('backend/data/jobs.db'); ..."` queries to confirm column.

**REFERENCE MATERIALS:**
- `/var/www/lead-generation-platform/PROVIDER_CASCADE_AUDIT.md` — defect list with file:line evidence.
- `/var/www/lead-generation-platform/backend/enrichment/pipeline.py` — `_resolve_person_email`, `route_enrichment`.
- `/var/www/lead-generation-platform/backend/enrichment/routes.py` — `unified_enrich_logic`, enhanced path.
- `/var/www/lead-generation-platform/backend/enrichment/wizleads_client.py` — `find_email`.
- `/var/www/lead-generation-platform/backend/enrichment/better_enrich_client.py` — `find_work_email_v3`, `_poll_v3_result`.
- `/var/www/lead-generation-platform/backend/enrichment/stats_store.py` — `record_stats`, `SOURCE_GROUPS`.
- `/var/www/lead-generation-platform/backend/main.py` — `jobs` table schema.
- `/var/www/lead-generation-platform/frontend/index.html` — line 2633 for `emails_wizleads` display.

**CONSTRAINTS:**
- Do NOT spend production provider credits — use mocks, monkey-patch, or stub providers in tests.
- Do NOT rewrite the whole enrichment system — minimal targeted changes.
- Preserve `force_provider` semantics.
- Do NOT break BetterEnrich 201 polling.
- All changes must be safe/idempotent (DB migration must not error if column already exists).

**ITERATION BUDGET:** 8

**AUTHORIZED ACTIONS:**
- Modify `pipeline.py`, `routes.py`, `stats_store.py`, `job_store.py`, `main.py`, `frontend/index.html` to fix the defects.
- Add new test file `backend/enrichment/tests/test_cascade_fixes.py`.
- Run the test suite.
- Read and query `jobs.db` (read-only).
- Run the DB migration.

**OUT OF SCOPE:**
- Cross-process rate limiting (acknowledged in audit §8 gap 7).
- New provider integrations.
- Performance work.
- Per-row `provider_attempts` in `jobs` table (out of scope; lives in sidecar).

## Best version
HEAD (master @ 2026-06-15) | iteration 2 | criteria passing: 9/9

## Iterations
| # | change attempted | verification result | decision |
|---|------------------|---------------------|---------|
| 1 | Apply all 6 cascade defects (WizLeads in route, unverified Blitz fix in pipeline._run_route_step ROUTE_METHOD_PERSON_ENRICH, record_provider_use plumbing, emails_wizleads migration, source normalization, used_providers in routes._run_job) | New test_cascade_fixes.py: 9/9 pass. Full test suite: 100/100 pass. jobs.db schema confirms emails_wizleads column. | DONE. |
| 2 | Wire WizLeads + BetterEnrich into GET /api/enrichment/enrich/ domain_only branch. Refactor find_email_for_person to call pipeline.route_enrichment + run_enrichment_route, achieving full Contacts DB -> Blitz -> WizLeads -> BetterEnrich cascade. Add routing block to response. | New test_domain_only_api_cascade.py: 4/4 pass (all-4-providers, wizleads short-circuit, force_provider=contacts_db, per-person wizleads force_provider). Cascade fix tests: 9/9 pass. Provider audit + source tracking tests: 35/35 pass. Total iter 2 scope: 48/48 pass. | DONE. |

## Blockers
None.

## Next action
Iter 2 is committed (commit 471964c) and ready for deploy. Verify on live endpoint with Clay's API key: GET /api/enrichment/enrich/?domain=example.com&first_name=Jane&last_name=Doe should show providers_called = [contacts_db, blitz, wizleads, better_enrich].
