# Loop: Strongest-identifier-first enrichment routing

Status: done

## Goal contract

OUTCOME: Both the direct enrichment API (`/api/enrichment/enrich`) and the CSV-driven job pipeline (`run_pipeline` → `_enrich_domain`) use one routing function that picks providers by strongest available identifier. When `linkedin_url` is present, LinkedIn-based providers are tried before name/domain providers. When only `phone` is present, the configured phone reverse lookup runs and any returned LinkedIn immediately cascades into LinkedIn-first email enrichment. `force_provider` strictly constrains which provider family is allowed and returns a clear `no_email_reason` when its inputs are insufficient.

SUCCESS CRITERIA (binary, in priority order):

1. **A single routing function `route_enrichment(...)` exists in `pipeline.py`** and is the only function that decides provider order for both `_unified_enrich_logic` and `run_pipeline.process_row`. (No copy-pasted cascade logic.)
2. **LinkedIn-first cascade** — given any input with a valid `linkedin_url`, the routing function calls (in order) Contacts DB by LinkedIn, Blitz `person_enrich_by_linkedin`, Blitz `find_work_email` from LinkedIn, then name/domain fallbacks. The first email it returns MUST come from a LinkedIn-based provider call; provider attempts are recorded in `provider_attempts`.
3. **LinkedIn-only input works** — `linkedin_url` alone (no domain, no name) MUST return a valid enrichment attempt path. No provider is called with insufficient input.
4. **Phone → LinkedIn → email cascade** — given a row with `phone` but no `linkedin_url` and no email, the routing function runs the configured phone reverse lookup; if it returns a LinkedIn URL, the LinkedIn-first cascade runs immediately after.
5. **CSV row path uses the same router** — a CSV row with `linkedin_url` reaches `route_enrichment` and triggers LinkedIn-first attempts. A test feeds a fake row and asserts the order of provider calls.
6. **force_provider=blitz calls only Blitz paths** — given a row with linkedin_url and force_provider=blitz, the routing function calls `blitz.person_enrich_by_linkedin` and `blitz.find_work_email` (and a name/domain blitz call only if name+domain present); Contacts DB and BetterEnrich and WizLeads are NOT called. Returns `no_email_reason` if Blitz cannot use the input.
7. **force_provider=contacts_db skips paid providers** — Contacts DB is called; no Blitz/BetterEnrich/WizLeads call happens. Returns `no_email_reason` if Contacts DB cannot use the input.
8. **Malformed LinkedIn returns `no_email_reason=linkedin_parse_failed`** — given a string that is not a LinkedIn URL, the routing function returns a result with `no_email_reason="linkedin_parse_failed"` and does not call any provider.
9. **Provider capability gates** — providers are never called with insufficient input. e.g. WizLeads requires first_name + last_name + domain; BetterEnrich `find_work_email_v3` requires full_name + domain; Blitz `person_enrich_by_linkedin` requires a valid LinkedIn URL.
10. **Source-path tracking** — every successful email is tagged with a `source_path` string that records the identifier→provider chain (e.g. `"linkedin -> contacts_db"`, `"phone -> blitz_reverse -> linkedin -> blitz_email"`, `"name_domain -> wizleads"`).

VERIFICATION:

- `cd /var/www/lead-generation-platform/backend && pytest enrichment/tests/test_routing.py -v` — a new pytest file with at least 8 tests that mock all four provider clients and assert (a) provider-call order for linkedin/url, (b) linkedin-only path, (c) csv-row path, (d) phone→linkedin→email path, (e) force_provider=blitz restricts to blitz, (f) force_provider=contacts_db skips paid, (g) malformed linkedin returns `no_email_reason="linkedin_parse_failed"`, (h) source_path on success.
- `cd /var/www/lead-generation-platform/backend && pytest enrichment/tests -v` — full enrichment test suite must remain green (no regressions in `test_identifier_propagation.py`, `test_source_tracking_integration.py`, `test_stats_store.py`).
- A mechanical check script `backend/scripts/check_routing.py` that prints the provider call order for 5 representative input shapes (linkedin+name+domain, linkedin-only, phone-only, name+domain, domain-only) and exits 0 when each input shape produces the documented order.

REFERENCE MATERIALS:

- `backend/enrichment/pipeline.py` (current `_resolve_email_for_person`, `_enrich_domain`, `run_pipeline`).
- `backend/enrichment/routes.py` (`_unified_enrich_logic`, `UnifiedEnrichRequest`, `_should_skip_provider`).
- `backend/enrichment/identifier_utils.py` (already provides `normalize_linkedin_url`, `linkedin_username_from_url`, `build_row_identifier_payload`).
- `backend/enrichment/blitz_client.py` (existing methods: `person_enrich_by_linkedin`, `find_work_email`, `person_enrich`, `domain_to_linkedin`).
- `backend/enrichment/contacts_client.py` (existing methods: `person_by_linkedin`, `person_by_name_and_domain`, `company_by_domain`, `company_contacts_enriched`).
- `backend/enrichment/wizleads_client.py` (`find_email` requires first/last/website).
- `backend/enrichment/better_enrich_client.py` (`find_work_email_v3` requires full_name + company_domain + linkedin_url; `find_company_email` requires website).
- `backend/enrichment/providers.py` (`is_provider_enabled`).
- `backend/enrichment/tests/test_source_tracking_integration.py` (assertion patterns for source tracking).
- `backend/enrichment/tests/test_identifier_propagation.py` (column-mapping helpers).

CONSTRAINTS:

- No external API calls. All tests mock provider clients.
- No new required fields on existing request schemas; the new fields stay optional.
- Existing domain-only and domain+name jobs must still complete.
- `force_provider` semantics: when set, the routing function must never call a provider family outside the forced one. When the forced provider cannot use the input, return `no_email_reason` instead of falling through.
- The phone→LinkedIn step is allowed to be a no-op stub if no configured reverse-lookup client exists, as long as the routing function calls it (or returns a clear `no_email_reason=phone_reverse_unavailable`) and that path is testable.
- Identifier propagation utilities from the previous loop (`identifier_utils.py`) must be reused — do not duplicate normalization logic.

ITERATION BUDGET: 8

AUTHORIZED ACTIONS:

- Edit `backend/enrichment/pipeline.py`, `backend/enrichment/routes.py`, `backend/enrichment/identifier_utils.py`.
- Create `backend/enrichment/tests/test_routing.py`.
- Create `backend/scripts/check_routing.py`.
- Run `pytest` against the enrichment test suite.
- Mock provider clients in tests (no live API calls).

OUT OF SCOPE:

- Adding new provider integrations.
- UI changes.
- Database schema changes.
- Blitz endpoint behavior changes beyond using the new routing function.
- Phone reverse-lookup client implementation (route stub is acceptable).

## Best version
backend/enrichment/pipeline.py (route_enrichment, run_enrichment_route, _build_source_path) + run_pipeline wiring + routes.py _unified_enrich_logic wiring + test_routing.py (25 tests) + check_routing.py | iteration 2 | criteria passing: 10/10

## Iterations
| # | change attempted | verification result | decision |
|---|------------------|---------------------|----------|
| 1 | Added `route_enrichment` (pure router), `run_enrichment_route` (executor), `_build_source_path` (path string builder), `_provider_label`, `_can_provider_use_method`, `_method_is_paid`/`_method_is_free`. Added `source_path`, `provider_attempts`, `no_email_reason` to ENRICHED_COLUMNS. Wired `process_row` in `run_pipeline` to call route_enrichment for any row with linkedin_url/phone/name+domain. Fixed Contacts DB company lookup in `_enrich_domain` to respect `force_provider`. Added `force_provider` parameter to `run_pipeline`. Wrote `test_routing.py` with 21 tests covering criteria 1-10. Wrote `check_routing.py` script. | All 21 new tests pass; 65/65 enrichment tests pass (44 existing + 21 new). Mechanical check script confirms 5 input shapes route correctly and force_provider restricts provider families. | PARTIAL — criterion 1 not yet met (routes.py not wired) |
| 2 | Refactored `_unified_enrich_logic` in routes.py: replaced the copy-pasted `linkedin_only` (lines 1425-1536) and `enhanced` (lines 1538-1741) cascade bodies with calls to `pipeline.route_enrichment()` and `pipeline.run_enrichment_route()`. The `domain_only` mode is unchanged (it uses the decision-maker waterfall, a fundamentally different code path). The response now includes a `routing` block with `mode`, `source_path`, `provider_attempts`, and `no_email_reason` for downstream visibility. The legacy behaviour of looking up company LinkedIn via Contacts DB in enhanced mode and best-effort name population from Contacts DB is preserved. Added 4 wiring tests in `TestUnifiedEnrichUsesRouter` (sync `fake_route_enrichment` because routes.py calls it without await, async `fake_run_enrichment_route`) asserting (a) router is called for linkedin_only, (b) router is called for enhanced, (c) force_provider is passed through, (d) response includes routing diagnostics and email_source is mapped from router result. | All 25 routing tests pass; 69/69 enrichment tests pass. Mechanical check script still passes. Direct API now uses the same router as the CSV pipeline. | DONE — all 10 success criteria met |

## Blockers
None.

## Investigation findings

- `pipeline._enrich_domain` is the per-domain entry. It currently takes only `domain` and `full_name` and never reads `linkedin_url`/`phone`/`company_name` from the input row, even though `identifier_utils.build_row_identifier_payload` extracts them.
- `pipeline._resolve_email_for_person` is called per decision maker. Its cascade order is: Contacts DB name+domain → Contacts DB LinkedIn → Blitz name+domain → Blitz LinkedIn → WizLeads → BetterEnrich → Contacts DB input name. There is no `phone` parameter.
- `routes._unified_enrich_logic` has its own duplicate routing block for the linkedin+name+domain case. The "primary path" branch (line 1437+) handles `linkedin_url` directly with a contacts_db → blitz cascade. But the CSV path (`run_pipeline`) does NOT go through `_unified_enrich_logic` — it goes through `_enrich_domain` which ignores the input linkedin_url.
- `force_provider` semantics are inconsistent: `_enrich_domain`'s Contacts DB company lookup at line 544 and decision-maker lookup at line 603 do not check `_should_skip_provider`. So `force_provider=blitz` still hits Contacts DB before Blitz, returning stale emails and marking them invalid (this is the actual reported failure mode).
- `blitz_client` has the right methods already: `person_enrich_by_linkedin(client, linkedin_url)` (line 499), `find_work_email(client, linkedin_url)` (line 200), `person_enrich(client, full_name, domain, include_phone)` (line 419), `domain_to_linkedin(client, domain)` (line 141), `waterfall_icp_search(client, company_linkedin_url, cascade, max_results)` (line 175).
- `contacts_client` has: `person_by_linkedin(client, linkedin_url)` (line 237), `person_by_name_and_domain(client, full_name, domain)` (line 253), `company_by_domain(client, domain)` (line 269), `company_contacts_enriched(client, domain, limit)` (line 296).
- `wizleads_client.find_email` (line 116) requires `first_name` + `last_name` + `website`.
- `better_enrich_client.find_work_email_v3` (line 271) requires `full_name` + `company_domain` + optional `linkedin_url`.
- `providers.is_provider_enabled(name)` is the global on/off gate.
- `routes._should_skip_provider` (line 109) already implements: skip if globally disabled OR force_provider set and provider doesn't match.
- `pipeline._should_skip_provider` (line 45) is a duplicate of the routes one. The pipeline one is used internally; the routes one is used in unified_enrich.
- `identifier_utils.normalize_linkedin_url` returns "" for non-LinkedIn input; the new router can rely on that to detect malformed input.

## Next action
Begin iteration 1: build `route_enrichment(...)` in `pipeline.py` plus a thin `enrich_row(...)` orchestrator that picks the cascade based on input identifiers. Wire it into `run_pipeline.process_row`. Use only mocks for verification.
