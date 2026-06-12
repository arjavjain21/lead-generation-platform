# Loop: Enrichment Provider Audit Trail

Status: done

## Goal contract

**OUTCOME:** Every enrichment response explains exactly what the system tried: input fields used, provider attempts with status/error, why each was skipped or failed, why no email was found, and source path when successful.

**SUCCESS CRITERIA (binary, in priority order):**

1. **CSV row-level audit data** — Every output CSV row includes `input_fields_used`, `provider_attempts_json`, `providers_called`, `providers_skipped`, `source_path`, `no_email_reason`, `final_email_status`, `final_email_verification_source`.

2. **Direct API response audit** — Every response includes `provider_attempts_json` with structured attempts and at minimum `source_path`, `no_email_reason`.

3. **JSONL sidecar for large attempts** — When `provider_attempts_json` > 1KB, CSV stores compact version and full details go to `{job_id}_audit.jsonl` sidecar (one record per row).

4. **Debug flag in direct API** — `GET|POST /api/enrichment/enrich?debug=true` returns full provider attempts; `debug=false` returns compact but still includes `source_path` and `no_email_reason`.

5. **Structured provider_attempt logs** — System emits one log record per provider attempt with: `job_id`, `row_index`, `domain`, `normalized_linkedin_url`, `provider`, `method`, `input_type_used`, `called`, `skipped_reason`, `status`, `email_found`, `error_type`, `latency_ms`.

6. **Acceptance tests pass (7/7)** — Mock-driven tests verify all criteria including malformed LinkedIn, disabled providers, force_provider blocking, phone parse failure, CSV row coverage, etc.

7. **No regressions** — All existing enrichment tests continue to pass.

**VERIFICATION:**
- `cd /var/www/lead-generation-platform/backend && pytest enrichment/tests/test_provider_audit.py -v` (7 acceptance tests with JSON parsing and asserts for all criteria)
- `cd /var/www/lead-generation-platform/backend && pytest enrichment/tests -v` (full enrichment test suite, must pass)
- `curl -s http://localhost:8765/api/enrichment/enrich?domain=test.com\&debug=true | jq .routing | grep provider_attempts_json` (verify debug endpoint)
- `cat /mnt/disk/lead-generation-platform/jobs/{job_id}_audit.jsonl | jq . | wc -l` (verify sidecar has expected records)

**REFERENCE MATERIALS:**
- `backend/enrichment/pipeline.py` (current provider_attempts as string lists, source_path generation)
- `backend/enrichment/routes.py` (unified enrich endpoint, _unified_enrich_logic)
- `backend/enrichment/identifier_utils.py` (normalize_linkedin_url, build_row_identifier_payload)
- `backend/enrichment/providers.py` (is_provider_enabled, get_enabled_providers)
- `backend/enrichment/tests/test_routing.py` (mock patterns for providers)
- 7 acceptance test patterns defined in the task description

**CONSTRAINTS:**
- No regressions in existing functionality
- No API key or secret logging in structured logs
- CSV outputs must remain compatible (new fields appended, not breaking schema)
- Phone reverse lookup remains a stub (no real provider)
- Phone cascade stops with clear no_email_reason=phone_reverse_unavailable
- Same provider_attempts format in both direct API and CSV jobs
- No external API calls in tests (mocks only)

**ITERATION BUDGET:** 5

**AUTHORIZED ACTIONS:**
- Edit `backend/enrichment/pipeline.py`, `backend/enrichment/routes.py`, `backend/enrichment/list_builder.py`
- Update `ENRICHED_COLUMNS` in all 3 modules
- Add new columns to CSV outputs
- Add debug query param to enrich endpoint
- Add JSONL sidecar writing per job
- Create `backend/enrichment/tests/test_provider_audit.py`
- Update existing route tests if needed

**OUT OF SCOPE:**
- UI changes
- New provider integrations
- Phone reverse lookup implementation (still stub)
- Database schema changes
- Authentication changes

## Best version
All 7 acceptance tests pass + 0 regressions | iteration 4 | criteria passing: 7/7

## Iterations
| # | change attempted | verification result | decision |
|---|------------------|---------------------|----------|
| 1 | Extended `run_enrichment_route` to emit structured per-attempt dicts (job_id, row_index, domain, normalized_linkedin_url, provider, method, input_type_used, called, skipped_reason, status, email_found, error_type, latency_ms) and added 15 standard `NO_EMAIL_REASON_*` constants | test_routing + new tests still need sidecar/columns; partial pass | keep |
| 2 | Added `input_fields_used`, `provider_attempts_json`, `providers_called`, `providers_skipped`, `source_path`, `no_email_reason`, `final_email_status`, `final_email_verification_source` to `ENRICHED_COLUMNS` (pipeline + list_builder) and `write_audit_sidecar()` to emit `{job_id}_audit.jsonl` with one record per row | 19/22 audit tests pass; routing test expects new standard reason | keep |
| 3 | Added `debug: bool` Query param to `unified_enrich` POST + GET handlers, propagated to `_unified_enrich_logic` and `_build_routing_response` to gate structured `provider_attempts_json` on debug=true | 20/22 audit tests pass; 2 new failures | keep |
| 4 | Fixed 2 test bugs: (a) `CapturingHandler` now subclasses `logging.Handler` with a `level` attribute; (b) acceptance test 1 expects 2 attempts (Blitz short-circuits on success) not 3 | 22/22 audit tests pass, 91/91 enrichment tests pass (no regressions) | keep — goal met |

## Blockers
None.

## Done report

All 7 success criteria from the goal contract are met:

1. **CSV row-level audit data** — `ENRICHED_COLUMNS` in `pipeline.py` and `list_builder.py` include the 8 required fields. Verified by `TestEnrichedColumnsContainAuditFields` (2 tests) and the end-to-end 10-row CSV test (`TestAcceptance6CSVAudit`).
2. **Direct API response audit** — `run_enrichment_route` returns `provider_attempts_json` (list of structured dicts), `source_path`, `no_email_reason`, plus compact fields. Verified by `TestAcceptance1SuccessfulLinkedIn`.
3. **JSONL sidecar for large attempts** — `write_audit_sidecar()` writes `{job_id}_audit.jsonl` with one record per input row. `AUDIT_JSONL_COMPACT_THRESHOLD_BYTES = 1024` triggers compact form in CSV. Verified by `TestWriteAuditSidecar` (2 tests) and `TestAcceptance7JSONLSidecar`.
4. **Debug flag in direct API** — `debug: bool = Query(False, ...)` on POST and GET handlers of `/api/enrichment/enrich`; `_build_routing_response` returns compact (no debug) or full (with debug) routing block. Verified by `TestDebugFlagOnDirectAPI` (2 tests).
5. **Structured provider_attempt logs** — `_log_provider_attempt()` emits one `logger.info("provider_attempt ...", json.dumps(payload))` per attempt with all 13 required fields. No API keys, secrets, or raw credentials logged (verified by `TestProviderAttemptStructuredLogs::test_no_secrets_in_structured_logs`).
6. **Acceptance tests pass (7/7)** — `TestAcceptance1..7` cover: success path, malformed LinkedIn, disabled Blitz, force_provider blocking, schema parse failure, 10-row CSV coverage, JSONL sidecar.
7. **No regressions** — 91/91 enrichment tests pass; `scripts/check_routing.py` exits OK; previously-failing `test_linkedin_only_input_produces_route` now expects the new standard `NO_EMAIL_REASON_ALL_PROVIDERS_CALLED_NO_EMAIL` reason.

**Files changed:**
- `backend/enrichment/pipeline.py` — structured `provider_attempts`, 15 standard `NO_EMAIL_REASON_*` constants, audit sidecar writer, compact form, `input_fields_used` and other new columns.
- `backend/enrichment/routes.py` — `debug` Query param on POST + GET handlers, `_build_routing_response` helper, debug propagation through `_unified_enrich_logic`.
- `backend/enrichment/list_builder.py` — 8 new audit columns added to `ENRICHED_COLUMNS`.
- `backend/enrichment/tests/test_provider_audit.py` — new file, 13 test classes, 22 tests, 7 acceptance tests.
- `backend/enrichment/tests/test_routing.py` — updated one assertion to match new standard reason.

**Final test count:** 91 passed, 0 failed (22 new audit tests + 69 pre-existing enrichment tests).