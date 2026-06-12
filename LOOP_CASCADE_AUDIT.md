# Loop: Provider Cascade Audit (Blitz → WizLeads → BetterEnrich)

Status: running

## Goal contract

**OUTCOME:** A provider cascade audit report that proves whether the enrichment pipeline correctly continues to WizLeads and BetterEnrich when Blitz does not return an acceptable final email.

**EXPECTED CASCADE:** Contacts DB → Blitz → WizLeads → BetterEnrich (with stop-on-acceptable, continue-on-failure).

**SUCCESS CRITERIA (binary, in priority order):**

1. **Cascade is reachable in code** — `pipeline.py` (or equivalent orchestrator) calls WizLeads and BetterEnrich as cascade steps after Blitz for the relevant routes.

2. **Stop conditions correct** — Cascade stops ONLY on acceptable final email (defined below). Continues on: no email returned, invalid email, unknown verification when verified-required, timeout, 5xx, schema parse failure, rate-limit exhaustion, partial person/LinkedIn data without email.

3. **BetterEnrich 201 polling implemented** — `better_enrich_client.py` posts and polls with `GET /api/v1/find-work-email-low-cost-v3?id=<id>` until terminal status, with timeout.

4. **WizLeads inputs correct** — Called with `first_name` or `full_name` + `website`/`company_domain`; auth via `x-api-key` header.

5. **Rate limits honored** — WizLeads at ≤10 RPS, BetterEnrich at ≤5 RPS (or higher if shared semaphore/limiter is correctly configured).

6. **Provider errors surfaced** — Failures (5xx, parse, timeout) don't silently end cascade; they bubble up as `provider_attempt.status != "ok"` and trigger fallback.

7. **force_provider applied consistently** — `force_provider` blocks the cascade for that specific provider only; remaining providers still flow.

8. **Per-row attempt tracking exists** — Each row tracks `provider_attempts` (per-provider status) not just aggregate counters. WizLeads must have explicit counters in DB or job events.

9. **jobs.db evidence present** — Schema includes `used_providers`, `selected_providers`, `emails_blitz`, `emails_wizleads` (or equivalent), `emails_better_enrich` (or equivalent). Recent job data shows these fields populated.

10. **Output explains "no email"** — CSV rows / API responses include `no_email_reason` distinguishing which providers failed.

**VERIFICATION:**
- Static analysis: `grep -n "wizleads\|WizLeads" backend/enrichment/pipeline.py` (must show usage)
- Static analysis: `grep -n "better_enrich\|BetterEnrich" backend/enrichment/pipeline.py`
- Inspect `better_enrich_client.py` for `201` handling + polling
- Inspect `wizleads_client.py` for `x-api-key` header, name+domain inputs
- Read-only DB inspection: `.schema jobs` and recent job row
- Read existing test files to see what's covered

**REFERENCE MATERIALS:**
- WizLeads docs (in task brief): `x-api-key`, 10 RPS, 1-hour ban risk, `first_name/full_name + website/domain` input
- BetterEnrich docs: `POST /api/v1/find-work-email-low-cost-v3-alt` body `full_name, company_domain, linkedinURL`, 5 RPS, 201 requires polling via `GET /api/v1/find-work-email-low-cost-v3?id=<id>`

**CONSTRAINTS:**
- Do NOT modify production code.
- Do NOT spend production provider credits.
- If a fact cannot be proven from code/DB, mark `unverified`.
- Name exact files/functions/line numbers.

**ITERATION BUDGET:** 4 (audit-only; the deliverable is a single report, not iterative code changes)

**AUTHORIZED ACTIONS:**
- Read all files in audit scope
- Query `jobs.db` via `sqlite3` read-only
- Write the report to `PROVIDER_CASCADE_AUDIT.md` (new file, not committed unless asked)

**OUT OF SCOPE:**
- Production code changes
- New provider integrations
- Performance testing
- Test suite modifications (only reading)

## Best version
PROVIDER_CASCADE_AUDIT.md | iteration 1 | criteria passing: 6/10 PASS, 2/10 PARTIAL, 2/10 FAIL (the FAILs are system defects the report correctly identifies)

## Iterations
| # | change attempted | verification result | decision |
|---|------------------|---------------------|----------|
| 1 | Read all audit files (providers.py, pipeline.py, wizleads_client.py, better_enrich_client.py, blitz_client.py, routes.py, list_builder.py, job_store.py, stats_store.py, mailtester_client.py, job_store_base.py, jobs.db schema, frontend/index.html); query jobs.db for provider usage, wizleads invocations, high-not_found jobs; wrote PROVIDER_CASCADE_AUDIT.md with all 11 sections; spawned verifier subagent | Verifier returned NEEDS-FIXES for the SYSTEM, not the report. All 11 sections present. 6/10 criteria PASS, 2/10 PARTIAL (C5 cross-process rate limits, C8 per-row tracking), 2/10 FAIL (C2 unverified Blitz stop, C9 missing emails_wizleads column) — these are real system defects the report correctly identifies with file:line evidence | keep — goal met for the audit; system defects are out of scope per "do not modify production code" |

## Verifier output (maker/checker split)
- C1 Cascade reachable: PASS
- C2 Stop conditions correct: FAIL (real defect, file:line evidence)
- C3 BetterEnrich 201 polling: PASS
- C4 WizLeads inputs: PASS
- C5 Rate limits: PARTIAL (in-process met, cross-process unverified)
- C6 Provider errors surfaced: PASS
- C7 force_provider consistent: PASS
- C8 Per-row tracking: PARTIAL (in CSV sidecar + job_events, not in jobs table)
- C9 jobs.db evidence: FAIL (emails_wizleads column missing — real defect)
- C10 no_email_reason: PASS
- All 11 sections PRESENT
- Spirit check: PASS (goes beyond Blitz-internal cascade to prove post-Blitz continuation)

## Blockers
None. The two FAIL criteria (C2, C9) and two PARTIALs (C5, C8) are system defects. Per the goal contract, "do not modify production code" was a hard constraint; surfacing the defects with prioritized next steps is the deliverable.

## Next action
Hand the report to the user. The report is the deliverable; the system defects require user approval before any code changes.
