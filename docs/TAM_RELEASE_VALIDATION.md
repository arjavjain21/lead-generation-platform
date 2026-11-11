# TAM 3-Step Release Validation

**Release branch:** `feat/tam-3step-release` (from master @ `384e33b`)
**Validated:** 11 November 2026 · **All validation below ran on the release branch's working tree.**

## 1. Automated test matrix (final)

Backend (23 suites, `backend/enrichment/tests/`):

| Suite | Result | Covers |
|---|---|---|
| test_mailtester_key / _timeout / test_enrich_throughput_hardening | 10 / 4 / 14 | PR-0.1 key policy, fail-open contract, breaker/cache |
| test_tam_flow | 52 | PR-2.3 chaining + restart parity (incl. hardening/parity classes) |
| test_flows_help_truth | 5 | PR-0.2 live cascade table |
| test_catch_all_retention | 9 | PR-1.2 retention, gates, counter, kill switch |
| test_verify_flow / test_verify_routes | 10 / 6 | PR-1.1 engine + API + runner + CSV schema |
| test_flexible_intake | 4 | PR-2.1 mixed identifiers + skip reasons |
| test_company_resolver | 10 | PR-2.2 ambiguity matrix, gating, cache |
| test_depth_location_knobs | 4 | PR-2.4/2.5 clamps + cascade location |
| test_quickenrich_endpoint | **18** | **pre-existing box hang FIXED** (was: hang >150 s; now 2.2 s) |
| test_company_linkedin_support / test_seg_csv / test_website_only_mode / test_domain_only_api_cascade / test_cascade_fixes / test_cascade_collector_wiring / test_company_fallbacks / test_moltsets_client / test_stored_phones_retrieval_20261028 / test_lookalike / test_tam_routes | 18/21/9/5/9/22/19+12/24/9/26/27 | regressions — all baseline-identical |

**Backend total: 364 passed, 0 failed.** Frontend (`frontend/tests/`): **148 passed, 0 failed** (incl. 11 verify-UI pins, 7 people-company-mode pins, 11 truth-sync pins). **Combined: 512 / 0.**

Secret scan on the release tree: **0 occurrences** of the burned MailTester literal.

## 2. API click-through (new code, mocked providers, temp DB)

| Step | Call | Result |
|---|---|---|
| Stage-2 intake | `POST /upload` (mixed CSV: LinkedIn-only + domain-only + name-only rows) | 200 |
| Stage-2 job | `POST /flows/domain-enrich` (3 identifier columns, strict titles) | 200, `total=3`, job reached `done`, `source_type=csv_upload` |
| Stage-3 inline | `POST /verify` (ok/mb/ko fixture) | 200 — `verified=1, catch_all=1, invalid=1, validated=3, sendable=1` (default policy) |
| Stage-3 chunk | `POST /verify/from-job/{id}` on an email-less job | **400 "No emails found"** — correct negative behavior |
| Auth | `POST /verify` without credentials | 401 |

## 3. Live-frontend DOM evidence (production URL, served from the working tree)

`curl -s https://listbuilding.eagleinfoservice.com/` contains:

- `id="page-verify"` + `data-page="verify"` + `'verify': 'verify'` — Verify page, nav, route ✓
- `id="peopleScopeCard"` + `id="verifyAllowCatchAll"` — People company-mode + verify options ✓
- `30M+ people`, `We use 8 active data sources`, `Last Updated: 2026-11-11`, `value="tam_flow"`, `catch-all retained` — truth-sync surfaces ✓

(The nginx no-cache config serves `frontend/index.html` straight from the working tree, so the UI shipped the moment it was committed. The BACKEND halves need the service restart — see §5.)

## 4. Count-reconciliation proofs

- Verify summary: `validated = verified + catch_all + invalid` (outages excluded — pinned in test_verify_flow's mixed fixture); runner counters == CSV row count (test_verify_routes background-runner test).
- Catch-all retention: `jobs.catch_all_retained` == rows with non-empty `dm_email_catch_all` (test_catch_all_retention), badge reads the same counter.
- TAM chain: `chained_total` == parity-CSV rows == runner rows (test_tam_flow parity tests); restart returns exactly `chained_total − done`.

## 5. Deployment status & the single blocker

| Piece | Status |
|---|---|
| Frontend (all 3 stages' UI) | **LIVE** (nginx serves the working tree) |
| Backend code | on `feat/tam-3step-release`, **not yet running** — gunicorn still executes the pre-release import |
| Blocker | **9 enrichment jobs running at validation time.** Restarting the service mid-job risks killing live work (the DB is the only cross-worker state). |
| Required operator sequence | §6 below — drain/cancel window → merge to master → restart → canary |

## 6. Post-restart validation (operator runs after the deploy window)

1. `curl -s localhost:8765/api/health` → `{"status":"ok","mcp_enabled":true}`; logs clean 60 s.
2. `curl -s localhost:8765/api/enrichment/flows/help | python3 -c "import json,sys; d=json.load(sys.stdin); print([ (t['name'], t['status']) for t in d['providers']['cascade_order']][:4])"` → first four tiers with live `status` fields.
3. **Verify canary (bounded, ~free):** `/#/verify` → paste 3–5 of your own addresses → expect per-address statuses + CSV download; run again → `cache_hit=true` on all.
4. **TAM→People→Verify canary (bounded):** `/#/search` with a small persona (≤25 companies) + "find contact emails" ticked → chain job total includes LinkedIn-only companies; parity file `backend/data/uploads/find_companies_contacts_*_<job8>.csv` exists; download the contacts CSV; on its Jobs card click **✓ Verify emails** (limit 20) → verify job completes; compare the three job cards' counts with their CSVs.
5. Restart-parity probe (optional, strongest): cancel the chain job mid-run → Restart it → resumed total == `chained_total − processed`, no duplicate rows.
6. MailTester key rotation per `TAM_IMPLEMENTATION_PROGRESS.md` §4.3 (proxy owner), then revoke the exposed key once the new one is confirmed working — do not hold an arbitrary 48 h window.

## 7. Rollback

- Env-only: `ENABLE_CATCH_ALL_RETENTION=false` (retention), `ENABLE_NAME_RESOLVER` stays false (resolver off — safe default).
- Code: `git revert` the release range on master → cancel/wait → restart → health checks.
- Frontend-only instant rollback: restore `frontend/index.html.bak_pre_truth_sync_20261111` (pre-truth-sync) — note the newer verify/people panels ship in later commits; a full-tree revert covers everything at once.
- Data artifacts: additive only (`verify_results`, `name_resolution` tables; trailing CSV columns; parity CSVs). No destructive cleanup; never restore a DB backup over new data.
