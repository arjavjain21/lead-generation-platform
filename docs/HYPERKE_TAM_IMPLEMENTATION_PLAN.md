# Hyperke TAM / People Finder / Verification — Phased Implementation Plan

**Prepared:** 10 November 2026 (immediately after the read-only audit)
**Input:** `docs/HYPERKE_TAM_PEOPLE_VERIFICATION_AUDIT_2026-11-10.md` (evidence register E-001…E-048)
**Baseline:** commit `384e33b` (branch `feat/moltsets-cascade` == `master`). All file/line references are as of this commit and will drift — re-locate by symbol name before editing.
**Status:** PLAN ONLY. Nothing herein has been implemented or deployed.

---

## 0. Scope, constraints, and design principles

### 0.1 Invariants (from the operating skill — every PR must respect these)

1. **Never change `job_type` / `status` literal values.** New surfaces reuse `job_type='enrichment'` with a new `source_type` (precedent: `tam_flow`, `tam_chain` at `tam_routes.py` / `tam_chain.py:107`).
2. **Never duplicate the enrichment waterfall.** The verify job calls **only** `mailtester_client`; the company-name resolver runs **before** the cascade; the People-page company mode **re-uses** `_run_domain_enrich_job`. No new cascade code paths.
3. **Preserve existing production behavior by default.** `dm_email` is only ever populated with policy-accepted emails (`MAILTESTER_ACCEPT_CODES=ok`). Catch-all retention adds *trailing, additive* CSV columns (precedent: `dm_mx_provider`, `dm_previous_companies` appended with `restval=''` for resume compat — `list_builder.py:297-309`). Every behavior-adjacent change carries an env kill-switch defaulting to the current behavior.
4. **Contacts write-back only via `contacts_writer`.** The verify job performs **no** contacts-DB writes except the existing `mark_email_invalid` path on `ko` (same call the cascade already makes), gated to match.
5. **No `.env` commits; no secrets in code** (PR-0.1 exists because this rule is currently violated by a hardcoded fallback key).
6. **CSV schema is append-only.** New columns go at the end of `ENRICHED_COLUMNS` (`list_builder.py:240-310`) and pipeline's mirrored list (`pipeline.py:506-579`).
7. Deploys follow the runbook: cancel running jobs → wait `cancelled` → `git tag pre-<change>-$(date +%Y-%m-%d)` → restart service → post-deploy checklist (`references/testing-and-deploy.md`).

### 0.2 The four review directives and where they are answered

| Directive | Answer |
|---|---|
| **Overlooked catch-all retention** — the audit noted `mb` emails are dropped and *not retained anywhere in the export*; the winning row loses the address entirely | **PR-1.2** — `catch_all_candidates` capture at every mailtester reject branch + trailing `dm_email_catch_all` column + job counter; **PR-1.1/1.3** make catch-all a first-class, retained status (`valid_catch_all`) on the verify surface with a per-run policy toggle |
| **Contact discovery limits** (QE ≤2 pages×20, LO ≤100/domain, waterfall `max_results` 3/5/10, contacts pool 12) | **PR-2.4** — per-row/per-job cap markers, per-request `dataset_max_pages` override bounded by env, UI helper text + TAM `capped` surfacing; **PR-2.6** exposes "more available" semantics to the operator |
| **Ambiguous company-name matching** (name-only rows; two companies same name; wrong-company joins) | **PR-2.2** — dedicated resolver module: internal contacts-API search (free) → Blitz name search (env-gated), country/industry corroboration, explicit `high/medium/ambiguous/none` confidence; **ambiguous never auto-joins** (row flagged `no_email_reason='ambiguous_company_name'`); conflict flag when provided identifiers disagree (**PR-2.1**) |
| **Inconsistent effort estimates** (audit exec verdict "≈2–3 weeks P0" vs backlog sum 12.5–19 pd vs "8–14 pd") | **§8 effort model** — one unit (1 pd = 6 focused hours, dev fluent in this repo, tests + doc touch included), P50–P90 ranges, explicit 15% integration overhead, reconciled audit-item → PR mapping |

### 0.3 Effort model (binding for all numbers below)

- **1 pd = 6 focused hours** of one developer already fluent in this repo (knows the job system, cascade, and deploy ritual).
- Estimates **include**: unit tests, pinned frontend tests where applicable, doc/reference touch, and the PR's own rollback prep.
- Estimates **exclude**: cross-PR integration testing (covered by the 15% phase overhead) and deploy windows (ops, not dev).
- Ranges are **P50–P90**. Totals sum the P50s and P90s; each phase adds 15% overhead for integration + test-suite triage.
- Assumed team: **1 developer**. Two developers do not halve wall-clock inside a phase (single-file frontend + one routes.py create merge friction); parallelize *across* phases instead.

---

## 1. Phase overview and dependency graph

```
Phase 0 (hygiene, 1 pd)          Phase 1 (Stage 3 + retention)         Phase 2 (Stage 2 intake + limits)
  PR-0.1 key rotation ──────────► PR-1.1 verify job backend ──► PR-1.3 verify tab UI
  PR-0.2 docs/truth sync            ▲                                   │
                                     └── PR-1.2 catch-all retention ──► PR-1.4 verify rollups
                                        (independent of 1.1; can land first)

  PR-2.1 flexible Flow-1 intake ──► PR-2.2 name resolver (ambiguity) ──► PR-2.3 TAM chain LI rows (needs 2.1 pattern)
  PR-2.4 discovery caps ──► PR-2.5 person location ──► PR-2.6 People-page company mode (needs 2.1)

Phase 3 (efficiency, after Phase-1 validation in prod)
  PR-3.1 persist verification (needs contacts-api change) ──► PR-3.2 shared verify cache for gates
  PR-3.3 GL matched_people semantics · PR-3.4 verify throughput · (PR-3.5 project tags — decision pending Q-3)
```

**Ship order:** PR-0.1 → PR-0.2 → PR-1.2 → PR-1.1 → PR-1.4 → PR-1.3 → *(operator validation window ≥1 week)* → PR-2.3 → PR-2.1 → PR-2.2 → PR-2.4 → PR-2.5 → PR-2.6 → Phase 3.
Rationale: catch-all retention (1.2) is the smallest highest-value behavioral fix and unblocks nothing risky; verify backend (1.1) is the only genuinely new engine; Phase 2 reshapes intake only after Stage 3 is proven.

---

## 2. Phase 0 — hygiene & truth sync (≈1 pd)

### PR-0.1 — Remove hardcoded MailTester key; rotate the secret

- **Why:** `mailtester_client.py:91` falls back to a plaintext API key committed in source (audit E-022). Violates security rule; the key must be treated as burned.
- **Files / functions / changes**
  - `backend/enrichment/mailtester_client.py`
    - `_get_api_key()` (lines 90–94): delete the `or "<literal>"` fallback; missing env → `raise RuntimeError("MAILTESTER_API_KEY not set")`. Delete the now-dead `if not key` branch beneath it. **Semantics preserved at call sites:** the raise happens inside `verify_email`, which callers already catch as `RuntimeError` → existing fail-open path (email accepted, `mailtester_code="unavailable"`). No cascade change.
    - `backend/.env`: replace `MAILTESTER_API_KEY` value with the rotated key (ops step, file is gitignored — never committed).
    - Rotation of the upstream key happens on the proxy side (`/opt/mailtester-proxy` — outside this repo; coordinate with owner).
- **Dependencies:** none. Deployed independently.
- **Tests:** new `backend/enrichment/tests/test_mailtester_key.py`: (a) env unset → `verify_email` raises `RuntimeError` (unit, monkeypatch `os.environ`); (b) existing `test_mailtester_timeout.py` still green (fail-open unchanged); (c) grep-style test asserting no 40-char hex literal remains in the module (cheap guard).
- **Rollback:** `git revert` + restore old env value (old key remains valid until proxy-side revocation — do not revoke before rollback window closes, 48h).
- **Acceptance:** (1) `pytest tests/test_mailtester_key.py tests/test_mailtester_timeout.py` green; (2) after deploy, one manual `POST /api/enrichment/quickenrich {domain:"example.com", validate_emails:true}` returns `summary.validator="mailtester"` with ≥0 `validated` (proves the new key authenticates); (3) `grep -E '[0-9a-f]{40}' backend/enrichment/mailtester_client.py` → empty.
- **Effort:** 0.5 pd.

### PR-0.2 — Operator-facing truth sync (docs-in-code)

- **Why:** three operator-visible surfaces contradict live code (E-005, E-015, E-041, E-043).
- **Files / functions / changes**
  - `backend/enrichment/routes.py` — `/flows/help` handler `cascade_order` list (lines 939–948): rewrite to the live 9-tier order incl. leadocean (tier 4) + moltsets (tier 6), better_enrich annotated "code-present, env kill-switch OFF in prod". Build it from `providers.get_enabled_providers()` + a static tier map rather than a second hand-maintained list (keeps membership live; order stays explicit).
  - `frontend/index.html` — Help page `#page-help` (lines ~2496–2706): "Last Updated" → 2026-11-10; source list → 7 active tiers + "(BetterEnrich/Prospeo disabled)"; People banner line 2090: "6.6M+ leads" → "30M+ people" (E-005/E-027).
  - `frontend/index.html` — Jobs page Source `<select>` (lines 2478–2485): add `<option value="tam_flow">🎯 TAM</option>`, `tam_chain`, and (forward-compatible) `verify`.
- **Dependencies:** none.
- **Tests:** extend `frontend/tests/test_tam_page_ui.py`-style pinned test (new `frontend/tests/test_truth_sync_ui.py`): assert Help page contains "LeadOcean" and "MoltSets", banner contains "30M", jobs filter contains `tam_flow`. Add `backend` unit test asserting `/flows/help` JSON lists leadocean + moltsets in that order.
- **Rollback:** `git revert`; content-only.
- **Acceptance:** `curl -s localhost:8765/api/enrichment/flows/help | jq '.providers.cascade_order[].name'` shows the 9 tiers in cascade order; frontend pinned tests green.
- **Effort:** 0.5 pd.

---

## 3. Phase 1 — Stage 3 verification + catch-all retention (≈13.5 pd ±, see §8)

### PR-1.1 — Standalone email-verification job (backend)

- **Why:** the only genuinely absent stage (E-025). Everything below is assembly of existing parts — **no enrichment-cascade calls anywhere in this PR.**
- **New files**
  - `backend/enrichment/verify_flow.py` (~250 lines): the runner + result store.
  - `backend/enrichment/verify_routes.py` (~150 lines): endpoints.
  - `backend/enrichment/tests/test_verify_flow.py`, `backend/enrichment/tests/test_verify_routes.py`.
- **Modified files**
  - `backend/main.py` (router mounts, after line 391): `app.include_router(verify_routes.router, tags=["enrichment"])` — same pattern as `tam_routes`.
  - `backend/enrichment/routes.py`: **no changes** (generic `/jobs`, `/jobs/{id}`, `/jobs/{id}/stream`, `/jobs/{id}/download` are reused as-is because the verify job is a plain `job_type='enrichment'` row).
- **Design**
  - **Job model (decides audit Q-1 → adopted default):** `job_type='enrichment'`, `source_type='verify'`. Inherits SSE, cancel, heartbeat, reaper, download, Jobs-page listing with zero new plumbing. New literal only in `source_type` (free-form column; `tam_flow` precedent).
  - **Endpoints (`verify_routes.py`):**
    - `POST /api/enrichment/verify` — body: `{emails: [≤1,000]}` **or** `{upload_id, email_col?}` (reuses the existing generic `POST /api/enrichment/upload` at `routes.py:4825` for CSV/paste), plus `{allow_catch_all: bool=false, recheck: bool=false, job_name?}`. ≤200 emails → **synchronous** response (mirrors quickenrich's ergonomics); >200 → background job, returns `{job_id}`.
    - `POST /api/enrichment/verify/from-job/{job_id}` — chunk selection (audit scenario 6): params `limit` (default 2,000), `offset`, `only_with_email=true`, `only_unverified=true`; reads the source job's output CSV server-side (`backend/data/outputs/{job_id}.csv`), extracts `dm_email` + `dm_email_catch_all` (from PR-1.2) + `company_email` per row, dedups, submits the same verify job with `source_job_id` lineage (`parent_job_id=job_id`).
    - Auth: `Depends(auth.get_current_user_with_api_key)` — identical to every enrichment endpoint.
  - **Runner (`verify_flow.py`):**
    - `async def run_verify_job(job_id, emails, allow_catch_all, recheck, …)` — mirrors the skeleton of `_run_domain_enrich_job` (`routes.py:7356+`) / `_run_tam_job` (`tam_routes.py:532-639`): `set_running`, 30s heartbeat, `append_event` counters, cancel → `partial` CSV, done → `_mark_done_and_cleanup` equivalent, `finally` cleanup of `_job_signals` registration.
    - Per email: **cache lookup first** (`verify_results` table, §storage), then `mailtester_client.verify_email(client, email)` under `asyncio.Semaphore(VERIFY_CONCURRENCY=8)` (≤ the client's 10 RPS limiter, `mailtester_client.py:32-33`).
    - **Status mapping — reuse, don't re-implement:** import `_map_validation_status` from `routes.py` is ugly (8.8K-line module); instead **move** that 12-line function (`routes.py:1580-1591`) verbatim into `verify_flow.map_validation_status(code, allow_catch_all)` and make `routes.py` import it from there (pure refactor, byte-identical outputs; `valid_catch_all` added only when `allow_catch_all and code=="mb"`, matching `routes.py:4375-4386` semantics).
    - Failure semantics: `RuntimeError` from mailtester (breaker open / outage) → row status `unknown`, code `unavailable`, **fail-open** — identical to cascade behavior (E-022). `ko` rows **do not** call `mark_email_invalid` here by default (verification is read-only by intent); add `poison_invalid: bool=false` opt-in that reuses the existing `contacts_client.mark_email_invalid` call shape — default off preserves behavior.
    - Output CSV (`backend/data/outputs/{job_id}.csv`, written incrementally): `email, email_norm, status, mailtester_code, mailtester_message, verifier, verified_at, cache_hit, source_job_id`. Job counters: `total, processed, valid_ok, valid_catch_all, invalid, unknown` — via `append_event` (atomic-counter rule #8).
  - **Storage — new table (additive, `call_tracker`-style module-local init):**
    ```sql
    CREATE TABLE IF NOT EXISTS verify_results (
      email_norm TEXT NOT NULL, code TEXT NOT NULL, message TEXT,
      allow_catch_all INTEGER NOT NULL,   -- policy under which verified
      verifier TEXT NOT NULL DEFAULT 'mailtester',
      verified_at TEXT NOT NULL, source_job_id TEXT,
      PRIMARY KEY (email_norm, allow_catch_all)
    );
    ```
    Created by `verify_flow._ensure_tables()` (guarded, called on first use — same pattern as `call_tracker.py:107-188` creating its tables; **no `shared/db.py` edit needed**). Cache read rule: reuse a row when `verified_at >= now - VERIFY_RESULT_TTL_DAYS (env, default 30)` **and** its policy matches the request's `allow_catch_all` (an `mb` verdict cached under strict policy is still `mb` — the *status* differs, so policy is part of the key). `recheck=true` bypasses reads (never deletes history).
  - **Concurrency/limits:** batch cap `VERIFY_MAX_EMAILS=50_000` (align with the platform's biggest export); per-request ≤1,000 inline. Daily quota: verify requests count against the existing `daily_api_requests` 50K/day via the standard dependency — no new metering.
- **Dependencies:** PR-0.1 (key hygiene first — this PR multiplies mailtester usage surface). No dependency on PR-1.2.
- **Tests (new files, ~35 cases)**
  - Unit (`test_verify_flow.py`, mock `httpx` via the existing monkeypatch pattern from `test_mailtester_timeout.py`): ok/mb/ko/unavailable mappings; `allow_catch_all` flag → `valid_catch_all`; cache hit path (second run makes zero HTTP calls — assert via call-count mock); TTL expiry; dedupe within batch; `poison_invalid=false` never calls `mark_email_invalid`; cancel mid-job → `partial` status + readable partial CSV; heartbeat appended.
  - Route (`test_verify_routes.py`): 401 without auth; ≤200 sync shape; >200 job shape; `from-job` chunk reads source CSV columns `dm_email`/`dm_email_catch_all`/`company_email` and respects `limit/offset/only_*`; unknown job → 404; job appears in `GET /api/enrichment/jobs` with `source_type='verify'`; download works via the generic endpoint.
- **Rollback:** `git tag pre-verify-job-$(date +%F)` before deploy. Rollback = revert commit + restart (check running jobs first). Table `verify_results` is additive — leave it (harmless) or `DROP TABLE verify_results` after revert if desired. No existing endpoint touched except the 12-line function move (byte-identical behavior, verified by existing quickenrich endpoint tests).
- **Acceptance criteria**
  1. Upload a 100-email fixture (mixed real-format addresses, no live cost — mailtester is self-hosted; still use the staging-safe rule: run against a 10-address smoke first) → all 100 rows carry `status ∈ {valid_ok, valid_catch_all, invalid, unknown}`; counts in job payload reconcile with CSV rows exactly.
  2. Immediate rerun of the same 100 → 100 `cache_hit=true`, zero new mailtester calls (verify via `provider_call_log` delta = 0).
  3. Job visible + cancellable + downloadable from the Jobs page with no UI changes (generic listing).
  4. `POST /verify/from-job/{id}?limit=50` on a fixture job with 120 emailed rows → exactly 50 unique addresses submitted.
- **Effort:** 4.5 pd (P50 4 / P90 5.5).

### PR-1.2 — Catch-all retention in the enrichment outputs

- **Why:** today, when MailTester returns `mb` and policy is `ok`-only, the candidate address is dropped at the gate and **does not survive anywhere in the export** (E-022/E-045) — the audit's overlooked retention issue. Operators cannot even see what they lost.
- **Files / functions / changes**
  - `backend/enrichment/pipeline.py`
    - `verification_info` initializer inside `_resolve_email_for_person` (lines 2512-2521): add `"catch_all_candidates": []` next to `provider_errors`.
    - **7 one-line appends at the policy-reject branches** (the `else` arm of `if result["valid"]:` where `code == "mb"`): contacts_db gates at 2604-2618 and 2666-2678; provider gates at 2855, 2979, 3037, 3104, 3214 areas. Exact shape:
      ```python
      if result["code"] == "mb":
          verification_info["catch_all_candidates"].append(email)
      ```
      (Only `mb`; `ko` stays fully dropped — it is poisoned in the contacts DB and worthless.)
    - `_build_person_row` (line 2174): serialize `verification_info.get("catch_all_candidates")` → `";".join(...)` into new row field `dm_email_catch_all`.
    - Pipeline's output-column list (506-579): append `dm_email_catch_all` **at the end** (resume-compat `restval=''` precedent).
  - `backend/enrichment/list_builder.py`
    - `_resolve_person_email` (line 627): it returns a tuple; **extend, don't reshape** — the two contacts_db gates (742-766, 794-818) get the same one-line append into a local `catch_all: list[str]`, returned as a new final tuple element (existing callers defensive-index `result[:6]` / `result[6]` — the new element is `result[7]`, strictly additive; the row build at ~4013-4045 reads it).
    - `ENRICHED_COLUMNS` (240-310): append `dm_email_catch_all` at the very end.
    - Row build (~4013-4045) and the company-URL orchestrator's row build (~3980-4045 shared path): set `row["dm_email_catch_all"]`.
    - Progress event in `process_row` (2694-2710): add `catch_all_retained` count (sum of rows with non-empty field) alongside `title_gate_dropped`.
  - **Job counter (jobs-table column, `title_gate_dropped` precedent):**
    - `backend/shared/db.py` migration block (~460-461): guarded `ALTER TABLE jobs ADD COLUMN catch_all_retained INTEGER DEFAULT 0` (try/except, idempotent — exact pattern of the neighboring lines).
    - `backend/enrichment/job_store.py`: counter update where `title_gate_dropped` is updated (same commit site).
    - `backend/enrichment/routes.py` `_run_domain_enrich_job` + `tam_routes.py` unaffected (counters flow through the shared progress-callback path that already increments `title_gate_dropped`).
  - **Kill switch:** `ENABLE_CATCH_ALL_RETENTION` env (default **true** — the change is additive columns only; `dm_email` semantics untouched). When false, gates skip the append and columns stay empty → CSV byte-identical to today *except* two trailing empty header cells; for true byte-parity set `VERIFY...` no — if byte-parity is demanded, also conditionally omit the headers; **decision default: always write the headers** (matches how `seg`/`mx` columns shipped — trailing blanks, E-013 note `restval=''`).
- **Dependencies:** none within Phase 1 (lands before or after PR-1.1). Touches the two executors — merge carefully with any in-flight cascade PRs (currently none).
- **Tests**
  - Extend the existing gate tests (pipeline + list_builder contacts_db mailtester suites): mocked `mb` verdict → row has `dm_email_catch_all == email`, `dm_email == ""` (or next-tier winner), `dm_email_verified` unchanged semantics.
  - Resume-compat test: craft a pre-feature partial CSV + new header via the incremental writer → old rows get blank `dm_email_catch_all` (`restval=''`).
  - Counter test: 3 rows with retained addresses → `jobs.catch_all_retained == 3` after job done.
  - Kill-switch test: env off → field always empty, counter 0.
  - Frontend: extend `frontend/tests/test_job_naming_and_title_gate_badge.py` pattern — badge shows the count (UI part lands in PR-1.4; test the endpoint payload here).
- **Rollback:** env `ENABLE_CATCH_ALL_RETENTION=false` (instant, no deploy) or `git revert`. The jobs-table column is additive — leave it.
- **Acceptance criteria**
  1. Fixture job with a mocked accept-all domain: every person whose only candidate was `mb` now shows the address in `dm_email_catch_all` and `dm_email_verified` reflects the *winning* email as before; **zero change** to `dm_email` values vs pre-PR run of the same fixture (diff the two CSVs on all pre-existing columns).
  2. `jobs.catch_all_retained` reconciles with CSV rows having non-empty `dm_email_catch_all`.
  3. Full `pytest -k "catch_all or mailtester"` green; suite shows no new failures beyond the known ~7.
- **Effort:** 3.5 pd (P50 3 / P90 4.5).

### PR-1.3 — Verification tab UI (+ job-card "verify these emails")

- **Files / functions / changes** — all in `frontend/index.html` (single-file app; `.bak_pre_verify_tab_20261110` backup first, per repo ritual):
  - Nav (after line 1173, LinkedIn Enrich): `<a class="nav-item" href="#" data-page="verify">…Verify Emails</a>`; add `"verify": "page-verify"` to the `ROUTES` map (~3146-3160 and the mirrored map at ~8280).
  - New `<div class="page" id="page-verify">` (place after `#page-linkedin`, ~2401): (a) paste textarea + upload zone (clone of the domain upload zone → `POST /api/enrichment/upload`, then `email_col` autodetect by header keywords `email/email address/work email`); (b) options: `allow_catch_all` checkbox (default **unchecked** — preserves current sendability posture), `recheck` checkbox, job name; (c) count preview before submit ("214 unique emails — will verify inline/job"); (d) progress card clone (total/processed/valid_ok/valid_catch_all/invalid/unknown stat tiles) fed by SSE for job mode; (e) results table + `Download CSV` via the native-anchor `downloadFile()` (~6005-6019 — the fetch+blob truncation trap is already solved there).
  - Enrichment job cards (jobs render, ~6880-6990): add `⚙ Verify emails` button (visible when `emails_found > 0`) → prompts for `limit` (default 2000) → `POST /api/enrichment/verify/from-job/{job_id}?limit=…` → toasts the created verify job and links to it.
  - **`window.*` exports** for any inline `onclick` handlers (the recurring missing-export trap, 3 prior outbreaks).
  - JS: new `submitVerify()`, `pollVerifyJob()`/SSE variant, `renderVerifyResults()` — follow the Find People block's abort/cancel pattern (5173-5217).
- **Dependencies:** PR-1.1 (endpoints), benefits from PR-1.2 (`dm_email_catch_all` included by `from-job`).
- **Tests:** new `frontend/tests/test_verify_ui.py` (pinned-HTML pattern of `test_tam_page_ui.py`): nav item exists; page div + the three option controls exist; jobs-card button string present; `ROUTES` contains `verify`. Manual: Playwright-style click-through is manual-only here (repo has no browser harness) — record in PR description.
- **Rollback:** restore `.bak` (frontend is served no-cache — instant) or `git revert`.
- **Acceptance:** operator pastes 50 emails → sees per-status counts within the inline window; a 2,000-row `from-job` verification appears on the Jobs page, streams progress, and downloads a CSV whose rows reconcile with the streamed counts; `curl -sI https://listbuilding.eagleinfoservice.com/#/verify` page loads (hash route — assert served HTML contains `page-verify`).
- **Effort:** 3.5 pd (P50 3 / P90 4.5).

### PR-1.4 — Verification + retention rollups on job payloads & cards

- **Files / changes:** `backend/enrichment/routes.py` `GET /jobs/{job_id}` (5101+): include `verification_rollup` computed from accumulated counters (`valid_ok/valid_catch_all/invalid/unknown/catch_all_retained` — the quickenrich summary-shape, `routes.py:4331-4339`, generalized); `frontend/index.html` job card: stat chips next to the title-gate badge (pattern: `test_job_naming_and_title_gate_badge.py`).
- **Dependencies:** PR-1.1 (verify counters), PR-1.2 (`catch_all_retained`).
- **Tests:** unit — enrichment job with mocked mixed verdicts exposes the rollup; frontend pinned — chip markup.
- **Rollback:** revert; read-path only.
- **Acceptance:** for any job, `Σ rollup == processed rows with emails` (where validator ran); UI chip numbers equal payload numbers.
- **Effort:** 2 pd (P50 1.5 / P90 2.5).

---

## 4. Phase 2 — Stage-2 intake flexibility, name ambiguity, discovery limits (≈16 pd ±, see §8)

### PR-2.1 — Identifier-flexible Flow 1 (`domain` column no longer mandatory)

- **Why:** E-009/E-010 — a LinkedIn-only or name-only list cannot be submitted at all (400 on missing `domain_col`), and name-only rows are silently `SKIPPED`. The brief requires any-of `company_linkedin | domain | company_name`.
- **Files / functions / changes**
  - `backend/enrichment/routes.py`
    - `domain_enrich_with_providers` (7200-7350): replace the hard check at 7226-7229 with: require **at least one of** `domain_col`, `company_linkedin_col`, `linkedin_url_col` to be present-and-non-null (400 message lists which). When `domain_col` is set it must still exist in the CSV (unchanged).
    - Pre-processing (7263-7271): `dedupe_by_domain` only when `domain_col` is set; otherwise dedupe by normalized company/person LinkedIn URL — new helper `dedupe_rows_by_identifier(rows, cols, normalize)` in `backend/enrichment/identifier_utils.py` (thin wrapper reusing `normalize_linkedin_url`; falls back to first-non-empty identifier per row).
    - Pass `domain_col: Optional[str]` through to `_run_domain_enrich_job` (7356+, signature already tolerant? — it currently requires it; make it Optional and thread into `run_domain_enrichment`, `list_builder.py:2350-2363` param `domain_col: Optional[str] = None`).
  - `backend/enrichment/list_builder.py`
    - `process_row` (2589-2682): the routing chain becomes **4-way**:
      1. company LinkedIn URL (explicit col or `/company/` autodetect) — unchanged, **still wins** (preserves E-010 precedence);
      2. domain present — unchanged domain cascade;
      3. **no domain, no URL, but `company_name_col` value present** → call the resolver (PR-2.2) when `ENABLE_NAME_RESOLVER=true`; resolved `high/medium` → inject `domain` and continue down the existing domain cascade (resolver output, not a new path); `ambiguous/none` → existing `STATUS_SKIPPED` row with `no_email_reason='ambiguous_company_name'|'name_unresolved'` (**no new status literal** — `no_email_reason` is a free-form column with rich precedent);
      4. nothing → `STATUS_SKIPPED` + `no_email_reason='no_identifier'` (today it is silently skipped with no reason — this is a strict improvement).
    - New trailing columns (both executors' lists + `_empty_enriched()`): `company_identifier_used` ∈ `domain | company_linkedin | name_resolved`, `identifier_conflict` ∈ `"" | "domain_mismatch" | "linkedin_mismatch"` — conflict = resolver/other-identifier disagrees with the identifier that won (e.g., company-URL's resolved domain ≠ provided domain). **Conflicts never reroute the winner** (precedence preserved); they only flag.
  - `frontend/index.html` Upload Domains: the "Domain Column" select gains "— None —" default only when a LinkedIn column is selected; helper text: "Provide at least one of: domain, company LinkedIn, company name (name resolves via database match)"; the company-URL warning (3603-3626) extended to name-column choice.
- **Dependencies:** PR-2.2 for branch 3 (merge order: 2.1 can land with branch 3 behind `ENABLE_NAME_RESOLVER=false` default; 2.2 flips it on). **No waterfall changes** — branches 1/2 byte-identical.
- **Tests**
  - `test_domain_enrich_intake.py` (new): 400-matrix (no identifier cols → 400 with message; domain-only / company-LI-only / both → accepted); dedupe-by-URL unit tests; routing unit tests with mocked resolver: LI-only row → company orchestrator called with `domain=""`; name-only + resolver `high` → domain cascade called with resolved domain; `ambiguous` → SKIPPED + reason; conflicting row → winner unchanged + `identifier_conflict` set.
  - Regression: existing Flow-1 suite (1278 `csv_upload` jobs of history) — full `pytest -k domain_enrich` green.
- **Rollback:** `ENABLE_NAME_RESOLVER=false` reverts to branches 1/2/4 (name rows skipped with reason). Full revert restores the 400. Columns additive.
- **Acceptance:** the audit's 3-fixture matrix passes: (a) LinkedIn-only CSV processes 100% of rows through the company path; (b) name-only CSV with resolver enabled resolves ≥ the mocked rate and flags the rest `ambiguous_company_name`; (c) mixed CSV with a deliberately conflicting row enriches by the company URL and carries `identifier_conflict="domain_mismatch"`; **a domain-only CSV produces byte-identical `dm_email` output vs pre-PR** (excluding new trailing columns).
- **Effort:** 4.5 pd (P50 4 / P90 5.5).

### PR-2.2 — Company-name resolver with explicit ambiguity handling

- **Why:** the directive on ambiguous company-name matching — name→domain must never silently join the wrong company.
- **New file:** `backend/enrichment/company_resolver.py` (~180 lines):
  - `async def resolve_company_name(client, name, *, country_hint=None, industry_hint=None) -> ResolveResult` where
    ```python
    ResolveResult = {status: "high"|"medium"|"ambiguous"|"none",
                     domain: str, company_linkedin_url: str,
                     candidates: [{name, domain, linkedin_url, country, score}], source: str}
    ```
  - **Source 1 (primary, free):** new thin wrapper `contacts_client.search_companies_by_name(client, q, limit=5)` → contacts-API `GET /v1/companies/search?q=…` (endpoint exists server-side per the contacts-api surface; **verify response fields before wiring — if the deployed contacts-api lacks `q`-by-name, fall back to `company_by_domain` guess-list or skip Source 1**; the wrapper is the only new contacts_client code, read-only, 75 RPS lane).
  - **Source 2 (fallback, env-gated OFF):** `blitz_client.company_search(client, name=…, limit=5)` (exists, `blitz_client.py:416-482`; cost 1 credit per 5 results) behind `ENABLE_NAME_RESOLVER_BLITZ=false`.
  - **Scoring / ambiguity rules (deterministic, tested exhaustively):**
    - Normalize names (lowercase, strip punctuation/legal suffixes inc/pvt/ltd/llc/gmbh — extend `identifier_utils` with `normalize_company_name`).
    - exact-token match = 1.0; strong prefix/contains = 0.7; else discard.
    - `high` = single candidate ≥0.7; `medium` = multiple ≥0.7 but exactly one matching `country_hint` (or unique domain-across-candidates); `ambiguous` = multiple ≥0.7 with no discriminator; `none` = zero.
    - **`ambiguous` and `medium`-rejected candidates are returned, never joined** — the row is flagged and the operator sees candidates in the CSV (`identifier_conflict` / new `name_candidates` trailing column, `;`-joined `"name|domain"`).
  - **Negative/positive cache:** table `name_resolution (name_norm, country_hint, result_json, resolved_at)` guarded-CREATE module-local (call_tracker pattern), TTL 14d (`NAME_RESOLVER_TTL_DAYS`).
  - Kill switch `ENABLE_NAME_RESOLVER` (default **false** until PR-2.1's fixtures prove quality on 50 real rows — flip-on is a deliberate ops moment, logged).
- **Dependencies:** PR-2.1 (call sites); contacts-api read endpoint availability (verify first — smallest access request: `curl -H "Authorization: Bearer …" https://leadsdatabase.cc/v1/companies/search?q=acme` shape check).
- **Tests:** `test_company_resolver.py` — the full ambiguity matrix (single exact; two same-name different countries w/ and w/o hint; suffix-stripping; blitz fallback gated; cache TTL; `ENABLE_NAME_RESOLVER_BLITZ` off → zero blitz calls asserted via call-log mock).
- **Rollback:** `ENABLE_NAME_RESOLVER=false` (instant). Revert for code. Table additive.
- **Acceptance:** on a 50-row real-world name-only fixture (owner-supplied), resolver returns `high` for a documented fraction with **zero verified wrong-company joins** (manual QA: sample 10 `high` rows, human-check domain); all `ambiguous` rows flagged with ≥2 candidates visible.
- **Effort:** 4.5 pd (P50 4 / P90 5.5) — includes the contacts-API wrapper + shape verification.

### PR-2.3 — TAM chain includes LinkedIn-bearing rows

- **Why:** `tam_chain.py:61-67` builds `chain_rows` only from domain-bearing TAM rows — LinkedIn-only accounts silently vanish from the automated Stage-1→2 handoff (audit G1).
- **Files / functions / changes**
  - `backend/enrichment/tam_chain.py` `create_chained_enrichment_job` (33-138):
    - Row build (61-65): keep domain rows as-is; **add** rows without domain but with `linkedin_url` → `{"domain": "", "company_linkedin": row["linkedin_url"], "company_name": row.get("name") or ""}`.
    - Dedupe (69-71): domain rows via existing `dedupe_rows_by_domain`; LI rows deduped by normalized URL; cross-set: drop an LI row whose URL normalizes onto a domain row's already-known company? — **no** (no cross-key available); dedupe within LI set only. `chained_total = len(domain_rows) + len(li_rows)`.
    - `chain_skipped` reason now fires only when **neither** domain nor LinkedIn exists (`"no_companies_with_identifier"`; keep old reason string for the empty-both case? — the old literal `no_companies_with_domain` is returned to API consumers; **keep the old string when both-empty** to avoid breaking any caller, add nothing).
    - Runner invocation (110-128): pass `company_linkedin_col="company_linkedin"` — **the parameter already exists** on `_run_domain_enrich_job` (`routes.py:7333`) and rows route via E-010 branch 1 unchanged.
  - `backend/enrichment/routes.py` `_run_domain_enrich_job` (7356+): no signature change needed; confirm `dedupe_by_domain` handling when some rows lack domains (dedupe helper already tolerates empty domains — verify in `identifier_utils.dedupe_rows_by_domain`; if it drops empties, pre-split like tam_chain does — likely the tam_chain approach suffices since it pre-dedupes).
- **Dependencies:** none (works today without PR-2.1 — branch-1 routing already handles company-URL rows with empty domain).
- **Tests:** extend `tests/test_tam_flow.py` chain block (existing 830-918): fixture with 2 domain + 2 LI-only + 1 name-only companies → chained job `total=4`, rows route 2×domain + 2×company-URL; both-empty fixture → old skip reason preserved.
- **Rollback:** revert (small, isolated). No schema/UI impact.
- **Acceptance:** a live TAM run with mixed rows (owner-approved, ~50 credits exposure optional) chains **every** company carrying domain or LinkedIn; CSV row count == chained_total.
- **Effort:** 1.5 pd (P50 1 / P90 2).

### PR-2.4 — Discovery-limit transparency + bounded knobs

- **Why:** QE ≤2 pages×20, LO ≤100 rows/domain, waterfall `max_results` 3/5/10, contacts pool 12 — none surfaced; CSVs just quietly stop (audit G7, E-020/E-021/E-034).
- **Files / functions / changes**
  - **Markers (read-path, zero billing change):**
    - `backend/enrichment/pipeline.py` `_quickenrich_domain_discovery` (~3323-3360): when the page loop stops because `page == max_pages` **and** the last page was full, return metadata `qe_capped=true`; `_leadocean_domain_discovery` (~3475): same when `rows_fetched == _DISCOVERY_MAX_ROWS` and the last page was full.
    - `backend/enrichment/list_builder.py` `_enrich_single_domain` + company orchestrator: propagate to row as trailing column `discovery_capped` ∈ `"" | "quickenrich" | "leadocean" | "waterfall"` (waterfall marker when returned persons == `max_decision_makers` and provider reported more-available/unknown — conservative: only when Blitz prepass explicitly had more pages).
    - Job counter `discovery_capped_rows` (guarded-ALTER jobs column, `title_gate_dropped` pattern).
  - **Knob (opt-in, env-bounded):**
    - `ProviderToggleRequest` + UI: `dataset_max_pages: Optional[int]` (1–10, default None=env `QUICKENRICH_DATASET_MAX_PAGES`=2). Thread: `routes.py` → `_run_domain_enrich_job` → `run_domain_enrichment` → `_quickenrich_domain_discovery(max_pages=…)`; clamp `min(request, env_ceiling_QUICKENRICH_DATASET_MAX_PAGES_CEIL=10)`. Cost note in UI helper: "each extra page can return up to 20 more contacts/domain (credits per returned employee-with-email)".
    - LO `LEADOCEAN_DISCOVERY_MAX_ROWS` stays env-only (25M/mo search window is the scarcer quota — per-request loosening is a later decision).
  - **UI:** TAM runs table (frontend 5131-5167): render `capped` (already in run summary, E-017) as a ⚠ chip "more companies available — raise Max Companies"; Upload Domains helper under "Max Decision Makers" listing the three caps verbatim.
- **Dependencies:** PR-1.2 pattern familiarity (trailing-column + counter mechanics) but no hard dep.
- **Tests:** unit — mocked QE pager returning full pages → `discovery_capped="quickenrich"` + counter; `dataset_max_pages=3` honored + clamped at ceiling; LO row-cap marker; waterfall marker only on explicit more-available.
- **Rollback:** revert; markers are additive columns/counter; knob default preserves behavior.
- **Acceptance:** fixture with a mocked 200-person domain at default settings returns 40 + `discovery_capped="quickenrich"`; same fixture at `dataset_max_pages=3` returns 60. Job payload carries the counter. TAM UI shows the ⚠ chip on a `capped=true` fixture run.
- **Effort:** 2.5 pd (P50 2 / P90 3.5).

### PR-2.5 — Person-location filter (Blitz-leg only, honest caveats)

- **Files / changes:** `ProviderToggleRequest` + StartJobRequest: `person_location_country: Optional[str]` (ISO-2 or name, normalized once); `_titles_to_cascade` (`routes.py`) injects `location: [normalized]` into each cascade tier when set (the `location` key already exists in tier shape, `blitz_client.py:378-406` — today always `["WORLD"]`); UI field with chip: "Applies to Blitz searches only — internal DB / QuickEnrich / GetLeads legs match on their own data and may return other locations" (E-033). LO/QE/GL legs unchanged.
- **Tests:** cascade-shape unit test (location injected, `["WORLD"]` when absent); regression that default requests produce byte-identical cascades.
- **Rollback:** revert; optional field.
- **Acceptance:** fixture cascade carries the country; default flow unchanged (test-asserted).
- **Effort:** 1 pd (P50 0.5 / P90 1.5).

### PR-2.6 — People-page company mode (thin frontend over existing engine)

- **Why:** audit G4 / brief Q-2 — `/#/people` cannot scope by company. **Zero new backend**: the mode builds a Flow-1 job.
- **Files / changes** — `frontend/index.html` only:
  - `#page-people` gains a collapsible "Scope to companies" panel: textarea for domain/LinkedIn-URL/name-per-line paste (or file) + the existing role input doubles as `titles` + max-DMs select; submit → client-side CSV synthesis (`domain,company_linkedin,company_name` columns by line-shape detection — reuse `_is_company_linkedin_url` logic ported to JS or a simple `/company/` substring test) → `POST /api/enrichment/upload` → `POST /flows/domain-enrich` (`domain_col="domain"`, `company_linkedin_col="company_linkedin"`, `company_name_col="company_name"`, `titles`, `strict_titles` from a new checkbox default **checked** per Q-5) → redirect card "Job started — view on Jobs page" with live mini-progress (existing SSE util).
  - Banner copy: distinguishes the two modes ("Search the database" vs "Enrich a company list").
- **Dependencies:** PR-2.1 (name column usable); benefits from PR-2.3 (operators coming from TAM can also just use the chain).
- **Tests:** pinned `frontend/tests/test_people_company_mode.py`: panel exists, submit handler present, checkbox default checked; unit-ish assertion that the CSV-synthesis JS emits the 3 columns (extract function to a testable `window.__peopleCsvBuilder`).
- **Rollback:** `.bak` restore / revert; frontend-only.
- **Acceptance:** paste of 10 mixed identifiers → one job on Jobs page processing all 10 (contingent on PR-2.1); role filter and strict toggle visibly carried (job `cascade_config` inspection).
- **Effort:** 2 pd (P50 1.5 / P90 3).

---

## 5. Phase 3 — efficiency & scale (after ≥1 week of Phase-1 production validation; ≈6.5 pd)

| PR | Scope (files/functions) | Deps | Tests | Rollback | Acceptance | Effort |
|---|---|---|---|---|---|---|
| **PR-3.1** Persist positive verification | contacts-api `/opt/contacts_api` (own repo+skill, separate deploy): accept `verification_status ∈ valid\|catch_all` on `/v1/persons/upsert` (server currently ignores it — E-026); then `contacts_writer.py` upsert body (:528-536) sends `verification_status` derived from `mailtester_code`. | contacts-api release first | contacts-api test + writer unit (body assertion); E2E: verified email → DB row `is_verified=true` | two-sided revert (writer first) | 100-verify fixture → all `valid_ok` rows persisted; rerun cascade hits stored-verified without mailtester call | 3 pd |
| **PR-3.2** Shared verify cache for the enrichment gates | `pipeline.py`/`list_builder.py` contacts-gates consult `verify_results` before `mailtester_client.verify_email` (policy-matched read, 30d TTL) — cuts the 181K/7d inline volume | PR-1.1 table, PR-3.1 optional | gate tests: cached hit → zero mailtester calls; miss → normal path | env `ENABLE_VERIFY_CACHE_FOR_GATES=false` | same fixture rerun: provider_call_log mailtester delta 0 | 1 pd |
| **PR-3.3** GL `matched_people` semantics | `tam_legs.py` flatten (:170-196): `matched_people_source` column (`blitz`\|`getleads_synthetic`); TAM CSV columns + frontend table header | none | extend `test_tam_flow.py` GL fixtures | revert | CSV column present; sorting note in UI helper | 1 pd |
| **PR-3.4** Verify throughput >10 RPS | capacity check on `/opt/mailtester-proxy`; then raise `_RATE_LIMIT_RPS` via env `MAILTESTER_RATE_LIMIT_RPS` (make it env-tunable in `mailtester_client.py:32`) + semaphore ceiling | proxy owner confirmation | soak: 2,000 emails < 4 min at approved RPS, zero 5xx | env back to 10 | documented soak numbers | 1.5 pd |
| **PR-3.5** Project tags (decision pending audit Q-3) | `jobs.client_tag` guarded-ALTER + Jobs-page filter; **build only if owner confirms** | owner decision | pinned UI + store tests | revert | tag filter round-trips | 1.5 pd |

---

## 6. Cross-cutting rollout & safety procedure (applies to every PR)

**Before merge:** branch `feat/<pr-name>` off `master`; `pytest --collect-only -q | tail -3` sanity (~1013 collected); full suite green except known ~7 pre-existing failures; frontend pinned tests green.

**Before deploy (per the operating skill):**
```bash
sqlite3 backend/data/jobs.db "SELECT job_type,COUNT(*) FROM jobs WHERE status='running' GROUP BY job_type;"
# if any running: cancel via API and WAIT for status='cancelled' — never restart over live jobs
git tag pre-<pr-name>-$(date +%Y-%m-%d)
# risky PRs (1.1, 1.2, 2.1, 2.2): cp backend/data/jobs.db backend/data/jobs.db.backup.$(date +%Y%m%d_%H%M%S)
```

**Deploy:** merge → `sudo systemctl restart lead-generation-platform.service` → post-deploy checklist (`/api/health` ok, service active, port 8765, frontend loads, 60s clean logs).

**Rollback ladder (per PR, cheapest first):** env kill-switch → `git revert` + restart → restore `frontend/index.html.bak_*` (UI PRs) → tag checkout for worst case. DB artifacts (new columns/tables) are additive and safe to leave in place; never restore a DB snapshot for a schema-additive PR unless data corruption is suspected.

**Canary discipline:** each Phase-1/2 PR ships dark or flag-off; flip the flag on prod with one small real job the owner initiates; watch `journalctl -u lead-generation-platform.service -f` + `provider_call_log` deltas for one hour before general announcement.

---

## 7. Verification / test plan summary

- **Per-PR unit/pinned tests** as specified in each PR block (estimated +120 cases across all PRs).
- **Contract fixtures (repo-checked, non-billable):** the audit's edge-case matrix as CSV fixtures — LI-only / name-only (mocked resolver) / mixed-conflict / invalid LinkedIn URL / rebrand redirect / two-same-name companies / zero-match company / >40-person company (QE cap) / duplicate-person-across-providers / accept-all domain / known-dead address / rerun-same-input.
- **Authorized paid probes (owner sign-off, bounded):** one 50-company Blitz TAM probe (~50 FUP records, ~10M headroom on the 15M/mo meter at 4.93M used, E-029); one blitz name-resolver batch of 25 names (~5 credits) for PR-2.2's live-quality gate; ~100 self-hosted mailtester verifications for PR-1.1 canary (no vendor cost).
- **Go/no-go gates:** Phase 1 → PR-1.1 acceptance 1–4 + one owner-run canary; Phase 2 → PR-2.1 byte-parity diff (domain-only fixture) + resolver zero-wrong-join QA; Phase 3 → PR-3.1 two-sided E2E. **Stop condition for any phase:** a `VALID_PROVIDERS` parity test failure, writer idempotency regression, or resume/partial-download regression — these guard the production invariants.

---

## 8. Effort summary (reconciled; audit items → PRs)

| Audit item (old ID) | PR | P50 | P90 |
|---|---|---|---|
| P0-5 key rotation | PR-0.1 | 0.5 | 0.5 |
| P1-6 docs sync | PR-0.2 | 0.5 | 0.5 |
| P0-1 verify job | PR-1.1 | 4.0 | 5.5 |
| (new, from review directive) catch-all retention | PR-1.2 | 3.0 | 4.5 |
| P0-4 verify UI | PR-1.3 | 3.0 | 4.5 |
| P1-1 rollups | PR-1.4 | 1.5 | 2.5 |
| P0-2 flexible intake | PR-2.1 | 4.0 | 5.5 |
| (new, from review directive) name resolver + ambiguity | PR-2.2 | 4.0 | 5.5 |
| P0-3 TAM chain LI rows | PR-2.3 | 1.0 | 2.0 |
| P1-2 discovery caps | PR-2.4 | 2.0 | 3.5 |
| P1-3 person location | PR-2.5 | 0.5 | 1.5 |
| G4 People-page company mode | PR-2.6 | 1.5 | 3.0 |
| P1-4 persist verification | PR-3.1 | 2.5 | 3.5 |
| P1-7 gate cache | PR-3.2 | 1.0 | 1.5 |
| P1-5 GL matched_people | PR-3.3 | 1.0 | 1.0 |
| (new) verify throughput | PR-3.4 | 1.0 | 2.0 |
| P2-4 project tags (conditional) | PR-3.5 | 1.5 | 1.5 |
| **Phase 0 total** | | **1.0** | **1.0** |
| **Phase 1 total (+15%)** | | **13.5** | **19.6** |
| **Phase 2 total (+15%)** | | **16.1** | **29.1** |
| **Phase 3 total (+15%, excl. 3.5)** | | **6.3** | **8.9** |
| **All phases (excl. conditional 3.5)** | | **36.9** | **58.6** |

*The audit's three inconsistent figures ("≈8–14 pd P0", "≈2–3 weeks", "12.5–19 pd") are superseded by this table. Like-for-like: the audit's P0 scope (old P0-1..P0-5) = PR-0.1 + PR-1.1 + PR-1.2 + PR-2.1 + PR-2.3 + the UI halves of PR-1.3/2.6 ≈ 14.5–22 pd — the earlier numbers under-counted the flexible-intake and retention work the review directives added.*

---

## 9. Decisions adopted, decisions still open

**Adopted defaults (from the audit's §13, now binding for this plan):** Q-1 → `source_type='verify'` on `job_type='enrichment'` (no new job_type literal). Q-5 → strict titles stay ON everywhere (no change). Q-2 → People page gains the thin company mode (PR-2.6), Upload Domains remains the bulk surface.

**Still open (do not block Phases 0–2):**
1. **Q-4 catch-all sendability policy** — PR-1.1/1.3 default `allow_catch_all=false` (preserves today's posture); the retention columns (PR-1.2) make the volume visible — decide the default after 2 weeks of real `catch_all_retained` numbers.
2. **Q-3 client separation** — gates PR-3.5 only.
3. **Contacts-API name-search availability** — PR-2.2 Source 1 needs a shape check on `/v1/companies/search?q=`; if absent, resolver ships Blitz-gated-off + `none`-heavy until the contacts-api adds it (smallest access request: one authenticated curl from the owner).
4. **Proxy capacity for >10 RPS verification** — PR-3.4 blocked on `/opt/mailtester-proxy` owner confirmation.

---

## 10. Explicitly out of scope (unchanged from audit §14)

Project-shell UI (Option B), directory scraping / Apollo / DiscoLike / signal-playbook automation, TAM free pre-count bridging, cross-identifier corroboration lookups beyond the conflict flag, and any change to the cascade order or provider enablement. The waterfall itself is never duplicated or reordered by this plan.

---

*End of plan. Prepared 2026-11-10 against commit `384e33b`. No code has been changed, no PRs opened, no deploys performed.*
