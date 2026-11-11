# TAM / People Finder / Verification — Implementation Progress

**Last updated:** 11 November 2026 (FINAL — full 3-step workflow implemented; release branch ready; deployment gated on one operator action)
**Plan:** `docs/HYPERKE_TAM_IMPLEMENTATION_PLAN.md` · **Audit:** `docs/HYPERKE_TAM_PEOPLE_VERIFICATION_AUDIT_2026-11-10.md`
**Companion docs:** `docs/TAM_3_STEP_OPERATOR_SOP.md` (operator guide) · `docs/TAM_RELEASE_VALIDATION.md` (test matrix, DOM evidence, deploy+rollback)

## 0. Release status (FINAL)

**Release branch `feat/tam-3step-release`** (from master @ `384e33b`; 17 commits; 32 files, +4,211/−77; **512 tests green — 364 backend + 148 frontend, 0 failed**; 0 secrets in tree):

| Piece | Status | Where |
|---|---|---|
| PR-0.1 MailTester key fallback removal | **implemented + merged into release** | `a3a5ef1` |
| PR-2.3 TAM chaining incl. LinkedIn-only + restart parity + hardening | **implemented + merged** | `e6171f2` `2cf37a5` `b30ab95` `915a951` |
| PR-0.2 operator truth sync | **implemented + merged; UI already LIVE** | `c990394` |
| Quickenrich test-suite hang (pre-existing) | **ROOT-CAUSED + FIXED** (TestClient lifespan deadlock; 18 tests, 2.2 s) | `77fcec9` |
| PR-1.2 catch-all retention | **implemented** (column, counter, badge, 12 gate sites) | `fad5d91` |
| PR-1.1 standalone verifier backend | **implemented** (engine, API, cache, runner, CSV) | `0957e81` |
| PR-1.4 rollups | **implemented** (verification_rollup on job GET, UI chips) | `e2e5474` |
| PR-1.3 Verify Emails UI | **implemented + LIVE** (page, nav, from-job button) | `e2e5474` |
| PR-2.1 flexible identifiers | **implemented** (LinkedIn/name-only CSVs; explicit skip reasons) | `42ddd46` |
| PR-2.2 safe name resolver | **implemented, ENABLE_NAME_RESOLVER=false by default** (ambiguous NEVER autojoins) | `b89d591` |
| PR-2.4/2.5 depth + location knobs | **implemented** (bounded; Blitz-only caveat surfaced) | `e9f2633` |
| PR-2.6 People-page company mode | **implemented + LIVE** (rides Flow-1; internal search preserved) | `f6becce` |
| Phase-3 items (PR-3.x) | **deliberately not started** (efficiency/scale, post-validation) | plan §Phase 3 |

**Deployment state:** the FRONTEND of all three stages is already served live (nginx no-cache reads the working tree — DOM-verified, §3 of the validation doc). The BACKEND halves run after the next service restart.

### ⛔ THE ONE REQUIRED OPERATOR ACTION (deployment gate)

**9 enrichment jobs were running at final validation.** A service restart mid-job can kill live work (the DB is the only cross-worker job-state source), so I did NOT restart. Required sequence in a maintenance/drain window:

```bash
# 1. Drain: let running jobs finish, or cancel each via the UI/API and WAIT
#    for status='cancelled' — never restart over live jobs.
sqlite3 -readonly backend/data/jobs.db \
  "SELECT job_type,status,COUNT(*) FROM jobs WHERE status IN ('running','starting') GROUP BY 1,2;"
# 2. Tag + snapshot, merge, deploy:
git tag pre-tam-3step-$(date +%F)
cp backend/data/jobs.db backend/data/jobs.db.backup.$(date +%Y%m%d_%H%M%S)
git checkout master && git merge --no-ff feat/tam-3step-release
cd backend && source venv/bin/activate && pytest enrichment/tests/ -q   # 364 expected
sudo systemctl restart lead-generation-platform.service
# 3. Health + canaries: docs/TAM_RELEASE_VALIDATION.md §6
# 4. MailTester key rotation (§4.3) — proxy-owner authority needed; revoke
#    the exposed key once the new one is confirmed, not after an arbitrary 48h.
```

### Config presence (verified, values not printed)

`MAILTESTER_API_KEY` ✓ present in `.env` (required since PR-0.1) · `MAILTESTER_ACCEPT_CODES=ok` ✓ · new env knobs (`ENABLE_CATCH_ALL_RETENTION` default on, `ENABLE_NAME_RESOLVER` default **off**, `ENABLE_NAME_RESOLVER_BLITZ` off, `QUICKENRICH_DATASET_MAX_PAGES_CEIL=10`, `VERIFY_RESULT_TTL_DAYS=30`, `NAME_RESOLVER_TTL_DAYS=14`) all default safely with NO `.env` change required.

### Corrections made to the original plan during implementation

1. PR-2.3's chain input rides `linkedin_url_col` (existing jobs column) — plan's `company_linkedin_col` was unpersisted and restart-unsafe; restart parity additionally required the atomic collision-safe persistence file (cycles 3–5).
2. The verify cache stores **raw verifier facts** keyed (email_norm, verifier) with policy applied at read — plan's policy-keyed schema dropped per review directive.
3. Bulk verification is **always a background job above 25 emails** (no synchronous large batches).
4. `providers.get_enabled_providers()` ignores env kill switches — all enablement truth uses `is_provider_enabled()` (found while building PR-0.2's live table).
5. The quickenrich-endpoint "hang" was a **test-harness deadlock** (TestClient entering the full app lifespan inside the portal), fixed in the suite — not a product bug.

### Known limitations (honest)

- Person-location filter is **Blitz-lane only** (UI says so); no cross-provider enforceable filter exists.
- Name resolution ships **off** (ENABLE_NAME_RESOLVER=false): name-only rows skip with `name_unresolved` until ops QA the resolver on real data and arms it.
- Provider caps remain (QE pages × 20, LO ≤100/domain, waterfall tiers) — surfaced via the depth knob + copy, not eliminated.
- "Not found" can mean cap-reached or no-data-in-source — the SOP states this explicitly.
- Phase-3 (verification persistence to contacts DB, gate cache reuse, GL matched_people semantics, >10 RPS verify throughput, project tags) intentionally deferred.

---

(Sections 1–9 below are the historical per-cycle record: baseline, PR-0.1, PR-2.3 incl. cycles 3–5, deployment checklist §8, PR-0.2 §9 — kept for audit trail.)

---

## 1. Baseline

### 1.1 Repository state at start

| Item | Value |
|---|---|
| Starting branch | `feat/moltsets-cascade` (== `master`) |
| Starting commit | `384e33b` ("chore(screts): …", 2026-11-08) |
| Worktree | Tracked files **clean**; untracked only (docs brief/audit/plan, `archive/`, `backend/.tables`, `backend/header`, `frontend/*.bak_*`) — none touched |
| Remote | `origin` = github.com/arjavjain21/lead-generation-platform (not pushed) |
| Implementation branch | **`fix/mailtester-key-fallback`** (created from `384e33b`) |
| PR-0.1 commit | **`a3a5ef1`** |
| AGENTS.md | Does not exist; `CLAUDE.md` + the `lead-generation-platform-workflow` skill govern |
| Running production jobs at start | **None** (`running`/`starting` count = 0 across all job types) |
| Services | `lead-generation-platform.service` active; not restarted, not touched |

### 1.2 Relevant existing functionality (verified, not assumed)

- `backend/enrichment/mailtester_client.py` — the only SMTP-grade verifier; called inline by the enrichment gates; `verify_email()` returns `{valid, code, message, email}`; fail-open on outage via `RuntimeError`.
- `_get_api_key()` (was lines 90–94) carried the hardcoded fallback `os.getenv("MAILTESTER_API_KEY", "") or "<40-char hex>"`, making the existing `if not key: raise` branch dead code.
- Sibling house pattern confirmed: `blitz_client.py:110-114` and `phone_enrichment/client.py:50-54` use exactly `getenv → if not key: raise RuntimeError → return key`. PR-0.1 brings mailtester into that shape.
- Production `.env` **contains** `MAILTESTER_API_KEY` (presence verified by name only; value never printed). Runtime check: the env value passes the new getter (48 chars, prefix `b0b4***`).
- `backend/conftest.py` **deliberately scrubs** provider API keys from the test env (anti-paid-call-leak, comment block at lines ~63-77) — so the fix could NOT be "set the key in conftest"; per-suite `monkeypatch.setenv` is the established pattern (blitz/moltsets/quickenrich suites).
- Two existing suites call the **real** `verify_email` through `httpx.MockTransport` and were silently relying on the hardcoded fallback: `test_mailtester_timeout.py` (4 tests) and `test_enrich_throughput_hardening.py` (14 tests). All other mailtester-touching suites mock `verify_email` itself and are unaffected.

### 1.3 Differences from the audit / implementation plan

| Plan claim | Reality found | Disposition |
|---|---|---|
| "delete the now-dead `if not key` branch" | Wrong direction — the branch must **stay** (it becomes the live guard); only the `or "<literal>"` fallback goes. Sibling clients use this exact shape. | Implemented correctly; plan wording superseded |
| Tests live in `backend/enrichment/tests/` per the plan | Confirmed; conftest at `backend/conftest.py` with env-snapshot restore + key scrub | Followed house pattern (per-suite setenv, not conftest setdefault) |
| Plan didn't anticipate existing tests depending on the fallback | 2 suites did (18 tests) | Fixed via one `monkeypatch.setenv` line in each autouse fixture |
| Plan's PR-2.3 "depends on PR-2.1 pattern" | **Independently implementable** — `company_linkedin_col` param already exists on `_run_domain_enrich_job` (`routes.py:7333`) and routing branch 1 (`list_builder.py:2635`) already handles company-URL rows with empty domain | Recorded; PR-2.3 can advance whenever convenient |
| Plan PR-1.1 `verify_results` keyed `(email_norm, allow_catch_all)` | Per review directive, **store raw verification results independent of sendability policy** (PK `email_norm`; policy applied at read time). Cleaner and matches "catch-all is a distinct classification" | Plan correction logged for PR-1.1 |
| Plan PR-1.1 "≤200 emails synchronous" | Per review directive, **bulk verification should run as background jobs** to avoid HTTP timeouts — lower or drop the sync threshold | Plan correction logged for PR-1.1 |

---

## 2. PR-0.1 implementation — remove hardcoded MailTester API key

**Commit `a3a5ef1` on `fix/mailtester-key-fallback` · 5 files · +243/−1 · no production behavior change while `.env` is present.**

### 2.1 Changes made

| File | Function / area | Change | Why |
|---|---|---|---|
| `backend/enrichment/mailtester_client.py` | module docstring | Added a "Configuration" paragraph: `MAILTESTER_API_KEY` required, no fallback, fail-open semantics on missing key | Document the new config contract at the module boundary |
| `backend/enrichment/mailtester_client.py` | `_get_api_key()` | Removed `or "<40-char hex>"` fallback; added `.strip()` (whitespace-only now raises); added docstring explaining the fail-open propagation; the previously-dead `if not key: raise RuntimeError` is now live | The security fix itself; aligns with blitz/phone client pattern; strip guards against trailing-space `.env` values |
| `backend/enrichment/tests/test_mailtester_timeout.py` | `_reset_module_state` (autouse fixture) | `monkeypatch.setenv("MAILTESTER_API_KEY", "unit-test-mailtester-key")` + comment | Suite calls real `verify_email`; was silently using the hardcoded fallback; conftest scrubs keys by design |
| `backend/enrichment/tests/test_enrich_throughput_hardening.py` | `_reset_mailtester_state` (autouse fixture) | Same one-line addition + comment | Same reason |
| `backend/enrichment/tests/test_mailtester_key.py` | **new file** (10 tests) | Missing/empty/whitespace env → `RuntimeError` from `_get_api_key`; `verify_email` raises **before any HTTP call** (call-count asserted 0); the raise is exactly `RuntimeError` so caller fail-open contract holds (a different exception type would invert to fail-closed); env key travels only as the proxy `key` query param; key never echoed in results; source guards: no 40-char hex literal, no `or "<literal>"` fallback shape in `_get_api_key` | Pin the config contract and prevent regression of the secret |
| `CLAUDE.md` | env block (`# Required`) | Added `MAILTESTER_API_KEY=<mailtester-proxy-key>` with a note that a missing key disables verification (emails pass unverified) | Configuration documentation for operators |

### 2.2 Behavior before → after

| Scenario | Before | After |
|---|---|---|
| `.env` has the key (production today) | Identical | **Identical** — same key sourced from env; `.strip()` is a no-op on the current value |
| Env var missing | Silently used the committed key; verification worked; secret shipped in source | `verify_email` raises `RuntimeError` before any HTTP call → callers fail **open** (email accepted unverified, `mailtester_code="unavailable"`). **This is the documented fail-open risk: deploying without the env var silently disables verification** — it cannot drop emails or break jobs |
| Env var empty / whitespace | Empty → fell back to committed key; whitespace → sent as key (broken proxy auth → fail-open anyway) | Both raise → clean fail-open, no doomed requests |
| Timeout / 5xx / breaker behavior | retry → fail-open | **Unchanged** (timeout suite re-run green) |
| Key in logs | Never logged (client errors log status code only; httpx exception `str()` carries no query params; no `resp.url` logging exists in the module) | Unchanged; new tests additionally assert the key never appears in returned results |

### 2.3 Secret exposure notes

- The literal existed in **exactly one** source location (`mailtester_client.py:91`) — verified by repo-wide grep (only other hits: the audit doc's redacted mention, and the key-name references).
- The committed value is **burned regardless of this fix** — it remains in git history. Removing it from HEAD stops future exposure; it does not un-leak it. Rotation (ops, not code) is therefore mandatory and is sequenced in §4.3.
- The production `.env` currently holds the **same value** as the burned literal (verified by matching prefix/length without printing) — rotation must update `.env` in the same operation.

---

## 3. Validation

### 3.1 Tests executed (all post-change; baseline captured pre-change on the same 9 files)

| Suite | Baseline (pre) | Post-change |
|---|---|---|
| `test_mailtester_key.py` (new) | — | **10 passed** |
| `test_mailtester_timeout.py` | 4 passed | **4 passed** |
| `test_enrich_throughput_hardening.py` | 14 passed | **14 passed** |
| `test_company_fallbacks.py` | 19 passed (+12 subtests) | **19 passed (+12 subtests)** |
| `test_cascade_collector_wiring.py` | 22 passed | **22 passed** |
| `test_cascade_fixes.py` | 9 passed | **9 passed** |
| `test_moltsets_client.py` | 24 passed | **24 passed** |
| `test_website_only_mode.py` | 9 passed | **9 passed** |
| `test_domain_only_api_cascade.py` | 5 passed | **5 passed** |
| `test_stored_phones_retrieval_20261028.py` | 9 passed | **9 passed** |
| **Total** | 115 passed | **125 passed, 0 failed, 0 regressions** |

### 3.2 Known baseline failures / environment issues (pre-existing, not caused by this PR)

- `enrichment/tests/test_quickenrich_endpoint.py` **hangs** during execution at `test_400_when_no_input` (2nd of 18 tests; collection works; killed at 90–150 s on three runs). Pre-existing on this box — matches the project memory note "~4 test files hang on this box". Unrelated to mailtester (it is a request-validation test). Not fixed here; flagged for the next PR's baseline.
- The ~7 known pre-existing suite failures noted in the skill were not encountered in the targeted scope above.
- Full-suite run deliberately not executed for this PR (hanging files + 3–5 min runtime); targeted scope covers every file importing or mocking `mailtester_client` (verified by grep).

### 3.3 Security checks

- Diff scan: **no** added 40-char hex literals; **no** added inline key assignments (automated grep on the diff, output shown in trace).
- Commit removes the secret from HEAD; rotation tracking in this report.
- New tests assert: no request attempted without a key; key only in the proxy query param; key absent from result dicts.
- Log-hygiene audit of the module: existing logs emit status codes and exception messages only — httpx exception `str()` does not embed query params; no change needed.

### 3.4 Revert safety

Single-commit revert (`git revert a3a5ef1`) restores the fallback verbatim; no schema, no API, no behavior surface touched while `.env` carries the key. Independent of every other planned PR.

---

## 4. Risks and dependencies

### 4.1 Production configuration requirements

- `MAILTESTER_API_KEY` **must be present** in `backend/.env` at deploy time (it is today). Deploying this commit to a host without the var silently disables verification (fail-open) — monitor via the existing `mailtester_code="unavailable"` volume or `/stats/email-quality`.
- No other config interaction (`MAILTESTER_ACCEPT_CODES`, rate limits, breaker, cache untouched).

### 4.2 MailTester proxy coordination

- The proxy (`validation.hyperke.org` → `/opt/mailtester-proxy`, separate codebase) is **not modified** by this PR and keeps accepting the current key until rotated.

### 4.3 Key rotation sequence (operational — NOT performed in this PR, per instructions)

1. Owner generates a new key on the proxy side (`/opt/mailtester-proxy` config).
2. Add the new value to `backend/.env` (`MAILTESTER_API_KEY=…`) on the VPS — keep the old value valid during the window.
3. Deploy/verify: `curl -s http://localhost:8765/api/health` + one manual `POST /api/enrichment/quickenrich {"domain":"<known-domain>","validate_emails":true}` → `summary.validator="mailtester"` with `validated ≥ 0` and zero `unavailable`.
4. Revoke the old (burned) key at the proxy **only after** the 48 h rollback window closes.
5. Optionally scrub history (BFG/filter-repo) — large, disruptive; recommended only if the repo will be shared beyond trusted CI/VPS. Rotation alone neutralizes the leak.

### 4.4 Rollback considerations

- Code: `git revert a3a5ef1` (no service-data coupling).
- Do **not** revoke the old proxy key before the rollback window closes, or a revert would land on a dead key (verification would fail-open platform-wide).

### 4.5 Unresolved issues

- Pre-existing hang in `test_quickenrich_endpoint.py::test_400_when_no_input` — needs its own RCA before PR-1.1 relies on that suite (PR-1.1 extends it).
- None blocking PR-0.1.

---

## 5. Remaining implementation (plan §2–§5 PRs)

| PR | Title | Status | Depends on | Priority | User-visible improvement | Plan correction needed |
|---|---|---|---|---|---|---|
| PR-0.1 | MailTester key fallback removal | **Complete** (`a3a5ef1`) | — | P0 | None (security posture) | Done; plan's "delete dead branch" wording was inverted — implemented per house pattern |
| PR-0.2 | Docs/truth sync (`/flows/help`, Help page, banner, jobs filter) | **Complete, awaiting review** (`c990394` on `fix/operator-truth-sync`) | — | P0 | Operators see the real 8-active-tier cascade + 30M count | §9: verified against live system; found `get_enabled_providers()` ignores env kill switches (helper uses `is_provider_enabled` per tier); home page had a second stale 6-source list |
| PR-1.1 | Standalone verification job backend | Not started | PR-0.1 ✓ | P0 | New "verify a list" capability; catch-all retained as distinct status | **Yes:** (a) cache raw results policy-independent (PK `email_norm`, policy applied at read); (b) bulk = background jobs (drop/lower the ≤200 sync window); (c) catch-all never marked individually verified |
| PR-1.2 | Catch-all retention in enrichment CSVs (`dm_email_catch_all` + counter) | Not started | — | P0 | Lost catch-all addresses become visible/retained | — |
| PR-1.3 | Verification tab UI + job-card "verify these emails" | Not started | PR-1.1 | P0 | Self-serve Stage 3 on the website | — |
| PR-1.4 | Verification/retention rollups on job payloads & cards | Not started | PR-1.1, PR-1.2 | P1 | Counts reconcile on screen | — |
| PR-2.1 | Identifier-flexible Flow 1 (domain col optional) | Not started | — (resolver integration waits PR-2.2) | P0 | One mixed-identifier list, one job | — |
| PR-2.2 | Company-name resolver w/ ambiguity handling | Not started | PR-2.1 call-sites; contacts-API `q=` shape check | P1 | Name-only rows usable; ambiguous names flagged, **never** auto-joined | — |
| PR-2.3 | TAM chain includes LinkedIn-bearing rows | **Complete + hardened** (`e6171f2` + `2cf37a5` + `b30ab95` — **GO**, merge all three as one unit) | **None — verified independently implementable** (param exists at `routes.py:7333`; routing branch handles it) | P0 | TAM→people handoff stops dropping LinkedIn-only accounts | Plan's `company_linkedin_col` approach replaced by persisted `linkedin_url_col` (resume-safe); cycle-3 added the restart-parity CSV; cycle-4 made it atomic/collision-safe/fail-closed; see §2b–§2d |
| PR-2.4 | Discovery-limit markers + `dataset_max_pages` knob | Not started | — | P1 | Coverage caps visible; deeper QE retrieval opt-in | — |
| PR-2.5 | Person-location filter (Blitz-leg, caveated) | Not started | — | P2 | Location targeting without implying global provider coverage | UI must carry the "Blitz-only" caveat chip |
| PR-2.6 | People-page company mode (thin frontend) | Not started | PR-2.1 | P1 | `/#/people` becomes a Stage-2 entry point | — |
| PR-3.1 | Persist positive verification (contacts-api change) | Not started | contacts-api release | P2 | Reruns stop re-verifying | — |
| PR-3.2 | Shared verify cache for enrichment gates | Not started | PR-1.1 | P2 | Cuts the 181K/7d inline MailTester volume | — |
| PR-3.3 | GL `matched_people` semantics column | Not started | — | P2 | Honest sorting of TAM CSV | — |
| PR-3.4 | Verification throughput >10 RPS | Blocked (proxy owner capacity confirmation) | — | P2 | 20K+ chunks in minutes | — |
| PR-3.5 | Project tags | Blocked (owner decision Q-3) | — | P3 | Client separation | — |

---

## 2b. PR-2.3 implementation — TAM chaining for LinkedIn-only companies

**Branch `feat/tam-chain-linkedin-rows` (independently based on `master` @ `384e33b` — deliberately NOT stacked on PR-0.1) · commit `e6171f2` · 2 files · +297/−7.**

### 2b.1 Review of PR-0.1 (performed before starting PR-2.3)

Independent verification of `a3a5ef1`: commit/parent (`384e33b`)/branch containment ✓; the removed literal is absent from the entire working tree and present in exactly 2 history commits (introduced `6be1700`, removed `a3a5ef1`) → **rotation remains mandatory ops work**; `_get_api_key`'s raise sits at the `params` construction (line 174) BEFORE the retry loop (line 179) — zero HTTP attempts, zero rate-limit sleeps, propagates as `RuntimeError` (the caller fail-open signal; a different exception type would have inverted fail-open to fail-closed) ✓; `git apply --check --reverse` confirms `git revert a3a5ef1` applies cleanly ✓; touched suites re-run green (28/28) ✓; no credentials in tests or new docs ✓. **Verdict: PASS — no changes required before merge.** Non-blocking notes: the removal hunk inevitably shows the old literal in the commit diff (history retention is why rotation is mandatory); after deploy, monitor `mailtester_code="unavailable"` volume as the missing-env canary.

### 2b.2 Preflight findings specific to PR-2.3

| Plan/review claim | Verified reality | Disposition |
|---|---|---|
| "PR-2.3 may be independently implementable before PR-2.1" | **Confirmed.** `company_linkedin_col`/`linkedin_url_col` params already thread `_run_domain_enrich_job` → `run_domain_enrichment` → `process_row`; the `/company/` auto-detect branch (`list_builder.py:2614-2617`) handles company-URL rows with empty domain today | Implemented independently on a branch off `master` |
| Plan: "pass `company_linkedin_col` to the runner" | **Improved:** the `jobs` table has **no** `company_linkedin_col` column — a chain job using it would lose routing on `/restart` + auto-resume (they rebuild kwargs only from persisted job columns). The chain instead carries URLs in a `linkedin_url` column persisted as `linkedin_url_col` (existing jobs column, restored automatically at `routes.py:6771/6813`) | Plan deviation, strictly safer |
| Resume safety for blank-domain rows | **Pre-existing support:** `_resume_remaining_rows`'s M1 path (`routes.py:6396-6444`) matches blank-domain rows against index checkpoints; `domain_checkpoint_key` returns `""` for blanks and `process_row`'s checkpoint write skips empty keys (`list_builder.py:2912`) | No change needed; relied upon |
| Restart input recovery | Chained jobs have no upload CSV (by module design, "rows handed over in-memory"); `/restart` falls back to the **parent TAM job's output CSV**, which already contains the `linkedin_url` column — so LinkedIn rows are recovered with correct routing via the persisted `linkedin_url_col` | No file-writing added |
| `normalize_linkedin_url` preserves `www.` hosts | Found by the new tests: `www.linkedin.com/company/acme` ≠ `linkedin.com/company/acme` as a dedupe key — the initial cross-dedupe implementation failed its own regression test | Fixed chain-locally (`_canonical_company_url`); the shared normalizer was deliberately NOT changed (it feeds stored `linkedin_norm` comparisons) |

### 2b.3 Changes made

| File | Function / area | Change | Why |
|---|---|---|---|
| `backend/enrichment/tam_chain.py` | new `CHAIN_LINKEDIN_COL = "linkedin_url"` + `_canonical_company_url()` + `_company_url_from_tam_row()` | Canonicalize (`normalize_linkedin_url` minus `www.` host) and gate to company-style pages (`/company/`, `/school/`, `/organization/` via `list_builder._is_company_linkedin_url`) | Only company pages can drive the Flow-1 company path; www-vs-bare hosts must share one dedupe key |
| `backend/enrichment/tam_chain.py` | `create_chained_enrichment_job()` row build | After the unchanged domain-row build: collect canonical URLs already represented by domain rows (cross-dedupe set), then append first-wins-deduped LinkedIn rows `{"domain": "", "linkedin_url": <canonical>, "company_name": <name>}` | Requirement 3: LinkedIn-only TAM accounts enter the existing enrichment flow |
| same | `create_enrichment_job(...)` call | persist `linkedin_url_col=CHAIN_LINKEDIN_COL` | Requirement 7: restart/auto-resume restore company-URL routing automatically |
| same | `_run_domain_enrich_job(...)` invocation | pass `linkedin_url_col=CHAIN_LINKEDIN_COL` | The persisted column drives `process_row`'s `/company/` auto-detect → `_enrich_by_company_linkedin` (requirement 4 — the existing path, zero waterfall duplication) |
| same | skip literal + return payload | `chain_skipped` stays `no_companies_with_domain` when a run has neither identifier (legacy API/UI compatibility); return adds `chained_linkedin_rows`; log line now splits domain vs linkedin-only counts | Observability without breaking consumers |
| `backend/enrichment/tests/test_tam_flow.py` | `TestChainedEnrichmentJob` +6 tests, new `TestChainedRowRouting` class +4 tests | See §3b | Requirements 2/6 |

**Behavior preserved:** domain rows are byte-for-byte unchanged (no `linkedin_url` key injected — pinned by test); domain-only runs produce identical jobs to pre-PR; `dedupe_rows_by_domain` semantics untouched (blank-domain rows pass through by its documented design); no CSV schema change; `source_type='tam_chain'`, `job_type`, and status literals untouched.

### 2b.4 Known limitations (documented, accepted)

> **Superseded by §2c (cycle 3):** limitations 1 and 2 below were re-examined on review, found to be **real defects** (silent row drops, not just cosmetic over-counting), and **fixed** in `2cf37a5`. Items 3–4 stand unchanged.

1. ~~Restart totals can exceed `chained_total`…~~ **FIXED for all new chain jobs** (parity CSV). Remains true only for chain jobs created before the fix (see §2c.4).
2. ~~Rare resume double-enrichment edge…~~ **FIXED for all new chain jobs** (same mechanism).
3. `chained_linkedin_rows` is a new summary key — additive; the UI runs table does not yet display it (cosmetic, Phase-2 UI work).
4. LinkedIn-only rows enrich via the Blitz-first company waterfall; when their company page yields no people, the existing GetLeads DM rescue requires a **domain** to fire (`list_builder.py:3961` gates on `domain`) — LinkedIn-only rows therefore have slightly thinner fallback coverage than domain rows. Inherent to the existing path; not introduced by this PR.

---

## 2c. Cycle-3 review — restart/resume parity (PR-2.3)

### 2c.1 Row-for-row comparison: original chain selection vs `/restart` reconstruction

`/restart` (`routes.py:6529-6580`) resolves a job's input in this order: **(1)** `UPLOAD_DIR/{filename}.csv` if it exists → **(2)** walk ancestors for a job row with `output_path` → for chain jobs that is the **parent TAM job's 21-column CSV** (all companies, TAM-source order). Before cycle 3, the chain wrote no upload file, so every chain restart took path (2). Comparing the two row sets on a mixed fixture (TAM order `[A(dom), L1(li), B(dom), L2(li), Decoy(no identifier)]`; chain selection `[A, B, L1, L2]` — domains first, decoys and cross-duplicates excluded):

| Aspect | Original chain rows | Pre-fix `/restart` reconstruction (parent TAM CSV) | Post-fix (parity CSV, `2cf37a5`) |
|---|---|---|---|
| Row set | 4 selected rows | **6 rows** — both decoys re-admitted | **4 rows — identical set & order** |
| Index space | chain space (checkpoints written here) | **TAM-CSV space — misaligned** | chain space (checkpoints align) |
| `total` of resumed job | `chained_total − done` | **inflated** (includes skipped decoys) | exact |
| Duplicate enrichment | none | **possible** (cross-deduped same-company rows re-admitted; misindexed blank-domain rows re-processed) | none |

### 2c.2 Verified defect (was limitation §2b.4-1/2 — now fixed)

The decisive failure is **not** cosmetic: with misaligned index spaces, the M1 blank-domain rule (`routes.py:6438-6444`, `if i not in orig_indices`) checks TAM-CSV positions against chain-space checkpoint indices. On the fixture above, after the two domain rows completed (chain indices 0-1), TAM index 1 is the **not-yet-done LinkedIn row L1** — but `1 ∈ orig_indices`, so **L1 is silently treated as done and never enriched**, while the identifier-less Decoy leaks into the resumed job. Pinned verbatim by `test_legacy_restart_without_upload_file_misbehaves` (assertions: L1 absent from remaining, decoy present, L2 present). That test also proves the same fixture resolves perfectly through the parity file.

### 2c.3 The fix (small + isolated — `2cf37a5`)

`create_chained_enrichment_job` now persists its **exact deduped rows** (header `domain,linkedin_url,company_name`) to `UPLOAD_DIR/find_companies_contacts_{stamp}.csv` — the file `/restart` resolves **first**. Restart therefore reads the chain's own row set, order, and index space: resumed totals equal `chained_total − done`, domain rows filter by domain checkpoints, LinkedIn rows by correctly-aligned index checkpoints, decoys never enter. Best-effort by design: on write failure the chain still runs and restart falls back to the legacy parent-walk (logged warning). No schema, env, API, or waterfall changes. Restart routing for the LinkedIn rows works because the job row persists `linkedin_url_col` (restored automatically at `routes.py:6771/6813`).

### 2c.4 Remaining limitation (exact scope — do not overstate)

**Chain jobs created before this fix** (i.e., between `e6171f2` and `2cf37a5` in any deploy window — none exist in production since neither commit is deployed) have no parity file and restart via the legacy path with the defects above. Operational impact for such jobs: one `/restart` may skip up to one not-yet-done LinkedIn-only company per misaligned index and add skipped decoy rows. Recommended separate remediation (NOT needed now): none — deploying `2cf37a5` together with `e6171f2` (single deploy) means no such jobs can exist.

### 2c.5 Cycle-3 regression tests (+6, all in `test_tam_flow.py`)

| Test | Pins |
|---|---|
| `test_chain_writes_upload_csv_matching_runner_rows` | restart source == runner rows exactly (mixed + cross-deduped decoys); job total == file rows |
| `test_restart_parity_interrupted_mixed_job` | interrupted after both domain rows → resume returns **exactly** the two undone LinkedIn rows; no parent-CSV extras |
| `test_restart_parity_all_done_short_circuits` | all rows checkpointed → zero remaining (restart's all-done branch; no re-enrichment) |
| `test_restart_parity_no_progress_full_reprocess` | zero checkpoints → all 4 chain rows reprocess; never the TAM CSV's 6 |
| `test_upload_csv_write_failure_does_not_break_the_chain` | write failure degrades to legacy path; chain job still created |
| `test_legacy_restart_without_upload_file_misbehaves` | **documents the pre-fix defect** (L1 silently dropped, decoy leaked) AND proves the same fixture correct via the parity file |

### 2c.0 Fixture reconciliation (report accuracy fix, cycle 4)

Two distinct fixtures were used across §2b/§2c and earlier wording blurred their row counts. For the record:

| Fixture | TAM rows | Chain rows | Used by |
|---|---|---|---|
| **F5** (legacy-demo) | **5** — `[A(dom), L1(li), B(dom), L2(li), Decoy(no id)]` in TAM order | **4** — `[A, B, L1, L2]` | `test_legacy_restart_without_upload_file_misbehaves` and the §2c.1 comparison table |
| **F6** (parity-class `MIXED_COMPANIES`) | **6** — `[A(dom), B(dom+li), Decoy-Nameless, L1(li), L2(li), B-copy(li, cross-dup of B)]` | **4** — `[A, B, L1, L2]` | the five cycle-3 parity tests ("never the TAM CSV's 6" in `test_restart_parity_no_progress_full_reprocess` refers to F6's six TAM entries) |

Both fixtures chain to the same 4 rows; the differing TAM counts are intentional (F6 adds the cross-deduped duplicate to prove it never enters the restart source). Wherever this report previously mixed "5" and "6" without qualification, the counts above are authoritative.

---

### 2c.6 Cycle-3 validation (exact results)

| Suite | Result |
|---|---|
| `test_tam_flow.py` | **49 passed** (43 prior + 6 new) |
| `test_tam_routes.py` / `test_lookalike.py` / `test_company_linkedin_support.py` / `test_seg_csv.py` / `test_website_only_mode.py` | 27 / 26 / 18 / 21 / 9 — **all identical to baseline** |
| Combined PR-0.1 + PR-2.3 tree (temp merge `2972f25`, deleted after verification) | no conflicts (disjoint files); `test_mailtester_key` 10, `test_mailtester_timeout` 4, `test_enrich_throughput_hardening` 14, `test_tam_flow` 49, `test_company_linkedin_support` 18, `test_seg_csv` 21 — **116 passed**; secret scan clean |

## 2d. Cycle-4 — production hardening of the parity persistence (`b30ab95`)

### 2d.1 Verified sequence BEFORE hardening (the reviewer's concern, confirmed)

`2cf37a5` wrote the parity CSV **best-effort AFTER** `create_enrichment_job` and after registering the job in `_job_signals`/`_active_jobs`, immediately before `asyncio.create_task(runner)` — i.e. **the job row existed and the runner was about to start before persistence was attempted**, the write was **non-atomic** (a crash mid-write leaves a truncated CSV at the final name that `/restart` trusts), and the filename `find_companies_contacts_{YYYYMMDD_HHMM}.csv` was **minute-granular** — two chains created in the same minute shared one file, the second overwriting the first job's restart input (also colliding in the `jobs.filename` column).

### 2d.2 The fix (all three closed)

New helper `_persist_chain_input_csv()` + resequenced `create_chained_enrichment_job`:

1. **Fail-closed:** persistence now runs **BEFORE** the job row is created and the runner scheduled, and re-raises on failure. The caller (`tam_flow`) already catches and records `chain_skipped="chain_error: …"` while keeping the TAM export `done`. A chain job therefore **cannot start** without its restart input — the unsafe parent-walk restart path is unreachable for newly created jobs.
2. **Atomic:** rows are written to `<name>.csv.tmp` in the same directory, flushed + `fsync`'d, then `os.replace`d onto the final name (atomic on POSIX). A crash can leave at most an orphaned `.tmp`, never a truncated final file; the failure path unlinks the temp.
3. **Collision-safe:** the filename embeds the chained job's uuid — `find_companies_contacts_{stamp}_{job8}.csv` — making same-minute overwrites impossible (and un-colliding `jobs.filename`). `/restart` resolution is unchanged (it derives the name from the job row). No external code or docs reference the old filename pattern (verified by grep).

### 2d.3 Cycle-4 tests (one inverted test rewritten, three added; `test_tam_flow` → 52)

| Test | Pins |
|---|---|
| `test_persistence_failure_fails_closed_no_job_no_runner` (rewrites the old best-effort test, inverted by design) | persistence failure → raises; **zero** `tam_chain` job rows; runner never awaited; no artifacts left |
| `test_partial_write_leaves_no_truncated_restart_input` | `writerows` explodes after a partial row → nothing at the final name, `.tmp` cleaned, job not created |
| `test_same_minute_chains_never_collide_on_restart_input` | two chains in the same minute → distinct uuid-suffixed files, each holding its own rows (first not overwritten) |
| `test_successful_chain_persists_parity_file_atomically` | success → final file exists, matches runner rows exactly, no `.tmp` residue |
| (cycle-3 parity tests re-run unchanged) | interrupted-mixed resume = exactly the undone LinkedIn rows; all-done short-circuit; no-progress bounded to the chain's 4 rows; legacy defect still pinned |

### 2d.4 Cycle-4 validation (exact results)

| Suite | Result |
|---|---|
| `test_tam_flow.py` | **52 passed** |
| `test_tam_routes` / `test_lookalike` / `test_company_linkedin_support` / `test_seg_csv` / `test_website_only_mode` | 27 / 26 / 18 / 21 / 9 — identical to baseline |
| Combined tree (`fix/mailtester-key-fallback` + hardened `feat/tam-chain-linkedin-rows`, temp merge deleted after) | no conflicts; `test_mailtester_key` 10 + `test_mailtester_timeout` 4 + `test_enrich_throughput_hardening` 14 + `test_tam_flow` 52 + `test_company_linkedin_support` 18 + `test_seg_csv` 21 = **119 passed** |

### 2d.5 Residual risk (exact, not overstated)

- The `.tmp`→final rename durability is file-fsync + `os.replace`; the directory entry itself is not dir-fsync'd (standard practice here; a host-level crash in the microsecond window could lose the rename, in which case `/restart` takes the legacy path for that one job — the same path that existed before PR-2.3, and tam_chain logs the missing file at restart time via the existing resolution logic).
- `chain_skipped="chain_error: …"` on persistence failure means the operator must re-tick "find contact emails" on the TAM run to retry the chain — acceptable: the TAM export itself is never lost, and silent unsafe restarts are strictly worse.

---

## 2e. Cycle-5 — final verification pass (re-audit of the hardening + `915a951`)

The cycle-4 request was re-issued; rather than assume, every property was re-verified against the tree, and one residual gap was found and fixed.

### 2e.1 Eight-point verification matrix

| # | Reviewer requirement | Status | Evidence |
|---|---|---|---|
| 1 | Sequence verified | ✅ | code order: `_persist_chain_input_csv` → `create_enrichment_job` → `_job_signals`/`_active_jobs` → `create_task` (tam_chain.py; persist strictly first) |
| 2 | Atomic + collision-safe | ✅ | `.csv.tmp` in same dir → flush + `fsync` → `os.replace` (atomic); filename `find_companies_contacts_{stamp}_{job8}.csv` embeds the job uuid; failure path unlinks the temp |
| 3 | Job cannot start without persisted input | ✅ | persistence precedes job creation and re-raises; tam_flow converts to `chain_skipped="chain_error: …"`; pinned by `test_persistence_failure_fails_closed_no_job_no_runner` (0 `tam_chain` job rows, runner never awaited) |
| 4 | Failed/partial writes safe | ✅ | pinned by `test_partial_write_leaves_no_truncated_restart_input` (mid-write explosion → no final file, no `.tmp` residue) |
| 5 | Persistence-failure / partial-write / collision tests | ✅ | `test_persistence_failure_…`, `test_partial_write_…`, `test_same_minute_chains_never_collide_…` — all enumerated PASSED |
| 6 | Successful jobs keep exact restart parity | ✅ **+ one fix** | row parity re-confirmed (`test_chain_writes_upload_csv_matching_runner_rows`, `test_restart_parity_interrupted_mixed_job`, `…all_done…`, `…no_progress…`); kwargs parity had a drift — see 2e.2 |
| 7 | Fixture counts reconciled | ✅ | §2c.0 (F5 = 5 TAM rows, F6 = 6, both chain to 4) |
| 8 | Regression + combined integration | ✅ | fresh runs: adjacent suites 27/26/18/21/9 (baseline-identical); combined temp-merge 10+4+14+52+18+21+9 = **128 passed**, no conflicts, secret scan clean; PR-0.1 branch secret scan: 0 occurrences |

### 2e.2 The one residual gap found and fixed (`915a951`)

**Kwargs parity drift:** the initial chain run passes `name_col="company_name"` to the runner, but the job row never persisted `name_col` — `/restart` rebuilds runner kwargs from job-row columns (`routes.py:6703`), so every restarted chain ran with `name_col=None`, silently dropping the company-name hint-column mapping. Pre-existing since the original chain code, exposed by PR-2.3's parity bar. Fix: one line — persist `name_col="company_name"` on the chain job row; pinned by a new assertion in the mixed-fixture chain test. Initial and restarted jobs now receive identical kwargs for every column the chain uses (`domain_col`, `linkedin_url_col`, `name_col`, `max_results`, `selected_providers`, `cascade_config`).

### 2e.3 Accepted remainder (unchanged from §2d.5)

No dir-fsync on the rename (host-crash microsecond window → legacy fallback for that one job); persistence failure requires re-ticking the chain on the TAM run; `chained_linkedin_rows` not yet in the UI; GL DM rescue domain-gated for LinkedIn-only rows.

### 2e.4 Cycle-5 validation (exact results)

`test_tam_flow.py` **52 passed** · adjacent suites 27 / 26 / 18 / 21 / 9 (baseline-identical) · combined tree (temp merge, deleted): **128 passed** across both PRs' suites.

### 2b.5 Validation (exact results)

| Suite | Baseline | Post-change |
|---|---|---|
| `test_tam_flow.py` | 33 passed | **43 passed** (10 new: 6 chain-level + 4 routing-level) |
| `test_tam_routes.py` | 27 passed | **27 passed** |
| `test_lookalike.py` | 26 passed | **26 passed** |
| `test_company_linkedin_support.py` | 18 passed | **18 passed** |
| `test_seg_csv.py` | 21 passed | **21 passed** |
| `test_website_only_mode.py` | 9 passed | **9 passed** |

New tests: `test_chain_includes_linkedin_only_companies` (mixed + normalization + persisted column + runner kwarg), `test_chain_linkedin_only_run_is_not_skipped`, `test_chain_cross_dedupes_linkedin_only_against_domain_rows` (www vs bare host), `test_chain_dedupes_duplicate_company_urls`, `test_chain_ignores_personal_and_garbage_urls_on_domainless_rows` (legacy skip literal), `test_chain_domain_only_rows_do_not_carry_linkedin_column` (byte-parity), and `TestChainedRowRouting`: routes-to-company-orchestrator / domain-row-still-routes-to-domain-cascade / mixed-rows-each-own-path / missing-identifier-skipped-not-routed (drives the real `run_domain_enrichment` router with recorders on both enrichment paths — the no-duplicate-waterfall proof). One intermediate run caught the www-canonicalization bug before commit (2 failures → fixed → 43 green).

Rollback: single-commit revert (`git revert e6171f2`); no schema, no env, no breaking API change (return key additive); independent of PR-0.1. No jobs run, no services touched, no `.env`/DB modifications, nothing pushed/merged/deployed.

---

## 6. Next recommended PR

**PR-0.2 (docs/truth sync)** — 0.5 pd, content-only, zero runtime risk, no decisions required (pre-start checklist from cycle 1 stands: confirm "30M+" phrasing; build `/flows/help` membership from `providers.get_enabled_providers()` with an explicit tier order).

**Why this position:** PR-0.1 (security) and PR-2.3 (first feature) are now both awaiting review/merge; PR-0.2 removes operator-facing falsehoods (6-source Help page, "6.6M+" banner, stale `/flows/help` cascade table, missing TAM options in the Jobs source filter) before Phase-1 surfaces make them more prominent. It also has **no code interaction** with either pending branch, so it can proceed the moment either is merged — or be reviewed in parallel.

**Sequencing note:** the plan's original order (0.1 → 0.2 → 1.2 → 1.1 …) stands; PR-2.3 was pulled forward at reviewer request and is now done. After PR-0.2, the next value-bearing PR is **PR-1.2 (catch-all retention)** — smallest Phase-1 item, independent of PR-1.1 — followed by **PR-1.1 (verify job backend)** which now carries the two logged plan corrections (raw-result cache independent of sendability policy; bulk = background jobs) plus the PR-1.1 blocker noted in §4.5 (pre-existing hang in `test_quickenrich_endpoint.py::test_400_when_no_input` should be RCA'd before that suite is extended).

**Decisions required before the NEXT feature PR (PR-1.1/1.2 work):** none blocking; Q-4 (catch-all sendability default) remains open and only affects the verify surface's default toggle, which is coded to preserve today's posture regardless.

---

## 8. Deployment checklist (operator-run — NOT executed in this session)

**Merge-prep validation (2026-11-11, this session):** `master` == `origin/master` @ `384e33b` (fetched, no drift); PR-2.3 = exactly the 4 required commits; PR-0.1 = 1 commit; file sets disjoint; both-merged tree: **269 tests passed / 0 failed** across the full 15-suite matrix, **0 secret occurrences** (verified: master + PR-2.3 only still has 1 — PR-0.1 must land in the same deploy for a clean tree); no accidental files (PR-0.1: 5 files; PR-2.3: 2 files; both exactly as reviewed); no key-like additions in either diff. **Active jobs at session time: 10 enrichment + 3 scraper RUNNING — repeat the §8.1 check at deploy time.**

### 8.1 Pre-merge (on the VPS)

```bash
cd /var/www/lead-generation-platform
# 1. Active jobs — AT SESSION TIME: 10 enrichment + 3 scraper RUNNING.
#    Repeat at deploy time; if any are running: cancel via
#    POST /api/{scraper|enrichment}/jobs/{id}/cancel and WAIT for
#    status='cancelled' (never restart over live jobs — DB is the only
#    cross-worker truth).
sqlite3 -readonly backend/data/jobs.db \
  "SELECT job_type,status,COUNT(*) FROM jobs WHERE status IN ('running','starting') GROUP BY 1,2;"
# 2. Rollback tag + optional DB snapshot (schema-additive PRs; snapshot is belt-and-braces)
git tag pre-tam-pr01-pr23-$(date +%F)
cp backend/data/jobs.db backend/data/jobs.db.backup.$(date +%Y%m%d_%H%M%S)
```

### 8.2 Merge + deploy (both branches, one deploy window)

```bash
git checkout master && git pull --ff-only
git merge --no-ff fix/mailtester-key-fallback    # PR-0.1 first (secret removal)
git merge --no-ff feat/tam-chain-linkedin-rows   # PR-2.3 — ALL FOUR commits land together
git merge --no-ff fix/operator-truth-sync        # PR-0.2 (optional same window; 3-way verified)
# verify: git log --oneline -7  → c990394, 915a951, b30ab95, 2cf37a5, e6171f2, a3a5ef1, 384e33b
#         git grep -c 'b0b4ddb5' -- → 0
cd backend && source venv/bin/activate && pytest enrichment/tests/test_mailtester_key.py enrichment/tests/test_tam_flow.py -q
sudo systemctl restart lead-generation-platform.service
```

### 8.3 Post-deploy health checks (in order)

1. `curl -s http://localhost:8765/api/health` → `{"status":"ok","mcp_enabled":true}`
2. `sudo systemctl is-active lead-generation-platform.service` → active; `ss -tlnp | grep 8765`
3. Frontend loads: `curl -sI https://listbuilding.eagleinfoservice.com/ | head -1`
4. Logs clean 60 s: `sudo journalctl -u lead-generation-platform.service -f --since "1 minute ago"`
5. **MailTester canary (validates the env-required key on the new code):** one `POST /api/enrichment/quickenrich {"domain":"<known-good-domain>","validate_emails":true}` → response `summary.validator == "mailtester"` with `validated >= 0` and no spike in `unavailable` — a run of `unavailable` results means `MAILTESTER_API_KEY` is missing/stripped in the service env (fail-open; fix env, not code)
6. **PR-0.2 truth surfaces (if riding this window):** `curl -s localhost:8765/api/enrichment/flows/help | python3 -m json.tool | grep -A2 leadocean` shows the tier with `"status"`, and the Help page shows "8 active data sources" + "Last Updated: 2026-11-11"; TAM UI still loads (`/#/search`, `/#/jobs`)

### 8.4 MailTester key rotation (AFTER the deploy above; §4.3 restated)

1. Owner mints a new key at the proxy (`/opt/mailtester-proxy`, separate codebase)
2. Update `backend/.env` `MAILTESTER_API_KEY` (old key stays valid during the window)
3. `sudo systemctl restart lead-generation-platform.service` (cancel/wait per §8.1) → repeat §8.3 step 5
4. Revoke the old (burned) key at the proxy **only after a 48 h rollback window closes**
5. Optional: history scrub (BFG) — only if the repo will be shared beyond trusted CI/VPS; rotation alone neutralizes the leak

### 8.5 TAM-chain canary (first real exercise of PR-2.3)

1. Owner runs one small TAM search on `/#/search` with "find contact emails" ticked: a persona filter set expected to return ~10–50 companies, ideally including some domain-less rows (GetLeads-leg runs produce them)
2. Verify on the chain job (Jobs page, "Contacts for TAM companies"):
   - `total` == domain-bearing + LinkedIn-bearing companies (decoys excluded); `chained_linkedin_rows` > 0 if any LI-only companies existed
   - `ls backend/data/uploads/find_companies_contacts_*_<job8>.csv` exists for the job (the parity file)
   - LinkedIn-only rows enrich via the company path (`dm_linkedin_url`/`company_linkedin_url` populated; provider chips fire)
3. **Restart parity probe (optional, strongest):** cancel the chain job mid-run → restart it → resumed `total` == `chained_total − processed`; no skipped decoy rows; no re-processing of done rows
4. Cost bound: ~1 Blitz FUP record per TAM company + normal cascade costs; empty results free

### 8.6 Rollback ladder

1. Env-only: n/a (no new env in these PRs; PR-0.1 *requires* the existing key to remain set)
2. Code: `git revert 915a951 b30ab95 2cf37a5 e6171f2 a3a5ef1` on master → cancel/wait → restart → §8.3 steps 1–4
3. Tag checkout `pre-tam-pr01-pr23-<date>` for worst case
4. DB artifacts: NONE (no schema changes in either PR; parity CSVs are additive files — safe to leave or `rm backend/data/uploads/find_companies_contacts_*.csv`)
5. Do NOT revoke the new proxy key before the rollback window closes (a reverted tree would run on the old key)

---

## 9. PR-0.2 — operator truth sync (`c990394` on `fix/operator-truth-sync`, awaiting review)

### 9.1 Verification against the current system (done before writing any copy)

| Claim surface | Verified against | Result |
|---|---|---|
| Active cascade tiers | `providers.py` `is_provider_enabled` + `backend/.env` kill switches + executor order (`pipeline.py`/`list_builder.py` step comments) | **8 active in prod**: contacts_db → smartprospect → quickenrich → **leadocean** → getleads → blitz → **moltsets** → wizleads; better_enrich code-on but `ENABLE_BETTER_ENRICH=false` in prod; prospeo off end-to-end |
| Internal DB size | `leadsdatabase.cc/stats.json` — persons **30,848,494** (measured 2026-11-10, audit E-027); companies 9,204,601 (2026-11-11; the `persons.total` scalar is not exposed in today's stats shape, last verified figure used) | "6.6M+ leads" banner was ~4.7× understated → "30M+ people" (conservative round-down) |
| Discovery during implementation | the HOME page also carried a second stale 6-source list (info-card) beyond the Help page — found by the new test suite, fixed in the same commit | both surfaces now identical |
| Subtle bug found & fixed | `providers.get_enabled_providers()` **ignores the per-provider env kill switches** (providers.py — dict-membership only); the first draft of the new `/flows/help` helper used it, which would have labeled better_enrich "enabled" in prod | helper uses `is_provider_enabled()` per tier (caught by the new reload test before commit) |

### 9.2 Changes (branch `fix/operator-truth-sync` off `master` @ `384e33b`; 5 files)

| File | Change |
|---|---|
| `backend/enrichment/routes.py` | `/flows/help` `cascade_order`: hand-maintained stale list → `_HELP_CASCADE_TIERS` (explicit 10-tier order incl. leadocean/moltsets/prospeo) + `_cascade_help_table()` deriving per-tier `status` from `providers.is_provider_enabled()`; `cascade_arms` copy documents the leadocean terminal-discovery arm. Additive `status` field; shape otherwise unchanged. |
| `frontend/index.html` | Help page: 6→8 active sources in cascade order (LeadOcean 4th, MoltSets 7th), BetterEnrich-vs-Prospeo disable semantics split, "database of 30M+ people", Last Updated 2026-11-11. Home page: "9-source cascade … Better Enrich" → 8-source; stale 6-item Data Sources card → same 8. People banner: 30M+ people. Jobs source filter: + `tam_flow`, `tam_chain` (pass-through filter — values match the `source_type` literals). Backup `frontend/index.html.bak_pre_truth_sync_20261111` per ritual. |
| `backend/enrichment/tests/test_flows_help_truth.py` | NEW (5 tests): exact tier order; per-tier status == `is_provider_enabled` at build; env kill-switch flip reflected on module reload; leadocean-terminal arms copy; rate/role completeness. |
| `frontend/tests/test_truth_sync_ui.py` | NEW (11 tests): 8-source copy (Help + home), tier presence/numbering, disable-block semantics, stale copy absent (incl. "6.6M+", "We use 6 data sources", old 9-source sentence), banner count, Jobs filter options, home card order. |
| `frontend/tests/test_quickenrich_ui.py` | Existing marketing pin updated to the new exact sentence with truth-basis comment (Better Enrich env-off in prod; the 9-source count overstated). Guard nature preserved — still pins the exact cascade sentence. |

### 9.3 Validation

- New suites: `test_flows_help_truth` **5 passed**; `test_truth_sync_ui` **11 passed**
- Full frontend suite: **130 passed, 0 failed**
- Backend targeted (routes-adjacent): `test_routing` 25 · `test_domain_only_api_cascade` 5 · `test_website_only_mode` 9 · `test_cascade_fixes` 9 · `test_tam_flow` 33 (master baseline — PR-2.3 raises it to 52)
- **3-way combined tree** (master + PR-0.1 + PR-2.3 + PR-0.2, temp merge, deleted): clean; `test_flows_help_truth` 5 · `test_tam_flow` 52 · `test_mailtester_key` 10 · `test_company_linkedin_support` 18 · frontend **130** — no conflicts anywhere
- Not run (known pre-existing box issue): `test_quickenrich_endpoint.py` (hangs at `test_400_when_no_input`; unrelated to this PR — PR-1.1 must RCA it before extending that suite)

### 9.4 Limitations / notes

- The help table's status is computed at **import time** (the payload is a module-level constant); equivalent to request-time because env flips require a service restart, which re-imports — documented in the helper docstring and pinned by the reload test.
- The "30M+" figure is a round-down of the 2026-11-10 measurement; persons only grow, and today's stats shape no longer exposes the scalar (companies 9.2M confirms scale). Re-pin when a fresh exact count is available.
- No backend behavior change beyond the `/flows/help` payload content (additive `status` field; the endpoint is public and unauthenticated as before).

---

## 7. Review summary (current)

**PR-0.1 — APPROVED, merge-prepped.** `fix/mailtester-key-fallback` @ `a3a5ef1` (5 files, +243/−1). Secret absent from the branch (re-scanned cycle 5); rotation sequence in §4.3 remains the mandatory ops follow-up. 125 tests green.

**PR-2.3 — GO. Merge all FOUR commits in one deploy** (`feat/tam-chain-linkedin-rows`, independently off `master` @ `384e33b`):

1. `e6171f2` — LinkedIn-only companies chain into the existing Flow-1 company-URL path (persisted `linkedin_url_col`; www-canonical cross-dedupe; legacy skip literal, domain rows, CSV schema, statuses all preserved; no waterfall duplication — routing proven by recorder tests against the real router)
2. `2cf37a5` — restart parity: the chain's exact rows become `/restart`'s primary input (fixed the cycle-3 defect: silent drops / leaks / inflated totals via the misaligned parent-CSV path)
3. `b30ab95` — persistence is **atomic** (tmp + fsync + `os.replace`), **collision-safe** (uuid-suffixed filename; minute-stamp overwrite impossible), and **fail-closed** (no job row, no runner, `chain_skipped="chain_error: …"` if the restart input cannot be persisted; TAM export stays done)
4. `915a951` — `name_col` persisted on the job row so restarted chains receive identical runner kwargs (initial-vs-restart drift closed; pinned by test)

**Verification (cycle 5, fresh):** `test_tam_flow.py` **52 passed** — all 9 hardening/parity tests enumerated PASSED by name; adjacent suites 27 / 26 / 18 / 21 / 9 baseline-identical; combined-tree integration **128 passed**, no conflicts, secret scan clean on the combined tree and on the PR-0.1 branch.

**Guarantee (exact scope):** every chain job created by the deployed code has an atomically-written, collision-free restart input and `/restart` prefers it — resume returns exactly `chained_total − done` rows, correctly-aligned checkpoints, identical kwargs. The legacy parent-CSV path remains only as the fallback for a job whose file is missing at restart time (documented §2c.4/§2d.5/§2e.3).

**Accepted remainder:** no dir-fsync on the rename (host-crash window → legacy fallback for that one job); persistence failure = operator re-ticks the chain; `chained_linkedin_rows` not yet in the UI; GL DM rescue domain-gated for LinkedIn-only rows.

**Rollback:** `git revert 915a951 b30ab95 2cf37a5 e6171f2` (revert the stack); all changes additive.

**Pending operator actions:** (1) run the §8 deploy (PR-0.1 + PR-2.3 in one window; PR-0.2 may ride along after review); (2) schedule MailTester key rotation (§4.3); (3) review PR-0.2 (`fix/operator-truth-sync` @ `c990394`, §9); (4) after PR-0.2 review, approve PR-1.2 (catch-all retention) as the next cycle.

**PR-0.2 — implemented this cycle, awaiting review** (`fix/operator-truth-sync` @ `c990394`, §9): truth-sync of `/flows/help`, Help + Home pages, People banner, Jobs filter; 16 new tests + 1 updated pin; frontend suite 130 green; 3-way combined merge with both approved branches verified clean.

**Not done (by instruction):** no push, no merge, no deploy, no service restarts, no `.env` edits, no key rotation, no job cancellations, no live DB writes, no live/paid provider calls.
