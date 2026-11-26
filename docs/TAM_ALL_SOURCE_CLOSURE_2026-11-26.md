# All-Source TAM Discovery — Implementation Closure Report

**Date:** 26 November 2026 · **Live release:** `5d9f153` · **Rollback tag:** `pre-allsource-tam-2026-11-26`

## 1. What was implemented

| Requirement | Status |
|---|---|
| GL country-format fix | **Done** — ISO→full-name map (23 values from GL's validator); multi-country canaries pass |
| GL failure/zero distinguishability | **Done** — `ok:False` → 3rd error element → `filter_notes` "request FAILED (…)" |
| QE band taxonomy | **Done** — LI bands never sent to QE (no exact mapping exists; overlap = broadening); rows tagged `size_unverified` |
| Adaptive budgeting (option B, bounded) | **Done** — `budget_mode="adaptive"`: round-1 fair share per provider (incl. blitz), round-2 proportional top-up to yield-proven, capacity-remaining providers only |
| Identity resolution | **Done** — `tam_identity_key` (normalized domain → normalized LinkedIn → name-only-when-both-absent); 19 deterministic fixtures |
| Survivorship | **Done** — fill-not-overwrite merge on rediscovery; 16 firmographic fields; matched_people basis-aware; sources accumulate |
| matched_people semantics | **Done** — `matched_people_basis` ∈ observed/lower_bound/placeholder; placeholder never beats observed |
| Strict filter compatibility | **Done** — per-provider×per-dimension capability matrix; `icp_status` qualified/candidate per row; `unverified_dims` lists exactly what couldn't be verified |
| QE keyword fallback honesty | **Done** — fallback rows `kw_approximate` → keyword candidates, never confirmed (live-proven: Pony Club no longer "confirmed") |
| Source selector | **Done** — All Sources (adaptive), Custom checkboxes + adaptive toggle, legacy presets unchanged (sequential) |
| Reporting | **Done** — `provider_stats` (selected/executed/raw/pages/notes/contributed_unique), `icp_stats`, `budget_mode`, `filter_notes` |
| Chaining integrity | **Done** — `candidate` rows never auto-chain (domain or LinkedIn path) |

## 2. Before vs after (live evidence)

| Run | Mode | Result |
|---|---|---|
| `46294ef2` | all-source, blitz unclamped | 24/24 blitz — other providers starved (the audit's predicted first-runner bias, reproduced then fixed) |
| `ee408c0c` | + blitz clamp, round-2 bug | job failed mid-run (NoneType) — fixed |
| `33769125` | all fixes, Adnan ICP | **18 unique: 6 blitz + 6 QE + 6 LO; 12 qualified, 6 candidates (size); 0 dups; all kw-confirmed** |
| `f39b0f03` | B2B SaaS ICP | 18 unique; QE's off-ICP fallback rows (Pony Club etc.) arrived keyword-"confirmed" → honesty gap found & fixed |
| `4d574751` | final Adnan re-run | 18 unique; QE rows now `candidate (keywords;size)` — exactly per policy; blitz+LO qualified |

## 3. Known limitations (unchanged from audit + new)

1. GL contributed 0 rows in the all-source canaries — its contact-search coverage for these niches/keyword-constrained queries is thin; failures now surface in `filter_notes`.
2. Adaptive stops at provider exhaustion under existing caps (18 vs 24 requested = honest partial, never labeled exhaustive).
3. `matched_people` for QE company-finder rows is placeholder (no persona).
4. Round-2 top-up only flows to QE/LO/(blitz-with-cursor); GL round-2 not implemented (its credit cap makes it rarely the marginal provider).

## 4. Rollback

`git revert` the five commits `c2dc6f6..5d9f153` → restart → tag `pre-allsource-tam-2026-11-26`. All changes additive (new CSV columns trailing, new request param default-legacy, new tables none).
