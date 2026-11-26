# All-Source TAM Discovery — Pre-Implementation Audit & Decision Gate

**Date:** 26 November 2026 · **Mode:** read-only audit + bounded empirical probes · **No production code changed.**

---

## 1. Why the providers were originally kept separate (historical truth)

| Combination | Origin | Reason it exists |
|---|---|---|
| `blitz` (default) | TAM-by-People launch (2026-09-16) | Only source with persona-filtered company discovery (`matched_people`) at the time; 1 FUP record/company |
| `blitz + getleads` ("Both") | Lookalike 2.0 (2026-09-18) | GL added as a *top-up* when Blitz under-fills; `getleads`-only refused when a company-type filter is set (GL has no ownership-type column — live-verified 2026-10-07) |
| `+ quickenrich`, `+ leadocean`, `qe+lo` | Owner direction (2026-11-11) | Shift discovery from credit-limited Blitz/GL onto the unlimited QE plan and LO's 25M/mo search window |

**No deliberate prohibition on combining all four exists.** `tam_routes.py:428` accepts any subset
of `("blitz","getleads","quickenrich","leadocean")`; `tam_flow.py` filters the same set. The UI
simply never offered it. **However** — the orchestration is a *sequential fill*, not parallel
independent discovery (§3.6), and that is the load-bearing fact.

## 2. Architecture: what an "All Sources" option would actually do today

Execution order in `run_tam_flow` (all under ONE shared `overall_budget = max_companies`):

1. **Blitz** fills first (cursor pages; its own 2,000-candidate fanout cap in lookalike mode)
2. **GetLeads** runs ONLY `if len(shared.rows) < max_companies` (tam_flow.py:725) — a pure top-up
3. **QuickEnrich** leg ONLY if still under (tam_flow.py:784)
4. **LeadOcean** leg ONLY if still under (tam_flow.py:798)

So "all sources" today = "Blitz until saturated, others fill the remainder." With a typical
500-company budget and healthy Blitz, **the other three legs contribute ~nothing**. The feature
as requested (independent discovery across providers) requires an orchestration change, not a
dropdown entry.

## 3. Risk register

| # | Risk | Sev | Already exists? | Mitigation |
|---|---|---|---|---|
| 1 | **GetLeads 400 on multi-country** — GL requires full country NAMES (`United States`), the leg sends ISO codes (`US`, `CA`) | **Critical** | **YES — live bug today** in `blitz+getleads` runs with ≥1 country (empirical: GL leg returned 0 with a swallowed 400) | ISO→name mapping table before the GL call (mirrors band conversion) |
| 2 | **QE 422 on LinkedIn-style enums** — QE's employee bands are `< 5, 5 - 19, 20 - 99, 100 - 249…` (not `11-50`); its industries differ from LinkedIn's (Apparel and Fashion invalid). The 422→keyword fallback covers industry but NOT bands | **Critical** | YES (V2 runs masked it: LO carried those runs) | Band-translation table (11-50→"20 - 99" overlap etc. — non-exact: bands are *incompatible taxonomies*; needs an explicit overlap-mapping policy + `filter_notes` disclosure) |
| 3 | Budget bias: first provider starves the rest; results not "max coverage" | **High** | YES (by design for GL) | Per-provider sub-budgets (e.g. split `max_companies` across selected sources) or parallel pulls + post-merge |
| 4 | Filter-parity dilution: GL can't apply keywords; QE can't in contact-finder mode; wrong-size/wrong-industry companies enter because a provider silently can't enforce a selected filter | **High** | Partially (keyword provenance exists; size/industry do NOT have a provenance story) | Extend the V2 `filter_notes`/`keyword_match` pattern to size+industry: per-row `size_match: confirmed/unverified` or skip providers that can't enforce restrictive filters |
| 5 | Duplicate inflation across providers | Medium | Mechanism exists (`_row_dedupe_key` domain→linkedin→name) but **untested at 4-way scale** — empirical 45-row sim showed 0 overlaps (providers genuinely returned disjoint sets) | Existing key is sound; add www-stripping to the domain key (it currently doesn't strip `www.` — LO/QE domains are bare, Blitz can carry `www.`) |
| 6 | Conflicting field values (employee count, industry label) with no survivorship rule | Medium | YES — first-writer-wins today (`shared.rows.append`; no overwrite) | Field-fill-not-overwrite merge is already implicit (append-only); document + optional field provenance column |
| 7 | Rate/credit contention: LO search window is **shared account-wide with the hermes harvester key**; GL capped 500 credits/run; QE finder lane 100 RPM default; Blitz FUP 15M/mo (4.4M used) | Medium | YES | Keep caps; run legs sequentially (they already are); monitor `/v1/usage` |
| 8 | Longer runtimes → more restart exposure; job_events growth | Low | Partially | Existing heartbeat/checkpoint machinery handles it |
| 9 | Provider failure inside a 4-way run | Low | YES | Each leg never raises (returns `[],pages`); V2 `filter_notes` + summary already surface failures |
| 10 | DB growth from `_backup_companies_to_contacts_db` upserts | Low | YES | Upsert is idempotent by domain; rate unchanged by source count (deduped rows only) |

## 4. Empirical cross-provider comparison (Adnan ICP: Apparel+Fashion, 11-50/51-200, US+CA, jewelry keywords)

| Provider | Job | Result | Notes |
|---|---|---|---|
| Blitz only | `b6b1d6ac` | **15, all kw-confirmed**, 14 US + 1 CA, correct bands | Rich metadata: domain 100%, linkedin 100%, industry 100%, size 100%, country 100%, type 87%; **slogan/desc 0%, revenue 0%**; true `matched_people` counts |
| LeadOcean | `fca8fefd` | 15, all confirmed, 14 US + 1 CA, correct bands | domain 93%, linkedin 100%, revenue 47%; **type 0%** |
| QuickEnrich | `b63ccf76` | **0 — 422** on employee bands (`11-50` invalid; QE enum is `< 5 / 5 - 19 / 20 - 99…`); industry also 422s (fallback covers industry only) | Same ICP ran fine industry-only (`80f7bbdb`, 15 cos, revenue 100%, **country 0%**) |
| GetLeads | `5a97cb58` | **0 — 400** on `countries: ["US","CA"]` (GL wants `United States`, `Canada`) | Swallowed error, leg silently empty |

**Merged simulation (45 raw rows, python-only):** 45 unique / 0 cross-provider duplicates / 0
identity collisions. Providers returned *complementary, essentially disjoint* sets — Blitz gave
US boutiques, LO gave brand-level jewelers, QE (industry-only) gave international manufacturers.
Overlap on this ICP ≈ 0%, so a 4-way run would roughly **3–4× unique coverage** — the coverage
premise is real, not noise-inflation.

**Schema compatibility (from live payloads):**

| Field | Blitz | LO | QE | GL |
|---|---|---|---|---|
| name | ✓ | ✓ | ✓ | ✓ |
| domain | ✓ | ✓ (93%) | ✓ | ✓ |
| linkedin_url | ✓ 100% | ✓ 100% | 67% | ~0% |
| industry | LinkedIn taxonomy | own (from `name3`) | own (LinkedIn-ish, differs) | LinkedIn-ish (≤3) |
| size | LinkedIn bands | own bands | **incompatible bands** | own bands (`11 to 50`) |
| country | ISO | ISO (nested addr) | **absent** | **full names required** |
| type | ✓ | absent | absent | absent |
| revenue | absent | USD int | ✓ | band |
| matched_people | **true counts** | 1 | 1 (contact-mode: real) | 1 |

## 5. Verdict

**Safe after minor compatibility changes — with one mandatory pre-fix and one design decision.**

- **Blocker (must fix before any multi-source UI):** Risks #1 and #2 are *live bugs* that make
  GL contribute zero on any multi-country search and QE zero whenever employee bands are set —
  both silently today. An "All Sources" option would advertise four engines while silently
  running one or two.
- **Design decision needed from you:** budget semantics. Options:
  - **(a) Sequential fill (today's behavior, just add the option)** — zero code risk, but
    Blitz starves the rest at big budgets; "Maximum Coverage" would be a misnomer.
  - **(b) Per-provider sub-budgets** (recommended) — e.g. split `max_companies` evenly across
    selected sources, merge + dedupe after; honest "N from each source" reporting; modest
    code change confined to `run_tam_flow` (leg orchestration already isolated per source).
  - **(c) Full parallel discovery** — most coverage, most runtime/complexity; not needed given
    (b) achieves ~the same unique set on disjoint providers.
- Custom provider selection: trivial after (a) or (b) — the API already accepts any subset.

**Recommended scope if approved:** fix #1 (GL country-name mapping) + #2 (QE band mapping with
overlap semantics + `filter_notes` disclosure) → implement (b) sub-budget orchestration → add
UI options (All Sources / QE+LO / custom checkboxes) → per-row provenance already exists via
`source`; add `sources_contributed` to the summary → acceptance tests incl. the §4 ICP as a
regression fixture. Estimated: 1–2 days including tests, no schema migrations.

**Not advisable as-is:** a pure frontend "all sources" option on today's orchestration — it
would ship the two silent-zero bugs and the Blitz-starvation behavior under a Maximum-Coverage
label.

---

*Evidence: jobs `b6b1d6ac` (blitz), `fca8fefd` (LO), `b63ccf76` (QE 422), `5a97cb58` (GL 400),
`80f7bbdb` (QE industry-only); live probes of QE lookups enums and the GL 400 body; code lines
cited inline. No production changes were made during this audit.*
