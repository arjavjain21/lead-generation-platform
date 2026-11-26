# TAM Discovery V2 — Closure Report

**Date:** 26 November 2026 · **Release:** `a711e56` + `fca-fixes` (LO schema normalization) · **Status: DEPLOYED & LIVE-VALIDATED**

## 1. What was implemented

| Feature | Before | After |
|---|---|---|
| **A. Employee ranges** | Single-select dropdown (`employee_range: [one]`) | Chip multi-select, OR-array; non-contiguous selections stay disjoint (11-50 + 201-500 never pulls 51-200); all four providers take the array natively in **one request** — no Cartesian fan-out |
| **B. Countries** | Single-select dropdown (`hq_country_code: [one]`) | Chip multi-select, OR-array on company HQ; person-level `people.location` untouched and visually distinct |
| **C. Company keywords** | Not in the UI (API fields existed, partially wired) | Include-ANY / exclude-ANY tag inputs (Enter/comma add, paste splits on commas+newlines, normalized+deduped); wired to LO `keywords[]`/`excludeKeywords[]` (description/specialties/tags/name — best coverage), QE `home_page_text` Filter (company-finder mode), Blitz native company-profile keywords |
| **D. Consistent UX** | Only Industry was multi-select | Employees + Countries join Industry as chip multi-selects (one reusable `makeTamChipSelect` component, industry-identical look); Type/MX/Seniority stay checkboxes (already effective multi); Min-matches stays scalar |

**Unknown-data policy (the honesty core):** results CSV carries a trailing `keyword_match` column.
`confirmed` = provider applied the keyword filter natively, or a local match was found in
name/industry text. `unverified` = the source has no company-keyword filter (GetLeads always;
QuickEnrich when job titles switch it to contact-finder mode, whose only text field is an
*employee* bio we deliberately refuse to use for company keywords) and no local match — the
row is **kept and counted, never claimed as a match**. Local exclusion hits drop the row
(`excluded_local` counter). The run summary carries `keyword_stats` +
`filter_notes` naming every provider that couldn't apply a filter.

## 2. Provider capability matrix (SDK + live-verified)

| Capability | Blitz | GetLeads | QuickEnrich | LeadOcean |
|---|---|---|---|---|
| Multi industry | Native | Native (≤3) | Native (own enum; 422→keyword fallback) | Native (532-value enum) |
| Multi employee bands | Native | Native (band-label conversion — **latent bug fixed**: raw labels were sent) | Native Filter arrays | Native `employeeRange[]` |
| Multi countries | Native `hq.country_code[]` | Native `countries[]` | Native | Native `hqCountry[]` |
| Company-desc keywords | Native (profile keywords) | **Unsupported → `unverified`** | Native `home_page_text` (company-finder mode only) | **Best**: `keywords[]` + `excludeKeywords[]` over description/specialties/tags/name |
| Requests needed | 1 | 1 | 1 | 1 |

## 3. Files changed

- `backend/enrichment/tam_flow.py` — `keyword_match` column; `_native_keyword_sources`, `_apply_keyword_provenance`; keyword pass + CSV rewrite; summary `keyword_stats`/`filter_notes`
- `backend/enrichment/tam_legs.py` — `_tam_filter_values` (clean include/exclude normalization, both payload+request shapes); QE leg keyword exclude; LO leg `excludeKeywords`; LO geo fill
- `backend/enrichment/quickenrich_client.py` — `company_finder(exclude_keywords=…)`
- `backend/enrichment/leadocean_client.py` — normalization to the real nested `company_data` schema (size/country/revenue/linkedin were blank before)
- `backend/enrichment/tam_routes.py` — GetLeads band conversion fix; display-name keyword summary
- `frontend/index.html` — chip multi-selects (employees/countries), keyword tag inputs, submit wiring, options-loader
- Tests: `test_tam_v2_filters.py` (16), `test_tam_v2_filters_ui.py` (11), updated pins in `test_tam_flow/routes/page_ui/ux_four_pack`

## 4. API contract

No breaking changes. `POST /flows/tam` `company.employee_range`, `company.hq_country_code`,
`company.keywords_include/exclude` accept arrays (single-element lists = legacy behavior).
New response keys: `keyword_stats`, `filter_notes`. New CSV trailing column: `keyword_match`.

## 5. Test results

**Backend:** 428 targeted tests green (tam_v2 16, tam_flow 52, tam_routes 27, unlimited_legs 9,
degraded 4, lookalike 26, verify 10+6, retention 9, intake 4, resolver 10, quickenrich 18,
help 5, + regression files). **Frontend: 188 passed.** All acceptance-matrix rows covered:
non-contiguous bands preserved, no middle-range bleed, multi-country OR, keyword OR/exclude,
missing-description ≠ confirmed, unsupported filter surfaced (`filter_notes`), legacy
single-value requests, multi-provider dedupe (existing suite), CSV contract.

## 6. Live validation (bounded, real providers)

| Run | Job | Query | Result |
|---|---|---|---|
| C1 industry-only | `80f7bbdb` | Apparel and Fashion | 15 cos — drifted to textile/IT manufacturers (QE industry-enum fuzzy), 0 jewelry — proves the gap Adnan hit |
| C2 +firmographics | `c85f2c67` | + 11-50/51-200 + US/CA | 15 cos — generic apparel retail (Old Navy, Forman Mills) — right size/geo, still no jewelry focus |
| C3/C5 +keywords (final) | `bacc0893` / `fca8fefd` | + include jewelry,jewellery,gemstones,fine jewelry; exclude manufacturing equipment,recruitment agency | **15/15 `confirmed` actual jewelry companies** (Anzie Jewelry CA, Yael Designs, Shiffon Co, arkfinejewelry.com, Cookie Lee Fine Fashion Jewelry…), **14 US + 1 CA**, sizes only `11-50`/`51-200` |
| C4 second ICP | `9b9b3d6f` | Food Production + 11-50/51-200 + US + include organic,natural,clean label; exclude equipment,packaging supplies | 15/15 confirmed organic/natural food brands (Madhava Foods, Nectave "The Healthier Sweet", Vego Naturals, Yumi) — generalizes beyond jewelry |

Credits: C1–C5 consumed QuickEnrich finder (unlimited plan) + LeadOcean search records
(15–30 records/run on the 25M/mo window). No GetLeads credits; no Blitz FUP usage.

## 7. Production deployment

Deployed 2026-11-26 via graceful restarts (three: feature, geo fix, LO-schema fix).
Zero running jobs at each restart gate (verified before each). Health OK, release
uniform (`/api/meta`), frontend live (nginx serves the tree). Rollback tag:
`pre-tam-v2-filters-2026-11-26`.

## 8. Known limitations (honest)

1. **GetLeads rows are `unverified` when keywords are set** — no company-description filter exists on that source; the summary says so.
2. **QuickEnrich keyword filtering only in company-finder mode** (no job titles). With titles set, QE switches to contact-finder (persona) whose only text filter is an *employee* bio — deliberately not abused for company keywords; those rows are `unverified`.
3. QE's own industry enum is fuzzy (C1 drifted); the existing 422→keyword fallback keeps it usable. LeadOcean's 532-value enum is stricter and gave the precise runs above.
4. `matched_people` on QE/LO legs is 1 (QE contact-mode accumulates real counts).
5. Employee-band *labels* differ per provider internally; conversions are applied (GetLeads `11 to 50`), and a band a provider doesn't recognize is silently dropped from that leg only — the summary's `filter_notes` covers keyword gaps; band-drop surfacing remains a P3 nicety.

## 9. Rollback

`git revert a711e56 4399996..HEAD` range on the release branch → graceful restart →
`pre-tam-v2-filters-2026-11-26` tag. Schema changes: none (CSV column + summary keys are
additive). The two latent-bug fixes (GL band conversion, LO schema normalization) are
independently safe to keep.

## 10. Skill & docs

Canonical skill + team repo (`lead-list-enricher`) updated and pushed (team VPS auto-pulls
within 17 min): V2 filter reference, OR semantics, non-contiguous guarantee, keyword
provenance policy, live-proof example, provider-specific limitations.
