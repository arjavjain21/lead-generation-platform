# Lead Universe Classification — Rule Map & Spec

**Status:** ✅ All three phases shipped.
- **Phase 1:** rules developed + validated.
- **Phase 2:** DB function `core.fn_classify_industry(industry text)` created + the NULL-person backfill run.
- **Phase 3:** write-back hook **live** — `classify_industry()` in `backend/enrichment/contacts_writer.py` mirrors the DB function so newly-enriched leads self-tag on write-back (explicit origin tag wins; else derive from `company_industry`). `raw_contact_collector.py` threads `company_industry` from provider responses into the payload; `routes.py` threads it from CSV rows.

**Last measured:** 2026-08-11 — **5,457,303 persons tagged** across the 4 buckets out of ~8.77M total (~62% coverage; ~38% NULL by design).
**Owner:** arjav · **DB:** `contacts` (port 5432, exposed as `leadsdatabase.cc`) · **No paid tools / no ML — pure rules.**

---

## 1. The model (read this first)

**Universe is a property of a COMPANY/domain, not a person.** A company has one industry → one universe. A person inherits their *current* company's universe.

- Source of truth for the tag: `core.person.lead_universe` (nullable text, indexed `idx_person_lead_universe`). No schema change was needed.
- The classification is **computed from `core.company.industry` alone** — the shipped function is single-argument. (An earlier plan added a `saasy_db` source-shortcut and an `outscraper_local_domains` domain-bridge tier; **neither shipped.** The `local_business` origin tag for scraper.tech leads is instead injected at the app layer on write-back, not in the DB function.)

## 2. The four buckets

| Universe | Means | Example industries |
|---|---|---|
| `saas` | Software / cloud product companies | software & internet, software and tech platforms, computer software, cloud, AI/ML, cybersecurity |
| `b2b_agency` | Sells to businesses: agencies, consulting, manufacturing, finance, pro services | marketing/advertising, consulting, financial services, manufacturing, IT services, legal, staffing |
| `local_business` | Physical / consumer-local services & locations | plumber, dentist, real estate, restaurant, hotel, gym, medical practice, auto repair |
| `ecom` | DTC / retail brands | retail, apparel/fashion, consumer electronics, cosmetics, "…brands" |
| `NULL` | Genuinely ambiguous or no industry signal | "Consumer Services", "food & beverages" (could be any), government, bare leads |

**Precedence (first match wins):** `saas → local → ecom → b2b → NULL`.

## 3. The classifier — `core.fn_classify_industry(industry text)` (shipped)

```sql
CREATE OR REPLACE FUNCTION core.fn_classify_industry(industry text)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  WITH norm AS (
    SELECT lower(replace(replace(replace(coalesce(industry,''),'_',' '),'/',' '),'&',' ')) AS i
  )
  SELECT CASE
    -- normalize: lowercase; underscores/slashes/ampersands -> spaces
    WHEN (SELECT i FROM norm) ILIKE ANY(ARRAY[
        '%software%','%saas%','%cloud computing%','%artificial intelligence%','%machine learning%',
        '%cybersecurity%','%data process%','%tech platform%','%computer software%','%information and internet%'
      ]) THEN 'saas'
    WHEN (SELECT i FROM norm) ILIKE ANY(ARRAY[
        '%plumb%','%hvac%','%electrician%','%roof%','%dentist%','%dental%','%salon%','%barber%','% spa%',
        '%restaurant%','%cafe%','%coffee%','%gym%','%fitness%','%wellness%','%pet groom%','%pet store%',
        '%moving%','%fence%','%landscap%','%tree service%','%cleaning%','%auto repair%','%automotive repair%',
        '%real estate%','%hotel%','%hospitality%','%hospital%','%medical practice%','%medical clinic%',
        '%medical spa%','%veterinar%','%bakery%','%grocery%','%catering%','%photograph%','%florist%',
        '%pest control%','%locksmith%','%travel%','%leisure%','%recreation%','%food and bev%'
      ]) THEN 'local_business'
    WHEN (SELECT i FROM norm) ILIKE ANY(ARRAY[
        '%retail%','%apparel%','%fashion%','%consumer goods%','%cosmetic%','%beauty product%','%jewel%',
        '%consumer electronic%','%ecom%','%e-commerce%','%merchand%','%sporting goods%','%furniture store%',
        '%interior design brand%'
      ]) THEN 'ecom'
    WHEN (SELECT i FROM norm) ILIKE ANY(ARRAY[
        '%agency%','%consulting%','%consultant%','%manufacturing%','%machinery%','%staffing%','%recruit%',
        '%financial service%','%financial planner%','%bank%','%insurance%','%accounting%','%engineering%',
        '%contractor%','%construction%','%legal%','%attorney%','%lawyer%','%public relations%','%advertising%',
        '%marketing%','%information technology%','%telecommunication%','%semiconductor%','%chemical%',
        '%research%','%medical device%','%food production%','%pharmaceutical%','%biotech%','%logistics%',
        '%warehouse%','%wholesale%','%printing%','%publishing%','%media%','%broadcast%','%entertainment%',
        '%music%','%website%','%design service%','%cabinet%','%automotive%','%events service%','%training%',
        '%energy%','%mining%','%education%'
      ]) THEN 'b2b_agency'
    ELSE NULL
  END
$$;
```

A companion `core.fn_classify_industry_category` is also installed (category-level rollup).

**Python mirror (write-back hook):** `classify_industry()` in `backend/enrichment/contacts_writer.py` reproduces this exactly — same normalize step, same pattern tuples, same precedence — so app-side write-back and DB-side backfill agree. A unit test (`test_classify_industry_buckets`) pins the two together.

## 4. Documented decisions on ambiguous cases

| Industry / case | Decision | Rationale |
|---|---|---|
| `information technology & services` | **b2b** (NOT saas) | "services" = IT services/consulting, not a software product |
| `software and tech platforms` / `Software & Internet` | saas | Software product companies |
| `Consumer Financial Services and Fintech` | b2b | Per user decision (2026-08-05): fintech → b2b |
| `food production` | b2b | Manufacturing; vs `food & beverages` → NULL (could be restaurant/brand/producer) |
| `medical devices` | b2b | Manufacturing; vs `medical practice/clinic/spa` → local |
| `property and interior design brands` | ecom | "brands" = product companies (DTC home/interior) |
| `property and interior design` (no "brands") | NULL | Ambiguous (could be design services = b2b) |
| Compound Apollo categories (`A / B`, e.g. `Real Estate & Construction / Building Materials`) | **first-term wins** (→ local here) | Apollo lists primary sector first; some edge cases misclassify (construction under real-estate) — acceptable noise |
| `Boat tour agency`, `Travel agency` | b2b (via "agency") | Known minor over-match; tours are arguably local — acceptable noise |
| `Consumer Services`, `government administration`, `nonprofit` | NULL | Too vague / not a lead category — conservative |

**Principle:** when genuinely unsure → NULL (do not miscategorize). Precision on what we tag > forced coverage.

## 5. Measured coverage & accuracy

**Live counts on `core.person` (2026-08-11, ~8.77M persons total):**

| Bucket | Persons | % of tagged | % of all |
|---|---:|---:|---:|
| `local_business` | 2,870,452 | 52.6% | 32.7% |
| `b2b_agency` | 1,717,524 | 31.5% | 19.6% |
| `saas` | 585,739 | 10.7% | 6.7% |
| `ecom` | 283,588 | 5.2% | 3.2% |
| **Tagged subtotal** | **5,457,303** | 100% | **62.2%** |
| `NULL` (unclassified) | ~3,309,116 | — | 37.8% |

- **Coverage: ~62%** of all persons; ~38% NULL (ambiguous or bare leads not yet tied to a company).
- `local_business` is the largest bucket because every scraper.tech-origin lead is tagged `local_business` on write-back (app-layer origin tag), and scraper volume is high.
- **Precision (spot-checked samples): ~85–90%** on what's tagged. Known noise: compound categories (first-term-wins) and broad `%agency%`.

*Historical (2026-08-07, measured over the 1,695,661 companies that had an industry at the time): b2b_agency 35.0%, local_business 27.5%, unclassified 17.3%, saas 12.5%, ecom 7.7% → 82.7% coverage among industry-bearing companies.*

## 6. Where the industry comes from (per lead)

1. `core.company.industry` (primary — the linked company)
2. `core.source_row.raw_snapshot->>'industry'` (Apollo keeps the original even when company.industry is empty)
3. `core.person.custom_fields->>'industry_category'` (Blitz's category; underscore-format — the normalizer handles it)

The industry values themselves trace back to **Apollo** (the contacts DB is seeded from Apollo data) and **scraper.tech business categories**.

## 7. Edge cases & known noise (to refine over time)

- **Compound Apollo categories** (`A / B`): classify by first term → ~5% edge noise (construction/building-materials under real-estate compounds).
- **`%agency%` breadth**: catches "travel/boat tour agency" as b2b (really local).
- **Tuning candidates** (high-confidence, not yet added): `%dtc%`→ecom, `%physician%`/`%clinic%`→local, `%campground%`→local, `%paper%`/`%forest products%`→b2b. Adding these would push coverage higher but each is a small noise risk — add only if review agrees.

## 8. Reversibility & safety

- Backfill writes only `lead_universe` (row-neutral — person count never changes).
- Each backfill batch captured in `public.universe_backfill_audit(person_id, old_lead_universe, industry, source, batch_id)` → one-command rollback per batch.
- No schema change (column + index already existed). No paid tools. Never touches PgBouncer or other DBs (5433 lead-gen, outscraper).

## 9. Maintenance

- Patterns live in `core.fn_classify_industry` (one source of truth on the DB) **and** in the Python mirror `classify_industry()` in `backend/enrichment/contacts_writer.py`. To refine: edit **both** (keeping the unit test green) + re-run the backfill for affected rows.
- Iterate using: `SELECT industry, core.fn_classify_industry(industry) FROM core.company GROUP BY 1,2` to spot misclassifications.
- The forward hook (Phase 3) tags new leads as they're written, so the NULL pool shrinks over time without manual runs; re-running the backfill periodically catches the legacy backlog.

## 10. Phase roadmap

- **Phase 1 ✅:** rules developed + validated. Zero prod impact.
- **Phase 2 ✅:** `core.fn_classify_industry` (+ `core.fn_classify_industry_category`) created; NULL-person backfill run (batched, audited, VACUUM). Reversible.
- **Phase 3 ✅:** write-back hook live in `contacts_writer.classify_industry` (mirrors the DB fn); `raw_contact_collector` + `routes.py` thread `company_industry` so new enrichment self-classifies.
- **Filters:** Find People works (UI + `POST /api/enrichment/search/employees` `universe` field + Contacts DB API `GET /v1/people/search?universe=`).
