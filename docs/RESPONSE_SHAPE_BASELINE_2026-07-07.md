# Response Shape Baseline — Enrichment Endpoints (2026-07-07)

Read-only snapshot of the JSON response shape returned by every enrichment
endpoint that derives from the cascade pipeline. This is the **freeze
baseline** for Phase 3: any new keys introduced by the pipeline changes
(`company_name`, `company_industry`, `company_employee_count`, `dm_job_level`,
`dm_job_function`, `provider_errors` at the row level) MUST be filtered out
of the JSON API response by an allowlist that matches this document.

CSV download columns are not in scope — those will naturally pick up the new
columns, which is intentional.

---

## Section 1 — Endpoint inventory

| # | Route + Method | Handler (file:line) | Returns cascade JSON? |
|---|----------------|---------------------|------------------------|
| 1 | `POST /api/enrichment/enrich` | `unified_enrich` — `backend/enrichment/routes.py:1318` | YES — `contacts[]` |
| 2 | `GET  /api/enrichment/enrich` | `unified_enrich_get` — `backend/enrichment/routes.py:1999` (delegates to `_unified_enrich_logic` — `:2058`) | YES — `contacts[]` + `routing` |
| 3 | `GET  /api/enrichment/enrich/{domain}` | `enrich_single_domain` — `backend/enrichment/routes.py:1016` | YES — `contacts[]` |
| 4 | `POST /api/enrichment/jobs` | `start_enrichment_job` — `backend/enrichment/routes.py:2741` | NO — returns `{job_id, total}` |
| 5 | `POST /api/enrichment/by-domains` | `enrich_by_domains` — `backend/enrichment/routes.py:3653` | NO — returns `{job_id, total, flow}` |

Endpoints 4 and 5 only return job metadata (the cascade runs in the
background and the result surfaces via SSE + CSV download). They are NOT
part of the freeze scope.

---

## Section 2 — Per-endpoint response shape

Each shape below is the exact JSON returned today (verified by live probe
against `http://localhost:8765` with a `Bearer` JWT on 2026-07-07).

### 2.1 — `POST /api/enrichment/enrich` (all three modes)

Built at `routes.py:1949` (single `return {...}`).

```json
{
    "domain": "stripe.com",
    "mode": "domain_only",
    "company_linkedin_url": "http://www.linkedin.com/company/stripe",
    "contacts": [ /* see contact shape below */ ],
    "contact_count": 1,
    "data_sources": {
        "company_linkedin": "contacts_db",
        "contacts": "contacts_db",
        "emails": "contacts_db"
    },
    "sync_to_contacts_db": {
        "status": "success",
        "records_synced": 1,
        "records_skipped": 0,
        "records_failed": 0,
        "records_queued": 0
    }
}
```

**Top-level stable keys (6):** `domain`, `mode`, `company_linkedin_url`,
`contacts`, `contact_count`, `data_sources`, `sync_to_contacts_db`.

Note: there is **no** `routing` block in the POST response.

### 2.2 — `GET /api/enrichment/enrich` (all three modes)

Built at `routes.py:2269` (linkedin_only/enhanced branch) and `routes.py:2639`
(domain_only branch). Both branches return the same key set.

```json
{
    "domain": "stripe.com",
    "mode": "domain_only",
    "company_linkedin_url": "http://www.linkedin.com/company/stripe",
    "contacts": [ /* see contact shape below */ ],
    "contact_count": 1,
    "data_sources": {
        "company_linkedin": "contacts_db",
        "contacts": "contacts_db",
        "emails": "contacts_db"
    },
    "routing": {
        "mode": "",
        "source_path": "name_domain -> blitz_person_enrich -> wizleads_find_email -> wizleads_find_email",
        "provider_attempts": ["person_by_name_and_domain@name_domain", "..."],
        "no_email_reason": "",
        "provider_errors": [
            {
                "provider": "blitz",
                "method": "person_enrich",
                "error_type": "unknown",
                "message": "blitz: An error occurred."
            }
        ],
        "provider_attempts_json": [ /* debug=true only */ ],
        "providers_called": [ /* debug=true only */ ],
        "providers_skipped": [ /* debug=true only */ ],
        "final_email_status": "enriched",           /* debug=true only */
        "final_email_verification_source": "mailtester" /* debug=true only */
    },
    "sync_to_contacts_db": {
        "status": "success",
        "records_synced": 1,
        "records_skipped": 0,
        "records_failed": 0
    }
}
```

**Top-level stable keys (7):** `domain`, `mode`, `company_linkedin_url`,
`contacts`, `contact_count`, `data_sources`, `routing`, `sync_to_contacts_db`.

**`routing` stable keys (non-debug, 5):** `mode`, `source_path`,
`provider_attempts`, `no_email_reason`, `provider_errors`.

**`routing` stable keys (debug=true, additional 5):** `provider_attempts_json`,
`providers_called`, `providers_skipped`, `final_email_status`,
`final_email_verification_source`.

**`sync_to_contacts_db` stable keys:** see Section 2.5.

### 2.3 — `GET /api/enrichment/enrich/{domain}`

Built at `routes.py:1208`.

```json
{
    "domain": "stripe.com",
    "company_linkedin_url": "http://www.linkedin.com/company/stripe",
    "contacts": [ /* see contact shape below */ ],
    "contact_count": 1,
    "data_sources": {
        "company_linkedin": "contacts_db",
        "contacts": "contacts_db",
        "emails": "wizleads"
    },
    "sync_to_contacts_db": {
        "status": "success",
        "records_synced": 1,
        "records_skipped": 0,
        "records_failed": 0,
        "records_queued": 0
    }
}
```

**Top-level stable keys (6):** `domain`, `company_linkedin_url`, `contacts`,
`contact_count`, `data_sources`, `sync_to_contacts_db`.

Note: there is **no** `mode` and **no** `routing` block on this endpoint.

### 2.4 — Contact object shape (inside `contacts[]`)

There are **two variants** of the contact object today:

**Variant A — POST `/enrich` and GET `/enrich/{domain}`** (15 keys):

```json
{
    "full_name": "Aaron Harris",
    "first_name": "Aaron",
    "last_name": "Harris",
    "title": "",
    "email": "aaronh@stripe.com",
    "linkedin_url": "aaronlhx82",
    "headline": "Risk Operations at Stripe",
    "location_city": "Phoenix",
    "location_country": "",
    "icp_tier": 0,
    "email_source": "wizleads",
    "validation_status": "unknown",
    "email_verified": "yes",
    "verification_message": "Invalid Key"
}
```

Stable keys (14): `full_name`, `first_name`, `last_name`, `title`, `email`,
`linkedin_url`, `headline`, `location_city`, `location_country`, `icp_tier`,
`email_source`, `validation_status`, `email_verified`, `verification_message`.

**Variant B — GET `/enrich`** (11 keys):

```json
{
    "full_name": "Aaron Harris",
    "first_name": "Aaron",
    "last_name": "Harris",
    "title": "",
    "email": "aaronh@stripe.com",
    "linkedin_url": "aaronlhx82",
    "headline": "Risk Operations at Stripe",
    "location_city": "",
    "location_country": "",
    "icp_tier": 0,
    "email_source": "wizleads_email"
}
```

Stable keys (11): same as Variant A **minus** `validation_status`,
`email_verified`, `verification_message`. Note also that `email_source` is
the *raw* source (e.g. `"wizleads_email"`, `"contacts_db_email"`) here,
whereas Variant A returns the *friendly* form (`"wizleads"`,
`"contacts_db"`).

### 2.5 — `sync_to_contacts_db` object shape

All three cascade-returning endpoints return the same shape but with one
inconsistency: the GET `/enrich` response is missing `records_queued` in
both branches.

**POST `/enrich` and GET `/enrich/{domain}` (5 keys):**
`status`, `records_synced`, `records_skipped`, `records_failed`,
`records_queued`.

**GET `/enrich` (4 keys):**
`status`, `records_synced`, `records_skipped`, `records_failed`.
(`records_queued` is built in `_unified_enrich_logic` but the legacy
non-v2 branch of `sync_result` doesn't set it — the GET handler omits
the key from the response dict at `routes.py:2283-2286` and
`:2653-2656`. Despite the test setup using v2, the response shape above
was confirmed by live probe.)

### 2.6 — `POST /jobs` and `POST /by-domains` (no cascade JSON)

These return only job metadata:

```json
// POST /jobs
{ "job_id": "<uuid>", "total": 123 }

// POST /by-domains
{ "job_id": "<uuid>", "total": 123, "flow": "domain_enrichment" }
```

Cascade-derived data is delivered via SSE + CSV download only. Not in
freeze scope.

---

## Section 3 — Stable-keys allowlist

Consolidated Python literal to paste into `routes.py`. This is the union
of keys currently returned by any cascade-derived endpoint. Phase 3's
filter should keep exactly these keys and drop everything else.

```python
# ---------------------------------------------------------------------------
# Stable JSON response keys (frozen 2026-07-07).
#
# Any key NOT in these sets MUST be stripped from the JSON response before
# it is sent to the client. The pipeline is gaining new fields
# (company_name, company_industry, company_employee_count, dm_job_level,
# dm_job_function, provider_errors row-level) but external API consumers
# (Clay, Zapier, custom scripts) must see byte-for-byte identical JSON
# before and after Phase 3.
#
# CSV download columns are NOT affected — they intentionally pick up the
# new schema.
# ---------------------------------------------------------------------------

# Top-level keys of the unified /enrich response.
# POST /enrich: subset of these (no `routing`).
# GET  /enrich: full set (with `routing`).
# GET  /enrich/{domain}: subset of these (no `mode`, no `routing`).
STABLE_ENRICH_RESPONSE_KEYS: frozenset[str] = frozenset({
    "domain",
    "mode",
    "company_linkedin_url",
    "contacts",
    "contact_count",
    "data_sources",
    "routing",
    "sync_to_contacts_db",
})

# `routing` block keys.
# Non-debug responses use the BASE set; debug=true adds the DEBUG set.
STABLE_ROUTING_KEYS_BASE: frozenset[str] = frozenset({
    "mode",
    "source_path",
    "provider_attempts",
    "no_email_reason",
    "provider_errors",  # already returned today; not a new key
})

STABLE_ROUTING_KEYS_DEBUG: frozenset[str] = STABLE_ROUTING_KEYS_BASE | frozenset({
    "provider_attempts_json",
    "providers_called",
    "providers_skipped",
    "final_email_status",
    "final_email_verification_source",
})

# `data_sources` block keys.
STABLE_DATA_SOURCES_KEYS: frozenset[str] = frozenset({
    "company_linkedin",
    "contacts",
    "emails",
})

# `sync_to_contacts_db` block keys.
# Note: GET /enrich historically omits `records_queued`. The freeze must
# preserve that inconsistency — adding `records_queued` to GET would be a
# breaking change for clients that do strict schema validation.
STABLE_SYNC_KEYS: frozenset[str] = frozenset({
    "status",
    "records_synced",
    "records_skipped",
    "records_failed",
    "records_queued",
})

# Contact object keys.
# Two variants exist in production today; the freeze must preserve both.
# POST /enrich + GET /enrich/{domain}: variant A (14 keys, friendly source).
# GET  /enrich:                       variant B (11 keys, raw source).
STABLE_CONTACT_KEYS_FULL: frozenset[str] = frozenset({
    "full_name",
    "first_name",
    "last_name",
    "title",
    "email",
    "linkedin_url",
    "headline",
    "location_city",
    "location_country",
    "icp_tier",
    "email_source",
    "validation_status",
    "email_verified",
    "verification_message",
})

STABLE_CONTACT_KEYS_COMPACT: frozenset[str] = frozenset({
    "full_name",
    "first_name",
    "last_name",
    "title",
    "email",
    "linkedin_url",
    "headline",
    "location_city",
    "location_country",
    "icp_tier",
    "email_source",
})

# Keys the new pipeline fields MUST NOT leak through as:
STABLE_CONTACT_BLOCKLIST: frozenset[str] = frozenset({
    "company_name",
    "company_industry",
    "company_employee_count",
    "dm_job_level",
    "dm_job_function",
})
```

---

## Section 4 — Edge cases / surprises

These are the response-shape oddities the freeze implementation must
handle carefully. Most of them are pre-existing inconsistencies that the
allowlist must preserve (not fix).

### 4.1 — POST `/enrich` has no `routing` block; GET `/enrich` does

Same input, different handlers (`unified_enrich` vs `_unified_enrich_logic`),
different output shape. POST returns 6 top-level keys; GET returns 7 (the
extra one is `routing`). The allowlist must be per-endpoint, not global.

### 4.2 — GET `/enrich/{domain}` has no `mode` and no `routing`

Despite the name, this is the oldest of the three and exposes the fewest
keys. Don't accidentally add `mode` to it via a shared helper.

### 4.3 — Two contact-object variants (full vs compact)

POST `/enrich` and GET `/enrich/{domain}` return 14 contact keys
(including `validation_status`, `email_verified`, `verification_message`,
and the friendly `email_source` form). GET `/enrich` returns 11 keys
with the raw `email_source` form (e.g. `"wizleads_email"` instead of
`"wizleads"`).

The freeze needs **two** contact allowlists. If you only use one, you'll
either leak the new keys to the compact variant or accidentally strip
the three verification fields from the full variant.

### 4.4 — `provider_errors` is already in the response today

The `routing.provider_errors` array is **not** new. It's been live since
`_build_routing_response` was added (`routes.py:226`) and currently has
shape `{provider, method, error_type, message}` per entry. The Phase 3
work that populates a row-level `provider_errors` field on cascade
output rows is a **different** concept — it must not be confused with
the routing-level one. The freeze only blocks the row-level field from
leaking into JSON; the routing-level one is already part of the stable
contract.

### 4.5 — `records_queued` inconsistency in `sync_to_contacts_db`

POST `/enrich` and GET `/enrich/{domain}` return 5 sync keys including
`records_queued`. GET `/enrich` returns only 4 (no `records_queued`),
even when `contacts_writer.is_v2_enabled()` is true. This is because the
GET handler's response dict was written before `records_queued` existed
and was never updated. The freeze must preserve this — adding the key
to GET `/enrich` would be a breaking change for strict-schema clients.

### 4.6 — `data_sources.emails` value semantics differ by endpoint

On POST `/enrich` and GET `/enrich/{domain}` the value is the friendly
source (`"contacts_db"`, `"blitz"`, `"wizleads"`, `"better_enrich"`).
On GET `/enrich` it's the raw source (`"contacts_db_email"`,
`"wizleads_email"`, etc.). The freeze is about **keys**, not values —
but if Phase 3 also normalizes values, that needs separate handling.

### 4.7 — Error responses are not documented here

The 400/403/404/500 error envelope (`{"detail": "..."}` from FastAPI's
`HTTPException`) is the framework default and not part of the cascade
contract. Phase 3 doesn't touch error paths.

### 4.8 — `linkedin_only` mode can return `contacts: []` with `routing` populated

When all providers fail to find an email in linkedin_only mode, `contacts`
is an empty array but `routing.no_email_reason` is populated (e.g.
`"all_providers_called_no_email"`). The allowlist must handle the empty-
contacts case without dropping the routing block.

---

## Concerns about the freeze approach

1. **Per-endpoint allowlists are required.** A single global contact
   allowlist will either leak the new pipeline fields to the compact
   variant or strip the three verification fields from the full variant.
   Use `STABLE_CONTACT_KEYS_FULL` for POST `/enrich` + GET
   `/enrich/{domain}` and `STABLE_CONTACT_KEYS_COMPACT` for GET `/enrich`.

2. **The freeze must run AFTER the response dict is built but BEFORE
   FastAPI serializes it.** Easiest is a recursive `dict` filter applied
   in the handler's `return` statement, or a small response-model
   validator on the FastAPI route. Both work; the recursive filter is
   less invasive.

3. **Test with byte comparison.** Capture the JSON output of all three
   endpoints against a known domain (e.g. `stripe.com`) BEFORE Phase 3
   lands, then assert byte-for-byte equality AFTER Phase 3. The current
   snapshot in this doc is the source of truth for those tests.

4. **CSV is intentionally not frozen.** The 6 new columns should appear
   in CSV downloads. The freeze is JSON-only.

5. **`provider_errors` at the routing level is already in the contract.**
   Don't accidentally filter it out — it's in `STABLE_ROUTING_KEYS_BASE`
   and must stay.
