# 📞 Phone Numbers SOP — List Building Platform

**Version:** 2026-10-29 (adds /phone endpoint) · **Status:** LIVE · **Owner:** List Building Platform team
**Works for:** API users, Clay/Make integrations, CSV job users

---

## The one-line summary

Every enrichment now returns phone numbers automatically — first **free from our own
database (2.64M numbers)**, and when we don't have one, **QuickEnrich finds a fresh one
(credit charged only if found)**. You barely have to change anything.

---

## How phone lookup works now (the ladder)

When you enrich a person, the platform tries phone sources **top to bottom** and stops
at the first hit:

| # | Source | Cost | Speed | Notes |
|---|--------|------|-------|-------|
| 1 | **Our own database** (2.64M numbers: Blitz waves, QuickEnrich imports, outscraper…) | **FREE** | ~1 ms | Automatic — always on, no flag needed |
| 2 | **QuickEnrich phone-search** (NEW) | 1 credit **only if a phone is found** — a miss costs nothing | ~1–2 s | Automatic when you tick "include phone"; works worldwide |


**Why this is good:**
- If we already own the number → you get it free, instantly.
- If we don't → QuickEnrich tries first. Unlike Blitz, **QuickEnrich doesn't charge for
  a "not found"** — so the risky paid call only happens when QE also missed, for
  US numbers Blitz might still have.
- Net effect: **same or better phone coverage, noticeably lower paid usage.**

---

## How to use it

### A) Automatic + free (recommended default)

Just call `/enrich` the way you always do. Every contact in the response now has a
`phone` field.

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "X-API-Key: lgp_YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"domain": "acme.com"}'
```

> ⚠️ **Auth rule (avoid the "Invalid token" trap):** `lgp_...` API keys go in the
> **`X-API-Key`** header — NOT in `Authorization: Bearer` (that header is only for
> login JWTs from `/api/auth/login`; a `lgp_` key there returns 401 "Invalid token.").
> In Clay/Make/Postman, set the header name to exactly `X-API-Key`.

**Response (trimmed):**

```json
{
  "contacts": [
    {
      "full_name": "Sanjeev Kumar",
      "email": "skumar@servefed.com",
      "phone": "+17035178527",
      "email_source": "contacts_db"
    }
  ]
}
```

Works with **domain**, **LinkedIn URL**, or **domain + full name** — same request
shapes as before:

| You have | Body |
|---|---|
| Company domain | `{"domain": "acme.com"}` |
| Person's LinkedIn | `{"linkedin_url": "https://linkedin.com/in/john-doe"}` |
| Name + company | `{"domain": "acme.com", "full_name": "John Doe"}` |

**Free-only mode** (guaranteed zero paid calls — useful for cheap bulk sweeps):
add `"force_provider": "contacts_db"`.

### A+) The dedicated phone-only endpoint (BEST for Clay / spreadsheets)

When you want **just the phone number** and nothing else, use `/api/enrichment/phone`.
Give a LinkedIn URL **or** company domain + full name; get one small JSON back.

```bash
curl -H "X-API-Key: lgp_YOUR_API_KEY" \
  "https://listbuilding.eagleinfoservice.com/api/enrichment/phone?linkedin_url=https://www.linkedin.com/in/jeff-brown-2b832118"
```

**Response — this is the WHOLE thing:**

```json
{
  "found": true,
  "phone": "+13033328473",
  "phone_type": "stored",
  "phone_source": "stored",
  "looked_up_by": "linkedin_url",
  "person_name": "Jeff Brown",
  "email": "jbrown@globalmachinery.com"
}
```

| Parameter | Required | Notes |
|---|---|---|
| `linkedin_url` | one of* | full URL **or bare slug** both work |
| `domain` + `full_name` | one of* | exact-name match, like the cascade's contacts_db tier |
| `fresh` | no (default `true`) | `false` = contacts-DB only, guaranteed zero paid calls |

**Which tiers run — the mode matrix** (Blitz removed 2026-10-29 — plan
downgraded; it billed per call even on a miss):

| Params | Ladder | Cost guarantee |
|---|---|---|
| defaults | stored → QuickEnrich | free unless QE *finds* a number |
| `fresh=false` | contacts-DB only | zero paid calls |

**Which tier answered:** read `phone_source` in the response — `stored` (free),
`quickenrich` (fresh, 1 credit — only when found), or `""` when `found: false`.
A miss returns `{"found": false, "phone": ""}` — never an error. Speed: stored
hits are instant (~0.1 s); fresh QE lookups add ~1–2 s.

**Bulk runs:** the endpoint self-throttles to ~5 concurrent lookups (bulk
clients just run slower — no errors, no timeouts). If a response ever carries
`"no_phone_reason": "contacts_db_unavailable"`, the free tier was momentarily
down — **retry those rows later**, don't trust the empty phone.

POST variant takes the same fields as a JSON body.

### B) Maximum phone coverage (fresh lookups for people we don't have)

Add `"include_phone": true` — this turns on the QuickEnrich tier (and Blitz as the
final fallback) for contacts that came back **without** a stored phone:

```json
{"domain": "acme.com", "include_phone": true}
```

Optional: `"phone_for_all": true` tries a fresh lookup for **every** contact without a
phone (not just the first one per company).

### C) CSV / bulk jobs (Flow 1 & Flow 3, and the web UI)

Nothing new to learn — tick **"include phone"** in the UI (or pass `include_phone:
true` in the API). The output CSV's **`dm_phone`** column now fills from:

1. our stored numbers (free), then
2. QuickEnrich fresh lookups (credit only on hit), then
3. Blitz (rare).

Any job you re-run or resume picks this up automatically.

---

## Speed (measured on production, 2026-10-28)

| Call | Time |
|---|---|
| Domain lookup with stored phones | **~0.3–0.5 s** |
| Single person by LinkedIn (stored phone) | **~0.1 s** |
| With `include_phone: true` (fresh QE lookup per phone-less contact) | + ~1–2 s per contact looked up |

The stored-phone part adds ~1 millisecond — it is a database read, not an API call.

---

## Format

All phones come back in **E.164 format** (`+15550123`) — clean for CRM imports,
dialers, and spreadsheets. (QuickEnrich sometimes returns US numbers without the
country code, like `757-619-2853`; the platform fixes this to `+17576192853`
automatically.)

---

## Limitations (honest list)

1. **Stored phones only cover what we've collected** — about 2.6M of our ~30M people
   (~1 in 10 overall; much higher in US / recently-enriched segments). For everyone
   else you need `include_phone: true`.
2. **`"phone": ""` means "not on file"** — it is not an error and doesn't mean the
   person has no phone.
3. **One phone per person** — if we hold several, you get the best one (mobile first,
   then work, then other).
4. **Stored numbers are not re-verified** at read time — they were valid when stored.
5. **QuickEnrich tier needs a LinkedIn URL or name+company** on the contact (it keys
   on identity). Contacts with neither can't get a fresh lookup.
6. **QuickEnrich phone coverage is best in the US**; international hits happen but are
   less consistent. Blitz remains US-only.
7. **`include_phone` on single lookups**: the fresh-lookup tiers run on **bulk/Flow
   jobs and domain lookups**; a single LinkedIn-URL request returns the stored phone
   but won't trigger a fresh paid search.

---

## Cost cheat-sheet

| Scenario | What you pay |
|---|---|
| Contact has a stored phone | **Nothing** |
| `include_phone: true`, QE finds nothing | **Nothing** (miss is free) |
| `include_phone: true`, QE finds a phone | 1 QuickEnrich credit |
| QE misses + Blitz has it (US) | 1 Blitz record (only case Blitz fires now) |

We are on QuickEnrich **GTM Unlimited** — credits are effectively unlimited; the only
real constraint is the shared 6000 requests/min QuickEnrich budget, which the platform
rate-limits automatically.

---

## For operators (kill switches)

- `ENABLE_QUICKENRICH_PHONE_SEARCH=false` in `backend/.env` — removes the QE phone
  tier everywhere (stored + Blitz still work). Default: **on**.
- `ENABLE_PHONE_BUNDLE=false` — kills ALL fresh phone lookups (stored phones still
  flow). Default: on.
- Both need a `systemctl restart lead-generation-platform.service` (and if a scraper
  job is running: cancel it via API first, then restart the platform **and**
  `lead-gen-scraper-runner.service`, then resume the job — the runner is linked to the
  platform service and stops with it).

---

## Verified live (2026-10-28)

- Stored phone via domain lookup: `servefed.com` → `+17035178527` ✅
- Stored phone via LinkedIn lookup: `jeff-brown-2b832118` → `+13033328473` ✅
- QuickEnrich fresh hit: `shannon-long-09bb387` → `+17576192853` (mobile) ✅
- QuickEnrich free miss: `kearia-watson-orr-dha-a47386b0` → `""`, 0 credits, Blitz
  fallback fired ✅
- Flow-1 job with `include_phone: true`: stored phone filled free + QE phone-search
  call confirmed in the provider call log ✅
