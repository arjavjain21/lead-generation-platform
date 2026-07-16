# ListBuilding — Clay Enrichment Step-by-Step Guide

- **Updated on:** 2026-07-16
- **Updated by:** @Arjav Jain
- **Changes since last version (2026-04-15):**
  - Provider cascade expanded: SmartProspect and WizLeads added (now 5 providers total)
  - Prospeo retired (no longer in the cascade)
  - New optional parameter `selected_providers` lets you restrict which providers run
  - API key authentication now supported alongside JWT
  - 0-email jobs now show a yellow warning banner in the tool's UI

---

## Loom demo

https://www.loom.com/share/4766fc92357441578f0163d1ee7da3cc

> **Note:** This video was recorded on an older version. The cascade order, provider list, and 0-email warning banner shown in the video may differ from what you see in the tool today. The core workflow (map fields, test on 10 rows, check `contacts`) is unchanged.

---

## What this does

This enrichment step helps you find contact data using any of these inputs:

- **Domain only** — get decision makers for a company
- **LinkedIn URL only** — enrich a specific person
- **Domain + full name** — enrich a specific person at a known company
- **Domain + LinkedIn URL** — verify a specific person at a known company

Behind the scenes (as of 2026-07-16), the system checks data in this order:

1. **Contacts DB** (internal database, free, always first)
2. **Blitz** (paid)
3. **SmartProspect** (paid, self-verifying)
4. **WizLeads** (paid, catch-all verified)
5. **BetterEnrich** (paid)

It stops at the first source that returns a valid email — so paid APIs are only called when the free internal DB has nothing.

Prospeo (a previous fallback) was retired on 2026-07-06.

---

## When to use each input type

### 1. Domain + Full Name

Use this when you know:
- the company domain
- the person's full name

Example:
- Domain: `google.com`
- Full Name: `John Doe`

This is the strongest combination for person-level enrichment.

### 2. Domain only

Use this when you only know the company domain and want any decision makers for that company.

Example:
- Domain: `google.com`

This can return multiple contacts (CEO, CTO, Founder, etc.).

### 3. LinkedIn URL only

Use this when you only have the person's LinkedIn profile URL and no domain.

Example:
- `https://linkedin.com/in/johndoe`

Useful for enriching one specific person when you don't know where they work.

### 4. Domain + LinkedIn URL

Use this when you have both the company domain and the person's LinkedIn URL.

This helps the system verify the person more accurately against the company.

---

## Step 1: Open the table you want to enrich

Open your table in Clay and click on the column where you want the enrichment result.

Then choose **Add Enrichment**.

---

## Step 2: Search for "LATEST" and select the Fetch Leads option

Search for **`(LATEST) Fetch Leads using Internal Endpoint (DB + Blitz + BetterEnrich)`**.

> **Note:** The card name in Clay may still reference the old provider set (DB + Blitz + BetterEnrich). The actual cascade now includes SmartProspect and WizLeads as well — the card name has not been updated on the Clay side, but the underlying API uses the full 5-provider cascade.

Select that enrichment card.

---

## Step 3: Map the correct input fields

This is the most important step. Only map the fields you actually have.

### If you have **Domain + Full Name**
Keep: **Domain**, **Full Name**
Remove: LinkedIn URL

### If you have **Domain only**
Keep: **Domain**
Remove: Full Name, LinkedIn URL

### If you have **LinkedIn URL only**
Keep: **LinkedIn URL**
Remove: Domain, Full Name

### If you have **Domain + LinkedIn URL**
Keep: **Domain**, **LinkedIn URL**
Remove: Full Name

Do not leave unused fields mapped — unmapped empty fields can confuse the routing logic.

---

## Step 4: Do not change extra settings

Once the correct columns are mapped:
- do **not** change anything else
- use the row condition to enrich only rows where your key field is present and valid

---

## Step 5: Save and test on a few rows

Click **Save**, then run it on a small sample first (e.g., **10 rows**).

This confirms the mapping is correct before you run the full list.

---

## Step 6: Check the result

After the run, look at the result for each row.

### If no contact is found
You may see `contacts: 0`. The system could not find a matching contact for that input combination. Try alternative inputs (see "What to do if nothing comes back" below).

### If a contact is found
You will see contact data returned for that row — typically `full_name`, `title`, `email`, `linkedin_url`, and the `email_source` (which provider found it).

---

## Step 7: Add the email as a column

Map the returned `email` field into a new column in your Clay table.

For rows where enrichment found a valid contact, the email will appear. For rows where nothing was found, the email field stays empty.

---

## Step 8: Push leads to campaign if needed

Once enrichment is done, you can push those leads directly into your campaign. You do **not** need to manually re-sync the data first — every successful enrichment is automatically written back to the internal Contacts DB.

---

## Optional: limit which providers run

By default, the system uses all 5 providers in cascade order. If you want to restrict the cascade to a subset (for example, only free providers, or skip a specific paid provider), you can pass `selected_providers` in the enrichment payload.

### Example values

| Goal | Value |
| --- | --- |
| Free tier only (no paid APIs) | `["contacts_db"]` |
| Free + Blitz only | `["contacts_db", "blitz"]` |
| Contacts DB + SmartProspect only | `["contacts_db", "smartprospect"]` |
| Skip only BetterEnrich | `["contacts_db", "blitz", "smartprospect", "wizleads"]` |

### Rules

- `contacts_db` is always allowed even if not listed (mandatory first step).
- Cannot be combined with `force_provider` — the API returns 400 if both are set.
- Empty list and unknown provider names are rejected with HTTP 400.
- Valid provider names: `contacts_db`, `blitz`, `smartprospect`, `wizleads`, `better_enrich`.

### How to use this in Clay

In the enrichment card's "Provider Selection" or "Advanced" section (depending on your Clay card version), pass `selected_providers` as a JSON array. If your Clay card doesn't expose this field, contact support to update the card.

---

## Authentication

The Clay enrichment card uses your account automatically — no extra setup needed inside Clay.

For direct API calls outside Clay (e.g., from a custom script or Postman), you can authenticate with either:

### Option A: API Key (recommended for integrations)
```
Header: X-API-Key: lgp_your_key_here
```
Generate one in the app under **Account → API Keys**. Keys do not expire.

### Option B: JWT Bearer token (alternative)
```
Header: Authorization: Bearer your_jwt_token
```
Obtained via `POST /api/auth/login` with your email + password. JWTs expire after 7 days.

Both methods work for the single-row `/api/enrichment/enrich` endpoint that Clay uses.

---

## What happens in the background

### Automatic source routing
The system automatically checks the best source available in this order:

1. **Contacts DB** (free, internal)
2. **Blitz** (paid)
3. **SmartProspect** (paid, self-verifying)
4. **WizLeads** (paid, catch-all verified)
5. **BetterEnrich** (paid)

You do not need to choose the source manually. The cascade stops at the first provider that returns a valid email.

### Automatic sync back to the database
When data is found, it is also synced back into the internal Contacts DB automatically. You may see a `sync_to_contacts_db` indicator in the response — that means the result has already been written back. You do not need to manually push the record back.

---

## How the API thinks about input modes

The enrichment endpoint automatically detects the mode based on what you send.

| Input | Mode |
| --- | --- |
| Domain only | `domain_only` |
| LinkedIn URL only (no domain) | `linkedin_only` |
| Company LinkedIn URL only (no domain) | `company_linkedin_only` |
| Domain + Full Name | `enhanced` |
| Domain + LinkedIn URL | `enhanced` |

---

## What kind of data can come back

A successful result can include:

- `full_name`, `first_name`, `last_name`
- `title`
- `email`
- `linkedin_url`
- `headline`
- `location_city`, `location_country`
- `email_source` (which provider found the email — e.g., `contacts_db_email`, `blitz_email`, `smartprospect_email`, `wizleads_email`, `better_enrich`)
- `contact_count`
- `data_sources` (which source was used for company, contacts, and emails)
- `sync_to_contacts_db` (status of write-back)

---

## 0-email warning banner (new as of 2026-07-13)

When you run a bulk enrichment job through the tool's UI (Upload Domains page), the Enrichment Jobs page now shows a prominent yellow banner on any job that completed with 0 emails on a non-empty input:

> ⚠ 0 emails found on a N-row input. Output CSV is likely empty. Retry the job or contact support.

The card also gets an orange left-border for visibility.

**What it means:** The cascade ran but found nothing — usually because of a pipeline issue rather than "no data exists for these domains". Click **Retry** to re-run, or contact support if multiple retries fail.

**Single-row API calls (the kind Clay makes) do not produce this banner** — it only applies to bulk CSV-upload jobs visible in the UI.

---

## What to do if nothing comes back

If you get `contacts: 0` for most rows:

1. Check the domain spelling (e.g., `google.com`, not `google`)
2. Make sure you mapped the correct fields
3. Remove fields you do not actually have
4. Try LinkedIn URL if available
5. Try domain only if person-level match fails

A bad mapping is one of the most common reasons enrichment returns nothing.

---

## Best practices

- Use the **fewest correct fields**, not every possible field
- Run **10 test rows first** before committing to the full list
- Use **Domain + Full Name** when possible — strongest person-level match
- Use **LinkedIn URL only** if that is your strongest identifier
- Do not manually sync results back — it happens automatically
- Check `contacts: 0` to spot rows with no match
- If you want to limit provider spend, use `selected_providers` to opt out of paid APIs

---

## Quick cheat sheet

| Scenario | Keep mapped | Remove |
| --- | --- | --- |
| Domain only | Domain | Full Name, LinkedIn URL |
| Domain + Full Name | Domain, Full Name | LinkedIn URL |
| LinkedIn only | LinkedIn URL | Domain, Full Name |
| Domain + LinkedIn | Domain, LinkedIn URL | Full Name |

---

## One-line summary

Choose the latest enrichment card, map only the fields you actually have, test on 10 rows, check whether contacts were found, then use the returned emails or push leads directly to your campaign. Database sync happens automatically.
