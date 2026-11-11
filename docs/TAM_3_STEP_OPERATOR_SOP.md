# TAM → People → Verify: Operator SOP

**For:** list builders (no technical background needed)
**Site:** https://listbuilding.eagleinfoservice.com
**Status:** UI live; backend pieces activate after the next service restart (see the note at the bottom).

---

## The 3-step workflow in one picture

```
STEP 1 — FIND COMPANIES          STEP 2 — FIND PEOPLE            STEP 3 — VERIFY EMAILS
#/search                         #/people                        #/verify
Describe who you want to         Paste your company list +        Paste/upload emails, or click
reach → get a qualified          the job titles you want →       "Verify emails" on any finished
account list (CSV). Optional:    one enrichment job finds        job → every address is checked
one click chains into Step 2.    decision makers + emails.       as verified / catch-all / invalid.
```

You can start at any step — Step 2 and Step 3 also work with lists you already have.

---

## Step 1 — Find Companies (TAM): `/#/search`

**When to use:** you know the *kind of company* (and ideally the kind of person inside it) but don't have a list yet.

1. **Describe the company** — industry, size, country, type (any or all; each is optional).
2. **Describe who works there** — e.g. "Head of Marketing". This is the key filter: you get companies that *employ* someone like that, each with a match count.
3. **Max Companies** is your cost ceiling (default 500, hard cap 10,000). An empty result costs nothing.
4. *(Optional)* tick **"Also find contact emails"** — automatically starts Step 2 for every company found, **including companies that only have a LinkedIn page and no website**.

**Or search by example:** paste 3–5 companies you like (lookalike mode) → results are ranked by similarity (0–100 match score).

**Download:** the company CSV has `domain`, `linkedin_url`, `name`, industry, size, and match counts. Every company is also saved to your internal database automatically.

**Honest limits:** this is *discovered* TAM — every provider has caps, and "0 more results" can mean "cap reached", not "no more companies exist". The run card shows a ⚠ **capped** chip when the source had more to give. Directory scraping, Google-search harvesting, Apollo/DiscoLike-style lookalikes are NOT in this flow — they stay manual; paste any company list into Step 2 and it works the same.

---

## Step 2 — Find People & Enrich Emails: `/#/people`

**When to use:** you have a company list (from Step 1 or your own) and want decision makers with emails.

Scroll to **🏢 Scope to Companies** and click **Show**:

1. **Paste companies** — one per line, any mix:
   ```
   https://www.linkedin.com/company/acme     ← strongest identifier
   stripe.com                                 ← website domain
   Notion                                     ← company name (resolved conservatively — see below)
   ```
   The counter under the box tells you how many of each you pasted.
2. **Job titles to find** — e.g. `CEO, Founder, Head of Marketing`. Leave blank for the default business titles (Owner, CEO, VP, Director).
3. **Strict title match stays ON by default** — only contacts matching your titles (synonyms count: VP = Vice President) are kept. Uncheck only when you want volume over precision.
4. **Max decision-makers per company** — 3 / 5 / 10.
5. **Person country (optional)** — ⚠ applies to Blitz searches only; other sources match on their own data and may return other countries.
6. **QuickEnrich depth (optional)** — deeper pages find more people per company and use more credits (bounded, max 10).

Click **Find People at These Companies** → one job starts. Live progress shows companies processed + emails found; download the CSV from the panel or the **Jobs** page (`/#/jobs`).

**How identifiers are used (important):**
- Priority is **company LinkedIn URL → website domain → company name**. A row with several identifiers enriches by the LinkedIn page — the strongest identity — and we never silently switch companies.
- **Name-only rows are resolved conservatively.** If the name matches exactly one company, it resolves; if several companies share the name, the row is **flagged for you to review in the CSV** (`no_email_reason = ambiguous_company_name`, with the candidate companies listed) — never guessed. (Name resolution is switched on by an admin; while off, name-only rows are skipped with `name_unresolved`.)

**Reading the output CSV (the columns that matter):**

| Column | Meaning |
|---|---|
| `dm_full_name`, `dm_title` | who we found |
| `dm_linkedin_url` | their LinkedIn |
| `dm_email` + `dm_email_source` | the winning email + which source found it (contacts_db, smartprospect, quickenrich, leadocean, getleads, blitz, moltsets, wizleads) |
| `dm_email_verified` | yes / no / unknown (MailTester gate) |
| `mailtester_code` | ok / mb / ko / unavailable — the raw verdict |
| **`dm_email_catch_all`** | catch-all candidates we did NOT put in `dm_email` — kept for you, see Step 3 |
| `row_status`, `no_email_reason` | why a company produced nothing (skipped / no_linkedin / no_identifier / ambiguous_company_name / provider errors) |

**Honest limits:** "not found" ≠ "no such person exists" — it can also mean a provider cap was reached or that person isn't in any source's data. Per-company results are capped by your Max decision-makers setting and by provider limits (QuickEnrich returns up to 20 employees per page × depth). The job card's chips show when the strict-title gate filtered contacts and how many catch-all candidates were retained.

**Advanced:** `/#/domains` (Upload Domains) is the same engine with every knob exposed (provider checkboxes, per-column mapping, website-only mode, recent-hires, MX filters). Use it for routine work only if you need those; otherwise the People page is the simple path.

---

## Step 3 — Verify Emails: `/#/verify`

**When to use:** before you send. Paste/upload emails, or verify a chunk straight from a finished job.

1. **Paste** emails (one per line, commas/semicolons OK, `Name <a@b.com>` OK) **or upload a CSV** — the first column containing emails is detected automatically.
2. Options:
   - **Count catch-all as sendable** (OFF by default) — changes only the summary number, never deletes rows.
   - **Re-check** (OFF by default) — fresh checks even for recently verified emails; otherwise results within 30 days come from cache instantly.
3. Click **Verify.** ≤25 emails → instant results on screen. More → a background job with live progress, a Cancel button, and a CSV download.

**Or from any finished job:** the **✓ Verify emails** button on its Jobs-page card asks how many emails to verify (default 2,000, most recent first) and starts the job.

**What the statuses mean:**

| Status | Meaning | What you should do |
|---|---|---|
| **✓ Verified** | Mailbox exists and accepts mail for this person | Safe to send |
| **Catch-all** | The company's mail server accepts *everything* — we cannot confirm this exact person's inbox | Send with care (lower confidence); consider a suppression rule for bounces |
| **✕ Invalid** | Mailbox is dead | Do not send |
| **Unknown** | The check failed (e.g. validator briefly down) | Try again later — never counted as invalid |

**The CSV columns:** `original_email, email_norm, classification, verifier, code, message, verified_at, cache_hit, source_job_id`.

**Guarantees:** verification is **read-only** — nothing is written back to your contact database, no campaign or blocklist is touched. Catch-all rows are always kept in the export; they are never labelled "verified".

---

## Where jobs live

Everything (TAM runs, people jobs, verify jobs) is on **`/#/jobs`** with filters (Source: 🎯 TAM, 📨 TAM Contacts, ✓ Verify Emails, 📤 Upload, …). Jobs support cancel, restart/resume (finished work is never repeated), and CSV download. Tampering protection: chain jobs keep a restart-parity file so a resumed job resumes *exactly* where it stopped.

## Data sources (live truth, 11 Nov 2026)

Eight active enrichment tiers, in cascade order: **Contacts DB** (internal 30M+ person database, free) → **smartprospect** → **QuickEnrich** → **LeadOcean** → **GetLeads** → **Blitz** → **MoltSets** → **WizLeads**. BetterEnrich exists in code but is switched off by an admin kill switch; Prospeo is disabled. **MailTester is the separate verification gate** — not a ninth contact source. TAM account discovery uses the TAM providers (Blitz company data + GetLeads top-up), which are not the same as the email tiers. The Help page (`/#/help`) and `/api/enrichment/flows/help` show this list live from configuration.

## Sample input CSV for Step 2

```csv
domain,linkedin_url,company_name
,https://www.linkedin.com/company/acme-corp,Acme Corp
stripe.com,,Stripe
,,Notion
shopify.com,https://www.linkedin.com/company/shopify,Shopify
```

## FAQ

- **Why do I see fewer contacts than expected?** Either the strict-title gate filtered them (the job card shows how many), or a provider cap was hit, or the company simply has no matching people in any source. Check `row_status`/`no_email_reason` in the CSV.
- **Can I re-run the same list?** Yes — already-done companies are skipped on restart, duplicate persons are merged in your database, and verified emails within 30 days come from cache (no re-checking cost).
- **Is Clay needed?** No. Every step runs entirely on this site. Clay (or any tool) can still call our API if you want it for something special.
