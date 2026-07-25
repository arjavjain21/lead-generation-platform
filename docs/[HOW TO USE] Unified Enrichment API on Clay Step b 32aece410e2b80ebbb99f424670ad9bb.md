# [HOW TO USE] Unified Enrichment  API on Clay Step by Step Guide - ListBuilding API Endpoint for Leads enrcihment

- Updated on: 2026-04-15
- Updated by: @Arjav Jain
- Changes made: SOP name updated

## Loom Demo

[https://www.loom.com/share/4766fc92357441578f0163d1ee7da3cc](https://www.loom.com/share/4766fc92357441578f0163d1ee7da3cc)

## What this does

This enrichment step helps you find contact data using any of these inputs:

- **Domain only**
- **LinkedIn URL only**
- **Domain + full name**
- **Domain + LinkedIn URL**

Behind the scenes (as of 03/21/2026), the system checks data in this order:

1. **Internal database**
2. **Blitz API**
3. **BetterEnrich**
4. Prospeo

It automatically uses the best source available. 

---

## When to use each input type

### 1. Use **Domain + Full Name**

Use this when you know:

- the company domain
- the person's full name

Example:

- Domain: `google.com`
- Full Name: `John Doe`

This is one of the best options when you want to enrich a specific person.

---

### 2. Use **Domain only**

Use this when you only know the company domain and want to get contacts for that company.

Example:

- Domain: `google.com`

This can return multiple contacts for that company.

---

### 3. Use **LinkedIn URL only**

Use this when you only have the person's LinkedIn profile URL.

Example:

- `https://linkedin.com/in/johndoe`

This is useful when you want to enrich one specific person but do not have the domain.

---

### 4. Use **Domain + LinkedIn URL**

Use this when you have both:

- the company domain
- the person's LinkedIn URL

This helps the system verify the person more accurately.

---

## Step by step: how to run it in the Clay UI

## Step 1: Open the table you want to enrich

Open your table and click on the column where you want the enrichment to run.

Then choose:

- **Add Enrichment**

---

## Step 2: Search for “LATEST” and select the Fetch Leads option

Search for **`(LATEST) Fetch Leads using Internal Endpoint (DB + Blitz + BetterEnrich)`** internal enrichment option.

This is the enrichment that uses:

- internal database
- Blitz API
- BetterEnrich

Select that option.

---

## Step 3: Map the correct input fields

This is the most important part.

You must only pass the fields you actually have.

### If you have **Domain + Full Name**

Keep:

- **Domain**
- **Full Name**

Remove:

- LinkedIn URL

### If you have **Domain only**

Keep:

- **Domain**

Remove:

- Full Name
- LinkedIn URL

### If you have **LinkedIn URL only**

Keep:

- **LinkedIn URL**

Remove:

- Domain
- Full Name

### If you have **Domain + LinkedIn URL**

Keep:

- **Domain**
- **LinkedIn URL**

Remove:

- Full Name

Do not leave unused fields mapped by mistake.

---

## Step 4: Do not change extra settings

Once the correct columns are mapped:

- do **not** change anything else
- use the condition to ensure you enrich only those columns that you need the data for

---

## Step 5: Save and test on a few rows

Click:

- **Save**

Then run it on a small sample first, such as:

- **10 rows**

This helps you confirm the mapping is correct before running it on the full list.

---

## Step 6: Check the result

After the run, look at the result for each row.

### If no contact is found

You may see something like:

- `contacts: 0`

That means the system could not find a matching contact for that input combination.

### If a contact is found

You will see contact data returned for that row.

---

## Step 7: Add the email as a column

If contact data is returned, you can add the email output into your table.

For rows where enrichment found a valid contact, the email will appear.

For rows where nothing was found, the email field will stay empty.

---

## Step 8: Push leads to campaign if needed

Once the enrichment is done, you can push those leads directly into your campaign if required.

You do **not** need to manually re-sync the data first.

---

## What happens in the background

## Automatic source routing

The system automatically checks the best source available:

1. **Internal DB first**
2. **Blitz API next**
3. **BetterEnrich last**

So you do not need to choose the source manually.

---

## Automatic sync back to the database

When data is found, it is also synced back into the contacts database automatically.

You may see indicators like:

- **Sync to Contacts DB**

That means the result has already been written back to the internal database.

You do **not** need to manually push the record back.

---

---

## How the API thinks about these modes

The enrichment endpoint automatically detects the mode based on what you send.

| Input | Mode |
| --- | --- |
| Domain only | `domain_only` |
| LinkedIn URL only | `linkedin_only` |
| Domain + Full Name | `enhanced` |
| Domain + LinkedIn URL | `enhanced` |

---

## What kind of data can come back

A successful result can include:

- full name
- first name
- last name
- title
- email
- LinkedIn URL
- headline
- location
- source used
- contact count
- sync status

---

## What to do if nothing comes back

If you get no results:

1. Check the domain spelling
2. Make sure you mapped the correct fields
3. Remove fields you do not actually have
4. Try LinkedIn URL if available
5. Try domain only if person-level match fails

A bad mapping is one of the most common reasons enrichment fails.

---

## Best practices

- Use the **fewest correct fields**, not every possible field
- Run **10 test rows first**
- Use **Domain + Full Name** when possible for person-level matching
- Use **LinkedIn URL only** if that is your strongest identifier
- Do not manually sync results back, it happens automatically
- Check `contacts = 0` to spot rows with no match

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

Choose the latest enrichment step, map only the fields you actually have, test on 10 rows, check whether contacts were found, and use the returned emails or push the leads directly to campaign, the database sync happens automatically.

---