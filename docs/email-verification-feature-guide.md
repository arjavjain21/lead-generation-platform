# Email Verification Feature - User Guide

## What is Email Verification?

When you enrich a list of domains or LinkedIn profiles, the platform now **automatically verifies** every email found in our internal database before showing it to you. This ensures you only get valid, deliverable email addresses.

---

## How It Works

### 1. Automatic Verification (Default Behavior)

When you run an enrichment job through the web UI or API:

1. **Email found in internal database** → We check if it's valid using Mailtester
2. **If email is valid** (codes: "ok", "mb") → You get the email with "Verified" status
3. **If email is invalid** (codes: "ko", etc.) → We remove it from our database and try the next provider (Blitz API) to find a better email
4. **If Mailtester is down** → We accept the email anyway (better to have an email than nothing)

### 2. Verification Status Codes

You'll see these codes in your output CSV:

| Code | Meaning | Action |
|------|---------|--------|
| `ok` | Email is valid and deliverable | ✅ Use with confidence |
| `mb` | Catch-all domain (accepts all mail) | ⚠️ Use with caution |
| `ko` | Email is invalid/doesn't exist | ❌ Removed, fallback to Blitz |
| `unavailable` | Mailtester service down | ✅ Accepted without verification |

---

## What's New in Your Output CSV

Your enriched CSV now includes these new columns:

| Column | Description | Example Values |
|--------|-------------|-----------------|
| `dm_email` | The email address found | `john@example.com` |
| `dm_email_source` | Where we found the email | `contacts_db`, `blitz`, `better_enrich` |
| `dm_email_verified` | Overall verification status | `yes`, `no`, `unknown` |
| `mailtester_code` | Mailtester response code | `ok`, `mb`, `ko`, `unavailable` |
| `mailtester_message` | Additional details | `Catch-All`, `MX Error`, `Limited` |

---

## Example Output

```
dm_email, dm_email_source, dm_email_verified, mailtester_code, mailtester_message
john@example.com, contacts_db, yes, ok, 
info@company.com, contacts_db, yes, mb, Catch-All
sales@startup.io, blitz, yes, ok, 
support@bad-domain.com, blitz, no, ko, No MX
```

---

## Important Notes

### "mb" (Catch-All) Status
- **What it means:** The domain accepts emails sent to any address (like `anything@company.com`)
- **Should you use it?** Use with caution - the email might not reach a specific person
- **Recommendation:** Consider these emails as "possible" but not guaranteed

### Invalid Emails Are Cleaned Up
- When we find an invalid email, we automatically **remove it from our internal database**
- This improves data quality for future enrichments
- No action needed on your part

### Default Behavior
- Email verification is **ON by default** for all enrichment jobs
- To disable it, you need to use the API directly with `validate_email=false`

---

## API Usage (For Developers)

If you're using the API directly, you can control email verification:

```json
POST /api/enrichment/jobs
{
    "upload_id": "your-upload-id",
    "domain_col": "website",
    "validate_email": true,  // true = verify emails (default), false = skip
    "max_results": 5
}
```

**Note:** If you omit `validate_email`, it defaults to `true` (verification enabled).

---

## FAQ

**Q: Do I need to change anything in my workflow?**
A: No! Email verification is automatic. Your enriched results will just have higher quality emails.

**Q: What if an email is marked "mb" (catch-all)?**
A: These emails might work, but aren't guaranteed to reach a specific person. Use your judgment.

**Q: Can I disable email verification?**
A: Yes, through the API with `validate_email: false`, but we recommend keeping it enabled for best results.

**Q: Does verification cost extra credits?**
A: No, email verification through Mailtester is free. You only pay for the enrichment APIs (Blitz, etc.).

**Q: What happens to invalid emails in your database?**
A: We automatically remove them so they don't appear in future enrichments. This keeps our database clean.

---

## Questions?

If you have questions about email verification or need help interpreting your results, contact the team.
