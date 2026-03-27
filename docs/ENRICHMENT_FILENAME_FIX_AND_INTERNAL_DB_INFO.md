# Enrichment System Questions & Answers - March 10, 2026

## Question 1: Are We Using an Internal Database for Enrichment?

### Answer: Yes, But as a **Fallback**

The enrichment system uses a **tiered approach**:

#### Primary: Blitz API (api.blitz-api.ai)
- Domain → Company LinkedIn URL
- Company LinkedIn → Decision Makers
- Person LinkedIn → Work Email

#### Fallback: Contacts DB (leadsdatabase.cc)
- Used when Blitz API doesn't find results
- **NOT an internal database** - it's an external service at `https://leadsdatabase.cc`
- Accessed via API calls with `CONTACTS_API_TOKEN`

### How the Fallback Works

**Code Location:** `/var/www/lead-generation-platform/backend/enrichment/pipeline.py`

For each domain being enriched:

1. **Blitz API** tries to find emails (primary method)
2. **If Blitz fails**, Contacts DB is tried in this order:
   - By LinkedIn URL (if available)
   - By person's name + domain (from Blitz results)
   - By input row name + domain (if name columns provided)

### Evidence from Code

```python
# pipeline.py lines 148-199
# Step 1: Blitz email lookup
# Step 2: Contacts DB by LinkedIn URL
contacts_linkedin = await contacts_client.get_person_by_linkedin(linkedin_url)

# Step 3: Contacts DB by person's Blitz-derived name + domain
contacts_name = await contacts_client.get_person_by_name(
    full_name=full_name,
    domain=domain
)

# Step 4: Contacts DB by input row name + domain
contacts_input = await contacts_client.get_person_by_name(
    full_name=input_row_full_name,
    domain=domain
)
```

### Configuration

```bash
# In backend/.env
CONTACTS_API_BASE_URL=https://leadsdatabase.cc
CONTACTS_API_TOKEN=<your-token-here>
```

### Summary

- **Primary source:** Blitz API (paid service)
- **Fallback source:** Contacts DB (leadsdatabase.cc) - external API, not internal
- **No internal database** is used for enrichment data
- The system is designed to maximize email discovery by using multiple sources

---

## Question 2: Why Do Enrichment Jobs Show UUIDs Instead of Filenames?

### Problem Identified

The enrichment job list was showing **UUIDs** (like `70f8ac67-0780-4575-befc-48a964ccdffd`) instead of the actual CSV filenames (like `australia-marketing-agency.csv`).

### Root Cause

**Database Schema:**
- `filename` column: Contains the upload_id (UUID)
- `original_filename` column: Contains the actual CSV filename

**Frontend Behavior:**
- Frontend React app displays the `filename` field
- Was showing the UUID instead of the user-friendly filename

### Example from Database

```
job_id: 096b75e5-...
filename: 70f8ac67-0780-4575-befc-48a964ccdffd  ← UUID (upload_id)
original_filename: australia-marketing-agency_3a8e411c.csv  ← Actual filename
```

### Solution Implemented

**Modified Files:**
1. `/var/www/lead-generation-platform/backend/enrichment/routes.py`
2. `/var/www/lead-generation-platform/backend/main.py`

**Changes:**
- API now returns `original_filename` as the `filename` field
- Frontend will automatically display the actual CSV filename
- Added fallback for jobs created before this fix

**Code Changes:**

```python
# In routes.py list_enrichment_jobs()
for job in jobs:
    # Use original_filename as the primary filename for display
    if job.get("original_filename"):
        job["filename"] = job["original_filename"]
        job["display_filename"] = job["original_filename"]
```

### What You'll See Now

**Before:**
```
Query/File: 70f8ac67-0780-4575-befc-48a964ccdffd
Status: done
Progress: 5080/5080
```

**After:**
```
Query/File: australia-marketing-agency_3a8e411c.csv
Status: done
Progress: 5080/5080
```

### First 32 Characters Note

The actual filename is now shown in full, not truncated to 32 characters. If you want truncation, that would be a frontend change.

Current behavior:
- Full filename: `australia-marketing-agency_3a8e411c.csv`
- Displayed as: `australia-marketing-agency_3a8e411c.csv` (complete)

If you need it truncated to 32 chars with "..." for very long filenames, that would require modifying the React frontend source code (which we don't have access to).

---

## Deployment Status

### ✅ All Changes Deployed

**Files Modified:**
1. `backend/enrichment/routes.py` - Updated `/jobs` and `/jobs/{job_id}` endpoints
2. `backend/main.py` - Updated combined `/jobs` and `/jobs/{job_id}` endpoints
3. Service restarted successfully

**No Frontend Changes Required:**
- Backend API now returns user-friendly filenames in the `filename` field
- Frontend will automatically display the correct filenames

**Action Required:**
- **Refresh your browser** (Ctrl+Shift+R or Cmd+Shift+R)
- The enrichment job list should now show actual CSV filenames

---

## Technical Details

### Why This Approach?

**Safe and Backwards Compatible:**
- Keeps UUID in database as `filename` (internal identifier)
- Returns user-friendly filename via API (for display)
- No database migration required
- No breaking changes to existing data

**Benefits:**
- Users can now identify which file was used for enrichment
- Better user experience - shows meaningful filenames
- Frontend doesn't need any changes
- Works for new and existing jobs (if `original_filename` is present)

### For Old Jobs Without original_filename

Jobs created before the `original_filename` feature was added will show:
- `uploaded_file_{job_id[:8]}.csv` if filename is a UUID
- Otherwise the existing filename with `.csv` appended

---

## Testing

### How to Verify the Fix

1. Go to **Domain Enrichment** page
2. Look at **Job History** table
3. Check the **Query/File** column
4. Should see actual CSV filenames (e.g., `australia-marketing-agency_3a8e411c.csv`)
5. **NOT** UUIDs (e.g., `70f8ac67-0780-4575-befc-48a964ccdffd`)

### Expected Results

**New Jobs:**
- Will show the original uploaded filename
- Example: `my-companies-list.csv`

**Recent Jobs (with original_filename):**
- Will show the original uploaded filename
- Example: `australia-marketing-agency_3a8e411c.csv`

**Very Old Jobs (without original_filename):**
- Will show a generated name based on job ID
- Example: `uploaded_file_096b75e5.csv`

---

## Summary

### Question 1: Internal Database?
**Answer:** No internal database. System uses:
- **Primary:** Blitz API
- **Fallback:** Contacts DB (leadsdatabase.cc) - external API

### Question 2: Filename Display?
**Answer:** Fixed! API now returns actual filenames instead of UUIDs.

**What Changed:**
- Backend now sends `original_filename` as the `filename` field
- Frontend automatically displays user-friendly filenames
- Service restarted successfully
- Just refresh your browser to see the changes

---

**Date:** March 10, 2026 - 09:50 UTC
**Status:** ✅ Deployed and active
**Action:** Refresh browser to see actual filenames in enrichment job list
