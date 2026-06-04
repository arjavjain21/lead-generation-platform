# Unified LinkedIn Enrichment - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable CSV upload with personal LinkedIn URLs and/or company LinkedIn URLs, with smart cascade logic (personal → company fallback) and expand-to-multiple-rows output.

**Architecture:**
- Backend: New `/api/enrichment/by-linkedin-v2` endpoint that accepts two optional columns (personal, company)
- Processing: Per-row cascade logic - personal URL primary, company waterfall fallback
- Output: Can expand 1 input row → N output rows (one per decision maker from waterfall)
- Blitz API: `waterfall_icp_search()` for company → decision makers cascade

**Tech Stack:** Python (FastAPI), httpx, SQLite, JavaScript (vanilla frontend)

---

## File Structure

| File | Purpose |
|------|---------|
| `backend/enrichment/blitz_client.py` | Already has `waterfall_icp_search()` - no changes needed |
| `backend/enrichment/list_builder.py` | Add `run_unified_linkedin_enrichment()` function |
| `backend/enrichment/routes.py:3182-3273` | Add new `POST /api/enrichment/by-linkedin-v2` endpoint |
| `frontend/index.html` | Add company column dropdown + max decision makers |

---

## Tasks

### Task 1: Add Company URL Type Detection

**Files:**
- Modify: `backend/enrichment/list_builder.py:1-50` (add helper function near imports)

- [ ] **Step 1: Add URL type detection helper**

Add these functions at the top of `list_builder.py` (after imports):

```python
def _is_company_linkedin_url(url: str) -> bool:
    """Check if LinkedIn URL is a company page (not personal profile)."""
    if not url or "linkedin.com" not in url:
        return False
    return "/company/" in url or "/school/" in url or "/organization/" in url

def _is_personal_linkedin_url(url: str) -> bool:
    """Check if LinkedIn URL is a personal profile."""
    if not url or "linkedin.com" not in url:
        return False
    return "/in/" in url

def _detect_linkedin_url_type(url: str) -> str:
    """Detect if URL is 'personal', 'company', or 'unknown'."""
    if _is_company_linkedin_url(url):
        return "company"
    elif _is_personal_linkedin_url(url):
        return "personal"
    return "unknown"
```

- [ ] **Step 2: Commit**

```bash
cd /var/www/lead-generation-platform/backend
git add enrichment/list_builder.py
git commit -m "feat: add LinkedIn URL type detection helpers"
```

---

### Task 2: Create Unified Enrichment Function

**Files:**
- Modify: `backend/enrichment/list_builder.py` (add new function at end of file)

- [ ] **Step 1: Read current end of list_builder.py to find insertion point**

Run: `tail -50 /var/www/lead-generation-platform/backend/enrichment/list_builder.py`
Expected: Ends with `return all_output` from `run_linkedin_enrichment()`

- [ ] **Step 2: Add new unified enrichment function**

Append this to `backend/enrichment/list_builder.py` (after line 956):

```python
# =============================================================================
# UNIFIED LINKEDIN ENRICHMENT (Personal + Company URLs)
# =============================================================================

CASCADE_TIER_1 = {
    "include_title": ["Owner", "CEO", "Founder", "Co-Founder", "President"],
    "exclude_title": ["assistant", "intern", "junior", "associate"],
    "location": ["WORLD"],
    "include_headline_search": False,
}
CASCADE_TIER_2 = {
    "include_title": ["CMO", "VP Marketing", "VP Sales", "Chief Revenue Officer",
                      "Chief Marketing Officer", "VP of Marketing", "VP of Sales"],
    "exclude_title": ["assistant", "intern", "junior"],
    "location": ["WORLD"],
    "include_headline_search": False,
}
CASCADE_TIER_3 = {
    "include_title": ["Director of Marketing", "Director of Sales", "Head of Marketing",
                      "Head of Sales", "Head of Growth", "Marketing Director", "Sales Director"],
    "exclude_title": ["assistant", "intern", "junior"],
    "location": ["WORLD"],
    "include_headline_search": False,
}
DEFAULT_CASCADE = [CASCADE_TIER_1, CASCADE_TIER_2, CASCADE_TIER_3]


async def _enrich_by_company_waterfall(
    blitz_http: httpx.AsyncClient,
    company_url: str,
    cascade: list[dict[str, Any]],
    max_dms: int = 5,
    semaphore: asyncio.Semaphore = None,
) -> list[dict[str, Any]]:
    """
    Use Blitz waterfall_icp_search to find decision makers from company LinkedIn URL.

    Returns list of person dictionaries with: first_name, last_name, full_name,
    title, job_level, linkedin_url, email, verified_email.
    """
    if not semaphore:
        semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)

    results = []
    async with semaphore:
        try:
            response = await blitz_client.waterfall_icp_search(
                blitz_http,
                company_linkedin_url=company_url,
                cascade=cascade,
                max_results=max_dms,
            )

            if response.get("results"):
                for result in response["results"]:
                    person = result.get("person", {})
                    first_name = person.get("first_name", "")
                    last_name = person.get("last_name", "")
                    full_name = person.get("full_name", "") or f"{first_name} {last_name}".strip()

                    # Try verified_email first, fallback to emails list
                    verified_email = person.get("verified_email", "")
                    if not verified_email and person.get("emails"):
                        verified_email = person[0].get("email", "") if isinstance(person.get("emails"), list) else ""

                    results.append({
                        "first_name": first_name,
                        "last_name": last_name,
                        "full_name": full_name,
                        "title": person.get("title", ""),
                        "job_level": _get_job_level(person.get("title", "")),
                        "linkedin_url": person.get("linkedin_url", ""),
                        "email": verified_email,
                        "verified_email": verified_email,
                        "headline": person.get("headline", ""),
                        "location_city": person.get("location", {}).get("city", "") if isinstance(person.get("location"), dict) else "",
                        "location_country": person.get("location", {}).get("country_code", "") if isinstance(person.get("location"), dict) else "",
                        "icp_tier": result.get("icp", ""),
                        "ranking": result.get("ranking", 0),
                    })
        except Exception as e:
            logger.debug("Company waterfall search failed for %s: %s", company_url, e)

    return results


def _get_job_level(title: str) -> str:
    """Map title to job level for output column."""
    title_lower = title.lower()
    if any(t in title_lower for t in ["owner", "ceo", "founder", "co-founder", "president"]):
        return "owner"
    elif any(t in title_lower for t in ["chief", "vp ", "vice president"]):
        return "vp"
    elif "director" in title_lower or "head of" in title_lower:
        return "director"
    elif any(t in title_lower for t in ["manager", "lead", "head"]):
        return "manager"
    return "other"


async def run_unified_linkedin_enrichment(
    rows: list[dict[str, Any]],
    personal_col: Optional[str] = None,
    company_col: Optional[str] = None,
    max_dms: int = 5,
    include_company: bool = True,
    on_progress: Optional[Callable[[dict[str, Any]], None]] = None,
) -> list[OutputRow]:
    """
    Unified enrichment for CSV with personal and/or company LinkedIn URLs.

    Args:
        rows: List of input rows from CSV
        personal_col: Column name containing personal LinkedIn URLs (optional)
        company_col: Column name containing company LinkedIn URLs (optional)
        max_dms: Max decision makers to return from company waterfall (default 5)
        include_company: Include company details in output
        on_progress: Callback for progress updates

    Returns:
        List of enriched output rows (can be > len(rows) due to waterfall expansion)
    """
    semaphore = asyncio.Semaphore(LINKEDIN_CONCURRENCY)
    blitz_http = httpx.AsyncClient()
    contacts_http = httpx.AsyncClient()

    all_output: list[OutputRow] = []
    total = len(rows)

    for idx, row in enumerate(rows):
        personal_url = str(row.get(personal_col, "")).strip() if personal_col else ""
        company_url = str(row.get(company_col, "")).strip() if company_col else ""

        # Skip if neither URL is present
        if not personal_url and not company_url:
            output_row = {**row, **_empty_enriched(), "row_status": STATUS_SKIPPED}
            all_output.append(output_row)
            if on_progress:
                on_progress({
                    "index": idx,
                    "total": total,
                    "status": STATUS_SKIPPED,
                    "email_found": False,
                    "source_counts": {},
                })
            continue

        # Track if we found any data
        found_data = False

        # Step 1: Try personal URL enrichment
        if personal_url and "linkedin.com" in personal_url:
            person_result = await _enrich_single_linkedin(
                blitz_http, contacts_http, row, personal_url,
                include_company=include_company, semaphore=semaphore
            )

            # If we got email from personal URL, use it
            if person_result.get("dm_email") or person_result.get("row_status") == STATUS_ENRICHED:
                all_output.append(person_result)
                found_data = True

                if on_progress:
                    source = person_result.get("dm_email_source", "")
                    provider = _normalize_source(source) if source else "unknown"
                    on_progress({
                        "index": idx,
                        "total": total,
                        "status": person_result.get("row_status", STATUS_ENRICHED),
                        "email_found": bool(person_result.get("dm_email")),
                        "source_counts": {provider: 1},
                    })
            elif person_result.get("row_status") == STATUS_NO_CONTACTS:
                # Personal URL found but no contacts - continue to company fallback
                # Don't output yet, try company
                pass

        # Step 2: If personal failed or not provided, try company waterfall
        if not found_data and company_url and "linkedin.com" in company_url:
            # Try company waterfall to get decision makers
            company_dms = await _enrich_by_company_waterfall(
                blitz_http, company_url, DEFAULT_CASCADE, max_dms, semaphore
            )

            if company_dms:
                # Create one output row per decision maker
                for dm in company_dms:
                    output_row = {
                        **row,
                        "dm_first_name": dm.get("first_name", ""),
                        "dm_last_name": dm.get("last_name", ""),
                        "dm_full_name": dm.get("full_name", ""),
                        "dm_title": dm.get("title", ""),
                        "dm_job_level": dm.get("job_level", ""),
                        "dm_linkedin_url": dm.get("linkedin_url", ""),
                        "dm_email": dm.get("email", ""),
                        "dm_email_verified": "yes" if dm.get("verified_email") else "no",
                        "dm_headline": dm.get("headline", ""),
                        "dm_location_city": dm.get("location_city", ""),
                        "dm_location_country": dm.get("location_country", ""),
                        "dm_icp_tier": dm.get("icp_tier", ""),
                        "company_linkedin_url": company_url,
                        "row_status": STATUS_ENRICHED if dm.get("email") else STATUS_NO_CONTACTS,
                        "dm_email_source": SOURCE_BLITZ_COMPANY,
                    }
                    if include_company:
                        output_row["company_name"] = _extract_company_name_from_url(company_url)

                    all_output.append(output_row)
                    found_data = True

                    if on_progress:
                        on_progress({
                            "index": idx,
                            "total": total,
                            "status": STATUS_ENRICHED,
                            "email_found": bool(dm.get("email")),
                            "source_counts": {"blitz_company": 1},
                        })
            else:
                # Company waterfall also failed
                output_row = {**row, **_empty_enriched(), "row_status": STATUS_NOT_FOUND}
                output_row["company_linkedin_url"] = company_url if company_url else ""
                all_output.append(output_row)
                found_data = True

                if on_progress:
                    on_progress({
                        "index": idx,
                        "total": total,
                        "status": STATUS_NOT_FOUND,
                        "email_found": False,
                        "source_counts": {},
                    })

        # Step 3: If no URLs at all, mark as skipped
        if not found_data:
            output_row = {**row, **_empty_enriched(), "row_status": STATUS_SKIPPED}
            all_output.append(output_row)

    await blitz_http.aclose()
    await contacts_http.aclose()

    return all_output


def _extract_company_name_from_url(url: str) -> str:
    """Extract company name from LinkedIn company URL."""
    if not url:
        return ""
    # URL format: https://www.linkedin.com/company/acme-corp
    parts = url.split("/company/")
    if len(parts) > 1:
        name = parts[-1].rstrip("/")
        # Decode URL encoding
        import urllib.parse
        return urllib.parse.unquote(name.replace("-", " ").replace("_", " ").title())
    return ""


def _normalize_source(source: str) -> str:
    """Normalize source string to provider name."""
    if "contacts" in source.lower():
        return "contacts_db"
    elif "blitz" in source.lower():
        return "blitz"
    return source
```

- [ ] **Step 3: Run linting to check for syntax errors**

Run: `cd /var/www/lead-generation-platform/backend && python -m py_compile enrichment/list_builder.py`
Expected: No output (success)

- [ ] **Step 4: Commit**

```bash
cd /var/www/lead-generation-platform
git add backend/enrichment/list_builder.py
git commit -m "feat: add unified LinkedIn enrichment with company waterfall fallback"
```

---

### Task 3: Add New API Endpoint

**Files:**
- Modify: `backend/enrichment/routes.py` (add new endpoint)

- [ ] **Step 1: Find where to add the new endpoint**

Run: `grep -n "by-linkedin" /var/www/lead-generation-platform/backend/enrichment/routes.py | head -5`
Expected: Shows the existing `/by-linkedin` endpoint location

- [ ] **Step 2: Read context around the existing endpoint**

Run: `sed -n '3175,3280p' /var/www/lead-generation-platform/backend/enrichment/routes.py`
Expected: Shows the existing `POST /api/enrichment/by-linkedin` endpoint

- [ ] **Step 3: Add new `/by-linkedin-v2` endpoint**

Add this new endpoint AFTER the existing `/by-linkedin` endpoint (after line 3273):

```python
@router.post("/by-linkedin-v2", response_model=dict)
async def enrich_linkedin_v2(
    upload_id: str = Body(...),
    personal_linkedin_col: Optional[str] = Body(None),
    company_linkedin_col: Optional[str] = Body(None),
    max_dms: int = Body(5),
    include_company: bool = Body(True),
):
    """
    Unified LinkedIn enrichment - supports both personal and company URLs.

    Upload a CSV file, select columns for personal LinkedIn URLs and/or
    company LinkedIn URLs, and get enriched decision-maker contacts.

    Processing logic:
    1. Personal URL → Enrich person directly
    2. If personal fails → Company URL → Waterfall cascade to find DMs
    3. Output can expand: 1 row → N rows (one per DM from waterfall)
    """
    if not personal_linkedin_col and not company_linkedin_col:
        raise HTTPException(
            status_code=400,
            detail="At least one of personal_linkedin_col or company_linkedin_col is required"
        )

    # Get upload metadata
    upload_meta = get_upload(upload_id)
    if not upload_meta:
        raise HTTPException(status_code=404, detail="Upload not found")

    # Read CSV
    csv_path = DATA_DIR / "uploads" / upload_meta["filename"]
    rows = _read_csv_safe(csv_path)

    # Validate columns if provided
    if personal_linkedin_col and personal_linkedin_col not in rows[0]:
        raise HTTPException(status_code=400, detail=f"Column '{personal_linkedin_col}' not found")
    if company_linkedin_col and company_linkedin_col not in rows[0]:
        raise HTTPException(status_code=400, detail=f"Column '{company_linkedin_col}' not found")

    # Count valid rows
    valid_rows = [
        r for r in rows[1:]  # Skip header
        if (personal_linkedin_col and r.get(personal_linkedin_col, "").strip()) or
           (company_linkedin_col and r.get(company_linkedin_col, "").strip())
    ]

    # Create job
    job_store = EnrichmentJobStore()
    job_id = job_store.create_enrichment_job(
        user_id=auth.get_current_user().id,
        job_type="enrichment",
        total=len(rows) - 1,  # Total input rows
        config={
            "upload_id": upload_id,
            "personal_linkedin_col": personal_linkedin_col,
            "company_linkedin_col": company_linkedin_col,
            "max_dms": max_dms,
            "include_company": include_company,
        }
    )

    # Start background task
    async def process_job():
        job_store = EnrichmentJobStore()
        job_store.update_job_status(job_id, "running")

        try:
            # Create progress callback
            async def on_progress(progress_data: dict):
                job_store.append_event(job_id, {
                    "type": "progress",
                    "processed": progress_data.get("index", 0) + 1,
                    "total": progress_data.get("total", len(rows)),
                    "email_found": progress_data.get("email_found", False),
                    "status": progress_data.get("status", ""),
                    "source_counts": progress_data.get("source_counts", {}),
                })

            result = await list_builder.run_unified_linkedin_enrichment(
                rows=rows[1:],  # Skip header
                personal_col=personal_linkedin_col,
                company_col=company_linkedin_col,
                max_dms=max_dms,
                include_company=include_company,
                on_progress=on_progress,
            )

            # Write output
            output_path = DATA_DIR / "outputs" / f"{job_id}.csv"
            _write_csv_output(result, output_path)

            job_store.update_job_status(job_id, "done")
            job_store.append_event(job_id, {
                "type": "complete",
                "total_output_rows": len(result),
                "output_file": str(output_path),
            })
        except Exception as e:
            logger.error("Enrichment job %s failed: %s", job_id, e)
            job_store.update_job_status(job_id, "failed", error=str(e))

    asyncio.create_task(process_job())

    return {
        "job_id": job_id,
        "total": len(rows) - 1,
        "upload_id": upload_id,
    }
```

Note: You may need to adjust imports and helper function references based on existing code patterns.

- [ ] **Step 4: Run linting**

Run: `cd /var/www/lead-generation-platform/backend && python -m py_compile routes.py`
Expected: If errors, fix them. If no output, success.

- [ ] **Step 5: Commit**

```bash
cd /var/www/lead-generation-platform
git add backend/enrichment/routes.py
git commit -m "feat: add /by-linkedin-v2 endpoint for unified personal+company enrichment"
```

---

### Task 4: Update Frontend - Add Company Column Dropdown

**Files:**
- Modify: `frontend/index.html`

- [ ] **Step 1: Find and read the LinkedIn page section**

Run: `sed -n '525,575p' /var/www/lead-generation-platform/frontend/index.html`
Expected: Shows current LinkedIn page HTML structure

- [ ] **Step 2: Add company column dropdown and max DMs selector**

Replace the options section (lines 544-558) with:

```html
            <div class="form-card" id="linkedinOptions" style="display:none;">
                <h3>⚙️ Options</h3>
                <div style="background:#f0f9ff;border-left:4px solid #1a315d;padding:12px;margin-bottom:1rem;border-radius:4px;font-size:0.85rem;color:#374151;">
                    <strong>Tip:</strong> Select columns for personal LinkedIn URLs (e.g., linkedin.com/in/...) and/or company URLs (e.g., linkedin.com/company/...). Personal URLs are enriched first; company waterfall is used as fallback.
                </div>
                <div class="filters">
                    <div class="filter-group">
                        <label>Personal LinkedIn Column (Optional)</label>
                        <select id="linkedinPersonalCol">
                            <option value="">-- Select Column --</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Company LinkedIn Column (Optional)</label>
                        <select id="linkedinCompanyCol">
                            <option value="">-- Select Column --</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label>Max Decision Makers (for company URLs)</label>
                        <select id="linkedinMaxDms">
                            <option value="3">3</option>
                            <option value="5" selected>5</option>
                            <option value="10">10</option>
                        </select>
                    </div>
                    <div class="filter-group">
                        <label style="display:flex;align-items:center;gap:0.5rem;">
                            <input type="checkbox" id="linkedinIncludeCompany" checked> Include Company Details
                        </label>
                    </div>
                </div>
                <div style="margin-top:1rem;color:#6b7280;font-size:0.85rem;">
                    At least one column must be selected.
                </div>
                <button class="btn btn-primary" id="startLinkedinEnrichment">Start Enrichment</button>
            </div>
```

- [ ] **Step 3: Find the JavaScript handler and update it**

Run: `grep -n "handleLinkedinFile\|linkedinCol\|startLinkedinEnrichment" /var/www/lead-generation-platform/frontend/index.html | head -10`
Expected: Shows line numbers for the relevant JavaScript functions

- [ ] **Step 4: Update `handleLinkedinFile` to populate both dropdowns**

Find the `handleLinkedinFile` function and update it to populate BOTH dropdowns:

```javascript
// Find handleLinkedinFile function and update the column population part
// Old code (single dropdown):
// const colSelect = document.getElementById('linkedinCol');
// colSelect.innerHTML = '';
// data.columns.forEach(c => {
//     const opt = document.createElement('option');
//     opt.value = c;
//     opt.textContent = c;
//     if (c.toLowerCase().includes('linkedin')) opt.selected = true;
//     colSelect.appendChild(opt);
// });

// Replace with BOTH dropdowns:
const personalColSelect = document.getElementById('linkedinPersonalCol');
const companyColSelect = document.getElementById('linkedinCompanyCol');
personalColSelect.innerHTML = '<option value="">-- Select Column --</option>';
companyColSelect.innerHTML = '<option value="">-- Select Column --</option>';

data.columns.forEach(c => {
    // Add to personal dropdown
    const personalOpt = document.createElement('option');
    personalOpt.value = c;
    personalOpt.textContent = c;
    // Prefer 'person' or 'personal' in column name for personal
    if (c.toLowerCase().includes('person') && c.toLowerCase().includes('linkedin')) {
        personalOpt.selected = true;
    }
    personalColSelect.appendChild(personalOpt);

    // Add to company dropdown
    const companyOpt = document.createElement('option');
    companyOpt.value = c;
    companyOpt.textContent = c;
    // Prefer 'company' in column name for company
    if (c.toLowerCase().includes('company') && c.toLowerCase().includes('linkedin')) {
        companyOpt.selected = true;
    }
    companyColSelect.appendChild(companyOpt);
});
```

- [ ] **Step 5: Update the start enrichment click handler**

Find `startLinkedinEnrichment` click handler and update the API call:

```javascript
// Old code:
document.getElementById('startLinkedinEnrichment').addEventListener('click', async () => {
    const linkedinCol = document.getElementById('linkedinCol').value;
    const includeCompany = document.getElementById('includeCompany').checked;
    const res = await fetch('/api/enrichment/by-linkedin', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
        body: JSON.stringify({ upload_id: linkedinUploadId, linkedin_col: linkedinCol, include_company: includeCompany })
    });
    // ...
});

// New code:
document.getElementById('startLinkedinEnrichment').addEventListener('click', async () => {
    const personalCol = document.getElementById('linkedinPersonalCol').value;
    const companyCol = document.getElementById('linkedinCompanyCol').value;
    const maxDms = parseInt(document.getElementById('linkedinMaxDms').value) || 5;
    const includeCompany = document.getElementById('linkedinIncludeCompany').checked;

    // Validation: at least one column required
    if (!personalCol && !companyCol) {
        alert('Please select at least one LinkedIn column (Personal or Company)');
        return;
    }

    const res = await fetch('/api/enrichment/by-linkedin-v2', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
        body: JSON.stringify({
            upload_id: linkedinUploadId,
            personal_linkedin_col: personalCol || null,
            company_linkedin_col: companyCol || null,
            max_dms: maxDms,
            include_company: includeCompany
        })
    });

    if (res.ok) {
        const data = await res.json();
        currentJobId = data.job_id;
        document.getElementById('linkedinTotal').textContent = data.total;
        document.getElementById('linkedinProgress').style.display = 'block';
        startProgressStream(currentJobId, 'linkedin');
    } else {
        const err = await res.json();
        alert('Error: ' + (err.detail || 'Failed to start enrichment'));
    }
});
```

- [ ] **Step 6: Update download button selector if needed**

Verify download button selector is correct:
Run: `grep -n "downloadLinkedinResults" /var/www/lead-generation-platform/frontend/index.html`
Expected: Should find the button and the click handler

- [ ] **Step 7: Commit**

```bash
cd /var/www/lead-generation-platform
git add frontend/index.html
git commit -m "feat: update LinkedIn enrichment UI with company column and max DMs"
```

---

### Task 5: Test the Complete Flow

**Files:**
- Test: Manual testing with actual CSV files

- [ ] **Step 1: Restart the backend service**

Run:
```bash
cd /var/www/lead-generation-platform
sudo systemctl restart lead-generation-platform.service
sleep 3
curl http://localhost:8765/api/health
```
Expected: `{"status":"ok"}` or similar

- [ ] **Step 2: Create test CSV files**

Create `test_personal_only.csv`:
```csv
person_linkedin
https://linkedin.com/in/jeffweiner
https://linkedin.com/in/sundarpichai
```

Create `test_company_only.csv`:
```csv
company_linkedin
https://www.linkedin.com/company/google
https://www.linkedin.com/company/microsoft
```

Create `test_both.csv`:
```csv
person_linkedin,company_linkedin
https://linkedin.com/in/jeffweiner,https://www.linkedin.com/company/linkedin
,https://www.linkedin.com/company/google
```

- [ ] **Step 3: Test via frontend or curl**

Login to the web UI at https://listbuilding.eagleinfoservice.com/
Navigate to LinkedIn Enrich page
Upload test CSV files and verify:
1. Both dropdowns appear
2. Processing starts without errors
3. Progress updates
4. CSV download works

- [ ] **Step 4: Verify output structure**

Download the result CSV and verify:
- Personal-only: Shows person's name, email, company
- Company-only: Shows 1 row per decision maker found
- Both: Personal data if found, company DMs if personal fails

- [ ] **Step 5: Commit test results**

```bash
cd /var/www/lead-generation-platform
git add -A
git commit -m "test: verify LinkedIn enrichment v2 works correctly"
```

---

## Checklist

- [ ] Task 1: URL type detection helpers added
- [ ] Task 2: Unified enrichment function created
- [ ] Task 3: New API endpoint added
- [ ] Task 4: Frontend updated with dual dropdowns
- [ ] Task 5: Manual testing completed

---

**Plan complete.** Two execution options:

**1. Subagent-Driven (recommended)** - I dispatch a fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** - Execute tasks in this session using executing-plans, batch execution with checkpoints

**Which approach?**