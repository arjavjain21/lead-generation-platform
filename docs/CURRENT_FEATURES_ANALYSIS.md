# Lead Generation Platform - Features & Structure Analysis

## Application Structure

### Three Main Pages

**1. Scraper Page** (`/src/pages/ScraperPage.tsx`)
- Uses: `/src/components/scraper/JobsList.tsx`
- Features: ✅ Has refresh button (lines 129-131)
- Features: ✅ NOW has download button for failed jobs
- Purpose: Dedicated view for scraper jobs only

**2. Enrichment Page** (`/src/pages/EnrichmentPage.tsx`)
- Uses: `/src/components/enrichment/JobsList.tsx`
- Features: ✅ Has refresh button (lines 111-126)
- Features: ✅ NOW has download button for failed jobs
- Purpose: Dedicated view for enrichment jobs only

**3. Job History Page** (`/src/pages/JobHistoryPage.tsx`) ⭐ **THIS IS WHAT YOU'RE LOOKING AT**
- Uses: Standalone component (NOT using JobsList components)
- Features: ❌ NO refresh button
- Features: ❌ NO download button for failed jobs (only for 'done' jobs, lines 185-193)
- Purpose: Combined view of ALL jobs (scraper + enrichment)
- Layout: Table format with filtering tabs (All/Scrapers/Enrichments)

---

## Current Features by Page

### Scraper Page Features
- Job creation form
- Region/state/city selector
- Jobs list with:
  - ✅ Refresh button (top-right)
  - ✅ Download button for DONE jobs
  - ✅ Download button for FAILED jobs (NEWLY ADDED)
  - ✅ Watch button for running jobs
  - ✅ Sync to Contacts DB button (for done jobs)
- Real-time progress updates via SSE
- Status badges (queued/running/done/failed)

### Enrichment Page Features
- CSV file upload
- Column mapping interface
- ICP configuration cascade builder
- Jobs list with:
  - ✅ Refresh button (top-right)
  - ✅ Download button for DONE jobs
  - ✅ Download button for FAILED jobs (NEWLY ADDED)
  - ✅ Watch button for running jobs
- Real-time progress updates via SSE
- Status badges (queued/running/done/failed)

### Job History Page Features (CURRENT ISSUE)
- Filter tabs: All Jobs / Scrapers / Enrichments
- Combined table view showing both job types
- Columns: Type, Query/File, Status, Progress, Results/Emails, Created, Actions
- Actions currently ONLY include:
  - ✅ Download button for DONE jobs
  - ❌ NO download button for FAILED jobs
  - ✅ Chain to Enrichment button (for done scraper jobs)
- ❌ NO refresh button anywhere
- ❌ NO way to reload data without navigating away and back

---

## What's Broken Right Now

### Issue 1: Job History Page - No Refresh Button
**Location:** `/src/pages/JobHistoryPage.tsx`
**Problem:** No refresh button in the header or anywhere on the page
**User Impact:** Must navigate away and back to see new data
**Expected Behavior:** Should have a refresh button like Scraper and Enrichment pages

### Issue 2: Job History Page - Failed Jobs Can't Download
**Location:** `/src/pages/JobHistoryPage.tsx` lines 185-193
**Current Code:**
```tsx
{job.status === 'done' && (
  <button
    onClick={() => handleDownload(job.job_id, job.job_type)}
    className="p-1.5 text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded transition-colors"
    title="Download"
  >
    <Download size={16} />
  </button>
)}
```

**Problem:** Only shows download button for `status === 'done'`
**User Impact:** Failed jobs show no action button, can't download partial results
**Expected Behavior:** Should show download button for BOTH 'done' AND 'failed' jobs

---

## Why Previous Build Didn't Work

**Root Cause:** I only modified the individual JobsList components, NOT the JobHistoryPage component!

**What I Fixed:**
- ✅ `/src/components/enrichment/JobsList.tsx` - Added download button for failed jobs
- ✅ `/src/components/scraper/JobsList.tsx` - Added download button for failed jobs

**What I Missed:**
- ❌ `/src/pages/JobHistoryPage.tsx` - Still missing download button for failed jobs
- ❌ `/src/pages/JobHistoryPage.tsx` - Still missing refresh button

**Why The Build Didn't Include Changes:**
- I made the edits to the source files
- But the JobHistoryPage.tsx was NOT edited
- So the build was correct - it just didn't have the changes the user needs!

---

## How the Application Currently Works

### Authentication Flow
1. User visits `https://listbuilding.eagleinfoservice.com/`
2. If not authenticated → Shows `LoginPage` (shared component)
3. Login → JWT token stored in localStorage
4. Authenticated → Shows main app with sidebar

### Navigation Flow
```
App.tsx
├── Sidebar (navigation)
│   ├── Scraper → ScraperPage → scraper/JobsList.tsx
│   ├── Enrichment → EnrichmentPage → enrichment/JobsList.tsx
│   └── History → JobHistoryPage.tsx (standalone)
```

### Data Fetching
**ScraperPage & EnrichmentPage:**
- Uses JobsList components
- Auto-refresh every 5 seconds (line 61 in both JobsList files)
- Has manual refresh button

**JobHistoryPage:**
- fetchJobs() called on mount and filter change (lines 27-29)
- ❌ NO auto-refresh
- ❌ NO manual refresh button
- Must navigate away and back to refresh

### Download Endpoints
**Scraper Jobs:**
- Endpoint: `/api/scraper/jobs/{job_id}/download`
- Backend supports: Done jobs + Failed jobs with partial output

**Enrichment Jobs:**
- Endpoint: `/api/enrichment/jobs/{job_id}/download`
- Backend supports: Done jobs + Failed jobs with partial output

**Backend Already Supports:**
- ✅ Downloading failed jobs if partial output exists
- ✅ Appropriate error messages if no output available

---

## What Needs To Be Fixed

### Fix 1: Add Download Button for Failed Jobs (JobHistoryPage)

**File:** `/src/pages/JobHistoryPage.tsx`
**Lines:** 183-204

**Change FROM:**
```tsx
<td className="px-4 py-3">
  <div className="flex items-center gap-2">
    {job.status === 'done' && (
      <button
        onClick={() => handleDownload(job.job_id, job.job_type)}
        className="p-1.5 text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded transition-colors"
        title="Download"
      >
        <Download size={16} />
      </button>
    )}
    {job.job_type === 'scraper' && job.status === 'done' && (
      <button
        onClick={() => handleChainToEnrichment(job)}
        className="p-1.5 text-purple-600 hover:text-purple-700 hover:bg-purple-50 rounded transition-colors"
        title="Chain to Enrichment"
      >
        <Link2 size={16} />
      </button>
    )}
  </div>
</td>
```

**Change TO:**
```tsx
<td className="px-4 py-3">
  <div className="flex items-center gap-2">
    {(job.status === 'done' || job.status === 'failed') && (
      <button
        onClick={() => handleDownload(job.job_id, job.job_type)}
        className="p-1.5 text-slate-500 hover:text-slate-700 hover:bg-slate-100 rounded transition-colors"
        title={job.status === 'failed' ? 'Download partial results' : 'Download'}
      >
        <Download size={16} />
      </button>
    )}
    {job.job_type === 'scraper' && job.status === 'done' && (
      <button
        onClick={() => handleChainToEnrichment(job)}
        className="p-1.5 text-purple-600 hover:text-purple-700 hover:bg-purple-50 rounded transition-colors"
        title="Chain to Enrichment"
      >
        <Link2 size={16} />
      </button>
    )}
  </div>
</td>
```

### Fix 2: Add Refresh Button (JobHistoryPage)

**File:** `/src/pages/JobHistoryPage.tsx`
**Location:** In the header section, after the title

**Change FROM:**
```tsx
<div className="flex items-center justify-between mb-6">
  <div className="flex items-center gap-3">
    <div className="w-10 h-10 bg-slate-700 rounded-lg flex items-center justify-center">
      <History size={20} className="text-white" />
    </div>
    <div>
      <h1 className="text-xl font-bold text-slate-900">Job History</h1>
      <p className="text-sm text-slate-500">View all your scraper and enrichment jobs</p>
    </div>
  </div>
</div>
```

**Change TO:**
```tsx
<div className="flex items-center justify-between mb-6">
  <div className="flex items-center gap-3">
    <div className="w-10 h-10 bg-slate-700 rounded-lg flex items-center justify-center">
      <History size={20} className="text-white" />
    </div>
    <div>
      <h1 className="text-xl font-bold text-slate-900">Job History</h1>
      <p className="text-sm text-slate-500">View all your scraper and enrichment jobs</p>
    </div>
  </div>
  <button
    onClick={() => fetchJobs()}
    className="px-4 py-2 bg-white border border-slate-300 rounded-lg text-sm font-medium text-slate-700 hover:bg-slate-50 transition-colors flex items-center gap-2"
  >
    <RefreshCw size={16} />
    Refresh
  </button>
</div>
```

**Also need to import RefreshCw icon:**
Add to line 2 imports: `import { History, Download, Link2, RefreshCw } from 'lucide-react'`

---

## Current Technical Stack

**Frontend:**
- React 19.0.0
- TypeScript 5.6.0
- Vite 5.0.0 (build tool)
- Lucide React 0.460.0 (icons)
- Tailwind CSS 3.4.0 (styling)

**Backend:**
- FastAPI (Python)
- SQLite database
- JWT authentication
- Server-Sent Events (SSE) for real-time updates

**Deployment:**
- Frontend: Static files served by Nginx
- Backend: systemd service (`lead-generation-platform.service`)
- Port: 8765

---

## File Locations Reference

**Source Code:**
```
/var/www/lead-generation-platform-new/frontend/src/
├── pages/
│   ├── ScraperPage.tsx         → uses scraper/JobsList.tsx
│   ├── EnrichmentPage.tsx      → uses enrichment/JobsList.tsx
│   └── JobHistoryPage.tsx      → STANDALONE (THIS IS THE ISSUE)
├── components/
│   ├── scraper/
│   │   └── JobsList.tsx        ✅ FIXED
│   └── enrichment/
│       └── JobsList.tsx        ✅ FIXED
```

**Production Build:**
```
/var/www/lead-generation-platform/frontend/
├── assets/
│   ├── index-DUpDm4o-.css
│   └── index-mbsMzj9g.js
├── categories.json
└── index.html
```

---

## Summary

**What Works:**
- ✅ Scraper page has refresh button + download buttons for failed jobs
- ✅ Enrichment page has refresh button + download buttons for failed jobs

**What Doesn't Work (Your Issue):**
- ❌ Job History page has NO refresh button
- ❌ Job History page has NO download button for failed jobs

**What Needs To Be Done:**
1. Fix JobHistoryPage.tsx to add download button for failed jobs
2. Fix JobHistoryPage.tsx to add refresh button
3. Rebuild frontend
4. Deploy to production
5. User refreshes browser

**Why This Happened:**
- JobHistoryPage is a separate standalone component
- It doesn't use the JobsList components I already fixed
- It has its own table layout and action buttons
- I didn't realize this page existed and needed the same fixes

---

## Lead Universe Classification (added 2026-08-08)

Every lead in the contacts DB has a `lead_universe` tag — `local_business`, `b2b_agency`, `saas`, or `ecom` (NULL = unclassified). ~62% of ~8.7M leads are classified.

- **Find People page** (nav: "Find People") — a Universe dropdown (All / Local business / B2B-Agency / SaaS / E-commerce) + colored badges on results. Filter any people search by lead type.
- **API** — `POST /api/enrichment/search/employees` (`universe` field) on this platform, or `GET https://leadsdatabase.cc/v1/people/search?universe=` on the Contacts DB API.
- **Self-maintaining** — new enriched leads are auto-tagged on write-back.
- Rules + mappings: `docs/LEAD_UNIVERSE_CLASSIFICATION.md`.
