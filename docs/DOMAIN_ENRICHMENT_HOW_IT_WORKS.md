# Domain Enrichment - Complete Guide
**How It Works: Backend + Frontend + User Flow**

---

## 🎯 WHAT IS DOMAIN ENRICHMENT?

**Takes a list of domains/websites → Finds decision makers → Returns their emails**

**Example:**
- Input: `google.com`, `microsoft.com`, `amazon.com`
- Output: Google CEO Sundar Pichai (sundar@google.com), Microsoft CEO Satya Nadella, etc.

---

## 📊 USER WORKFLOW (Frontend)

### Step 1: User Uploads CSV
```
User → "Enrichment" tab → Clicks "Upload CSV"
↓
Selects CSV file with domains
↓
Frontend → POST /api/enrichment/upload
↓
Backend saves file → Returns upload_id + column names
```

**CSV Example:**
```csv
website, company_name
google.com, Google
microsoft.com, Microsoft
example.com, Example Corp
```

### Step 2: User Configures Enrichment
```
Frontend shows columns: ["website", "company_name"]
↓
User selects:
  - Domain column: "website" (required)
  - Name column: "company_name" (optional, for fallback)
  - Job titles to find: Use default cascade
  - Max results per company: 5 decision makers
↓
User clicks "Start Enrichment"
```

### Step 3: Job Runs in Background
```
Frontend → POST /api/enrichment/jobs
↓
Backend → Returns job_id immediately
↓
Frontend → Opens SSE connection: GET /api/enrichment/jobs/{job_id}/stream
↓
Real-time progress updates every few seconds:
  - { index: 0, total: 100, domain: "google.com", status: "enriched", emails_found: 1 }
  - { index: 1, total: 100, domain: "microsoft.com", status: "enriched", emails_found: 1 }
  - ...
```

### Step 4: User Downloads Results
```
When job completes:
↓
Frontend → "Download Results" button enabled
↓
Frontend → GET /api/enrichment/jobs/{job_id}/download
↓
Browser downloads CSV with enriched data
```

**Output CSV Example:**
```csv
website,company_name,company_linkedin_url,dm_first_name,dm_last_name,dm_title,dm_email,row_status
google.com,Google,https://linkedin.com/company/google,Sundar,Pichai,CEO,sundar@google.com,enriched
microsoft.com,Microsoft,https://linkedin.com/company/microsoft,Satya,Nadella,CEO,satya@microsoft.com,enriched
example.com,Example Corp,,,,,,,,no_linkedin
```

---

## ⚙️ BACKEND WORKFLOW (API Endpoints)

### 1. Upload Endpoint
```
POST /api/enrichment/upload
↓
Input: CSV file (multipart/form-data)
↓
Backend:
  - Validates CSV format
  - Reads first 5 rows to detect columns
  - Saves to: /var/www/lead-generation-platform/backend/data/uploads/{upload_id}.csv
  - Returns: { upload_id, columns, preview, row_count }
↓
Frontend receives column names to show user
```

### 2. Start Job Endpoint
```
POST /api/enrichment/jobs
↓
Input: { upload_id, domain_col, name_col, cascade, max_results }
↓
Backend:
  - Validates upload_id exists
  - Validates domain_col exists in CSV
  - Creates job in database with status="queued"
  - Starts background task (runs asynchronously)
  - Returns immediately: { job_id, total }
↓
Background task runs enrichment pipeline
```

### 3. Progress Stream Endpoint (Real-Time Updates)
```
GET /api/enrichment/jobs/{job_id}/stream
↓
Returns: Server-Sent Events (SSE) stream
↓
Events sent as rows are processed:
  - { index: 5, total: 100, domain: "example.com", status: "enriched", emails_found: 1 }
  - { index: 6, total: 100, domain: "test.com", status: "no_linkedin", emails_found: 0 }
  - { done: true, total: 100, processed: 100, emails_found: 45 }
↓
Frontend updates progress bar in real-time
```

### 4. Download Endpoint
```
GET /api/enrichment/jobs/{job_id}/download
↓
Backend:
  - Validates job exists and user owns it
  - Returns CSV file from: /var/www/lead-generation-platform/backend/data/outputs/{job_id}.csv
↓
Browser downloads file
```

### 5. Partial Download Endpoint (While Job Runs)
```
GET /api/enrichment/jobs/{job_id}/partial-download
↓
Returns whatever data has been processed so far
↓
Useful for long-running jobs - can download early results
```

---

## 🔧 ENRICHMENT PIPELINE (What Happens in Background)

### For EACH Domain Row:

#### Step 1: Find Company LinkedIn URL
```
Domain → Blitz API (domain-to-linkedin)
↓
POST https://api.blitz-api.ai/v2/enrichment/domain-to-linkedin
{ "domain": "google.com" }
↓
Returns: { "found": true, "company_linkedin_url": "https://linkedin.com/company/google" }
↓
If not found: row_status = "no_linkedin" (stop here for this domain)
```

#### Step 2: Find Decision Makers (Waterfall Search)
```
Company LinkedIn URL → Blitz API (waterfall-icp-keyword)
↓
POST https://api.blitz-api.ai/v2/search/waterfall-icp-keyword
{
  "linkedin_url": "https://linkedin.com/company/google",
  "cascade": [
    { include_title: ["Owner", "CEO", "Founder"], exclude_title: ["assistant"] },
    { include_title: ["VP Marketing", "VP Sales"], exclude_title: ["intern"] },
    { include_title: ["Director of Marketing", "Head of Sales"] }
  ]
}
↓
Returns up to 5 decision makers:
[
  { name: "Sundar Pichai", linkedin_url: "https://linkedin.com/in/sundarpichai", title: "CEO" },
  { name: "Sundar Pichai", linkedin_url: "https://linkedin.com/in/sundarpichai", title: "CEO" }
]
```

#### Step 3: Find Email for Each Person
```
For each person found:
↓
Person LinkedIn URL → Blitz API (email enrichment)
↓
POST https://api.blitz-api.ai/v2/enrichment/email
{ "linkedin_url": "https://linkedin.com/in/sundarpichai" }
↓
Returns: { "found": true, "email": "sundar@google.com" }
↓
If Blitz fails, try fallbacks:
  1. Contacts DB by LinkedIn URL
  2. Contacts DB by person name + domain
  3. Contacts DB by input CSV name + domain
↓
If all fail: dm_email = "", row_status = "no_contacts"
```

#### Step 4: Create Output Rows
```
For each person with email:
↓
Create row:
  - Original columns: website, company_name, ...
  - company_linkedin_url
  - dm_first_name, dm_last_name, dm_full_name
  - dm_title, dm_linkedin_url
  - dm_email, dm_email_source
  - dm_headline, dm_location_city, dm_location_country
  - dm_icp_tier (1, 2, or 3 based on which cascade tier matched)
  - row_status: "enriched"
```

---

## 🎯 DEFAULT CASCADE (Job Titles to Find)

**3-Tier Cascade (runs in order):**

### Tier 1: C-Level & Owners
```
Titles: Owner, CEO, Founder, Co-Founder, President
Exclude: assistant, intern, junior, associate
Location: WORLD (any country)
Max: Up to 5 people
```

### Tier 2: VP Level
```
Titles: CMO, VP Marketing, VP Sales, Chief Revenue Officer, Chief Marketing Officer
Exclude: assistant, intern, junior
Location: WORLD
Max: Up to 5 people
```

### Tier 3: Director Level
```
Titles: Director of Marketing, Director of Sales, Head of Marketing, Head of Sales, Head of Growth
Exclude: assistant, intern, junior
Location: WORLD
Max: Up to 5 people
```

**Total Results:** Up to 15 decision makers per company (5 per tier)

---

## 🔐 AUTHENTICATION & SECURITY

### All Endpoints Require Login
```
GET /api/enrichment/jobs → Requires JWT token in header
POST /api/enrichment/jobs → Requires JWT token
GET /api/enrichment/jobs/{id}/download → Requires JWT token + ownership check
```

### User Permissions
```
Regular User:
  - Can only see their own jobs
  - Has API quota: 50,000 requests/day

Admin User:
  - Can see all jobs across all users
  - Has unlimited API quota
```

---

## ⚡ PERFORMANCE & CONCURRENCY

### Rate Limiting
```
Blitz API: 4 requests per second (conservative, limit is 5 RPS)
↓
Backend enforces:
  - 5 domains processed concurrently
  - 10 email lookups concurrently
↓
Prevents API overload
```

### Incremental Writes
```
As rows are enriched:
↓
Written to CSV file incrementally
↓
User can download partial results while job still running
↓
File: /var/www/lead-generation-platform/backend/data/outputs/{job_id}.csv
```

---

## 📈 ROW STATUS VALUES

| Status | Meaning | When It Happens |
|--------|---------|-----------------|
| `enriched` | Found company LinkedIn + person + email | Success! |
| `no_linkedin` | Company LinkedIn URL not found | Domain invalid or not on LinkedIn |
| `no_contacts` | Found company LinkedIn but no emails | Blitz + Contacts DB both failed |
| `error` | Exception occurred during processing | Bug or API error |
| `skipped_no_domain` | Domain column empty in input | Invalid input row |

---

## 🔄 ALTERNATIVE: CHAIN FROM SCRAPER

### User can chain scraper output directly to enrichment:
```
Scraper job completes (status="done")
↓
Frontend shows "Enrich This Job" button
↓
User clicks → POST /api/scraper/jobs/{scraper_job_id}/chain
↓
Backend:
  - Reads scraper output CSV
  - Extracts "website" column
  - Creates enrichment job automatically
  - Links jobs: enrichment.parent_job_id = scraper_job_id
↓
Enrichment runs as normal
```

---

## 📊 DATABASE SCHEMA

### Enrichment Job Record:
```sql
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    user_id TEXT,
    job_type TEXT,  -- 'enrichment'
    status TEXT,    -- 'queued' | 'running' | 'done' | 'failed'

    -- Enrichment-specific fields:
    total INTEGER,        -- Total rows to process
    processed INTEGER,    -- Rows processed so far
    emails_found INTEGER, -- Total emails found

    filename TEXT,        -- Upload ID
    domain_col TEXT,      -- Column name used

    output_path TEXT,     -- Path to output CSV
    error TEXT,           -- Error message if failed
    created_at TEXT,
    updated_at TEXT
);
```

### Job Events (for SSE streaming):
```sql
CREATE TABLE job_events (
    id INTEGER PRIMARY KEY,
    job_id TEXT,
    seq INTEGER,          -- Event sequence number
    payload TEXT          -- JSON event data
);
```

---

## 🛠️ OUR FIXES IMPACT

### Before Our Fixes:
```
1 bad row → Entire enrichment job failed → NO RESULTS ✗
422 errors → Retried 3 times → SLOW ✗
Job failed → Partial output lost ✗
```

### After Our Fixes:
```
1 bad row → Becomes error row → Job continues → GET RESULTS ✓
422 errors → Skipped immediately → FAST ✓
Job with errors → Partial output downloadable ✓
```

---

## 📝 EXAMPLE CSV INPUT/OUTPUT

### Input CSV:
```csv
website, company_name, industry
google.com, Google, Technology
microsoft.com, Microsoft, Technology
invalid-domain-$$$-.com, Bad Domain, Test
```

### Output CSV (Enriched):
```csv
website,company_name,industry,company_linkedin_url,dm_first_name,dm_last_name,dm_full_name,dm_title,dm_linkedin_url,dm_email,dm_email_source,row_status
google.com,Google,Technology,https://linkedin.com/company/google,Sundar,Pichai,Sundar Pichai,CEO,https://linkedin.com/in/sundarpichai,sundar@google.com,blitz_email,enriched
google.com,Google,Technology,https://linkedin.com/company/google,Sundar,Pichai,Sundar Pichai,CEO,https://linkedin.com/in/sundarpichai,sundar@google.com,blitz_email,enriched
microsoft.com,Microsoft,Technology,https://linkedin.com/company/microsoft,Satya,Nadella,Satya Nadella,CEO,https://linkedin.com/in/satyanadella,satya@microsoft.com,blitz_email,enriched
invalid-domain-$$$-.com,Bad Domain,Test,,,,,,,,error
```

---

## 🎯 SUMMARY

**User Flow:**
1. Upload CSV with domains
2. Select domain column + configure job titles
3. Start job (runs in background)
4. Watch real-time progress
5. Download enriched CSV with decision makers + emails

**Backend Flow:**
1. Receives CSV upload
2. Creates background job
3. For each domain: Find LinkedIn → Find people → Find emails
4. Writes results incrementally
5. Streams progress via SSE
6. Returns final CSV download

**Time Estimate:**
- 100 domains: ~10-30 minutes (depends on Blitz API speed)
- 1,000 domains: ~2-5 hours

**Success Rate:**
- Expected: 70-90% rows enriched (depends on data quality)
- No LinkedIn: 10-20%
- No emails found: 5-10%
- Errors: <5% (after our fixes!)
