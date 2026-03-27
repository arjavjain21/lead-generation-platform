# List Building Tool - Unified Enrichment Platform

## Project Overview

**Project Name:** List Building Tool (formerly Lead Generation Platform)
**Location:** `/var/www/lead-generation-platform`
**Status:** Enhancement/Migration

---

## Executive Summary

Build a comprehensive, one-stop list building tool that combines Blitz API with internal Contacts Database for maximum enrichment coverage. The tool supports 3 distinct flows to handle different input types and use cases.

### Key Objectives

1. **Maximum Coverage:** Use Contacts DB as primary, Blitz API as fallback (70-90% cost reduction)
2. **Multiple Input Types:** Domains, LinkedIn URLs, search criteria
3. **Professional UI:** Modern React dashboard for Account Managers
4. **Speed:** Optimized rate limits (50 RPS internal, 25 RPS Blitz)

---

## The 3 Enrichment Flows

### Flow 1: Domain Upload → Generic Emails + Decision Makers

**Input:** CSV with list of domains (e.g., `example.com`)

**Processing:**
1. For each domain:
   - Get company LinkedIn URL (Contacts DB → Blitz fallback)
   - Get generic emails for domain (from waterfall results or Blitz)
   - Get up to 5 decision makers with emails

**Output:** Two CSV files
- (a) `generic_emails.csv` - All emails found per domain
- (b) `decision_makers.csv` - Up to 5 contacts per company with:
  - Name, Title, Email, Phone, LinkedIn, ICP Tier

**Use Case:** "I have a list of 500 companies I want to target"

---

### Flow 2: Search Criteria → Companies → Enrich

**Input:** Search filters (industry, location, size, etc.)

**Processing:**
1. Use Blitz Company Search to find companies matching criteria
2. Export company list
3. Apply Flow 1 to enrich each company

**Search Filters (from Blitz API + Internal DB):**

| Filter | Source | Values |
|--------|--------|--------|
| Industry | Blitz API | 100+ values (see Appendix A) |
| Employee Range | Blitz API | 1-10, 11-50, 51-200, 201-500, 501-1000, 1001-5000, 5001-10000, 10001+ |
| Company Type | Blitz API | Educational, Government Agency, Nonprofit, Partnership, Privately Held, Public Company |
| Country | Blitz API | ISO 3166-1 alpha-2 (US, GB, CA, DE, etc.) |
| Sales Region | Blitz API | NORAM, LATAM, EMEA, APAC |
| Location | Internal DB | Cities, States from existing data |
| Technology | Internal DB | Tech stacks from existing data |

**Output:** Enriched companies with decision makers

**Use Case:** "Find all SaaS companies in San Francisco with 50-200 employees"

---

### Flow 3: LinkedIn URLs Upload → Full Enrichment

**Input:** CSV with LinkedIn URLs (from Clay, SmartProspect, Apollo, etc.)

**Processing:**
1. For each LinkedIn URL:
   - Get person details (name, title, company, location)
   - Find work email (Contacts DB → Blitz fallback)
   - Get company details

**Output:** Fully enriched CSV with:
- Original data
- Enriched: Name, Title, Company, Work Email, Phone, Company LinkedIn, Industry, Employee Count

**Use Case:** "Enrich this list of 1000 leads from Apollo"

---

## The 3 Ways to Find Emails

### Priority Order (Contacts DB First)

```
1. LinkedIn URL → Work Email
   ├── Primary: Contacts DB (by LinkedIn URL)
   └── Fallback: Blitz API /v2/enrichment/email

2. Full Name + Domain → Work Email
   ├── Primary: Contacts DB (by name + domain)
   └── Fallback: Blitz API /v2/enrichment/person-enrich

3. Domain → Generic Emails
   ├── Primary: Contacts DB company_contacts_enriched
   └── Fallback: Extract from Waterfall ICP results
```

---

## Technical Architecture

### Backend Stack

- **Framework:** FastAPI (Python 3.12)
- **Database:** SQLite (jobs) + PostgreSQL (Contacts DB)
- **HTTP Client:** httpx (async)
- **Rate Limiting:** Custom sliding window

### New API Endpoints Needed

```python
# Domain Enrichment (Flow 1)
POST /api/v1/enrichment/by-domains
POST /api/v1/enrichment/by-domains/stream

# Search (Flow 2)
POST /api/v1/search/companies
GET  /api/v1/search/companies/results/{job_id}

# LinkedIn Enrichment (Flow 3)
POST /api/v1/enrichment/by-linkedin
POST /api/v1/enrichment/by-linkedin/stream
```

### Rate Limits

| Service | Current | Recommended |
|---------|---------|-------------|
| Internal Contacts DB | 25 RPS | **50 RPS** |
| Blitz API | 25 RPS | 25 RPS (max) |

---

## UI/UX Design

### Main Dashboard

```
┌─────────────────────────────────────────────────────────────────┐
│  List Building Tool                                    [User]   │
├─────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   📥 Upload     │  │   🔍 Search     │  │   🔗 LinkedIn   │ │
│  │   Domains       │  │   Companies     │  │   Enrichment    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│                                                                     │
│  Recent Jobs                                                    │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Job #123 | Domain Enrich | 500 domains | 45% complete | ⏸ │ │
│  │ Job #122 | Company Search | SaaS + US | 100% | 📥 Download │ │
│  │ Job #121 | LinkedIn Enrich | 1000 URLs | 100% | 📥 Download │ │
│  └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Flow 1: Domain Upload

```
┌─────────────────────────────────────────────────────────────────┐
│  ← Back                    Domain Enrichment                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                     │
│  📤 Upload CSV                                                    │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                                                              │ │
│  │           Drag & drop your CSV here                        │ │
│  │                    or click to browse                      │ │
│  │                                                              │ │
│  │           Required columns: domain                          │ │
│  │           Optional: first_name, last_name, full_name       │ │
│  │                                                              │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  ⚙️ Options                                                     │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Decision Makers per Company: [5 ▼]                          │ │
│  │ Include Generic Emails:  [x] Yes                           │ │
│  │ Email Cascade:        [Default ▼]                          │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  [▶️ Start Enrichment]                                           │
└─────────────────────────────────────────────────────────────────┘
```

### Flow 2: Company Search

```
┌─────────────────────────────────────────────────────────────────┐
│  ← Back                      Company Search                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                     │
│  🔍 Search Criteria                                              │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Industry:        [Select... ▼]  + Add more                 │ │
│  │ Employee Range:  [Select... ▼]                             │ │
│  │ Company Type:    [Select... ▼]                             │ │
│  │ Country:         [Select... ▼]                             │ │
│  │ Sales Region:   [Select... ▼]                             │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  📊 Results Preview (will be enriched)                           │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Company                    | Industry        | Employees    │ │
│  │ Acme Corp                 | Computer Soft..| 51-200       │ │
│  │ Tech Solutions Inc        | Information... | 201-500       │ │
│  │ ...                                                          │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  [🔍 Search Companies]  [➡️ Enrich Results]                        │
└─────────────────────────────────────────────────────────────────┘
```

### Flow 3: LinkedIn Enrichment

```
┌─────────────────────────────────────────────────────────────────┐
│  ← Back                  LinkedIn Enrichment                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                     │
│  📤 Upload CSV                                                    │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                                                              │ │
│  │           Drag & drop your CSV here                        │ │
│  │           (must have linkedin_url column)                  │ │
│  │                                                              │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  ⚙️ Options                                                     │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Enrichment Level:  [Full (Email + Phone + Company) ▼]      │ │
│  │ Find Company Details: [x] Yes                              │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                     │
│  [▶️ Start Enrichment]                                           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Database Schema Changes

### New Tables

```sql
-- Enrichment jobs tracking
CREATE TABLE enrichment_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL, -- 'domain', 'search', 'linkedin'
    status TEXT NOT NULL, -- 'queued', 'running', 'completed', 'failed'
    input_source TEXT,
    filters JSON,
    total_count INTEGER,
    processed_count INTEGER DEFAULT 0,
    results_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    output_file TEXT
);

-- Search criteria presets (for quick access)
CREATE TABLE search_presets (
    preset_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    filters JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Implementation Phases

### Phase 1: Backend Enhancement (Week 1)

**Priority:** Critical

- [ ] Update Blitz API client to 50 RPS
- [ ] Add Company Search endpoint to blitz_client.py
- [ ] Add Employee Finder endpoint to blitz_client.py
- [ ] Add person-enrich endpoint (name + domain → email)
- [ ] Create unified pipeline for all 3 flows
- [ ] Add job tracking to database

**Files to Modify:**
- `/var/www/lead-generation-platform/backend/enrichment/blitz_client.py`
- `/var/www/lead-generation-platform/backend/enrichment/pipeline.py`
- `/var/www/lead-generation-platform/backend/enrichment/routes.py`

---

### Phase 2: API Endpoints (Week 1-2)

**Priority:** High

- [ ] `POST /api/v1/enrichment/by-domains` - Flow 1
- [ ] `POST /api/v1/enrichment/by-linkedin` - Flow 3
- [ ] `POST /api/v1/search/companies` - Flow 2
- [ ] `GET /api/v1/jobs/{job_id}` - Job status
- [ ] `GET /api/v1/jobs/{job_id}/download` - Download results
- [ ] `GET /api/v1/search/presets` - Saved search presets

**Files to Modify:**
- `/var/www/lead-generation-platform/backend/enrichment/routes.py`

---

### Phase 3: Frontend Enhancement (Week 2-3)

**Priority:** High

- [ ] Create new React dashboard (or enhance existing)
- [ ] Implement Flow 1 UI: Domain upload with options
- [ ] Implement Flow 2 UI: Search criteria form
- [ ] Implement Flow 3 UI: LinkedIn upload
- [ ] Add job progress tracking with SSE
- [ ] Add CSV download functionality
- [ ] Add search presets (save/load)

**Files to Create/Modify:**
- New frontend in `/var/www/lead-generation-platform/frontend/`
- Or integrate into existing React app

---

### Phase 4: Testing & Optimization (Week 3)

**Priority:** Medium

- [ ] Load testing with 10K+ domains
- [ ] Rate limit tuning
- [ ] Error handling improvements
- [ ] Cost optimization (maximize Contacts DB usage)

---

## Appendix A: Blitz API Field Values

### Industry (100+ values)

```python
INDUSTRIES = [
    "Accounting",
    "Airlines and Aviation",
    "Animation",
    "Apparel and Fashion",
    "Architecture and Planning",
    "Automotive",
    "Banking",
    "Biotechnology",
    "Computer Software",
    "Construction",
    "Defense and Space",
    "E-Learning",
    "Education Management",
    "Electrical/Electronic Manufacturing",
    "Entertainment",
    "Financial Services",
    "Food and Beverages",
    "Government Administration",
    "Health, Wellness and Fitness",
    "Hospital and Health Care",
    "Hospitality",
    "Information Technology and Services",
    "Insurance",
    "Internet",
    "Legal Services",
    "Logistics and Supply Chain",
    "Marketing and Advertising",
    "Mechanical or Industrial Engineering",
    "Medical Devices",
    "Music",
    "Non-Profit Organization Management",
    "Oil and Energy",
    "Pharmaceuticals",
    "Professional Training and Coaching",
    "Real Estate",
    "Restaurants",
    "Retail",
    "Security and Investigations",
    "Sports",
    "Staffing and Recruiting",
    "Telecommunications",
    "Venture Capital and Private Equity",
    # ... and 60+ more
]
```

### Employee Range

```python
EMPLOYEE_RANGES = [
    "1-10",
    "11-50",
    "51-200",
    "201-500",
    "501-1000",
    "1001-5000",
    "5001-10000",
    "10001+"
]
```

### Job Levels

```python
JOB_LEVELS = [
    "C-Team",      # CEO, CTO, CFO, CMO, CRO...
    "VP",          # Vice Presidents
    "Director",    # Directors
    "Manager",    # Managers
    "Staff",       # Individual contributors
    "Other"
]
```

### Job Functions (22 values)

```python
JOB_FUNCTIONS = [
    "Advertising & Marketing",
    "Art, Culture and Creative Professionals",
    "Construction",
    "Customer/Client Service",
    "Education",
    "Engineering",
    "Finance & Accounting",
    "General Business & Management",
    "Healthcare & Human Services",
    "Human Resources",
    "Information Technology",
    "Legal",
    "Manufacturing & Production",
    "Operations",
    "Public Administration & Safety",
    "Purchasing",
    "Research & Development",
    "Sales & Business Development",
    "Science",
    "Supply Chain & Logistics",
    "Writing/Editing"
]
```

### Sales Regions

```python
SALES_REGIONS = [
    "NORAM",   # North America
    "LATAM",   # Latin America
    "EMEA",    # Europe, Middle East, Africa
    "APAC"     # Asia-Pacific
]
```

### Country Codes

```python
# Common codes (full list: 200+ countries)
COUNTRIES = {
    "US": "United States",
    "GB": "United Kingdom",
    "CA": "Canada",
    "DE": "Germany",
    "FR": "France",
    "AU": "Australia",
    "NL": "Netherlands",
    "IN": "India",
    "JP": "Japan",
    "BR": "Brazil",
    # ... etc
}
```

---

## Cost Optimization Strategy

### Current vs. Projected Costs

| Scenario | Current (Blitz Only) | Projected (DB First) |
|----------|---------------------|---------------------|
| 10K domains | $X | $X * 0.3 |
| 100K domains | $X | $X * 0.3 |
| 1M domains | $X | $X * 0.3 |

### How It Works

1. **Contacts DB Check First** (free)
   - Check if company exists in internal DB
   - Check if person exists in internal DB

2. **Blitz API Fallback** (paid)
   - Only call Blitz if Contacts DB fails
   - Typical hit rate: 70-90%

---

## Success Metrics

- [ ] Flow 1: 500 domains → 5 minutes
- [ ] Flow 2: Search returns 1000 companies in 10 seconds
- [ ] Flow 3: 1000 LinkedIn URLs → 3 minutes
- [ ] Cost reduction: 70%+ via Contacts DB
- [ ] UI: Account Manager can complete any flow without support

---

## Next Steps

1. **Approve this design** - Confirm the 3 flows and priorities
2. **Phase 1: Backend** - Start with blitz_client.py enhancements
3. **Phase 2: API** - Add FastAPI endpoints
4. **Phase 3: Frontend** - Build React dashboard
5. **Phase 4: Test** - Load testing and optimization

---

**Document Version:** 1.0
**Created:** 2026-03-13
**Last Updated:** 2026-03-13
