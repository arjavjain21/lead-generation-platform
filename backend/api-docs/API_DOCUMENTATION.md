# Lead Generation Platform API Documentation

---

## Overview

The Lead Generation Platform API enriches domains with decision-maker contacts by checking an internal database first, then falling back to an external Blitz API, and automatically syncing results back to the internal database.

**Base URL:** `https://listbuilding.eagleinfoservice.com`

---

## Authentication

You have two options for authentication:

### Option 1: API Keys (Recommended)

API keys are simpler and don't expire. Perfect for programmatic access.

#### Creating an API Key

**Endpoint:** `POST /api/api-keys`

```bash
# First login to get JWT token
TOKEN=$(curl -s -X POST https://listbuilding.eagleinfoservice.com/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"your-email@example.com","password":"your-password"}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

# Create API key
curl -X POST https://listbuilding.eagleinfoservice.com/api/api-keys \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"My API Key"}'
```

**Response:**
```json
{
  "key_id": "550e8400-e29b-41d4-a716-446655440000",
  "api_key": "lgp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "name": "My API Key",
  "created_at": "2026-03-19T04:45:44.564458+00:00"
}
```

**⚠️ Important:** The `api_key` is shown only once! Save it immediately - you won't be able to see it again.

#### Using an API Key

Include the key in the `X-API-Key` header:

```bash
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich/google.com?max_results=3" \
  -H "X-API-Key: lgp_your_api_key_here"
```

#### Listing Your API Keys

**Endpoint:** `GET /api/api-keys`

```bash
curl -X GET https://listbuilding.eagleinfoservice.com/api/api-keys \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

**Response:**
```json
{
  "api_keys": [
    {
      "key_id": "550e8400-e29b-41d4-a716-446655440000",
      "name": "My API Key",
      "created_at": "2026-03-19T04:45:44.564458+00:00",
      "last_used_at": "2026-03-19T05:30:00.000000+00:00",
      "is_active": 1
    }
  ]
}
```

#### Revoking an API Key

**Endpoint:** `DELETE /api/api-keys/{key_id}`

```bash
curl -X DELETE https://listbuilding.eagleinfoservice.com/api/api-keys/550e8400-e29b-41d4-a716-446655440000 \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

---

### Option 2: JWT Tokens

All API endpoints (except login) require a JWT token for authentication.

**Endpoint:** `POST /api/auth/login`

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "your-email@example.com",
    "password": "your-password"
  }'
```

**Response:**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "is_admin": false
}
```

**Using the Token:**

Include the token in the `Authorization` header:

```bash
curl -X GET https://listbuilding.eagleinfoservice.com/api/enrichment/enrich/google.com \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

### Refreshing the Token

Tokens expire after 7 days. Refresh using:

**Endpoint:** `POST /api/auth/refresh`

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/auth/refresh \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

### Getting Current User Info

**Endpoint:** `GET /api/auth/me`

```bash
curl -X GET https://listbuilding.eagleinfoservice.com/api/auth/me \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

---

## Domain Enrichment API

### Unified Enrichment Endpoint

The unified enrichment endpoint supports multiple input types - domain only, LinkedIn only, or domain with person details.

**Endpoint:** `POST /api/enrichment/enrich` (with sync) or `GET /api/enrichment/enrich` (no sync)

**Authentication:** Required (JWT or API Key)

**Key Difference:**
- **POST**: Returns contacts AND syncs them to internal database
- **GET**: Returns contacts only (no sync)

---

#### Using GET (Query Parameters)

For simpler use cases, you can use GET with query parameters:

```bash
# Domain only
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com&max_results=5" \
  -H "X-API-Key: YOUR_API_KEY"

# LinkedIn only
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?linkedin_url=https://linkedin.com/in/johndoe" \
  -H "X-API-Key: YOUR_API_KEY"

# Domain + Name
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com&full_name=John%20Doe" \
  -H "X-API-Key: YOUR_API_KEY"

# Domain + LinkedIn
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com&linkedin_url=https://linkedin.com/in/johndoe" \
  -H "X-API-Key: YOUR_API_KEY"
```

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| domain | string | Company domain (e.g., `google.com`) |
| linkedin_url | string | LinkedIn profile URL |
| full_name | string | Full name of person |
| first_name | string | First name |
| last_name | string | Last name |
| max_results | integer | Maximum contacts (default: 5, max: 10) |

**Note:** The GET endpoint returns contacts found but does NOT sync to the internal database. For full sync functionality, use the POST endpoint.

---

#### Request Body (POST)

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| domain | string | No* | Company domain (e.g., `google.com`) |
| linkedin_url | string | No* | LinkedIn profile URL |
| full_name | string | No | Full name of person (e.g., "John Doe") |
| first_name | string | No | First name (alternative to full_name) |
| last_name | string | No | Last name (alternative to full_name) |
| max_results | integer | No | Maximum contacts to return (default: 5) |

*Either `domain` or `linkedin_url` must be provided.

---

#### Input Modes

The endpoint automatically detects the mode based on inputs:

| Input | Mode | Description |
|-------|------|-------------|
| `domain` only | `domain_only` | Find multiple decision makers at company |
| `linkedin_url` only | `linkedin_only` | Find person by LinkedIn URL |
| `domain` + `full_name` | `enhanced` | Find specific person at company |
| `domain` + `linkedin_url` | `enhanced` | Find and enrich person by LinkedIn |

---

#### Example Requests

**1. Domain Only - Find decision makers at a company**

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "google.com",
    "max_results": 5
  }'
```

**2. LinkedIn Only - Find person by LinkedIn URL**

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "linkedin_url": "https://linkedin.com/in/johndoe"
  }'
```

**3. Domain + Full Name - Find specific person**

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "google.com",
    "full_name": "John Doe"
  }'
```

**4. Domain + LinkedIn - Enrich person with LinkedIn**

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "google.com",
    "linkedin_url": "https://linkedin.com/in/johndoe"
  }'
```

---

#### Response Format

```json
{
  "domain": "google.com",
  "mode": "enhanced",
  "company_linkedin_url": "http://www.linkedin.com/company/google",
  "contacts": [
    {
      "full_name": "John Doe",
      "first_name": "John",
      "last_name": "Doe",
      "title": "Software Engineer",
      "email": "john@google.com",
      "linkedin_url": "https://linkedin.com/in/johndoe",
      "headline": "Software Engineer at Google",
      "location_city": "San Francisco",
      "location_country": "USA",
      "icp_tier": 1,
      "email_source": "blitz"
    }
  ],
  "contact_count": 1,
  "data_sources": {
    "company_linkedin": "contacts_db",
    "contacts": "blitz",
    "emails": "blitz"
  },
  "sync_to_contacts_db": {
    "status": "success",
    "records_synced": 1,
    "records_skipped": 0,
    "records_failed": 0
  }
}
```

---

#### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| domain | string | The domain that was enriched |
| mode | string | Detection mode: `domain_only`, `linkedin_only`, or `enhanced` |
| company_linkedin_url | string | Company's LinkedIn URL |
| contacts | array | Array of contact objects |
| contact_count | integer | Number of contacts found |
| data_sources | object | Shows which source provided the data |
| sync_to_contacts_db | object | Sync status to internal database |

**Contact Object Fields:**

| Field | Type | Description |
|-------|------|-------------|
| full_name | string | Full name of the contact |
| first_name | string | First name |
| last_name | string | Last name |
| title | string | Job title |
| email | string | Work email address |
| linkedin_url | string | LinkedIn profile URL |
| headline | string | LinkedIn headline |
| location_city | string | City |
| location_country | string | Country |
| icp_tier | integer | ICP tier (1 = highest priority) |
| email_source | string | Source of email: `contacts_db`, `blitz`, `better_enrich` |

---

#### Data Sources

The `data_sources` field indicates where the data came from:

| Source | Description |
|--------|-------------|
| `contacts_db` | Internal database |
| `blitz` | Blitz API |
| `better_enrich` | BetterEnrich V2 (fallback) |

---

#### Enrichment Flow

The endpoint uses a cascading approach:

1. **Domain Only Mode:**
   - Contacts DB → Blitz API (cascade: CEO → VP → Director) → Sync to DB

2. **LinkedIn Only Mode:**
   - Contacts DB (by LinkedIn) → Blitz API → BetterEnrich V2 (fallback)

3. **Enhanced Mode:**
   - Contacts DB (by LinkedIn or name) → Blitz API → BetterEnrich V2 (fallback) → Sync to DB

---

### Enrich Single Domain

Enrich a domain with decision-maker contacts.

**Endpoint:** `GET /api/enrichment/enrich/{domain}`

**Path Parameters:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| domain | string | The domain to enrich | `google.com` |

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| max_results | integer | 5 | Maximum contacts to return (1-10) |
| cascade_json | string | null | Custom cascade JSON for title prioritization |

**Example Request:**

```bash
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich/google.com?max_results=3" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

**Example Response:**

```json
{
  "domain": "google.com",
  "company_linkedin_url": "http://www.linkedin.com/company/google",
  "contacts": [
    {
      "full_name": "John Doe",
      "first_name": "John",
      "last_name": "Doe",
      "title": "CEO",
      "email": "john@google.com",
      "linkedin_url": "http://www.linkedin.com/in/johndoe",
      "headline": "CEO at Google",
      "location_city": "San Francisco",
      "location_country": "US",
      "icp_tier": 1,
      "email_source": "contacts_db_email"
    }
  ],
  "contact_count": 3,
  "data_sources": {
    "company_linkedin": "contacts_db",
    "contacts": "contacts_db",
    "emails": "contacts_db"
  },
  "sync_to_contacts_db": {
    "status": "success",
    "records_synced": 3,
    "records_skipped": 0,
    "records_failed": 0
  }
}
```

---

## Understanding the Response

### Contact Fields

| Field | Type | Description |
|-------|------|-------------|
| full_name | string | Full name of the contact |
| first_name | string | First name |
| last_name | string | Last name |
| title | string | Job title |
| email | string | Work email address |
| linkedin_url | string | LinkedIn profile URL |
| headline | string | LinkedIn headline |
| location_city | string | City location |
| location_country | string | Country code |
| icp_tier | integer | ICP priority tier (1=highest) |
| email_source | string | Source of email data |

### Email Source Values

| Value | Description |
|-------|-------------|
| `contacts_db_email` | Email found in internal Contacts DB |
| `blitz_email` | Email found via Blitz API |
| `not_found` | No email found |

### Data Sources

The `data_sources` object shows where each piece of data came from:

| Field | Values | Description |
|-------|--------|-------------|
| company_linkedin | `contacts_db`, `blitz`, `not_found` | Source of company LinkedIn URL |
| contacts | `contacts_db`, `blitz`, `not_found` | Source of decision-maker contacts |
| emails | `contacts_db`, `blitz`, `not_found` | Source of email addresses |

### Sync Status

The `sync_to_contacts_db` object shows write-back results:

| Field | Description |
|-------|-------------|
| status | `success`, `failed`, or `no_contacts_to_sync` |
| records_synced | Number of contacts successfully written to internal DB |
| records_skipped | Number of contacts skipped (duplicates or invalid) |
| records_failed | Number of contacts that failed to sync |

---

## Decision Maker Priority (Cascade)

The system uses a 3-tier cascade to prioritize decision makers:

### Tier 1 (Highest Priority)
- Owner
- CEO
- Founder
- Co-Founder
- President

### Tier 2 (Medium Priority)
- CMO
- VP Marketing
- VP Sales
- Chief Revenue Officer
- Chief Marketing Officer
- VP of Marketing
- VP of Sales

### Tier 3 (Lower Priority)
- Director of Marketing
- Director of Sales
- Head of Marketing
- Head of Sales
- Head of Growth
- Marketing Director
- Sales Director

### Custom Cascade

You can provide a custom cascade using the `cascade_json` parameter:

```bash
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich/google.com?cascade_json=%5B%7B%22include_title%22%3A%20%5B%22CEO%22%2C%20%22CTO%22%5D%7D%5D" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

Decoded JSON: `[{"include_title": ["CEO", "CTO"]}]`

---

## How It Works

### Enrichment Flow

1. **Check Internal Contacts DB First**
   - Look up company by domain
   - Get decision-maker contacts with emails

2. **Fallback to Blitz API**
   - If not found in internal DB, use Blitz API
   - Search for company LinkedIn URL
   - Find decision makers using waterfall ICP search

3. **Email Resolution**
   - For each person, try to find work email
   - Priority: Contacts DB → Blitz API

4. **Write Back to Internal DB**
   - All found contacts are automatically synced
   - Next lookup will find them in internal DB

---

## Error Handling

### Common Error Responses

**401 - Authentication Required:**
```json
{
  "detail": "Authentication required."
}
```

**400 - Invalid Domain:**
```json
{
  "detail": "Invalid domain format"
}
```

**400 - Invalid Cascade JSON:**
```json
{
  "detail": "Invalid cascade JSON"
}
```

---

## Rate Limits

- **Non-admin users:** 50,000 API requests per day
- **Admin users:** Unlimited

Check your quota:

**Endpoint:** `GET /api/quota`

```bash
curl -X GET https://listbuilding.eagleinfoservice.com/api/quota \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

**Response:**
```json
{
  "limit": 50000,
  "used": 1234,
  "remaining": 48766,
  "resets_at": "2026-03-20T00:00:00+00:00",
  "is_admin": false
}
```

---

## Testing the API

### Example: Enrich Google.com

```bash
# Step 1: Login to get token
TOKEN=$(curl -s -X POST https://listbuilding.eagleinfoservice.com/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"your-email@example.com","password":"your-password"}' | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

# Step 2: Enrich domain
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich/google.com?max_results=3" \
  -H "Authorization: Bearer $TOKEN"
```

---

## OpenAPI/Swagger Specification

For integration with other tools, an OpenAPI 3.1 specification is available at:

`/api-docs/openapi.json`

You can import this into:
- Postman
- Swagger UI
- API clients
- LLM tools (ChatGPT, Claude, etc.)

---

## Support

- **Email:** support@eagleinfoservice.com
- **API Base URL:** https://listbuilding.eagleinfoservice.com

---

*Last Updated: March 2026*
