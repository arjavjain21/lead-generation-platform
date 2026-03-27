# Unified Enrichment API Documentation

## Overview

The Unified Enrichment API provides a single endpoint to enrich company domains and person data with decision-maker contacts, email addresses, and LinkedIn profiles. It automatically routes requests through the optimal data source based on availability.

**Base URL:** `https://listbuilding.eagleinfoservice.com/api/enrichment/enrich`

---

## Authentication

All requests require a JWT Bearer token. Obtain a token via:

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "your@email.com", "password": "yourpassword"}'
```

Use the returned `access_token` in all subsequent requests:

```bash
-H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```

---

## Input Modes

The endpoint automatically detects the enrichment mode based on input parameters:

| Mode | Parameters Required | Description |
|------|-------------------|-------------|
| `domain_only` | `domain` only | Get decision makers for a company |
| `linkedin_only` | `linkedin_url` only (no domain) | Enrich a specific person by LinkedIn |
| `enhanced` | `domain` + (`full_name` or `linkedin_url`) | Get company contacts + enrich specific person |

---

## API Reference

### GET Endpoint

```http
GET /api/enrichment/enrich?domain=example.com&titles=CEO,CTO
```

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `domain` | string | Yes* | Company domain (e.g., `google.com`) |
| `linkedin_url` | string | Yes* | LinkedIn profile URL |
| `full_name` | string | No | Full name of person |
| `first_name` | string | No | First name (use with `last_name`) |
| `last_name` | string | No | Last name (use with `first_name`) |
| `max_results` | integer | No | Max contacts to return (1-10, default: 5) |
| `titles` | string | No | Comma-separated titles (e.g., `CEO,CTO,VP`) |
| `cascade_json` | string | No | Custom cascade as JSON string (advanced) |

*At least `domain` OR `linkedin_url` must be provided.

### POST Endpoint

```bash
curl -X POST https://listbuilding.eagleinfoservice.com/api/enrichment/enrich \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "example.com",
    "titles": "CEO,CTO",
    "max_results": 5
  }'
```

**Request Body:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `domain` | string | Yes* | Company domain |
| `linkedin_url` | string | Yes* | LinkedIn profile URL |
| `full_name` | string | No | Full name of person |
| `first_name` | string | No | First name |
| `last_name` | string | No | Last name |
| `max_results` | integer | No | Max contacts (default: 5) |
| `titles` | string | No | Comma-separated titles filter |
| `cascade` | array | No | Custom cascade filter (advanced) |

*At least `domain` OR `linkedin_url` must be provided.

---

## Response Format

```json
{
  "domain": "google.com",
  "mode": "domain_only",
  "company_linkedin_url": "http://www.linkedin.com/company/google",
  "contacts": [
    {
      "full_name": "John Doe",
      "first_name": "John",
      "last_name": "Doe",
      "title": "VP of Sales",
      "email": "john@google.com",
      "linkedin_url": "https://linkedin.com/in/johndoe",
      "headline": "VP of Sales at Google",
      "location_city": "San Francisco",
      "location_country": "US",
      "icp_tier": 1,
      "email_source": "blitz_email"
    }
  ],
  "contact_count": 1,
  "data_sources": {
    "company_linkedin": "contacts_db",
    "contacts": "blitz",
    "emails": "blitz_email"
  },
  "sync_to_contacts_db": {
    "status": "success",
    "records_synced": 1,
    "records_skipped": 0,
    "records_failed": 0
  }
}
```

**Response Fields:**

| Field | Description |
|-------|-------------|
| `domain` | Input domain |
| `mode` | Detection mode: `domain_only`, `linkedin_only`, or `enhanced` |
| `company_linkedin_url` | Company LinkedIn URL |
| `contacts` | Array of contact objects |
| `contact_count` | Number of contacts returned |
| `data_sources` | Source tracking for each data type |
| `sync_to_contacts_db` | Sync status to internal database |

---

## Data Source Priority

The system automatically routes to the best available data source:

1. **Company LinkedIn URL:**
   - Contacts DB (Primary)
   - Blitz API (Fallback)

2. **Decision Makers:**
   - Without titles filter: Contacts DB → Blitz
   - With titles filter: Blitz only (to apply title filters)

3. **Email Addresses:**
   - Blitz API (Primary)
   - Contacts DB (Fallback)
   - BetterEnrich (Final fallback)

---

## Title Filtering

### Simple Titles Filter

Use the `titles` parameter for easy filtering:

```
?titles=CEO,CTO,VP
```

This converts to a single-tier cascade requesting only people with those titles.

### Custom Cascade (Advanced)

For full control, use the `cascade` parameter:

```json
{
  "cascade": [
    {
      "include_title": ["CEO", "Founder", "Owner"],
      "exclude_title": ["assistant", "intern", "junior"],
      "location": ["WORLD"],
      "include_headline_search": false
    },
    {
      "include_title": ["VP", "Director"],
      "exclude_title": ["assistant"],
      "location": ["US"],
      "include_headline_search": true
    }
  ]
}
```

**Cascade Properties:**
- `include_title`: Array of job titles to include
- `exclude_title`: Array of titles to exclude
- `location`: Geographic filter (e.g., `["WORLD"]`, `["US"]`, `["UK"]`)
- `include_headline_search`: Search LinkedIn headlines in addition to titles

### Default Cascade

If no titles or cascade is provided, the default 3-tier cascade is used:

1. **Tier 1:** Owner, CEO, Founder, Co-Founder, President
2. **Tier 2:** CMO, VP Marketing, VP Sales, Chief Revenue Officer
3. **Tier 3:** Director of Marketing, Director of Sales, Head of Marketing, Head of Sales

---

## Example Calls

### 1. Domain Only - Default (No Title Filter)

Returns decision makers using the default cascade (Owner/CEO/Founder → C-level → Director).

```bash
# GET
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com" \
  -H "Authorization: Bearer YOUR_TOKEN"

# POST
curl -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"domain": "google.com"}'
```

**Expected Response:**
- Data Source: Contacts DB (preferred)
- Returns: Up to 5 decision makers with emails

---

### 2. Domain Only - With Title Filter

Filter results to specific titles (e.g., only CEOs and CTOs).

```bash
# GET with titles
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com&titles=CEO,CTO" \
  -H "Authorization: Bearer YOUR_TOKEN"

# POST with titles
curl -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"domain": "google.com", "titles": "CEO,CTO"}'
```

**Expected Response:**
- Data Source: Blitz API (required for title filtering)
- Returns: People with matching titles

---

### 3. Domain Only - Custom Cascade

Use custom cascade for multi-tier filtering.

```bash
# GET with cascade_json (URL encoded)
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com&cascade_json=%5B%7B%22include_title%22%3A%5B%22CEO%22%5D%2C%22exclude_title%22%3A%5B%22assistant%22%5D%2C%22location%22%3A%5B%22WORLD%22%5D%7D%5D" \
  -H "Authorization: Bearer YOUR_TOKEN"

# POST with cascade
curl -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "google.com",
    "cascade": [
      {
        "include_title": ["CEO"],
        "exclude_title": ["assistant"],
        "location": ["WORLD"],
        "include_headline_search": false
      }
    ]
  }'
```

---

### 4. LinkedIn Only Mode

Enrich a specific person by their LinkedIn URL (no domain required).

```bash
# GET
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?linkedin_url=https://linkedin.com/in/johndoe" \
  -H "Authorization: Bearer YOUR_TOKEN"

# POST
curl -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"linkedin_url": "https://linkedin.com/in/johndoe"}'
```

---

### 5. Domain + Full Name (Enhanced Mode)

Get company contacts AND enrich a specific person.

```bash
# GET
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com&full_name=John%20Doe" \
  -H "Authorization: Bearer YOUR_TOKEN"

# POST
curl -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"domain": "google.com", "full_name": "John Doe"}'
```

---

### 6. Domain + LinkedIn (Enhanced Mode)

Get company contacts AND verify/enrich a specific LinkedIn profile.

```bash
# GET
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com&linkedin_url=https://linkedin.com/in/johndoe" \
  -H "Authorization: Bearer YOUR_TOKEN"

# POST
curl -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"domain": "google.com", "linkedin_url": "https://linkedin.com/in/johndoe"}'
```

---

### 7. First Name + Last Name + Domain

Alternative to full_name.

```bash
# POST
curl -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"domain": "google.com", "first_name": "John", "last_name": "Doe"}'
```

---

### 8. With Custom max_results

Limit the number of contacts returned.

```bash
# GET
curl -X GET "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich?domain=google.com&max_results=3" \
  -H "Authorization: Bearer YOUR_TOKEN"

# POST
curl -X POST "https://listbuilding.eagleinfoservice.com/api/enrichment/enrich" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"domain": "google.com", "max_results": 3}'
```

---

## Data Source Examples

### Example: Contacts DB Response (Default)

```json
{
  "data_sources": {
    "company_linkedin": "contacts_db",
    "contacts": "contacts_db",
    "emails": "contacts_db"
  }
}
```

### Example: Blitz API Response (Title Filter)

```json
{
  "data_sources": {
    "company_linkedin": "contacts_db",
    "contacts": "blitz",
    "emails": "blitz_email"
  }
}
```

### Example: Mixed Sources

```json
{
  "data_sources": {
    "company_linkedin": "blitz",
    "contacts": "blitz",
    "emails": "contacts_db"
  }
}
```

---

## Error Handling

### Invalid Domain Format

```json
{
  "detail": "Invalid domain format"
}
```

### Missing Required Parameter

```json
{
  "detail": "Either 'domain' or 'linkedin_url' must be provided"
}
```

### Authentication Failed

```json
{
  "detail": "Not authenticated"
}
```

---

## Rate Limits

**There are no per-user rate limits on the enrichment endpoint.**

The system automatically manages upstream API rate limits:

| API Service | Rate Limit | Behavior |
|-------------|------------|----------|
| Blitz API | 25 requests/second | Automatic throttling |
| Contacts DB | 75 requests/second | Automatic throttling |
| BetterEnrich | 10 requests/second | Automatic throttling |

If an upstream API returns a rate limit error (429), the system automatically retries with exponential backoff.

---

## Troubleshooting

### No Contacts Returned

1. **Check domain spelling** - Ensure domain is correct (e.g., `google.com`, not `google`)
2. **Try with titles filter** - Different filters may return different results
3. **Check data_sources** - If `contacts: not_found`, the domain may not have data in our sources

### Wrong Titles Returned

- Ensure `titles` parameter is properly formatted: `CEO,CTO` (no spaces after commas)
- Or use `cascade_json` for precise control

### API Errors

Check the response `data_sources` to see which source failed:
- `"not_found"`: Source doesn't have data for this entity
- Check service logs for specific error messages

---

## Summary

| Scenario | Parameters | Data Source |
|----------|-----------|-------------|
| Get company decision makers | `domain=google.com` | Contacts DB |
| Filter by title | `domain=google.com&titles=CEO` | Blitz |
| Find person by LinkedIn | `linkedin_url=...` | Contacts DB → Blitz |
| Company + specific person | `domain=google.com&full_name=John Doe` | Contacts DB → Blitz |

## Support

For issues or questions:
- Email: arjav@eagleinfoservice.com
- Check system logs: `journalctl -u lead-generation-platform.service -f`
