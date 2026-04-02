# BetterEnrich API Documentation

## Overview
BetterEnrich is an external API service that provides email enrichment and person lookup capabilities. It can be used as a fallback when internal databases and Blitz API don't return results.

**Base URL:** `https://app.betterenrich.com/`

**Authentication:** API Key via `Authorization` header
```
Authorization: YOUR_API_KEY
```

**Rate Limit:** 10 requests/second

---

## Endpoints

### 1. Find Work Email using Waterfall
**POST** `/api/v1/find-work-email`

Find work email from person details. This is the main endpoint for our use case.

**Request:**
```json
{
  "full_name": "John Doe",
  "company_domain": "google.com",
  "linkedinURL": "https://linkedin.com/in/johndoe"
}
```

**Response (202 - In Progress):**
```json
{
  "message": "Success",
  "id": "unique-task-id"
}
```

**Check Status:** Use the returned `id` with Get Work Email Result endpoint.

---

### 2. Get Work Email Result
**GET** `/api/v1/find-work-email?id=task-id`

Check the status of an async email lookup task.

**Response (200 - Completed):**
```json
{
  "id": "task-id",
  "data": {
    "email": "john@google.com",
    "status": "verified",
    "verifier": "some-verifier",
    "ESP": "google"
  },
  "status": "completed"
}
```

---

### 3. Find LinkedIn Profile URL by Email
**POST** `/api/v1/find-linkedin-profile-url-by-email`

Get LinkedIn profile URL from email address.

**Request:**
```json
{
  "Email": "john@google.com"
}
```

**Response:**
```json
{
  "id": "task-id",
  "data": {
    "LinkedIn Profile URL": "https://linkedin.com/in/johndoe"
  },
  "message": "Success"
}
```

---

### 4. Find LinkedIn Profile URL by Name
**POST** `/api/v1/find-linkedin-profile-url-by-name`

Get LinkedIn profile URL from person name and company name.

**Request:**
```json
{
  "FullName": "John Doe",
  "CompanyName": "Google"
}
```

---

### 5. Find Website from Company
**POST** `/api/v1/find-website-from-company`

Get company website from company name.

**Request:**
```json
{
  "CompanyName": "Google Inc"
}
```

---

### 6. Get Credits Balance
**GET** `/api/v1/credits`

Check remaining API credits.

**Response:**
```json
{
  "message": "Success",
  "onetimeCredit": 1000,
  "subscriptionCredit": 500,
  "totalCredit": 1500
}
```

---

## Integration Notes for Lead Generation Platform

### Required Parameters
- **Find Work Email**: Requires `full_name` as mandatory field
- `company_domain` and `linkedinURL` are optional but improve accuracy

### Rate Limiting
- Max 10 requests/second
- Consider implementing request throttling

### Credits
- Monitor credit balance via `/api/v1/credits`
- One credit per API call (typically)

---

*Last Updated: March 2026*
