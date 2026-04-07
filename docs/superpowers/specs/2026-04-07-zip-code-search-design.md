# Zip Code Search Feature Design

## Overview
Add US zip code search capability to the Google Maps scraper. Users can search by entering one or more US zip codes, which are geocoded to lat/lng coordinates for the scraper.

**Scope:** US zip codes only, single zoom level (12), supports both input formats.

---

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|----------|
| Countries | US only | Simpler, faster to implement |
| Zoom level | Single (12) | Focused local search, faster |
| Input format | Both (lines + comma-sep) | Maximum flexibility |

---

## Backend Changes

### 1. Zip Code Geocoding (`scraper/centers.py`)
- Use local US zip-to-lat/lng database (CSV or dict)
- No external API calls - fastest option
- Include ~40K US zip codes with centroids

### 2. New Mode: `zips`
```python
# In StartJobRequest
class StartJobRequest(BaseModel):
    query: str
    country: str = "us"
    mode: str = "all"  # "all" | "states" | "cities" | "zips"
    states: list[str] = []
    cities: list[str] = []
    zips: list[str] = []  # NEW: list of US zip codes
    expected_types: list[str] = []
```

### 3. Validation
- US zip codes: exactly 5 digits (0-9)
- Country must be "us" for zip mode
- Return clear errors for invalid format

### 4. Display Name Generation
- Format: `{query}-zip_codes({count})`
- Example: `dentists-zip_codes(3)`

### 5. Endpoint Changes
- `POST /api/scraper/jobs` - Accept new `zips` field
- `POST /api/scraper/regions/estimate` - Calculate tasks for zip mode

---

## Frontend Changes

### UI Components
1. **New Tab: "Zip Codes"** - Add alongside All/States/Cities tabs
2. **Input Field** - Text area for zip codes
3. **Helper Text** - "Enter zip codes, one per line or comma-separated"
4. **Real-time Validation** - Show errors for invalid zips (non-5-digit)

### Example Display
```
Zip Codes:
┌─────────────────────────────────┐
│ 90210                          │
│ 90211                          │
│ 10001, 10002, 10003            │
└─────────────────────────────────┘
[3 zip codes detected]
```

---

## Error Handling

| Error | Message |
|-------|---------|
| Invalid format | "Invalid zip code '123': must be 5 digits" |
| Not in US mode | "Zip codes only supported for United States" |
| Zip not found | "Zip code '00000' not found in database" |
| Empty input | "Please enter at least one zip code" |

---

## Data Flow

```
1. User enters: "dentists" + zips [90210, 10001, 33101]
2. Frontend validates format → OK
3. Backend geocodes zips:
   - 90210 → {lat: 34.0901, lng: -118.4065}
   - 10001 → {lat: 40.7484, lng: -73..9967}
   - 33101 → {lat: 25.7617, lng: -80.1918}
4. Create 3 centers, run at zoom 12 each
5. Display name: dentists-zip_codes(3)
```

---

## Files to Modify

| File | Changes |
|------|---------|
| `scraper/centers.py` | Add zip database, `get_centers_for_zips()` |
| `routes.py` | Add `zips` field, validate, generate display name |
| `scraper/crawler.py` | No changes needed |
| Frontend (index.html) | Add zip code tab and input |

---

## Testing Checklist

- [ ] Valid US zip codes return correct lat/lng
- [ ] Invalid format (4 digits, letters) rejected
- [ ] Non-US zips rejected when country != us
- [ ] Display name correct format
- [ ] Estimate shows correct task count (1 per zip)
- [ ] Existing all/states/cities modes unchanged
- [ ] Job completes and results downloadable

---

## Rollback Plan
If issues arise, remove `zips` field and zip tab. All existing functionality preserved.
