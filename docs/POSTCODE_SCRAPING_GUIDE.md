# Postcode Scraping Guide

This guide explains how to use postal/zip code-based scraping in the Lead Generation Platform.

## Overview

The platform supports scraping by postal/zip codes for three countries:

| Country | Coverage | Format | Example |
|---------|----------|--------|---------|
| United States | 41,488 zip codes (100%) | NNNNN | 90210, 10001 |
| United Kingdom | ~27,000 postcodes | AN(N)(A) NAA | SW1A 1AA, M1 1AA |
| Canada | ~1,600 FSAs | ANA NAN | K1A 0A1, M5H 2N2 |

## How It Works

When you scrape by postal/zip codes:

1. Each postal/zip code becomes a geographic "center"
2. For each center, API calls are made at zoom levels [10, 11, 12]
3. Each zoom level covers a different radius:
   - Zoom 10: ~100km radius
   - Zoom 11: ~50km radius
   - Zoom 12: ~25km radius
4. Results are deduplicated by place_id
5. Haversine radius filtering is applied

## API Usage

### United States (Zip Codes)

**Mode:** `zips`  
**Country:** `us`

```bash
curl -X POST "https://listbuilding.eagleinfoservice.com/api/scraper/jobs" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "restaurants",
    "mode": "zips",
    "country": "us",
    "zips": ["90210", "10001", "90211"]
  }'
```

### United Kingdom (Postcodes)

**Mode:** `zips`  
**Country:** `gb`

```bash
curl -X POST "https://listbuilding.eagleinfoservice.com/api/scraper/jobs" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "plumbers",
    "mode": "zips",
    "country": "gb",
    "zips": ["SW1A 1AA", "EH1 1PT", "M1 1AA"]
  }'
```

### Canada (Postal Codes)

**Mode:** `zips`  
**Country:** `ca`

```bash
curl -X POST "https://listbuilding.eagleinfoservice.com/api/scraper/jobs" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "dentists",
    "mode": "zips",
    "country": "ca",
    "zips": ["K1A 0A1", "M5H 2N2", "V6B 2W6"]
  }'
```

## CSV Upload

### Upload US Zip Codes

```bash
curl -X POST "https://listbuilding.eagleinfoservice.com/api/scraper/regions/parse-zip-csv" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@us_zips.csv"
```

### Upload UK Postcodes

```bash
curl -X POST "https://listbuilding.eagleinfoservice.com/api/scraper/regions/parse-uk-postcode-csv" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@uk_postcodes.csv"
```

### Upload Canada Postal Codes

```bash
curl -X POST "https://listbuilding.eagleinfoservice.com/api/scraper/regions/parse-ca-postal-csv" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@ca_postcodes.csv"
```

## CSV Format

Your CSV file should contain postal/zip codes in any column. The system will automatically detect and extract them.

### Example US Zip Codes CSV
```csv
zip_code,city
90210,Beverly Hills
10001,New York
90211,Beverly Hills
```

### Example UK Postcodes CSV
```csv
postcode,city
SW1A 1AA,London
EH1 1PT,Edinburgh
M1 1AA,Manchester
```

### Example Canada Postal Codes CSV
```csv
postal_code,city
K1A 0A1,Ottawa
M5H 2N2,Toronto
V6B 2W6,Vancouver
```

## Validation Rules

### US Zip Codes
- Must be exactly 5 digits
- Example: `90210`, `10001`

### UK Postcodes
- Format: A(A)N(N)(A) NAA
- Examples: `SW1A 1AA`, `M1 1AA`, `EH1 1PT`
- Case insensitive (system converts to uppercase)
- Space is optional (system normalizes)

### Canada Postal Codes
- Format: ANA NAN
- Examples: `K1A 0A1`, `M5H 2N2`, `V6B 2W6`
- Case insensitive (system converts to uppercase)
- Space is optional (system normalizes)

## Response Format

### Successful CSV Parse Response (US)
```json
{
  "zips_found": ["90210", "10001"],
  "count_found": 2,
  "valid_zips": ["90210", "10001"],
  "invalid_zips": [],
  "count_valid": 2,
  "count_invalid": 0
}
```

### Successful CSV Parse Response (UK)
```json
{
  "postcodes_found": ["SW1A 1AA", "EH1 1PT"],
  "count_found": 2,
  "valid_postcodes": ["SW1A 1AA", "EH1 1PT"],
  "invalid_postcodes": [],
  "count_valid": 2,
  "count_invalid": 0
}
```

### Successful CSV Parse Response (Canada)
```json
{
  "postal_codes_found": ["K1A 0A1", "M5H 2N2"],
  "count_found": 2,
  "valid_postal_codes": ["K1A 0A1", "M5H 2N2"],
  "invalid_postal_codes": [],
  "count_valid": 2,
  "count_invalid": 0
}
```

## Frontend Usage

### Web Interface

1. Navigate to the Scraper section
2. Select your country (US, UK, or Canada)
3. Select "Zip Codes" / "Postcodes" / "Postal Codes" mode
4. Either:
   - Enter codes manually (one per line or comma-separated)
   - Upload a CSV file containing codes
5. Click "Start Scraper Job"

### Country-Specific Labels

The interface automatically updates labels based on selected country:

| Country | Mode Label | Input Label |
|---------|-----------|-------------|
| US | Zip Codes | Enter US Zip Codes |
| UK | Postcodes | Enter UK Postcodes |
| Canada | Postal Codes | Enter Canada Postal Codes |

## Data Sources

Postal/zip code data is sourced from:

- **US:** Internal database (41,488 zip codes)
- **UK:** GeoNames.org (~27,000 postcodes)
- **Canada:** GeoNames.org (~1,600 forward sortation areas)

## Rate Limits and Quotas

- Non-admin users: 50,000 requests per day
- Each postal/zip code requires 3 API calls (zoom levels 10, 11, 12)
- Example: 100 zip codes = 300 API requests

## Best Practices

1. **Deduplicate:** The system automatically deduplicates codes
2. **Batch Size:** For large jobs, consider splitting into batches of 1,000 codes
3. **Validation:** Always validate codes before starting jobs
4. **Specificity:** More codes = more targeted results, but higher API usage

## Troubleshooting

### Code Not Found in Database

If you receive "not found in database" errors:

- **US:** Verify the 5-digit zip code is correct
- **UK:** Verify the postcode format (try with space: "SW1A 1AA")
- **Canada:** Verify the FSA is correct (first 3 characters for FSAs)

### Invalid Format Errors

- **US:** Must be exactly 5 digits
- **UK:** Must match format AN(N)(A) NAA
- **Canada:** Must match format ANA NAN

## Comparison: Zip Codes vs City-Based

| Feature | Zip/Postal Codes | City-Based |
|---------|------------------|-------------|
| Precision | High (specific areas) | Lower (broader areas) |
| API Calls | 3 per code | 24 per city (8 offsets × 3 zooms) |
| Coverage | Near 100% | Major cities only |
| Overlap | Minimal | Possible overlap |

## Future Enhancements

Planned improvements:

- Enhanced UK coverage (Ordnance Survey CodePoint Open)
- Full Canadian postal codes (currently FSAs only)
- Australia postcodes
- European postal codes
- Custom lat/lng coordinates
