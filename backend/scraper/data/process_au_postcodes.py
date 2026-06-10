#!/usr/bin/env python3
"""
Process Australian postcode data to create au_postcodes.csv.
Reads from au_postcodes_raw.csv and creates au_postcodes.csv in format compatible with the scraper.
"""

import csv

# Read the raw postcode data
postcodes = []
with open('au_postcodes_raw.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Extract relevant fields: postcode, lat, lng, locality, state
        # Use Lat_precise/Long_precise when available, otherwise lat/long
        lat = row.get('Lat_precise') or row.get('lat') or '0'
        lng = row.get('Long_precise') or row.get('long') or '0'
        locality = row.get('locality') or ''
        state = row.get('state') or ''
        postcode = row.get('postcode') or ''

        # Skip entries with invalid coordinates
        if lat == '0' or lng == '0' or not postcode:
            continue

        postcodes.append({
            'postcode': postcode,
            'lat': lat,
            'lng': lng,
            'locality': locality.title(),  # Capitalize for consistency
            'state': state
        })

# Remove duplicates (some postcodes have multiple localities)
seen = {}
unique_postcodes = []
for pc in postcodes:
    key = f"{pc['postcode']}_{pc['locality']}_{pc['state']}"
    if key not in seen:
        seen[key] = True
        unique_postcodes.append(pc)

# Write output CSV
with open('au_postcodes.csv', 'w', newline='') as f:
    fieldnames = ['postcode', 'lat', 'lng', 'locality', 'state']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(unique_postcodes)

print(f"Processed {len(postcodes)} raw postcode entries")
print(f"Created {len(unique_postcodes)} unique postcode entries")
print(f"Output written to au_postcodes.csv")

# Show some statistics
states = {}
for pc in unique_postcodes:
    state = pc['state']
    states[state] = states.get(state, 0) + 1

print(f"\nPostcodes by state:")
for state, count in sorted(states.items()):
    print(f"  {state}: {count:,}")
