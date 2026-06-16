#!/usr/bin/env python3
"""
Generate expanded Canadian centers CSV with offset rings.

Reads from ca_cities_statscan.csv (top 100 Canadian municipalities by
2021 Census population) and creates ca_centers.csv with anchor cities
plus 8 offset rings for cities >= 30K population.

Mirrors generate_au_centers.py structure.
"""
import csv
from pathlib import Path

DATA_DIR = Path(__file__).parent
INPUT_FILE = DATA_DIR / "ca_cities_statscan.csv"
OUTPUT_FILE = DATA_DIR / "ca_centers.csv"

cities = []
with open(INPUT_FILE, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            row['population'] = int(row['population'])
        except (ValueError, TypeError):
            continue
        cities.append(row)

# Sort by population descending
cities.sort(key=lambda x: x['population'], reverse=True)

# Take top 100 (already capped by source CSV, but defensive)
top_cities = cities[:100]

output_rows = []
rank = 1

# Offset deltas: 0.18 deg lat ~ 20km, 0.29 deg lng ~ 20-25km at 49N.
# Diagonals at 0.13/0.21 give ~20km radial coverage at 49N.
# Same deltas as AU/UK work in CA latitudes (CA spans 42-62N).
OFFSETS = [
    ('East',      0,     0.29),
    ('North',     0.18,  0),
    ('South',    -0.18,  0),
    ('West',      0,    -0.29),
    ('Northeast', 0.13,  0.21),
    ('Northwest', 0.13, -0.21),
    ('Southeast',-0.13,  0.21),
    ('Southwest',-0.13, -0.21),
]

for city in top_cities:
    lat = float(city['lat'])
    lng = float(city['lng'])
    name = city['name']
    province = city['province']
    population = city['population']

    # Tier classification (mirrors AU thresholds)
    if population >= 500000:
        tier = 'metro'
    elif population >= 100000:
        tier = 'regional_large'
    elif population >= 30000:
        tier = 'regional'
    else:
        tier = 'town'

    # Anchor city
    output_rows.append({
        'name': name,
        'state': province,
        'lat': f"{lat:.7f}",
        'lng': f"{lng:.7f}",
        'tier': tier,
        'rank': str(rank),
        'population_basis': f'statscan_ca_2021_{population}',
        'center_type': 'anchor_city',
        'anchor_city': name,
        'country': 'ca',
    })

    # Offset rings for cities >= 30K
    if population >= 30000:
        for direction, offset_lat, offset_lng in OFFSETS:
            output_rows.append({
                'name': f"{name} - {direction}",
                'state': province,
                'lat': f"{round(lat + offset_lat, 7):.7f}",
                'lng': f"{round(lng + offset_lng, 7):.7f}",
                'tier': f'{tier}_offset',
                'rank': str(rank),
                'population_basis': 'derived',
                'center_type': 'offset_ring',
                'anchor_city': name,
                'country': 'ca',
            })

    rank += 1

# Write
with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['name', 'state', 'lat', 'lng', 'tier', 'rank',
                  'population_basis', 'center_type', 'anchor_city', 'country']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)

anchors = [r for r in output_rows if r['center_type'] == 'anchor_city']
offsets = [r for r in output_rows if r['center_type'] == 'offset_ring']
print(f"Wrote {len(output_rows)} centers from {len(top_cities)} cities")
print(f"  - Anchor cities: {len(anchors)}")
print(f"  - Offset rings:  {len(offsets)}")
print(f"  - Total:         {len(output_rows)}")
print(f"  - Output: {OUTPUT_FILE}")

print("\nTop 10 cities included:")
for i, c in enumerate(top_cities[:10], 1):
    print(f"  {i}. {c['name']}, {c['province']} - {c['population']:,}")
