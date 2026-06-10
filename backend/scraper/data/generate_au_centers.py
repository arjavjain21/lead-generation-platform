#!/usr/bin/env python3
"""
Generate expanded Australian centers CSV with offset rings.
Reads from au_cities_simplemaps.csv and creates au_centers_expanded.csv.
"""

import csv
from pathlib import Path

# Read the Simplemaps data
cities = []
with open('au_cities_simplemaps.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        try:
            row['population'] = int(row['population']) if row['population'] else 0
            cities.append(row)
        except ValueError:
            continue

# Sort by population (descending)
cities.sort(key=lambda x: x['population'], reverse=True)

# Take top 100 cities by population
top_cities = cities[:100]

# Generate centers with offsets
output_rows = []
rank = 1

for city in top_cities:
    lat = float(city['lat'])
    lng = float(city['lng'])
    city_name = city['city']
    state = city['admin_name']
    population = city['population']

    # Determine tier based on population
    if population >= 100000:
        tier = 'metro'
    elif population >= 30000:
        tier = 'regional'
    else:
        tier = 'town'

    # Add anchor city
    output_rows.append({
        'name': city_name,
        'state': state,
        'lat': str(lat),
        'lng': str(lng),
        'tier': tier,
        'rank': str(rank),
        'population_basis': f'simplemaps_au_2026_{population}',
        'center_type': 'anchor_city',
        'anchor_city': city_name,
        'country': 'au'
    })

    # Add offset rings for cities with population >= 30000
    if population >= 30000:
        for direction, offset_lat, offset_lng in [
            ('East', 0, 0.29),
            ('North', 0.18, 0),
            ('South', -0.18, 0),
            ('West', 0, -0.29),
            ('Northeast', 0.13, 0.21),
            ('Northwest', 0.13, -0.21),
            ('Southeast', -0.13, 0.21),
            ('Southwest', -0.13, -0.21)
        ]:
            output_rows.append({
                'name': f"{city_name} - {direction}",
                'state': state,
                'lat': str(round(lat + offset_lat, 7)),
                'lng': str(round(lng + offset_lng, 7)),
                'tier': f'{tier}_offset',
                'rank': str(rank),
                'population_basis': 'derived',
                'center_type': 'offset_ring',
                'anchor_city': city_name,
                'country': 'au'
            })

    rank += 1

# Write output CSV
with open('au_centers_expanded.csv', 'w', newline='') as f:
    fieldnames = ['name', 'state', 'lat', 'lng', 'tier', 'rank', 'population_basis', 'center_type', 'anchor_city', 'country']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)

print(f"Generated {len(output_rows)} centers from {len(top_cities)} cities")
print(f"Output written to au_centers_expanded.csv")

# Show some stats
anchor_count = len([r for r in output_rows if r['center_type'] == 'anchor_city'])
offset_count = len([r for r in output_rows if r['center_type'] == 'offset_ring'])
print(f"  - Anchor cities: {anchor_count}")
print(f"  - Offset rings: {offset_count}")
print(f"  - Total: {len(output_rows)}")

# Show top 10 cities
print("\nTop 10 cities included:")
for i, city in enumerate(top_cities[:10], 1):
    print(f"  {i}. {city['city']}, {city['admin_name']} - {city['population']:,} population")
