#!/usr/bin/env python3
"""
Generate expanded UK centers CSV with offset rings.
Uses current UK centers as base + adds major UK cities from postcode database.
"""

import csv
from pathlib import Path

# Major UK cities with their coordinates (from reliable sources)
# Format: (name, state/region, lat, lng, population_estimate)
uk_major_cities = [
    # Current anchors (keep these)
    ("London", "England", 51.5074, -0.1278, 9000000),
    ("Manchester", "England", 53.4808, -2.2426, 2700000),
    ("Birmingham", "England", 52.4862, -1.8904, 1100000),
    ("Leeds", "England", 53.8008, -1.5491, 790000),
    ("Glasgow", "Scotland", 55.8642, -4.2518, 620000),
    ("Liverpool", "England", 53.4084, -2.9916, 498000),
    ("Sheffield", "England", 53.3811, -1.4701, 580000),
    ("Edinburgh", "Scotland", 55.9533, -3.1883, 500000),
    ("Bristol", "England", 51.4545, -2.5879, 470000),
    ("Cardiff", "Wales", 51.4816, -3.1791, 350000),
    ("Leicester", "England", 52.6369, -1.1398, 350000),
    ("Belfast", "Northern Ireland", 54.5973, -5.9301, 340000),
    ("Nottingham", "England", 52.9548, -1.1581, 310000),
    ("Newcastle", "England", 54.9783, -1.6178, 300000),
    ("Brighton", "England", 50.8225, -0.1372, 290000),
    ("Plymouth", "England", 50.3755, -4.1427, 260000),
    ("Southampton", "England", 50.9097, -1.4044, 250000),
    ("Portsmouth", "England", 50.8198, -1.0880, 205000),
    ("Derby", "England", 52.9225, -1.4746, 250000),
    ("Swansea", "Wales", 51.6214, -3.9436, 245000),
    ("Aberdeen", "Scotland", 57.1497, -2.0943, 200000),
    ("Northampton", "England", 52.2408, -0.9027, 220000),
    ("Reading", "England", 51.4543, -0.9781, 165000),
    (" Dudley", "England", 52.5125, -2.0823, 200000),
    ("Wolverhampton", "England", 52.5814, -2.1281, 265000),
    ("Milton Keynes", "England", 52.0406, -0.7594, 260000),
    ("Coventry", "England", 52.4068, -1.5197, 365000),
    ("Stockport", "England", 53.4106, -2.1587, 140000),
    ("Bolton", "England", 53.5785, -2.4255, 140000),
    ("Blackburn", "England", 53.7489, -2.4888, 120000),
    ("Blackpool", "England", 53.8176, -3.0554, 142000),
    ("Bournemouth", "England", 50.7359, -1.9876, 200000),
    ("Warrington", "England", 53.3917, -2.5954, 170000),
    ("Stoke-on-Trent", "England", 53.0373, -2.2797, 260000),
    ("Sunderland", "England", 54.9047, -1.3817, 175000),
    ("Peterborough", "England", 52.5726, -0.2506, 160000),
    ("Luton", "England", 51.8767, -0.4079, 160000),
    ("York", "England", 53.9576, -1.0827, 150000),
    ("Doncaster", "England", 53.5228, -1.1266, 120000),
    ("Hull", "England", 53.7444, -0.3351, 160000),
    # Additional significant regional cities
    (" Preston", "England", 53.7662, -2.7041, 140000),
    ("Middlesbrough", "England", 54.5771, -1.2345, 150000),
    ("Bradford", "England", 53.7960, -1.7594, 540000),
    ("Halifax", "England", 53.7166, -1.8579, 88000),
    ("Wakefield", "England", 53.6815, -1.4966, 78000),
    ("Wigan", "England", 53.5451, -2.6378, 98000),
    ("Huddersfield", "England", 53.6456, -1.7843, 123000),
    ("Dundee", "Scotland", 56.4620, -2.9707, 150000),
    ("Inverness", "Scotland", 57.4778, -4.2243, 50000),
    ("Perth", "Scotland", 56.3958, -3.4324, 47000),
    ("Dunfermline", "Scotland", 56.0716, -3.4662, 50000),
    ("Stirling", "Scotland", 56.1188, -3.9449, 40000),
    ("Newport", "Wales", 51.5877, -3.0006, 150000),
    ("Swansea", "Wales", 51.6214, -3.9436, 245000),
    ("Cardiff", "Wales", 51.4816, -3.1791, 350000),
    ("Bangor", "Wales", 53.2277, -4.1303, 15000),
    ("Wrexham", "Wales", 53.0455, -3.0013, 42000),
    ("Barry", "Wales", 51.3997, -3.2777, 54000),
    ("Caerphilly", "Wales", 51.5726, -3.2177, 30000),
    ("Neath", "Wales", 51.6615, -3.8024, 50000),
    ("Lisburn", "Northern Ireland", 54.5075, -6.0375, 45000),
    ("Derry", "Northern Ireland", 55.0096, -7.3131, 85000),
]

# Generate centers with offsets
output_rows = []
rank = 1

# Tier 1: Major metros (population 500K+) - add offset rings
tier1_cities = [c for c in uk_major_cities if c[4] >= 500000]

# Tier 2: Significant cities (population 100K-500K) - add offset rings
tier2_cities = [c for c in uk_major_cities if 100000 <= c[4] < 500000]

# Tier 3: Regional cities (population 30K-100K) - add offset rings
tier3_cities = [c for c in uk_major_cities if 30000 <= c[4] < 100000]

# Tier 4: Smaller cities (population <30K) - no offsets
tier4_cities = [c for c in uk_major_cities if c[4] < 30000]

all_cities = tier1_cities + tier2_cities + tier3_cities + tier4_cities

for city in all_cities:
    name, state, lat, lng, population = city

    # Determine tier
    if population >= 500000:
        tier = 'metro'
    elif population >= 100000:
        tier = 'regional_large'
    elif population >= 30000:
        tier = 'regional'
    else:
        tier = 'town'

    # Add anchor city
    output_rows.append({
        'name': name,
        'state': state,
        'lat': str(lat),
        'lng': str(lng),
        'tier': tier,
        'rank': str(rank),
        'population_basis': f'uk_major_cities_2026_{population}',
        'center_type': 'anchor_city',
        'anchor_city': name,
        'country': 'gb'
    })

    # Add offset rings for cities >= 30K population
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
                'name': f"{name} - {direction}",
                'state': state,
                'lat': str(round(lat + offset_lat, 7)),
                'lng': str(round(lng + offset_lng, 7)),
                'tier': f'{tier}_offset',
                'rank': str(rank),
                'population_basis': 'derived',
                'center_type': 'offset_ring',
                'anchor_city': name,
                'country': 'gb'
            })

    rank += 1

# Write output CSV
with open('uk_centers_expanded.csv', 'w', newline='') as f:
    fieldnames = ['name', 'state', 'lat', 'lng', 'tier', 'rank', 'population_basis', 'center_type', 'anchor_city', 'country']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)

print(f"Generated {len(output_rows)} centers from {len(all_cities)} cities")
print(f"Output written to uk_centers_expanded.csv")

# Show some stats
anchor_count = len([r for r in output_rows if r['center_type'] == 'anchor_city'])
offset_count = len([r for r in output_rows if r['center_type'] == 'offset_ring'])
print(f"  - Anchor cities: {anchor_count}")
print(f"  - Offset rings: {offset_count}")
print(f"  - Total: {len(output_rows)}")

# Show tier breakdown
tier_counts = {}
for r in output_rows:
    tier_base = r['tier'].replace('_offset', '')
    tier_counts[tier_base] = tier_counts.get(tier_base, 0) + 1

print(f"\nBy tier:")
for tier, count in sorted(tier_counts.items()):
    print(f"  - {tier}: {count}")

# Show top 15 cities
print("\nTop 15 cities included:")
for i, city in enumerate(all_cities[:15], 1):
    print(f"  {i}. {city[0]}, {city[1]} - Population: {city[4]:,}")
