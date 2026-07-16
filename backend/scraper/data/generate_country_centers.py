#!/usr/bin/env python3
"""
Generic centers generator for Latin American countries.

Reads GeoNames `cities15000` (+ `admin1CodesASCII.txt` for state names),
filters by ISO-2 country code, ranks by population, dedups nearby metros,
and emits a `<code>_centers.csv` with anchor cities (+ optional offset rings)
using the SAME offset deltas and 10-column schema as generate_au/ca/uk_centers.py.

Usage:
    python generate_country_centers.py mx            # generate one country
    python generate_country_centers.py mx --dry-run  # print plan, write nothing
    python generate_country_centers.py --all         # generate all configured
"""
from __future__ import annotations

import argparse
import csv
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).parent
DEFAULT_SOURCE = DATA_DIR / "geonames_cities15000.txt"
ADMIN1_FILE = DATA_DIR / "admin1CodesASCII.txt"

# ---------------------------------------------------------------------------
# Constants — copied EXACTLY from generate_au_centers.py / generate_ca_centers.py
# ---------------------------------------------------------------------------

# 8 offset directions: ~20km tiles (0.18 deg lat ~ 20km; 0.29 deg lng ~ 20-25km
# at mid-latitudes). Sign-correct globally: +lat always points north.
OFFSETS = [
    ("East", 0, 0.29),
    ("North", 0.18, 0),
    ("South", -0.18, 0),
    ("West", 0, -0.29),
    ("Northeast", 0.13, 0.21),
    ("Northwest", 0.13, -0.21),
    ("Southeast", -0.13, 0.21),
    ("Southwest", -0.13, -0.21),
]

OUTPUT_FIELDS = [
    "name", "state", "lat", "lng", "tier", "rank",
    "population_basis", "center_type", "anchor_city", "country",
]

# Population tier thresholds (mirrors generate_ca/uk_centers.py).
# `tier` is metadata only — the crawler never branches on it.
TIER_THRESHOLDS = [(500000, "metro"), (100000, "regional_large"), (30000, "regional")]


def _tier_for(pop: int) -> str:
    for threshold, label in TIER_THRESHOLDS:
        if pop >= threshold:
            return label
    return "town"


# ---------------------------------------------------------------------------
# Config per country
# ---------------------------------------------------------------------------

@dataclass
class CountryConfig:
    iso2: str
    display_name: str
    top_n: int
    include_offset_rings: bool
    ring_min_population: int = 30000
    min_distance_km: float = 20.0
    fetch_multiplier: float = 1.4
    exclude_names: set[str] = field(default_factory=set)
    # Cities that must appear as anchors regardless of population rank (e.g. an
    # isolated far-flung hub like Punta Arenas that the over-fetch pool misses).
    force_include: set[str] = field(default_factory=set)
    # bbox = (lat_min, lat_max, lng_min, lng_max); used to validate anchors.
    bbox: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 0.0)

    @property
    def output_path(self) -> Path:
        return DATA_DIR / f"{self.iso2}_centers.csv"

    @property
    def population_basis_label(self) -> str:
        return f"geonames_{self.iso2}_2026"


# Manifest (see plan §2). bbox from Natural Earth / Wikipedia extreme points.
COUNTRY_CONFIGS: dict[str, CountryConfig] = {
    "mx": CountryConfig("mx", "Mexico", 30, True, 30000,
                        force_include={"Guadalajara"},  # GeoNames ranks Zapopan higher; force canonical metro name
                        bbox=(14.54, 32.72, -117.13, -86.81)),
    "br": CountryConfig("br", "Brazil", 20, True, 30000,
                        bbox=(-33.75, 5.27, -73.99, -34.79)),
    "ar": CountryConfig("ar", "Argentina", 15, True, 500000,
                        bbox=(-55.06, -21.80, -73.45, -53.68)),
    "co": CountryConfig("co", "Colombia", 17, True, 30000,
                        bbox=(-4.23, 12.46, -79.01, -66.85)),
    "cl": CountryConfig("cl", "Chile", 15, True, 400000,
                        force_include={"Punta Arenas"},
                        bbox=(-56.54, -17.50, -75.65, -66.98)),
    "pe": CountryConfig("pe", "Peru", 15, False, bbox=(-18.35, -0.04, -81.33, -68.65)),
    "ve": CountryConfig("ve", "Venezuela", 15, False, bbox=(0.72, 12.16, -73.31, -59.76)),
    "ec": CountryConfig("ec", "Ecuador", 15, False, bbox=(-4.96, 1.38, -80.97, -75.23)),
    "bo": CountryConfig("bo", "Bolivia", 12, False, bbox=(-22.87, -9.76, -69.65, -57.45)),
    "py": CountryConfig("py", "Paraguay", 10, False, bbox=(-27.55, -19.34, -62.69, -54.29)),
    "uy": CountryConfig("uy", "Uruguay", 13, False, bbox=(-34.95, -30.11, -58.43, -53.21)),
}


# ---------------------------------------------------------------------------
# Source loaders
# ---------------------------------------------------------------------------

def _load_admin1_names() -> dict[str, str]:
    """Return {concatenated_code: readable_name} from admin1CodesASCII.txt."""
    names: dict[str, str] = {}
    if not ADMIN1_FILE.exists():
        return names
    with open(ADMIN1_FILE, encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2 and parts[0]:
                names[parts[0]] = parts[1]  # "MX.09" -> "Mexico City"
    return names


def _load_country_cities(code: str, source: Path = DEFAULT_SOURCE) -> list[dict[str, Any]]:
    """Read GeoNames cities15000, filter by ISO-2, drop PPLX, parse + sort by pop desc."""
    cities: list[dict[str, Any]] = []
    if not source.exists():
        raise FileNotFoundError(f"GeoNames source not found: {source}")
    with open(source, encoding="utf-8") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if len(p) < 15:
                continue
            cc = p[8].upper()
            if cc != code.upper():
                continue
            feature_code = p[7]
            if feature_code == "PPLX":  # neighborhoods/boroughs (Iztapalapa, Kennedy)
                continue
            try:
                pop = int(p[14])
                lat = float(p[4])
                lng = float(p[5])
            except ValueError:
                continue
            if pop <= 0:
                continue
            cities.append({
                "name": p[1].strip(),
                "lat": lat,
                "lng": lng,
                "admin1": p[10].strip(),
                "population": pop,
            })
    cities.sort(key=lambda x: x["population"], reverse=True)
    return cities


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius_km = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius_km * math.asin(math.sqrt(a))


def _select_anchors(cities: list[dict[str, Any]], cfg: CountryConfig) -> list[dict[str, Any]]:
    """Force-include required cities, over-fetch by population rank, dedup by
    min_distance_km, cap at top_n. Returns anchors sorted by population desc."""
    # Force-included cities first (appear regardless of rank, exempt from dedup).
    forced_names = {n.lower() for n in cfg.force_include}
    accepted: list[dict[str, Any]] = [c for c in cities if c["name"].lower() in forced_names]

    pool = [c for c in cities
            if c["name"] not in cfg.exclude_names and c["name"].lower() not in forced_names]
    over_fetch = int(cfg.top_n * cfg.fetch_multiplier)
    candidates = pool[:max(over_fetch, cfg.top_n)]
    for c in candidates:
        if len(accepted) >= cfg.top_n:
            break
        too_close = any(
            _haversine_km(c["lat"], c["lng"], a["lat"], a["lng"]) < cfg.min_distance_km
            for a in accepted
        )
        if not too_close:
            accepted.append(c)
    # Rank by population so rank #1 is the largest city.
    accepted.sort(key=lambda x: x["population"], reverse=True)
    return accepted


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def _validate_bbox(anchors: list[dict[str, Any]], cfg: CountryConfig) -> list[str]:
    """Return list of violation messages for anchors outside cfg.bbox (empty = ok)."""
    lat_min, lat_max, lng_min, lng_max = cfg.bbox
    violations: list[str] = []
    for a in anchors:
        if not (lat_min <= a["lat"] <= lat_max and lng_min <= a["lng"] <= lng_max):
            violations.append(
                f"  {a['name']} ({a['lat']}, {a['lng']}) outside bbox "
                f"lat[{lat_min},{lat_max}] lng[{lng_min},{lng_max}]"
            )
    return violations


def build_rows(cfg: CountryConfig, admin1_names: dict[str, str],
               source: Path = DEFAULT_SOURCE) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    """Build output CSV rows + return accepted anchors (for reporting/validation)."""
    cities = _load_country_cities(cfg.iso2, source)
    if not cities:
        raise ValueError(f"No cities found in source for country '{cfg.iso2}'")
    anchors = _select_anchors(cities, cfg)

    rows: list[dict[str, str]] = []
    for rank, a in enumerate(anchors, 1):
        state = admin1_names.get(f"{cfg.iso2.upper()}.{a['admin1']}", a["admin1"]) or a["admin1"]
        tier = _tier_for(a["population"])
        rows.append({
            "name": a["name"],
            "state": state,
            "lat": f"{a['lat']:.7f}",
            "lng": f"{a['lng']:.7f}",
            "tier": tier,
            "rank": str(rank),
            "population_basis": f"{cfg.population_basis_label}_{a['population']}",
            "center_type": "anchor_city",
            "anchor_city": a["name"],
            "country": cfg.iso2,
        })
        if cfg.include_offset_rings and a["population"] >= cfg.ring_min_population:
            for direction, dlat, dlng in OFFSETS:
                rows.append({
                    "name": f"{a['name']} - {direction}",
                    "state": state,
                    "lat": f"{round(a['lat'] + dlat, 7):.7f}",
                    "lng": f"{round(a['lng'] + dlng, 7):.7f}",
                    "tier": f"{tier}_offset",
                    "rank": str(rank),
                    "population_basis": "derived",
                    "center_type": "offset_ring",
                    "anchor_city": a["name"],
                    "country": cfg.iso2,
                })
    return rows, anchors


def write_csv(cfg: CountryConfig, rows: list[dict[str, str]]) -> None:
    with open(cfg.output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _print_plan(cfg: CountryConfig, anchors: list[dict[str, Any]]) -> None:
    ringed = [a for a in anchors if cfg.include_offset_rings and a["population"] >= cfg.ring_min_population]
    total = len(anchors) + len(ringed) * len(OFFSETS)
    print(f"\n=== {cfg.display_name} ({cfg.iso2}) ===")
    print(f"accepted anchors: {len(anchors)}  |  ringed metros: {len(ringed)}  "
          f"|  total centers: {total}  |  tasks (x3): {total * 3}")
    print(f"rings: {'on (min_pop=' + str(cfg.ring_min_population) + ')' if cfg.include_offset_rings else 'off'}"
          f"  |  dedup: {cfg.min_distance_km} km")
    print("top anchors (rank, name, state, pop, rings):")
    for i, a in enumerate(anchors[:12], 1):
        has_rings = cfg.include_offset_rings and a["population"] >= cfg.ring_min_population
        print(f"  {i:>2}. {a['name']:<28} {a.get('_state_disp', ''):<22} "
              f"{a['population']:>11,}  {'8' if has_rings else '0'}")


def generate(cfg: CountryConfig, source: Path = DEFAULT_SOURCE,
             dry_run: bool = False) -> dict[str, Any]:
    """Generate one country. Returns a result dict with counts + bbox violations."""
    admin1_names = _load_admin1_names()
    rows, anchors = build_rows(cfg, admin1_names, source)
    # attach display state for printing
    for a in anchors:
        a["_state_disp"] = admin1_names.get(f"{cfg.iso2.upper()}.{a['admin1']}", a["admin1"])
    violations = _validate_bbox(anchors, cfg)
    ringed = sum(1 for a in anchors if cfg.include_offset_rings and a["population"] >= cfg.ring_min_population)

    _print_plan(cfg, anchors)
    if violations:
        print("BBOX VIOLATIONS:")
        for v in violations:
            print(v)

    result = {
        "code": cfg.iso2,
        "anchors": len(anchors),
        "ringed": ringed,
        "total": len(rows),
        "violations": violations,
    }
    if not dry_run:
        if violations:
            print(f"  !! {cfg.iso2}: NOT writing (bbox violations present)")
        else:
            write_csv(cfg, rows)
            print(f"  -> wrote {cfg.output_path.name} ({len(rows)} rows)")
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate LATAM centers CSVs from GeoNames.")
    ap.add_argument("country", nargs="?", help="ISO-2 code (e.g. mx) — omit with --all")
    ap.add_argument("--all", action="store_true", help="generate all configured countries")
    ap.add_argument("--dry-run", action="store_true", help="print plan, write nothing")
    ap.add_argument("--source", default=str(DEFAULT_SOURCE), help="path to geonames cities15000")
    args = ap.parse_args()

    source = Path(args.source)
    if args.all:
        targets = list(COUNTRY_CONFIGS.values())
    elif args.country:
        code = args.country.lower()
        if code not in COUNTRY_CONFIGS:
            print(f"Unknown country '{code}'. Configured: {', '.join(COUNTRY_CONFIGS)}")
            return 2
        targets = [COUNTRY_CONFIGS[code]]
    else:
        ap.error("provide a country code or --all")

    any_violation = False
    for cfg in targets:
        res = generate(cfg, source=source, dry_run=args.dry_run)
        if res["violations"]:
            any_violation = True

    print("\n" + "=" * 60)
    if any_violation:
        print("RESULT: bbox violations found — fix config/bbox before writing CSVs.")
        return 1
    print("RESULT: ok" + (" (dry-run, nothing written)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
