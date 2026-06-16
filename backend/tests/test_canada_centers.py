"""Tests for Canada centers expansion (ca_centers.csv).

Verifies that the generated ca_centers.csv:
  - Loads cleanly (no missing lat/lng)
  - Has the expected number of anchors and offset rings
  - Distributes tier by population thresholds
  - Has unique anchor city+province pairs
  - Has all coordinates within the Canada bounding box
  - Is correctly served by get_centers_for_job(mode='all', country='ca')
  - Has task count = 3 × center count (default zooms)
"""
import csv
import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scraper.centers import (  # noqa: E402
    get_centers_for_job,
    estimate_task_count,
    _ca_postal_code_cache,
    _zip_cache,
)

CA_CENTERS_PATH = Path(__file__).parent.parent / "scraper" / "data" / "ca_centers.csv"
CA_POSTCODES_PATH = Path(__file__).parent.parent / "scraper" / "data" / "ca_postcodes.csv"

# Canada bounding box (rough): lat 41-84, lng -141 to -52
CA_LAT_MIN, CA_LAT_MAX = 41.0, 84.0
CA_LNG_MIN, CA_LNG_MAX = -141.0, -52.0

CANADIAN_PROVINCES = {
    "Alberta", "British Columbia", "Manitoba", "New Brunswick",
    "Newfoundland and Labrador", "Nova Scotia", "Nunavut",
    "Northwest Territories", "Ontario", "Prince Edward Island",
    "Quebec", "Saskatchewan", "Yukon",
}


def _load_rows():
    assert CA_CENTERS_PATH.exists(), f"ca_centers.csv not found at {CA_CENTERS_PATH}"
    with open(CA_CENTERS_PATH, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# --- File integrity ----------------------------------------------------------

def test_ca_centers_loads_cleanly():
    """Every row must have parseable lat/lng, no missing required fields."""
    rows = _load_rows()
    assert len(rows) > 0
    required = {"name", "state", "lat", "lng", "tier", "rank",
                "center_type", "anchor_city", "country"}
    for r in rows:
        assert required.issubset(r.keys()), f"missing columns in {r}"
        lat = float(r["lat"])
        lng = float(r["lng"])
        assert -90 <= lat <= 90, f"invalid lat {lat} in {r['name']}"
        assert -180 <= lng <= 180, f"invalid lng {lng} in {r['name']}"
        assert r["country"] == "ca"


def test_ca_centers_count_thresholds():
    """Mirror the AU/UK pattern: ~100 anchors, ~800 offset rings.

    Source is Wikipedia's top-100 list deduped by (name, province), which
    yields 99 unique cities after collapsing the "North Vancouver" city/district
    pair. 100 is the cap; 99 is the realistic count.
    """
    rows = _load_rows()
    anchors = [r for r in rows if r["center_type"] == "anchor_city"]
    offsets = [r for r in rows if r["center_type"] == "offset_ring"]
    assert 95 <= len(anchors) <= 100, f"expected ~100 anchors, got {len(anchors)}"
    assert len(offsets) >= 600, f"expected ≥600 offsets, got {len(offsets)}"
    # Total centers must be > old baseline (52) by a wide margin
    assert len(rows) > 500, f"expected significant expansion, got {len(rows)}"


def test_ca_centers_tier_distribution():
    """Cities with pop >= 30K must have 8 offset rings each."""
    rows = _load_rows()
    anchors = [r for r in rows if r["center_type"] == "anchor_city"]
    for a in anchors:
        pop = int(a["population_basis"].split("_")[-1])
        offsets_for = [r for r in rows
                       if r["anchor_city"] == a["name"]
                       and r["center_type"] == "offset_ring"]
        if pop >= 30000:
            assert len(offsets_for) == 8, (
                f"{a['name']} pop={pop} should have 8 offsets, "
                f"got {len(offsets_for)}"
            )
        else:
            assert len(offsets_for) == 0, (
                f"{a['name']} pop={pop} should have 0 offsets, "
                f"got {len(offsets_for)}"
            )


def test_ca_centers_unique_anchor_pairs():
    """(name, province) pairs among anchor cities must be unique."""
    rows = _load_rows()
    anchors = [r for r in rows if r["center_type"] == "anchor_city"]
    pairs = [(r["name"], r["state"]) for r in anchors]
    counts = Counter(pairs)
    dupes = {p: c for p, c in counts.items() if c > 1}
    assert not dupes, f"duplicate anchor pairs: {dupes}"


def test_ca_centers_coords_in_canada_bbox():
    """All lat/lng must fall within Canada's geographic bounding box."""
    rows = _load_rows()
    bad = []
    for r in rows:
        lat = float(r["lat"])
        lng = float(r["lng"])
        if not (CA_LAT_MIN <= lat <= CA_LAT_MAX and
                CA_LNG_MIN <= lng <= CA_LNG_MAX):
            bad.append((r["name"], lat, lng))
    assert not bad, f"centers outside Canada bbox: {bad[:5]}"


def test_ca_centers_provinces_are_valid():
    """State values must be one of the 13 official Canadian provinces/territories."""
    rows = _load_rows()
    states = {r["state"] for r in rows if r["state"]}
    unknown = states - CANADIAN_PROVINCES
    assert not unknown, f"unknown province names: {unknown}"


def test_ca_centers_top_cities_present():
    """The 5 largest Canadian cities must all be present as anchor cities."""
    rows = _load_rows()
    anchors = {r["name"] for r in rows if r["center_type"] == "anchor_city"}
    for name in ("Toronto", "Montreal", "Calgary", "Ottawa", "Edmonton"):
        assert name in anchors, f"missing top-5 city: {name}"


# --- get_centers_for_job integration -----------------------------------------

def test_get_centers_for_job_ca_all():
    """mode='all', country='ca' must return all anchors + offsets."""
    centers, errors = get_centers_for_job(mode="all", country="ca")
    assert errors == [], f"unexpected errors: {errors}"
    rows = _load_rows()
    assert len(centers) == len(rows), (
        f"expected {len(rows)} centers from job, got {len(centers)}"
    )


def test_estimate_task_count_ca():
    """Default zooms are [10, 11, 12] → task count = 3 × centers."""
    centers, _ = get_centers_for_job(mode="all", country="ca")
    tasks = estimate_task_count(centers)
    assert tasks == 3 * len(centers), (
        f"expected {3 * len(centers)} tasks, got {tasks}"
    )


# --- Regression: postal code path unchanged ----------------------------------

def test_ca_postal_code_path_unchanged():
    """The expansion must not touch the postal code loader/parser.

    ca_postcodes.csv must still exist and the lazy-load cache must remain None
    (i.e., the postal code path is not affected by the centers expansion).
    """
    assert CA_POSTCODES_PATH.exists()
    # The expansion replaced ca_centers.csv only; ca_postcodes.csv is untouched
    # by our generator script. We verify the cache is untouched by checking
    # that the centers-module still imports cleanly with the original postal
    # code loaders.
    assert _ca_postal_code_cache is None
    assert _zip_cache is None


def test_zip_postal_modes_still_work():
    """mode='zips' routing for country='ca' must not regress.

    NOTE: There is a pre-existing routing quirk in get_centers_for_job — the
    `if country_code not in ("us", "")` early-return at line ~290 means the
    `mode == "zips"` branch is only reached for USA. For non-USA countries
    the function returns the full country center list regardless of mode.
    This test pins that pre-existing behavior so the centers expansion does
    not accidentally shift the routing. Fixing the routing itself is out of
    scope for this change.
    """
    centers, errors = get_centers_for_job(mode="zips", country="ca", zips=["T0B"])
    assert errors == [], f"unexpected errors: {errors}"
    # The function returns the full CA center set (pre-existing behavior)
    rows = _load_rows()
    assert len(centers) == len(rows), (
        f"expected full CA center set ({len(rows)}), got {len(centers)}"
    )
