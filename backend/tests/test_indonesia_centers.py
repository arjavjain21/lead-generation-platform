"""Tests for Indonesia centers expansion (id_centers.csv).

Verifies that the generated id_centers.csv:
  - Loads cleanly (no missing lat/lng, all rows country='id')
  - Has the expected number of anchors and offset rings
  - Gives every >=30K anchor its 8 offset rings
  - Has unique anchor city+province pairs
  - Has all coordinates within the Indonesia bounding box (padded for rings)
  - Includes the tourism hubs this expansion targets (Denpasar/Mataram/Yogyakarta)
  - Is correctly served by get_centers_for_job(mode='all', country='id')
  - Has task count = 3 × center count (default zooms)
  - Registers 'id' in COUNTRY_NAMES + get_countries() ordering
"""
import csv
import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scraper.centers import (  # noqa: E402
    COUNTRY_NAMES,
    estimate_task_count,
    get_centers_for_country,
    get_centers_for_job,
    get_countries,
    _ca_postal_code_cache,
    _zip_cache,
)

ID_CENTERS_PATH = Path(__file__).parent.parent / "scraper" / "data" / "id_centers.csv"

# Indonesia bbox from the generator config (lat -11.05..6.18, lng 94.97..141.02),
# padded by the max ring offset (±0.18 lat / ±0.29 lng) so rings validate too.
ID_LAT_MIN, ID_LAT_MAX = -11.25, 6.40
ID_LNG_MIN, ID_LNG_MAX = 94.60, 141.35

EXPECTED_ANCHORS = 32
EXPECTED_RINGS_PER_ANCHOR = 8


def _load_rows():
    assert ID_CENTERS_PATH.exists(), f"id_centers.csv not found at {ID_CENTERS_PATH}"
    with open(ID_CENTERS_PATH, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# --- File integrity ----------------------------------------------------------

def test_id_centers_loads_cleanly():
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
        assert r["country"] == "id"


def test_id_centers_count_thresholds():
    """32 anchors, each with 8 offset rings → 288 centers total."""
    rows = _load_rows()
    anchors = [r for r in rows if r["center_type"] == "anchor_city"]
    offsets = [r for r in rows if r["center_type"] == "offset_ring"]
    assert len(anchors) == EXPECTED_ANCHORS, (
        f"expected {EXPECTED_ANCHORS} anchors, got {len(anchors)}"
    )
    assert len(offsets) == EXPECTED_ANCHORS * EXPECTED_RINGS_PER_ANCHOR, (
        f"expected {EXPECTED_ANCHORS * EXPECTED_RINGS_PER_ANCHOR} offsets, got {len(offsets)}"
    )
    assert len(rows) == EXPECTED_ANCHORS * (1 + EXPECTED_RINGS_PER_ANCHOR)


def test_id_centers_every_anchor_ringed():
    """All top-32 anchors exceed the 30K ring threshold → 8 rings each."""
    rows = _load_rows()
    anchors = [r for r in rows if r["center_type"] == "anchor_city"]
    for a in anchors:
        pop = int(a["population_basis"].split("_")[-1])
        offsets_for = [r for r in rows
                       if r["anchor_city"] == a["name"]
                       and r["center_type"] == "offset_ring"]
        assert pop >= 30000, f"{a['name']} pop={pop} unexpectedly below 30K"
        assert len(offsets_for) == EXPECTED_RINGS_PER_ANCHOR, (
            f"{a['name']} pop={pop} should have 8 offsets, got {len(offsets_for)}"
        )


def test_id_centers_unique_anchor_pairs():
    """(name, province) pairs among anchor cities must be unique."""
    rows = _load_rows()
    anchors = [r for r in rows if r["center_type"] == "anchor_city"]
    pairs = [(r["name"], r["state"]) for r in anchors]
    counts = Counter(pairs)
    dupes = {p: c for p, c in counts.items() if c > 1}
    assert not dupes, f"duplicate anchor pairs: {dupes}"


def test_id_centers_coords_in_indonesia_bbox():
    """All lat/lng (anchors + rings) must fall inside the padded Indonesia bbox."""
    rows = _load_rows()
    bad = []
    for r in rows:
        lat = float(r["lat"])
        lng = float(r["lng"])
        if not (ID_LAT_MIN <= lat <= ID_LAT_MAX and
                ID_LNG_MIN <= lng <= ID_LNG_MAX):
            bad.append((r["name"], lat, lng))
    assert not bad, f"centers outside Indonesia bbox: {bad[:5]}"


def test_id_centers_major_and_tourism_cities_present():
    """Largest metros + the tourism hubs driving this expansion must anchor."""
    rows = _load_rows()
    anchors = {r["name"] for r in rows if r["center_type"] == "anchor_city"}
    # Top-4 by population
    for name in ("Jakarta", "Surabaya", "Bandung", "Medan"):
        assert name in anchors, f"missing major city: {name}"
    # Tourism hubs force-included in the generator config
    for name in ("Denpasar", "Mataram", "Yogyakarta"):
        assert name in anchors, f"missing tourism hub: {name}"


def test_id_centers_states_are_provinces():
    """Spot-check province names resolve to readable English forms."""
    rows = _load_rows()
    state_of = {r["name"]: r["state"] for r in rows
                if r["center_type"] == "anchor_city"}
    assert state_of.get("Jakarta") == "Jakarta"
    assert state_of.get("Denpasar") == "Bali"
    assert state_of.get("Surabaya") == "East Java"


# --- Registry integration ------------------------------------------------------

def test_id_registered_in_country_names():
    """'id' must be in COUNTRY_NAMES (the get_centers_for_job allowlist)."""
    assert COUNTRY_NAMES.get("id") == "Indonesia"


def test_get_centers_for_job_id_all():
    """mode='all', country='id' must return all anchors + offsets, no errors."""
    centers, errors = get_centers_for_job(mode="all", country="id")
    assert errors == [], f"unexpected errors: {errors}"
    rows = _load_rows()
    assert len(centers) == len(rows), (
        f"expected {len(rows)} centers from job, got {len(centers)}"
    )


def test_get_centers_for_country_id():
    """Direct country lookup returns 288 centers, all country='id'."""
    centers = get_centers_for_country("id")
    assert len(centers) == EXPECTED_ANCHORS * (1 + EXPECTED_RINGS_PER_ANCHOR)
    assert all(c["country"] == "id" for c in centers)


def test_id_in_countries_listing():
    """get_countries() must expose Indonesia, ordered after New Zealand."""
    countries = get_countries()
    codes = [c["code"] for c in countries]
    assert "id" in codes
    entry = next(c for c in countries if c["code"] == "id")
    assert entry["name"] == "Indonesia"
    assert codes.index("id") > codes.index("nz"), "id should sort after nz (Asia-Pacific after Oceania)"


def test_unknown_country_still_rejected():
    """Registry expansion must not loosen the unknown-country guard."""
    centers, errors = get_centers_for_job(mode="all", country="zz")
    assert centers == []
    assert errors and "Unknown country" in errors[0]


def test_estimate_task_count_id():
    """Default zooms are [10, 11, 12] → task count = 3 × centers = 864."""
    centers, _ = get_centers_for_job(mode="all", country="id")
    tasks = estimate_task_count(centers)
    assert tasks == 3 * len(centers), (
        f"expected {3 * len(centers)} tasks, got {tasks}"
    )
    assert tasks == 864


# --- Regression: postal/zip paths unchanged ----------------------------------

def test_id_postal_code_paths_unchanged():
    """The expansion must not touch the postal/zip loaders or their caches."""
    assert _ca_postal_code_cache is None
    assert _zip_cache is None
