"""Tests for Latin America centers (mx, br, ar, co, cl, pe, ve, ec, bo, py, uy).

Parameterized over the 11 countries; mirrors test_canada_centers.py assertions:
loads cleanly, anchor count in range, ring rule honored, unique anchor pairs,
coords within bbox, coordinate signs correct, top cities present, served by
get_centers_for_job(mode='all'), task count = 3 x centers, and the postal/zip
caches remain untouched (regression guard).
"""
import csv
import sys
import unicodedata
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from scraper.centers import (  # noqa: E402
    estimate_task_count,
    get_centers_for_job,
    _ca_postal_code_cache,
    _zip_cache,
)

DATA_DIR = Path(__file__).parent.parent / "scraper" / "data"

# Per-country parameters:
#   code, bbox(lat_min,lat_max,lng_min,lng_max), top_cities (ascii-folded),
#   expect_rings, ring_threshold, anchor_range(min,max), sign_rule
PARAMS = [
    pytest.param("mx", (14.54, 32.72, -117.13, -86.81),
                 ["mexico city", "guadalajara", "monterrey", "puebla", "tijuana", "merida"],
                 True, 30000, (25, 30), "pos_neg", id="mx"),
    pytest.param("br", (-33.75, 5.27, -73.99, -34.79),
                 ["sao paulo", "rio de janeiro", "brasilia", "fortaleza", "manaus"],
                 True, 30000, (18, 22), "neg_neg", id="br"),
    pytest.param("ar", (-55.06, -21.80, -73.45, -53.68),
                 ["buenos aires", "cordoba", "rosario"],
                 True, 500000, (14, 16), "neg_neg", id="ar"),
    pytest.param("co", (-4.23, 12.46, -79.01, -66.85),
                 ["bogota", "medellin", "cali", "barranquilla"],
                 True, 30000, (15, 19), "pos_neg", id="co"),
    pytest.param("cl", (-56.54, -17.50, -75.65, -66.98),
                 ["santiago", "punta arenas", "antofagasta"],
                 True, 400000, (12, 15), "neg_neg", id="cl"),
    pytest.param("pe", (-18.35, -0.04, -81.33, -68.65),
                 ["lima", "arequipa", "trujillo"],
                 False, None, (14, 16), "neg_neg", id="pe"),
    pytest.param("ve", (0.72, 12.16, -73.31, -59.76),
                 ["caracas", "maracaibo", "valencia"],
                 False, None, (13, 17), "pos_neg", id="ve"),
    pytest.param("ec", (-4.96, 1.38, -80.97, -75.23),
                 ["quito", "guayaquil", "cuenca"],
                 False, None, (14, 16), "mixed", id="ec"),
    pytest.param("bo", (-22.87, -9.76, -69.65, -57.45),
                 ["la paz", "santa cruz de la sierra", "cochabamba"],
                 False, None, (10, 14), "neg_neg", id="bo"),
    pytest.param("py", (-27.55, -19.34, -62.69, -54.29),
                 ["asuncion", "ciudad del este"],
                 False, None, (5, 10), "neg_neg", id="py"),
    pytest.param("uy", (-34.95, -30.11, -58.43, -53.21),
                 ["montevideo", "salto"],
                 False, None, (12, 14), "neg_neg", id="uy"),
]


def _fold(s: str) -> str:
    """Lowercase + strip diacritics for robust name matching."""
    nf = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nf if not unicodedata.combining(c)).lower()


def _load_rows(code: str) -> list[dict]:
    path = DATA_DIR / f"{code}_centers.csv"
    assert path.exists(), f"{path} not found"
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


@pytest.mark.parametrize(
    "code,bbox,top_cities,expect_rings,ring_thr,anchor_range,sign_rule",
    PARAMS,
)
class TestLatinAmericaCenters:
    def test_loads_cleanly(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        required = {"name", "state", "lat", "lng", "tier", "rank",
                    "population_basis", "center_type", "anchor_city", "country"}
        for r in _load_rows(code):
            assert required.issubset(r.keys()), f"missing cols in {r}"
            lat, lng = float(r["lat"]), float(r["lng"])
            assert -90 <= lat <= 90, f"bad lat {lat} in {r['name']}"
            assert -180 <= lng <= 180, f"bad lng {lng} in {r['name']}"
            assert r["country"] == code, f"wrong country {r['country']} in {r['name']}"

    def test_anchor_count_in_range(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        rows = _load_rows(code)
        anchors = [r for r in rows if r["center_type"] == "anchor_city"]
        lo, hi = anchor_range
        assert lo <= len(anchors) <= hi, f"{code}: {len(anchors)} anchors not in [{lo},{hi}]"

    def test_ring_rule_honored(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        rows = _load_rows(code)
        for a in [r for r in rows if r["center_type"] == "anchor_city"]:
            pop = int(a["population_basis"].split("_")[-1])
            offsets = [r for r in rows
                       if r["anchor_city"] == a["name"] and r["center_type"] == "offset_ring"]
            expected = 8 if (expect_rings and ring_thr and pop >= ring_thr) else 0
            assert len(offsets) == expected, (
                f"{code} {a['name']} pop={pop}: expected {expected} rings, got {len(offsets)}")

    def test_unique_anchor_pairs(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        rows = _load_rows(code)
        anchors = [r for r in rows if r["center_type"] == "anchor_city"]
        pairs = [(r["name"], r["state"]) for r in anchors]
        assert len(pairs) == len(set(pairs)), f"{code}: duplicate anchor (name,state) pairs"

    def test_coords_in_bbox(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        lat_min, lat_max, lng_min, lng_max = bbox
        bad = []
        for r in _load_rows(code):
            if r["center_type"] != "anchor_city":
                continue  # offset rings may stray slightly past borders by design
            lat, lng = float(r["lat"]), float(r["lng"])
            if not (lat_min <= lat <= lat_max and lng_min <= lng <= lng_max):
                bad.append((r["name"], lat, lng))
        assert not bad, f"{code}: anchors outside bbox: {bad[:5]}"

    def test_coordinate_signs(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        bad = []
        for r in [x for x in _load_rows(code) if x["center_type"] == "anchor_city"]:
            lat, lng = float(r["lat"]), float(r["lng"])
            ok = (
                (sign_rule == "pos_neg" and lat > 0 and lng < 0)
                or (sign_rule == "neg_neg" and lat < 0 and lng < 0)
                or (sign_rule == "mixed")
            )
            if not ok:
                bad.append((r["name"], lat, lng))
        assert not bad, f"{code}: sign-rule '{sign_rule}' violations: {bad[:5]}"

    def test_top_cities_present(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        rows = _load_rows(code)
        anchor_names = {_fold(r["name"]) for r in rows if r["center_type"] == "anchor_city"}
        missing = [c for c in top_cities if c not in anchor_names]
        assert not missing, f"{code}: missing top cities (folded): {missing}\n got: {sorted(anchor_names)}"

    def test_served_by_get_centers_for_job(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        centers, errors = get_centers_for_job(mode="all", country=code)
        assert errors == [], f"{code}: unexpected errors: {errors}"
        assert len(centers) == len(_load_rows(code)), (
            f"{code}: get_centers_for_job returned {len(centers)}, CSV has {len(_load_rows(code))}")

    def test_task_count(self, code, bbox, top_cities, expect_rings, ring_thr, anchor_range, sign_rule):
        centers, _ = get_centers_for_job(mode="all", country=code)
        assert estimate_task_count(centers) == 3 * len(centers)


def test_postal_zip_caches_untouched():
    """Regression guard: the LATAM expansion must not touch postal/zip loaders."""
    assert _ca_postal_code_cache is None
    assert _zip_cache is None
