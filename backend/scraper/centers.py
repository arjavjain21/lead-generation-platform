"""
Centers loader and region filtering.

Loads centers from multiple country CSVs and provides filtering by country,
state (USA), city (USA), or center selection.
"""

from __future__ import annotations

import csv
import difflib
import functools
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).parent / "data"

# Country CSVs: (file, country_code)
COUNTRY_FILES: list[tuple[Path, str]] = [
    (DATA_DIR / "us_centers_842_high_value.csv", "us"),
    (DATA_DIR / "uk_centers.csv", "gb"),
    (DATA_DIR / "ie_centers.csv", "ie"),
    (DATA_DIR / "au_centers.csv", "au"),
    (DATA_DIR / "ca_centers.csv", "ca"),
    # European countries
    (DATA_DIR / "de_centers.csv", "de"),
    (DATA_DIR / "fr_centers.csv", "fr"),
    (DATA_DIR / "es_centers.csv", "es"),
    (DATA_DIR / "it_centers.csv", "it"),
    (DATA_DIR / "nl_centers.csv", "nl"),
    (DATA_DIR / "be_centers.csv", "be"),
    (DATA_DIR / "pl_centers.csv", "pl"),
    (DATA_DIR / "se_centers.csv", "se"),
    (DATA_DIR / "dk_centers.csv", "dk"),
    (DATA_DIR / "at_centers.csv", "at"),
    (DATA_DIR / "ch_centers.csv", "ch"),
    (DATA_DIR / "pt_centers.csv", "pt"),
]

# Country code -> display name
COUNTRY_NAMES: dict[str, str] = {
    "us": "United States",
    "gb": "United Kingdom",
    "ie": "Ireland",
    "au": "Australia",
    "ca": "Canada",
    # European countries
    "de": "Germany",
    "fr": "France",
    "es": "Spain",
    "it": "Italy",
    "nl": "Netherlands",
    "be": "Belgium",
    "pl": "Poland",
    "se": "Sweden",
    "dk": "Denmark",
    "at": "Austria",
    "ch": "Switzerland",
    "pt": "Portugal",
}

# ---------------------------------------------------------------------------
# State alias table (USA only)
# Covers 2-letter codes, common abbreviations, any casing (keys are lowercase)
# ---------------------------------------------------------------------------

STATE_ALIASES: dict[str, str] = {
    "al": "Alabama", "ala": "Alabama", "alabama": "Alabama",
    "ak": "Alaska", "alaska": "Alaska",
    "az": "Arizona", "ariz": "Arizona", "arizona": "Arizona",
    "ar": "Arkansas", "ark": "Arkansas", "arkansas": "Arkansas",
    "ca": "California", "cal": "California", "calif": "California", "california": "California",
    "co": "Colorado", "col": "Colorado", "colo": "Colorado", "colorado": "Colorado",
    "ct": "Connecticut", "conn": "Connecticut", "connecticut": "Connecticut",
    "de": "Delaware", "del": "Delaware", "delaware": "Delaware",
    "dc": "District of Columbia", "d.c.": "District of Columbia",
    "district of columbia": "District of Columbia", "washington dc": "District of Columbia",
    "washington d.c.": "District of Columbia",
    "fl": "Florida", "fla": "Florida", "florida": "Florida",
    "ga": "Georgia", "georgia": "Georgia",
    "hi": "Hawaii", "hawaii": "Hawaii",
    "id": "Idaho", "idaho": "Idaho",
    "il": "Illinois", "ill": "Illinois", "illinois": "Illinois",
    "in": "Indiana", "ind": "Indiana", "indiana": "Indiana",
    "ia": "Iowa", "iowa": "Iowa",
    "ks": "Kansas", "kan": "Kansas", "kans": "Kansas", "kansas": "Kansas",
    "ky": "Kentucky", "ken": "Kentucky", "kent": "Kentucky", "kentucky": "Kentucky",
    "la": "Louisiana", "lou": "Louisiana", "louisiana": "Louisiana",
    "me": "Maine", "maine": "Maine",
    "md": "Maryland", "maryland": "Maryland",
    "ma": "Massachusetts", "mass": "Massachusetts", "massachusetts": "Massachusetts",
    "mi": "Michigan", "mich": "Michigan", "michigan": "Michigan",
    "mn": "Minnesota", "minn": "Minnesota", "minnesota": "Minnesota",
    "ms": "Mississippi", "miss": "Mississippi", "mississippi": "Mississippi",
    "mo": "Missouri", "missouri": "Missouri",
    "mt": "Montana", "mont": "Montana", "montana": "Montana",
    "ne": "Nebraska", "neb": "Nebraska", "nebr": "Nebraska", "nebraska": "Nebraska",
    "nv": "Nevada", "nev": "Nevada", "nevada": "Nevada",
    "nh": "New Hampshire", "new hampshire": "New Hampshire",
    "nj": "New Jersey", "new jersey": "New Jersey",
    "nm": "New Mexico", "n.m.": "New Mexico", "new mexico": "New Mexico",
    "ny": "New York", "n.y.": "New York", "new york": "New York",
    "nc": "North Carolina", "n.c.": "North Carolina", "north carolina": "North Carolina",
    "nd": "North Dakota", "n.d.": "North Dakota", "north dakota": "North Dakota",
    "oh": "Ohio", "ohio": "Ohio",
    "ok": "Oklahoma", "okla": "Oklahoma", "oklahoma": "Oklahoma",
    "or": "Oregon", "ore": "Oregon", "oreg": "Oregon", "oregon": "Oregon",
    "pa": "Pennsylvania", "penn": "Pennsylvania", "penna": "Pennsylvania", "pennsylvania": "Pennsylvania",
    "ri": "Rhode Island", "r.i.": "Rhode Island", "rhode island": "Rhode Island",
    "sc": "South Carolina", "s.c.": "South Carolina", "south carolina": "South Carolina",
    "sd": "South Dakota", "s.d.": "South Dakota", "south dakota": "South Dakota",
    "tn": "Tennessee", "tenn": "Tennessee", "tennessee": "Tennessee",
    "tx": "Texas", "tex": "Texas", "texas": "Texas",
    "ut": "Utah", "utah": "Utah",
    "vt": "Vermont", "vermont": "Vermont",
    "va": "Virginia", "virginia": "Virginia",
    "wa": "Washington", "wash": "Washington", "washington": "Washington",
    "wv": "West Virginia", "w.v.": "West Virginia", "w. va.": "West Virginia",
    "west virginia": "West Virginia", "wva": "West Virginia",
    "wi": "Wisconsin", "wis": "Wisconsin", "wisc": "Wisconsin", "wisconsin": "Wisconsin",
    "wy": "Wyoming", "wyo": "Wyoming", "wyoming": "Wyoming",
}

CANONICAL_STATES = sorted(set(STATE_ALIASES.values()))


def normalize_state(raw: str) -> str | None:
    """Normalize a state input to its canonical name. Returns None if unrecognized."""
    return STATE_ALIASES.get(raw.strip().lower())


def suggest_state(raw: str) -> list[str]:
    """Return up to 3 closest canonical state name matches using difflib."""
    key = raw.strip().lower()
    matches = difflib.get_close_matches(key, STATE_ALIASES.keys(), n=5, cutoff=0.5)
    seen: list[str] = []
    result: list[str] = []
    for m in matches:
        canonical = STATE_ALIASES[m]
        if canonical not in seen:
            seen.append(canonical)
            result.append(canonical)
        if len(result) >= 3:
            break
    return result


# ---------------------------------------------------------------------------
# Centers CSV loader (cached after first load)
# ---------------------------------------------------------------------------

def _parse_center_row(row: dict[str, str], country_code: str) -> dict[str, Any] | None:
    """Parse a CSV row into a center dict. Returns None if invalid."""
    try:
        lat = float(row["lat"])
        lng = float(row["lng"])
    except (KeyError, ValueError):
        return None
    # Use country from CSV if present, otherwise use injected code
    country = row.get("country", "").strip().lower() or country_code
    if len(country) != 2:
        country = country_code
    return {
        "name": row.get("name", "").strip(),
        "state": row.get("state", "").strip(),
        "lat": lat,
        "lng": lng,
        "tier": row.get("tier", "").strip(),
        "center_type": row.get("center_type", "").strip(),
        "anchor_city": row.get("anchor_city", "").strip(),
        "country": country,
    }


@functools.lru_cache(maxsize=1)
def _load_all_centers() -> list[dict[str, Any]]:
    centers: list[dict[str, Any]] = []
    for filepath, country_code in COUNTRY_FILES:
        if not filepath.exists():
            continue
        with open(filepath, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                c = _parse_center_row(row, country_code)
                if c:
                    centers.append(c)
    return centers


def get_all_centers() -> list[dict[str, Any]]:
    return _load_all_centers()


def get_countries() -> list[dict[str, str]]:
    """Return list of {code, name} for all countries with center data."""
    seen: set[str] = set()
    result: list[dict[str, str]] = []
    for c in get_all_centers():
        code = c.get("country", "us")
        if code not in seen:
            seen.add(code)
            result.append({
                "code": code,
                "name": COUNTRY_NAMES.get(code, code.upper()),
            })
    # Ensure consistent order: Americas, Oceania, Europe
    order = ["us", "ca", "au", "gb", "ie", "de", "fr", "es", "it", "nl", "be", "pl", "se", "dk", "at", "ch", "pt"]
    return sorted(result, key=lambda x: (order.index(x["code"]) if x["code"] in order else 99, x["code"]))


def get_centers_for_country(country_code: str) -> list[dict[str, Any]]:
    """Return all centers for a given country (ISO 2-letter code)."""
    code = (country_code or "us").strip().lower()
    return [c for c in get_all_centers() if c.get("country", "us") == code]


def get_anchor_cities() -> list[dict[str, Any]]:
    """Return anchor cities for USA only (used for city search autocomplete)."""
    return [c for c in get_all_centers() if c["center_type"] == "anchor_city" and c.get("country", "us") == "us"]


def search_cities(query: str, country: str = "us") -> list[dict[str, Any]]:
    """Search anchor cities by partial name or state match for the given country."""
    q = query.strip().lower()
    country = (country or "us").strip().lower()

    # Get anchor cities for the specified country
    if country == "us":
        anchors = get_anchor_cities()
    else:
        anchors = [c for c in get_all_centers() if c.get("center_type") == "anchor_city" and c.get("country", "us") == country]

    if not q:
        return anchors[:50]

    exact_prefix: list[dict[str, Any]] = []
    partial: list[dict[str, Any]] = []

    for c in anchors:
        name_lower = c["name"].lower()
        state_lower = c.get("state", "").lower()
        if name_lower.startswith(q):
            exact_prefix.append(c)
        elif q in name_lower or q in state_lower:
            partial.append(c)

    results = exact_prefix + partial
    return results[:20]


# ---------------------------------------------------------------------------
# Main filter: get centers for a job
# ---------------------------------------------------------------------------

def get_centers_for_job(
    mode: str,
    country: str | None = None,
    states: list[str] | None = None,
    cities: list[str] | None = None,
    center_ids: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Returns (centers, errors).
    - country="us" (or unset): mode "all" | "states" | "cities" (USA-only)
    - country in (gb, ie, au, ca): mode "all" = all centers for country; mode "centers" = filter by center_ids
    """
    all_centers = get_all_centers()
    errors: list[str] = []
    country_code = (country or "us").strip().lower()

    # Non-USA countries: use country centers, optionally filtered by center_ids
    if country_code not in ("us", ""):
        if country_code not in COUNTRY_NAMES:
            return [], [f"Unknown country '{country_code}'."]
        country_centers = get_centers_for_country(country_code)
        if not country_centers:
            return [], [f"No centers found for country {COUNTRY_NAMES.get(country_code, country_code)}."]

        if mode == "centers" and center_ids:
            # Filter to selected center names
            id_set = {s.strip() for s in center_ids if s.strip()}
            if not id_set:
                return [], ["No centers selected."]
            name_to_center = {c["name"]: c for c in country_centers}
            matched: list[dict[str, Any]] = []
            for name in id_set:
                if name in name_to_center:
                    matched.append(name_to_center[name])
                else:
                    suggestions = difflib.get_close_matches(name, [c["name"] for c in country_centers], n=3, cutoff=0.5)
                    hint = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
                    errors.append(f"Unknown center '{name}'.{hint}")
            return matched, errors

        # mode "all" or center_ids empty → all centers for country
        return country_centers, []

    # USA: existing behavior
    us_centers = [c for c in all_centers if c.get("country", "us") == "us"]

    if mode == "all":
        return us_centers, []

    if mode == "states":
        if not states:
            return [], ["No states provided."]

        canonical_set: set[str] = set()
        for raw in states:
            canonical = normalize_state(raw)
            if canonical is None:
                suggestions = suggest_state(raw)
                hint = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
                errors.append(f"Unrecognized state '{raw}'.{hint}")
            else:
                canonical_set.add(canonical)

        if not canonical_set:
            return [], errors

        filtered = [c for c in us_centers if c["state"] in canonical_set]
        return filtered, errors

    if mode == "cities":
        if not cities:
            return [], ["No cities provided."]

        matched = []
        seen_names: set[str] = set()

        for raw_city in cities:
            city_centers = _find_centers_for_city(raw_city, us_centers)
            if not city_centers:
                anchors = get_anchor_cities()
                anchor_names = [c["name"] for c in anchors]
                suggestions = difflib.get_close_matches(raw_city, anchor_names, n=3, cutoff=0.4)
                hint = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
                errors.append(f"No centers found for city '{raw_city}'.{hint}")
            else:
                for c in city_centers:
                    key = f"{c['name']}|{c['state']}"
                    if key not in seen_names:
                        seen_names.add(key)
                        matched.append(c)

        return matched, errors

    return [], [f"Unknown mode '{mode}'."]


def _find_centers_for_city(raw_city: str, all_centers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Find all centers (anchor + offset-ring) for a given city name (USA)."""
    q = raw_city.strip().lower()
    anchors = [c for c in all_centers if c["center_type"] == "anchor_city"]

    matching_anchors: list[str] = []
    for anchor in anchors:
        name_lower = anchor["name"].lower()
        if q == name_lower or name_lower.startswith(q) or q in name_lower:
            matching_anchors.append(anchor["name"])

    if not matching_anchors:
        return []

    result: list[dict[str, Any]] = []
    for c in all_centers:
        for anchor_name in matching_anchors:
            if c["anchor_city"] == anchor_name or c["name"] == anchor_name:
                result.append(c)
                break

    return result


def estimate_task_count(centers: list[dict[str, Any]], zooms: list[int] | None = None) -> int:
    """Returns total API call count for a given set of centers × zoom levels."""
    if zooms is None:
        zooms = [10, 11, 12]
    return len(centers) * len(zooms)
