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
    (DATA_DIR / "no_centers.csv", "no"),
    (DATA_DIR / "nz_centers.csv", "nz"),
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
    "no": "Norway",
    "nz": "New Zealand",
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
    order = ["us", "ca", "au", "nz", "gb", "ie", "de", "fr", "es", "it", "nl", "be", "pl", "se", "no", "dk", "at", "ch", "pt"]
    return sorted(result, key=lambda x: (order.index(x["code"]) if x["code"] in order else 99, x["code"]))


def get_centers_for_country(country_code: str) -> list[dict[str, Any]]:
    """Return all centers for a given country (ISO 2-letter code)."""
    code = (country_code or "us").strip().lower()
    return [c for c in get_all_centers() if c.get("country", "us") == code]


def get_anchor_cities() -> list[dict[str, Any]]:
    """Return anchor cities for USA only (used for city search autocomplete)."""
    return [c for c in get_all_centers() if c["center_type"] == "anchor_city" and c.get("country", "us") == "us"]


def search_cities(query: str, country: str = "us") -> list[dict[str, Any]]:
    """Search cities by partial name or state match for the given country.

    For US: searches all 29,546 cities from zip code database.
    For other countries: searches anchor cities only.
    """
    q = query.strip().lower()
    country = (country or "us").strip().lower()

    # For US, use the full city database from zip codes
    if country == "us":
        all_cities = get_all_us_cities()
        # Add lat/lng for search compatibility (will be populated on demand)
        return _search_cities_in_list(all_cities, q)
    else:
        # For other countries, use anchor cities
        anchors = [c for c in get_all_centers() if c.get("center_type") == "anchor_city" and c.get("country", "us") == country]
        return _search_cities_in_list(anchors, q)


def _search_cities_in_list(cities: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    """Helper function to search within a list of cities."""
    if not query:
        return cities[:50]

    exact_prefix: list[dict[str, Any]] = []
    partial: list[dict[str, Any]] = []

    for c in cities:
        name_lower = c["name"].lower()
        state_lower = c.get("state", "").lower()
        if name_lower.startswith(query):
            exact_prefix.append(c)
        elif query in name_lower or query in state_lower:
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
    zips: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Returns (centers, errors).
    - country="us" (or unset): mode "all" | "states" | "cities" | "zips" (USA-only)
    - country in (gb, ie, au, ca): mode "all" = all centers for country; mode "centers" = filter by center_ids

    Args:
        zips: List of US zip codes (for mode="zips")
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

    # Handle zip/postal codes mode
    if mode == "zips":
        if not zips:
            return [], ["No zip/postal codes provided."]

        # Route to appropriate handler based on country
        if country_code == "gb":
            centers, zip_errors = get_centers_for_uk_postcodes(zips)
            return centers, zip_errors
        elif country_code == "ca":
            centers, zip_errors = get_centers_for_ca_postal_codes(zips)
            return centers, zip_errors
        else:
            # US zip codes (default)
            centers, zip_errors = get_centers_for_zips(zips)
            return centers, zip_errors

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
            # Skip empty strings
            if not raw_city or not raw_city.strip():
                continue

            # First try to find anchor city centers
            city_centers = _find_centers_for_city(raw_city, us_centers)

            # If no anchor centers found, try dynamic centers from zip database
            if not city_centers:
                city_centers = get_centers_for_city_name(raw_city)

            if not city_centers:
                # Still no centers - suggest alternatives
                anchors = get_anchor_cities()
                anchor_names = [c["name"] for c in anchors]
                suggestions = difflib.get_close_matches(raw_city, anchor_names, n=3, cutoff=0.4)
                hint = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
                errors.append(f"No centers found for city '{raw_city}'.{hint}")
            else:
                # Add all centers, but deduplicate anchor city centers
                # Dynamic centers (city_dynamic type) can have duplicates
                for c in city_centers:
                    key = f"{c['name']}|{c['state']}"
                    # For dynamic centers, allow multiple entries for the same city
                    # For anchor centers, deduplicate by name|state
                    if c.get('center_type') == 'city_dynamic':
                        matched.append(c)
                    elif key not in seen_names:
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


# ---------------------------------------------------------------------------
# Job display name generation
# ---------------------------------------------------------------------------

def _to_snake_case(text: str) -> str:
    """Convert text to snake_case: lowercase, replace spaces with underscores."""
    if not text:
        return ""
    return text.lower().strip().replace(" ", "_").replace("-", "_")


def generate_job_display_name(
    query: str,
    country_code: str,
    mode: str,
    states: list[str] | None = None,
    cities: list[str] | None = None,
    zips: list[str] | None = None,
) -> str:
    """
    Generate a formatted display name for a scraper job.

    Format: {query}-{country}({scope})
    Examples:
        - internet_marketing-united_states(all)
        - lawyers-california(2_states)
        - restaurants-new_york(5_cities)

    Args:
        query: Original search query
        country_code: 2-letter country code (us, gb, ie, etc.)
        mode: "all" | "states" | "cities"
        states: List of selected state names (for mode="states")
        cities: List of selected city names (for mode="cities")

    Returns:
        Formatted job display name in snake_case
    """
    # Convert query to snake_case
    query_snake = _to_snake_case(query)

    # Get country name and convert to snake_case
    country_name = COUNTRY_NAMES.get(country_code.lower(), country_code)
    country_snake = _to_snake_case(country_name)

    # Determine scope suffix
    if mode == "all":
        scope_suffix = "(all)"
    elif mode == "states" and states:
        count = len(states)
        scope_suffix = f"({count}_states)"
    elif mode == "cities" and cities:
        count = len(cities)
        scope_suffix = f"({count}_cities)"
    elif mode == "zips" and zips:
        count = len(zips)
        scope_suffix = f"({count}_zips)"
    else:
        scope_suffix = "(all)"

    return f"{query_snake}-{country_snake}{scope_suffix}"


# ---------------------------------------------------------------------------
# Zip code geocoding
# ---------------------------------------------------------------------------

# Lazy-loaded zip code database
_zip_cache: dict[str, dict[str, Any]] | None = None


def _load_zip_database() -> dict[str, dict[str, Any]]:
    """Load US zip code database lazily. Returns dict: zip_code -> {lat, lng, city, state}."""
    global _zip_cache
    if _zip_cache is not None:
        return _zip_cache

    _zip_cache = {}
    zip_file = DATA_DIR / "us_zips.csv"

    if zip_file.exists():
        import csv
        with open(zip_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                zip_code = row["zip"].strip()
                if zip_code:
                    _zip_cache[zip_code] = {
                        "lat": float(row["lat"]),
                        "lng": float(row["lng"]),
                        "city": row.get("city", ""),
                        "state": row.get("state", ""),
                    }

    return _zip_cache


def get_centers_for_zips(zips: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Convert a list of US zip codes to center dicts.

    Returns (centers, errors):
    - centers: List of center dicts with lat/lng for valid zip codes
    - errors: List of error messages for invalid/unknown zip codes

    Args:
        zips: List of 5-digit US zip codes

    Returns:
        Tuple of (valid_centers, error_messages)
    """
    errors: list[str] = []
    centers: list[dict[str, Any]] = []
    seen_zips: set[str] = set()  # Deduplicate

    # Validate zip format (5 digits)
    for raw_zip in zips:
        zip_code = raw_zip.strip()
        if not zip_code:
            continue

        # Validate format: must be 5 digits
        if not zip_code.isdigit() or len(zip_code) != 5:
            errors.append(f"Invalid zip code '{zip_code}': must be exactly 5 digits")
            continue

        # Deduplicate
        if zip_code in seen_zips:
            continue
        seen_zips.add(zip_code)

        # Look up in database
        zip_db = _load_zip_database()
        if zip_code in zip_db:
            data = zip_db[zip_code]
            centers.append({
                "name": zip_code,
                "state": data["state"],
                "lat": data["lat"],
                "lng": data["lng"],
                "tier": "zip",
                "center_type": "zip_code",
                "anchor_city": data["city"],
                "country": "us",
            })
        else:
            errors.append(f"Zip code '{zip_code}' not found in database")

    return centers, errors


def get_zip_coordinates(zip_code: str) -> tuple[float, float] | None:
    """
    Get lat/lng coordinates for a single US zip code.

    Args:
        zip_code: 5-digit US zip code

    Returns:
        Tuple of (lat, lng) or None if not found
    """
    zip_db = _load_zip_database()
    if zip_code in zip_db:
        data = zip_db[zip_code]
        return data["lat"], data["lng"]
    return None


def validate_zip_codes(zips: list[str]) -> tuple[list[str], list[str]]:
    """
    Validate a list of zip codes, returning valid and invalid codes.

    Args:
        zips: List of zip codes (may include duplicates)

    Returns:
        Tuple of (valid_zips, invalid_zips_with_errors)
    """
    valid: list[str] = []
    invalid: list[str] = []

    for raw_zip in zips:
        zip_code = raw_zip.strip()
        if not zip_code:
            continue

        # Validate format
        if not zip_code.isdigit() or len(zip_code) != 5:
            invalid.append(f"'{zip_code}': must be 5 digits")
            continue

        # Check if in database
        zip_db = _load_zip_database()
        if zip_code in zip_db:
            valid.append(zip_code)
        else:
            invalid.append(f"'{zip_code}': not found")

    return valid, invalid


def parse_zips_from_string(input_str: str) -> list[str]:
    """
    Parse zip codes from a string that can contain both comma-separated
    and line-separated zip codes.

    Args:
        input_str: String like "90210\n10001, 10002\n90211"

    Returns:
        List of cleaned 5-digit zip codes (deduplicated)
    """
    if not input_str:
        return []

    # Split by both commas and newlines
    parts = input_str.replace(",", "\n").split("\n")
    zips = []

    for part in parts:
        # Strip whitespace
        zip_code = part.strip()
        # Only include 5-digit codes
        if zip_code.isdigit() and len(zip_code) == 5:
            zips.append(zip_code)

    # Return deduplicated list while preserving order
    seen = set()
    result = []
    for z in zips:
        if z not in seen:
            seen.add(z)
            result.append(z)

    return result


# ---------------------------------------------------------------------------
# UK Postcode database (lazy-loaded)
# ---------------------------------------------------------------------------

_uk_postcode_cache: dict[str, dict[str, Any]] | None = None


def _load_uk_postcode_database() -> dict[str, dict[str, Any]]:
    """Load UK postcode database lazily. Returns dict: postcode -> {lat, lng, city, region}."""
    global _uk_postcode_cache
    if _uk_postcode_cache is not None:
        return _uk_postcode_cache

    _uk_postcode_cache = {}
    postcode_file = DATA_DIR / "uk_postcodes.csv"

    if postcode_file.exists():
        import csv
        with open(postcode_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                postcode = row["postcode"].strip().upper()
                if postcode:
                    _uk_postcode_cache[postcode] = {
                        "lat": float(row["lat"]),
                        "lng": float(row["lng"]),
                        "city": row.get("city", ""),
                        "region": row.get("region", ""),
                    }

    return _uk_postcode_cache


def get_centers_for_uk_postcodes(postcodes: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Convert a list of UK postcodes to center dicts.

    Returns (centers, errors):
    - centers: List of center dicts with lat/lng for valid postcodes
    - errors: List of error messages for invalid/unknown postcodes

    Args:
        postcodes: List of UK postcodes (format: AA9A 9AA or A9A 9AA)

    Returns:
        Tuple of (valid_centers, error_messages)
    """
    errors: list[str] = []
    centers: list[dict[str, Any]] = []
    seen_postcodes: set[str] = set()

    # UK postcode regex (accepts both outward codes and full postcodes)
    # Outward code: A(A)N(N)(A) (e.g., SW1A, M9, S5)
    # Full postcode: A(A)N(N)(A) NAA (e.g., SW1A 1AA, M9 4AA)
    import re
    uk_postcode_pattern = re.compile(r'^[A-Z]{1,2}[0-9][A-Z0-9]?( ?[0-9][A-Z]{2})?$')

    for raw_postcode in postcodes:
        postcode = raw_postcode.strip().upper()
        if not postcode:
            continue

        # Validate format
        if not uk_postcode_pattern.match(postcode):
            errors.append(f"Invalid UK postcode '{raw_postcode}': format should be like 'SW1A 1AA' or 'M1 1AA'")
            continue

        # Normalize: ensure space between outward and inward codes
        if " " not in postcode and len(postcode) >= 5:
            postcode = postcode[:-3] + " " + postcode[-3:]

        # Deduplicate
        if postcode in seen_postcodes:
            continue
        seen_postcodes.add(postcode)

        # Look up in database
        postcode_db = _load_uk_postcode_database()
        if postcode in postcode_db:
            data = postcode_db[postcode]
            centers.append({
                "name": postcode,
                "state": data["region"] or "Unknown",
                "lat": data["lat"],
                "lng": data["lng"],
                "tier": "postcode",
                "center_type": "uk_postcode",
                "anchor_city": data["city"] or "Unknown",
                "country": "gb",
            })
        else:
            errors.append(f"UK postcode '{postcode}' not found in database")

    return centers, errors


def validate_uk_postcodes(postcodes: list[str]) -> tuple[list[str], list[str]]:
    """
    Validate a list of UK postcodes, returning valid and invalid codes.

    Args:
        postcodes: List of UK postcodes (may include duplicates)

    Returns:
        Tuple of (valid_postcodes, invalid_postcodes_with_errors)
    """
    valid: list[str] = []
    invalid: list[str] = []
    import re
    # UK postcode regex (accepts both outward codes and full postcodes)
    uk_postcode_pattern = re.compile(r'^[A-Z]{1,2}[0-9][A-Z0-9]?( ?[0-9][A-Z]{2})?$')

    for raw_postcode in postcodes:
        postcode = raw_postcode.strip().upper()
        if not postcode:
            continue

        # Validate format
        if not uk_postcode_pattern.match(postcode):
            invalid.append(f"'{raw_postcode}': invalid format")
            continue

        # For full postcodes with inward code, normalize to ensure space
        # For outward codes only, keep as is
        if " " in postcode:
            # Full postcode - ensure it has a space
            parts = postcode.split()
            if len(parts) == 2:
                normalized = parts[0] + " " + parts[1]
            else:
                normalized = postcode
        else:
            # Outward code only - keep as is
            normalized = postcode

        # Check if in database
        postcode_db = _load_uk_postcode_database()
        if normalized in postcode_db:
            valid.append(normalized)
        else:
            invalid.append(f"'{raw_postcode}': not found in database")

    return valid, invalid


def parse_uk_postcodes_from_string(input_str: str) -> list[str]:
    """
    Parse UK postcodes from a string that can contain both comma-separated
    and line-separated postcodes.

    Args:
        input_str: String like "SW1A 1AA\nEH1 1PT, M1 1AA"

    Returns:
        List of cleaned UK postcodes (deduplicated, normalized)
    """
    if not input_str:
        return []

    # Split by both commas and newlines
    parts = input_str.replace(",", "\n").split("\n")
    postcodes = []
    import re
    uk_postcode_pattern = re.compile(r'^[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}$')

    for part in parts:
        # Strip whitespace and uppercase
        postcode = part.strip().upper()
        # Validate format
        if postcode and uk_postcode_pattern.match(postcode):
            # Normalize: ensure space
            if " " not in postcode and len(postcode) >= 5:
                postcode = postcode[:-3] + " " + postcode[-3:]
            postcodes.append(postcode)

    # Return deduplicated list while preserving order
    seen = set()
    result = []
    for pc in postcodes:
        if pc not in seen:
            seen.add(pc)
            result.append(pc)

    return result


# ---------------------------------------------------------------------------
# Canada Postal Code database (lazy-loaded)
# ---------------------------------------------------------------------------

_ca_postal_code_cache: dict[str, dict[str, Any]] | None = None


def _load_ca_postal_code_database() -> dict[str, dict[str, Any]]:
    """Load Canada postal code database lazily. Returns dict: postal_code -> {lat, lng, city, province}."""
    global _ca_postal_code_cache
    if _ca_postal_code_cache is not None:
        return _ca_postal_code_cache

    _ca_postal_code_cache = {}
    postal_code_file = DATA_DIR / "ca_postcodes.csv"

    if postal_code_file.exists():
        import csv
        with open(postal_code_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                postal_code = row["postal_code"].strip().upper()
                if postal_code:
                    _ca_postal_code_cache[postal_code] = {
                        "lat": float(row["lat"]),
                        "lng": float(row["lng"]),
                        "city": row.get("city", ""),
                        "province": row.get("province", ""),
                    }

    return _ca_postal_code_cache


def get_centers_for_ca_postal_codes(postal_codes: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Convert a list of Canada postal codes/FSAs to center dicts.

    Returns (centers, errors):
    - centers: List of center dicts with lat/lng for valid postal codes
    - errors: List of error messages for invalid/unknown postal codes

    Args:
        postal_codes: List of Canada postal codes (format: ANA or ANA NAN)

    Returns:
        Tuple of (valid_centers, error_messages)
    """
    errors: list[str] = []
    centers: list[dict[str, Any]] = []
    seen_postal_codes: set[str] = set()

    # Canada postal code regex (accepts both FSA and full postal code)
    import re
    ca_postal_code_pattern = re.compile(r'^[A-Z][0-9][A-Z]( ?[0-9][A-Z][0-9])?$')

    for raw_postal_code in postal_codes:
        postal_code = raw_postal_code.strip().upper()
        if not postal_code:
            continue

        # Validate format
        if not ca_postal_code_pattern.match(postal_code):
            errors.append(f"Invalid Canada postal code '{raw_postal_code}': format should be like 'K1A' or 'K1A 0A1'")
            continue

        # For full postal codes with inward code, normalize to ensure space
        # For FSAs only, keep as is
        if " " in postal_code:
            # Full postal code - ensure it has a space
            parts = postal_code.split()
            if len(parts) == 2:
                normalized = parts[0] + " " + parts[1]
            else:
                normalized = postal_code
        else:
            # FSA only - keep as is
            normalized = postal_code

        # Deduplicate
        if normalized in seen_postal_codes:
            continue
        seen_postal_codes.add(normalized)

        # Look up in database
        postal_code_db = _load_ca_postal_code_database()
        if normalized in postal_code_db:
            data = postal_code_db[normalized]
            centers.append({
                "name": normalized,
                "state": data["province"] or "Unknown",
                "lat": data["lat"],
                "lng": data["lng"],
                "tier": "postal_code",
                "center_type": "ca_postal_code",
                "anchor_city": data["city"] or "Unknown",
                "country": "ca",
            })
        else:
            errors.append(f"Canada postal code '{normalized}' not found in database")

    return centers, errors


def validate_ca_postal_codes(postal_codes: list[str]) -> tuple[list[str], list[str]]:
    """
    Validate a list of Canada postal codes, returning valid and invalid codes.

    Args:
        postal_codes: List of Canada postal codes (may include duplicates)

    Returns:
        Tuple of (valid_postal_codes, invalid_postal_codes_with_errors)
    """
    valid: list[str] = []
    invalid: list[str] = []
    import re
    # Canada postal code regex (accepts both FSA and full postal code)
    ca_postal_code_pattern = re.compile(r'^[A-Z][0-9][A-Z]( ?[0-9][A-Z][0-9])?$')

    for raw_postal_code in postal_codes:
        postal_code = raw_postal_code.strip().upper()
        if not postal_code:
            continue

        # Validate format
        if not ca_postal_code_pattern.match(postal_code):
            invalid.append(f"'{raw_postal_code}': invalid format")
            continue

        # Normalize for database lookup
        normalized = postal_code
        if " " in normalized:
            # Full postal code with space - keep as is
            pass
        else:
            # Could be FSA (3 chars) or full postal code without space
            if len(normalized) == 3:
                # FSA only - keep as is
                pass
            elif len(normalized) == 6:
                # Full postal code without space - add space
                normalized = normalized[:3] + " " + normalized[3:]

        # Check if in database
        postal_code_db = _load_ca_postal_code_database()
        if normalized in postal_code_db:
            valid.append(normalized)
        else:
            invalid.append(f"'{raw_postal_code}': not found in database")

    return valid, invalid


def parse_ca_postal_codes_from_string(input_str: str) -> list[str]:
    """
    Parse Canada postal codes from a string that can contain both comma-separated
    and line-separated postal codes.

    Args:
        input_str: String like "K1A 0A1\nM5H 2N2, V6B 2W6"

    Returns:
        List of cleaned Canada postal codes (deduplicated, normalized)
    """
    if not input_str:
        return []

    # Split by both commas and newlines
    parts = input_str.replace(",", "\n").split("\n")
    postal_codes = []
    import re
    ca_postal_code_pattern = re.compile(r'^[A-Z][0-9][A-Z] ?[0-9][A-Z][0-9]$')

    for part in parts:
        # Strip whitespace and uppercase
        postal_code = part.strip().upper()
        # Validate format
        if postal_code and ca_postal_code_pattern.match(postal_code):
            # Normalize: ensure space
            if " " not in postal_code:
                postal_code = postal_code[:3] + " " + postal_code[3:]
            postal_codes.append(postal_code)

    # Return deduplicated list while preserving order
    seen = set()
    result = []
    for pc in postal_codes:
        if pc not in seen:
            seen.add(pc)
            result.append(pc)

    return result


# ---------------------------------------------------------------------------
# City-to-Zip/Postal Code Mapping
# ---------------------------------------------------------------------------

def get_zips_for_cities(cities: list[str]) -> tuple[dict[str, list[str]], list[str]]:
    """
    Get all zip codes for a list of cities (US only).

    Args:
        cities: List of city names (can include state like "Charleston, SC")

    Returns:
        Tuple of (city_zips_map, errors)
        - city_zips_map: Dict mapping {city_state: [zip_codes]}
        - errors: List of error messages for cities not found
    """
    import csv

    zip_db = _load_zip_database()
    city_zips_map: dict[str, list[str]] = {}
    errors: list[str] = []

    for city_input in cities:
        city_input = city_input.strip()
        if not city_input:
            continue

        # Parse city input - could be "Charleston" or "Charleston, SC"
        parts = [p.strip() for p in city_input.split(',')]
        city_name = parts[0].lower()
        state = parts[1].upper() if len(parts) > 1 else None

        # Find matching zips
        matched_zips = []
        for zip_code, data in zip_db.items():
            zip_city = data['city'].lower()
            zip_state = data['state']

            # Check if city matches (and state if provided)
            if zip_city == city_name:
                if state is None or zip_state == state:
                    matched_zips.append(zip_code)

        # Use the city_state format for the key
        if state:
            # Use the first matched zip to get the proper city name
            first_matched_city = None
            for zip_code, data in zip_db.items():
                if zip_code in matched_zips:
                    first_matched_city = data['city']
                    break
            city_key = f"{first_matched_city or city_input}, {state}"
        else:
            city_key = city_input

        if matched_zips:
            if city_key not in city_zips_map:
                city_zips_map[city_key] = []
            city_zips_map[city_key].extend(matched_zips)
            # Deduplicate
            city_zips_map[city_key] = list(set(city_zips_map[city_key]))
            # Sort for consistency
            city_zips_map[city_key].sort()
        else:
            errors.append(f"City '{city_input}' not found in database")

    return city_zips_map, errors


def get_all_us_cities() -> list[dict[str, Any]]:
    """
    Get all US cities from the zip code database.

    Returns:
        List of city dicts with name, state, zip_count, lat, lng
    """
    import csv

    zip_file = DATA_DIR / "us_zips.csv"
    cities_dict: dict[str, dict[str, Any]] = {}

    with open(zip_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            city = row['city'].strip()
            state = row['state'].strip()
            city_state = f"{city}, {state}"

            if city_state not in cities_dict:
                cities_dict[city_state] = {
                    "name": city,
                    "state": state,
                    "zip_codes": [],
                    "zip_count": 0,
                    "lat": float(row['lat']),
                    "lng": float(row['lng'])
                }
            cities_dict[city_state]["zip_codes"].append(row['zip'])

    # Convert to list and count
    cities_list = []
    for city_state, data in cities_dict.items():
        data["zip_count"] = len(data["zip_codes"])
        # Keep only zip_count, don't include all zips in response
        del data["zip_codes"]
        cities_list.append(data)

    # Sort by zip_count (most zips first) then by name
    cities_list.sort(key=lambda x: (-x["zip_count"], x["name"]))

    return cities_list


def get_centers_for_city_name(raw_city: str) -> list[dict[str, Any]]:
    """
    Create center objects from a city name using the zip code database.

    This bridges the gap between the expanded city search (29,546 cities)
    and the scraper's center-based system (142 anchor cities).

    Args:
        raw_city: City name, optionally with state (e.g., "Charleston" or "Charleston, WV")

    Returns:
        List of center objects that can be used by the scraper
    """
    import csv
    from collections import defaultdict

    # Parse city and state from input
    parts = [p.strip() for p in raw_city.split(',')]
    city_input = parts[0]
    state_input = parts[1] if len(parts) > 1 else None

    # Read zip database and group by state
    zip_file = DATA_DIR / "us_zips.csv"
    cities_by_state = defaultdict(list)  # state -> list of (zip, lat, lng)

    with open(zip_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            zip_city = row['city'].strip()
            zip_state = row['state'].strip()

            # Match by city (and state if provided)
            if zip_city.lower() == city_input.lower():
                if state_input is None or zip_state.lower() == state_input.lower():
                    cities_by_state[zip_state].append({
                        'zip': row['zip'],
                        'lat': float(row['lat']),
                        'lng': float(row['lng'])
                    })

    if not cities_by_state:
        return []

    # If multiple states match, pick the one with most zips (largest city)
    if state_input is None and len(cities_by_state) > 1:
        # Find state with most zips
        target_state = max(cities_by_state.keys(), key=lambda s: len(cities_by_state[s]))
        zips_data = cities_by_state[target_state]
        city_name = city_input
        state_name = target_state
    elif state_input:
        # State was specified, use that state's data
        target_state = state_input
        zips_data = cities_by_state.get(state_input, [])
        city_name = city_input
        state_name = state_input
    else:
        # Only one state matched
        target_state = list(cities_by_state.keys())[0]
        zips_data = cities_by_state[target_state]
        city_name = city_input
        state_name = target_state

    if not zips_data:
        return []

    # Calculate average coordinates
    num_zips = len(zips_data)
    avg_lat = sum(z['lat'] for z in zips_data) / num_zips
    avg_lng = sum(z['lng'] for z in zips_data) / num_zips

    # Create centers based on city size
    max_centers = min(10, num_zips)
    centers = []

    if num_zips <= 5:
        # Small city: one center with zoom 12
        centers.append({
            "name": city_name,
            "state": state_name,
            "country": "us",
            "lat": avg_lat,
            "lng": avg_lng,
            "zoom": 12,
            "center_type": "city_dynamic",
            "anchor_city": city_name,
            "zip_count": num_zips
        })
    else:
        # Large city: multiple centers at zoom 11
        step = max(1, num_zips // max_centers)
        for i in range(0, num_zips, step):
            if len(centers) >= max_centers:
                break
            centers.append({
                "name": city_name,
                "state": state_name,
                "country": "us",
                "lat": avg_lat,
                "lng": avg_lng,
                "zoom": 11,
                "center_type": "city_dynamic",
                "anchor_city": city_name,
                "zip_count": num_zips
            })

    return centers


