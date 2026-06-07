"""
Enhanced cache signature generation including zoom levels and expected types.
"""
import hashlib
import json
from typing import Any, Optional


def generate_complete_cache_signature(
    query: str,
    regions: dict,
    zooms: list,
    expected_types: Optional[list] = None
) -> str:
    """
    Generate complete cache signature from all search parameters.

    This prevents cache collisions when:
    - Same query with different zoom levels
    - Same query with different expected types filters
    """
    # Normalize query
    normalized_query = query.lower().strip()

    # Normalize regions (sort arrays for consistency)
    normalized_regions = regions.copy()
    for key in ['states', 'cities', 'zips', 'center_ids']:
        if key in normalized_regions and isinstance(normalized_regions[key], list):
            normalized_regions[key] = sorted(normalized_regions[key])

    # Normalize and sort zooms
    normalized_zooms = sorted(zooms)

    # Normalize expected types (sort, lowercase)
    if expected_types:
        normalized_types = sorted([t.lower() for t in expected_types])
    else:
        normalized_types = []

    # Combine all parameters
    combined = json.dumps({
        'q': normalized_query,
        'r': normalized_regions,
        'z': normalized_zooms,
        't': normalized_types
    }, sort_keys=True)

    return hashlib.sha256(combined.encode()).hexdigest()[:32]


def are_zooms_compatible(cached_zooms: list, requested_zooms: list) -> bool:
    """
    Check if zoom levels are compatible for cache reuse.

    Different zoom levels = different search areas = cache miss.
    """
    return sorted(cached_zooms) == sorted(requested_zooms)


def are_expected_types_compatible(cached_types: Optional[list], requested_types: Optional[list]) -> bool:
    """
    Check if expected types are compatible.

    - No cached filter + requested filter = miss (can't filter old results)
    - Cached filter + no requested filter = hit (cached results are subset)
    - Both have filters = must match exactly
    """
    if not cached_types and not requested_types:
        return True
    if cached_types and not requested_types:
        return True  # Cached subset is fine
    if not cached_types and requested_types:
        return False  # Can't filter cached results
    return sorted([t.lower() for t in cached_types]) == sorted([t.lower() for t in requested_types])
