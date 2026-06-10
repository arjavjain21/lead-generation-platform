"""
Cache operations for scraper results.
Provides lookup, storage, and management of cached scraping results.
"""
import hashlib
import json
import logging
import csv
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from shared import db

logger = logging.getLogger(__name__)

CACHE_DIR = db.CACHE_DIR
CACHE_EXPIRY_DAYS = db.CACHE_EXPIRY_DAYS


def generate_region_signature(regions: dict) -> str:
    """Generate deterministic hash for region configuration."""
    # Normalize: sort arrays for consistency
    normalized = regions.copy()
    if "states" in normalized:
        normalized["states"] = sorted(normalized.get("states", []))
    if "cities" in normalized:
        normalized["cities"] = sorted(normalized.get("cities", []))
    if "zips" in normalized:
        normalized["zips"] = sorted(normalized.get("zips", []))

    return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()[:16]


def generate_zoom_signature(zooms: list) -> str:
    """Generate hash for zoom levels."""
    normalized = json.dumps(sorted(zooms), sort_keys=True)
    return hashlib.sha256(normalized.encode()).hexdigest()[:8]


def generate_expected_types_signature(expected_types: Optional[list]) -> str:
    """Generate hash for expected types filter."""
    if not expected_types:
        return "none"
    normalized = json.dumps(sorted([t.lower() for t in expected_types]), sort_keys=True)
    return hashlib.sha256(normalized.encode()).hexdigest()[:8]


def generate_cache_id(query: str, region_sig: str, zoom_sig: str, types_sig: str) -> str:
    """Generate unique cache ID from all parameters."""
    combined = f"{query.lower().strip()}:{region_sig}:{zoom_sig}:{types_sig}"
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def calculate_checksum(file_path: Path) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def count_csv_results(file_path: Path) -> int:
    """Count rows in CSV file (excluding header)."""
    count = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for _ in reader:
            count += 1
    return count


def check_cache(query: str, regions: dict, zooms: list, expected_types: Optional[list] = None) -> Optional[dict]:
    """
    Check if cached results exist for the given parameters.

    Returns:
        Cache entry dict if found, None otherwise
    """
    conn = db.get_db()
    now = datetime.now(timezone.utc)

    # Generate signatures
    region_sig = generate_region_signature(regions)
    zoom_sig = generate_zoom_signature(zooms)
    types_sig = generate_expected_types_signature(expected_types)
    cache_id = generate_cache_id(query, region_sig, zoom_sig, types_sig)

    # Query for active, non-expired cache entry
    row = conn.execute("""
        SELECT * FROM scraped_cache
        WHERE cache_id = ?
        AND status = 'active'
        AND expires_at > ?
    """, (cache_id, now.isoformat())).fetchone()

    if not row:
        # Record cache miss
        _record_cache_stats(False)
        return None

    # Record cache hit and update access tracking
    conn.execute("""
        UPDATE scraped_cache
        SET last_accessed_at = ?, access_count = access_count + 1
        WHERE cache_id = ?
    """, (now.isoformat(), cache_id))
    conn.commit()

    # Record cache hit
    _record_cache_stats(True, dict(row)["total_results"])

    logger.info(f"Cache hit for query '{query}' ({cache_id})")
    return dict(row)


def store_cache(
    job_id: str,
    user_id: str,
    query: str,
    regions: dict,
    zooms: list,
    expected_types: Optional[list],
    result_file_path: Path,
    total_results: int,
    is_partial: bool = False,
    percentage_complete: float = 100.0
) -> str:
    """
    Store job results in cache.

    Returns:
        The cache_id of the stored entry
    """
    conn = db.get_db()
    now = datetime.now(timezone.utc)

    # Generate signatures
    region_sig = generate_region_signature(regions)
    zoom_sig = generate_zoom_signature(zooms)
    types_sig = generate_expected_types_signature(expected_types)
    cache_id = generate_cache_id(query, region_sig, zoom_sig, types_sig)

    # Calculate expiry
    created_at = now.isoformat()
    expires_at = (now + timedelta(days=CACHE_EXPIRY_DAYS)).isoformat()

    # Calculate checksum
    checksum = calculate_checksum(result_file_path)

    # Store or update cache entry
    conn.execute("""
        INSERT OR REPLACE INTO scraped_cache (
            cache_id, query, region_signature, regions,
            zoom_signature, expected_types_signature,
            total_results, result_file_path,
            created_at, updated_at, expires_at, last_accessed_at,
            access_count, is_partial, percentage_complete, checksum, status,
            job_id, user_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        cache_id, query, region_sig, json.dumps(regions),
        zoom_sig, types_sig,
        total_results, str(result_file_path),
        created_at, created_at, expires_at, created_at,
        1, 1 if is_partial else 0, percentage_complete, checksum, 'active',
        job_id, user_id
    ))
    conn.commit()

    # Store per-center counts for subset queries
    _store_center_counts(cache_id, result_file_path)

    logger.info(f"Cache stored: {cache_id} ({total_results} results, expires {expires_at[:10]})")
    return cache_id


def _store_center_counts(cache_id: str, result_file_path: Path) -> None:
    """Parse CSV and store result counts per center for subset queries."""
    # Check if CSV has center_id column
    try:
        with open(result_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if 'center_id' not in reader.fieldnames:
                logger.debug(f"CSV {result_file_path} has no center_id column, skipping center counts")
                return
    except Exception as e:
        logger.warning(f"Failed to read CSV for center counts: {e}")
        return

    conn = db.get_db()
    center_counts = {}

    try:
        with open(result_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                center_id = row.get('center_id', 'unknown')
                if center_id and center_id != 'unknown':
                    center_counts[center_id] = center_counts.get(center_id, 0) + 1
    except Exception as e:
        logger.warning(f"Failed to parse CSV for center counts: {e}")
        return

    # Batch insert
    for center_id, count in center_counts.items():
        # Parse center_id back to name, state
        parts = center_id.replace('_', ' ').split()
        if len(parts) >= 2:
            center_name = ' '.join(parts[:-2])
            center_state = parts[-2]
        else:
            center_name = center_id
            center_state = ''

        try:
            conn.execute("""
                INSERT OR REPLACE INTO cache_center_counts
                (cache_id, center_id, center_name, center_state, result_count)
                VALUES (?, ?, ?, ?, ?)
            """, (cache_id, center_id, center_name, center_state, count))
        except Exception as e:
            logger.warning(f"Failed to insert center count for {center_id}: {e}")

    try:
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to commit center counts: {e}")


def get_cache_file_path(cache_id: str) -> Optional[Path]:
    """Get the file path for a cached result."""
    conn = db.get_db()
    row = conn.execute("""
        SELECT result_file_path FROM scraped_cache
        WHERE cache_id = ? AND status = 'active'
    """, (cache_id,)).fetchone()

    if row:
        return Path(row["result_file_path"])
    return None


def _record_cache_stats(is_hit: bool, results_served: int = 0) -> None:
    """Record cache hit/miss statistics."""
    conn = db.get_db()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if is_hit:
        conn.execute("""
            INSERT OR REPLACE INTO cache_stats (date, cache_hits, results_served)
            VALUES (?, COALESCE((SELECT cache_hits FROM cache_stats WHERE date = ?), 0) + 1,
                COALESCE((SELECT results_served FROM cache_stats WHERE date = ?), 0) + ?)
        """, (today, today, today, results_served))
    else:
        conn.execute("""
            INSERT OR REPLACE INTO cache_stats (date, cache_misses)
            VALUES (?, COALESCE((SELECT cache_misses FROM cache_stats WHERE date = ?), 0) + 1)
        """, (today, today))

    try:
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to record cache stats: {e}")
