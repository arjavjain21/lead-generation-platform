"""Regression tests for scraper.cache region handling.

Guards against the bug where ``regions`` arrives at the cache layer as a
JSON *string* (read back from the jobs DB) instead of a dict. Before the
fix, ``generate_region_signature`` called ``.copy()`` on the string and
raised ``AttributeError``, which silently killed every ``store_cache``
write.
"""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.cache import (  # noqa: E402
    generate_region_signature,
    _normalize_regions,
)


SAMPLE_REGIONS = {
    "states": ["CA", "TX", "NY"],
    "cities": ["San Francisco", "Austin"],
    "zips": ["94110", "10001"],
}


def test_str_and_dict_forms_produce_identical_signatures():
    """Regression guard: JSON-string form must hash identically to dict form."""
    dict_sig = generate_region_signature(SAMPLE_REGIONS)
    str_sig = generate_region_signature(json.dumps(SAMPLE_REGIONS))
    assert dict_sig == str_sig


def test_dict_form_signature_is_stable():
    """Same dict in must give same signature across calls (byte stability)."""
    assert generate_region_signature(SAMPLE_REGIONS) == generate_region_signature(SAMPLE_REGIONS)


def test_unsorted_lists_normalized_identically():
    """List order inside the dict/JSON must not change the signature."""
    shuffled = {
        "states": ["NY", "CA", "TX"],
        "cities": ["Austin", "San Francisco"],
        "zips": ["10001", "94110"],
    }
    assert generate_region_signature(shuffled) == generate_region_signature(SAMPLE_REGIONS)
    assert generate_region_signature(json.dumps(shuffled)) == generate_region_signature(
        json.dumps(SAMPLE_REGIONS)
    )


@pytest.mark.parametrize("empty", [None, "", "{}", "  ", "[]"])
def test_does_not_crash_on_empty_or_degenerate_input(empty):
    """None / empty-string / empty-object inputs must not raise."""
    sig = generate_region_signature(empty)
    assert isinstance(sig, str)
    assert len(sig) == 16


def test_empty_inputs_share_a_stable_signature():
    """Equivalent empty forms should collapse to one stable signature."""
    assert generate_region_signature(None) == generate_region_signature({})
    assert generate_region_signature("") == generate_region_signature("{}")


def test_normalize_regions_passthrough_dict():
    """A dict passes through _normalize_regions unchanged (same object)."""
    d = {"states": ["CA"]}
    assert _normalize_regions(d) is d


def test_normalize_regions_parses_json_string():
    assert _normalize_regions(json.dumps({"states": ["CA"]})) == {"states": ["CA"]}


def test_normalize_regions_handles_garbage_string():
    """A non-JSON string degrades to {} rather than raising."""
    assert _normalize_regions("not-json") == {}
