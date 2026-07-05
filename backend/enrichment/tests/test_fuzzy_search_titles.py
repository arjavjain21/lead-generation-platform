import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_dir))

from enrichment.routes import _titles_to_cascade

def test_titles_to_cascade_enables_fuzzy_search():
    """Custom titles should enable include_headline_search."""
    result = _titles_to_cascade("dentist,orthodontist")
    assert len(result) == 1
    assert result[0]["include_headline_search"] == True, f"Expected include_headline_search=True, got {result[0].get('include_headline_search')}"
    assert result[0]["include_title"] == ["dentist", "orthodontist"]
    assert "assistant" in result[0]["exclude_title"]
    print("✓ test_titles_to_cascade_enables_fuzzy_search passed")

def test_empty_titles_returns_empty_list():
    """Empty titles should return empty list."""
    assert _titles_to_cascade("") == []
    assert _titles_to_cascade(None) == []
    print("✓ test_empty_titles_returns_empty_list passed")

def test_titles_with_spaces():
    """Titles should be trimmed."""
    result = _titles_to_cascade(" dentist , orthodontist ")
    assert result[0]["include_title"] == ["dentist", "orthodontist"]
    print("✓ test_titles_with_spaces passed")

if __name__ == "__main__":
    test_titles_to_cascade_enables_fuzzy_search()
    test_empty_titles_returns_empty_list()
    test_titles_with_spaces()
    print("\n✅ All tests passed!")
