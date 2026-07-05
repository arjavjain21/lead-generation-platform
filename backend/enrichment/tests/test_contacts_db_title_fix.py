"""
Test that Contacts DB title field is preserved during enrichment.

This test verifies the fix for the bug where Contacts DB titles were
being lost during conversion to Blitz-compatible format.
"""

import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(backend_dir))

from enrichment.pipeline import _current_title
from enrichment.list_builder import _current_title as list_builder_current_title


def test_current_title_with_experiences():
    """Test _current_title with Blitz-style experiences array."""
    experiences = [
        {"job_title": "CEO", "job_is_current": True},
        {"job_title": "VP", "job_is_current": False},
    ]
    result = _current_title(experiences)
    assert result == "CEO", f"Expected 'CEO', got '{result}'"
    print("✓ _current_title with experiences works")


def test_current_title_with_empty_experiences():
    """Test _current_title with empty experiences (old Contacts DB behavior)."""
    experiences = []
    result = _current_title(experiences, "Dentist")
    assert result == "Dentist", f"Expected 'Dentist', got '{result}'"
    print("✓ _current_title with empty experiences + direct_title works")


def test_current_title_without_direct_title():
    """Test _current_title without direct title (should return empty)."""
    experiences = []
    result = _current_title(experiences, "")
    assert result == "", f"Expected '', got '{result}'"
    print("✓ _current_title without direct_title returns empty")


def test_current_title_prioritizes_experiences():
    """Test that experiences array takes priority over direct_title."""
    experiences = [
        {"job_title": "Orthodontist", "job_is_current": True},
    ]
    # Even though direct_title is "Dentist", experiences should win
    result = _current_title(experiences, "Dentist")
    assert result == "Orthodontist", f"Expected 'Orthodontist', got '{result}'"
    print("✓ _current_title prioritizes experiences over direct_title")


def test_list_builder_current_title():
    """Test list_builder's _current_title (should be identical)."""
    experiences = []
    result = list_builder_current_title(experiences, "Pediatric Dentist")
    assert result == "Pediatric Dentist", f"Expected 'Pediatric Dentist', got '{result}'"
    print("✓ list_builder _current_title works")


def test_contacts_db_conversion_pattern():
    """
    Test the exact pattern used in Contacts DB → Blitz conversion.

    This simulates what happens when Contacts DB data is converted
    to Blitz-compatible format in the enrichment pipeline.
    """
    # Simulated Contacts DB response
    contacts_db_contact = {
        "full_name": "Dr. Sarah Smith",
        "title": "Orthodontist",  # ← This field was being lost
        "email": "sarah@dental.com",
        "linkedin_url": "https://linkedin.com/in/sarahsmith",
        "city": "New York",
        "country_code": "US",
    }

    # Simulated conversion (what the fixed code does)
    person_dict = {
        "person": {
            "title": contacts_db_contact.get("title", ""),  # ← FIX: Preserve title
            "first_name": contacts_db_contact.get("first_name", ""),
            "last_name": contacts_db_contact.get("last_name", ""),
            "full_name": contacts_db_contact.get("full_name", ""),
            "headline": contacts_db_contact.get("headline", ""),
            "linkedin_url": contacts_db_contact.get("linkedin_url", ""),
            "location": {
                "city": contacts_db_contact.get("city", ""),
                "country_code": contacts_db_contact.get("country_code", ""),
            },
            "experiences": [],  # Empty - Contacts DB doesn't provide this
        },
        "icp": 0,
    }

    # Extract title using the fixed _current_title function
    person = person_dict["person"]
    result_title = _current_title(
        person.get("experiences", []),
        person.get("title", "")
    )

    assert result_title == "Orthodontist", f"Expected 'Orthodontist', got '{result_title}'"
    print("✓ Contacts DB conversion pattern preserves title")


def test_dental_titles_examples():
    """Test various dental title examples."""
    dental_titles = [
        "Dentist",
        "General Dentist",
        "Pediatric Dentist",
        "Orthodontist",
        "Periodontist",
        "Endodontist",
        "Prosthodontist",
        "Oral Surgeon",
        "Maxillofacial Surgeon",
        "Cosmetic Dentist",
        "Dental Director",
        "Chief Clinical Officer - Dental",
        "Principal Dentist",
        "Managing Dentist",
        "Associate Dentist",
        "Lead Dentist",
        "Practice Owner",
    ]

    for title in dental_titles:
        experiences = []
        result = _current_title(experiences, title)
        assert result == title, f"Expected '{title}', got '{result}'"

    print(f"✓ All {len(dental_titles)} dental title examples work")


if __name__ == "__main__":
    print("\n=== Testing Contacts DB Title Fix ===\n")

    test_current_title_with_experiences()
    test_current_title_with_empty_experiences()
    test_current_title_without_direct_title()
    test_current_title_prioritizes_experiences()
    test_list_builder_current_title()
    test_contacts_db_conversion_pattern()
    test_dental_titles_examples()

    print("\n✅ All tests passed!\n")
