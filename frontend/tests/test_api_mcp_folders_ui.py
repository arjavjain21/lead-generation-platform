"""API/MCP system folders in the scraper jobs list (2026-10-13).

Jobs created via the external API key surface or the MCP scraper tools get
group_name='API'/'MCP' and must render as the existing collapsed folder cards
— WITHOUT the combined-CSV button (merging hundreds of unrelated one-off jobs
is meaningless and could stream a giant file). Regular UI submissions stay as
normal cards below the folders.

Follows the served-HTML assertion convention (see test_indonesia_and_campaign_groups_ui.py).
"""
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX_HTML.read_text(encoding="utf-8")


def test_system_folder_guard_exists(html):
    assert "isSystemFolder" in html, "system-folder guard missing from group render"
    assert "groupName === 'API' || groupName === 'MCP'" in html


def test_combined_csv_button_conditional_on_system_folder(html):
    """The combined-CSV button must be wrapped in the isSystemFolder ternary
    (campaign folders keep it; API/MCP folders drop it)."""
    guard = html.find("(isSystemFolder ? '' :")
    assert guard != -1, "combined-CSV button not gated on isSystemFolder"
    window = html[guard:guard + 400]
    assert "Download combined CSV" in window, "campaign combined-CSV button lost"


def test_campaign_button_still_present_for_regular_groups(html):
    """Campaign buckets keep their untouched wiring (2026-09-27 regression guard)."""
    assert "Download combined CSV" in html
    assert "downloadScraperGroup" in html
    assert "toggleScraperGroup" in html
