"""People-page 'Scope to Companies' mode assertions (PR-2.6, 2026-11-11).

The company-scoped mode is a thin frontend over the EXISTING Flow-1 engine
(/upload → /flows/domain-enrich) — these pins keep it that way and keep the
honesty copy intact.
"""
from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX.read_text(encoding="utf-8")


def test_panel_present_and_collapsed(html):
    assert 'id="peopleScopeCard"' in html
    assert 'id="peopleScopePanel" style="display:none;"' in html


def test_inputs_present(html):
    for el in ("peopleScopeList", "peopleScopeTitles", "peopleScopeStrict",
               "peopleScopeMax", "peopleScopeCountry", "peopleScopeDepth", "peopleScopeGo"):
        assert f'id="{el}"' in html


def test_strict_default_on(html):
    assert 'id="peopleScopeStrict" checked' in html


def test_identifier_priority_copy(html):
    assert "company LinkedIn URL" in html
    assert "website domain" in html
    assert "never silently switched" in html
    assert "never guessed" in html


def test_location_caveat_present(html):
    assert "Applies to Blitz searches only" in html


def test_rides_flow1_engine_not_a_new_one(html):
    assert "/api/enrichment/flows/domain-enrich" in html
    assert "/api/enrichment/upload" in html
    # and it does NOT call any provider directly:
    assert "/v2/search" not in html.split('id="peopleScopeCard"')[1].split("</div>\n\n            <!-- Results")[0]


def test_internal_search_mode_preserved(html):
    # The original database-search mode is untouched alongside the new panel.
    assert 'id="findPeopleBtn"' in html
    assert "/api/enrichment/search/employees" in html
