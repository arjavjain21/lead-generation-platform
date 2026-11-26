"""TAM V2 filter UI pins (2026-11-26).

Multi-select employees/countries, keyword include/exclude tag inputs, OR
semantics copy, and the honest keyword_match provenance note. Served-HTML
convention (test_tam_page_ui.py).
"""
from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX.read_text(encoding="utf-8")


# --- Feature A: multi-select employee ranges ---------------------------------

def test_employee_multiselect_present(html):
    assert 'id="tamEmployeeSearch"' in html
    assert 'id="tamEmployeeDrop"' in html
    assert 'id="tamEmployeeChips"' in html
    assert 'id="searchEmployeeRange"' not in html  # single-select removed


def test_employee_or_semantics_copy(html):
    assert "matched as OR — e.g. 11-50 + 201-500 skips 51-200" in html


# --- Feature B: multi-select countries ----------------------------------------

def test_country_multiselect_present(html):
    assert 'id="tamCountrySearch"' in html
    assert 'id="tamCountryChips"' in html
    assert 'id="searchCountry"' not in html  # single-select removed


def test_country_company_vs_person_distinction(html):
    assert "company headquarters" in html  # company-level copy on the TAM page


# --- Feature C: keyword tag inputs ---------------------------------------------

def test_keyword_inputs_present(html):
    assert 'id="tamKwInclude"' in html
    assert 'id="tamKwIncludeChips"' in html
    assert 'id="tamKwExclude"' in html
    assert 'id="tamKwExcludeChips"' in html


def test_keyword_copy_states_or_and_provenance(html):
    assert "mention ANY of these (OR)" in html
    assert "unverified" in html  # unknown-data policy surfaced to the operator


def test_keyword_tag_behaviour_wired(html):
    assert "makeTamKeywordTags" in html
    assert "tamKwIncludeList" in html and "tamKwExcludeList" in html


# --- Submit payload ------------------------------------------------------------

def test_submit_sends_arrays_and_keywords(html):
    assert "company.employee_range = Array.from(tamEmployeeSelected)" in html
    assert "company.hq_country_code = Array.from(tamCountrySelected)" in html
    assert "company.keywords_include = tamKwIncludeList.slice()" in html
    assert "company.keywords_exclude = tamKwExcludeList.slice()" in html


# --- Options loader --------------------------------------------------------------

def test_options_loader_feeds_v2_chips(html):
    assert "setupTamV2Chips(data.employee_ranges, data.countries)" in html


# --- Industry multi-select preserved --------------------------------------------

def test_industry_multiselect_untouched(html):
    assert 'id="tamIndustrySearch"' in html
    assert "company.industry_include = Array.from(tamIndustrySelected)" in html
