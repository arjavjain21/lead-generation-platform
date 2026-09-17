"""Structural assertions for the TAM ("Find Companies") page.

Follows the repo's served-HTML assertion convention (see
test_latam_dropdown_options.py / test_preprocessing_checkboxes_present.py):
the shipped index.html is the product, so the page's contract — explainer
copy, filter element ids, the /flows/tam call, and the REMOVAL of the old
sync company-search flow — is pinned here to catch regressions from future
hand edits of the single-file frontend.
"""
from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX.read_text(encoding="utf-8")


# --- Page + navigation -----------------------------------------------------

def test_tam_page_present_with_explainer(html):
    assert 'id="page-search"' in html
    assert "Find Companies (TAM)" in html


def test_nav_label_points_at_tam(html):
    assert 'data-page="search"' in html  # nav key unchanged (same page slot)
    assert ">Find Companies (TAM)</a>" in html


def test_explainer_answers_why_and_where(html):
    # The page must be understandable standalone: what it does, an example,
    # where results go, the contacts-DB save guarantee, and the cost note.
    assert "What this page does" in html
    assert "Head of Marketing" in html
    assert "Jobs</strong> page" in html
    assert "saved to your contacts database automatically" in html
    assert "uses 1 search record" in html


# --- Filters (company side unchanged, people side new) ---------------------

def test_company_filter_ids_survived_for_options_loader(html):
    # loadSearchOptions() fills these by id — renaming them breaks it.
    for element_id in (
        "searchName", "searchIndustry", "searchEmployeeRange",
        "searchCompanyType", "searchCountry",
    ):
        assert f'id="{element_id}"' in html


def test_people_filter_elements_present(html):
    for element_id in ("tamTitleInclude", "tamTitleExclude", "tamJobLevels",
                       "tamMinPerCompany", "tamMaxCompanies"):
        assert f'id="{element_id}"' in html
    # Seniority checkboxes use the Blitz job_level taxonomy.
    for level in ("C-Team", "VP", "Director", "Manager", "Staff"):
        assert f'value="{level}"' in html


def test_chain_enrich_opt_in_present(html):
    assert 'id="tamChainEnrich"' in html
    assert 'id="tamChainOptions"' in html
    assert 'id="tamMaxDms"' in html
    assert 'id="tamChainTitles"' in html


# --- Job flow contract ------------------------------------------------------

def test_tam_handler_posts_to_flows_tam(html):
    assert "/api/enrichment/flows/tam" in html
    assert "tamFindCompanies" in html


def test_tam_monitors_on_jobs_page_convention(html):
    # Same convention as the domain-enrich flow: navigate to the Jobs page.
    assert "navigateToPage('jobs')" in html


def test_tam_requires_at_least_one_filter_client_side(html):
    assert "Choose at least one filter" in html


# --- Old sync search removed -----------------------------------------------

def test_jobs_card_renders_tam_vocabulary(html):
    # Jobs page must not describe a company search in email/rows terms.
    assert "tam_flow: { t: '🎯 Find Companies (TAM)'" in html
    assert "tam_chain: { t: '🎯→📧 TAM → Contacts'" in html
    assert "const _isTamFlow = job.source_type === 'tam_flow';" in html
    # TAM jobs are exempt from the 0-emails bug warning; they get a
    # "no companies matched" hint instead.
    assert "_isTamFlow && job.status === 'done'" in html
    assert "No companies matched" in html
    # Providers block swaps to a single Source line for TAM jobs.
    assert "Blitz company search (TAM by People)" in html
    # Job titles may come from display_name (what was submitted).
    assert "job.display_name || job.display_filename" in html


def test_tam_page_lists_own_runs(html):
    assert 'id="tamRunsCard"' in html
    assert 'id="tamRunsBody"' in html
    assert "function loadTamRuns" in html
    assert "if (pageId === 'search') loadTamRuns();" in html


def test_old_sync_company_search_buttons_gone(html):
    assert "searchAndEnrich" not in html
    assert "searchCompanies" not in html
    assert "displaySearchResults" not in html


def test_old_search_enrich_endpoint_no_longer_called_from_ui(html):
    assert "/api/enrichment/search/companies/enrich" not in html


def test_find_people_and_domain_flows_untouched(html):
    # Neighbouring flows must not have been clipped by the page surgery.
    assert "/api/enrichment/flows/domain-enrich" in html
    assert "downloadLinkedinResults" in html
    assert "downloadDomainResults" in html
