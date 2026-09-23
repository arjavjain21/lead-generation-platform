"""Verify the Indonesia option + campaign-bucket UI wiring in the served HTML.

Mirrors the project's served-HTML assertion convention
(see test_latam_dropdown_options.py).
"""
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX_HTML.read_text(encoding="utf-8")


# --- Indonesia dropdown -------------------------------------------------------

def test_scraper_country_select_exists(html):
    assert 'id="scraperCountry"' in html


def test_indonesia_option_present_in_dropdown(html):
    sel = html.find('id="scraperCountry"')
    assert sel != -1, "scraperCountry select not found"
    opt = html.find('<option value="id">', sel)
    assert opt != -1, "missing <option value=\"id\"> after scraperCountry select"
    assert "Indonesia" in html[opt:opt + 80]


def test_new_zealand_option_healed(html):
    """The nz backend region was historically missing from the dropdown."""
    sel = html.find('id="scraperCountry"')
    opt = html.find('<option value="nz">', sel)
    assert opt != -1, "missing <option value=\"nz\"> after scraperCountry select"


def test_indonesia_after_nz_pacific_order(html):
    sel = html.find('id="scraperCountry"')
    nz = html.find('<option value="nz">', sel)
    idn = html.find('<option value="id">', sel)
    assert -1 < nz < idn, "Indonesia should follow New Zealand (Asia-Pacific block)"


# --- Campaign bucket UI -------------------------------------------------------

def test_scraper_group_collapse_function_exists(html):
    assert "function toggleScraperGroup(" in html


def test_scraper_group_download_function_exists(html):
    assert "function downloadScraperGroup(" in html


def test_group_download_hits_group_endpoint(html):
    idx = html.find("function downloadScraperGroup(")
    assert idx != -1
    body = html[idx:idx + 1500]
    assert "/api/scraper/jobs/group/" in body
    assert "encodeURIComponent(groupName)" in body


def test_job_card_builder_extracted(html):
    assert "function scraperJobCardHtml(" in html


def test_render_groups_by_group_name(html):
    idx = html.find("function renderScraperJobs(")
    assert idx != -1
    body = html[idx:html.find("const scraperGroupCollapsed", idx)]
    assert body, "renderScraperJobs body not found"
    assert "job.group_name" in body
    assert "Download combined CSV" in body
