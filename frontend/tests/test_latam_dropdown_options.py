"""Verify the 11 Latin America country options are present in the scraper
country dropdown, in the correct grouped order. Mirrors the project's
served-HTML assertion convention (see test_preprocessing_checkboxes_present.py).
"""
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"
LATAM = ["mx", "br", "ar", "co", "cl", "pe", "ve", "ec", "bo", "py", "uy"]


@pytest.fixture(scope="module")
def html():
    return INDEX_HTML.read_text(encoding="utf-8")


def test_scraper_country_select_exists(html):
    assert 'id="scraperCountry"' in html


@pytest.mark.parametrize("code", LATAM)
def test_latam_option_present_in_dropdown(html, code):
    sel = html.find('id="scraperCountry"')
    assert sel != -1, "scraperCountry select not found"
    opt = html.find(f'<option value="{code}">', sel)
    assert opt != -1, f'missing <option value="{code}"> after scraperCountry select'


def test_americas_grouping_order(html):
    """ca (Canada) < LATAM bloc < au (Australia) — Americas grouped together."""
    i_ca = html.find('value="ca">Canada')
    i_mx = html.find('value="mx">Mexico')
    i_uy = html.find('value="uy">Uruguay')
    i_au = html.find('value="au">Australia')
    assert -1 < i_ca < i_mx < i_uy < i_au
