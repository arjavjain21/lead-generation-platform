"""Structural assertions for the Data Explorer page (#/explorer).

Follows the repo's served-HTML assertion convention (see
frontend/tests/test_tam_page_ui.py): the shipped index.html is the
product, so the page's contract — page div, hidden-by-default nav item,
filter/count/facet/results element ids, the /api/explorer/* BFF calls,
and the explorerState/init function names — is pinned here to catch
regressions from future hand edits of the single-file frontend.
"""
from pathlib import Path

import pytest

INDEX = Path(__file__).resolve().parents[2] / "frontend" / "index.html"


@pytest.fixture(scope="module")
def html():
    return INDEX.read_text(encoding="utf-8")


# --- Page + navigation -------------------------------------------------------

def test_explorer_page_div_present(html):
    assert 'id="page-explorer"' in html


def test_nav_item_exists_and_is_hidden_by_default(html):
    assert 'data-page="explorer"' in html
    assert ">Data Explorer</a>" in html
    # Hidden until explorerCheckStatus() sees {enabled: true} from /status.
    # Without this the page ships visible to every user while disabled.
    assert 'id="navExplorer" style="display:none;"' in html


def test_router_routes_explorer_hash_to_init(html):
    assert "'explorer': 'explorer'," in html
    assert "if (pageId === 'explorer') initExplorerPage();" in html


def test_boot_checks_status_after_auth(html):
    # Both post-auth boot paths (checkAuth success + login success) probe
    # /api/explorer/status before revealing the nav item.
    assert html.count("explorerCheckStatus();") >= 2


def test_disabled_empty_state_present(html):
    assert 'id="exDisabledState"' in html
    assert "Data Explorer is disabled" in html


# --- Filters / count / facets / results ---------------------------------------

def test_filter_panel_element_ids_present(html):
    # Person / Company / Email / Data collapsible groups in the sidebar.
    for element_id in (
        "exFilterPanel",
        "exQ", "exTitleKeywords", "exSeniorityChips", "exCountryChips",
        "exUniverseChips", "exHasLinkedin",
        "exDomain", "exIndustryChips", "exBandChips",
        "exHasEmail", "exVerified", "exEmailResultChips", "exSegChips",
        "exExcludeGeneric",
        "exCreatedAfter", "exCreatedBefore", "exUpdatedAfter", "exUpdatedBefore",
    ):
        assert f'id="{element_id}"' in html, element_id


def test_active_filter_chips_and_reset_present(html):
    assert 'id="exActiveChips"' in html
    assert 'id="exResetFilters"' in html


def test_count_bar_with_approximate_handling(html):
    assert 'id="exCountBar"' in html
    assert 'id="exCountNum"' in html
    # Contract: approximate counts render with a leading "≈" and an
    # "estimated" tooltip/legend.
    assert "'≈ '" in html
    assert "'estimated'" in html


def test_facet_panel_present(html):
    assert 'id="exFacets"' in html
    assert 'id="exFacetsAsOf"' in html


def test_results_table_contract(html):
    assert 'id="exResultsTable"' in html
    assert 'class="results-table" id="exResultsTable"' in html
    assert 'id="exResultsBody"' in html
    assert 'id="exLoadMore"' in html  # keyset pagination via next_cursor
    assert 'id="exEmptyState"' in html
    # Table columns the drawer link lives in.
    for col in ("<th>Name</th>", "<th>Title</th>", "<th>Seniority</th>",
                "<th>Company</th>", "<th>Industry</th>", "<th>Country</th>",
                "<th>Email</th>", "<th>Badges</th>", "<th>Updated</th>"):
        assert col in html


# --- Views / export / detail drawer --------------------------------------------

def test_saved_views_elements_present(html):
    for element_id in ("exSavedViews", "exSaveViewBtn", "exDeleteViewBtn",
                       "explorerSaveViewModal", "exViewName", "exViewSaveConfirm"):
        assert f'id="{element_id}"' in html, element_id


def test_export_dialog_elements_present(html):
    for element_id in ("exExportBtn", "explorerExportModal", "exExportFormat",
                       "exExportGzip", "exExportConfirm", "exExportDownload",
                       "exExportStatus", "exExportError"):
        assert f'id="{element_id}"' in html, element_id


def test_detail_drawer_present(html):
    assert 'id="explorerDrawer"' in html
    assert 'id="exDrawerTitle"' in html
    assert 'id="exDrawerBody"' in html


# --- JS contract ---------------------------------------------------------------

def test_explorer_state_and_functions_exist(html):
    assert "window.explorerState" in html
    for fn in ("explorerCheckStatus", "initExplorerPage", "explorerRefresh",
               "explorerReadFilters", "explorerWriteFilters", "explorerToggleMulti",
               "explorerRenderResults", "explorerOpenPerson", "explorerLoadViews",
               "explorerConfirmSaveView", "explorerConfirmExport",
               "explorerPollExport", "explorerDownloadExport"):
        assert f"function {fn}" in html, fn


def test_bff_endpoints_called_from_ui(html):
    # explorerApi prefixes every call with the BFF base URL.
    assert "'/api/explorer'" in html
    for call in ("explorerApi('/people/search'", "explorerApi('/people/count'",
                 "explorerApi('/people/facets'", "explorerApi('/views'",
                 "explorerApi('/exports'", "explorerApi('/status'"):
        assert call in html, call


def test_export_poll_interval_and_gzip_threshold(html):
    assert "}, 2500);" in html          # poll every 2.5s per contract
    assert "count > 100000" in html     # gzip auto-checked above 100k rows


def test_neighbour_pages_untouched(html):
    # The page surgery must not clip neighbouring pages.
    assert 'id="page-search"' in html
    assert 'id="page-people"' in html
    assert 'id="findPeopleBody"' in html
    assert 'id="tamFindCompanies"' in html
    assert 'id="page-linkedin"' in html
