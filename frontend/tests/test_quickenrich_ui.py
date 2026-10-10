"""UI tests for the QuickEnrich provider + cascade reorder (2026-10-10).

New cascade order: contacts_db -> smartprospect -> quickenrich -> getleads
-> blitz -> wizleads (better_enrich disabled/unchecked, prospeo not shown).

Validates against the served single-file app (frontend/index.html):
- QuickEnrich checkboxes exist + are checked in BOTH provider sets
  (Flow-1 "Data Sources" panel and the chain modal), positioned between
  smartprospect and getleads to match the new cascade order.
- The cascade chips row (#enrichmentCascadeSteps) renders the new order
  with quickenrich between smartprospect and getleads, and the JS
  providerOrder mirror in updateCascadeStep matches the chips exactly.
- Every providers.push collector reads the quickenrich checkbox (in
  cascade-order position: after smartprospect, before getleads).
- The website-only grey-out provider-id list includes provider_quickenrich.
- The results tile + updateProviderResults setter wiring exists.
- Marketing copy no longer claims the old order/counts.
- Tag balance sanity (a missing </div> blanked whole pages before — 9daa4b2).
"""
import re
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"

NEW_CHIP_ORDER = [
    "contacts_db",
    "smartprospect",
    "quickenrich",
    "getleads",
    "blitz",
    "wizleads",
    "complete",
]


@pytest.fixture(scope="module")
def html_content():
    return INDEX_HTML.read_text()


class TestQuickEnrichCheckboxes:
    def test_domain_flow_checkbox_present_and_checked(self, html_content):
        assert 'id="provider_quickenrich"' in html_content
        match = re.search(
            r'<input type="checkbox" id="provider_quickenrich"[^>]*checked',
            html_content,
        )
        assert match is not None

    def test_chain_modal_checkbox_present_and_checked(self, html_content):
        assert 'id="chain_provider_quickenrich"' in html_content
        match = re.search(
            r'<input type="checkbox" id="chain_provider_quickenrich"[^>]*checked',
            html_content,
        )
        assert match is not None

    def test_domain_flow_checkbox_between_smartprospect_and_getleads(self, html_content):
        # First occurrences of all three ids are inside the Data Sources panel.
        sp = html_content.find('id="provider_smartprospect"')
        qe = html_content.find('id="provider_quickenrich"')
        gl = html_content.find('id="provider_getleads"')
        assert -1 < sp < qe < gl

    def test_chain_checkbox_between_smartprospect_and_getleads(self, html_content):
        sp = html_content.find('id="chain_provider_smartprospect"')
        qe = html_content.find('id="chain_provider_quickenrich"')
        gl = html_content.find('id="chain_provider_getleads"')
        assert -1 < sp < qe < gl


class TestCascadeChipsOrder:
    def test_chips_render_new_order(self, html_content):
        start = html_content.find('id="enrichmentCascadeSteps"')
        end = html_content.find('id="providerResults"')
        assert start != -1 and end != -1 and start < end
        chips = re.findall(r'data-provider="([^"]+)"', html_content[start:end])
        assert chips == NEW_CHIP_ORDER

    def test_quickenrich_chip_between_smartprospect_and_getleads(self, html_content):
        start = html_content.find('id="enrichmentCascadeSteps"')
        end = html_content.find('id="providerResults"')
        chips = re.findall(r'data-provider="([^"]+)"', html_content[start:end])
        assert -1 < chips.index("smartprospect") < chips.index("quickenrich") < chips.index("getleads")

    def test_provider_order_js_matches_chips(self, html_content):
        # updateCascadeStep assigns active/complete/pending by index against
        # this array — it must mirror the chips markup exactly.
        match = re.search(r"const providerOrder = \[([^\]]+)\]", html_content)
        assert match is not None
        order = [p.strip().strip("'\"") for p in match.group(1).split(",")]
        assert order == NEW_CHIP_ORDER


class TestSubmitHandlersPushQuickEnrich:
    def test_domain_flow_pushes_quickenrich_in_order(self, html_content):
        sp = html_content.find("getElementById('provider_smartprospect').checked) providers.push('smartprospect')")
        qe = html_content.find("getElementById('provider_quickenrich').checked) providers.push('quickenrich')")
        gl = html_content.find("getElementById('provider_getleads').checked) providers.push('getleads')")
        assert -1 < sp < qe < gl

    def test_chain_handler_pushes_quickenrich_in_order(self, html_content):
        handler = html_content.find("chainConfirmBtn")
        assert handler != -1
        sp = html_content.find("getElementById('chain_provider_smartprospect').checked) providers.push('smartprospect')", handler)
        qe = html_content.find("getElementById('chain_provider_quickenrich').checked) providers.push('quickenrich')", handler)
        gl = html_content.find("getElementById('chain_provider_getleads').checked) providers.push('getleads')", handler)
        assert -1 < sp < qe < gl

    def test_at_least_two_push_sites(self, html_content):
        # Flow-1 collector + chain handler (the third, dead executeChainEnrichment
        # collector is also kept in sync — never fewer than two live sites).
        assert html_content.count("providers.push('quickenrich')") >= 2

    def test_submit_bodies_send_providers(self, html_content):
        assert "providers: providers.length > 0 ? providers : null" in html_content


class TestProviderIdListAndResults:
    def test_website_only_greyout_list_includes_quickenrich(self, html_content):
        start = html_content.find("const providerBoxes = [")
        end = html_content.find("].map", start)
        assert start != -1 and end != -1
        assert "'provider_quickenrich'" in html_content[start:end]

    def test_results_tile_present(self, html_content):
        assert 'id="quickenrichResults"' in html_content

    def test_update_provider_results_sets_quickenrich(self, html_content):
        assert "results.quickenrich !== undefined" in html_content
        assert "getElementById('quickenrichResults').textContent = results.quickenrich" in html_content

    def test_display_name_maps_include_quickenrich(self, html_content):
        # SSE activity text, error tags, and jobs-page labels.
        assert "quickenrich: 'QuickEnrich'" in html_content
        assert "'quickenrich': 'QuickEnrich'" in html_content
        assert "if (p === 'quickenrich') return 'QuickEnrich';" in html_content


class TestMarketingCopyUpdated:
    def test_home_copy_lists_new_order(self, html_content):
        assert "6-source cascade: Contacts DB, smartprospect, QuickEnrich, GetLeads, Blitz API, and WizLeads" in html_content

    def test_old_home_order_copy_gone(self, html_content):
        assert "Contacts DB, Blitz API, GetLeads, smartprospect, WizLeads, and BetterEnrich" not in html_content

    def test_stale_help_page_count_gone(self, html_content):
        assert "We use 4 data sources" not in html_content


class TestTagBalance:
    def test_div_tags_balanced(self, html_content):
        # The shipped file has a long-standing skew of at most 1; a larger
        # gap means a </div> went missing (blank-page regression, commit 9daa4b2).
        assert abs(html_content.count("<div") - html_content.count("</div>")) <= 1

    def test_script_tags_balanced(self, html_content):
        assert html_content.count("<script") == html_content.count("</script>")
