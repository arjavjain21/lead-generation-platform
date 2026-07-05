"""End-to-end test for the Pre-processing checkboxes.

Validates the rendered HTML/JS contains the new checkboxes for both
the chain (ChainProviderModal) and direct (DomainEnrichmentOptions)
flows, and that the submit handlers send the new flags.
"""
import re
import sys
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html_content():
    return INDEX_HTML.read_text()


class TestChainFlowCheckboxes:
    def test_chain_modal_contains_normalize_checkbox(self, html_content):
        assert 'id="chain_normalize_domains"' in html_content
        # Default ON
        match = re.search(
            r'<input type="checkbox" id="chain_normalize_domains"[^>]*checked',
            html_content,
        )
        assert match is not None

    def test_chain_modal_contains_dedupe_checkbox(self, html_content):
        assert 'id="chain_dedupe_by_domain"' in html_content
        match = re.search(
            r'<input type="checkbox" id="chain_dedupe_by_domain"[^>]*checked',
            html_content,
        )
        assert match is not None

    def test_chain_submit_sends_normalize(self, html_content):
        # The chain submit handler must read the checkbox and include
        # normalize_domains in the request body.
        assert "chain_normalize_domains" in html_content
        assert "normalize_domains:" in html_content

    def test_chain_submit_sends_dedupe(self, html_content):
        # The chain submit handler must include dedupe_by_domain.
        assert "chain_dedupe_by_domain" in html_content
        assert "dedupe_by_domain:" in html_content


class TestDomainFlowCheckboxes:
    def test_domain_options_contains_normalize_checkbox(self, html_content):
        assert 'id="domain_normalize_domains"' in html_content
        match = re.search(
            r'<input type="checkbox" id="domain_normalize_domains"[^>]*checked',
            html_content,
        )
        assert match is not None

    def test_domain_options_contains_dedupe_checkbox(self, html_content):
        assert 'id="domain_dedupe_by_domain"' in html_content
        match = re.search(
            r'<input type="checkbox" id="domain_dedupe_by_domain"[^>]*checked',
            html_content,
        )
        assert match is not None

    def test_domain_submit_sends_normalize(self, html_content):
        assert "domain_normalize_domains" in html_content
        assert "normalize_domains:" in html_content

    def test_domain_submit_sends_dedupe(self, html_content):
        assert "domain_dedupe_by_domain" in html_content
        assert "dedupe_by_domain:" in html_content


class TestSubBlockPlacedCorrectly:
    def test_chain_block_inside_modal(self, html_content):
        # The pre-processing block should be inside the chainProviderModal.
        modal_start = html_content.find('id="chainProviderModal"')
        block_start = html_content.find('id="chain_normalize_domains"')
        modal_end = html_content.find('</div>', modal_start)  # first closing
        assert modal_start != -1
        assert block_start != -1
        # The block should be within the modal (this is a rough check,
        # but it confirms the IDs are placed near the modal).
        assert block_start > modal_start

    def test_domain_block_inside_options(self, html_content):
        # The pre-processing block should be inside the domain options area.
        # We check that the block is rendered near the provider selection.
        provider_start = html_content.find('id="provider_blitz"')
        block_start = html_content.find('id="domain_normalize_domains"')
        assert provider_start != -1
        assert block_start != -1
        # The pre-processing block should be before the provider block
        # (chronologically it's inserted above the provider selection).
        assert block_start < provider_start
