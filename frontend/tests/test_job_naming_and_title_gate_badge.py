"""Job naming + strict-title badge UI guards (2026-09-28).

Pins the three UI pieces added after the Sep-23 museums incident:
1. Optional Job Name input on the Upload Domains Options card, sent as
   ``job_name`` on POST /api/enrichment/flows/domain-enrich.
2. ``title_gate`` badge chips (Strict titles ON / OFF) on enrichment job cards.
3. ``display_name`` taking precedence over the filename in the card title
   (named runs must be identifiable at a glance).
"""
import re
from pathlib import Path

import pytest

INDEX_HTML = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html_content():
    return INDEX_HTML.read_text()


class TestJobNameInput:
    def test_input_exists_on_options_card(self, html_content):
        assert 'id="jobNameInput"' in html_content
        # sits inside the domain options card, near the decision-maker settings
        idx_input = html_content.find('id="jobNameInput"')
        idx_card = html_content.find('id="domainOptions"')
        assert 0 < idx_card < idx_input

    def test_submit_body_sends_job_name(self, html_content):
        assert re.search(
            r"job_name:\s*\(document\.getElementById\('jobNameInput'\)",
            html_content,
        ) is not None


class TestTitleGateBadge:
    def test_chip_rendered_for_on_and_off(self, html_content):
        assert "Strict titles ON" in html_content
        assert "Strict titles OFF" in html_content
        assert "titleGateChip" in html_content

    def test_chip_inserted_into_card(self, html_content):
        # the chip must actually be appended inside the card markup, not just defined
        card_match = re.search(
            r"\+ sourceChip\s*\n\s*\+ titleGateChip", html_content
        )
        assert card_match is not None

    def test_chip_reads_server_field(self, html_content):
        assert "job.title_gate" in html_content


class TestDisplayNamePrecedence:
    def test_card_title_prefers_display_name(self, html_content):
        assert re.search(
            r"esc\(job\.display_name \|\| ctx\.title", html_content
        ) is not None


class TestTitleGateDropCounter:
    """2026-10-06: the badge shows how many off-ICP contacts the gate
    filtered (job.title_gate_dropped) so users can SEE the gate working —
    the RCA complaint was 'strict titles gave me CTO/Finance/Sales people'
    with a badge that just said ON."""

    def test_counter_field_read(self, html_content):
        assert "job.title_gate_dropped" in html_content

    def test_counter_appended_to_on_chip(self, html_content):
        assert re.search(
            r"_tgDropped\s*=\s*parseInt\(job\.title_gate_dropped", html_content
        ) is not None
        assert "off-ICP filtered" in html_content


class TestCompanyUrlColumnWarning:
    """2026-10-06: upload page warns when the LinkedIn URL column mapping
    selects a Company LinkedIn column — that routing fed the strict-titles
    bypass (lookalikes_STL uploads)."""

    def test_warning_element_exists(self, html_content):
        assert 'id="domainCompanyUrlWarning"' in html_content

    def test_warning_function_wired(self, html_content):
        assert "updateDomainCompanyUrlWarning" in html_content
        # re-evaluates when either the column mapping or the titles change
        assert re.search(
            r"getElementById\('linkedinUrlCol'\)\.onchange\s*=\s*updateDomainCompanyUrlWarning",
            html_content,
        ) is not None
        assert re.search(
            r"getElementById\('customTitles'\)\.oninput\s*=\s*updateDomainCompanyUrlWarning",
            html_content,
        ) is not None

    def test_warning_targets_company_columns_only(self, html_content):
        # the guard checks for a column that is BOTH linkedin and company
        assert re.search(
            r"selCol\.includes\('linkedin'\)\s*&&\s*selCol\.includes\('company'\)",
            html_content,
        ) is not None
