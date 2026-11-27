"""Team admin page UI structure tests (multi-user rollout, 2026-11-26).

Served-HTML assertion convention: each test reads the shipped
../index.html as text and asserts substrings (presence and absence) to pin
structure against hand edits — same pattern as test_tam_page_ui.py.

What this pins:
- admin-only Team nav item (hidden by default, id navTeam)
- ROUTES entry + handleHashChange loader with the non-admin redirect guard
- updateUserUI gates the nav item on user.is_admin
- page-team container: users list, create-user form, activity feed + filters
- the /api/admin/* endpoints the page calls
- window exports for the inline onclick handlers (3rd-outbreak bug class)
- owner badge chips on enrichment + scraper job cards (user_email driven)
"""

from pathlib import Path

import pytest

INDEX = Path(__file__).parent.parent / "index.html"


@pytest.fixture(scope="module")
def html() -> str:
    return INDEX.read_text(encoding="utf-8")


class TestNavAndRouting:
    def test_nav_item_exists_hidden_by_default(self, html):
        assert 'data-page="team"' in html
        assert 'id="navTeam" style="display:none;"' in html

    def test_routes_entry(self, html):
        assert "'team': 'team'," in html

    def test_hash_change_loader_with_admin_guard(self, html):
        assert "if (pageId === 'team') {" in html
        assert "if (!(user && user.is_admin)) { navigateToPage('home'); return; }" in html
        assert "loadTeamUsers();" in html
        assert "loadTeamActivity(true);" in html

    def test_update_user_ui_gates_nav(self, html):
        assert "navTeam.style.display = (user && user.is_admin) ? '' : 'none';" in html


class TestPageMarkup:
    def test_page_container(self, html):
        assert 'id="page-team"' in html

    def test_users_list_and_form(self, html):
        assert 'id="teamUsersList"' in html
        assert 'id="teamNewEmail"' in html
        assert 'id="teamNewPassword"' in html
        assert 'id="teamNewAdmin"' in html
        assert 'id="teamCreateBtn"' in html

    def test_activity_feed_and_filters(self, html):
        assert 'id="teamActivityList"' in html
        assert 'id="teamActivityEmail"' in html
        assert 'id="teamActivityAction"' in html
        assert 'id="teamActivityMore"' in html
        # Retention copy matches the backend default (AUDIT_RETENTION_DAYS=90).
        assert "Retention: 90 days" in html


class TestApiWiring:
    def test_admin_endpoints_called(self, html):
        assert "'/api/admin/users'" in html
        assert "'/api/admin/users/' + userId + '/reset-password'" in html
        assert "'/api/admin/users/' + userId + '/status'" in html
        assert "'/api/admin/users/' + userId + '/quota'" in html
        assert "'/api/admin/activity?'" in html

    def test_inline_onclick_window_exports(self, html):
        # The row buttons are inline onclick= handlers — the functions MUST
        # be exported to window or the clicks silently no-op (bug class with
        # three prior outbreaks in this file).
        assert "window.teamResetPassword = teamResetPassword;" in html
        assert "window.teamToggleActive = teamToggleActive;" in html
        assert "window.teamSetQuota = teamSetQuota;" in html


class TestOwnerBadges:
    def test_enrichment_card_owner_chip(self, html):
        assert "job.user_email && (!user || job.user_email !== user.email)" in html
        # Two renderers (enrichment + scraper) each build the badge.
        assert html.count("job.user_email && (!user || job.user_email !== user.email)") == 2
