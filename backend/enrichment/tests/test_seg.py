"""Tests for enrichment.seg — SEG (Secure Email Gateway) MX classification.

Pins three things the module MUST never drift on:
1. Provider label parity — the exact display literals the contacts DB stores
   ("SEG: Mimecast", "Microsoft", "Other / Unknown", "No Email (no MX)",
   "Invalid Domain"), sourced from /opt/contacts_api/scripts/seg_common.py.
   Platform-computed rows must match DB rows byte-for-byte.
2. Layer discipline — flag off => zero HTTP/DNS; cache hit short-circuits;
   contacts-db hit caches with source='contacts_db'; miss flows to DoH;
   negative caching suppresses repeat layer-2/3 calls within the TTL.
3. Defensiveness — the /v1/domain/seg endpoint may not exist yet (404/timeout
   => miss, never an error), and cache-write failures never raise.

HTTP is mocked with httpx.MockTransport (no network). SQLite runs against a
temp file via the shared.db.DB_PATH monkeypatch pattern used by
tests/test_auto_resume.py and tests/test_auth_last_used_debounce.py.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional

import httpx
import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from enrichment import seg  # noqa: E402
from shared import db as shared_db  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_seg_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point shared.db at a temp file, reinit the thread-local conn, create
    the schema (including domain_seg_cache). Restores the prod DB path and
    closes the temp conn afterwards so no other test inherits it.

    Pattern mirrors tests/test_auth_last_used_debounce.py (DB_PATH patch +
    thread-local conn reset) and tests/test_auto_resume.py (real schema on a
    temp file) — tests never touch the live 4.4 GB jobs.db.
    """
    db_path = tmp_path / "seg_test.db"
    monkeypatch.setattr(shared_db, "DB_PATH", db_path)
    # Force get_db() to reopen against the temp path for this thread.
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)
    shared_db.init_db()
    yield db_path
    conn = getattr(shared_db._local, "conn", None)
    if conn is not None:
        conn.close()
        shared_db._local.conn = None


@pytest.fixture
def seg_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_SEG_CLASSIFICATION", "true")
    monkeypatch.setenv("CONTACTS_API_TOKEN", "test-token")


class _RecordingTransport(httpx.MockTransport):
    """MockTransport that records every request it serves.

    httpx 0.27 routes AsyncClient through handle_async_request (NOT
    handle_request), so both entry points are overridden.
    """

    def __init__(self, handler, journal: list[httpx.Request]):
        super().__init__(handler)
        self._journal = journal

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self._journal.append(request)
        return super().handle_request(request)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self._journal.append(request)
        return await super().handle_async_request(request)


def _install_client(journal: list[httpx.Request], handler) -> httpx.AsyncClient:
    """Swap seg's shared client for a recording MockTransport client."""
    client = httpx.AsyncClient(transport=_RecordingTransport(handler, journal))
    return client


def _doh_response(mx_hosts: list[str]) -> httpx.Response:
    """Build a Cloudflare DoH JSON body with the given MX hostnames."""
    return httpx.Response(
        200,
        json={
            "Status": 0,
            "Answer": [
                {"name": "example.com", "type": 15, "TTL": 300, "data": f"10 {h}"}
                for h in mx_hosts
            ],
        },
    )


def _doh_no_mx() -> httpx.Response:
    return httpx.Response(200, json={"Status": 0, "Answer": []})


def _contacts_seg_handler(
    found: Optional[dict[str, tuple[str, str]]] = None,
    calls: Optional[list[list[str]]] = None,
    status: int = 200,
):
    """Handler for GET {base}/v1/domain/seg returning found/missing."""

    def handler(request: httpx.Request) -> httpx.Response:
        # Only GETs are layer-2 lookups; POSTs are map contributions
        # (they carry no `domains` query param and must not pollute the
        # layer-2 call journal).
        if calls is not None and request.method == "GET":
            domains = (request.url.params.get("domains") or "").split(",")
            calls.append(domains)
        if request.method == "POST":
            return httpx.Response(201, json={"inserted": [], "skipped_existing": []})
        if status != 200:
            return httpx.Response(status, json={"detail": "not deployed"})
        payload_found = [
            {"domain": d, "seg_classification": c, "seg_provider": p}
            for d, (c, p) in (found or {}).items()
        ]
        requested = (request.url.params.get("domains") or "").split(",")
        missing = [d for d in requested if d not in (found or {})]
        return httpx.Response(200, json={"found": payload_found, "missing": missing})

    return handler


def _doh_handler(
    mx_map: dict[str, list[str]],
    calls: Optional[list[str]] = None,
    fail_domains: Optional[set[str]] = None,
):
    """Handler for cloudflare-dns.com MX queries."""

    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.params.get("name") or ""
        if calls is not None:
            calls.append(name)
        if fail_domains and name in fail_domains:
            return httpx.Response(500)
        hosts = mx_map.get(name)
        if hosts is None:
            return _doh_no_mx()
        return _doh_response(hosts)

    return handler


def _install_transport(journal, contacts_status=200, contacts_found=None,
                       contacts_calls=None, mx_map=None, doh_calls=None,
                       doh_fail=None):
    """Build one client handling both hosts; install as seg's shared client."""

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "cloudflare-dns.com":
            return _doh_handler(mx_map or {}, doh_calls, doh_fail)(request)
        return _contacts_seg_handler(contacts_found, contacts_calls,
                                     contacts_status)(request)

    return _install_client(journal, handler)


# ---------------------------------------------------------------------------
# 1. Feature flag
# ---------------------------------------------------------------------------

class TestFeatureFlag:
    def test_flag_off_returns_empty_and_makes_no_calls(
        self, temp_seg_db, monkeypatch
    ):
        monkeypatch.delenv("ENABLE_SEG_CLASSIFICATION", raising=False)
        journal: list[httpx.Request] = []
        client = _install_client(journal, lambda r: httpx.Response(200, json={}))
        monkeypatch.setattr(seg, "_shared_http", client)

        async def _spy_doh(*_a, **_k):
            raise AssertionError("DoH must not run when the flag is off")

        monkeypatch.setattr(seg, "_doh_scan", _spy_doh)

        result = asyncio.run(seg.classify_domains(["acme.com", "gmail.com"]))
        assert result == {}
        assert journal == []

    def test_flag_off_contribute_is_noop(self, temp_seg_db, monkeypatch):
        monkeypatch.delenv("ENABLE_SEG_CLASSIFICATION", raising=False)
        journal: list[httpx.Request] = []
        client = _install_client(journal, lambda r: httpx.Response(200, json={}))
        monkeypatch.setattr(seg, "_shared_http", client)

        asyncio.run(seg.contribute_to_map(
            [{"domain": "acme.com", "classification": "external_seg",
              "provider": "SEG: Mimecast"}]
        ))
        assert journal == []

    def test_default_is_off(self, monkeypatch):
        monkeypatch.delenv("ENABLE_SEG_CLASSIFICATION", raising=False)
        assert seg.is_seg_enabled() is False

    def test_flag_true_enables(self, monkeypatch):
        monkeypatch.setenv("ENABLE_SEG_CLASSIFICATION", "true")
        assert seg.is_seg_enabled() is True

    def test_flag_is_read_per_call(self, monkeypatch):
        monkeypatch.delenv("ENABLE_SEG_CLASSIFICATION", raising=False)
        assert seg.is_seg_enabled() is False
        monkeypatch.setenv("ENABLE_SEG_CLASSIFICATION", "true")
        assert seg.is_seg_enabled() is True


# ---------------------------------------------------------------------------
# 2. _normalize_for_seg parity
# ---------------------------------------------------------------------------

class TestNormalizeForSeg:
    def test_bare_domain(self):
        assert seg._normalize_for_seg("acme.com") == "acme.com"

    def test_https_prefix(self):
        assert seg._normalize_for_seg("https://acme.com") == "acme.com"

    def test_http_prefix(self):
        assert seg._normalize_for_seg("http://acme.com") == "acme.com"

    def test_www_prefix(self):
        assert seg._normalize_for_seg("www.acme.com") == "acme.com"

    def test_www_prefix_with_scheme(self):
        assert seg._normalize_for_seg("https://www.acme.com") == "acme.com"

    def test_trailing_slash(self):
        assert seg._normalize_for_seg("acme.com/") == "acme.com"

    def test_trailing_dot(self):
        assert seg._normalize_for_seg("acme.com.") == "acme.com"

    def test_uppercase(self):
        assert seg._normalize_for_seg("Acme.COM") == "acme.com"

    def test_utm_query(self):
        # The 18-emails-from-96k-rows bug: raw scraper URLs must collapse
        # to the bare domain before any lookup.
        assert (
            seg._normalize_for_seg("https://mesterh-service.de/?utm_source=gmb")
            == "mesterh-service.de"
        )

    def test_full_url_with_path_and_query(self):
        assert seg._normalize_for_seg("http://www.acme.com/path?q=1") == "acme.com"

    def test_fragment(self):
        assert seg._normalize_for_seg("acme.com/#/about") == "acme.com"

    def test_port_stripped(self):
        # seg_common cuts at ':' — the map key must too.
        assert seg._normalize_for_seg("acme.com:8080") == "acme.com"

    def test_double_www(self):
        assert seg._normalize_for_seg("www.www.acme.com") == "acme.com"

    def test_email_rejected(self):
        assert seg._normalize_for_seg("user@example.com") == ""

    def test_no_dot_kept_as_invalid(self):
        # Stage-1 normalize_domain rejects dot-less words as domains, but
        # seg_common stamps them no_email/'Invalid Domain'. _normalize_for_seg
        # therefore keeps them so _classify_offline can do exactly that,
        # with zero network. (Verified against seg_common.is_malformed.)
        assert seg._normalize_for_seg("localhost") == "localhost"
        assert seg._normalize_for_seg("acme") == "acme"
        assert seg._classify_offline("localhost") == (
            "no_email", "Invalid Domain", ""
        )

    def test_empty_and_none(self):
        assert seg._normalize_for_seg("") == ""
        assert seg._normalize_for_seg(None) == ""

    def test_noise_tokens_rejected(self):
        for token in ("nan", "none", "n/a", "-"):
            assert seg._normalize_for_seg(token) == ""


# ---------------------------------------------------------------------------
# 3. _classify_mx_hosts label parity (literals from seg_common.py)
# ---------------------------------------------------------------------------

class TestClassifyMxHostsLabels:
    """Each case pins the EXACT provider literal the contacts DB stores."""

    def test_proofpoint(self):
        assert seg._classify_mx_hosts(["mx1.ppphosted.com"]) == (
            "external_seg", "SEG: Proofpoint"
        )

    def test_proofpoint_essentials(self):
        assert seg._classify_mx_hosts(["mx.ppe-hosted.com"]) == (
            "external_seg", "SEG: Proofpoint Essentials"
        )

    def test_mimecast(self):
        assert seg._classify_mx_hosts(["eu-smtp-in-01.mimecast.com"]) == (
            "external_seg", "SEG: Mimecast"
        )

    def test_barracuda(self):
        assert seg._classify_mx_hosts(["mx.ess.barracudanetworks.com"]) == (
            "external_seg", "SEG: Barracuda"
        )

    def test_cisco_iphmx(self):
        assert seg._classify_mx_hosts(["mx.iphmx.com"]) == (
            "external_seg", "SEG: Cisco IronPort"
        )

    def test_messagelabs(self):
        assert seg._classify_mx_hosts(["mail5.messagelabs.com"]) == (
            "external_seg", "SEG: Symantec BES (MessageLabs)"
        )

    def test_google(self):
        assert seg._classify_mx_hosts(["aspmx.l.google.com"]) == (
            "direct_google", "Google"
        )

    def test_google_generic_pattern(self):
        # Not a signature hit, but the joined-string fallback catches it.
        assert seg._classify_mx_hosts(["alt1.gmail-scanner.example.net",
                                       "google-frontend.example.net"]) == (
            "direct_google", "Google"
        )

    def test_microsoft_outlook(self):
        assert seg._classify_mx_hosts(["outlook-com.olc.protection.outlook.com"]) == (
            "direct_microsoft", "Microsoft"
        )

    def test_microsoft_generic_pattern(self):
        assert seg._classify_mx_hosts(["mail.microsoft-frontend.example.net"]) == (
            "direct_microsoft", "Microsoft"
        )

    def test_other_unknown(self):
        assert seg._classify_mx_hosts(["mx1.acme-mail.example.net"]) == (
            "other_or_unknown", "Other / Unknown"
        )

    def test_seg_beats_google_when_seg_host_is_first(self):
        # seg_common iterates HOST-major: the first MX host that matches ANY
        # signature wins, so a Mimecast host ahead of a Google host classifies
        # as the SEG. (Conversely a Google host first wins as direct_google —
        # pinned below.) Both orders verified against seg_common.classify_mx.
        assert seg._classify_mx_hosts(
            ["mx1.mimecast.com", "aspmx.l.google.com"]
        ) == ("external_seg", "SEG: Mimecast")

    def test_google_first_wins_when_google_host_is_first(self):
        # Host-major iteration, not signature-major: the DB's own behaviour.
        assert seg._classify_mx_hosts(
            ["aspmx.l.google.com", "mx1.mimecast.com"]
        ) == ("direct_google", "Google")

    def test_signature_order_within_one_host(self):
        # Within a SINGLE host, signature insertion order is precedence
        # (verified against seg_common.classify_mx).
        assert seg._classify_mx_hosts(["mail.ppe-hosted.com.pphosted.com"]) == (
            "external_seg", "SEG: Proofpoint"
        )
        assert seg._classify_mx_hosts(["mx.iphmx.com.mimecast.com"]) == (
            "external_seg", "SEG: Mimecast"
        )

    def test_case_insensitive_host_match(self):
        assert seg._classify_mx_hosts(["MX1.MIMECAST.COM"]) == (
            "external_seg", "SEG: Mimecast"
        )

    def test_empty_mx_list(self):
        # No hosts => no signatures => other/unknown (callers handle the
        # no-MX verdict before ever reaching _classify_mx_hosts).
        assert seg._classify_mx_hosts([]) == ("other_or_unknown", "Other / Unknown")

    def test_signature_table_matches_db_size(self):
        # 31 signatures in seg_common.SEG_SIGNATURES — the port must be 1:1.
        assert len(seg.SEG_SIGNATURES) == 31


# ---------------------------------------------------------------------------
# 4. Free-webmail fast path + malformed (both no-network)
# ---------------------------------------------------------------------------

class TestOfflineFastPaths:
    def test_gmail_direct_google_no_network(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        journal: list[httpx.Request] = []
        client = _install_client(
            journal,
            lambda r: (_ for _ in ()).throw(AssertionError("no network expected")),
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["gmail.com"]))
        assert result == {
            "gmail.com": {
                "seg_classification": "direct_google",
                "seg_provider": "Google",
                "source": "doh",
            }
        }
        assert journal == []
        # Cached so the next call doesn't redo the fast path.
        row = sqlite3.connect(temp_seg_db).execute(
            "SELECT seg_classification, seg_provider, source, mx_hosts "
            "FROM domain_seg_cache WHERE domain='gmail.com'"
        ).fetchone()
        assert row == ("direct_google", "Google", "doh", "google.com (free webmail)")

    def test_hotmail_direct_microsoft(self):
        result = seg._free_webmail_lookup("hotmail.com")
        assert result == ("direct_microsoft", "Microsoft", "microsoft (free webmail)")

    def test_yahoo_free_webmail_other(self):
        classification, provider, mx = seg._free_webmail_lookup("yahoo.com")
        assert classification == "other_or_unknown"
        assert provider == "Free Webmail (yahoo.com)"
        assert mx == "other free webmail"

    def test_corporate_domain_not_free_webmail(self):
        assert seg._free_webmail_lookup("acme.com") is None

    def test_malformed_invalid_domain_no_network(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        journal: list[httpx.Request] = []
        client = _install_client(
            journal,
            lambda r: (_ for _ in ()).throw(AssertionError("no network expected")),
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["no-dot-here"]))
        assert result == {
            "no-dot-here": {
                "seg_classification": "no_email",
                "seg_provider": "Invalid Domain",
                "source": "doh",
            }
        }
        assert journal == []

    def test_malformed_variants(self):
        for domain in ("-acme.com", ".acme.com", "ac me.com", "acme,com"):
            assert seg._classify_offline(domain) == ("no_email", "Invalid Domain", "")

    def test_valid_domain_not_offline_classifiable(self):
        # Needs MX lookup — offline returns None.
        assert seg._classify_offline("acme.com") is None


# ---------------------------------------------------------------------------
# 5. Layer behaviour with mocked HTTP
# ---------------------------------------------------------------------------

class TestLayerFlow:
    def test_cache_hit_short_circuits_network(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        conn = shared_db.get_db()
        conn.execute(
            "INSERT INTO domain_seg_cache (domain, seg_classification, "
            "seg_provider, source, mx_hosts, fetched_at) "
            "VALUES ('acme.com', 'external_seg', 'SEG: Mimecast', "
            "'contacts_db', NULL, datetime('now'))"
        )
        conn.commit()

        journal: list[httpx.Request] = []
        client = _install_client(
            journal,
            lambda r: (_ for _ in ()).throw(AssertionError("no network expected")),
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result == {
            "acme.com": {
                "seg_classification": "external_seg",
                "seg_provider": "SEG: Mimecast",
                "source": "contacts_db",
            }
        }
        assert journal == []

    def test_contacts_db_hit_caches_with_source(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        journal: list[httpx.Request] = []
        layer2_calls: list[list[str]] = []
        layer3_calls: list[str] = []
        client = _install_transport(
            journal,
            contacts_found={"acme.com": ("external_seg", "SEG: Proofpoint")},
            contacts_calls=layer2_calls,
            mx_map={},
            doh_calls=layer3_calls,
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result == {
            "acme.com": {
                "seg_classification": "external_seg",
                "seg_provider": "SEG: Proofpoint",
                "source": "contacts_db",
            }
        }
        assert layer2_calls == [["acme.com"]]
        assert layer3_calls == []  # found => no DoH

        row = sqlite3.connect(temp_seg_db).execute(
            "SELECT seg_classification, seg_provider, source "
            "FROM domain_seg_cache WHERE domain='acme.com'"
        ).fetchone()
        assert row == ("external_seg", "SEG: Proofpoint", "contacts_db")

    def test_miss_flows_to_doh_and_caches(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        journal: list[httpx.Request] = []
        layer2_calls: list[list[str]] = []
        layer3_calls: list[str] = []
        client = _install_transport(
            journal,
            contacts_found={},
            contacts_calls=layer2_calls,
            mx_map={"acme.com": ["mx1.mimecast.com", "mx2.mimecast.com"]},
            doh_calls=layer3_calls,
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["https://acme.com/"]))
        assert result == {
            "acme.com": {
                "seg_classification": "external_seg",
                "seg_provider": "SEG: Mimecast",
                "source": "doh",
            }
        }
        assert layer2_calls == [["acme.com"]]
        assert layer3_calls == ["acme.com"]

        row = sqlite3.connect(temp_seg_db).execute(
            "SELECT seg_classification, seg_provider, source, mx_hosts "
            "FROM domain_seg_cache WHERE domain='acme.com'"
        ).fetchone()
        assert row == (
            "external_seg", "SEG: Mimecast", "doh",
            '["mx1.mimecast.com", "mx2.mimecast.com"]',
        )

    def test_contacts_db_404_treated_as_miss(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        """The endpoint may not exist yet — 404 must not crash and must
        flow through to DoH."""
        journal: list[httpx.Request] = []
        layer3_calls: list[str] = []
        client = _install_transport(
            journal,
            contacts_status=404,
            mx_map={"acme.com": ["aspmx.l.google.com"]},
            doh_calls=layer3_calls,
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result["acme.com"]["seg_classification"] == "direct_google"
        assert layer3_calls == ["acme.com"]

    def test_contacts_db_timeout_treated_as_miss(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "cloudflare-dns.com":
                return _doh_response(["mx.iphmx.com"])
            raise httpx.ConnectTimeout("simulated timeout")

        journal: list[httpx.Request] = []
        client = _install_client(journal, handler)
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result["acme.com"]["seg_provider"] == "SEG: Cisco IronPort"

    def test_contacts_db_found_but_blank_flows_to_doh(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        """A found row with an empty classification is not an answer (and the
        contract's `missing` list won't include it) — it must still reach the
        DoH layer rather than silently vanishing between layers."""
        journal: list[httpx.Request] = []
        layer3_calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            if host == "cloudflare-dns.com":
                layer3_calls.append(request.url.params.get("name") or "")
                return _doh_response(["mx1.mimecast.com"])
            # contacts DB: found, but blank classification, and missing=[].
            return httpx.Response(200, json={
                "found": [{"domain": "acme.com",
                           "seg_classification": "",
                           "seg_provider": ""}],
                "missing": [],
            })

        client = _install_client(journal, handler)
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result == {
            "acme.com": {
                "seg_classification": "external_seg",
                "seg_provider": "SEG: Mimecast",
                "source": "doh",
            }
        }
        assert layer3_calls == ["acme.com"]

    def test_negative_caching_suppresses_repeat_calls(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        """A domain missing from every layer is cached as '' and is NOT
        re-queried within the TTL."""
        journal: list[httpx.Request] = []
        layer2_calls: list[list[str]] = []
        layer3_calls: list[str] = []
        client = _install_transport(
            journal,
            contacts_found={},
            contacts_calls=layer2_calls,
            mx_map={},  # no MX for anyone
            doh_calls=layer3_calls,
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        first = asyncio.run(seg.classify_domains(["acme.com"]))
        assert first == {}
        assert layer2_calls == [["acme.com"]]
        assert layer3_calls == ["acme.com"]

        row = sqlite3.connect(temp_seg_db).execute(
            "SELECT seg_classification, seg_provider, source "
            "FROM domain_seg_cache WHERE domain='acme.com'"
        ).fetchone()
        assert row == ("", "", "doh")

        second = asyncio.run(seg.classify_domains(["acme.com"]))
        assert second == {}
        # Neither layer ran again — the negative cache answered.
        assert layer2_calls == [["acme.com"]]
        assert layer3_calls == ["acme.com"]

    def test_stale_negative_cache_rechecks(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        conn = shared_db.get_db()
        conn.execute(
            "INSERT INTO domain_seg_cache (domain, seg_classification, "
            "seg_provider, source, mx_hosts, fetched_at) "
            "VALUES ('acme.com', '', '', 'doh', NULL, datetime('now', '-30 days'))"
        )
        conn.commit()

        journal: list[httpx.Request] = []
        layer2_calls: list[list[str]] = []
        layer3_calls: list[str] = []
        client = _install_transport(
            journal,
            contacts_found={},
            contacts_calls=layer2_calls,
            mx_map={"acme.com": ["mx1.ppphosted.com"]},
            doh_calls=layer3_calls,
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result == {
            "acme.com": {
                "seg_classification": "external_seg",
                "seg_provider": "SEG: Proofpoint",
                "source": "doh",
            }
        }
        assert layer2_calls == [["acme.com"]]
        assert layer3_calls == ["acme.com"]

    def test_doh_transport_failure_negative_caches(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        """A 5xx from DoH is a miss, not a fake no_email verdict."""
        journal: list[httpx.Request] = []
        client = _install_transport(
            journal,
            contacts_found={},
            mx_map={},
            doh_fail={"acme.com"},
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result == {}
        row = sqlite3.connect(temp_seg_db).execute(
            "SELECT seg_classification FROM domain_seg_cache WHERE domain='acme.com'"
        ).fetchone()
        assert row == ("",)

    def test_mixed_batch_hit_and_miss(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        journal: list[httpx.Request] = []
        layer2_calls: list[list[str]] = []
        layer3_calls: list[str] = []
        client = _install_transport(
            journal,
            contacts_found={
                "cached.com": ("direct_microsoft", "Microsoft"),
            },
            contacts_calls=layer2_calls,
            mx_map={"fresh.com": ["mail5.messagelabs.com"]},
            doh_calls=layer3_calls,
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains([
            "cached.com", "https://fresh.com/?utm=x", "gmail.com", "not-a-domain",
        ]))
        assert set(result) == {"cached.com", "fresh.com", "gmail.com", "not-a-domain"}
        assert result["cached.com"]["seg_provider"] == "Microsoft"
        assert result["fresh.com"]["seg_provider"] == "SEG: Symantec BES (MessageLabs)"
        assert result["fresh.com"]["seg_classification"] == "external_seg"
        assert result["gmail.com"]["seg_classification"] == "direct_google"
        # Malformed input is stamped Invalid Domain offline — no network.
        assert result["not-a-domain"] == {
            "seg_classification": "no_email",
            "seg_provider": "Invalid Domain",
            "source": "doh",
        }
        # Only the non-offline, non-found domains hit layer 2/3.
        assert layer2_calls == [["cached.com", "fresh.com"]]
        assert layer3_calls == ["fresh.com"]

    def test_dedupe_in_input(self, temp_seg_db, seg_enabled, monkeypatch):
        journal: list[httpx.Request] = []
        layer2_calls: list[list[str]] = []
        client = _install_transport(
            journal,
            contacts_found={},
            contacts_calls=layer2_calls,
            mx_map={"acme.com": ["mx1.mimecast.com"]},
            doh_calls=[],
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains([
            "acme.com", "ACME.com", "https://acme.com/", "www.acme.com",
        ]))
        assert list(result) == ["acme.com"]
        assert layer2_calls == [["acme.com"]]

    def test_layer2_chunks_at_100(self, temp_seg_db, seg_enabled, monkeypatch):
        domains = [f"d{i}.com" for i in range(250)]
        journal: list[httpx.Request] = []
        layer2_calls: list[list[str]] = []
        client = _install_transport(
            journal,
            contacts_found={},
            contacts_calls=layer2_calls,
            mx_map={},
            doh_calls=[],
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(domains))
        assert result == {}
        assert [len(c) for c in layer2_calls] == [100, 100, 50]


# ---------------------------------------------------------------------------
# 6. Defensiveness
# ---------------------------------------------------------------------------

class TestDefensiveBehaviour:
    def test_cache_write_failure_does_not_raise(
        self, temp_seg_db, seg_enabled, monkeypatch
    ):
        def _boom(*_a, **_k):
            raise sqlite3.OperationalError("simulated write failure")

        monkeypatch.setattr(seg, "_write_cache", _boom)

        journal: list[httpx.Request] = []
        layer3_calls: list[str] = []
        client = _install_transport(
            journal,
            contacts_found={},
            mx_map={"acme.com": ["mx1.mimecast.com"]},
            doh_calls=layer3_calls,
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result["acme.com"]["seg_provider"] == "SEG: Mimecast"

    def test_missing_token_degrades_to_miss(
        self, temp_seg_db, monkeypatch
    ):
        monkeypatch.setenv("ENABLE_SEG_CLASSIFICATION", "true")
        monkeypatch.delenv("CONTACTS_API_TOKEN", raising=False)
        journal: list[httpx.Request] = []
        layer3_calls: list[str] = []
        client = _install_transport(
            journal,
            contacts_found={},
            mx_map={"acme.com": ["mx1.mimecast.com"]},
            doh_calls=layer3_calls,
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        result = asyncio.run(seg.classify_domains(["acme.com"]))
        assert result["acme.com"]["seg_provider"] == "SEG: Mimecast"
        assert layer3_calls == ["acme.com"]

    def test_empty_input(self, temp_seg_db, seg_enabled):
        assert asyncio.run(seg.classify_domains([])) == {}

    def test_all_noise_input(self, temp_seg_db, seg_enabled):
        assert asyncio.run(seg.classify_domains(["", "nan", None])) == {}


# ---------------------------------------------------------------------------
# 7. contribute_to_map
# ---------------------------------------------------------------------------

class TestContributeToMap:
    def test_payload_shape_and_chunking(self, temp_seg_db, seg_enabled, monkeypatch):
        journal: list[httpx.Request] = []
        posted: list[Any] = []

        def handler(request: httpx.Request) -> httpx.Response:
            posted.append(request.read())
            return httpx.Response(200, json={"ok": True})

        client = _install_client(journal, handler)
        monkeypatch.setattr(seg, "_shared_http", client)

        entries = [
            {
                "domain": f"d{i}.com",
                "classification": "external_seg",
                "provider": "SEG: Mimecast",
                "mx_hosts": ["mx1.mimecast.com"],
            }
            for i in range(150)
        ]
        asyncio.run(seg.contribute_to_map(entries))

        assert len(journal) == 2  # 100 + 50
        first = httpx.Response(200, content=posted[0]).json()
        assert len(first["entries"]) == 100
        assert first["entries"][0] == {
            "domain": "d0.com",
            "classification": "external_seg",
            "provider": "SEG: Mimecast",
            "mx_hosts": ["mx1.mimecast.com"],
        }

    def test_404_swallowed(self, temp_seg_db, seg_enabled, monkeypatch):
        journal: list[httpx.Request] = []
        client = _install_client(
            journal, lambda r: httpx.Response(404, json={"detail": "nope"})
        )
        monkeypatch.setattr(seg, "_shared_http", client)

        asyncio.run(seg.contribute_to_map(
            [{"domain": "acme.com", "classification": "external_seg",
              "provider": "SEG: Mimecast"}]
        ))
        assert len(journal) == 1  # attempted, failure swallowed

    def test_transport_error_swallowed(self, temp_seg_db, seg_enabled, monkeypatch):
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("simulated failure")

        journal: list[httpx.Request] = []
        client = _install_client(journal, handler)
        monkeypatch.setattr(seg, "_shared_http", client)

        asyncio.run(seg.contribute_to_map(
            [{"domain": "acme.com", "classification": "external_seg",
              "provider": "SEG: Mimecast"}]
        ))

    def test_empty_entries_noop(self, temp_seg_db, seg_enabled, monkeypatch):
        journal: list[httpx.Request] = []
        client = _install_client(journal, lambda r: httpx.Response(200))
        monkeypatch.setattr(seg, "_shared_http", client)
        asyncio.run(seg.contribute_to_map([]))
        assert journal == []


# ---------------------------------------------------------------------------
# 8. Sync cache-only lookup
# ---------------------------------------------------------------------------

class TestSyncLookup:
    def test_cache_only_no_network(self, temp_seg_db, seg_enabled, monkeypatch):
        conn = shared_db.get_db()
        conn.execute(
            "INSERT INTO domain_seg_cache (domain, seg_classification, "
            "seg_provider, source, mx_hosts, fetched_at) "
            "VALUES ('acme.com', 'direct_google', 'Google', 'doh', NULL, "
            "datetime('now'))"
        )
        conn.commit()

        async def _spy(*_a, **_k):
            raise AssertionError("sync lookup must not hit the network")

        monkeypatch.setattr(seg, "_fetch_seg_from_contacts_db", _spy)
        monkeypatch.setattr(seg, "_doh_scan", _spy)

        result = seg.get_seg_for_domains_sync(["acme.com", "uncached.com"])
        assert result == {
            "acme.com": {
                "seg_classification": "direct_google",
                "seg_provider": "Google",
                "source": "doh",
            }
        }

    def test_negative_row_excluded(self, temp_seg_db):
        conn = shared_db.get_db()
        conn.execute(
            "INSERT INTO domain_seg_cache (domain, seg_classification, "
            "seg_provider, source, mx_hosts, fetched_at) "
            "VALUES ('acme.com', '', '', 'doh', NULL, datetime('now'))"
        )
        conn.commit()
        assert seg.get_seg_for_domains_sync(["acme.com"]) == {}


# ---------------------------------------------------------------------------
# 9. DoH response parsing
# ---------------------------------------------------------------------------

class TestMxExtraction:
    def test_extracts_mx_type_only(self):
        body = {
            "Answer": [
                {"type": 1, "data": "1.2.3.4"},  # A record — skipped
                {"type": 15, "data": "10 mx1.acme.com."},
                {"type": 15, "data": "20 mx2.acme.com."},
            ]
        }
        assert seg._extract_mx_hosts(body) == ["mx1.acme.com", "mx2.acme.com"]

    def test_empty_answer(self):
        assert seg._extract_mx_hosts({"Answer": []}) == []
        assert seg._extract_mx_hosts({}) == []
        assert seg._extract_mx_hosts(None) == []

    def test_single_token_data(self):
        # Malformed MX data without a priority prefix — take the whole token.
        assert seg._extract_mx_hosts(
            {"Answer": [{"type": 15, "data": "mx.acme.com"}]}
        ) == ["mx.acme.com"]
