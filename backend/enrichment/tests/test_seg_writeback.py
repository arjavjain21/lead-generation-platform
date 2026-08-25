"""SEG classification passthrough into contacts-DB write-back.

Wave 2c. The contacts API's PersonUpsertRequest model ignores unknown body
keys, so the only server-persisted route for the platform's SEG (MX gateway)
verdict is ``custom_fields`` JSONB via ``_FIRMOGRAPHIC_CUSTOM_FIELDS``. The
canonical ``core.email.seg_classification`` columns are populated by the
contacts DB's own 6h cron from the domain map — this passthrough is the
belt-and-suspenders record on the person.

Pinned behaviors:
1. ``_FIRMOGRAPHIC_CUSTOM_FIELDS`` carries the two seg tuples, appended at
   the END so position-sensitive readers are unaffected.
2. Cache hit → the outgoing person upsert body's custom_fields carries both
   seg keys.
3. Explicit wins: a payload already carrying a verdict is never overwritten
   by the cache (same precedence as lead_universe: explicit > derived).
4. Cache miss / flag off / no domain → custom_fields carries NO seg keys and
   never an empty-string value.
5. No network from the writer path: the async ``seg.classify_domains`` (and
   every other network-capable seg entry point) is never invoked — only the
   cache-only ``get_seg_for_domains_sync``.
6. Outbox path unchanged: a transient failure still parks the row in
   contacts_write_outbox with the seg custom_fields intact in payload_json,
   and the batch path decorates the same way.

HTTP is mocked at ``contacts_writer._do_upsert`` (the pattern used by
test_contacts_writer.py and test_getleads_field_propagation.py). SQLite runs
against a temp file via the shared.db.DB_PATH monkeypatch pattern from
tests/test_seg.py — the live 4.4 GB jobs.db is never touched.

Run:
    python -m pytest enrichment/tests/test_seg_writeback.py -v
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

os.environ.setdefault("CONTACTS_API_TOKEN", "test-token-from-suite")

from enrichment import contacts_writer as cw  # noqa: E402
from enrichment import seg  # noqa: E402
from shared import db as shared_db  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point shared.db at a temp file, reinit the thread-local conn, create
    the schema (incl. contacts_write_outbox + domain_seg_cache). Restores
    the prod DB path afterwards so no other test inherits the temp conn.

    Pattern mirrors enrichment/tests/test_seg.py (which itself mirrors
    tests/test_auto_resume.py) — tests never touch the live jobs.db.
    """
    db_path = tmp_path / "seg_writeback_test.db"
    monkeypatch.setattr(shared_db, "DB_PATH", db_path)
    monkeypatch.setattr(shared_db._local, "conn", None, raising=False)
    shared_db.init_db()
    yield db_path
    conn = getattr(shared_db._local, "conn", None)
    if conn is not None:
        conn.close()
        shared_db._local.conn = None


@pytest.fixture
def seg_cache_rows():
    """Default cache seed; tests override via `_seed`."""
    return [("acme.com", "external_seg", "SEG: Mimecast")]


@pytest.fixture
def seg_db(temp_db: Path, seg_cache_rows) -> Path:
    """temp_db + domain_seg_cache seeded with seg_cache_rows."""
    conn = shared_db.get_db()
    for domain, classification, provider in seg_cache_rows:
        conn.execute(
            "INSERT OR REPLACE INTO domain_seg_cache "
            "(domain, seg_classification, seg_provider, source, mx_hosts, fetched_at) "
            "VALUES (?, ?, ?, 'contacts_db', NULL, datetime('now'))",
            (domain, classification, provider),
        )
    conn.execute("DELETE FROM contacts_write_outbox")
    conn.commit()
    return temp_db


@pytest.fixture
def seg_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_SEG_CLASSIFICATION", "true")


@pytest.fixture
def seg_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ENABLE_SEG_CLASSIFICATION", raising=False)


@pytest.fixture
def no_network(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Poison every network-capable entry point in seg. Returns the call
    journal so tests can assert it stayed empty."""
    calls: list[str] = []

    async def _explode(*a, **kw):  # pragma: no cover - must never run
        calls.append("async_network")
        raise AssertionError("network-capable seg call ran in the writer path")

    def _explode_sync(*a, **kw):  # pragma: no cover - must never run
        calls.append("sync_network")
        raise AssertionError("network-capable seg call ran in the writer path")

    monkeypatch.setattr(seg, "classify_domains", _explode)
    monkeypatch.setattr(seg, "_fetch_seg_from_contacts_db", _explode)
    monkeypatch.setattr(seg, "_doh_scan", _explode)
    monkeypatch.setattr(seg, "_doh_mx_for_domain", _explode)
    monkeypatch.setattr(seg, "contribute_to_map", _explode)
    monkeypatch.setattr(seg, "_get_http", _explode_sync)
    return calls


def _capture_person_body(payload: dict) -> dict:
    """Run write_enrichment_result; return the person body sent to _do_upsert."""
    bodies: list[dict] = []

    async def fake_do_upsert(client, body, pl, **kw):
        bodies.append(body)
        return cw.WriteStatus.INSERTED

    async def runner():
        with patch.object(cw, "_do_upsert", new=fake_do_upsert):
            await cw.write_enrichment_result(payload, job_id="seg-test", row_index=0)

    asyncio.run(runner())
    assert bodies, "no person upsert body was produced"
    return bodies[0]


# ---------------------------------------------------------------------------
# 1. Tuple contents + convention
# ---------------------------------------------------------------------------

class TestFirmographicTuple:
    def test_seg_tuples_present(self):
        assert ("seg_classification", "seg_classification") in cw._FIRMOGRAPHIC_CUSTOM_FIELDS
        assert ("seg_provider", "seg_provider") in cw._FIRMOGRAPHIC_CUSTOM_FIELDS

    def test_existing_entries_define_the_convention(self):
        """FIRST element = custom_fields key, SECOND = payload key (read off
        the pre-existing entries: 'headline' is the custom_fields key for
        payload key 'dm_headline'). The two seg entries are appended at the
        END so any position-sensitive reader of the tuple is unaffected."""
        entries = cw._FIRMOGRAPHIC_CUSTOM_FIELDS
        assert entries[0] == ("headline", "dm_headline")
        assert entries[-2] == ("seg_classification", "seg_classification")
        assert entries[-1] == ("seg_provider", "seg_provider")
        assert len(entries) == 10

    def test_seg_payload_keys_read_defensively(self):
        """The loop reads via payload.get(), so an absent key (the normal
        case while upstream wiring lands) yields no custom_fields entry —
        never a KeyError, never an empty string."""
        body = _capture_person_body({"dm_email": "a@nowhere.test"})
        assert body.get("custom_fields") in (None, {})

    def test_empty_string_never_lands_in_custom_fields(self):
        """Guard: an upstream '' seg value must NOT be written as ''."""
        body = _capture_person_body({
            "dm_email": "a@acme.com",
            "domain": "acme.com",
            "seg_classification": "",
            "seg_provider": "",
        })
        cf = body.get("custom_fields")
        if cf:
            assert "seg_classification" not in cf
            assert "seg_provider" not in cf
            assert "" not in cf.values()


# ---------------------------------------------------------------------------
# 2. Cache hit populates custom_fields
# ---------------------------------------------------------------------------

class TestSegCacheHit:
    def test_cache_hit_populates_custom_fields(self, seg_db, seg_enabled, no_network):
        body = _capture_person_body({"dm_email": "a@acme.com", "domain": "acme.com"})
        cf = body.get("custom_fields")
        assert isinstance(cf, dict)
        assert cf.get("seg_classification") == "external_seg"
        assert cf.get("seg_provider") == "SEG: Mimecast"
        # canonical person fields unchanged
        assert body["email"] == "a@acme.com"
        assert body["domain"] == "acme.com"

    def test_raw_domain_url_normalizes_to_cache_key(
        self, seg_db, seg_enabled, no_network
    ):
        """The writer's domain (https://www.acme.com/about) must resolve to
        the same normalized cache key seg.classify_domains would produce."""
        body = _capture_person_body({
            "dm_email": "b@acme.com",
            "domain": "https://www.acme.com/about",
        })
        cf = body.get("custom_fields")
        assert isinstance(cf, dict)
        assert cf.get("seg_classification") == "external_seg"
        assert cf.get("seg_provider") == "SEG: Mimecast"

    def test_domain_derived_from_email(self, seg_db, seg_enabled, no_network):
        """No explicit domain — the email's domain is used (the writer's
        existing fallback order: normalized_domain > domain > website >
        domain-from-email)."""
        body = _capture_person_body({"dm_email": "c@acme.com"})
        cf = body.get("custom_fields")
        assert isinstance(cf, dict)
        assert cf.get("seg_classification") == "external_seg"


# ---------------------------------------------------------------------------
# 3. Explicit wins
# ---------------------------------------------------------------------------

class TestSegExplicitWins:
    def test_explicit_payload_value_not_overwritten(
        self, seg_db, seg_enabled, no_network
    ):
        """Payload already carrying a verdict → cache value does NOT
        overwrite (explicit > derived, same precedence as lead_universe)."""
        body = _capture_person_body({
            "dm_email": "a@acme.com",
            "domain": "acme.com",
            "seg_classification": "direct_microsoft",
            "seg_provider": "Microsoft",
        })
        cf = body.get("custom_fields")
        assert cf.get("seg_classification") == "direct_microsoft"
        assert cf.get("seg_provider") == "Microsoft"

    def test_partial_explicit_value_preserved(
        self, seg_db, seg_enabled, no_network
    ):
        """Only one of the two keys explicit → neither is overwritten; the
        helper is all-or-nothing on 'already populated'."""
        body = _capture_person_body({
            "dm_email": "a@acme.com",
            "domain": "acme.com",
            "seg_classification": "direct_google",
        })
        cf = body.get("custom_fields")
        assert cf.get("seg_classification") == "direct_google"
        assert cf.get("seg_provider") in (None, "Google")


# ---------------------------------------------------------------------------
# 4. Miss / flag off / no domain
# ---------------------------------------------------------------------------

class TestSegMissAndFlagOff:
    def test_cache_miss_leaves_no_seg_keys(self, seg_db, seg_enabled, no_network):
        body = _capture_person_body({"dm_email": "a@uncached.test", "domain": "uncached.test"})
        cf = body.get("custom_fields")
        if cf is not None:
            assert "seg_classification" not in cf
            assert "seg_provider" not in cf
            assert "" not in cf.values()
        else:
            assert cf is None

    def test_flag_off_leaves_no_seg_keys_even_when_cached(
        self, seg_db, seg_disabled, no_network
    ):
        """Cache HAS the domain but the flag is off — no lookup happens."""
        body = _capture_person_body({"dm_email": "a@acme.com", "domain": "acme.com"})
        cf = body.get("custom_fields")
        if cf is not None:
            assert "seg_classification" not in cf
            assert "seg_provider" not in cf
        else:
            assert cf is None

    def test_undomainable_payload_leaves_no_seg_keys(
        self, seg_db, seg_enabled, no_network
    ):
        """No domain at all and no derivable email domain → no lookup, no keys."""
        body = _capture_person_body({"dm_email": "a@nowhere.test"})
        cf = body.get("custom_fields")
        if cf is not None:
            assert "seg_classification" not in cf
            assert "seg_provider" not in cf
        else:
            assert cf is None

    def test_lookup_failure_is_swallowed(self, seg_db, seg_enabled, monkeypatch):
        """A cache-read explosion must never break the write — seg's own
        helpers never raise, but the writer guards regardless."""
        def _explode(domains):
            raise RuntimeError("cache read exploded")

        monkeypatch.setattr(seg, "get_seg_for_domains_sync", _explode)
        body = _capture_person_body({"dm_email": "a@acme.com", "domain": "acme.com"})
        assert body["email"] == "a@acme.com"
        assert body.get("custom_fields") in (None, {})


# ---------------------------------------------------------------------------
# 5. No network from the writer path
# ---------------------------------------------------------------------------

class TestNoNetworkInWriterPath:
    def test_cache_hit_never_touches_network_layers(self, seg_db, seg_enabled, no_network):
        """The hit must come from domain_seg_cache, not classify_domains /
        DoH / the contacts-DB endpoint / the shared httpx client."""
        body = _capture_person_body({"dm_email": "a@acme.com", "domain": "acme.com"})
        assert no_network == []
        assert body.get("custom_fields", {}).get("seg_classification") == "external_seg"

    def test_cache_miss_never_touches_network_layers(self, seg_db, seg_enabled, no_network):
        body = _capture_person_body({"dm_email": "a@miss.test", "domain": "miss.test"})
        assert no_network == []
        assert body.get("custom_fields") in (None, {})

    def test_flag_off_never_touches_network_layers(
        self, seg_db, seg_disabled, no_network
    ):
        body = _capture_person_body({"dm_email": "a@acme.com", "domain": "acme.com"})
        assert no_network == []

    def test_sync_lookup_is_pure_cache_read(self, seg_db, seg_enabled, no_network):
        """get_seg_for_domains_sync itself must stay network-free (the API
        the writer path is allowed to call)."""
        assert seg.get_seg_for_domains_sync(["acme.com"]) == {
            "acme.com": {
                "seg_classification": "external_seg",
                "seg_provider": "SEG: Mimecast",
                "source": "contacts_db",
            }
        }
        assert seg.get_seg_for_domains_sync(["uncached.test"]) == {}
        assert no_network == []

    def test_negative_cache_rows_do_not_leak(self, seg_db, seg_enabled, no_network):
        """A '' (negative-cache) row must NOT surface as an empty-string
        custom_fields value."""
        conn = shared_db.get_db()
        conn.execute(
            "INSERT OR REPLACE INTO domain_seg_cache "
            "(domain, seg_classification, seg_provider, source, mx_hosts, fetched_at) "
            "VALUES ('fresh-miss.test', '', '', 'doh', NULL, datetime('now'))"
        )
        conn.commit()
        body = _capture_person_body({
            "dm_email": "a@fresh-miss.test", "domain": "fresh-miss.test",
        })
        cf = body.get("custom_fields")
        if cf is not None:
            assert "seg_classification" not in cf
            assert "seg_provider" not in cf
            assert "" not in cf.values()
        else:
            assert cf is None


# ---------------------------------------------------------------------------
# 6. Outbox path unchanged
# ---------------------------------------------------------------------------

class TestOutboxPreservesSegFields:
    def test_transient_failure_parks_row_with_seg_custom_fields(
        self, seg_db, seg_enabled, no_network
    ):
        """A network-level failure routes to contacts_write_outbox (QUEUED)
        with the seg custom_fields intact in payload_json — the outbox
        semantics (pending status, attempt_count=0, next_retry_at) are
        unchanged."""
        conn = shared_db.get_db()

        class _FailingClient:
            async def post(self, *a, **kw):
                raise RuntimeError("simulated network outage")

        async def runner():
            with patch.object(
                cw._contacts_client, "_acquire_upsert_rate_limit", new=AsyncMock()
            ):
                return await cw.write_enrichment_result(
                    {"dm_email": "a@acme.com", "domain": "acme.com"},
                    client=_FailingClient(),
                    job_id="seg-outbox-test",
                    row_index=7,
                )

        status = asyncio.run(runner())
        assert status == cw.WriteStatus.QUEUED

        rows = conn.execute(
            "SELECT job_id, row_index, payload_json, status, attempt_count "
            "FROM contacts_write_outbox WHERE job_id='seg-outbox-test'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["status"] == "pending"
        assert rows[0]["row_index"] == 7
        assert rows[0]["attempt_count"] == 0
        parked = json.loads(rows[0]["payload_json"])
        cf = parked.get("custom_fields")
        assert isinstance(cf, dict)
        assert cf.get("seg_classification") == "external_seg"
        assert cf.get("seg_provider") == "SEG: Mimecast"

    def test_batch_path_also_populates_seg(self, seg_db, seg_enabled, no_network):
        """write_enrichment_result_batch goes through the same decoration."""
        seen: list[str | None] = []

        async def fake_write(p, *, client=None, job_id=None, row_index=None):
            seen.append(p.get("seg_classification"))
            return cw.WriteStatus.INSERTED

        with patch.object(cw, "write_enrichment_result", new=fake_write):
            result = asyncio.run(
                cw.write_enrichment_result_batch(
                    [
                        {"dm_email": "a@acme.com", "domain": "acme.com"},
                        {"dm_email": "b@miss.test", "domain": "miss.test"},
                    ],
                    job_id="seg-batch-test",
                )
            )
        assert result.inserted == 2
        assert seen == ["external_seg", None]

    def test_batch_real_path_writes_seg_custom_fields(
        self, seg_db, seg_enabled, no_network
    ):
        """End-to-end through the real batch loop (only the HTTP layer is
        mocked): the person body carries the seg custom_fields."""
        bodies: list[dict] = []

        async def fake_do_upsert(client, body, pl, **kw):
            bodies.append(body)
            return cw.WriteStatus.SYNCED

        with patch.object(cw, "_do_upsert", new=fake_do_upsert):
            result = asyncio.run(
                cw.write_enrichment_result_batch(
                    [{"dm_email": "a@acme.com", "domain": "acme.com"}],
                    job_id="seg-batch-e2e",
                )
            )
        assert result.inserted == 1
        cf = bodies[0].get("custom_fields")
        assert cf.get("seg_classification") == "external_seg"
        assert cf.get("seg_provider") == "SEG: Mimecast"

    def test_company_email_path_unaffected(self, seg_db, seg_enabled, no_network):
        """The company-email upsert has no custom_fields block by design —
        the seg decoration must not add one or break that path."""
        bodies: list[dict] = []

        async def fake_do_upsert(client, body, pl, **kw):
            bodies.append(body)
            return cw.WriteStatus.SYNCED

        with patch.object(cw, "_do_upsert", new=fake_do_upsert):
            asyncio.run(
                cw.write_enrichment_result(
                    {
                        "dm_email": "",
                        "company_email": "info@acme.com",
                        "domain": "acme.com",
                    },
                    job_id="seg-company-test",
                    row_index=0,
                )
            )
        company_bodies = [b for b in bodies if b.get("is_company_email")]
        assert len(company_bodies) == 1
        assert "custom_fields" not in company_bodies[0]
        assert company_bodies[0]["domain"] == "acme.com"


if __name__ == "__main__":
    unittest.main()
