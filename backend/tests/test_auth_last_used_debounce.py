"""Debounce tests for shared.auth.verify_api_key last_used_at writes.

Context (RCA 2026-08-24): verify_api_key performed an UPDATE + COMMIT on
every successful verification. On the MCP auth path that write ran on the
event loop, and a contended SQLite write lock (busy_timeout=30s) stalled
MCP requests for the full 30s — the exact 30000ms timeout the client
reported. The write is now debounced to once per
_LAST_USED_WRITE_INTERVAL per key. last_used_at is display-only
telemetry (frontend date label; never sorted, filtered, or expired on),
so skipping intermediate writes cannot change any functional outcome.
"""

from unittest.mock import patch

import pytest

import shared.auth as auth


@pytest.fixture()
def temp_auth_db(tmp_path, monkeypatch):
    """Fresh SQLite auth DB + reset thread-local conn and debounce state."""
    db_path = tmp_path / "auth_debounce_test.db"
    monkeypatch.setattr(auth, "DB_PATH", db_path)
    # Force _conn() to reopen against the temp path for this thread.
    monkeypatch.setattr(auth._local, "conn", None, raising=False)
    auth._last_used_write.clear()
    auth.init_auth_db()
    # init_auth_db only creates `users`; api_keys DDL lives in shared/db.py.
    auth._conn().execute(
        """
        CREATE TABLE IF NOT EXISTS api_keys (
            key_id        TEXT PRIMARY KEY,
            user_id       TEXT NOT NULL,
            key_hash      TEXT NOT NULL,
            name          TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            last_used_at  TEXT,
            is_active     INTEGER NOT NULL DEFAULT 1,
            key_plain     TEXT
        )
        """
    )
    auth._conn().execute(
        "INSERT INTO users (user_id, email, password_hash, is_admin, created_at) "
        "VALUES ('u1', 'debounce@test.local', 'x', 0, '2026-01-01T00:00:00+00:00')"
    )
    auth._conn().commit()
    yield db_path
    # Close the temp connection; next user of this thread reopens cleanly.
    conn = getattr(auth._local, "conn", None)
    if conn is not None:
        conn.close()
        auth._local.conn = None
    auth._last_used_write.clear()


def _set_sentinel(key_id):
    """Forge a sentinel so any subsequent write is detectable."""
    auth._conn().execute(
        "UPDATE api_keys SET last_used_at = 'SENTINEL' WHERE key_id = ?",
        (key_id,),
    )
    auth._conn().commit()


def _last_used(key_id):
    row = auth._conn().execute(
        "SELECT last_used_at FROM api_keys WHERE key_id = ?", (key_id,)
    ).fetchone()
    return row["last_used_at"]


def test_first_use_writes_last_used(temp_auth_db):
    created = auth.create_api_key("u1", "test-key")
    with patch("shared.auth.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        user = auth.verify_api_key(created["api_key"])

    assert user is not None
    assert user["key_id"] == created["key_id"]
    assert _last_used(created["key_id"]) not in (None, "SENTINEL")


def test_second_use_within_window_skips_write(temp_auth_db):
    created = auth.create_api_key("u1", "test-key")
    with patch("shared.auth.time") as mock_time:
        # One monotonic() call per verify: t=1000.0 writes, t=1000.5 skips.
        mock_time.monotonic.side_effect = [1000.0, 1000.5]
        first = auth.verify_api_key(created["api_key"])
        _set_sentinel(created["key_id"])
        second = auth.verify_api_key(created["api_key"])

    assert first is not None and second is not None
    assert _last_used(created["key_id"]) == "SENTINEL"


def test_use_after_window_expires_writes_again(temp_auth_db):
    created = auth.create_api_key("u1", "test-key")
    with patch("shared.auth.time") as mock_time:
        mock_time.monotonic.side_effect = [1000.0, 1000.5, 1100.0]
        auth.verify_api_key(created["api_key"])   # writes
        _set_sentinel(created["key_id"])
        auth.verify_api_key(created["api_key"])   # within window → skip
        auth.verify_api_key(created["api_key"])   # past window → writes

    assert _last_used(created["key_id"]) != "SENTINEL"


def test_return_value_shape_unchanged(temp_auth_db):
    created = auth.create_api_key("u1", "test-key")
    with patch("shared.auth.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        user = auth.verify_api_key(created["api_key"])

    assert user is not None
    assert set(user.keys()) == {"user_id", "email", "is_admin", "key_id", "key_name"}
    assert user["user_id"] == "u1"
    assert user["email"] == "debounce@test.local"
    assert user["is_admin"] is False
    assert user["key_name"] == "test-key"


def test_bogus_key_returns_none_without_write(temp_auth_db):
    created = auth.create_api_key("u1", "test-key")
    _set_sentinel(created["key_id"])

    with patch("shared.auth.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        assert auth.verify_api_key("lgp_totally_bogus") is None

    assert _last_used(created["key_id"]) == "SENTINEL"


def test_telemetry_write_failure_does_not_fail_auth(temp_auth_db):
    """A locked/failed UPDATE must not 401 the request nor suppress retry."""
    import sqlite3 as _sqlite3

    created = auth.create_api_key("u1", "test-key")
    real_conn = auth._conn()

    def failing_conn():
        # Telemetry write sees a locked DB; the auth SELECT path never
        # calls this because _touch_last_used is the only consumer patched.
        raise _sqlite3.OperationalError("database is locked")

    with patch("shared.auth.time") as mock_time:
        mock_time.monotonic.return_value = 1000.0
        # Inject failure only into _touch_last_used's DB access: swap the
        # connection the helper uses AFTER the SELECT has already run.
        original_touch = auth._touch_last_used

        def touch_and_fail(key_id):
            with patch.object(auth, "_conn", side_effect=failing_conn):
                original_touch(key_id)

        with patch.object(auth, "_touch_last_used", touch_and_fail):
            user = auth.verify_api_key(created["api_key"])  # must not raise

    assert user is not None  # auth succeeded despite telemetry failure
    assert created["key_id"] not in auth._last_used_write  # marker not recorded

    # Retry on the next call succeeds (not suppressed for a full interval).
    with patch("shared.auth.time") as mock_time:
        mock_time.monotonic.return_value = 1000.5
        assert auth.verify_api_key(created["api_key"]) is not None

    assert _last_used(created["key_id"]) not in (None, "SENTINEL")
    assert real_conn  # silence linter; connection kept open by fixture
