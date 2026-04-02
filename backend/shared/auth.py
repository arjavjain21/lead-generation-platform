"""
Authentication module: user management, password hashing, JWT tokens.

Users are created by an admin via the create_user.py CLI — no public registration.
Login is email + password → JWT (HS256, 7-day expiry).

Schema
------
users
  user_id       TEXT PRIMARY KEY  (UUID)
  email         TEXT UNIQUE NOT NULL  (always lowercase)
  password_hash TEXT NOT NULL  (bcrypt)
  is_admin      INTEGER NOT NULL DEFAULT 0
  created_at    TEXT NOT NULL  (ISO-8601 UTC)

JWT payload: { user_id, email, is_admin, exp }
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import bcrypt
import jwt
from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "jobs.db"

_local = threading.local()

JWT_ALGORITHM = "HS256"
JWT_EXPIRY_DAYS = 7


def _conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return _local.conn


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_auth_db() -> None:
    """Create auth tables if they don't exist. Safe to call multiple times."""
    c = _conn()
    c.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id       TEXT PRIMARY KEY,
            email         TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin      INTEGER NOT NULL DEFAULT 0,
            created_at    TEXT NOT NULL
        );
        """
    )
    c.commit()


# ---------------------------------------------------------------------------
# User CRUD
# ---------------------------------------------------------------------------

def create_user(email: str, password: str, is_admin: bool = False) -> dict[str, Any]:
    email = email.strip().lower()
    if not email or "@" not in email:
        raise ValueError("Invalid email address.")
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters.")

    password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    user_id = str(uuid.uuid4())
    now = _now()

    try:
        _conn().execute(
            "INSERT INTO users (user_id, email, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, email, password_hash, int(is_admin), now),
        )
        _conn().commit()
    except sqlite3.IntegrityError:
        raise ValueError(f"A user with email '{email}' already exists.")

    return {"user_id": user_id, "email": email, "is_admin": is_admin, "created_at": now}


def get_user_by_email(email: str) -> Optional[dict[str, Any]]:
    email = email.strip().lower()
    row = _conn().execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: str) -> Optional[dict[str, Any]]:
    row = _conn().execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
    return dict(row) if row else None


def list_users() -> list[dict[str, Any]]:
    rows = _conn().execute(
        "SELECT user_id, email, is_admin, created_at FROM users ORDER BY created_at DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def update_password(email: str, new_password: str) -> None:
    if len(new_password) < 8:
        raise ValueError("Password must be at least 8 characters.")
    email = email.strip().lower()
    password_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    _conn().execute(
        "UPDATE users SET password_hash = ? WHERE email = ?",
        (password_hash, email),
    )
    _conn().commit()


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------

def _jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET", "")
    if not secret:
        raise RuntimeError("JWT_SECRET environment variable is not set.")
    return secret


def create_token(user: dict[str, Any]) -> str:
    payload = {
        "user_id": user["user_id"],
        "email": user["email"],
        "is_admin": bool(user.get("is_admin", 0)),
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRY_DAYS),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(token, _jwt_secret(), algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired. Please log in again.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token.",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)


def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> dict[str, Any]:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return decode_token(credentials.credentials)


def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> Optional[dict[str, Any]]:
    """Like get_current_user but returns None instead of raising 401 if no credentials."""
    if credentials is None:
        return None
    try:
        return decode_token(credentials.credentials)
    except HTTPException:
        return None


def require_admin(
    current_user: dict[str, Any] = Depends(get_current_user),
) -> dict[str, Any]:
    if not current_user.get("is_admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )
    return current_user


# ---------------------------------------------------------------------------
# Combined JWT + API Key Authentication
# ---------------------------------------------------------------------------

async def get_current_user_from_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    Authenticate using either JWT token or API key.

    Checks X-API-Key header first, then falls back to Bearer token.
    """
    # Try API key first
    if x_api_key:
        user = verify_api_key(x_api_key)
        if user:
            return user

    # If no API key, will be handled by get_current_user dependency
    # This requires either a valid JWT or raises 401
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required. Provide either JWT token or X-API-Key header.",
        headers={"WWW-Authenticate": "Bearer", "X-API-Key-Optional": "true"},
    )


def get_current_user_with_api_key(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    Authenticate using either JWT token or API key.

    This is the main authentication dependency that should be used for protected endpoints.
    """
    # Try API key first
    if x_api_key:
        user = verify_api_key(x_api_key)
        if user:
            return user

    # Try JWT token
    if credentials:
        return decode_token(credentials.credentials)

    # No valid authentication
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required. Provide either JWT token (Authorization: Bearer) or X-API-Key header.",
        headers={"WWW-Authenticate": "Bearer"},
    )


# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------

import secrets
import hashlib


def _generate_api_key() -> str:
    """Generate a secure random API key."""
    return f"lgp_{secrets.token_urlsafe(32)}"


def _hash_api_key(key: str) -> str:
    """Hash an API key using SHA256 for storage."""
    return hashlib.sha256(key.encode()).hexdigest()


def create_api_key(user_id: str, name: str) -> dict[str, Any]:
    """
    Create a new API key for a user.

    Returns the full API key along with key metadata.
    """
    key_id = str(uuid.uuid4())
    api_key = _generate_api_key()
    key_hash = _hash_api_key(api_key)
    now = _now()

    _conn().execute(
        "INSERT INTO api_keys (key_id, user_id, key_hash, key_plain, name, created_at, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (key_id, user_id, key_hash, api_key, name, now, 1),
    )
    _conn().commit()

    return {
        "key_id": key_id,
        "api_key": api_key,
        "name": name,
        "created_at": now,
    }


def get_api_keys(user_id: str, include_key: bool = False) -> list[dict[str, Any]]:
    """
    Get all API keys for a user.
    By default, doesn't include the plaintext key. Set include_key=True to include it.
    """
    if include_key:
        rows = _conn().execute(
            "SELECT key_id, key_plain, name, created_at, last_used_at, is_active FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            d["api_key"] = d.pop("key_plain")
            result.append(d)
        return result
    else:
        rows = _conn().execute(
            "SELECT key_id, name, created_at, last_used_at, is_active FROM api_keys WHERE user_id = ? ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def delete_api_key(key_id: str, user_id: str) -> bool:
    """Delete (revoke) an API key. Returns True if deleted, False if not found."""
    cursor = _conn().execute(
        "DELETE FROM api_keys WHERE key_id = ? AND user_id = ?",
        (key_id, user_id),
    )
    _conn().commit()
    return cursor.rowcount > 0


def verify_api_key(api_key: str) -> Optional[dict[str, Any]]:
    """
    Verify an API key and return the user if valid.

    Also updates the last_used_at timestamp.
    """
    key_hash = _hash_api_key(api_key)

    row = _conn().execute(
        "SELECT key_id, user_id, name, is_active FROM api_keys WHERE key_hash = ? AND is_active = 1",
        (key_hash,),
    ).fetchone()

    if not row:
        return None

    # Update last_used_at
    _conn().execute(
        "UPDATE api_keys SET last_used_at = ? WHERE key_id = ?",
        (_now(), row["key_id"]),
    )
    _conn().commit()

    # Get user info
    user = get_user_by_id(row["user_id"])
    if not user:
        return None

    return {
        "user_id": user["user_id"],
        "email": user["email"],
        "is_admin": bool(user.get("is_admin", 0)),
        "key_id": row["key_id"],
        "key_name": row["name"],
    }


def get_api_key_by_id(key_id: str, user_id: str) -> Optional[dict[str, Any]]:
    """Get a specific API key metadata by ID."""
    row = _conn().execute(
        "SELECT key_id, name, created_at, last_used_at, is_active FROM api_keys WHERE key_id = ? AND user_id = ?",
        (key_id, user_id),
    ).fetchone()
    return dict(row) if row else None
