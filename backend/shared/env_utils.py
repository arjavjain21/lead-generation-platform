"""Env-var parsing helpers that never raise at import time.

Incident 2026-10-13: backend/.env carried an inline comment
(`BLITZ_RPS_PHONE=50  # plan limit 50/endpoint`) — systemd's EnvironmentFile
does NOT strip inline '#'-comments, so the raw value reached float() and
crash-looped every gunicorn worker on boot. Module-level
``float(os.getenv(...))`` sites are load-bearing boot code; a malformed value
must degrade to the default (or the salvaged prefix), never kill the worker.
"""

import logging
import os

logger = logging.getLogger(__name__)


def env_float(name: str, default) -> float:
    """Parse env var ``name`` as float; on malformed input salvage the part
    before any inline '#'-comment, else fall back to ``default`` (never raise).

    ``default`` may be a float or a numeric string.
    """
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return float(default)
    raw = raw.strip()
    try:
        return float(raw)
    except ValueError:
        pass
    candidate = raw.split("#", 1)[0].strip()
    try:
        return float(candidate)
    except ValueError:
        logger.warning(
            "env var %s=%r is not numeric — using default %s", name, raw, default
        )
        return float(default)
