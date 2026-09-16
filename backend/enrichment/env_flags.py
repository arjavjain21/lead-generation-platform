"""Single boolean env-flag parser for the enrichment package.

Before this module the flag vocabulary was inconsistent across (and even
within) files: ``list_builder._env_flag`` treated ``off`` as false while the
inline gates in ``pipeline.py`` accepted only ``false``/``0``/``no`` — so
``ENABLE_PHONE_BUNDLE=off`` silently meant ON in the pipeline. One helper,
one vocabulary, everywhere.

Semantics (identical for every previously-valid value; ``off`` now works
everywhere):

- unset or blank           -> ``default``
- true / 1 / yes / on      -> True   (case-insensitive, surrounding space ok)
- false / 0 / no / off     -> False
- anything else (garbage)  -> ``default``

Garbage falls back to the default rather than True so a future flag whose
default is False cannot be accidentally armed by a typo.
"""

from __future__ import annotations

import os

_TRUE_VALUES = frozenset({"true", "1", "yes", "on"})
_FALSE_VALUES = frozenset({"false", "0", "no", "off"})


def env_flag(name: str, default: bool = True) -> bool:
    """Resolve boolean env var ``name`` with a permissive on/off vocabulary.

    Args:
        name: environment variable name (e.g. ``ENABLE_PHONE_BUNDLE``).
        default: value used when the variable is unset, blank, or holds an
            unrecognized string. All current call sites rely on the
            kill-switch pattern (default True, explicit off values only).
    """
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    return default
