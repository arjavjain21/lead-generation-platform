"""
Configuration for BetterEnrich company / Facebook page email fallbacks.

These flags control when and how the company/page-level fallbacks are
used as a *secondary* tier after the person-level waterfall
(Contacts DB -> Blitz -> WizLeads -> BetterEnrich V3) has already
exhausted itself.

All flags are env-overridable. Defaults are conservative: the
fallbacks are OFF unless explicitly enabled.

Flags:
  ENABLE_COMPANY_EMAIL_FALLBACK
      When True, after the person-level cascade fails to find a
      decision-maker email, call BetterEnrich's find-company-email
      endpoint using the row's website/domain.

  ENABLE_FACEBOOK_EMAIL_FALLBACK
      When True, after the person-level cascade fails, AND the row
      carries a Facebook page URL, call BetterEnrich's
      find-email-from-facebook-page endpoint.

  ALLOW_GENERIC_COMPANY_EMAIL
      When True, generic prefix emails (info@, contact@, support@,
      hello@, sales@, admin@, office@) returned by the company/page
      fallbacks are acceptable for company_email_type='generic'.
      When False, generic emails are rejected (set company_email=""
      and emit no_email_reason='generic_company_email_rejected').

  ALLOW_COMPANY_EMAIL_AS_FINAL
      When True, the final_email column will be populated with the
      company/page email when no person-level email was found.
      When False (default), final_email remains blank unless a
      person-level email was found. The company_email column is
      always populated when a fallback email is found (subject to
      ALLOW_GENERIC_COMPANY_EMAIL).
"""

from __future__ import annotations

import os
from typing import Final


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "enabled")


# Whether the company-email fallback tier is enabled. Off by default
# so the existing person-only waterfall is unchanged out of the box.
ENABLE_COMPANY_EMAIL_FALLBACK: Final[bool] = _env_bool(
    "ENABLE_COMPANY_EMAIL_FALLBACK",
    default=False,
)

# Whether the Facebook page email fallback tier is enabled. Off by
# default. Requires a row that carries a facebook_url/page_url column.
ENABLE_FACEBOOK_EMAIL_FALLBACK: Final[bool] = _env_bool(
    "ENABLE_FACEBOOK_EMAIL_FALLBACK",
    default=False,
)

# Whether generic prefix emails (info@, contact@, etc.) are acceptable.
# When False, generic emails are kept in company_email_type='generic'
# but final_email is blank and no_email_reason='generic_company_email_rejected'.
ALLOW_GENERIC_COMPANY_EMAIL: Final[bool] = _env_bool(
    "ALLOW_GENERIC_COMPANY_EMAIL",
    default=False,
)

# Whether a company/page email can ever be assigned to final_email
# (i.e. the "best" email for this row). Off by default: final_email
# only carries a decision-maker email unless the operator explicitly
# allows company-level emails as a final answer.
ALLOW_COMPANY_EMAIL_AS_FINAL: Final[bool] = _env_bool(
    "ALLOW_COMPANY_EMAIL_AS_FINAL",
    default=False,
)


# The set of email local-parts treated as generic.
GENERIC_EMAIL_PREFIXES: Final[frozenset[str]] = frozenset({
    "info",
    "contact",
    "support",
    "hello",
    "sales",
    "admin",
    "office",
})


def is_generic_email(email: str) -> bool:
    """Return True if `email` is a generic prefix email.

    Strips whitespace, lowercases, and checks the local part against
    GENERIC_EMAIL_PREFIXES. Empty / unparsable emails return False.
    """
    if not email or "@" not in email:
        return False
    local = email.split("@", 1)[0].strip().lower()
    return local in GENERIC_EMAIL_PREFIXES
