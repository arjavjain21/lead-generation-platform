from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from .client import MoltSetsClient

DEFAULT_ACCEPTED_RISK_SCORES = ("A", "B", "C", "F")


@dataclass(slots=True)
class RecipeAttempt:
    tool: str
    status: str
    found: bool
    note: str | None = None


@dataclass(slots=True)
class EmailRecipeResult:
    found: bool
    accepted: bool
    email: str | None = None
    email_type: str | None = None
    risk_score: str | None = None
    linkedin_url: str | None = None
    source_tool: str | None = None
    path_used: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    attempts: list[RecipeAttempt] = field(default_factory=list)


@dataclass(slots=True)
class SearchProbe:
    total: int | None
    sample: list[dict[str, Any]]
    raw_results: Any


def _iter_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for key in ("results", "data", "person", "profile"):
            child = value.get(key)
            if isinstance(child, dict):
                yield from _iter_dicts(child)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, dict):
                        yield from _iter_dicts(item)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield from _iter_dicts(item)


def _extract_first(value: Any, keys: Sequence[str]) -> Any:
    for block in _iter_dicts(value):
        for key in keys:
            candidate = block.get(key)
            if candidate not in (None, "", [], {}):
                return candidate
    return None


def _extract_email(value: Any) -> tuple[str | None, str | None, str | None]:
    email = _extract_first(value, ("email", "business_email", "personal_email"))
    risk = _extract_first(
        value,
        (
            "risk_score",
            "business_email_risk_score",
            "personal_email_risk_score",
        ),
    )
    email_type = _extract_first(value, ("type", "email_type"))
    if not email_type and email:
        for block in _iter_dicts(value):
            if block.get("business_email") == email:
                email_type = "business"
                break
            if block.get("personal_email") == email:
                email_type = "personal"
                break
    return (
        str(email) if email else None,
        str(risk).upper() if risk else None,
        str(email_type) if email_type else None,
    )


def _accepted(risk_score: str | None, accepted_risk_scores: Sequence[str]) -> bool:
    if risk_score is None:
        return False
    allowed = {grade.upper() for grade in accepted_risk_scores}
    return risk_score.upper() in allowed


def probe_search_people(client: MoltSetsClient, **filters: Any) -> SearchProbe:
    """Probe a people search with limit=1 and return its advertised total.

    MoltSets support documentation states that search_people does not support
    count_only. The recommended cheap probe is therefore limit=1.
    """
    filters.pop("count_only", None)
    response = client.search_people(limit=1, offset=0, **filters)
    block = response.results if isinstance(response.results, dict) else {}
    rows = block.get("results", []) if isinstance(block, dict) else []
    total = block.get("total") if isinstance(block, dict) else None
    return SearchProbe(
        total=total if isinstance(total, int) else None,
        sample=[row for row in rows if isinstance(row, dict)],
        raw_results=response.results,
    )


def find_best_email(
    client: MoltSetsClient,
    *,
    linkedin_url: str | None = None,
    name: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    company: str | None = None,
    company_domain: str | None = None,
    accepted_risk_scores: Sequence[str] = DEFAULT_ACCEPTED_RISK_SCORES,
) -> EmailRecipeResult:
    """Resolve a contact's best available email using MoltSets-native fallbacks.

    This intentionally stops short of fabricated/pattern-guessed addresses.
    It only returns data confirmed by a MoltSets endpoint.
    """
    attempts: list[RecipeAttempt] = []

    def build_result(response: Any, tool: str, path: str, fallback_linkedin: str | None = None) -> EmailRecipeResult | None:
        email, risk, email_type = _extract_email(response.results)
        attempts.append(RecipeAttempt(tool=tool, status=response.status, found=bool(email)))
        if not email:
            return None
        resolved_linkedin = _extract_first(response.results, ("linkedin_url", "linkedin")) or fallback_linkedin
        return EmailRecipeResult(
            found=True,
            accepted=_accepted(risk, accepted_risk_scores),
            email=email,
            email_type=email_type,
            risk_score=risk,
            linkedin_url=str(resolved_linkedin) if resolved_linkedin else None,
            source_tool=tool,
            path_used=path,
            data=response.results if isinstance(response.results, dict) else {"results": response.results},
            attempts=list(attempts),
        )

    if linkedin_url:
        response = client.linkedin_to_best_email(linkedin_url=linkedin_url)
        result = build_result(response, "linkedin_to_best_email", "linkedin_direct", linkedin_url)
        if result:
            return result

        reverse = client.reverse_linkedin_lookup(linkedin_url=linkedin_url)
        result = build_result(reverse, "reverse_linkedin_lookup", "linkedin_reverse", linkedin_url)
        if result:
            return result

    if name or first_name or last_name:
        if company_domain or company:
            response = client.search_business_email_by_name(
                name=name,
                first_name=first_name,
                last_name=last_name,
                company=company,
                company_domain=company_domain,
            )
            result = build_result(response, "search_business_email_by_name", "name_company_direct")
            if result:
                return result

        profile = client.search_linkedin_profile(
            name=name,
            first_name=first_name,
            last_name=last_name,
            company=company,
            company_domain=company_domain,
        )
        result = build_result(profile, "search_linkedin_profile", "name_profile_search")
        if result:
            return result

        resolved_linkedin = _extract_first(profile.results, ("linkedin_url", "linkedin"))
        attempts.append(
            RecipeAttempt(
                tool="search_linkedin_profile",
                status=profile.status,
                found=bool(resolved_linkedin),
                note="resolved LinkedIn URL" if resolved_linkedin else None,
            )
        )
        if resolved_linkedin:
            enriched = client.linkedin_to_best_email(linkedin_url=str(resolved_linkedin))
            result = build_result(
                enriched,
                "linkedin_to_best_email",
                "name_profile_then_best_email",
                str(resolved_linkedin),
            )
            if result:
                return result

    return EmailRecipeResult(found=False, accepted=False, attempts=attempts)


def enrich_linkedin_profile(
    client: MoltSetsClient,
    linkedin_url: str,
    *,
    include_mobile: bool = True,
) -> dict[str, Any]:
    """Combine reverse LinkedIn enrichment with an optional verified mobile lookup."""
    profile = client.reverse_linkedin_lookup(linkedin_url=linkedin_url)
    result: dict[str, Any] = {
        "linkedin_url": linkedin_url,
        "profile_status": profile.status,
        "profile": profile.results,
    }
    if include_mobile:
        phone = client.linkedin_to_mobile_phone(linkedin_url=linkedin_url)
        result["mobile_status"] = phone.status
        result["mobile"] = phone.results
    return result


def chunked_mobile_lookup(
    client: MoltSetsClient,
    linkedin_urls: Sequence[str],
    *,
    batch_size: int = 100,
):
    """Yield native mobile batches. MoltSets documents a maximum of 100 URLs."""
    if batch_size < 1 or batch_size > 100:
        raise ValueError("batch_size must be between 1 and 100")
    for start in range(0, len(linkedin_urls), batch_size):
        batch = list(linkedin_urls[start : start + batch_size])
        yield client.linkedin_to_mobile_phone(linkedin_urls=batch)
