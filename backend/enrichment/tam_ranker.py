"""Lookalike 2.0 ranker — deterministic query fan-out + similarity scoring.

No LLM, no embeddings, no new services: given the resolved seed profiles and
the synthesized query plan, this module decides WHICH retrieval queries to
run (one per seed industry + one keywords-only) and, once candidates are in,
scores every row against the seeds (text similarity, industry match, size
proximity, niche-keyword hits) so the export is ranked best-match-first with
a human-readable ``why_matched`` explanation.

Pure functions only — all I/O lives in ``tam_flow``.
"""

from __future__ import annotations

from typing import Any

from enrichment import seed_text
from enrichment.lookalike import SIZE_BANDS, _band_index

# Signal weights (sum to 1.0). Score = 100 * weighted sum, rounded to int.
WEIGHT_TEXT = 0.5
WEIGHT_INDUSTRY = 0.2
WEIGHT_SIZE = 0.15
WEIGHT_KEYWORDS = 0.15

# Fan-out shape: max niche keywords forwarded to a single Blitz query and the
# cap on keywords shown in one ``why_matched`` explanation.
BLITZ_KEYWORD_QUERY_TOP = 4
WHY_KEYWORDS_SHOWN = 3


# ---------------------------------------------------------------------------
# Query variants (fan-out retrieval planning)
# ---------------------------------------------------------------------------

def build_blitz_variants(
    base_company_filters: dict[str, Any], plan: dict[str, Any]
) -> list[tuple[str, dict[str, Any]]]:
    """One Blitz company-filter variant per plan industry, plus one
    keywords-only variant — the fan-out that replaces the single
    majority-industry query (fixes the 'different industries' seed failure).

    Each variant keeps the SHARED base filters (size band, HQ, name, type,
    ...) and overrides only ``industry``; the keywords variant drops
    ``industry`` and sets ``keywords.include`` to the top plan keywords.
    Returns ``[]`` when the plan carries neither industries nor keywords (the
    caller then falls back to the single base query).
    """
    base = dict(base_company_filters or {})
    shared = {k: v for k, v in base.items() if k not in ("industry", "keywords")}
    industries = [i for i in (plan or {}).get("industries") or [] if i]
    keywords = [k for k in (plan or {}).get("keywords") or [] if k]
    variants: list[tuple[str, dict[str, Any]]] = []
    for industry in industries:
        variants.append((
            f"industry={industry}",
            {**shared, "industry": {"include": [industry]}},
        ))
    if keywords:
        top_keywords = keywords[:BLITZ_KEYWORD_QUERY_TOP]
        variants.append((
            "keywords=" + "|".join(top_keywords),
            {**shared, "keywords": {"include": top_keywords}},
        ))
    return variants


def build_getleads_variants(
    base_getleads_filters: dict[str, Any], plan: dict[str, Any]
) -> list[tuple[str, dict[str, Any]]]:
    """GetLeads leg of the same plan. GetLeads contact search has NO keyword
    filter surface, so only the per-industry variants are meaningful; when
    the plan is keywords-only the base filters stand unchanged (ranked
    scoring sorts the noise out afterwards)."""
    base = dict(base_getleads_filters or {})
    industries = [i for i in (plan or {}).get("industries") or [] if i]
    if not industries:
        return [("getleads=base", base)] if base else []
    shared = {k: v for k, v in base.items() if k != "industries"}
    return [
        (f"getleads industry={industry}", {**shared, "industries": [industry]})
        for industry in industries
    ]


def split_fanout_budget(
    total_budget: int, remaining_variants: int, last_takes_rest: bool = True
) -> int:
    """Balanced per-query budget: each variant gets an equal integer share of
    what remains, so query 1 can never starve the industries behind it (a
    literal min(cap, remaining) let the first industry eat a 25-company
    budget whole). The final variant takes everything left."""
    if remaining_variants <= 0 or total_budget <= 0:
        return 0
    if last_takes_rest and remaining_variants == 1:
        return total_budget
    return max(1, total_budget // remaining_variants)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _seed_context(
    rank_seeds: list[dict[str, Any]], plan: dict[str, Any]
) -> dict[str, Any]:
    """Precompute the scoring context from the seed profiles."""
    texts = [
        " ".join(
            str(part) for part in (
                s.get("text"), s.get("name"), s.get("industry"),
                s.get("domain"),
            ) if part
        )
        for s in rank_seeds or []
    ]
    seed_industries = {
        s.get("industry") for s in (rank_seeds or []) if s.get("industry")
    }
    band_idxs = sorted(
        i for i in (_band_index(s.get("size_band")) for s in (rank_seeds or []))
        if i is not None
    )
    median_band_idx = (
        band_idxs[len(band_idxs) // 2] if band_idxs else None
    )
    return {
        "combined_text": " ".join(texts),
        "seed_industries": seed_industries,
        "median_band_idx": median_band_idx,
        "keywords": [k for k in (plan or {}).get("keywords") or [] if k],
    }


def _candidate_text(row: dict[str, Any]) -> str:
    """Rows carry no description column — assemble what exists."""
    return " ".join(
        str(part) for part in (
            row.get("name"), row.get("industry"),
            (row.get("domain") or "").replace(".", " ") if row.get("domain") else None,
            row.get("slogan"),
        ) if part
    )


def _keyword_hits(candidate: str, keywords: list[str]) -> list[str]:
    lowered = candidate.lower()
    return [kw for kw in keywords if kw and kw.lower() in lowered]


def score_row(
    row: dict[str, Any], context: dict[str, Any]
) -> tuple[int, str]:
    """Score one candidate row: 0-100 int + ' · '-joined why_matched."""
    candidate = _candidate_text(row)
    text_sim = seed_text.trigram_similarity(
        candidate, context["combined_text"]
    )
    industry_match = 1.0 if row.get("industry") in context["seed_industries"] else 0.0
    row_band_idx = _band_index(row.get("size"))
    median_idx = context["median_band_idx"]
    band_span = max(len(SIZE_BANDS) - 1, 1)
    size_dist = (
        abs(row_band_idx - median_idx)
        if row_band_idx is not None and median_idx is not None else None
    )
    size_prox = 0.0 if size_dist is None else max(0.0, 1.0 - size_dist / band_span)
    hits = _keyword_hits(candidate, context["keywords"])
    keyword_score = (len(hits) / len(context["keywords"])) if context["keywords"] else 0.0

    raw = (
        WEIGHT_TEXT * text_sim
        + WEIGHT_INDUSTRY * industry_match
        + WEIGHT_SIZE * size_prox
        + WEIGHT_KEYWORDS * keyword_score
    )
    score = max(0, min(100, round(100 * raw)))

    signals: list[str] = []
    if hits:
        signals.append(" ".join(hits[:WHY_KEYWORDS_SHOWN]))
    if industry_match:
        signals.append("industry match")
    if size_dist is not None and size_dist <= 1:
        signals.append(f"size {row.get('size')}")
    why = " · ".join(signals) if signals else "broad match"
    return score, why


def rank_rows(
    rows: list[dict[str, Any]],
    rank_seeds: list[dict[str, Any]],
    plan: dict[str, Any],
) -> list[dict[str, Any]]:
    """Annotate every row with ``match_score`` / ``why_matched`` and return a
    NEW list sorted best-match first (score desc, name asc for determinism).

    Input rows are never mutated: each output row is a shallow copy carrying
    the two ranking columns that ``TAM_CSV_COLUMNS`` appends.
    """
    context = _seed_context(rank_seeds, plan)
    scored = []
    for row in rows or []:
        score, why = score_row(row, context)
        scored.append({**row, "match_score": score, "why_matched": why})
    scored.sort(key=lambda r: (-(r.get("match_score") or 0), str(r.get("name") or "")))
    return scored
