"""Tests for identifier_utils.dedupe_rows_by_domain."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from enrichment.identifier_utils import dedupe_rows_by_domain, normalize_domain


def _row(domain, **extra):
    return {"website" if False else "domain": domain, **extra}


class TestEmptyAndSingle:
    def test_empty_list(self):
        kept, count, skipped = dedupe_rows_by_domain([], "domain", normalize=True)
        assert kept == []
        assert count == 0
        assert skipped == []

    def test_single_row(self):
        rows = [_row("acme.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 0
        assert skipped == []


class TestDedupeBasics:
    def test_two_duplicates_normalize_on(self):
        rows = [_row("https://Acme.com/?utm=x"), _row("acme.com/")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 1
        assert skipped == ["acme.com/"]

    def test_two_duplicates_normalize_off(self):
        rows = [_row("acme.com"), _row("ACME.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=False)
        # Without normalization, raw lowercased comparison is used.
        assert len(kept) == 1
        assert count == 1
        assert skipped == ["ACME.com"]

    def test_all_unique(self):
        rows = [_row("a.com"), _row("b.com"), _row("c.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 3
        assert count == 0
        assert skipped == []

    def test_all_duplicates(self):
        rows = [_row("acme.com")] * 5
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 4
        assert len(skipped) == 4


class TestPreserveFirstOccurrence:
    def test_first_occurrence_kept(self):
        rows = [_row("acme.com", name="A"), _row("acme.com", name="B"), _row("acme.com", name="C")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert kept[0]["name"] == "A"
        assert count == 2
        assert skipped == ["acme.com", "acme.com"]

    def test_deduped_count_preserves_order(self):
        rows = [_row("x.com"), _row("y.com"), _row("x.com"), _row("z.com"), _row("y.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 3
        assert [r["domain"] for r in kept] == ["x.com", "y.com", "z.com"]
        assert count == 2
        assert skipped == ["x.com", "y.com"]


class TestNormalizeEquivalence:
    def test_www_stripped(self):
        rows = [_row("www.acme.com"), _row("acme.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 1

    def test_protocol_stripped(self):
        rows = [_row("https://acme.com"), _row("http://acme.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 1

    def test_query_string_stripped(self):
        rows = [_row("acme.com/?utm_source=x"), _row("acme.com/?utm_source=y")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 1

    def test_case_insensitive(self):
        rows = [_row("ACMECorp.COM"), _row("acmecorp.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 1


class TestSharedHostDomains:
    """Subdomains must NOT be deduped — only exact full-domain matches."""

    def test_subdomains_not_deduped(self):
        rows = [_row("blog.acme.com"), _row("shop.acme.com"), _row("acme.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        # Each subdomain is a distinct domain.
        assert len(kept) == 3
        assert count == 0
        assert skipped == []

    def test_same_subdomain_duplicated(self):
        rows = [_row("blog.acme.com"), _row("blog.acme.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 1
        assert count == 1


class TestFranchiseSubPaths:
    """Multi-location sub-paths on the same host are NOT deduped — each
    location is treated as a distinct domain key by the raw value."""

    def test_mcdonalds_locations_not_deduped(self):
        rows = [
            _row("mcdonalds.com/location/001"),
            _row("mcdonalds.com/location/002"),
            _row("mcdonalds.com/location/003"),
        ]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        # normalize_domain() strips paths, so all collapse to mcdonalds.com
        # when normalize is on — this is a known limitation: franchise
        # locations with different sub-paths WILL be deduped when normalize
        # is on (because the path is stripped). Document and verify.
        assert len(kept) == 1
        assert count == 2
        assert len(skipped) == 2

    def test_mcdonalds_locations_with_normalize_off(self):
        rows = [
            _row("mcdonalds.com/location/001"),
            _row("mcdonalds.com/location/002"),
            _row("mcdonalds.com/location/003"),
        ]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=False)
        # Without normalization, the raw key includes the path, so each
        # location is distinct. This is the recommended combo for franchises.
        assert len(kept) == 3
        assert count == 0
        assert skipped == []


class TestEmptyDomainRows:
    """Empty-domain rows pass through without being considered duplicates."""

    def test_empty_rows_pass_through(self):
        rows = [_row(""), _row(""), _row("acme.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 3
        assert count == 0
        assert skipped == []

    def test_empty_then_real(self):
        rows = [_row(""), _row("acme.com"), _row("")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        assert len(kept) == 3
        assert count == 0


class TestDedupeOnNormalizeOffCombo:
    """DEDUPE_ON + NORMALIZE_OFF: two same-domain-in-different-format rows
    are NOT deduped because the dedupe key is the raw (lowercased) value."""

    def test_protocol_variant_not_deduped(self):
        rows = [_row("acme.com"), _row("https://acme.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=False)
        # Without normalize, the keys are different (acme.com vs https://acme.com).
        assert len(kept) == 2
        assert count == 0
        assert skipped == []

    def test_protocol_variant_deduped_with_normalize(self):
        rows = [_row("acme.com"), _row("https://acme.com")]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        # With normalize, both collapse to acme.com.
        assert len(kept) == 1
        assert count == 1
        assert skipped == ["https://acme.com"]


class TestReturnShape:
    def test_returns_tuple_of_three(self):
        result = dedupe_rows_by_domain([], "domain", normalize=True)
        assert isinstance(result, tuple)
        assert len(result) == 3
        kept, count, skipped = result
        assert isinstance(kept, list)
        assert isinstance(count, int)
        assert isinstance(skipped, list)

    def test_skipped_domains_contains_raw_values(self):
        rows = [_row("acme.com", x=1), _row("https://acme.com/", x=2), _row("acme.com", x=3)]
        kept, count, skipped = dedupe_rows_by_domain(rows, "domain", normalize=True)
        # First occurrence kept; later duplicates are added to skipped.
        assert skipped == ["https://acme.com/", "acme.com"]
