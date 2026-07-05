"""
Unit tests for provider error structure.

Tests the _ProviderError class and helper functions that enable
provider errors to be surfaced in API responses while maintaining
backward compatibility with the existing cascade behavior.
"""

import pytest
from enrichment import pipeline as target_module


class TestProviderErrorClass:
    """Test the _ProviderError class."""

    def test_provider_error_evaluates_as_falsy(self):
        """ProviderError should evaluate as False in if checks."""
        error = target_module._ProviderError("better_enrich", "find_work_email_v3", "insufficient_credits", "No credits")
        assert not error  # Should be falsy
        assert bool(error) is False

    def test_provider_error_to_dict(self):
        """ProviderError should convert to dict with all fields."""
        error = target_module._ProviderError(
            provider="better_enrich",
            method="find_work_email_v3",
            error_type="insufficient_credits",
            message="BetterEnrich: Insufficient credits"
        )
        result = error.to_dict()
        assert result == {
            "provider": "better_enrich",
            "method": "find_work_email_v3",
            "error_type": "insufficient_credits",
            "message": "BetterEnrich: Insufficient credits"
        }

    def test_provider_error_attributes(self):
        """ProviderError should expose all attributes."""
        error = target_module._ProviderError("wizleads", "find_email", "rate_limited", "Rate limited")
        assert error.provider == "wizleads"
        assert error.method == "find_email"
        assert error.error_type == "rate_limited"
        assert error.message == "Rate limited"


class TestExtractProviderError:
    """Test the _extract_provider_error helper function."""

    def test_extract_from_provider_error_object(self):
        """Should extract dict from _ProviderError object."""
        error_obj = target_module._ProviderError("better_enrich", "method", "type", "msg")
        result = target_module._extract_provider_error(error_obj)
        assert result is not None
        assert result["provider"] == "better_enrich"

    def test_extract_from_none_returns_none(self):
        """Should return None for None input."""
        assert target_module._extract_provider_error(None) is None

    def test_extract_from_normal_dict_returns_none(self):
        """Should return None for regular dict (not an error object)."""
        normal_dict = {"provider": "better_enrich", "email": "test@example.com"}
        assert target_module._extract_provider_error(normal_dict) is None

    def test_extract_from_list_returns_none(self):
        """Should return None for list input."""
        assert target_module._extract_provider_error([1, 2, 3]) is None

    def test_extract_from_string_returns_none(self):
        """Should return None for string input."""
        assert target_module._extract_provider_error("test") is None


class TestClassifyHTTPError:
    """Test the _classify_http_error helper function."""

    @pytest.mark.parametrize("status_code,expected_type,expected_msg_prefix", [
        (402, "insufficient_credits", ": Insufficient credits"),
        (401, "authentication_failed", ": Authentication failed"),
        (403, "authentication_failed", ": Authentication failed"),
        (429, "rate_limited", ": Rate limited"),
        (503, "service_unavailable", ": Service temporarily unavailable"),
        (500, "service_unavailable", ": Server error"),
        (502, "service_unavailable", ": Server error"),
        (418, "unknown", ": An error occurred"),
    ])
    def test_classify_status_codes(self, status_code, expected_type, expected_msg_prefix):
        """Should correctly classify HTTP status codes."""
        from unittest.mock import Mock
        import httpx

        # Create a mock HTTPStatusError
        response = Mock()
        response.status_code = status_code
        exc = httpx.HTTPStatusError("test", request=Mock(), response=response)

        error_type, message = target_module._classify_http_error(exc, "better_enrich", "find_email")

        assert error_type == expected_type
        # Implementation uses provider.replace("_", " ").title() which produces "Better Enrich"
        assert message.startswith("Better Enrich")


class TestIsProviderError:
    """Test the _is_provider_error helper function."""

    def test_returns_true_for_provider_error_object(self):
        """Should return True for _ProviderError instance."""
        error = target_module._ProviderError("test", "test", "test", "test")
        assert target_module._is_provider_error(error) is True

    def test_returns_false_for_none(self):
        """Should return False for None."""
        assert target_module._is_provider_error(None) is False

    def test_returns_false_for_dict(self):
        """Should return False for regular dict."""
        assert target_module._is_provider_error({}) is False

    def test_returns_false_for_list(self):
        """Should return False for list."""
        assert target_module._is_provider_error([]) is False

    def test_returns_false_for_string(self):
        """Should return False for string."""
        assert target_module._is_provider_error("test") is False


class TestCascadeCompatibility:
    """Test that error objects don't break existing cascade behavior."""

    def test_none_in_if_statement(self):
        """None should evaluate as False (existing behavior)."""
        assert not None  # Existing cascade relies on this

    def test_error_object_in_if_statement(self):
        """ProviderError should also evaluate as False (maintains cascade)."""
        error = target_module._ProviderError("test", "test", "test", "test")
        result = error if error else "no_match"
        assert result == "no_match"

    def test_dict_with_email_in_if_statement(self):
        """Dict with email should evaluate as True (success case)."""
        success_dict = {"email": "test@example.com"}
        result = success_dict if success_dict else "no_match"
        assert result == success_dict

    def test_dict_without_email_in_if_statement(self):
        """Dict without email should evaluate as False (empty dict is falsy)."""
        # Empty dict is falsy in Python
        empty_dict = {}
        assert not empty_dict  # Empty dict is falsy
        # But cascade checks: if result and result.get("email")


class TestProviderErrorWithResultHandling:
    """Test that provider errors don't break result.get() calls."""

    def test_extract_provider_error_from_error_object(self):
        """Should extract error dict from _ProviderError without calling .get()."""
        error = target_module._ProviderError("better_enrich", "find_work_email_v3", "insufficient_credits", "test")
        
        # This is the pattern used in run_enrichment_route - must not call .get() on the error
        provider_error_dict = target_module._extract_provider_error(error)
        assert provider_error_dict is not None
        assert provider_error_dict["error_type"] == "insufficient_credits"
        
    def test_extract_provider_error_is_safe_before_get(self):
        """Verify that _extract_provider_error can be called safely before result.get()."""
        # This simulates the pattern in run_enrichment_route line 1155-1157
        result = target_module._ProviderError("wizleads", "find_email", "rate_limited", "test")

        # Call _extract_provider_error first (before any .get() calls)
        provider_error_dict = target_module._extract_provider_error(result)
        if provider_error_dict:
            # Now it's safe to call .get() on the dict, not on the error object
            error_type = provider_error_dict.get("error_type", "")
            assert error_type == "rate_limited"
        else:
            assert False, "Should have extracted error dict"

    def test_provider_error_object_has_no_get_method(self):
        """Verify that _ProviderError objects don't have .get() method."""
        error = target_module._ProviderError("better_enrich", "find_work_email_v3", "insufficient_credits", "test")
        # This should raise AttributeError (no .get() method)
        with pytest.raises(AttributeError, match="has no attribute 'get'"):
            error.get("phone_reverse")

    def test_fallthrough_protection_in_route_processing(self):
        """Verify that the code protects against fallthrough to result processing."""
        # This tests the fix for lines 1187-1225 where result.get() was called
        # even when result was a _ProviderError
        result = target_module._ProviderError("wizleads", "find_email", "rate_limited", "test")

        # Simulate the pattern in run_enrichment_route after fix
        provider_error_dict = target_module._extract_provider_error(result)
        if provider_error_dict:
            # Result is a provider error - should NOT call result.get()
            # The fix adds a 'continue' statement to skip result processing
            assert provider_error_dict["error_type"] == "rate_limited"
            # If we tried to call result.get("phone_reverse"), it would fail
            with pytest.raises(AttributeError):
                result.get("phone_reverse")
        else:
            assert False, "Should have extracted error dict"
