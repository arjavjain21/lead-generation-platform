#!/usr/bin/env python3
"""
WizLeads Integration Test Script

Tests the complete WizLeads integration:
1. API client functionality
2. Cascade integration
3. Force provider parameter
4. Source tracking
5. Sync to Contacts DB

Run with: python test_wizleads_integration.py
"""

import asyncio
import httpx
import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

from enrichment import wizleads_client, providers


def print_test(test_name: str):
    """Print a test header."""
    print(f"\n{'=' * 60}")
    print(f"TEST: {test_name}")
    print('=' * 60)


def print_result(passed: bool, message: str = ""):
    """Print test result."""
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"{status}: {message}")
    return passed


async def test_01_wizleads_provider_enabled():
    """Test that WizLeads provider is enabled."""
    print_test("WizLeads Provider Enabled")

    is_enabled = providers.is_provider_enabled("wizleads")
    enabled_providers = providers.get_enabled_providers()

    print(f"Enabled providers: {enabled_providers}")

    passed = (
        is_enabled and
        "wizleads" in enabled_providers and
        enabled_providers.index("wizleads") == 2  # After contacts_db and blitz
    )

    return print_result(
        passed,
        f"WizLeads enabled in correct position: {enabled_providers}"
    )


async def test_02_api_key_configured():
    """Test that WizLeads API key is configured."""
    print_test("WizLeads API Key Configuration")

    api_key = os.getenv("WIZLEADS_API_KEY", "")

    passed = bool(api_key and api_key.startswith("wl_"))

    return print_result(
        passed,
        f"API key configured: {api_key[:10]}..." if passed else "API key missing or invalid"
    )


async def test_03_wizleads_client_basic():
    """Test basic WizLeads client functionality."""
    print_test("WizLeads Client - Basic Email Lookup")

    api_key = os.getenv("WIZLEADS_API_KEY", "")
    if not api_key:
        return print_result(False, "API key not configured")

    async with httpx.AsyncClient() as client:
        # Test with known data (Google CEO)
        result = await wizleads_client.find_email(
            client,
            first_name="Sundar",
            last_name="Pichai",
            website="google.com"
        )

        print(f"Result: {result}")

        if result and result.get("email"):
            email = result["email"]
            catchall = result.get("catchall", "UNKNOWN")
            passed = "@" in email and email.endswith("google.com")
            return print_result(
                passed,
                f"Found email: {email}, Catchall: {catchall}"
            )
        else:
            return print_result(
                False,
                "No email found (might be expected for this test case)"
            )


async def test_04_wizleads_full_name():
    """Test WizLeads with full name instead of first/last split."""
    print_test("WizLeads Client - Full Name Parameter")

    api_key = os.getenv("WIZLEADS_API_KEY", "")
    if not api_key:
        return print_result(False, "API key not configured")

    async with httpx.AsyncClient() as client:
        # Test with full name (WizLeads supports this)
        result = await wizleads_client.find_email(
            client,
            first_name="Sundar Pichai",  # Full name
            last_name="",  # Empty when using full name
            website="google.com"
        )

        print(f"Result: {result}")

        if result and result.get("email"):
            email = result["email"]
            passed = "@" in email and email.endswith("google.com")
            return print_result(
                passed,
                f"Full name lookup works: {email}"
            )
        else:
            return print_result(
                False,
                "No email found with full name"
            )


async def test_05_wizleads_rate_limiting():
    """Test that rate limiting is working (should not exceed 10 RPS)."""
    print_test("WizLeads Client - Rate Limiting")

    api_key = os.getenv("WIZLEADS_API_KEY", "")
    if not api_key:
        return print_result(False, "API key not configured")

    async with httpx.AsyncClient() as client:
        import time

        # Make 5 requests and measure time
        start = time.monotonic()

        results = []
        for i in range(5):
            result = await wizleads_client.find_email(
                client,
                first_name=f"Test{i}",
                last_name="User",
                website="example.com"
            )
            results.append(result)

        elapsed = time.monotonic() - start

        # With 10 RPS, 5 requests should take at least 0.4 seconds
        # (rate limiter: 1/10 = 0.1s between requests)
        min_expected_time = 0.4  # 4 intervals of 0.1s

        passed = elapsed >= min_expected_time

        return print_result(
            passed,
            f"Rate limiting working: {elapsed:.2f}s for 5 requests (expected >= {min_expected_time}s)"
        )


async def test_06_wizleads_error_handling():
    """Test WizLeads error handling for invalid input."""
    print_test("WizLeads Client - Error Handling")

    api_key = os.getenv("WIZLEADS_API_KEY", "")
    if not api_key:
        return print_result(False, "API key not configured")

    async with httpx.AsyncClient() as client:
        # Test with invalid domain
        result = await wizleads_client.find_email(
            client,
            first_name="Test",
            last_name="User",
            website="invalid-domain-that-does-not-exist.com"
        )

        # Should return None for invalid domain
        passed = result is None or (result and not result.get("email"))

        return print_result(
            passed,
            f"Invalid domain handled correctly: {result}"
        )


async def test_07_source_constant():
    """Test that WizLeads source constant is defined."""
    print_test("WizLeads Source Constant")

    from enrichment import pipeline

    # Check if SOURCE_WIZLEADS is defined
    has_source = hasattr(pipeline, "SOURCE_WIZLEADS")

    if has_source:
        source_value = pipeline.SOURCE_WIZLEADS
        passed = source_value == "wizleads_email"
        return print_result(
            passed,
            f"SOURCE_WIZLEADS = '{source_value}'"
        )
    else:
        return print_result(
            False,
            "SOURCE_WIZLEADS not defined"
        )


async def test_08_normalize_source():
    """Test that _normalize_source handles WizLeads sources."""
    print_test("WizLeads Source Normalization")

    from enrichment import pipeline

    # Test various WizLeads source formats
    test_cases = [
        ("wizleads_email", "wizleads"),
        ("wizleads_anything", "wizleads"),
    ]

    passed = True
    for input_source, expected in test_cases:
        result = pipeline._normalize_source(input_source)
        if result != expected:
            print(f"  ❌ Expected '{expected}' for '{input_source}', got '{result}'")
            passed = False
        else:
            print(f"  ✅ '{input_source}' → '{result}'")

    return print_result(
        passed,
        "Source normalization works correctly"
    )


async def test_09_valid_providers():
    """Test that WizLeads is in VALID_PROVIDERS."""
    print_test("WizLeads in VALID_PROVIDERS")

    from enrichment import pipeline, routes

    # Check pipeline.VALID_PROVIDERS
    pipeline_has_wizleads = "wizleads" in pipeline.VALID_PROVIDERS

    # Check routes.VALID_PROVIDERS
    routes_has_wizleads = "wizleads" in routes.VALID_PROVIDERS

    passed = pipeline_has_wizleads and routes_has_wizleads

    return print_result(
        passed,
        f"WizLeads in VALID_PROVIDERS - pipeline: {pipeline_has_wizleads}, routes: {routes_has_wizleads}"
    )


async def test_10_sync_contacts_db_format():
    """Test that WizLeads output format is compatible with Contacts DB sync."""
    print_test("WizLeads Output Format for Contacts DB Sync")

    from enrichment import pipeline

    # Check that WizLeads results populate required columns for sync
    # Note: 'domain' is an input column, not in ENRICHED_COLUMNS
    # The sync function extracts domain from email if not present
    required_enriched_columns = [
        "dm_email",
        "dm_full_name",
        "dm_first_name",
        "dm_last_name",
        "dm_linkedin_url",
        "dm_title",
        "dm_email_source",  # Important for tracking WizLeads as source
    ]

    # Check that all required columns are in ENRICHED_COLUMNS
    missing = []
    for col in required_enriched_columns:
        if col not in pipeline.ENRICHED_COLUMNS:
            missing.append(col)

    passed = len(missing) == 0

    return print_result(
        passed,
        f"All required enriched columns present - Missing: {missing}" if missing else "All columns present for sync"
    )


async def run_all_tests():
    """Run all tests and report results."""
    print("\n" + "=" * 60)
    print("WIZLEADS INTEGRATION TEST SUITE")
    print("=" * 60)

    tests = [
        test_01_wizleads_provider_enabled,
        test_02_api_key_configured,
        test_03_wizleads_client_basic,
        test_04_wizleads_full_name,
        test_05_wizleads_rate_limiting,
        test_06_wizleads_error_handling,
        test_07_source_constant,
        test_08_normalize_source,
        test_09_valid_providers,
        test_10_sync_contacts_db_format,
    ]

    results = []
    for test in tests:
        try:
            result = await test()
            results.append(result)
        except Exception as e:
            print(f"❌ FAIL: Exception - {e}")
            results.append(False)

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    total = len(results)
    passed = sum(results)
    failed = total - passed

    print(f"Total: {total}")
    print(f"Passed: {passed} ✅")
    print(f"Failed: {failed} ❌")
    print(f"Success Rate: {passed/total*100:.1f}%")

    if failed == 0:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(run_all_tests())
    sys.exit(exit_code)
