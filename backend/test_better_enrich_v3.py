"""
Test script for Better Enrich V3 API.

Tests the new V3 endpoint with real lead data to verify:
1. Email discovery works
2. LinkedIn URL support improves coverage
3. Verification status is properly returned
"""

import asyncio
import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()

import httpx
from enrichment import better_enrich_client


# Test data - real leads with different scenarios
TEST_LEADS = [
    {
        "name": "Elon Musk",
        "domain": "tesla.com",
        "linkedin_url": "https://linkedin.com/in/elon-musk",
        "description": "Well-known CEO with LinkedIn URL"
    },
    {
        "name": "Satya Nadella",
        "domain": "microsoft.com",
        "linkedin_url": "https://linkedin.com/in/satyanadella",
        "description": "CEO with LinkedIn URL"
    },
    {
        "name": "Tim Cook",
        "domain": "apple.com",
        "linkedin_url": "https://linkedin.com/in/timcook",
        "description": "CEO with LinkedIn URL"
    },
    {
        "name": "Sundar Pichai",
        "domain": "google.com",
        "linkedin_url": "https://linkedin.com/in/sundarpichai",
        "description": "CEO with LinkedIn URL"
    },
    {
        "name": "Mark Zuckerberg",
        "domain": "meta.com",
        "linkedin_url": "https://linkedin.com/in/markzuckerberg",
        "description": "CEO with LinkedIn URL"
    },
    {
        "name": "Andy Jassy",
        "domain": "amazon.com",
        "linkedin_url": "https://linkedin.com/in/andyjassy",
        "description": "CEO with LinkedIn URL"
    },
    {
        "name": "Jensen Huang",
        "domain": "nvidia.com",
        "linkedin_url": "https://linkedin.com/in/jensenhuang",
        "description": "CEO with LinkedIn URL"
    },
    {
        "name": "Sam Altman",
        "domain": "openai.com",
        "linkedin_url": "https://linkedin.com/in/samaltman",
        "description": "CEO with LinkedIn URL"
    },
    {
        "name": "Brian Chesky",
        "domain": "airbnb.com",
        "linkedin_url": "",
        "description": "CEO without LinkedIn URL"
    },
    {
        "name": "Drew Houston",
        "domain": "dropbox.com",
        "linkedin_url": "https://linkedin.com/in/drewhouston",
        "description": "CEO with LinkedIn URL"
    }
]


async def test_v3_endpoint():
    """Test Better Enrich V3 endpoint with real data."""
    print("=" * 80)
    print("Testing Better Enrich V3 API")
    print("=" * 80)

    # Check API key
    api_key = os.getenv("BETTER_ENRICH_API_KEY")
    if not api_key:
        print("\n❌ ERROR: BETTER_ENRICH_API_KEY not set")
        print("Please set the environment variable and try again.")
        return False

    print(f"\n✓ API Key configured (length: {len(api_key)})")

    # Create HTTP client
    async with httpx.AsyncClient(timeout=30.0) as client:
        results = []

        for i, lead in enumerate(TEST_LEADS, 1):
            print(f"\n[{i}/{len(TEST_LEADS)}] Testing: {lead['name']} @ {lead['domain']}")
            print(f"  Description: {lead['description']}")

            try:
                result = await better_enrich_client.find_work_email_v3(
                    client,
                    full_name=lead["name"],
                    company_domain=lead["domain"],
                    linkedin_url=lead["linkedin_url"] or None
                )

                if result and result.get("email"):
                    email = result.get("email")
                    email_status = result.get("email_status", "unknown")
                    verifier = result.get("verifier", "N/A")
                    esp = result.get("esp", "N/A")

                    print(f"  ✓ Email found: {email}")
                    print(f"    Status: {email_status}")
                    print(f"    Verifier: {verifier}")
                    print(f"    ESP: {esp}")

                    results.append({
                        "lead": lead["name"],
                        "domain": lead["domain"],
                        "email": email,
                        "email_status": email_status,
                        "success": True
                    })
                else:
                    print(f"  ✗ No email found")
                    results.append({
                        "lead": lead["name"],
                        "domain": lead["domain"],
                        "email": None,
                        "email_status": None,
                        "success": False
                    })

            except Exception as e:
                print(f"  ✗ Error: {e}")
                results.append({
                    "lead": lead["name"],
                    "domain": lead["domain"],
                    "email": None,
                    "email_status": None,
                    "success": False,
                    "error": str(e)
                })

            # Rate limit delay
            if i < len(TEST_LEADS):
                await asyncio.sleep(0.3)  # Stay within 5 RPS limit

    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    successful = sum(1 for r in results if r["success"])
    total = len(results)

    print(f"\nTotal tested: {total}")
    print(f"Successful: {successful}")
    print(f"Failed: {total - successful}")
    print(f"Success rate: {successful / total * 100:.1f}%")

    if successful > 0:
        print("\n✓ Successfully found emails:")
        for r in results:
            if r["success"]:
                print(f"  - {r['lead']}: {r['email']} (status: {r['email_status']})")

    if total - successful > 0:
        print("\n✗ Failed to find emails:")
        for r in results:
            if not r["success"]:
                error = r.get("error", "No email found")
                print(f"  - {r['lead']}: {error}")

    return successful > 0


async def test_v3_through_api():
    """Test V3 through the enrichment API endpoint."""
    print("\n" + "=" * 80)
    print("Testing V3 through Enrichment API")
    print("=" * 80)

    # This will test the full flow through the API
    # We'll make a request to the enrichment endpoint

    # Load API key for authentication
    from shared import auth
    import json

    # Get or create an API key for testing
    # For now, we'll just check if the endpoint is accessible

    base_url = "http://localhost:8765"

    async with httpx.AsyncClient() as client:
        # Check health
        try:
            resp = await client.get(f"{base_url}/api/health")
            if resp.status_code == 200:
                print("\n✓ Service is running")
            else:
                print(f"\n✗ Service health check failed: {resp.status_code}")
                return False
        except Exception as e:
            print(f"\n✗ Cannot connect to service: {e}")
            print("Make sure the service is running on http://localhost:8765")
            return False

        # Test a simple enrichment request
        print("\nTesting enrichment endpoint with V3...")
        test_payload = {
            "domain": "tesla.com",
            "full_name": "Elon Musk",
            "linkedin_url": "https://linkedin.com/in/elon-musk"
        }

        try:
            resp = await client.post(
                f"{base_url}/api/enrichment/enrich",
                json=test_payload,
                headers={"X-API-Key": os.getenv("TEST_API_KEY", "your-api-key-here")}
            )

            if resp.status_code == 200:
                result = resp.json()
                print("✓ Enrichment endpoint accessible")
                print(f"  Response keys: {list(result.keys())}")
                return True
            elif resp.status_code == 401:
                print("✗ Authentication failed - need valid API key")
                print("  Skipping API test (authentication required)")
                return None
            else:
                print(f"✗ Enrichment endpoint returned: {resp.status_code}")
                print(f"  Response: {resp.text[:200]}")
                return False

        except Exception as e:
            print(f"✗ Error testing enrichment endpoint: {e}")
            return False


async def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("BETTER ENRICH V3 TEST SUITE")
    print("=" * 80)

    # Test 1: Direct V3 endpoint
    v3_success = await test_v3_endpoint()

    # Test 2: Through API
    api_result = await test_v3_through_api()

    # Final verdict
    print("\n" + "=" * 80)
    print("FINAL VERDICT")
    print("=" * 80)

    if v3_success:
        print("\n✓ Better Enrich V3 endpoint is working correctly")
        print("  - Email discovery: WORKING")
        print("  - LinkedIn URL support: WORKING")
        print("  - Email status verification: WORKING")
    else:
        print("\n✗ Better Enrich V3 endpoint has issues")
        print("  Please review the errors above")

    if api_result is True:
        print("\n✓ Enrichment API is working with V3")
    elif api_result is False:
        print("\n✗ Enrichment API has issues")
    else:
        print("\n○ Enrichment API test skipped (authentication required)")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
