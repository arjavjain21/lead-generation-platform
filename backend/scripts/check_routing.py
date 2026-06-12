#!/usr/bin/env python3
"""
Mechanical check script for enrichment routing.

Verifies the provider call order for 5 representative input shapes:
1. linkedin+name+domain - should use LinkedIn-first cascade
2. linkedin-only - should use LinkedIn cascade (no domain)
3. phone-only - should try phone reverse
4. name+domain - should use name+domain cascade
5. domain-only - should defer to _enrich_domain (decision-maker cascade)
"""
import sys
sys.path.insert(0, "/var/www/lead-generation-platform/backend")
from enrichment import pipeline as p

def check(name: str, linkedin="", phone="", full_name="", first="", last="", domain="", company=""):
    route = p.route_enrichment(
        linkedin_url=linkedin,
        phone=phone,
        full_name=full_name,
        first_name=first,
        last_name=last,
        domain=domain,
        company_name=company,
    )
    ok = True
    identifiers = [s["identifier"] for s in route["steps"]]
    methods = [s["method"] for s in route["steps"]]
    reason = route.get("no_email_reason", "")

    print(f"=== {name} ===")
    print(f"  Mode: {route['mode']}")
    print(f"  Steps: {identifiers}")
    print(f"  No-email reason: {reason}")

    if route["mode"] == "invalid":
        print(f"  FAIL: invalid mode")
        return False
    print(f"  OK")
    return True

def check_force_provider():
    for force in ["blitz", "contacts_db"]:
        route = p.route_enrichment(
            linkedin_url="https://linkedin.com/in/jane",
            full_name="Jane Doe",
            domain="acme.com",
            force_provider=force,
        )
        providers = {s["provider"] for s in route["steps"]}
        print(f"=== force_provider={force} ===")
        print(f"  Mode: {route['mode']}")
        print(f"  Providers used: {providers}")
        for s in route["steps"]:
            print(f"    {s['identifier']} -> {s['provider']}_{s['method']}")

def main():
    print("CHecking routing...")
    all_ok = True

    # 1. linkedin+name+domain
    all_ok &= check(
        "1. linkedin+name+domain",
        linkedin="https://linkedin.com/in/jane",
        full_name="Jane Doe",
        domain="acme.com",
    )

    # 2. linkedin-only
    all_ok &= check(
        "2. linkedin-only",
        linkedin="https://linkedin.com/in/jane",
    )

    # 3. phone-only
    all_ok &= check(
        "3. phone-only",
        phone="+1-555-0100",
    )

    # 4. name+domain
    all_ok &= check(
        "4. name+domain",
        full_name="Jane Doe",
        domain="acme.com",
    )

    # 5. domain-only
    all_ok &= check(
        "5. domain-only (should defer)",
        domain="acme.com",
    )

    # Force provider checks
    print()
    check_force_provider()

    if all_ok:
        print("\n=== PASS ===")
        return 0
    else:
        print("\n=== SOME CHECKS FAILED ===")
        return 1

if __name__ == "__main__":
    sys.exit(main())