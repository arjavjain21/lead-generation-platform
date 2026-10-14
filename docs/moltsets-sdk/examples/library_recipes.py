from moltsets import MoltSetsClient
from moltsets.recipes import enrich_linkedin_profile, find_best_email, probe_search_people


with MoltSetsClient(user_agent="internal-waterfall/1.0") as client:
    email = find_best_email(
        client,
        name="Jane Doe",
        company_domain="example.com",
        accepted_risk_scores=("A", "B", "C", "F"),
    )
    print(email)

    probe = probe_search_people(
        client,
        seniority=["VP", "C Suite"],
        country="United States",
        employee_range=["51-200", "201-500"],
    )
    print("estimated total:", probe.total)

    profile = enrich_linkedin_profile(
        client,
        "https://www.linkedin.com/in/example",
        include_mobile=True,
    )
    print(profile)
