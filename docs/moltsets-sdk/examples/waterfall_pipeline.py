import os
from moltsets import MoltSetsClient, MoltSetsAPIError, risk_score_is_sendable

client = MoltSetsClient(os.environ["MOLTSETS_API_KEY"], user_agent="eis-waterfall/1.0")

linkedin_url = "https://linkedin.com/in/example"

try:
    result = client.linkedin_to_best_email(linkedin_url=linkedin_url)

    if result.not_found:
        # Clean waterfall miss. Continue to your next provider.
        print("MISS")
    else:
        email = (result.results or {}).get("email")
        risk = (result.results or {}).get("risk_score")
        if email and risk_score_is_sendable(risk):
            print("HIT", email, risk)
        else:
            print("HIT_BUT_NOT_SENDABLE", email, risk)

except MoltSetsAPIError as exc:
    if exc.code in {"rate_limited", "fair_use_limit_exceeded", "fair_use_requests_exceeded"}:
        # Requeue this stage using exc.retry_after rather than burning quota in a tight loop.
        print("REQUEUE", exc.code, exc.retry_after)
    elif exc.code in {"plan_restricted", "insufficient_tokens", "insufficient_phone_tokens"}:
        print("CONFIG_OR_BILLING_BLOCK", exc.code)
    else:
        raise
