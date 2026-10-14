import json
import httpx
import pytest

from moltsets import MoltSetsAPIError, MoltSetsClient


def test_not_found_is_normal_response():
    def handler(request: httpx.Request):
        assert request.headers["Authorization"] == "Bearer ms_test"
        assert request.headers["User-Agent"] == "test-sdk/1"
        return httpx.Response(200, json={"results": {}, "status": "not_found", "metadata": {}})

    client = MoltSetsClient("ms_test", user_agent="test-sdk/1", transport=httpx.MockTransport(handler))
    response = client.email_to_linkedin(email="x@example.com")
    assert response.not_found
    assert not response.found


def test_endpoint_payload_and_outcome_header():
    def handler(request: httpx.Request):
        payload = json.loads(request.content)
        assert payload == {"linkedin_url": "https://linkedin.com/in/test"}
        return httpx.Response(
            200,
            headers={"X-MoltSets-Outcome": "ok"},
            json={"results": {"email": "a@b.com", "risk_score": "A"}, "status": "ok", "metadata": {}},
        )

    client = MoltSetsClient("ms_test", transport=httpx.MockTransport(handler))
    response = client.linkedin_to_best_email(linkedin_url="https://linkedin.com/in/test")
    assert response.found
    assert response.outcome == "ok"


def test_long_fair_use_retry_after_is_exposed_not_slept():
    def handler(request: httpx.Request):
        return httpx.Response(
            429,
            headers={"Retry-After": "7200"},
            json={
                "error": {"code": "fair_use_limit_exceeded", "message": "wait"},
                "metadata": {"retry_after": 7200},
            },
        )

    client = MoltSetsClient("ms_test", max_retry_after=1, transport=httpx.MockTransport(handler))
    with pytest.raises(MoltSetsAPIError) as excinfo:
        client.get_account()
    assert excinfo.value.retry_after == 7200
