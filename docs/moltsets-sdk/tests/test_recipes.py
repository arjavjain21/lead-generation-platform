import httpx

from moltsets import MoltSetsClient
from moltsets.recipes import find_best_email, probe_search_people


def test_find_best_email_prefers_direct_linkedin():
    calls = []

    def handler(request: httpx.Request):
        calls.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "results": {"email": "jane@example.com", "risk_score": "A", "type": "business"},
                "metadata": {},
            },
        )

    client = MoltSetsClient("ms_test", transport=httpx.MockTransport(handler))
    result = find_best_email(client, linkedin_url="https://linkedin.com/in/jane")

    assert result.found is True
    assert result.accepted is True
    assert result.email == "jane@example.com"
    assert result.source_tool == "linkedin_to_best_email"
    assert len(calls) == 1


def test_find_best_email_flags_d_without_dropping_it():
    def handler(request: httpx.Request):
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "results": {"email": "bad@example.com", "risk_score": "D"},
                "metadata": {},
            },
        )

    client = MoltSetsClient("ms_test", transport=httpx.MockTransport(handler))
    result = find_best_email(client, linkedin_url="https://linkedin.com/in/jane")

    assert result.found is True
    assert result.accepted is False
    assert result.risk_score == "D"


def test_probe_search_people_uses_limit_one_not_count_only():
    def handler(request: httpx.Request):
        payload = request.read().decode("utf-8")
        assert '"limit":1' in payload
        assert "count_only" not in payload
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "results": {"total": 123, "results": [{"full_name": "A Person"}]},
                "metadata": {},
            },
        )

    client = MoltSetsClient("ms_test", transport=httpx.MockTransport(handler))
    probe = probe_search_people(client, title="CEO")
    assert probe.total == 123
    assert probe.sample[0]["full_name"] == "A Person"
