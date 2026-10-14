from moltsets import CURRENT_ENDPOINTS, LEGACY_ENDPOINTS, MoltSetsClient


def test_every_current_endpoint_has_sdk_method():
    missing = [name for name in CURRENT_ENDPOINTS if not hasattr(MoltSetsClient, name)]
    assert missing == []


def test_documented_legacy_routes_have_sdk_methods():
    missing = [name for name in LEGACY_ENDPOINTS if not hasattr(MoltSetsClient, name)]
    assert missing == []


def test_current_endpoint_count_snapshot():
    assert len(CURRENT_ENDPOINTS) == 20
