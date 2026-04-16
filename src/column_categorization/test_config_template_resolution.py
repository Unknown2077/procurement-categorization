from __future__ import annotations

import pytest

from column_categorization.config import load_sink_config_from_env, resolve_env_template


def test_resolve_env_template_supports_nested_env_reference(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACCOUNT_UID", "acc-123")
    monkeypatch.setenv("DATASET_UID", "ds-456")
    monkeypatch.setenv("API_PATH_INSERT_DATA", "/api/v1/accounts/{account_uid}/datasets/{dataset_uid}/data")
    monkeypatch.setenv("SINK_HTTP_PATH", "{API_PATH_INSERT_DATA}")
    output = resolve_env_template(
        env_key="SINK_HTTP_PATH",
        required=True,
        fallback_env_key="API_PATH_INSERT_DATA",
        context={"account_uid": "acc-123", "dataset_uid": "ds-456"},
    )
    assert output == "/api/v1/accounts/acc-123/datasets/ds-456/data"


def test_resolve_env_template_raises_for_unresolved_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SINK_HTTP_PATH", "/api/v1/accounts/{missing_uid}/datasets")
    with pytest.raises(ValueError, match="missing_uid"):
        resolve_env_template(
            env_key="SINK_HTTP_PATH",
            required=True,
            context={"account_uid": "acc-123"},
        )


def test_load_sink_config_uses_api_path_insert_data_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SINK_TYPE", "http")
    monkeypatch.delenv("SINK_HTTP_BASE_URL", raising=False)
    monkeypatch.setenv("API_BASE_URL", "https://example-api.test")
    monkeypatch.delenv("SINK_HTTP_PATH", raising=False)
    monkeypatch.setenv("API_PATH_INSERT_DATA", "/api/v1/accounts/{account_uid}/datasets/{dataset_uid}/data")
    monkeypatch.setenv("ACCOUNT_UID", "acc-123")
    monkeypatch.setenv("DATASET_UID", "ds-456")
    monkeypatch.setenv("SINK_HTTP_AUTH_TOKEN", "token")
    output = load_sink_config_from_env()
    assert output.http_base_url == "https://example-api.test"
    assert output.http_path == "/api/v1/accounts/acc-123/datasets/ds-456/data"
