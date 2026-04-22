from __future__ import annotations

import pytest

from column_categorization.config import load_etl_config_from_env, load_sink_config_from_env, resolve_env_template


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


def _set_minimal_neutral_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_URL", "postgresql://localhost/db")
    monkeypatch.setenv("LLM_MODEL", "m")
    monkeypatch.setenv("LLM_API_KEY", "k")


def test_load_etl_config_uses_batch_process_from_neutral(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("TARGET_TYPE", "api")
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("TARGET_ACCOUNT_UID", "acc-123")
    monkeypatch.setenv("TARGET_DATASET_UID", "ds-456")
    monkeypatch.setenv("TARGET_ACCESS_TOKEN", "token")
    monkeypatch.setenv("BATCH_PROCESS", "25")
    etl = load_etl_config_from_env()
    assert etl.load_batch_size == 25


def test_load_etl_config_uses_retry_delay_and_dead_letter_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("TARGET_TYPE", "api")
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("TARGET_ACCOUNT_UID", "acc-123")
    monkeypatch.setenv("TARGET_DATASET_UID", "ds-456")
    monkeypatch.setenv("TARGET_ACCESS_TOKEN", "token")
    monkeypatch.setenv("LOAD_MAX_RETRIES", "5")
    monkeypatch.setenv("LOAD_RETRY_DELAY_SECONDS", "7")
    monkeypatch.setenv("DEAD_LETTER_PATH", "outputs/custom_dead_letter.jsonl")
    etl = load_etl_config_from_env()
    assert etl.load_max_retries == 5
    assert etl.load_retry_delay_seconds == 7
    assert etl.dead_letter_path == "outputs/custom_dead_letter.jsonl"


def test_load_sink_config_builds_neutral_insert_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("TARGET_TYPE", "api")
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("TARGET_ACCOUNT_UID", "acc-123")
    monkeypatch.setenv("TARGET_DATASET_UID", "ds-456")
    monkeypatch.setenv("TARGET_ACCESS_TOKEN", "token")
    output = load_sink_config_from_env()
    assert output.http_base_url == "https://example-api.test"
    assert output.http_path == "/api/v1/accounts/acc-123/datasets/ds-456/data"
    assert output.target_type == "api"


def test_load_sink_config_uses_insert_path_and_timeout_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("TARGET_TYPE", "api")
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("TARGET_ACCOUNT_UID", "acc-123")
    monkeypatch.setenv("TARGET_DATASET_UID", "ds-456")
    monkeypatch.setenv("TARGET_ACCESS_TOKEN", "token")
    monkeypatch.setenv("TARGET_API_PATH_INSERT_DATA", "/custom/accounts/{account_uid}/datasets/{dataset_uid}/rows")
    monkeypatch.setenv("TARGET_API_TIMEOUT_SECONDS", "99")
    output = load_sink_config_from_env()
    assert output.http_path == "/custom/accounts/acc-123/datasets/ds-456/rows"
    assert output.http_timeout_seconds == 99
