from __future__ import annotations

import argparse
from unittest.mock import MagicMock

import main
import pytest
from column_categorization import api_check
from column_categorization.categorization.llm_categorizer import CategorizationBatchResult
from column_categorization.config import NeutralRuntimeConfig


def _neutral_runtime(
    *,
    source_query: str | None = None,
    do_categorize: bool = False,
    categorize_columns: tuple[str, ...] = (),
    categorize_max_retries: int = 3,
    batch_process: int = 10,
) -> NeutralRuntimeConfig:
    return NeutralRuntimeConfig(
        db_url="postgresql://localhost/db",
        source_query=source_query,
        target_type="api",
        target_api_base_url="https://example-api.test",
        target_api_path_insert_data=None,
        target_account_uid="acc",
        target_access_token="tok",
        target_dataset_uid="ds",
        target_output_path=None,
        batch_process=batch_process,
        do_categorize=do_categorize,
        categorize_columns=categorize_columns,
        categorize_max_retries=categorize_max_retries,
        llm_model="m",
        llm_api_key="k",
        llm_base_url=None,
    )


def test_normalize_mode_maps_legacy_etl_alias() -> None:
    assert main._normalize_mode("etl") == "categorize_to_api"
    assert main._normalize_mode("raw_to_api") == "raw_to_api"


def test_parse_raw_columns_uses_fallback() -> None:
    output = main._parse_raw_columns(None, fallback_columns=["source_event_id", "raw_value"])
    assert output == ["source_event_id", "raw_value"]


def test_parse_raw_columns_parses_comma_list() -> None:
    output = main._parse_raw_columns(
        "source_event_id, raw_value , notes",
        fallback_columns=["ignored"],
    )
    assert output == ["source_event_id", "raw_value", "notes"]


def test_parse_categorized_columns_uses_fallback() -> None:
    output = main._parse_categorized_columns(None, fallback_column="note")
    assert output == ["note"]


def test_parse_categorized_columns_parses_comma_list() -> None:
    output = main._parse_categorized_columns("note, description", fallback_column="ignored")
    assert output == ["note", "description"]


def test_resolve_source_query_uses_cli_value() -> None:
    arguments = argparse.Namespace(source_sql="SELECT id, name FROM public.org")
    neutral = _neutral_runtime(source_query="SELECT 1")
    output = main._resolve_source_query(arguments, neutral)
    assert output == "SELECT id, name FROM public.org"


def test_resolve_source_query_uses_neutral_when_cli_empty() -> None:
    arguments = argparse.Namespace(source_sql=None)
    neutral = _neutral_runtime(source_query="SELECT id, name, note FROM public.org")
    output = main._resolve_source_query(arguments, neutral)
    assert output == "SELECT id, name, note FROM public.org"


def test_require_source_query_raises_when_unset() -> None:
    with pytest.raises(ValueError, match="SOURCE_QUERY is required"):
        main._require_source_query(None)


def test_resolve_database_url_prefers_cli_over_neutral() -> None:
    arguments = argparse.Namespace(database_url="postgresql://cli/db")
    neutral = _neutral_runtime(source_query="SELECT 1")
    output = main._resolve_database_url(arguments, neutral)
    assert output == "postgresql://cli/db"


def test_resolve_database_url_uses_neutral_db_url() -> None:
    arguments = argparse.Namespace(database_url=None)
    neutral = _neutral_runtime(source_query="SELECT 1")
    output = main._resolve_database_url(arguments, neutral)
    assert output == "postgresql://localhost/db"


def test_resolve_effective_batch_size_prefers_cli() -> None:
    arguments = argparse.Namespace(batch_size=7)
    neutral = _neutral_runtime(source_query="SELECT 1", batch_process=99)
    assert main._resolve_effective_batch_size(arguments, neutral) == 7


def test_resolve_effective_batch_size_falls_back_to_neutral() -> None:
    arguments = argparse.Namespace(batch_size=None)
    neutral = _neutral_runtime(source_query="SELECT 1", batch_process=12)
    assert main._resolve_effective_batch_size(arguments, neutral) == 12


def test_resolve_categorize_column_names_reads_neutral_csv_when_flagged() -> None:
    arguments = argparse.Namespace(categorized_columns=None, raw_value_column="raw_value")
    neutral = _neutral_runtime(source_query="SELECT 1", do_categorize=True, categorize_columns=("a", "b"))
    output = main._resolve_categorize_column_names(arguments, neutral, apply_categorization=True)
    assert output == ["a", "b"]


def test_resolve_categorize_column_names_falls_back_to_cli_when_neutral_empty() -> None:
    arguments = argparse.Namespace(categorized_columns="x, y", raw_value_column="raw_value")
    neutral = _neutral_runtime(source_query="SELECT 1", do_categorize=True, categorize_columns=())
    output = main._resolve_categorize_column_names(arguments, neutral, apply_categorization=True)
    assert output == ["x", "y"]


def test_resolve_llm_runtime_prefers_cli_over_neutral() -> None:
    arguments = argparse.Namespace(llm_api_key="cli-key", llm_base_url="https://cli", llm_model="cli-model")
    neutral = _neutral_runtime(source_query="SELECT 1")
    key, base, model = main._resolve_llm_runtime(arguments, neutral)
    assert key == "cli-key"
    assert base == "https://cli"
    assert model == "cli-model"


def test_resolve_llm_runtime_uses_neutral_defaults() -> None:
    arguments = argparse.Namespace(llm_api_key=None, llm_base_url=None, llm_model=None)
    neutral = _neutral_runtime(source_query="SELECT 1")
    key, base, model = main._resolve_llm_runtime(arguments, neutral)
    assert key == "k"
    assert base is None
    assert model == "m"


def test_ensure_required_columns_raises_on_missing_column() -> None:
    with pytest.raises(ValueError, match="SOURCE_QUERY result is missing required columns: note"):
        main._ensure_required_columns(
            available_columns=["id", "name"],
            required_columns=["id", "note"],
            source_label="SOURCE_QUERY result",
        )


def test_serialize_categorized_labels_returns_json_string() -> None:
    output = main._serialize_categorized_labels(["Jasa Survei", "Jasa Pemetaan"])
    assert output == "[\"Jasa Survei\", \"Jasa Pemetaan\"]"


def test_build_api_check_url_uses_env_template_for_me(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://example-api.test")
    output = api_check.build_api_check_url(
        base_url="https://example-api.test",
        mode="me",
        account_uid_cli=None,
    )
    assert output == "https://example-api.test/api/v1/auth/me"


def test_build_api_check_url_resolves_account_uid_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("TARGET_ACCOUNT_UID", "acc-123")
    output = api_check.build_api_check_url(
        base_url="https://example-api.test",
        mode="datasets",
        account_uid_cli=None,
    )
    assert output == "https://example-api.test/api/v1/accounts/acc-123/datasets"


def test_build_api_check_url_fails_when_path_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("TARGET_API_PATH_AUTH_ME", "/api/v1/auth/{missing}")
    with pytest.raises(ValueError, match="unresolved template placeholders"):
        api_check.build_api_check_url(
            base_url="https://example-api.test",
            mode="me",
            account_uid_cli=None,
        )


def test_resolve_api_timeout_seconds_uses_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_CHECK_TIMEOUT_SECONDS", "45")
    assert api_check.resolve_api_check_timeout_seconds(None) == 45


def test_resolve_api_output_dir_uses_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_CHECK_OUTPUT_DIR", "outputs/custom_api")
    assert (
        api_check.resolve_api_check_output_dir(None).as_posix()
        == "outputs/custom_api"
    )


def test_categorize_batch_with_retries_succeeds_after_transient_failure() -> None:
    categorizer = MagicMock()
    success = CategorizationBatchResult(column_description="", mappings=[])
    categorizer.categorize_batch.side_effect = [ValueError("temporary"), success]
    result = main._categorize_batch_with_retries(
        categorizer,  # type: ignore[arg-type]
        categorized_column="c",
        batch_index=1,
        values_batch=["a"],
        categorize_max_retries=3,
    )
    assert result is success
    assert categorizer.categorize_batch.call_count == 2


def test_categorize_batch_with_retries_raises_after_exhausted() -> None:
    categorizer = MagicMock()
    categorizer.categorize_batch.side_effect = ValueError("always fails")
    with pytest.raises(RuntimeError, match="Categorization failed"):
        main._categorize_batch_with_retries(
            categorizer,  # type: ignore[arg-type]
            categorized_column="c",
            batch_index=2,
            values_batch=["x"],
            categorize_max_retries=1,
        )
