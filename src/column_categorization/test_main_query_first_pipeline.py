from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

import main
from column_categorization.schemas.categorization import CategorizationResponse, SourceInfo
from column_categorization.schemas.load import LoadResult


def _set_minimal_neutral_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_URL", "postgresql://localhost/db")
    monkeypatch.setenv("TARGET_TYPE", "api")
    monkeypatch.setenv("TARGET_API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("TARGET_ACCOUNT_UID", "acc-1")
    monkeypatch.setenv("TARGET_DATASET_UID", "ds-1")
    monkeypatch.setenv("TARGET_ACCESS_TOKEN", "token-1")
    monkeypatch.setenv("LLM_MODEL", "model-1")
    monkeypatch.setenv("LLM_API_KEY", "key-1")
    monkeypatch.setenv("SOURCE_QUERY", "SELECT 1 AS source_event_id, 'v' AS raw_value")


def _base_arguments() -> argparse.Namespace:
    return argparse.Namespace(
        database_url=None,
        source_sql=None,
        raw_columns="source_event_id,raw_value",
        raw_id_column="source_event_id",
        raw_value_column="raw_value",
        categorized_columns=None,
        batch_size=None,
        nim_api_key=None,
        nim_base_url=None,
        model=None,
    )


def test_categorize_mode_respects_do_categorize_false(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("DO_CATEGORIZE", "false")
    reader_instance = MagicMock()
    reader_instance.fetch_rows_and_columns_by_sql.return_value = (
        [{"source_event_id": 1, "raw_value": "alpha"}],
        ["source_event_id", "raw_value"],
    )
    with patch.object(main, "PostgresReader", return_value=reader_instance):
        with patch.object(main, "OpenAILLMCategorizer") as categorizer_cls:
            exit_code = main._run_query_first_to_api_flow(
                _base_arguments(),
                dry_run=True,
                categorization_mode=True,
            )
    assert exit_code == 0
    categorizer_cls.assert_not_called()


def test_categorize_mode_with_do_categorize_true_builds_categorizer(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("DO_CATEGORIZE", "true")
    monkeypatch.setenv("CATEGORIZE_COLUMNS", "raw_value")
    reader_instance = MagicMock()
    reader_instance.fetch_rows_and_columns_by_sql.return_value = (
        [{"source_event_id": 1, "raw_value": "alpha"}],
        ["source_event_id", "raw_value"],
    )
    mock_categorizer = MagicMock()
    from column_categorization.categorization.llm_categorizer import CategorizationBatchResult
    from column_categorization.schemas.categorization import ValueMapping

    mock_categorizer.categorize_batch.return_value = CategorizationBatchResult(
        column_description="",
        mappings=[ValueMapping(raw_value="alpha", labels=["L1"])],
    )
    with patch.object(main, "PostgresReader", return_value=reader_instance):
        with patch.object(main, "OpenAILLMCategorizer", return_value=mock_categorizer):
            exit_code = main._run_query_first_to_api_flow(
                _base_arguments(),
                dry_run=True,
                categorization_mode=True,
            )
    assert exit_code == 0
    mock_categorizer.categorize_batch.assert_called()


def test_categorization_fail_fast_skips_sink_load(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("DO_CATEGORIZE", "true")
    monkeypatch.setenv("CATEGORIZE_COLUMNS", "raw_value")
    monkeypatch.setenv("CATEGORIZE_MAX_RETRIES", "0")
    reader_instance = MagicMock()
    reader_instance.fetch_rows_and_columns_by_sql.return_value = (
        [{"source_event_id": 1, "raw_value": "alpha"}],
        ["source_event_id", "raw_value"],
    )
    mock_categorizer = MagicMock()
    mock_categorizer.categorize_batch.side_effect = ValueError("llm failure")
    sink = MagicMock()
    with patch.object(main, "PostgresReader", return_value=reader_instance):
        with patch.object(main, "OpenAILLMCategorizer", return_value=mock_categorizer):
            with patch.object(main, "_ensure_http_sink", return_value=sink):
                with pytest.raises(RuntimeError, match="Categorization failed"):
                    main._run_query_first_to_api_flow(
                        _base_arguments(),
                        dry_run=False,
                        categorization_mode=True,
                    )
    sink.load_rows.assert_not_called()


def test_raw_mode_with_json_target_uses_file_sink_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("TARGET_TYPE", "json")
    monkeypatch.setenv("TARGET_OUTPUT_PATH", "outputs/result.jsonl")
    reader_instance = MagicMock()
    reader_instance.fetch_rows_and_columns_by_sql.return_value = (
        [{"source_event_id": 1, "raw_value": "alpha"}],
        ["source_event_id", "raw_value"],
    )
    file_sink = MagicMock()
    file_sink.load_rows.return_value = LoadResult(
        sink_type="file",
        total_records=1,
        loaded_records=1,
        failed_records=0,
        failures=[],
    )
    with patch.object(main, "PostgresReader", return_value=reader_instance):
        with patch.object(main, "_ensure_file_sink", return_value=file_sink):
            exit_code = main._run_query_first_to_api_flow(
                _base_arguments(),
                dry_run=False,
                categorization_mode=False,
            )
    assert exit_code == 0
    file_sink.reset_output.assert_called_once()
    file_sink.load_rows.assert_called_once()


def test_categorize_only_uses_query_first_request(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("DO_CATEGORIZE", "true")
    monkeypatch.setenv("CATEGORIZE_COLUMNS", "raw_value")
    reader_instance = MagicMock()
    reader_instance.fetch_rows_and_columns_by_sql.return_value = (
        [{"source_event_id": 1, "raw_value": "alpha"}],
        ["source_event_id", "raw_value"],
    )
    arguments = _base_arguments()
    arguments.target_columns_json = None
    pipeline_instance = MagicMock()
    pipeline_instance.run.return_value = CategorizationResponse(
        source=SourceInfo(database="db", schema_name="query", table="SOURCE_QUERY"),
        columns=[],
        errors=[],
    )
    with patch.object(main, "PostgresReader", return_value=reader_instance):
        with patch.object(main, "OpenAILLMCategorizer", return_value=MagicMock()):
            with patch.object(main, "ColumnCategorizationPipeline", return_value=pipeline_instance):
                exit_code = main._run_categorization_only_mode(arguments)
    assert exit_code == 0
    call_args = pipeline_instance.run.call_args
    request = call_args.args[0]
    assert request.source_query == "SELECT 1 AS source_event_id, 'v' AS raw_value"


def test_nullable_mismatch_error_message_is_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_minimal_neutral_env(monkeypatch)
    monkeypatch.setenv("LOAD_MAX_RETRIES", "0")
    reader_instance = MagicMock()
    reader_instance.fetch_rows_and_columns_by_sql.return_value = (
        [{"source_event_id": 1, "raw_value": None}],
        ["source_event_id", "raw_value"],
    )
    sink = MagicMock()
    sink.load_rows.side_effect = ValueError("null value in column \"raw_value\" violates not-null constraint")
    with patch.object(main, "PostgresReader", return_value=reader_instance):
        with patch.object(main, "_ensure_http_sink", return_value=sink):
            with pytest.raises(RuntimeError, match="null value in column"):
                main._run_query_first_to_api_flow(
                    _base_arguments(),
                    dry_run=False,
                    categorization_mode=False,
                )
