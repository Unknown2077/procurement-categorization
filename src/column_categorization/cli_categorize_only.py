from __future__ import annotations

import argparse

from column_categorization.categorization.llm_categorizer import OpenAILLMCategorizer
from column_categorization.cli_common import (
    _fetch_source_rows_with_retry,
    _find_missing_columns,
    _parse_categorized_columns,
    _parse_manual_target_columns,
    _resolve_effective_raw_id_column,
    _resolve_llm_runtime,
)
from column_categorization.cli_interactive import _prompt_interactive_categorize_column_selection
from column_categorization.cli_resolution import (
    _require_source_query,
    _resolve_database_url,
    _resolve_effective_batch_size,
    _resolve_source_query,
    _resolve_source_row_limit,
)
from column_categorization.config import NeutralRuntimeConfig, load_neutral_runtime_config_from_env
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.pipelines.column_categorization import ColumnCategorizationPipeline
from column_categorization.schemas.categorization import CategorizationRequest, TargetColumn


def _categorize_only_fetch_and_resolve_source_context(
    arguments: argparse.Namespace,
    *,
    database_url: str,
    source_sql: str,
    source_row_limit: int,
    interactive_mode: bool,
) -> tuple[PostgresReader, str, list[str], list[dict[str, object]], str]:
    reader = PostgresReader(database_url=database_url)
    source_rows, source_columns, source_sql = _fetch_source_rows_with_retry(
        reader,
        source_sql=source_sql,
        interactive_mode=interactive_mode,
    )
    limited_source_rows = source_rows[:source_row_limit]
    effective_raw_id_column = _resolve_effective_raw_id_column(
        preferred_column=arguments.raw_id_column,
        source_columns=source_columns,
        source_rows=limited_source_rows,
    )
    return reader, source_sql, source_columns, limited_source_rows, effective_raw_id_column


def _categorize_only_resolve_target_columns(
    arguments: argparse.Namespace,
    neutral: NeutralRuntimeConfig,
    *,
    source_columns: list[str],
    limited_source_rows: list[dict[str, object]],
    effective_raw_id_column: str,
    interactive_mode: bool,
) -> list[TargetColumn]:
    if arguments.target_columns_json:
        parsed_specs = _parse_manual_target_columns(arguments.target_columns_json)
        categorize_names = [str(spec["name"]) for spec in parsed_specs]
        missing_specs = _find_missing_columns(source_columns, categorize_names)
        if missing_specs:
            available_text = ", ".join(source_columns)
            raise ValueError(
                "Column(s) not found in SOURCE_QUERY result: "
                f"{', '.join(missing_specs)}. Valid column names: {available_text}"
            )
        return [TargetColumn.model_validate(spec) for spec in parsed_specs]
    if neutral.categorize_columns:
        categorize_column_names = list(neutral.categorize_columns)
        missing = _find_missing_columns(source_columns, categorize_column_names)
        if missing:
            if interactive_mode:
                print(
                    "Configured categorize columns not found in SOURCE_QUERY result: "
                    f"{', '.join(missing)}"
                )
                print("Switching to interactive categorized-column selection.")
                categorize_column_names = _prompt_interactive_categorize_column_selection(
                    source_columns=source_columns,
                    source_rows=limited_source_rows,
                    id_column=effective_raw_id_column,
                )
            else:
                available_text = ", ".join(source_columns)
                raise ValueError(
                    "Column(s) not found in SOURCE_QUERY result: "
                    f"{', '.join(missing)}. Valid column names: {available_text}"
                )
        return [TargetColumn(name=name, description=None) for name in categorize_column_names]
    if arguments.categorized_columns is not None:
        categorize_column_names = _parse_categorized_columns(
            arguments.categorized_columns,
            fallback_column=arguments.raw_value_column,
        )
        missing = _find_missing_columns(source_columns, categorize_column_names)
        if missing:
            if interactive_mode:
                print(
                    "Configured categorize columns not found in SOURCE_QUERY result: "
                    f"{', '.join(missing)}"
                )
                print("Switching to interactive categorized-column selection.")
                categorize_column_names = _prompt_interactive_categorize_column_selection(
                    source_columns=source_columns,
                    source_rows=limited_source_rows,
                    id_column=effective_raw_id_column,
                )
            else:
                available_text = ", ".join(source_columns)
                raise ValueError(
                    "Column(s) not found in SOURCE_QUERY result: "
                    f"{', '.join(missing)}. Valid column names: {available_text}"
                )
        return [TargetColumn(name=name, description=None) for name in categorize_column_names]
    if interactive_mode:
        categorize_column_names = _prompt_interactive_categorize_column_selection(
            source_columns=source_columns,
            source_rows=limited_source_rows,
            id_column=effective_raw_id_column,
        )
        return [TargetColumn(name=name, description=None) for name in categorize_column_names]
    raise ValueError(
        "At least one column to categorize is required: set CATEGORIZE_COLUMNS, "
        "--categorized-columns, --target-columns-json, or run interactively."
    )


def _categorize_only_build_request_and_run_pipeline(
    reader: PostgresReader,
    *,
    nim_api_key: str,
    nim_base_url: str | None,
    model: str,
    batch_size: int,
    database_url: str,
    source_sql: str,
    source_columns: list[str],
    limited_source_rows: list[dict[str, object]],
    source_row_limit: int,
    target_columns: list[TargetColumn],
) -> int:
    request = CategorizationRequest(
        database_url=database_url,
        target_columns=target_columns,
        batch_size=batch_size,
        source_query=source_sql,
        source_row_limit=source_row_limit,
        prefetched_query_rows=limited_source_rows,
        prefetched_query_columns=source_columns,
    )
    categorizer = OpenAILLMCategorizer(
        api_key=nim_api_key,
        model=model,
        base_url=nim_base_url,
    )
    result = ColumnCategorizationPipeline(reader=reader, categorizer=categorizer).run(request)
    print(result.model_dump_json(indent=2, ensure_ascii=False))
    return 0


def _run_categorization_only_mode(arguments: argparse.Namespace) -> int:
    neutral = load_neutral_runtime_config_from_env()
    database_url = _resolve_database_url(arguments, neutral)
    source_sql = _require_source_query(_resolve_source_query(arguments, neutral))
    nim_api_key, nim_base_url, model = _resolve_llm_runtime(arguments, neutral)
    batch_size = _resolve_effective_batch_size(arguments, neutral)
    source_row_limit = _resolve_source_row_limit(arguments, neutral)
    interactive_mode = bool(getattr(arguments, "interactive_mode", False))

    reader, source_sql, source_columns, limited_source_rows, effective_raw_id_column = (
        _categorize_only_fetch_and_resolve_source_context(
            arguments,
            database_url=database_url,
            source_sql=source_sql,
            source_row_limit=source_row_limit,
            interactive_mode=interactive_mode,
        )
    )
    target_columns = _categorize_only_resolve_target_columns(
        arguments,
        neutral,
        source_columns=source_columns,
        limited_source_rows=limited_source_rows,
        effective_raw_id_column=effective_raw_id_column,
        interactive_mode=interactive_mode,
    )
    return _categorize_only_build_request_and_run_pipeline(
        reader,
        nim_api_key=nim_api_key,
        nim_base_url=nim_base_url,
        model=model,
        batch_size=batch_size,
        database_url=database_url,
        source_sql=source_sql,
        source_columns=source_columns,
        limited_source_rows=limited_source_rows,
        source_row_limit=source_row_limit,
        target_columns=target_columns,
    )
