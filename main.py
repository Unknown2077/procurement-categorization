from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from column_categorization.categorization.llm_categorizer import OpenAILLMCategorizer
from column_categorization.config import load_etl_config_from_env, load_sink_config_from_env
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.pipelines.column_categorization import (
    ColumnCategorizationPipeline,
    RecordCategorizationPipeline,
)
from column_categorization.pipelines.etl_execution import EtlExecutionPipeline
from column_categorization.schemas.categorization import CategorizationRequest, RecordCategorizationRequest
from column_categorization.sinks.router import SinkRouter


def _load_dotenv_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", maxsplit=1)
            normalized_key = key.strip()
            normalized_value = value.strip().strip('"').strip("'")
            if normalized_key and normalized_value and normalized_key not in os.environ:
                os.environ[normalized_key] = normalized_value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Simple runner for categorization and ETL pipeline")
    parser.add_argument("--mode", choices=["etl", "manual"], default=None, help="etl or manual")
    parser.add_argument("--database-url", default=None, help="PostgreSQL connection string")
    parser.add_argument("--nim-api-key", default=None, help="LLM API key")
    parser.add_argument("--nim-base-url", default=None, help="LLM base URL")
    parser.add_argument("--model", default=None, help="LLM model")
    parser.add_argument("--schema-name", default="public", help="Schema name")
    parser.add_argument("--table-name", default="event_raw_staging", help="Table name")
    parser.add_argument("--batch-size", type=int, default=10, help="Values per categorization batch")

    parser.add_argument("--raw-id-column", default="source_event_id", help="ID column for ETL mode")
    parser.add_argument("--raw-value-column", default="raw_value", help="Raw text column for ETL mode")

    parser.add_argument(
        "--target-columns-json",
        default=None,
        help='Manual mode only. JSON array format: [{"name":"raw_value","description":"..."}]',
    )
    return parser


def _prompt_mode() -> str:
    print("Select mode:")
    print("1) etl    -> extract -> categorize -> load (uses SINK_TYPE from .env)")
    print("2) manual -> categorization only (no sink load)")
    selected = input("Enter choice [1/2] (default 1): ").strip()
    return "manual" if selected == "2" else "etl"


def _prompt_text(label: str, default_value: str) -> str:
    value = input(f"{label} [{default_value}]: ").strip()
    return value or default_value


def _resolve_required_runtime(arguments: argparse.Namespace) -> tuple[str, str, str | None, str]:
    database_url = arguments.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is required in .env or --database-url")

    nim_api_key = arguments.nim_api_key or os.environ.get("NIM_API_KEY")
    if not nim_api_key:
        raise ValueError("NIM_API_KEY is required in .env or --nim-api-key")

    nim_base_url = arguments.nim_base_url or os.environ.get("NIM_BASE_URL")
    model = arguments.model or os.environ.get("NIM_MODEL") or "qwen/qwen3-next-80b-a3b-instruct"
    return database_url, nim_api_key, nim_base_url, model


def _parse_manual_target_columns(target_columns_json: str) -> list[dict[str, Any]]:
    try:
        loaded = json.loads(target_columns_json)
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid target columns JSON: {error}") from error
    if not isinstance(loaded, list) or not loaded:
        raise ValueError("target columns JSON must be a non-empty array")
    return loaded


def _run_etl_mode(
    arguments: argparse.Namespace,
    database_url: str,
    nim_api_key: str,
    nim_base_url: str | None,
    model: str,
) -> int:
    reader = PostgresReader(database_url=database_url)
    categorizer = OpenAILLMCategorizer(
        api_key=nim_api_key,
        model=model,
        base_url=nim_base_url,
    )
    request = RecordCategorizationRequest(
        database_url=database_url,
        schema_name=arguments.schema_name,
        table_name=arguments.table_name,
        id_column_name=arguments.raw_id_column,
        raw_value_column_name=arguments.raw_value_column,
        batch_size=arguments.batch_size,
        model_name=model,
    )
    categorized = RecordCategorizationPipeline(reader=reader, categorizer=categorizer).run(request)
    sink = SinkRouter(sink_config=load_sink_config_from_env()).build_sink()
    execution = EtlExecutionPipeline(sink=sink, load_config=load_etl_config_from_env()).run(
        records=categorized.records,
        categorization_error_count=len(categorized.errors),
    )
    print(execution.model_dump_json(indent=2, ensure_ascii=False))
    return 0


def _run_manual_mode(
    arguments: argparse.Namespace,
    database_url: str,
    nim_api_key: str,
    nim_base_url: str | None,
    model: str,
) -> int:
    if arguments.target_columns_json:
        target_columns = _parse_manual_target_columns(arguments.target_columns_json)
    else:
        column_name = _prompt_text("Column name for categorization", arguments.raw_value_column)
        column_description = _prompt_text("Column description (optional)", "")
        target_columns = [{"name": column_name, "description": column_description or None}]

    request = CategorizationRequest(
        database_url=database_url,
        target_columns=target_columns,
        batch_size=arguments.batch_size,
        schema_name=arguments.schema_name,
        table_name=arguments.table_name,
    )
    reader = PostgresReader(database_url=database_url)
    categorizer = OpenAILLMCategorizer(
        api_key=nim_api_key,
        model=model,
        base_url=nim_base_url,
    )
    result = ColumnCategorizationPipeline(reader=reader, categorizer=categorizer).run(request)
    print(result.model_dump_json(indent=2, ensure_ascii=False))
    return 0


def main() -> int:
    _load_dotenv_file()
    parser = _build_parser()
    arguments = parser.parse_args()

    selected_mode = arguments.mode or _prompt_mode()
    if arguments.mode is None:
        arguments.schema_name = _prompt_text("Schema", arguments.schema_name)
        arguments.table_name = _prompt_text("Table", arguments.table_name)
        arguments.batch_size = int(_prompt_text("Batch size", str(arguments.batch_size)))
        if selected_mode == "etl":
            arguments.raw_id_column = _prompt_text("Raw ID column", arguments.raw_id_column)
            arguments.raw_value_column = _prompt_text("Raw value column", arguments.raw_value_column)

    database_url, nim_api_key, nim_base_url, model = _resolve_required_runtime(arguments)
    if selected_mode == "etl":
        return _run_etl_mode(arguments, database_url, nim_api_key, nim_base_url, model)
    return _run_manual_mode(arguments, database_url, nim_api_key, nim_base_url, model)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
