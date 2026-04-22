from __future__ import annotations

import argparse
import json
import os
import sys

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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run column categorization pipeline")
    parser.add_argument(
        "--database-url",
        required=False,
        default=None,
        help="PostgreSQL connection string (overrides DB_URL / DATABASE_URL from env)",
    )
    parser.add_argument(
        "--target-columns-json",
        required=False,
        default=None,
        help='JSON array format: [{"name":"...", "description":"..."}]',
    )
    parser.add_argument("--batch-size", type=int, default=100, help="Rows per categorization batch")
    parser.add_argument("--schema-name", default="public", help="Schema name")
    parser.add_argument("--table-name", default="original_data", help="Table name")
    parser.add_argument("--llm-api-key", default=None, help="LLM API key")
    parser.add_argument("--llm-base-url", default=None, help="LLM base URL")
    parser.add_argument("--llm-model", default=None, help="LLM model id")
    parser.add_argument("--nim-api-key", dest="llm_api_key", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--nim-base-url", dest="llm_base_url", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--model", dest="llm_model", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--run-etl", action="store_true", help="Run end-to-end ETL mode")
    parser.add_argument("--raw-id-column", default="source_event_id", help="Record identifier column")
    parser.add_argument("--raw-value-column", default="raw_value", help="Raw value column to categorize")
    parser.add_argument(
        "--load-batch-size",
        type=int,
        default=None,
        help="Override BATCH_PROCESS (load batch size from neutral env contract)",
    )
    parser.add_argument("--load-max-retries", type=int, default=None, help="Override LOAD_MAX_RETRIES")
    parser.add_argument(
        "--load-retry-delay-seconds",
        type=int,
        default=None,
        help="Override LOAD_RETRY_DELAY_SECONDS",
    )
    parser.add_argument(
        "--dead-letter-path",
        default=None,
        help="Override DEAD_LETTER_PATH",
    )
    return parser


def _load_dotenv_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", maxsplit=1)
            normalized_key = key.strip()
            normalized_value = value.strip().strip('"').strip("'")
            if normalized_key and normalized_value and normalized_key not in os.environ:
                os.environ[normalized_key] = normalized_value


def main() -> int:
    _load_dotenv_file()
    parser = _build_parser()
    arguments = parser.parse_args()

    database_url = (
        arguments.database_url or os.environ.get("DB_URL") or os.environ.get("DATABASE_URL")
    )
    if not database_url:
        raise ValueError("database_url is required via --database-url or DB_URL (or legacy DATABASE_URL)")

    llm_api_key = arguments.llm_api_key or os.environ.get("LLM_API_KEY") or os.environ.get("NIM_API_KEY")
    if not llm_api_key:
        raise ValueError(
            "LLM API key is required via --llm-api-key / --nim-api-key or LLM_API_KEY (or legacy NIM_API_KEY)"
        )

    llm_base_url = arguments.llm_base_url or os.environ.get("LLM_BASE_URL") or os.environ.get("NIM_BASE_URL")
    llm_model = (
        arguments.llm_model
        or os.environ.get("LLM_MODEL")
        or os.environ.get("NIM_MODEL")
        or "qwen/qwen3-next-80b-a3b-instruct"
    )

    if arguments.load_batch_size is not None:
        os.environ["BATCH_PROCESS"] = str(arguments.load_batch_size)
    if arguments.load_max_retries is not None:
        os.environ["LOAD_MAX_RETRIES"] = str(arguments.load_max_retries)
    if arguments.load_retry_delay_seconds is not None:
        os.environ["LOAD_RETRY_DELAY_SECONDS"] = str(arguments.load_retry_delay_seconds)
    if arguments.dead_letter_path is not None:
        os.environ["DEAD_LETTER_PATH"] = arguments.dead_letter_path

    reader = PostgresReader(database_url=database_url)
    categorizer = OpenAILLMCategorizer(
        api_key=llm_api_key,
        model=llm_model,
        base_url=llm_base_url,
    )

    if arguments.run_etl:
        etl_request = RecordCategorizationRequest(
            database_url=database_url,
            schema_name=arguments.schema_name,
            table_name=arguments.table_name,
            id_column_name=arguments.raw_id_column,
            raw_value_column_name=arguments.raw_value_column,
            batch_size=arguments.batch_size,
            model_name=llm_model,
        )
        categorization_response = RecordCategorizationPipeline(reader=reader, categorizer=categorizer).run(etl_request)
        sink_config = load_sink_config_from_env()
        etl_config = load_etl_config_from_env()
        sink = SinkRouter(sink_config=sink_config).build_sink()
        execution_response = EtlExecutionPipeline(sink=sink, load_config=etl_config).run(
            records=categorization_response.records,
            categorization_error_count=len(categorization_response.errors),
        )
        print(execution_response.model_dump_json(indent=2, ensure_ascii=False))
        return 0

    if arguments.target_columns_json is None:
        raise ValueError("--target-columns-json is required when --run-etl is not used")

    try:
        target_columns = json.loads(arguments.target_columns_json)
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid --target-columns-json: {error}") from error

    request = CategorizationRequest(
        database_url=database_url,
        target_columns=target_columns,
        batch_size=arguments.batch_size,
        schema_name=arguments.schema_name,
        table_name=arguments.table_name,
    )
    pipeline = ColumnCategorizationPipeline(reader=reader, categorizer=categorizer)
    response = pipeline.run(request)
    print(response.model_dump_json(indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
