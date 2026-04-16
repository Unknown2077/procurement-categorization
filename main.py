from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib import error, request

from column_categorization.categorization.llm_categorizer import OpenAILLMCategorizer
from column_categorization.config import (
    load_etl_config_from_env,
    load_sink_config_from_env,
    resolve_env_template,
)
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.pipelines.column_categorization import (
    ColumnCategorizationPipeline,
    RecordCategorizationPipeline,
)
from column_categorization.pipelines.etl_execution import EtlExecutionPipeline
from column_categorization.pipelines.raw_to_api import RawDbToApiPipeline, RawToApiRequest
from column_categorization.schemas.categorization import CategorizationRequest, RecordCategorizationRequest
from column_categorization.schemas.load import EtlExecutionResult, LoadResult
from column_categorization.sinks.http_api_sink import HttpApiSink
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
    parser = argparse.ArgumentParser(description="Interactive multi-flow runner")
    parser.add_argument(
        "--mode",
        choices=["raw_to_api", "categorize_to_api", "manual", "etl", "api_check"],
        default=None,
        help="raw_to_api, categorize_to_api, manual, api_check, or etl (alias for categorize_to_api)",
    )
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
        "--raw-columns",
        default=None,
        help="Comma-separated DB columns to send directly in raw_to_api mode",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Prepare and preview payload without sending to API",
    )

    parser.add_argument(
        "--target-columns-json",
        default=None,
        help='Manual mode only. JSON array format: [{"name":"raw_value","description":"..."}]',
    )
    parser.add_argument(
        "--api-check-mode",
        choices=["me", "account", "datasets"],
        default="me",
        help="api_check mode only: me, account, or datasets",
    )
    parser.add_argument(
        "--account-uid",
        default=None,
        help="account UID for api_check mode",
    )
    parser.add_argument(
        "--api-base-url",
        default=None,
        help="API base URL for api_check mode",
    )
    parser.add_argument(
        "--api-token",
        default=None,
        help="Bearer token for api_check mode",
    )
    parser.add_argument(
        "--api-timeout-seconds",
        type=int,
        default=30,
        help="api_check mode only: HTTP timeout in seconds",
    )
    parser.add_argument(
        "--api-output-dir",
        default="outputs/api",
        help="api_check mode only: output directory for JSON response",
    )
    return parser


def _prompt_mode() -> str:
    print("Select flow:")
    print("1) raw_to_api        -> DB raw rows -> Erica API")
    print("2) categorize_to_api -> DB -> categorize -> Erica API")
    print("3) manual            -> prompt-driven ad-hoc mode")
    print("4) api_check         -> GET /me, /account, /datasets")
    selected = input("Enter choice [1/2/3/4] (default 2): ").strip()
    if selected == "1":
        return "raw_to_api"
    if selected == "3":
        return "manual"
    if selected == "4":
        return "api_check"
    return "categorize_to_api"


def _prompt_text(label: str, default_value: str) -> str:
    value = input(f"{label} [{default_value}]: ").strip()
    return value or default_value


def _normalize_mode(value: str) -> str:
    return "categorize_to_api" if value == "etl" else value


def _resolve_database_url(arguments: argparse.Namespace) -> str:
    database_url = arguments.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is required in .env or --database-url")
    return database_url


def _resolve_llm_runtime(arguments: argparse.Namespace) -> tuple[str, str | None, str]:
    nim_api_key = arguments.nim_api_key or os.environ.get("NIM_API_KEY")
    if not nim_api_key:
        raise ValueError("NIM_API_KEY is required in .env or --nim-api-key")

    nim_base_url = arguments.nim_base_url or os.environ.get("NIM_BASE_URL")
    model = arguments.model or os.environ.get("NIM_MODEL") or "qwen/qwen3-next-80b-a3b-instruct"
    return nim_api_key, nim_base_url, model


def _resolve_api_base_url(arguments: argparse.Namespace) -> str:
    base_url = arguments.api_base_url or os.environ.get("API_BASE_URL") or os.environ.get("SINK_HTTP_BASE_URL")
    if not base_url:
        raise ValueError("API base URL is required via --api-base-url, API_BASE_URL, or SINK_HTTP_BASE_URL")
    return base_url.rstrip("/")


def _resolve_api_token(arguments: argparse.Namespace) -> str:
    api_token = arguments.api_token or os.environ.get("SINK_HTTP_AUTH_TOKEN")
    if not api_token:
        raise ValueError("API token is required via --api-token or SINK_HTTP_AUTH_TOKEN")
    return api_token


def _resolve_account_uid(arguments: argparse.Namespace) -> str:
    account_uid = arguments.account_uid or os.environ.get("ACCOUNT_UID")
    if not account_uid:
        raise ValueError("ACCOUNT_UID is required for account/datasets mode")
    return account_uid


def _build_api_template_context(arguments: argparse.Namespace, require_account_uid: bool) -> dict[str, str]:
    context: dict[str, str] = {}
    account_uid = arguments.account_uid or os.environ.get("ACCOUNT_UID")
    if require_account_uid and not account_uid:
        raise ValueError("ACCOUNT_UID is required for account/datasets mode")
    if account_uid:
        context["account_uid"] = account_uid
    dataset_uid = os.environ.get("DATASET_UID")
    if dataset_uid:
        context["dataset_uid"] = dataset_uid
    return context


def _build_api_check_url(arguments: argparse.Namespace) -> str:
    base_url = _resolve_api_base_url(arguments)
    if arguments.api_check_mode == "me":
        path = resolve_env_template(
            env_key="API_PATH_AUTH_ME",
            required=True,
            context=_build_api_template_context(arguments, require_account_uid=False),
        )
    elif arguments.api_check_mode == "datasets":
        path = resolve_env_template(
            env_key="API_PATH_DATASETS",
            required=True,
            context=_build_api_template_context(arguments, require_account_uid=True),
        )
    else:
        path = resolve_env_template(
            env_key="API_PATH_ACCOUNT",
            required=True,
            context=_build_api_template_context(arguments, require_account_uid=True),
        )
    if path is None:
        raise ValueError("API path resolution failed")
    return f"{base_url}/{path.lstrip('/')}"


def _build_api_check_output_path(arguments: argparse.Namespace) -> Path:
    output_dir = Path(arguments.api_output_dir)
    if arguments.api_check_mode == "me":
        return output_dir / "getme.json"
    account_uid = _resolve_account_uid(arguments)
    if arguments.api_check_mode == "datasets":
        return output_dir / f"datasets_{account_uid}.json"
    return output_dir / f"account_{account_uid}.json"


def _run_api_check_mode(arguments: argparse.Namespace) -> int:
    url = _build_api_check_url(arguments)
    token = _resolve_api_token(arguments)
    api_request = request.Request(
        url=url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with request.urlopen(api_request, timeout=arguments.api_timeout_seconds) as response:
            status_code = response.status
            body_text = response.read().decode("utf-8", errors="replace")
    except error.HTTPError as http_error:
        status_code = http_error.code
        body_text = http_error.read().decode("utf-8", errors="replace")
    except error.URLError as url_error:
        raise ValueError(f"Request failed: {url_error.reason}") from url_error

    output_path = _build_api_check_output_path(arguments)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        parsed_body = json.loads(body_text)
    except json.JSONDecodeError:
        payload: dict[str, Any] = {"status_code": status_code, "body_text": body_text}
    else:
        payload = {"status_code": status_code, "body": parsed_body}
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"URL: {url}")
    print(f"Status: {status_code}")
    print(f"Saved: {output_path}")
    try:
        print(json.dumps(json.loads(body_text), indent=2, ensure_ascii=False))
    except json.JSONDecodeError:
        print(body_text)
    return 0 if 200 <= status_code < 300 else 1


def _parse_manual_target_columns(target_columns_json: str) -> list[dict[str, Any]]:
    try:
        loaded = json.loads(target_columns_json)
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid target columns JSON: {error}") from error
    if not isinstance(loaded, list) or not loaded:
        raise ValueError("target columns JSON must be a non-empty array")
    return loaded


def _parse_raw_columns(raw_columns: str | None, fallback_columns: list[str]) -> list[str]:
    if raw_columns is None:
        return fallback_columns
    parsed_columns = [column.strip() for column in raw_columns.split(",") if column.strip()]
    if not parsed_columns:
        raise ValueError("raw_columns must contain at least one column name")
    return parsed_columns


def _ensure_http_sink() -> HttpApiSink:
    sink_config = load_sink_config_from_env()
    if sink_config.sink_type != "http":
        raise ValueError("SINK_TYPE must be set to http for API flows")
    if sink_config.http_path is None:
        raise ValueError("SINK_HTTP_PATH is required for API flows")
    if "{" in sink_config.http_path or "}" in sink_config.http_path:
        raise ValueError("SINK_HTTP_PATH contains unresolved template placeholders")
    sink = SinkRouter(sink_config=sink_config).build_sink()
    if not isinstance(sink, HttpApiSink):
        raise ValueError("Expected HttpApiSink for SINK_TYPE=http")
    return sink


def _summarize_load_result(
    load_result: LoadResult | None,
    max_failures: int = 5,
) -> dict[str, Any] | None:
    if load_result is None:
        return None
    return {
        "sink_type": load_result.sink_type,
        "total_records": load_result.total_records,
        "loaded_records": load_result.loaded_records,
        "failed_records": load_result.failed_records,
        "failure_preview": [failure.model_dump(mode="json") for failure in load_result.failures[:max_failures]],
    }


def _summarize_execution_result(
    execution: EtlExecutionResult,
    max_failures: int = 5,
    max_dead_letter_records: int = 3,
) -> dict[str, Any]:
    return {
        "flow": "categorize_to_api",
        "total_categorized_records": execution.total_categorized_records,
        "total_errors": execution.total_errors,
        "load_result": _summarize_load_result(execution.load_result, max_failures=max_failures),
        "dead_letter_count": len(execution.dead_letter_records),
        "dead_letter_preview": [
            record.model_dump(mode="json") for record in execution.dead_letter_records[:max_dead_letter_records]
        ],
    }


def _run_raw_to_api_mode(
    arguments: argparse.Namespace,
    database_url: str,
    dry_run: bool,
) -> int:
    reader = PostgresReader(database_url=database_url)
    sink = _ensure_http_sink()
    raw_columns = _parse_raw_columns(
        arguments.raw_columns,
        fallback_columns=[arguments.raw_id_column, arguments.raw_value_column],
    )
    request = RawToApiRequest(
        schema_name=arguments.schema_name,
        table_name=arguments.table_name,
        columns=raw_columns,
        order_by_column=arguments.raw_id_column,
        dry_run=dry_run,
    )
    result = RawDbToApiPipeline(reader=reader, sink=sink, load_config=load_etl_config_from_env()).run(request)
    output = {
        "flow": "raw_to_api",
        "dry_run": result.dry_run,
        "total_rows": result.total_rows,
        "sample_rows": result.sample_rows,
        "load_result": _summarize_load_result(result.load_result),
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


def _run_categorize_to_api_mode(
    arguments: argparse.Namespace,
    database_url: str,
    nim_api_key: str,
    nim_base_url: str | None,
    model: str,
    dry_run: bool,
) -> int:
    _ensure_http_sink()
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
    if dry_run:
        output = {
            "flow": "categorize_to_api",
            "dry_run": True,
            "total_input_rows": categorized.total_input_rows,
            "total_distinct_values": categorized.total_distinct_values,
            "total_categorized_records": len(categorized.records),
            "errors": [error.model_dump(mode="json") for error in categorized.errors],
            "sample_records": [record.model_dump(mode="json") for record in categorized.records[:3]],
        }
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return 0
    sink = _ensure_http_sink()
    execution = EtlExecutionPipeline(sink=sink, load_config=load_etl_config_from_env()).run(
        records=categorized.records,
        categorization_error_count=len(categorized.errors),
    )
    print(json.dumps(_summarize_execution_result(execution), indent=2, ensure_ascii=False))
    return 0


def _run_categorization_only_mode(
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


def _run_manual_mode(arguments: argparse.Namespace) -> int:
    print("Manual action:")
    print("1) raw_to_api")
    print("2) categorize_to_api")
    print("3) categorize_only")
    print("4) api_check")
    selected_action = input("Enter action [1/2/3/4] (default 3): ").strip()
    if selected_action == "1":
        database_url = _resolve_database_url(arguments)
        run_dry = _prompt_text("Dry run (yes/no)", "yes").lower() == "yes"
        return _run_raw_to_api_mode(arguments, database_url=database_url, dry_run=run_dry)
    if selected_action == "4":
        arguments.api_check_mode = _prompt_text("API check mode (me/account/datasets)", arguments.api_check_mode)
        if arguments.api_check_mode not in {"me", "account", "datasets"}:
            raise ValueError("API check mode must be one of: me, account, datasets")
        return _run_api_check_mode(arguments)

    nim_api_key, nim_base_url, model = _resolve_llm_runtime(arguments)
    if selected_action == "2":
        database_url = _resolve_database_url(arguments)
        run_dry = _prompt_text("Dry run (yes/no)", "yes").lower() == "yes"
        return _run_categorize_to_api_mode(
            arguments=arguments,
            database_url=database_url,
            nim_api_key=nim_api_key,
            nim_base_url=nim_base_url,
            model=model,
            dry_run=run_dry,
        )
    database_url = _resolve_database_url(arguments)
    return _run_categorization_only_mode(
        arguments=arguments,
        database_url=database_url,
        nim_api_key=nim_api_key,
        nim_base_url=nim_base_url,
        model=model,
    )


def main() -> int:
    _load_dotenv_file()
    parser = _build_parser()
    arguments = parser.parse_args()

    selected_mode = _normalize_mode(arguments.mode or _prompt_mode())

    if arguments.mode is None:
        if selected_mode == "api_check":
            arguments.api_check_mode = _prompt_text("API check mode (me/account/datasets)", arguments.api_check_mode)
            if arguments.api_check_mode not in {"me", "account", "datasets"}:
                raise ValueError("API check mode must be one of: me, account, datasets")
            return _run_api_check_mode(arguments)
        arguments.schema_name = _prompt_text("Schema", arguments.schema_name)
        arguments.table_name = _prompt_text("Table", arguments.table_name)
        arguments.batch_size = int(_prompt_text("Batch size", str(arguments.batch_size)))
        if selected_mode in {"raw_to_api", "categorize_to_api"}:
            arguments.raw_id_column = _prompt_text("Raw ID column", arguments.raw_id_column)
            arguments.raw_value_column = _prompt_text("Raw value column", arguments.raw_value_column)
            if selected_mode == "raw_to_api":
                arguments.raw_columns = _prompt_text(
                    "Raw columns (comma-separated)",
                    f"{arguments.raw_id_column},{arguments.raw_value_column}",
                )

    if selected_mode == "api_check":
        return _run_api_check_mode(arguments)
    if selected_mode == "raw_to_api":
        database_url = _resolve_database_url(arguments)
        return _run_raw_to_api_mode(arguments=arguments, database_url=database_url, dry_run=arguments.dry_run)
    if selected_mode == "categorize_to_api":
        database_url = _resolve_database_url(arguments)
        nim_api_key, nim_base_url, model = _resolve_llm_runtime(arguments)
        return _run_categorize_to_api_mode(
            arguments=arguments,
            database_url=database_url,
            nim_api_key=nim_api_key,
            nim_base_url=nim_base_url,
            model=model,
            dry_run=arguments.dry_run,
        )
    return _run_manual_mode(arguments=arguments)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
