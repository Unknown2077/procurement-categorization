from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any

from column_categorization.categorization.llm_categorizer import (
    CategorizationBatchResult,
    OpenAILLMCategorizer,
)
from column_categorization.api_check import (
    api_check_get,
    build_api_check_output_path,
    build_api_check_url,
    resolve_api_check_base_url,
    resolve_api_check_bearer_token,
    resolve_api_check_output_dir,
    resolve_api_check_timeout_seconds,
    write_api_check_result_file,
)
from column_categorization.config import (
    NeutralRuntimeConfig,
    load_etl_config_from_env,
    load_neutral_runtime_config_from_env,
    load_sink_config_from_env,
)
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.pipelines.column_categorization import (
    ColumnCategorizationPipeline,
)
from column_categorization.schemas.categorization import CategorizationRequest
from column_categorization.schemas.load import LoadFailure, LoadResult
from column_categorization.sinks.file_sink import FileSink
from column_categorization.sinks.http_api_sink import HttpApiSink
from column_categorization.sinks.router import SinkRouter
from column_categorization.utils.batching import chunked


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
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL connection string (overrides DB_URL from neutral env)",
    )
    parser.add_argument("--llm-api-key", default=None, help="LLM API key")
    parser.add_argument("--llm-base-url", default=None, help="LLM base URL")
    parser.add_argument("--llm-model", default=None, help="LLM model")
    parser.add_argument("--nim-api-key", dest="llm_api_key", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--nim-base-url", dest="llm_base_url", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--model", dest="llm_model", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--schema-name", default="public", help="Schema name")
    parser.add_argument("--table-name", default="event_raw_staging", help="Table name")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Values per categorization batch (defaults to BATCH_PROCESS from neutral env)",
    )
    parser.add_argument(
        "--source-sql",
        default=None,
        help="SQL query for source extraction (overrides SOURCE_QUERY from neutral env)",
    )

    parser.add_argument("--raw-id-column", default="source_event_id", help="ID column for ETL mode")
    parser.add_argument("--raw-value-column", default="raw_value", help="Raw text column for ETL mode")
    parser.add_argument(
        "--raw-columns",
        default=None,
        help="Comma-separated DB columns to read from source table",
    )
    parser.add_argument(
        "--categorized-columns",
        default=None,
        help="Comma-separated source columns to categorize and append as <column>_categorized",
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
        default=None,
        help="api_check mode only: HTTP timeout in seconds (falls back to API_CHECK_TIMEOUT_SECONDS or 30)",
    )
    parser.add_argument(
        "--api-output-dir",
        default=None,
        help="api_check mode only: output directory for JSON response (falls back to API_CHECK_OUTPUT_DIR or outputs/api)",
    )
    return parser


def _prompt_mode() -> str:
    print("Select flow:")
    print("1) categorize_to_api (recommended)")
    print("2) raw_to_api")
    print("3) categorize_only")
    print("4) api_check")
    selected = input("Enter choice [1/2/3/4] (default 1): ").strip()
    if selected == "2":
        return "raw_to_api"
    if selected == "3":
        return "categorize_only"
    if selected == "4":
        return "api_check"
    return "categorize_to_api"


def _prompt_yes_no(label: str, default_yes: bool = True) -> bool:
    default_value = "yes" if default_yes else "no"
    return _prompt_text(label, default_value).lower() == "yes"


def _configure_interactive_query_first_mode(arguments: argparse.Namespace, selected_mode: str) -> None:
    arguments.source_sql = _prompt_text(
        "SOURCE_QUERY / SQL (blank uses SOURCE_QUERY from .env)",
        arguments.source_sql or "",
    )
    if not _prompt_yes_no("Customize advanced column options? (yes/no)", default_yes=False):
        arguments.dry_run = _prompt_yes_no("Dry run (yes/no)", default_yes=True)
        print(f"Dry run: {'enabled' if arguments.dry_run else 'disabled'} (no API load when enabled)")
        return
    arguments.raw_id_column = _prompt_text("Raw ID column", arguments.raw_id_column)
    arguments.raw_value_column = _prompt_text("Raw value column", arguments.raw_value_column)
    default_raw_columns = f"{arguments.raw_id_column},{arguments.raw_value_column}"
    arguments.raw_columns = _prompt_text("Source columns (comma-separated)", arguments.raw_columns or default_raw_columns)
    if selected_mode == "categorize_to_api":
        arguments.categorized_columns = _prompt_text(
            "Columns to categorize (comma-separated)",
            arguments.categorized_columns or arguments.raw_value_column,
        )
    arguments.batch_size = int(_prompt_text("Batch size", str(arguments.batch_size or 10)))
    arguments.dry_run = _prompt_yes_no("Dry run (yes/no)", default_yes=True)
    print(f"Dry run: {'enabled' if arguments.dry_run else 'disabled'} (no API load when enabled)")


def _prompt_text(label: str, default_value: str) -> str:
    value = input(f"{label} [{default_value}]: ").strip()
    return value or default_value


def _normalize_mode(value: str) -> str:
    return "categorize_to_api" if value == "etl" else value


def _resolve_database_url(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> str:
    database_url = arguments.database_url or neutral.db_url
    if not database_url:
        raise ValueError("DB_URL is required in .env or --database-url")
    return database_url


def _resolve_source_query(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> str | None:
    cli_query = arguments.source_sql
    if isinstance(cli_query, str) and cli_query.strip():
        return cli_query.strip()
    neutral_query = neutral.source_query
    if neutral_query is not None and neutral_query.strip():
        return neutral_query.strip()
    return None


def _require_source_query(resolved_query: str | None) -> str:
    if resolved_query is None or not resolved_query.strip():
        raise ValueError(
            "SOURCE_QUERY is required for query-first extraction. "
            "Set SOURCE_QUERY in the environment or pass --source-sql."
        )
    return resolved_query.strip()


def _resolve_effective_batch_size(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> int:
    if arguments.batch_size is not None:
        return arguments.batch_size
    return neutral.batch_process


def _resolve_categorize_column_names(
    arguments: argparse.Namespace,
    neutral: NeutralRuntimeConfig,
    apply_categorization: bool,
) -> list[str]:
    if not apply_categorization:
        return []
    if neutral.categorize_columns:
        return list(neutral.categorize_columns)
    return _parse_categorized_columns(
        arguments.categorized_columns,
        fallback_column=arguments.raw_value_column,
    )


def _resolve_llm_runtime(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> tuple[str, str | None, str]:
    llm_api_key = getattr(arguments, "llm_api_key", None) or neutral.llm_api_key
    if not llm_api_key:
        raise ValueError("LLM_API_KEY is required in .env or --llm-api-key")
    llm_base_url = getattr(arguments, "llm_base_url", None) or neutral.llm_base_url
    llm_model = getattr(arguments, "llm_model", None) or neutral.llm_model
    return llm_api_key, llm_base_url, llm_model


def _run_api_check_mode(arguments: argparse.Namespace) -> int:
    base_url = resolve_api_check_base_url(
        arguments.api_base_url,
        option_hint="--api-base-url",
    )
    token = resolve_api_check_bearer_token(
        arguments.api_token,
        option_hint="--api-token",
    )
    url = build_api_check_url(
        base_url=base_url,
        mode=arguments.api_check_mode,
        account_uid_cli=arguments.account_uid,
    )
    timeout_seconds = resolve_api_check_timeout_seconds(
        arguments.api_timeout_seconds,
        timeout_flag_hint="--api-timeout-seconds",
    )
    status_code, body_text = api_check_get(
        url=url,
        token=token,
        timeout_seconds=timeout_seconds,
    )
    output_dir = resolve_api_check_output_dir(arguments.api_output_dir)
    output_path = build_api_check_output_path(
        mode=arguments.api_check_mode,
        output_dir=output_dir,
        account_uid_cli=arguments.account_uid,
    )
    write_api_check_result_file(
        path=output_path,
        status_code=status_code,
        response_body=body_text,
    )

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


def _parse_categorized_columns(categorized_columns: str | None, fallback_column: str) -> list[str]:
    if categorized_columns is None:
        return [fallback_column]
    parsed_columns = [column.strip() for column in categorized_columns.split(",") if column.strip()]
    if not parsed_columns:
        raise ValueError("categorized_columns must contain at least one column name")
    return parsed_columns


def _ensure_required_columns(
    available_columns: list[str],
    required_columns: list[str],
    source_label: str,
) -> None:
    missing_columns = sorted({column for column in required_columns if column not in set(available_columns)})
    if missing_columns:
        missing_text = ", ".join(missing_columns)
        raise ValueError(
            f"{source_label} is missing required columns: {missing_text}. "
            "Ensure SOURCE_QUERY (or --source-sql) returns matching column aliases for "
            "--raw-columns, --raw-id-column, and categorization columns when applicable."
        )


def _serialize_categorized_labels(labels: list[str]) -> str:
    return json.dumps(labels, ensure_ascii=False)


def _ensure_http_sink() -> HttpApiSink:
    sink_config = load_sink_config_from_env()
    if sink_config.sink_type != "http":
        raise ValueError("TARGET_TYPE must be set to api for API flows")
    if sink_config.http_path is None:
        raise ValueError("TARGET_TYPE=api requires TARGET_API_BASE_URL, TARGET_ACCOUNT_UID, and TARGET_DATASET_UID")
    if "{" in sink_config.http_path or "}" in sink_config.http_path:
        raise ValueError("Resolved API path contains unresolved template placeholders")
    sink = SinkRouter(sink_config=sink_config).build_sink()
    if not isinstance(sink, HttpApiSink):
        raise ValueError("Expected HttpApiSink for TARGET_TYPE=api")
    return sink


def _ensure_file_sink() -> FileSink:
    sink_config = load_sink_config_from_env()
    if sink_config.sink_type != "file":
        raise ValueError("TARGET_TYPE must be set to json for JSON target flows")
    sink = SinkRouter(sink_config=sink_config).build_sink()
    if not isinstance(sink, FileSink):
        raise ValueError("Expected FileSink for TARGET_TYPE=json")
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


def _load_rows_with_retry(
    sink: HttpApiSink,
    rows_batch: list[dict[str, object]],
    load_max_retries: int,
    retry_delay_seconds: int,
) -> LoadResult:
    last_error: Exception | None = None
    for attempt in range(load_max_retries + 1):
        try:
            return sink.load_rows(rows_batch)
        except Exception as error:
            last_error = error
            if attempt >= load_max_retries:
                break
            if retry_delay_seconds > 0:
                time.sleep(retry_delay_seconds)
    if last_error is None:
        raise RuntimeError("Load retry finished without result and without exception")
    raise RuntimeError(f"Load failed after {load_max_retries + 1} attempts: {last_error}")


def _load_output_rows(output_rows: list[dict[str, object]]) -> LoadResult:
    sink_config = load_sink_config_from_env()
    if sink_config.sink_type == "http":
        sink = _ensure_http_sink()
        load_config = load_etl_config_from_env()
        aggregate_failures: list[LoadFailure] = []
        loaded_records = 0
        for rows_batch in chunked(output_rows, load_config.load_batch_size):
            batch_result = _load_rows_with_retry(
                sink=sink,
                rows_batch=rows_batch,
                load_max_retries=load_config.load_max_retries,
                retry_delay_seconds=load_config.load_retry_delay_seconds,
            )
            loaded_records += batch_result.loaded_records
            aggregate_failures.extend(batch_result.failures)
        return LoadResult(
            sink_type="http",
            total_records=len(output_rows),
            loaded_records=loaded_records,
            failed_records=len(aggregate_failures),
            failures=aggregate_failures,
        )
    file_sink = _ensure_file_sink()
    file_sink.reset_output()
    return file_sink.load_rows(output_rows)


def _categorize_batch_with_retries(
    categorizer: OpenAILLMCategorizer,
    *,
    categorized_column: str,
    batch_index: int,
    values_batch: list[str],
    categorize_max_retries: int,
) -> CategorizationBatchResult:
    last_error: BaseException | None = None
    for attempt in range(categorize_max_retries + 1):
        try:
            return categorizer.categorize_batch(
                column_name=categorized_column,
                column_description=None,
                raw_values=values_batch,
            )
        except BaseException as error:
            last_error = error
            if attempt >= categorize_max_retries:
                break
    raise RuntimeError(
        f"Categorization failed for column={categorized_column!r} batch_index={batch_index} "
        f"after {categorize_max_retries + 1} attempt(s): {last_error!r}"
    ) from last_error


def _run_query_first_to_api_flow(
    arguments: argparse.Namespace,
    *,
    dry_run: bool,
    categorization_mode: bool,
) -> int:
    neutral = load_neutral_runtime_config_from_env()
    database_url = _resolve_database_url(arguments, neutral)
    source_sql = _require_source_query(_resolve_source_query(arguments, neutral))
    apply_categorization = categorization_mode and neutral.do_categorize
    categorize_column_names = _resolve_categorize_column_names(arguments, neutral, apply_categorization)
    if apply_categorization and not categorize_column_names:
        raise ValueError(
            "DO_CATEGORIZE=true requires at least one column: set CATEGORIZE_COLUMNS or --categorized-columns."
        )
    batch_size = _resolve_effective_batch_size(arguments, neutral)

    reader = PostgresReader(database_url=database_url)
    raw_columns = _parse_raw_columns(
        arguments.raw_columns,
        fallback_columns=[arguments.raw_id_column, arguments.raw_value_column],
    )
    selected_columns = list(raw_columns)
    for categorized_column in categorize_column_names:
        if categorized_column not in selected_columns:
            selected_columns.append(categorized_column)

    source_rows, source_columns = reader.fetch_rows_and_columns_by_sql(source_sql)
    source_label = "SOURCE_QUERY result"
    if apply_categorization:
        _ensure_required_columns(
            available_columns=source_columns,
            required_columns=list(
                dict.fromkeys(selected_columns + categorize_column_names + [arguments.raw_id_column])
            ),
            source_label=source_label,
        )
    else:
        _ensure_required_columns(
            available_columns=source_columns,
            required_columns=list(dict.fromkeys(raw_columns + [arguments.raw_id_column])),
            source_label=source_label,
        )

    projected_selected = [{column: source_row[column] for column in selected_columns} for source_row in source_rows]
    output_rows = [dict(row) for row in projected_selected]

    if apply_categorization:
        nim_api_key, nim_base_url, model = _resolve_llm_runtime(arguments, neutral)
        categorizer = OpenAILLMCategorizer(api_key=nim_api_key, model=model, base_url=nim_base_url)
        for categorized_column in categorize_column_names:
            distinct_values = sorted(
                {
                    value.strip()
                    for source_row in projected_selected
                    for value in [source_row.get(categorized_column)]
                    if isinstance(value, str) and value.strip()
                }
            )
            mapping_by_raw_value: dict[str, list[str]] = {}
            for batch_index, values_batch in enumerate(chunked(distinct_values, batch_size), start=1):
                values_list = list(values_batch)
                if not values_list:
                    continue
                categorized_batch = _categorize_batch_with_retries(
                    categorizer,
                    categorized_column=categorized_column,
                    batch_index=batch_index,
                    values_batch=values_list,
                    categorize_max_retries=neutral.categorize_max_retries,
                )
                for mapping in categorized_batch.mappings:
                    mapping_by_raw_value[mapping.raw_value] = mapping.labels
            categorized_key = f"{categorized_column}_categorized"
            for categorized_row in output_rows:
                source_value = categorized_row.get(categorized_column)
                if isinstance(source_value, str):
                    labels = mapping_by_raw_value.get(source_value.strip(), [])
                    categorized_row[categorized_key] = _serialize_categorized_labels(labels)
                else:
                    categorized_row[categorized_key] = _serialize_categorized_labels([])

    flow_name = "categorize_to_api" if categorization_mode else "raw_to_api"

    if dry_run:
        if categorization_mode:
            dry_run_output: dict[str, Any] = {
                "flow": flow_name,
                "dry_run": True,
                "source_query": source_sql,
                "do_categorize": neutral.do_categorize,
                "applied_categorization": apply_categorization,
                "categorized_columns": categorize_column_names,
                "total_input_rows": len(projected_selected),
                "total_output_rows": len(output_rows),
                "sample_records": output_rows[:3],
            }
        else:
            dry_run_output = {
                "flow": flow_name,
                "dry_run": True,
                "source_query": source_sql,
                "total_rows": len(output_rows),
                "sample_rows": output_rows[:3],
            }
        print(json.dumps(dry_run_output, indent=2, ensure_ascii=False))
        return 0

    load_result = _load_output_rows(output_rows)

    if categorization_mode:
        success_output: dict[str, Any] = {
            "flow": flow_name,
            "dry_run": False,
            "source_query": source_sql,
            "do_categorize": neutral.do_categorize,
            "applied_categorization": apply_categorization,
            "categorized_columns": categorize_column_names,
            "total_input_rows": len(projected_selected),
            "total_output_rows": len(output_rows),
            "load_result": _summarize_load_result(load_result),
        }
    else:
        success_output = {
            "flow": flow_name,
            "dry_run": False,
            "source_query": source_sql,
            "total_rows": len(output_rows),
            "sample_rows": output_rows[:3],
            "load_result": _summarize_load_result(load_result),
        }
    print(json.dumps(success_output, indent=2, ensure_ascii=False))
    return 0


def _run_raw_to_api_mode(arguments: argparse.Namespace, dry_run: bool) -> int:
    return _run_query_first_to_api_flow(arguments=arguments, dry_run=dry_run, categorization_mode=False)


def _run_categorize_to_api_mode(arguments: argparse.Namespace, dry_run: bool) -> int:
    return _run_query_first_to_api_flow(arguments=arguments, dry_run=dry_run, categorization_mode=True)


def _run_categorization_only_mode(arguments: argparse.Namespace) -> int:
    neutral = load_neutral_runtime_config_from_env()
    database_url = _resolve_database_url(arguments, neutral)
    nim_api_key, nim_base_url, model = _resolve_llm_runtime(arguments, neutral)
    batch_size = _resolve_effective_batch_size(arguments, neutral)
    if arguments.target_columns_json:
        target_columns = _parse_manual_target_columns(arguments.target_columns_json)
    else:
        column_name = _prompt_text("Column name for categorization", arguments.raw_value_column)
        column_description = _prompt_text("Column description (optional)", "")
        target_columns = [{"name": column_name, "description": column_description or None}]

    request = CategorizationRequest(
        database_url=database_url,
        target_columns=target_columns,
        batch_size=batch_size,
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
        arguments.source_sql = _prompt_text(
            "SOURCE_QUERY / SQL (blank uses SOURCE_QUERY from .env)",
            arguments.source_sql or "",
        )
        run_dry = _prompt_text("Dry run (yes/no)", "yes").lower() == "yes"
        print(f"Dry run: {'enabled' if run_dry else 'disabled'} (no API load when enabled)")
        return _run_raw_to_api_mode(arguments, dry_run=run_dry)
    if selected_action == "4":
        arguments.api_check_mode = _prompt_text("API check mode (me/account/datasets)", arguments.api_check_mode)
        if arguments.api_check_mode not in {"me", "account", "datasets"}:
            raise ValueError("API check mode must be one of: me, account, datasets")
        return _run_api_check_mode(arguments)

    if selected_action == "2":
        arguments.source_sql = _prompt_text(
            "SOURCE_QUERY / SQL (blank uses SOURCE_QUERY from .env)",
            arguments.source_sql or "",
        )
        run_dry = _prompt_text("Dry run (yes/no)", "yes").lower() == "yes"
        print(f"Dry run: {'enabled' if run_dry else 'disabled'} (no API load when enabled)")
        return _run_categorize_to_api_mode(arguments, dry_run=run_dry)
    return _run_categorization_only_mode(arguments=arguments)


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
        if selected_mode in {"raw_to_api", "categorize_to_api"}:
            _configure_interactive_query_first_mode(arguments, selected_mode)

    if selected_mode == "api_check":
        return _run_api_check_mode(arguments)
    if selected_mode == "raw_to_api":
        return _run_raw_to_api_mode(arguments=arguments, dry_run=arguments.dry_run)
    if selected_mode == "categorize_to_api":
        return _run_categorize_to_api_mode(arguments=arguments, dry_run=arguments.dry_run)
    if selected_mode == "categorize_only":
        return _run_categorization_only_mode(arguments=arguments)
    return _run_manual_mode(arguments=arguments)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
