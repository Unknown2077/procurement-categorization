from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
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
from column_categorization.schemas.categorization import CategorizationRequest, TargetColumn
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
        choices=[
            "to_api",
            "raw_to_api",
            "categorize_to_api",
            "categorize_only",
            "manual",
            "etl",
            "api_check",
        ],
        default=None,
        help="to_api, raw_to_api, categorize_to_api, categorize_only, manual, api_check, or etl (alias for categorize_to_api)",
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
    parser.add_argument(
        "--source-row-limit",
        type=int,
        default=None,
        help="Max rows fetched from source query (overrides SOURCE_ROW_LIMIT from env)",
    )
    parser.add_argument(
        "--preview-row-limit",
        type=int,
        default=None,
        help="Max rows shown in sample_rows/sample_records (overrides PREVIEW_ROW_LIMIT from env)",
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
        "--dry-run-output-path",
        default=None,
        help="Optional JSON file path for full dry-run output (defaults to outputs/dry_run_<flow>_<timestamp>.json)",
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
    print("1) to_api (recommended)")
    print("2) categorize_only")
    print("3) api_check")
    selected = input("Enter choice [1/2/3] (default 1): ").strip()
    if selected == "2":
        return "categorize_only"
    if selected == "3":
        return "api_check"
    return "to_api"


def _prompt_yes_no(label: str, default_yes: bool = True) -> bool:
    default_value = "yes" if default_yes else "no"
    return _prompt_text(label, default_value).lower() == "yes"


def _interactive_acquire_source_sql(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> str:
    resolved = (_resolve_source_query(arguments, neutral) or "").strip()
    if resolved:
        prompted = _prompt_text(
            "SOURCE_QUERY / SQL (blank keeps default from .env or prior entry)",
            resolved,
        ).strip()
        return prompted or resolved
    database_url = _resolve_database_url(arguments, neutral)
    reader = PostgresReader(database_url=database_url)
    relation_names = _list_accessible_relations(reader)
    print("No SOURCE_QUERY in .env or CLI. Enter a SELECT that returns the columns you need.")
    if relation_names:
        print("Available tables/views (schema.table):")
        for relation_name in relation_names[:20]:
            print(f"  - {relation_name}")
        if len(relation_names) > 20:
            print(f"  ... and {len(relation_names) - 20} more")
    else:
        print("No accessible tables/views listed for this database user.")
    while True:
        entered = input("SOURCE_QUERY / SQL (required): ").strip()
        if entered:
            return entered
        print("Error: SOURCE_QUERY cannot be empty.")


def _configure_interactive_query_first_mode(arguments: argparse.Namespace, selected_mode: str) -> None:
    neutral = load_neutral_runtime_config_from_env()
    arguments.source_sql = _interactive_acquire_source_sql(arguments, neutral)
    if selected_mode in {"to_api", "raw_to_api", "categorize_to_api"}:
        arguments.dry_run = _prompt_yes_no("Dry run (yes/no)", default_yes=True)
        print(f"Dry run: {'enabled' if arguments.dry_run else 'disabled'} (no API load when enabled)")


def _prompt_text(label: str, default_value: str) -> str:
    value = input(f"{label} [{default_value}]: ").strip()
    return value or default_value


def _normalize_mode(value: str) -> str:
    if value == "etl":
        return "categorize_to_api"
    return value


def _parse_bool_text(value: str, label: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{label} must be a boolean, got '{value}'")


def _resolve_interactive_to_api_do_categorize() -> bool:
    raw_value = os.environ.get("DO_CATEGORIZE")
    if raw_value is None or not raw_value.strip():
        return _prompt_yes_no("Apply categorization? (yes/no)", default_yes=False)
    return _parse_bool_text(raw_value, "DO_CATEGORIZE")


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


def _resolve_source_row_limit(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> int:
    source_row_limit = arguments.source_row_limit
    if source_row_limit is None:
        source_row_limit = neutral.source_row_limit
    if source_row_limit <= 0:
        raise ValueError(f"source row limit must be >= 1, got {source_row_limit}")
    return source_row_limit


def _resolve_preview_row_limit(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> int:
    preview_row_limit = arguments.preview_row_limit
    if preview_row_limit is None:
        preview_row_limit = neutral.preview_row_limit
    if preview_row_limit <= 0:
        raise ValueError(f"preview row limit must be >= 1, got {preview_row_limit}")
    return preview_row_limit


def _match_column_name(available_columns: list[str], candidate_names: list[str]) -> str | None:
    available_by_lower = {column.lower(): column for column in available_columns}
    for candidate_name in candidate_names:
        matched = available_by_lower.get(candidate_name.lower())
        if matched is not None:
            return matched
    return None


def _find_first_text_column(
    source_rows: list[dict[str, object]],
    source_columns: list[str],
    excluded_columns: set[str] | None = None,
) -> str | None:
    excluded = excluded_columns or set()
    for column in source_columns:
        if column in excluded:
            continue
        for source_row in source_rows:
            value = source_row.get(column)
            if isinstance(value, str) and value.strip():
                return column
    return None


def _find_uuid_like_column(
    source_rows: list[dict[str, object]],
    source_columns: list[str],
) -> str | None:
    uuid_pattern = re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    )
    for column in source_columns:
        for source_row in source_rows:
            value = source_row.get(column)
            if isinstance(value, str) and uuid_pattern.match(value.strip()):
                return column
    return None


def _resolve_effective_raw_id_column(
    preferred_column: str,
    source_columns: list[str],
    source_rows: list[dict[str, object]],
) -> str:
    if preferred_column in source_columns:
        return preferred_column
    candidate_match = _match_column_name(
        source_columns,
        [
            "source_event_id",
            "event_id",
            "source_id",
            "id",
            "uuid",
            "external_uuid",
        ],
    )
    if candidate_match is not None:
        print(f"Auto-resolved raw ID column: {preferred_column!r} -> {candidate_match!r}")
        return candidate_match
    suffixed_id = next((column for column in source_columns if column.lower().endswith("_id")), None)
    if suffixed_id is not None:
        print(f"Auto-resolved raw ID column: {preferred_column!r} -> {suffixed_id!r}")
        return suffixed_id
    uuid_column = _find_uuid_like_column(source_rows=source_rows, source_columns=source_columns)
    if uuid_column is not None:
        print(f"Auto-resolved raw ID column: {preferred_column!r} -> {uuid_column!r}")
        return uuid_column
    if source_columns:
        fallback_column = source_columns[0]
        print(f"Auto-resolved raw ID column: {preferred_column!r} -> {fallback_column!r}")
        return fallback_column
    raise ValueError("SOURCE_QUERY returned no columns; unable to resolve raw ID column.")


def _resolve_effective_raw_value_column(
    preferred_column: str,
    source_columns: list[str],
    source_rows: list[dict[str, object]],
    raw_id_column: str,
) -> str:
    if preferred_column in source_columns:
        return preferred_column
    candidate_match = _match_column_name(
        source_columns,
        [
            "raw_value",
            "value",
            "name",
            "note",
            "description",
            "title",
            "supplier_name",
            "text",
        ],
    )
    if candidate_match is None:
        candidate_match = _find_first_text_column(
            source_rows=source_rows,
            source_columns=source_columns,
            excluded_columns={raw_id_column},
        )
    if candidate_match is not None:
        print(f"Auto-resolved raw value column: {preferred_column!r} -> {candidate_match!r}")
        return candidate_match
    raise ValueError(
        "Unable to resolve raw value column from SOURCE_QUERY result. "
        "Set --raw-value-column explicitly or alias one text column as raw_value."
    )


def _resolve_categorize_column_names(
    arguments: argparse.Namespace,
    neutral: NeutralRuntimeConfig,
    apply_categorization: bool,
    fallback_column: str,
) -> list[str]:
    if not apply_categorization:
        return []
    if neutral.categorize_columns:
        return list(neutral.categorize_columns)
    return _parse_categorized_columns(
        arguments.categorized_columns,
        fallback_column=fallback_column,
    )


_CATEGORIZE_SUGGEST_KEYWORDS: tuple[str, ...] = (
    "raw_value",
    "value",
    "name",
    "note",
    "description",
    "title",
    "paket",
    "penyedia",
    "status",
)


def _source_column_has_non_empty_text(
    source_rows: list[dict[str, object]],
    column: str,
) -> bool:
    for source_row in source_rows:
        value = source_row.get(column)
        if isinstance(value, str) and value.strip():
            return True
    return False


def _suggest_categorize_columns(
    source_columns: list[str],
    source_rows: list[dict[str, object]],
    id_column: str,
    max_suggestions: int = 3,
) -> list[str]:
    suggestions: list[str] = []
    seen: set[str] = set()
    for column in source_columns:
        if len(suggestions) >= max_suggestions:
            break
        if not _source_column_has_non_empty_text(source_rows, column):
            continue
        lowered = column.lower()
        if any(keyword in lowered for keyword in _CATEGORIZE_SUGGEST_KEYWORDS):
            suggestions.append(column)
            seen.add(column)
    for column in source_columns:
        if len(suggestions) >= max_suggestions:
            break
        if column == id_column or column in seen:
            continue
        if not _source_column_has_non_empty_text(source_rows, column):
            continue
        suggestions.append(column)
        seen.add(column)
    return suggestions


def _read_validated_categorize_column_names(
    available_columns: list[str],
    *,
    default_line: str,
) -> list[str]:
    available: set[str] = set(available_columns)
    while True:
        raw = _prompt_text(
            "Comma-separated source columns to categorize (append <col>_categorized in output)",
            default_line,
        )
        names = [part.strip() for part in raw.split(",") if part.strip()]
        if not names:
            print("Error: at least one column is required.")
            continue
        unknown = sorted({name for name in names if name not in available})
        if unknown:
            available_text = ", ".join(available_columns)
            print(
                "Error: column name(s) not in SOURCE_QUERY result: "
                f"{', '.join(unknown)}. "
                f"Valid column names: {available_text}"
            )
            continue
        return names


def _prompt_interactive_categorize_column_selection(
    source_columns: list[str],
    source_rows: list[dict[str, object]],
    id_column: str,
) -> list[str]:
    suggestions = _suggest_categorize_columns(
        source_columns=source_columns,
        source_rows=source_rows,
        id_column=id_column,
        max_suggestions=3,
    )
    print("Available columns from SOURCE_QUERY result:")
    for name in source_columns:
        print(f"  - {name}")
    if suggestions:
        print("Suggested columns to categorize:")
        for name in suggestions:
            print(f"  - {name}")
    else:
        print("No columns matched the suggestion heuristic (name keywords + first non-id text columns).")
    if suggestions and _prompt_yes_no("Use suggested columns? (yes/no)"):
        return list(suggestions)
    default = ",".join(suggestions) if suggestions else ""
    return _read_validated_categorize_column_names(
        available_columns=source_columns,
        default_line=default,
    )


def _resolve_llm_runtime(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> tuple[str, str | None, str]:
    llm_api_key = getattr(arguments, "llm_api_key", None) or neutral.llm_api_key
    if not llm_api_key:
        raise ValueError("LLM_API_KEY is required in .env or --llm-api-key")
    llm_base_url = getattr(arguments, "llm_base_url", None) or neutral.llm_base_url
    llm_model = getattr(arguments, "llm_model", None) or neutral.llm_model
    return llm_api_key, llm_base_url, llm_model


def _list_accessible_relations(reader: PostgresReader) -> list[str]:
    relation_query = (
        "SELECT table_schema, table_name "
        "FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
        "ORDER BY table_schema, table_name"
    )
    relation_rows, _ = reader.fetch_rows_and_columns_by_sql(relation_query)
    relation_names: list[str] = []
    for relation_row in relation_rows:
        schema_name = relation_row.get("table_schema")
        table_name = relation_row.get("table_name")
        if isinstance(schema_name, str) and isinstance(table_name, str):
            relation_names.append(f"{schema_name}.{table_name}")
    return relation_names


def _is_missing_relation_error(error: BaseException) -> bool:
    message = str(error).lower()
    return "does not exist" in message and "relation" in message


def _normalize_sql_spacing(sql_text: str) -> str:
    normalized = sql_text
    for keyword in ("FROM", "ORDER BY", "WHERE", "GROUP BY", "LIMIT", "JOIN"):
        normalized = re.sub(rf"(?i)(\S)({keyword})\b", r"\1 \2", normalized)
    return normalized


def _fetch_source_rows_with_retry(
    reader: PostgresReader,
    *,
    source_sql: str,
    interactive_mode: bool,
) -> tuple[list[dict[str, object]], list[str], str]:
    current_source_sql = source_sql
    while True:
        try:
            source_rows, source_columns = reader.fetch_rows_and_columns_by_sql(current_source_sql)
            return source_rows, source_columns, current_source_sql
        except Exception as error:
            if interactive_mode and "syntax error at or near" in str(error).lower():
                normalized_sql = _normalize_sql_spacing(current_source_sql)
                if normalized_sql != current_source_sql:
                    current_source_sql = normalized_sql
                    continue
            if not interactive_mode or not _is_missing_relation_error(error):
                raise
            print(f"Error: {error}")
            relation_names = _list_accessible_relations(reader)
            if relation_names:
                print("Available tables/views:")
                for relation_name in relation_names[:20]:
                    print(f"- {relation_name}")
                if len(relation_names) > 20:
                    print(f"... and {len(relation_names) - 20} more")
            else:
                print("No accessible tables/views found in current database.")
            current_source_sql = _prompt_text(
                "SOURCE_QUERY / SQL (try one of the relations above)",
                current_source_sql,
            )


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
    if raw_columns is None or not raw_columns.strip():
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


def _find_missing_columns(
    available_columns: list[str],
    required_columns: list[str],
) -> list[str]:
    return sorted({column for column in required_columns if column not in set(available_columns)})


def _serialize_categorized_labels(labels: list[str]) -> str:
    return json.dumps(labels, ensure_ascii=False)


def _to_pretty_json(data: object) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False, default=str)


def _resolve_dry_run_output_path(arguments: argparse.Namespace, flow_name: str) -> str:
    cli_path = getattr(arguments, "dry_run_output_path", None)
    if isinstance(cli_path, str) and cli_path.strip():
        return cli_path.strip()
    env_path = os.environ.get("DRY_RUN_OUTPUT_PATH")
    if env_path is not None and env_path.strip():
        return env_path.strip()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"outputs/dry_run_{flow_name}_{timestamp}.json"


def _write_json_file(path: str, payload: object) -> None:
    output_dir = os.path.dirname(path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2, ensure_ascii=False, default=str)


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
    force_do_categorize: bool | None = None,
    interactive_mode: bool = False,
) -> int:
    neutral = load_neutral_runtime_config_from_env()
    database_url = _resolve_database_url(arguments, neutral)
    source_sql = _require_source_query(_resolve_source_query(arguments, neutral))
    effective_do_categorize = neutral.do_categorize if force_do_categorize is None else force_do_categorize
    apply_categorization = categorization_mode and effective_do_categorize
    batch_size = _resolve_effective_batch_size(arguments, neutral)
    source_row_limit = _resolve_source_row_limit(arguments, neutral)
    preview_row_limit = _resolve_preview_row_limit(arguments, neutral)

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
    use_hybrid_categorize_column_selection = (
        categorization_mode
        and getattr(arguments, "mode", None) is None
        and apply_categorization
        and not neutral.categorize_columns
        and arguments.categorized_columns is None
    )
    effective_raw_value_column: str | None = None
    if apply_categorization and not neutral.categorize_columns and arguments.categorized_columns is None and not use_hybrid_categorize_column_selection:
        effective_raw_value_column = _resolve_effective_raw_value_column(
            preferred_column=arguments.raw_value_column,
            source_columns=source_columns,
            source_rows=limited_source_rows,
            raw_id_column=effective_raw_id_column,
        )
    if use_hybrid_categorize_column_selection:
        print()
        categorize_column_names = _prompt_interactive_categorize_column_selection(
            source_columns=source_columns,
            source_rows=limited_source_rows,
            id_column=effective_raw_id_column,
        )
    else:
        categorize_column_names = _resolve_categorize_column_names(
            arguments,
            neutral,
            apply_categorization,
            fallback_column=effective_raw_value_column or arguments.raw_value_column,
        )
    if apply_categorization:
        missing_categorize_columns = _find_missing_columns(source_columns, categorize_column_names)
        if missing_categorize_columns:
            if interactive_mode:
                print(
                    "Configured categorize columns not found in SOURCE_QUERY result: "
                    f"{', '.join(missing_categorize_columns)}"
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
                    f"{', '.join(missing_categorize_columns)}. Valid column names: {available_text}"
                )
    if apply_categorization and not categorize_column_names:
        raise ValueError(
            "DO_CATEGORIZE=true requires at least one column: set CATEGORIZE_COLUMNS or --categorized-columns."
        )
    raw_columns = _parse_raw_columns(
        arguments.raw_columns,
        fallback_columns=list(source_columns),
    )
    selected_columns = list(raw_columns)
    for categorized_column in categorize_column_names:
        if categorized_column not in selected_columns:
            selected_columns.append(categorized_column)

    source_label = "SOURCE_QUERY result"
    if apply_categorization:
        _ensure_required_columns(
            available_columns=source_columns,
            required_columns=list(
                dict.fromkeys(selected_columns + categorize_column_names + [effective_raw_id_column])
            ),
            source_label=source_label,
        )
    else:
        _ensure_required_columns(
            available_columns=source_columns,
            required_columns=list(dict.fromkeys(raw_columns)),
            source_label=source_label,
        )

    projected_selected = [{column: source_row[column] for column in selected_columns} for source_row in limited_source_rows]
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
        dry_run_output_path = _resolve_dry_run_output_path(arguments, flow_name)
        if categorization_mode:
            dry_run_output: dict[str, Any] = {
                "flow": flow_name,
                "dry_run": True,
                "source_query": source_sql,
                "do_categorize": effective_do_categorize,
                "applied_categorization": apply_categorization,
                "categorized_columns": categorize_column_names,
                "source_row_limit": source_row_limit,
                "preview_row_limit": preview_row_limit,
                "total_input_rows": len(projected_selected),
                "total_output_rows": len(output_rows),
                "sample_records": output_rows[:preview_row_limit],
            }
            full_dry_run_output: dict[str, Any] = {
                "flow": flow_name,
                "dry_run": True,
                "source_query": source_sql,
                "do_categorize": effective_do_categorize,
                "applied_categorization": apply_categorization,
                "categorized_columns": categorize_column_names,
                "source_row_limit": source_row_limit,
                "preview_row_limit": preview_row_limit,
                "total_input_rows": len(projected_selected),
                "total_output_rows": len(output_rows),
                "records": output_rows,
            }
        else:
            dry_run_output = {
                "flow": flow_name,
                "dry_run": True,
                "source_query": source_sql,
                "source_row_limit": source_row_limit,
                "preview_row_limit": preview_row_limit,
                "total_rows": len(output_rows),
                "sample_rows": output_rows[:preview_row_limit],
            }
            full_dry_run_output = {
                "flow": flow_name,
                "dry_run": True,
                "source_query": source_sql,
                "source_row_limit": source_row_limit,
                "preview_row_limit": preview_row_limit,
                "total_rows": len(output_rows),
                "rows": output_rows,
            }
        _write_json_file(dry_run_output_path, full_dry_run_output)
        dry_run_output["dry_run_output_path"] = dry_run_output_path
        print(_to_pretty_json(dry_run_output))
        return 0

    load_result = _load_output_rows(output_rows)

    if categorization_mode:
        success_output: dict[str, Any] = {
            "flow": flow_name,
            "dry_run": False,
            "source_query": source_sql,
            "do_categorize": effective_do_categorize,
            "applied_categorization": apply_categorization,
            "categorized_columns": categorize_column_names,
            "source_row_limit": source_row_limit,
            "preview_row_limit": preview_row_limit,
            "total_input_rows": len(projected_selected),
            "total_output_rows": len(output_rows),
            "sample_records": output_rows[:preview_row_limit],
            "load_result": _summarize_load_result(load_result),
        }
    else:
        success_output = {
            "flow": flow_name,
            "dry_run": False,
            "source_query": source_sql,
            "source_row_limit": source_row_limit,
            "preview_row_limit": preview_row_limit,
            "total_rows": len(output_rows),
            "sample_rows": output_rows[:preview_row_limit],
            "load_result": _summarize_load_result(load_result),
        }
    print(_to_pretty_json(success_output))
    return 0


def _run_raw_to_api_mode(arguments: argparse.Namespace, dry_run: bool) -> int:
    return _run_query_first_to_api_flow(
        arguments=arguments,
        dry_run=dry_run,
        categorization_mode=False,
        interactive_mode=bool(getattr(arguments, "interactive_mode", False)),
    )


def _run_categorize_to_api_mode(arguments: argparse.Namespace, dry_run: bool) -> int:
    return _run_query_first_to_api_flow(
        arguments=arguments,
        dry_run=dry_run,
        categorization_mode=True,
        force_do_categorize=getattr(arguments, "force_do_categorize", None),
        interactive_mode=bool(getattr(arguments, "interactive_mode", False)),
    )


def _run_categorization_only_mode(arguments: argparse.Namespace) -> int:
    neutral = load_neutral_runtime_config_from_env()
    database_url = _resolve_database_url(arguments, neutral)
    source_sql = _require_source_query(_resolve_source_query(arguments, neutral))
    nim_api_key, nim_base_url, model = _resolve_llm_runtime(arguments, neutral)
    batch_size = _resolve_effective_batch_size(arguments, neutral)
    source_row_limit = _resolve_source_row_limit(arguments, neutral)
    interactive_mode = bool(getattr(arguments, "interactive_mode", False))

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

    target_columns: list[TargetColumn]
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
        target_columns = [TargetColumn.model_validate(spec) for spec in parsed_specs]
    elif neutral.categorize_columns:
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
        target_columns = [TargetColumn(name=name, description=None) for name in categorize_column_names]
    elif arguments.categorized_columns is not None:
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
        target_columns = [TargetColumn(name=name, description=None) for name in categorize_column_names]
    elif interactive_mode:
        categorize_column_names = _prompt_interactive_categorize_column_selection(
            source_columns=source_columns,
            source_rows=limited_source_rows,
            id_column=effective_raw_id_column,
        )
        target_columns = [TargetColumn(name=name, description=None) for name in categorize_column_names]
    else:
        raise ValueError(
            "At least one column to categorize is required: set CATEGORIZE_COLUMNS, "
            "--categorized-columns, --target-columns-json, or run interactively."
        )

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
    neutral_manual = load_neutral_runtime_config_from_env()
    arguments.source_sql = _interactive_acquire_source_sql(arguments, neutral_manual)
    setattr(arguments, "interactive_mode", True)
    return _run_categorization_only_mode(arguments)


def main() -> int:
    _load_dotenv_file()
    parser = _build_parser()
    arguments = parser.parse_args()

    selected_mode = _normalize_mode(arguments.mode or _prompt_mode())

    if arguments.mode is None:
        setattr(arguments, "interactive_mode", True)
        if selected_mode == "api_check":
            arguments.api_check_mode = _prompt_text("API check mode (me/account/datasets)", arguments.api_check_mode)
            if arguments.api_check_mode not in {"me", "account", "datasets"}:
                raise ValueError("API check mode must be one of: me, account, datasets")
            return _run_api_check_mode(arguments)
        if selected_mode in {"to_api", "raw_to_api", "categorize_to_api", "categorize_only"}:
            _configure_interactive_query_first_mode(arguments, selected_mode)
        if selected_mode == "to_api":
            setattr(arguments, "force_do_categorize", _resolve_interactive_to_api_do_categorize())
    else:
        setattr(arguments, "interactive_mode", False)

    if selected_mode == "api_check":
        return _run_api_check_mode(arguments)
    if selected_mode == "to_api":
        return _run_categorize_to_api_mode(arguments=arguments, dry_run=arguments.dry_run)
    if selected_mode == "raw_to_api":
        return _run_raw_to_api_mode(arguments=arguments, dry_run=arguments.dry_run)
    if selected_mode == "categorize_to_api":
        return _run_categorize_to_api_mode(arguments=arguments, dry_run=arguments.dry_run)
    if selected_mode == "categorize_only":
        return _run_categorization_only_mode(arguments)
    return _run_manual_mode(arguments=arguments)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
