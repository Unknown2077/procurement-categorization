from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from typing import Any

from column_categorization.categorization.llm_categorizer import (
    CategorizationBatchResult,
    OpenAILLMCategorizer,
)
from column_categorization.cli_interactive import _prompt_text
from column_categorization.config import NeutralRuntimeConfig
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.db.relation_listing import list_accessible_relations


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


def _resolve_llm_runtime(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> tuple[str, str | None, str]:
    llm_api_key = getattr(arguments, "llm_api_key", None) or neutral.llm_api_key
    if not llm_api_key:
        raise ValueError("LLM_API_KEY is required in .env or --llm-api-key")
    llm_base_url = getattr(arguments, "llm_base_url", None) or neutral.llm_base_url
    llm_model = getattr(arguments, "llm_model", None) or neutral.llm_model
    return llm_api_key, llm_base_url, llm_model


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
            relation_names = list_accessible_relations(reader)
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
