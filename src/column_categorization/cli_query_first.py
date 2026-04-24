from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime
from typing import Any

from column_categorization.categorization.llm_categorizer import (
    CategorizationBatchResult,
    OpenAILLMCategorizer,
)
from column_categorization.cli_interactive import (
    _prompt_interactive_categorize_column_selection,
    _prompt_text,
)
from column_categorization.cli_resolution import (
    _require_source_query,
    _resolve_database_url,
    _resolve_effective_batch_size,
    _resolve_preview_row_limit,
    _resolve_source_query,
    _resolve_source_row_limit,
)
from column_categorization.config import (
    NeutralRuntimeConfig,
    load_etl_config_from_env,
    load_neutral_runtime_config_from_env,
    load_sink_config_from_env,
)
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.db.relation_listing import list_accessible_relations
from column_categorization.schemas.load import LoadFailure, LoadResult
from column_categorization.sinks.file_sink import FileSink
from column_categorization.sinks.http_api_sink import HttpApiSink
from column_categorization.sinks.router import SinkRouter
from column_categorization.utils.batching import chunked


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


def _parse_categorized_columns(categorized_columns: str | None, fallback_column: str) -> list[str]:
    if categorized_columns is None:
        return [fallback_column]
    parsed_columns = [column.strip() for column in categorized_columns.split(",") if column.strip()]
    if not parsed_columns:
        raise ValueError("categorized_columns must contain at least one column name")
    return parsed_columns


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


def _parse_raw_columns(raw_columns: str | None, fallback_columns: list[str]) -> list[str]:
    if raw_columns is None or not raw_columns.strip():
        return fallback_columns
    parsed_columns = [column.strip() for column in raw_columns.split(",") if column.strip()]
    if not parsed_columns:
        raise ValueError("raw_columns must contain at least one column name")
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


def _query_first_fetch_source_and_resolve_base(
    arguments: argparse.Namespace,
    neutral: NeutralRuntimeConfig,
    *,
    interactive_mode: bool,
) -> tuple[str, list[str], list[dict[str, object]], str]:
    database_url = _resolve_database_url(arguments, neutral)
    source_sql = _require_source_query(_resolve_source_query(arguments, neutral))
    source_row_limit = _resolve_source_row_limit(arguments, neutral)
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
    return source_sql, source_columns, limited_source_rows, effective_raw_id_column


def _query_first_resolve_and_validate_categorize_columns(
    arguments: argparse.Namespace,
    neutral: NeutralRuntimeConfig,
    *,
    categorization_mode: bool,
    apply_categorization: bool,
    source_columns: list[str],
    limited_source_rows: list[dict[str, object]],
    effective_raw_id_column: str,
    interactive_mode: bool,
) -> list[str]:
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
    return categorize_column_names


def _query_first_build_projected_output_rows(
    arguments: argparse.Namespace,
    *,
    source_columns: list[str],
    limited_source_rows: list[dict[str, object]],
    categorize_column_names: list[str],
    apply_categorization: bool,
    effective_raw_id_column: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
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
    return projected_selected, output_rows


def _query_first_apply_categorization_to_output_rows(
    arguments: argparse.Namespace,
    neutral: NeutralRuntimeConfig,
    *,
    categorize_column_names: list[str],
    projected_selected: list[dict[str, object]],
    output_rows: list[dict[str, object]],
    batch_size: int,
) -> None:
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


def _query_first_write_dry_run_payload(
    arguments: argparse.Namespace,
    *,
    flow_name: str,
    categorization_mode: bool,
    source_sql: str,
    effective_do_categorize: bool,
    apply_categorization: bool,
    categorize_column_names: list[str],
    source_row_limit: int,
    preview_row_limit: int,
    projected_selected: list[dict[str, object]],
    output_rows: list[dict[str, object]],
) -> int:
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


def _query_first_build_success_payload(
    *,
    categorization_mode: bool,
    flow_name: str,
    source_sql: str,
    effective_do_categorize: bool,
    apply_categorization: bool,
    categorize_column_names: list[str],
    source_row_limit: int,
    preview_row_limit: int,
    projected_selected: list[dict[str, object]],
    output_rows: list[dict[str, object]],
    load_result: LoadResult,
) -> dict[str, Any]:
    if categorization_mode:
        return {
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
    return {
        "flow": flow_name,
        "dry_run": False,
        "source_query": source_sql,
        "source_row_limit": source_row_limit,
        "preview_row_limit": preview_row_limit,
        "total_rows": len(output_rows),
        "sample_rows": output_rows[:preview_row_limit],
        "load_result": _summarize_load_result(load_result),
    }


def _run_query_first_to_api_flow(
    arguments: argparse.Namespace,
    *,
    dry_run: bool,
    categorization_mode: bool,
    force_do_categorize: bool | None = None,
    interactive_mode: bool = False,
) -> int:
    neutral = load_neutral_runtime_config_from_env()
    effective_do_categorize = neutral.do_categorize if force_do_categorize is None else force_do_categorize
    apply_categorization = categorization_mode and effective_do_categorize
    batch_size = _resolve_effective_batch_size(arguments, neutral)
    source_row_limit = _resolve_source_row_limit(arguments, neutral)
    preview_row_limit = _resolve_preview_row_limit(arguments, neutral)

    source_sql, source_columns, limited_source_rows, effective_raw_id_column = _query_first_fetch_source_and_resolve_base(
        arguments,
        neutral,
        interactive_mode=interactive_mode,
    )
    categorize_column_names = _query_first_resolve_and_validate_categorize_columns(
        arguments,
        neutral,
        categorization_mode=categorization_mode,
        apply_categorization=apply_categorization,
        source_columns=source_columns,
        limited_source_rows=limited_source_rows,
        effective_raw_id_column=effective_raw_id_column,
        interactive_mode=interactive_mode,
    )
    projected_selected, output_rows = _query_first_build_projected_output_rows(
        arguments,
        source_columns=source_columns,
        limited_source_rows=limited_source_rows,
        categorize_column_names=categorize_column_names,
        apply_categorization=apply_categorization,
        effective_raw_id_column=effective_raw_id_column,
    )
    if apply_categorization:
        _query_first_apply_categorization_to_output_rows(
            arguments,
            neutral,
            categorize_column_names=categorize_column_names,
            projected_selected=projected_selected,
            output_rows=output_rows,
            batch_size=batch_size,
        )

    flow_name = "categorize_to_api" if categorization_mode else "raw_to_api"

    if dry_run:
        return _query_first_write_dry_run_payload(
            arguments,
            flow_name=flow_name,
            categorization_mode=categorization_mode,
            source_sql=source_sql,
            effective_do_categorize=effective_do_categorize,
            apply_categorization=apply_categorization,
            categorize_column_names=categorize_column_names,
            source_row_limit=source_row_limit,
            preview_row_limit=preview_row_limit,
            projected_selected=projected_selected,
            output_rows=output_rows,
        )

    load_result = _load_output_rows(output_rows)
    success_output = _query_first_build_success_payload(
        categorization_mode=categorization_mode,
        flow_name=flow_name,
        source_sql=source_sql,
        effective_do_categorize=effective_do_categorize,
        apply_categorization=apply_categorization,
        categorize_column_names=categorize_column_names,
        source_row_limit=source_row_limit,
        preview_row_limit=preview_row_limit,
        projected_selected=projected_selected,
        output_rows=output_rows,
        load_result=load_result,
    )
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


def _prepare_interactive_to_api_categorize_columns(arguments: argparse.Namespace) -> None:
    if not bool(getattr(arguments, "interactive_mode", False)):
        return
    if not bool(getattr(arguments, "force_do_categorize", False)):
        return
    neutral = load_neutral_runtime_config_from_env()
    if neutral.categorize_columns:
        return
    existing_columns = getattr(arguments, "categorized_columns", None)
    if isinstance(existing_columns, str) and existing_columns.strip():
        return
    source_sql, source_columns, limited_source_rows, effective_raw_id_column = _query_first_fetch_source_and_resolve_base(
        arguments,
        neutral,
        interactive_mode=True,
    )
    arguments.source_sql = source_sql
    selected_columns = _prompt_interactive_categorize_column_selection(
        source_columns=source_columns,
        source_rows=limited_source_rows,
        id_column=effective_raw_id_column,
    )
    arguments.categorized_columns = ",".join(selected_columns)
