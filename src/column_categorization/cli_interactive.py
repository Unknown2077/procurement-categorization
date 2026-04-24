from __future__ import annotations

import argparse
import os

from column_categorization.cli_resolution import (
    _require_source_query,
    _resolve_database_url,
    _resolve_preview_row_limit,
    _resolve_source_query,
    _resolve_source_row_limit,
)
from column_categorization.config import NeutralRuntimeConfig, load_neutral_runtime_config_from_env
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.db.relation_listing import list_accessible_relations


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


def _prompt_text(label: str, default_value: str) -> str:
    value = input(f"{label} [{default_value}]: ").strip()
    return value or default_value


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
    relation_names = list_accessible_relations(reader)
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


def _default_yes_for_do_categorize_prompt() -> bool:
    raw_value = os.environ.get("DO_CATEGORIZE")
    if raw_value is None or not raw_value.strip():
        return False
    return _parse_bool_text(raw_value.strip(), "DO_CATEGORIZE")


def _resolve_interactive_to_api_do_categorize() -> bool:
    return _prompt_yes_no(
        "Apply categorization? (yes/no)",
        default_yes=_default_yes_for_do_categorize_prompt(),
    )


def _describe_categorize_only_target_columns_source(
    arguments: argparse.Namespace,
    neutral: NeutralRuntimeConfig,
) -> str:
    tj = getattr(arguments, "target_columns_json", None)
    if isinstance(tj, str) and tj.strip():
        return "cli (--target-columns-json)"
    cc = getattr(arguments, "categorized_columns", None)
    if isinstance(cc, str) and cc.strip():
        return "cli (--categorized-columns)"
    if neutral.categorize_columns:
        return f"env (CATEGORIZE_COLUMNS: {', '.join(neutral.categorize_columns)})"
    return "interactive (will prompt) / pending"


def _describe_api_check_base_url_source(arguments: argparse.Namespace) -> str:
    cli = getattr(arguments, "api_base_url", None)
    if isinstance(cli, str) and cli.strip():
        return "cli (--api-base-url)"
    if os.environ.get("TARGET_API_BASE_URL", "").strip():
        return "env (TARGET_API_BASE_URL)"
    return "unresolved (set --api-base-url or TARGET_API_BASE_URL before run)"


def _print_interactive_to_api_preview(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> None:
    source_sql = _require_source_query(_resolve_source_query(arguments, neutral))
    source_row_limit = _resolve_source_row_limit(arguments, neutral)
    preview_row_limit = _resolve_preview_row_limit(arguments, neutral)
    apply_flag = bool(getattr(arguments, "force_do_categorize", None))
    if apply_flag:
        if isinstance(arguments.categorized_columns, str) and arguments.categorized_columns.strip():
            cat_desc = f"cli: {arguments.categorized_columns.strip()}"
        elif neutral.categorize_columns:
            cat_desc = f"env: {', '.join(neutral.categorize_columns)}"
        else:
            cat_desc = "pending (interactive or hybrid at runtime)"
    else:
        cat_desc = "n/a (categorization disabled)"
    print()
    print("--- Execution preview ---")
    print(f"  mode: to_api")
    print(f"  dry_run: {arguments.dry_run}")
    print(f"  source_query: {source_sql}")
    print(f"  source_row_limit: {source_row_limit}")
    print(f"  preview_row_limit: {preview_row_limit}")
    print(f"  apply_categorization: {apply_flag}")
    print(f"  categorized_columns: {cat_desc}")
    print(f"  target_type: {neutral.target_type}")
    print("-------------------------")


def _print_interactive_categorize_only_preview(
    arguments: argparse.Namespace,
    neutral: NeutralRuntimeConfig,
) -> None:
    source_sql = _require_source_query(_resolve_source_query(arguments, neutral))
    source_row_limit = _resolve_source_row_limit(arguments, neutral)
    print()
    print("--- Execution preview ---")
    print(f"  mode: categorize_only")
    print(f"  source_query: {source_sql}")
    print(f"  source_row_limit: {source_row_limit}")
    print(
        f"  target columns source: {_describe_categorize_only_target_columns_source(arguments, neutral)}"
    )
    print(f"  target_type: {neutral.target_type}")
    print("-------------------------")


def _print_interactive_api_check_preview(arguments: argparse.Namespace) -> None:
    print()
    print("--- Execution preview ---")
    print(f"  mode: api_check")
    print(f"  api_check_mode: {arguments.api_check_mode}")
    print(f"  base_url option source: {_describe_api_check_base_url_source(arguments)}")
    print("-------------------------")


def _confirm_interactive_proceed() -> bool:
    return _prompt_yes_no("Proceed with this configuration? (yes/no)", default_yes=True)


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
