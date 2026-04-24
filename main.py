from __future__ import annotations

import argparse
import json
import os
import sys

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
from column_categorization.cli_categorize_only import _run_categorization_only_mode
from column_categorization.cli_interactive import (
    _configure_interactive_query_first_mode,
    _confirm_interactive_proceed,
    _interactive_acquire_source_sql,
    _normalize_mode,
    _print_interactive_api_check_preview,
    _print_interactive_categorize_only_preview,
    _print_interactive_to_api_preview,
    _prompt_mode,
    _prompt_text,
    _resolve_interactive_to_api_do_categorize,
)
from column_categorization.cli_query_first import _run_categorize_to_api_mode, _run_raw_to_api_mode
from column_categorization.config import load_neutral_runtime_config_from_env


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
        choices=["to_api", "raw_to_api", "categorize_to_api", "categorize_only", "manual", "etl", "api_check"],
        default=None,
        help="to_api, raw_to_api, categorize_to_api, categorize_only, manual, api_check, or etl (alias for categorize_to_api)",
    )
    parser.add_argument("--database-url", default=None, help="PostgreSQL connection string (overrides DB_URL from neutral env)")
    parser.add_argument("--llm-api-key", default=None, help="LLM API key")
    parser.add_argument("--llm-base-url", default=None, help="LLM base URL")
    parser.add_argument("--llm-model", default=None, help="LLM model")
    parser.add_argument("--nim-api-key", dest="llm_api_key", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--nim-base-url", dest="llm_base_url", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--model", dest="llm_model", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--schema-name", default="public", help="Schema name")
    parser.add_argument("--table-name", default="event_raw_staging", help="Table name")
    parser.add_argument("--batch-size", type=int, default=None, help="Values per categorization batch (defaults to BATCH_PROCESS from neutral env)")
    parser.add_argument("--source-sql", default=None, help="SQL query for source extraction (overrides SOURCE_QUERY from neutral env)")
    parser.add_argument("--source-row-limit", type=int, default=None, help="Max rows fetched from source query (overrides SOURCE_ROW_LIMIT from env)")
    parser.add_argument("--preview-row-limit", type=int, default=None, help="Max rows shown in sample_rows/sample_records (overrides PREVIEW_ROW_LIMIT from env)")
    parser.add_argument("--raw-id-column", default="source_event_id", help="ID column for ETL mode")
    parser.add_argument("--raw-value-column", default="raw_value", help="Raw text column for ETL mode")
    parser.add_argument("--raw-columns", default=None, help="Comma-separated DB columns to read from source table")
    parser.add_argument("--categorized-columns", default=None, help="Comma-separated source columns to categorize and append as <column>_categorized")
    parser.add_argument("--dry-run", action="store_true", help="Prepare and preview payload without sending to API")
    parser.add_argument("--dry-run-output-path", default=None, help="Optional JSON file path for full dry-run output (defaults to outputs/dry_run_<flow>_<timestamp>.json)")
    parser.add_argument("--target-columns-json", default=None, help='Manual mode only. JSON array format: [{"name":"raw_value","description":"..."}]')
    parser.add_argument("--api-check-mode", choices=["me", "account", "datasets"], default="me", help="api_check mode only: me, account, or datasets")
    parser.add_argument("--account-uid", default=None, help="account UID for api_check mode")
    parser.add_argument("--api-base-url", default=None, help="API base URL for api_check mode")
    parser.add_argument("--api-token", default=None, help="Bearer token for api_check mode")
    parser.add_argument("--api-timeout-seconds", type=int, default=None, help="api_check mode only: HTTP timeout in seconds (falls back to API_CHECK_TIMEOUT_SECONDS or 30)")
    parser.add_argument("--api-output-dir", default=None, help="api_check mode only: output directory for JSON response (falls back to API_CHECK_OUTPUT_DIR or outputs/api)")
    return parser


def _run_api_check_mode(arguments: argparse.Namespace) -> int:
    base_url = resolve_api_check_base_url(arguments.api_base_url, option_hint="--api-base-url")
    token = resolve_api_check_bearer_token(arguments.api_token, option_hint="--api-token")
    url = build_api_check_url(base_url=base_url, mode=arguments.api_check_mode, account_uid_cli=arguments.account_uid)
    timeout_seconds = resolve_api_check_timeout_seconds(arguments.api_timeout_seconds, timeout_flag_hint="--api-timeout-seconds")
    status_code, body_text = api_check_get(url=url, token=token, timeout_seconds=timeout_seconds)
    output_dir = resolve_api_check_output_dir(arguments.api_output_dir)
    output_path = build_api_check_output_path(mode=arguments.api_check_mode, output_dir=output_dir, account_uid_cli=arguments.account_uid)
    write_api_check_result_file(path=output_path, status_code=status_code, response_body=body_text)
    print(f"URL: {url}")
    print(f"Status: {status_code}")
    print(f"Saved: {output_path}")
    try:
        print(json.dumps(json.loads(body_text), indent=2, ensure_ascii=False))
    except json.JSONDecodeError:
        print(body_text)
    return 0 if 200 <= status_code < 300 else 1


def _run_manual_mode(arguments: argparse.Namespace) -> int:
    print("Manual action:")
    print("1) raw_to_api")
    print("2) categorize_to_api")
    print("3) categorize_only")
    print("4) api_check")
    selected_action = input("Enter action [1/2/3/4] (default 3): ").strip()
    if selected_action == "1":
        arguments.source_sql = _prompt_text("SOURCE_QUERY / SQL (blank uses SOURCE_QUERY from .env)", arguments.source_sql or "")
        return _run_raw_to_api_mode(arguments, dry_run=_prompt_text("Dry run (yes/no)", "yes").lower() == "yes")
    if selected_action == "2":
        arguments.source_sql = _prompt_text("SOURCE_QUERY / SQL (blank uses SOURCE_QUERY from .env)", arguments.source_sql or "")
        return _run_categorize_to_api_mode(arguments, dry_run=_prompt_text("Dry run (yes/no)", "yes").lower() == "yes")
    if selected_action == "4":
        arguments.api_check_mode = _prompt_text("API check mode (me/account/datasets)", arguments.api_check_mode)
        if arguments.api_check_mode not in {"me", "account", "datasets"}:
            raise ValueError("API check mode must be one of: me, account, datasets")
        return _run_api_check_mode(arguments)
    neutral_manual = load_neutral_runtime_config_from_env()
    arguments.source_sql = _interactive_acquire_source_sql(arguments, neutral_manual)
    setattr(arguments, "interactive_mode", True)
    return _run_categorization_only_mode(arguments)


def _prepare_interactive_mode(arguments: argparse.Namespace, selected_mode: str) -> int | None:
    setattr(arguments, "interactive_mode", True)
    if selected_mode == "api_check":
        arguments.api_check_mode = _prompt_text("API check mode (me/account/datasets)", arguments.api_check_mode)
        if arguments.api_check_mode not in {"me", "account", "datasets"}:
            raise ValueError("API check mode must be one of: me, account, datasets")
    elif selected_mode in {"to_api", "raw_to_api", "categorize_to_api", "categorize_only"}:
        _configure_interactive_query_first_mode(arguments, selected_mode)
    if selected_mode == "to_api":
        setattr(arguments, "force_do_categorize", _resolve_interactive_to_api_do_categorize())
    if selected_mode in {"to_api", "categorize_only", "api_check"}:
        neutral_preview = load_neutral_runtime_config_from_env()
        if selected_mode == "to_api":
            _print_interactive_to_api_preview(arguments, neutral_preview)
        elif selected_mode == "categorize_only":
            _print_interactive_categorize_only_preview(arguments, neutral_preview)
        else:
            _print_interactive_api_check_preview(arguments)
        if not _confirm_interactive_proceed():
            print("Cancelled: operation aborted (confirmation declined).")
            return 0
    return None


def main() -> int:
    _load_dotenv_file()
    arguments = _build_parser().parse_args()
    selected_mode = _normalize_mode(arguments.mode or _prompt_mode())
    if arguments.mode is None:
        early_exit = _prepare_interactive_mode(arguments, selected_mode)
        if early_exit is not None:
            return early_exit
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
