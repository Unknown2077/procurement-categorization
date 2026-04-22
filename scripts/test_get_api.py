from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
source_root = project_root / "src"
if str(source_root) not in sys.path:
    sys.path.insert(0, str(source_root))

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
    parser = argparse.ArgumentParser(description="Test GET endpoints with bearer token")
    parser.add_argument(
        "--mode",
        choices=["me", "account", "datasets"],
        default="me",
        help="me, account, or datasets (path templates resolved from env)",
    )
    parser.add_argument(
        "--account-uid",
        default=None,
        help="Required when mode=account unless TARGET_ACCOUNT_UID is set in .env",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Base URL. Falls back to TARGET_API_BASE_URL from .env",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Bearer token. Falls back to TARGET_ACCESS_TOKEN from .env",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help="HTTP timeout in seconds (falls back to API_CHECK_TIMEOUT_SECONDS or 30)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to store response JSON files (falls back to API_CHECK_OUTPUT_DIR or outputs/api)",
    )
    return parser


def main() -> int:
    _load_dotenv_file()
    parser = _build_parser()
    arguments = parser.parse_args()

    base_url = resolve_api_check_base_url(
        arguments.base_url, option_hint="--base-url"
    )
    token = resolve_api_check_bearer_token(
        arguments.token, option_hint="--token"
    )
    url = build_api_check_url(
        base_url=base_url,
        mode=arguments.mode,
        account_uid_cli=arguments.account_uid,
    )
    output_path = build_api_check_output_path(
        mode=arguments.mode,
        output_dir=resolve_api_check_output_dir(arguments.output_dir),
        account_uid_cli=arguments.account_uid,
    )
    timeout_seconds = resolve_api_check_timeout_seconds(
        arguments.timeout_seconds, timeout_flag_hint="--timeout-seconds"
    )
    status_code, response_body = api_check_get(
        url=url, token=token, timeout_seconds=timeout_seconds
    )
    write_api_check_result_file(
        path=output_path, status_code=status_code, response_body=response_body
    )
    print(f"URL: {url}")
    print(f"Status: {status_code}")
    print(f"Saved: {output_path}")
    try:
        parsed_json = json.loads(response_body)
        print(json.dumps(parsed_json, indent=2, ensure_ascii=False))
    except json.JSONDecodeError:
        print(response_body)
    return 0 if 200 <= status_code < 300 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
