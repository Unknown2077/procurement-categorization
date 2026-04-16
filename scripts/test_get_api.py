from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib import error, request

project_root = Path(__file__).resolve().parents[1]
source_root = project_root / "src"
if str(source_root) not in sys.path:
    sys.path.insert(0, str(source_root))

from column_categorization.config import resolve_env_template


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
        help="Required when mode=account unless ACCOUNT_UID is set in .env",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Base URL. Falls back to API_BASE_URL or SINK_HTTP_BASE_URL from .env",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Bearer token. Falls back to SINK_HTTP_AUTH_TOKEN from .env",
    )
    parser.add_argument("--timeout-seconds", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument(
        "--output-dir",
        default="outputs/api",
        help="Directory to store response JSON files",
    )
    return parser


def _resolve_base_url(value: str | None) -> str:
    if value and value.strip():
        return value.rstrip("/")
    env_base_url = os.environ.get("API_BASE_URL") or os.environ.get("SINK_HTTP_BASE_URL")
    if env_base_url and env_base_url.strip():
        return env_base_url.rstrip("/")
    raise ValueError("API base URL is required via --base-url, API_BASE_URL, or SINK_HTTP_BASE_URL")


def _resolve_token(value: str | None) -> str:
    if value and value.strip():
        return value.strip()
    env_token = os.environ.get("SINK_HTTP_AUTH_TOKEN")
    if env_token and env_token.strip():
        return env_token.strip()
    raise ValueError("Bearer token is required via --token or SINK_HTTP_AUTH_TOKEN in .env")


def _resolve_account_uid(value: str | None) -> str:
    if value and value.strip():
        return value.strip()
    env_account_uid = os.environ.get("ACCOUNT_UID")
    if env_account_uid and env_account_uid.strip():
        return env_account_uid.strip()
    raise ValueError("account_uid is required via --account-uid or ACCOUNT_UID in .env")


def _build_url(base_url: str, mode: str, account_uid: str | None) -> str:
    context: dict[str, str] = {}
    if account_uid and account_uid.strip():
        context["account_uid"] = account_uid.strip()
    elif mode in {"account", "datasets"}:
        context["account_uid"] = _resolve_account_uid(account_uid)
    dataset_uid = os.environ.get("DATASET_UID")
    if dataset_uid and dataset_uid.strip():
        context["dataset_uid"] = dataset_uid.strip()
    if mode == "me":
        resolved_path = resolve_env_template(
            env_key="API_PATH_AUTH_ME",
            required=True,
            context=context,
        )
    elif mode == "datasets":
        resolved_path = resolve_env_template(
            env_key="API_PATH_DATASETS",
            required=True,
            context=context,
        )
    else:
        resolved_path = resolve_env_template(
            env_key="API_PATH_ACCOUNT",
            required=True,
            context=context,
        )
    if resolved_path is None:
        raise ValueError("Failed to resolve API path from environment")
    return f"{base_url}/{resolved_path.lstrip('/')}"


def _build_output_path(mode: str, output_dir: str, account_uid: str | None) -> Path:
    output_directory = Path(output_dir)
    if mode == "me":
        return output_directory / "getme.json"
    resolved_account_uid = _resolve_account_uid(account_uid)
    if mode == "datasets":
        return output_directory / f"datasets_{resolved_account_uid}.json"
    return output_directory / f"account_{resolved_account_uid}.json"


def _write_output_file(path: Path, status_code: int, response_body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        parsed_body = json.loads(response_body)
    except json.JSONDecodeError:
        payload: dict[str, object] = {
            "status_code": status_code,
            "body_text": response_body,
        }
    else:
        payload = {
            "status_code": status_code,
            "body": parsed_body,
        }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _perform_get(url: str, token: str, timeout_seconds: int) -> tuple[int, str]:
    http_request = request.Request(
        url=url,
        method="GET",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with request.urlopen(http_request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
            return response.status, body
    except error.HTTPError as http_error:
        body = http_error.read().decode("utf-8", errors="replace")
        return http_error.code, body
    except error.URLError as url_error:
        raise ValueError(f"Request failed: {url_error.reason}") from url_error


def main() -> int:
    _load_dotenv_file()
    parser = _build_parser()
    arguments = parser.parse_args()

    base_url = _resolve_base_url(arguments.base_url)
    token = _resolve_token(arguments.token)
    url = _build_url(base_url=base_url, mode=arguments.mode, account_uid=arguments.account_uid)
    output_path = _build_output_path(
        mode=arguments.mode,
        output_dir=arguments.output_dir,
        account_uid=arguments.account_uid,
    )

    status_code, response_body = _perform_get(url=url, token=token, timeout_seconds=arguments.timeout_seconds)
    _write_output_file(path=output_path, status_code=status_code, response_body=response_body)
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
