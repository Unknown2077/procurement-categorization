from __future__ import annotations

import json
import os
from pathlib import Path
from urllib import error, request

from column_categorization.config import resolve_env_template


def resolve_api_check_base_url(
    cli_value: str | None, *, option_hint: str = "--base-url"
) -> str:
    if cli_value and cli_value.strip():
        return cli_value.rstrip("/")
    env_base_url = os.environ.get("TARGET_API_BASE_URL")
    if env_base_url and env_base_url.strip():
        return env_base_url.rstrip("/")
    raise ValueError(f"API base URL is required via {option_hint} or TARGET_API_BASE_URL")


def resolve_api_check_bearer_token(
    cli_value: str | None, *, option_hint: str = "--token"
) -> str:
    if cli_value and cli_value.strip():
        return cli_value.strip()
    env_token = os.environ.get("TARGET_ACCESS_TOKEN")
    if env_token and env_token.strip():
        return env_token.strip()
    raise ValueError(
        f"Bearer token is required via {option_hint} or TARGET_ACCESS_TOKEN in .env"
    )


def build_api_check_template_context(*, mode: str, account_uid_cli: str | None) -> dict[str, str]:
    require_account_uid = mode != "me"
    context: dict[str, str] = {}
    account_uid = account_uid_cli or os.environ.get("TARGET_ACCOUNT_UID")
    if require_account_uid and not account_uid:
        raise ValueError("TARGET_ACCOUNT_UID is required for account/datasets mode")
    if account_uid:
        context["account_uid"] = account_uid
    dataset_uid = os.environ.get("TARGET_DATASET_UID")
    if dataset_uid:
        context["dataset_uid"] = dataset_uid
    return context


def resolve_api_check_path(mode: str, context: dict[str, str]) -> str:
    if mode == "me":
        env_key = "TARGET_API_PATH_AUTH_ME"
        default_path = "/api/v1/auth/me"
    elif mode == "datasets":
        env_key = "TARGET_API_PATH_DATASETS"
        default_path = "/api/v1/accounts/{account_uid}/datasets"
    else:
        env_key = "TARGET_API_PATH_ACCOUNT"
        default_path = "/api/v1/accounts/{account_uid}"
    try:
        resolved_path = resolve_env_template(
            env_key=env_key, required=False, context=context
        )
    except ValueError as template_error:
        raise ValueError(
            f"API check path contains unresolved template placeholders: {template_error}"
        ) from template_error
    if resolved_path is None:
        resolved_path = default_path
        for variable_name, replacement_value in context.items():
            resolved_path = resolved_path.replace(
                f"{{{variable_name}}}", replacement_value
            )
    if "{" in resolved_path or "}" in resolved_path:
        raise ValueError(
            f"API check path contains unresolved template placeholders: {resolved_path}"
        )
    return resolved_path


def build_api_check_url(
    *, base_url: str, mode: str, account_uid_cli: str | None
) -> str:
    context = build_api_check_template_context(
        mode=mode, account_uid_cli=account_uid_cli
    )
    path = resolve_api_check_path(mode, context)
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def resolve_api_check_account_uid(cli_value: str | None) -> str:
    if cli_value and cli_value.strip():
        return cli_value.strip()
    env_account_uid = os.environ.get("TARGET_ACCOUNT_UID")
    if env_account_uid and env_account_uid.strip():
        return env_account_uid.strip()
    raise ValueError(
        "Account UID is required via --account-uid or TARGET_ACCOUNT_UID in .env"
    )


def build_api_check_output_path(
    *, mode: str, output_dir: str | Path, account_uid_cli: str | None
) -> Path:
    out = Path(output_dir)
    if mode == "me":
        return out / "getme.json"
    account_uid = resolve_api_check_account_uid(account_uid_cli)
    if mode == "datasets":
        return out / f"datasets_{account_uid}.json"
    return out / f"account_{account_uid}.json"


def resolve_api_check_timeout_seconds(
    cli_value: int | None, *, timeout_flag_hint: str = "--timeout-seconds"
) -> int:
    if cli_value is not None:
        if cli_value <= 0:
            raise ValueError(f"{timeout_flag_hint} must be >= 1")
        return cli_value
    raw_env_timeout = os.environ.get("API_CHECK_TIMEOUT_SECONDS")
    if raw_env_timeout is None or not raw_env_timeout.strip():
        return 30
    try:
        parsed_timeout = int(raw_env_timeout.strip())
    except ValueError as parse_error:
        raise ValueError(
            f"API_CHECK_TIMEOUT_SECONDS must be an integer, got '{raw_env_timeout}'"
        ) from parse_error
    if parsed_timeout <= 0:
        raise ValueError(
            f"API_CHECK_TIMEOUT_SECONDS must be >= 1, got {parsed_timeout}"
        )
    return parsed_timeout


def resolve_api_check_output_dir(cli_value: str | None) -> Path:
    if cli_value is not None and cli_value.strip():
        return Path(cli_value.strip())
    env_output_dir = os.environ.get("API_CHECK_OUTPUT_DIR")
    if env_output_dir is not None and env_output_dir.strip():
        return Path(env_output_dir.strip())
    return Path("outputs/api")


def api_check_get(
    url: str, token: str, timeout_seconds: int
) -> tuple[int, str]:
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
        err_body = http_error.read().decode("utf-8", errors="replace")
        return http_error.code, err_body
    except error.URLError as url_error:
        raise ValueError(f"Request failed: {url_error.reason}") from url_error


def write_api_check_result_file(
    path: Path, status_code: int, response_body: str
) -> None:
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
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )
