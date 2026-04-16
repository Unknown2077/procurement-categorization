from __future__ import annotations

import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class EtlSinkConfig:
    sink_type: str
    http_base_url: str | None
    http_path: str | None
    http_auth_token: str | None
    http_timeout_seconds: int
    db_url: str | None
    db_schema: str | None
    db_table: str | None
    file_path: str | None
    file_format: str


@dataclass(frozen=True)
class EtlLoadConfig:
    load_batch_size: int
    load_max_retries: int
    load_retry_delay_seconds: int
    dead_letter_path: str


def load_sink_config_from_env() -> EtlSinkConfig:
    sink_type = _read_required_env("SINK_TYPE").lower()
    if sink_type not in {"http", "db", "file"}:
        raise ValueError("SINK_TYPE must be one of: http, db, file")
    template_context = _build_template_context()
    http_path = resolve_env_template(
        env_key="SINK_HTTP_PATH",
        required=False,
        fallback_env_key="API_PATH_INSERT_DATA",
        context=template_context,
    )
    return EtlSinkConfig(
        sink_type=sink_type,
        http_base_url=_read_optional_env("SINK_HTTP_BASE_URL") or _read_optional_env("API_BASE_URL"),
        http_path=http_path,
        http_auth_token=_read_optional_env("SINK_HTTP_AUTH_TOKEN"),
        http_timeout_seconds=_read_int_env("SINK_HTTP_TIMEOUT_SECONDS", default_value=30, minimum_value=1),
        db_url=_read_optional_env("SINK_DB_URL"),
        db_schema=_read_optional_env("SINK_DB_SCHEMA"),
        db_table=_read_optional_env("SINK_DB_TABLE"),
        file_path=_read_optional_env("SINK_FILE_PATH"),
        file_format=_read_optional_env("SINK_FILE_FORMAT") or "jsonl",
    )


def load_etl_config_from_env() -> EtlLoadConfig:
    return EtlLoadConfig(
        load_batch_size=_read_int_env("LOAD_BATCH_SIZE", default_value=10, minimum_value=1),
        load_max_retries=_read_int_env("LOAD_MAX_RETRIES", default_value=3, minimum_value=0),
        load_retry_delay_seconds=_read_int_env("LOAD_RETRY_DELAY_SECONDS", default_value=2, minimum_value=0),
        dead_letter_path=_read_optional_env("LOAD_DEAD_LETTER_PATH") or "outputs/dead_letter_records.jsonl",
    )


def _read_required_env(key: str) -> str:
    value = os.environ.get(key)
    if value is None or not value.strip():
        raise ValueError(f"{key} is required")
    return value.strip()


def _read_optional_env(key: str) -> str | None:
    value = os.environ.get(key)
    if value is None:
        return None
    stripped_value = value.strip()
    return stripped_value if stripped_value else None


def _read_int_env(key: str, default_value: int, minimum_value: int) -> int:
    raw_value = _read_optional_env(key)
    if raw_value is None:
        return default_value
    try:
        parsed_value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{key} must be an integer, got '{raw_value}'") from error
    if parsed_value < minimum_value:
        raise ValueError(f"{key} must be >= {minimum_value}, got {parsed_value}")
    return parsed_value


def resolve_env_template(
    env_key: str,
    required: bool,
    fallback_env_key: str | None = None,
    context: dict[str, str] | None = None,
) -> str | None:
    raw_template = _read_optional_env(env_key)
    if raw_template is None and fallback_env_key is not None:
        raw_template = _read_optional_env(fallback_env_key)
    if raw_template is None:
        if required:
            fallback_label = f" or {fallback_env_key}" if fallback_env_key is not None else ""
            raise ValueError(f"{env_key} is required{fallback_label}")
        return None
    return _resolve_template_value(
        source_key=env_key,
        template=raw_template,
        context=context or {},
    )


def _build_template_context() -> dict[str, str]:
    context: dict[str, str] = {}
    account_uid = _read_optional_env("ACCOUNT_UID")
    dataset_uid = _read_optional_env("DATASET_UID")
    if account_uid is not None:
        context["account_uid"] = account_uid
    if dataset_uid is not None:
        context["dataset_uid"] = dataset_uid
    return context


def _resolve_template_value(source_key: str, template: str, context: dict[str, str]) -> str:
    resolved_template = template
    variable_pattern = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
    for _ in range(10):
        variables = variable_pattern.findall(resolved_template)
        if not variables:
            return resolved_template
        did_replace = False
        for variable_name in set(variables):
            replacement_value = context.get(variable_name) or _read_optional_env(variable_name)
            if replacement_value is None:
                continue
            resolved_template = resolved_template.replace(f"{{{variable_name}}}", replacement_value)
            did_replace = True
        if not did_replace:
            break
    unresolved_variables = sorted(set(variable_pattern.findall(resolved_template)))
    if unresolved_variables:
        unresolved_list = ", ".join(unresolved_variables)
        raise ValueError(f"{source_key} contains unresolved template variables: {unresolved_list}")
    return resolved_template
