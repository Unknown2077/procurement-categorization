from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Literal


TargetType = Literal["api", "json"]


@dataclass(frozen=True)
class EtlSinkConfig:
    target_type: TargetType
    http_base_url: str | None
    http_path: str | None
    http_auth_token: str | None
    http_timeout_seconds: int
    file_path: str | None
    file_format: str

    @property
    def sink_type(self) -> Literal["http", "file"]:
        return "http" if self.target_type == "api" else "file"


@dataclass(frozen=True)
class EtlLoadConfig:
    load_batch_size: int
    load_max_retries: int
    load_retry_delay_seconds: int
    dead_letter_path: str


@dataclass(frozen=True)
class NeutralRuntimeConfig:
    db_url: str
    source_query: str | None
    target_type: TargetType
    target_api_base_url: str | None
    target_api_path_insert_data: str | None
    target_account_uid: str | None
    target_access_token: str | None
    target_dataset_uid: str | None
    target_output_path: str | None
    batch_process: int
    do_categorize: bool
    categorize_columns: tuple[str, ...]
    categorize_max_retries: int
    llm_model: str
    llm_api_key: str
    llm_base_url: str | None
    source_row_limit: int = 10
    preview_row_limit: int = 10


def load_neutral_runtime_config_from_env() -> NeutralRuntimeConfig:
    db_url = _read_required_env("DB_URL")
    source_query = _read_optional_env("SOURCE_QUERY")
    target_type = _read_target_type()
    target_api_base_url = _read_optional_env("TARGET_API_BASE_URL")
    target_api_path_insert_data = _read_optional_env("TARGET_API_PATH_INSERT_DATA")
    target_account_uid = _read_optional_env("TARGET_ACCOUNT_UID")
    target_access_token = _read_optional_env("TARGET_ACCESS_TOKEN")
    target_dataset_uid = _read_optional_env("TARGET_DATASET_UID")
    target_output_path = _read_optional_env("TARGET_OUTPUT_PATH")
    batch_process = _read_int_env("BATCH_PROCESS", default_value=10, minimum_value=1)
    do_categorize = _read_bool_env("DO_CATEGORIZE", default_value=False)
    categorize_columns = _read_csv_tokens_env("CATEGORIZE_COLUMNS")
    categorize_max_retries = _read_int_env("CATEGORIZE_MAX_RETRIES", default_value=3, minimum_value=0)
    llm_model = _read_required_env("LLM_MODEL")
    llm_api_key = _read_required_env("LLM_API_KEY")
    llm_base_url = _read_optional_env("LLM_BASE_URL")
    source_row_limit = _read_int_env("SOURCE_ROW_LIMIT", default_value=10, minimum_value=1)
    preview_row_limit = _read_int_env("PREVIEW_ROW_LIMIT", default_value=10, minimum_value=1)
    if target_type == "api":
        if target_api_base_url is None:
            raise ValueError("TARGET_API_BASE_URL is required when TARGET_TYPE=api")
        if target_account_uid is None:
            raise ValueError("TARGET_ACCOUNT_UID is required when TARGET_TYPE=api")
        if target_dataset_uid is None:
            raise ValueError("TARGET_DATASET_UID is required when TARGET_TYPE=api")
        if target_access_token is None:
            raise ValueError("TARGET_ACCESS_TOKEN is required when TARGET_TYPE=api")
    else:
        if target_output_path is None:
            raise ValueError("TARGET_OUTPUT_PATH is required when TARGET_TYPE=json")
    return NeutralRuntimeConfig(
        db_url=db_url,
        source_query=source_query,
        target_type=target_type,
        target_api_base_url=target_api_base_url,
        target_api_path_insert_data=target_api_path_insert_data,
        target_account_uid=target_account_uid,
        target_access_token=target_access_token,
        target_dataset_uid=target_dataset_uid,
        target_output_path=target_output_path,
        batch_process=batch_process,
        do_categorize=do_categorize,
        categorize_columns=categorize_columns,
        categorize_max_retries=categorize_max_retries,
        llm_model=llm_model,
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
        source_row_limit=source_row_limit,
        preview_row_limit=preview_row_limit,
    )


def load_sink_config_from_env() -> EtlSinkConfig:
    neutral = load_neutral_runtime_config_from_env()
    return _sink_config_from_neutral(neutral)


def load_etl_config_from_env() -> EtlLoadConfig:
    neutral = load_neutral_runtime_config_from_env()
    return EtlLoadConfig(
        load_batch_size=neutral.batch_process,
        load_max_retries=_read_int_env("LOAD_MAX_RETRIES", default_value=3, minimum_value=0),
        load_retry_delay_seconds=_read_int_env("LOAD_RETRY_DELAY_SECONDS", default_value=2, minimum_value=0),
        dead_letter_path=_read_optional_env("DEAD_LETTER_PATH") or "outputs/dead_letter_records.jsonl",
    )


def _sink_config_from_neutral(neutral: NeutralRuntimeConfig) -> EtlSinkConfig:
    http_timeout_seconds = _read_int_env("TARGET_API_TIMEOUT_SECONDS", default_value=30, minimum_value=1)
    if neutral.target_type == "api":
        assert neutral.target_api_base_url is not None
        assert neutral.target_account_uid is not None
        assert neutral.target_dataset_uid is not None
        assert neutral.target_access_token is not None
        insert_path = (
            resolve_env_template(
                env_key="TARGET_API_PATH_INSERT_DATA",
                required=False,
                context={
                    "account_uid": neutral.target_account_uid,
                    "dataset_uid": neutral.target_dataset_uid,
                },
            )
            or _neutral_insert_path(
                account_uid=neutral.target_account_uid,
                dataset_uid=neutral.target_dataset_uid,
            )
        )
        return EtlSinkConfig(
            target_type="api",
            http_base_url=neutral.target_api_base_url,
            http_path=insert_path,
            http_auth_token=neutral.target_access_token,
            http_timeout_seconds=http_timeout_seconds,
            file_path=None,
            file_format="jsonl",
        )
    assert neutral.target_output_path is not None
    return EtlSinkConfig(
        target_type="json",
        http_base_url=None,
        http_path=None,
        http_auth_token=None,
        http_timeout_seconds=http_timeout_seconds,
        file_path=neutral.target_output_path,
        file_format="jsonl",
    )


def _neutral_insert_path(account_uid: str, dataset_uid: str) -> str:
    return f"/api/v1/accounts/{account_uid}/datasets/{dataset_uid}/data"


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


def _read_bool_env(key: str, default_value: bool) -> bool:
    raw_value = _read_optional_env(key)
    if raw_value is None:
        return default_value
    normalized = raw_value.lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{key} must be a boolean, got '{raw_value}'")


def _read_target_type() -> TargetType:
    raw_value = _read_required_env("TARGET_TYPE").lower()
    if raw_value == "api":
        return "api"
    if raw_value == "json":
        return "json"
    raise ValueError("TARGET_TYPE must be one of: api, json")


def _read_csv_tokens_env(key: str) -> tuple[str, ...]:
    raw_value = _read_optional_env(key)
    if raw_value is None:
        return ()
    tokens = tuple(token.strip() for token in raw_value.split(",") if token.strip())
    return tokens


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


def _resolve_template_value(source_key: str, template: str, context: dict[str, str]) -> str:
    resolved_template = template
    variable_pattern = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
    for _ in range(10):
        variables = variable_pattern.findall(resolved_template)
        if not variables:
            return resolved_template
        did_replace = False
        for variable_name in set(variables):
            replacement_value = context.get(variable_name) or _read_optional_env(variable_name.upper())
            if replacement_value is None:
                replacement_value = _read_optional_env(variable_name)
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
