from __future__ import annotations

import argparse

import main
import pytest


def test_normalize_mode_maps_legacy_etl_alias() -> None:
    assert main._normalize_mode("etl") == "categorize_to_api"
    assert main._normalize_mode("raw_to_api") == "raw_to_api"


def test_parse_raw_columns_uses_fallback() -> None:
    output = main._parse_raw_columns(None, fallback_columns=["source_event_id", "raw_value"])
    assert output == ["source_event_id", "raw_value"]


def test_parse_raw_columns_parses_comma_list() -> None:
    output = main._parse_raw_columns(
        "source_event_id, raw_value , notes",
        fallback_columns=["ignored"],
    )
    assert output == ["source_event_id", "raw_value", "notes"]


def test_parse_categorized_columns_uses_fallback() -> None:
    output = main._parse_categorized_columns(None, fallback_column="note")
    assert output == ["note"]


def test_parse_categorized_columns_parses_comma_list() -> None:
    output = main._parse_categorized_columns("note, description", fallback_column="ignored")
    assert output == ["note", "description"]


def test_resolve_source_sql_uses_cli_value() -> None:
    arguments = argparse.Namespace(source_sql="SELECT id, name FROM public.org")
    output = main._resolve_source_sql(arguments)
    assert output == "SELECT id, name FROM public.org"


def test_resolve_source_sql_uses_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_SQL", "SELECT id, name, note FROM public.org")
    arguments = argparse.Namespace(source_sql=None)
    output = main._resolve_source_sql(arguments)
    assert output == "SELECT id, name, note FROM public.org"


def test_ensure_required_columns_raises_on_missing_column() -> None:
    with pytest.raises(ValueError, match="source_sql result is missing required columns: note"):
        main._ensure_required_columns(
            available_columns=["id", "name"],
            required_columns=["id", "note"],
            source_label="source_sql result",
        )


def test_serialize_categorized_labels_returns_json_string() -> None:
    output = main._serialize_categorized_labels(["Jasa Survei", "Jasa Pemetaan"])
    assert output == "[\"Jasa Survei\", \"Jasa Pemetaan\"]"


def test_build_api_check_url_uses_env_template_for_me(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("API_PATH_AUTH_ME", "/api/v1/auth/me")
    arguments = argparse.Namespace(
        api_base_url=None,
        api_check_mode="me",
        account_uid=None,
    )
    output = main._build_api_check_url(arguments)
    assert output == "https://example-api.test/api/v1/auth/me"


def test_build_api_check_url_resolves_account_uid_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_BASE_URL", "https://example-api.test")
    monkeypatch.setenv("API_PATH_DATASETS", "/api/v1/accounts/{account_uid}/datasets")
    monkeypatch.setenv("ACCOUNT_UID", "acc-123")
    arguments = argparse.Namespace(
        api_base_url=None,
        api_check_mode="datasets",
        account_uid=None,
    )
    output = main._build_api_check_url(arguments)
    assert output == "https://example-api.test/api/v1/accounts/acc-123/datasets"


def test_build_api_check_url_fails_when_path_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_BASE_URL", "https://example-api.test")
    monkeypatch.delenv("API_PATH_AUTH_ME", raising=False)
    arguments = argparse.Namespace(
        api_base_url=None,
        api_check_mode="me",
        account_uid=None,
    )
    with pytest.raises(ValueError, match="API_PATH_AUTH_ME"):
        main._build_api_check_url(arguments)
