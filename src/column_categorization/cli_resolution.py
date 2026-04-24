from __future__ import annotations

import argparse

from column_categorization.config import NeutralRuntimeConfig


def _resolve_database_url(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> str:
    database_url = arguments.database_url or neutral.db_url
    if not database_url:
        raise ValueError("DB_URL is required in .env or --database-url")
    return database_url


def _resolve_source_query(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> str | None:
    cli_query = arguments.source_sql
    if isinstance(cli_query, str) and cli_query.strip():
        return cli_query.strip()
    neutral_query = neutral.source_query
    if neutral_query is not None and neutral_query.strip():
        return neutral_query.strip()
    return None


def _require_source_query(resolved_query: str | None) -> str:
    if resolved_query is None or not resolved_query.strip():
        raise ValueError(
            "SOURCE_QUERY is required for query-first extraction. "
            "Set SOURCE_QUERY in the environment or pass --source-sql."
        )
    return resolved_query.strip()


def _resolve_effective_batch_size(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> int:
    if arguments.batch_size is not None:
        return arguments.batch_size
    return neutral.batch_process


def _resolve_source_row_limit(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> int:
    source_row_limit = arguments.source_row_limit
    if source_row_limit is None:
        source_row_limit = neutral.source_row_limit
    if source_row_limit <= 0:
        raise ValueError(f"source row limit must be >= 1, got {source_row_limit}")
    return source_row_limit


def _resolve_preview_row_limit(arguments: argparse.Namespace, neutral: NeutralRuntimeConfig) -> int:
    preview_row_limit = arguments.preview_row_limit
    if preview_row_limit is None:
        preview_row_limit = neutral.preview_row_limit
    if preview_row_limit <= 0:
        raise ValueError(f"preview row limit must be >= 1, got {preview_row_limit}")
    return preview_row_limit
