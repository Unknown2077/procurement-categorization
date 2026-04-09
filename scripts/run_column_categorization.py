from __future__ import annotations

import argparse
import json
import os
import sys

from column_categorization.categorization.llm_categorizer import OpenAILLMCategorizer
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.pipelines.column_categorization import ColumnCategorizationPipeline
from column_categorization.schemas.categorization import CategorizationRequest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run column categorization pipeline")
    parser.add_argument("--database-url", required=False, default=None, help="PostgreSQL connection string")
    parser.add_argument(
        "--target-columns-json",
        required=True,
        help='JSON array format: [{"name":"...", "description":"..."}]',
    )
    parser.add_argument("--batch-size", type=int, default=100, help="Rows per categorization batch")
    parser.add_argument("--schema-name", default="public", help="Schema name")
    parser.add_argument("--table-name", default="original_data", help="Table name")
    parser.add_argument("--nim-api-key", default=None, help="NVIDIA NIM API key")
    parser.add_argument("--nim-base-url", default=None, help="NVIDIA NIM base URL")
    parser.add_argument("--model", default=None, help="NVIDIA NIM model name")
    return parser


def _load_dotenv_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, value = line.split("=", maxsplit=1)
            normalized_key = key.strip()
            normalized_value = value.strip().strip('"').strip("'")
            if normalized_key and normalized_value and normalized_key not in os.environ:
                os.environ[normalized_key] = normalized_value


def main() -> int:
    _load_dotenv_file()
    parser = _build_parser()
    arguments = parser.parse_args()

    database_url = arguments.database_url or os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("database_url is required via --database-url or DATABASE_URL")

    nim_api_key = arguments.nim_api_key or os.environ.get("NIM_API_KEY")
    if not nim_api_key:
        raise ValueError("NIM API key is required via --nim-api-key or NIM_API_KEY")

    nim_base_url = arguments.nim_base_url or os.environ.get("NIM_BASE_URL")
    model = arguments.model or os.environ.get("NIM_MODEL") or "qwen/qwen3-next-80b-a3b-instruct"

    try:
        target_columns = json.loads(arguments.target_columns_json)
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid --target-columns-json: {error}") from error

    request = CategorizationRequest(
        database_url=database_url,
        target_columns=target_columns,
        batch_size=arguments.batch_size,
        schema_name=arguments.schema_name,
        table_name=arguments.table_name,
    )
    reader = PostgresReader(database_url=request.database_url)
    categorizer = OpenAILLMCategorizer(
        api_key=nim_api_key,
        model=model,
        base_url=nim_base_url,
    )
    pipeline = ColumnCategorizationPipeline(reader=reader, categorizer=categorizer)
    response = pipeline.run(request)
    print(response.model_dump_json(indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(1)
