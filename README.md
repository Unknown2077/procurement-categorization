# Vendor-Agnostic ETL Categorization

Simple ETL runner:
- Read rows from PostgreSQL using one SQL query
- Optional LLM categorization
- Load to API (`TARGET_TYPE=api`) or JSONL file (`TARGET_TYPE=json`)

## Quick Start

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env
```

Set minimal `.env`:

```env
DB_URL=postgresql://user:pass@127.0.0.1:5432/db_name
SOURCE_QUERY=SELECT * FROM public.my_view
SOURCE_ROW_LIMIT=50
PREVIEW_ROW_LIMIT=10

TARGET_TYPE=json
TARGET_OUTPUT_PATH=outputs/categorized_preview.jsonl

DO_CATEGORIZE=false
LLM_MODEL=qwen/qwen3-next-80b-a3b-instruct
LLM_API_KEY=replace_me
```

## How To Run

Interactive (recommended):
```bash
.venv/bin/python main.py
```

In interactive mode:
1. Choose `2` for `raw_to_api` (or `1` for `categorize_to_api`)
2. Press Enter to use `SOURCE_QUERY` from `.env` (or type custom SQL)
3. Choose `yes` for dry-run (safe preview), `no` for real load

## Most Used Commands

Raw preview (safe):
```bash
.venv/bin/python main.py --mode raw_to_api --dry-run
```

Raw real load:
```bash
.venv/bin/python main.py --mode raw_to_api
```

Categorize + load:
```bash
.venv/bin/python main.py --mode categorize_to_api
```

Custom SQL one-off:
```bash
.venv/bin/python main.py --mode raw_to_api --source-sql "SELECT * FROM public.my_view"
```

## Key Env Variables

- `DB_URL`: PostgreSQL connection string
- `SOURCE_QUERY`: SQL for source extraction
- `SOURCE_ROW_LIMIT`: max rows processed (default 10)
- `PREVIEW_ROW_LIMIT`: max rows shown in terminal JSON (default 10)
- `TARGET_TYPE`: `json` or `api`
- `TARGET_OUTPUT_PATH`: required for `TARGET_TYPE=json`
- `TARGET_API_BASE_URL`, `TARGET_ACCOUNT_UID`, `TARGET_DATASET_UID`, `TARGET_ACCESS_TOKEN`: required for `TARGET_TYPE=api`
- `DO_CATEGORIZE`: `true`/`false`
- `CATEGORIZE_COLUMNS`: optional columns to categorize

## Output Behavior (Important)

- `dry-run=yes`: prints JSON preview to terminal, does **not** load to target.
- `dry-run=no`:
  - `TARGET_TYPE=json`: writes rows to `TARGET_OUTPUT_PATH`.
  - `TARGET_TYPE=api`: sends rows to API.
  - both still print JSON summary to terminal.

## Notes

- If query relation/table is wrong in interactive mode, the app shows available relations and asks for query again.
- If `--raw-columns` is omitted, the app uses all columns returned by the query.
- API target may reject rows if schema/types/nullability do not match destination dataset.
