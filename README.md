# Vendor-Agnostic ETL Categorization

Query-first ETL: run one PostgreSQL `SELECT`, optionally categorize with an LLM, then write JSONL (`TARGET_TYPE=json`) or POST to an API (`TARGET_TYPE=api`).

## Quick start

```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env
```

Set `DB_URL` and adjust `SOURCE_QUERY` for your data. Defaults in [.env.example](.env.example) are demo-safe: `TARGET_TYPE=json`, `DO_CATEGORIZE=false`, and a generic in-memory `SELECT` (no vendor schema).

## Interactive (recommended)

```bash
.venv/bin/python main.py
```

1. Choose `1` **to_api** (unified flow) or `2` **categorize_only** for categorization output only, or `3` **api_check**.
2. Press Enter to use `SOURCE_QUERY` from `.env`, or paste different SQL.
3. In **to_api** only: **Dry run (yes/no)** (non-interactive: `--dry-run`):
   - **yes**: prints JSON preview only; nothing is written to the target.
   - **no**: `TARGET_TYPE=json` appends to `TARGET_OUTPUT_PATH`; `TARGET_TYPE=api` sends rows to the API (requires target credentials).
4. In **to_api** only: you are always asked **Apply categorization? (yes/no)**. `DO_CATEGORIZE` in `.env` is only the default (blank default is *no*).
5. A short **execution preview** and **Proceed with this configuration? (yes/no)**. Answering *no* exits with code 0 and a cancellation message.

## CLI (non-interactive)

Dry-run (preview only):

```bash
.venv/bin/python main.py --mode raw_to_api --dry-run
```

Write to the configured target:

```bash
.venv/bin/python main.py --mode raw_to_api
```

Categorize then load (`--mode categorize_to_api`; `--mode etl` is the same). Use `--dry-run` for preview only.

```bash
.venv/bin/python main.py --mode categorize_to_api --dry-run
.venv/bin/python main.py --mode categorize_to_api
```

Useful flags: `--source-sql`, `--source-row-limit`, `--preview-row-limit`, `--database-url` (override `DB_URL`). See `main.py --help`.
Dry-run now saves full output JSON into `outputs/` and prints only preview + `dry_run_output_path` in terminal. Override file path with `--dry-run-output-path`.

## Advanced

- **Custom SQL**: `--source-sql "SELECT ..."` overrides `SOURCE_QUERY`. Any valid PostgreSQL works (CTEs, subqueries, casts, column aliases).
- **Env reference**: comments in `.env.example` list `DB_URL`, row limits, `TARGET_TYPE`, API/JSON paths, `DO_CATEGORIZE`, `CATEGORIZE_COLUMNS`, LLM settings, and api_check paths.
- **Output**: dry-run never loads; a real run still prints a JSON summary. API loads can fail if column types/nullability do not match the destination dataset.
- **Interactive recovery**: if the relation in your SQL is wrong, the app may list available relations and ask again.
- **`--raw-columns`**: when set, restricts which columns are read from the source row set; if omitted, all columns returned by the query are used.

Other `--mode` values: `manual` (sub-menu), `api_check` (GET smoke checks; separate flags `--api-check-mode`, `--api-token`, etc.).
