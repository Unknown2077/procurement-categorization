# Vendor-Agnostic ETL Categorization

This project reads raw values from PostgreSQL, optionally categorizes them with an LLM, and loads results to a configurable target (`TARGET_TYPE=api` for HTTP POST, or `TARGET_TYPE=json` for a local JSONL file).

## Setup
```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env
```

Fill `.env` first. What you need depends on the mode (see **Environment contract** and **Query-first source** below). `api_check` only needs API URL, token, and path-related variables; it does not read `DB_URL` or LLM settings.

Minimal JSON target env:
```env
DB_URL=postgresql://user:pass@127.0.0.1:5432/db_name
TARGET_TYPE=json
TARGET_OUTPUT_PATH=outputs/categorized_records.jsonl
LLM_MODEL=qwen/qwen3-next-80b-a3b-instruct
LLM_API_KEY=replace_me
```

Minimal API target env:
```env
DB_URL=postgresql://user:pass@127.0.0.1:5432/db_name
TARGET_TYPE=api
TARGET_API_BASE_URL=https://example-api.test
TARGET_ACCOUNT_UID=replace_me
TARGET_DATASET_UID=replace_me
TARGET_ACCESS_TOKEN=replace_me
LLM_MODEL=qwen/qwen3-next-80b-a3b-instruct
LLM_API_KEY=replace_me
# Rows are POSTed to /api/v1/accounts/{TARGET_ACCOUNT_UID}/datasets/{TARGET_DATASET_UID}/data
# Optional source override:
# SOURCE_QUERY=SELECT id, name, note FROM public.org
```

## Run (recommended)
Interactive mode:
```bash
.venv/bin/python main.py
```

## Choose a Flow
- `raw_to_api`: read selected raw columns from DB and load to the configured target (`api` or `json`)
- `categorize_to_api`: read DB, optionally run LLM categorization, then load to the configured target
- `manual`: prompt-driven mode for ad-hoc runs (`raw_to_api`, `categorize_to_api`, or `categorize_only`)
- `api_check`: run GET checks (`me`, `account`, `datasets`) from `main.py`

## Query-first source (`raw_to_api`, `categorize_to_api`)

These modes always run a single SQL statement to fetch rows. You must supply that statement via **`SOURCE_QUERY` in the environment** or **`--source-sql` on the CLI** (CLI wins when both are set). If neither is set, the program raises a clear error. Table/schema flags (`--schema-name`, `--table-name`) are not used for this path.

Direct raw flow:
```bash
.venv/bin/python main.py --mode raw_to_api --raw-columns "source_event_id,raw_value"
```

Direct categorize flow:
```bash
.venv/bin/python main.py --mode categorize_to_api
```

Categorize flow with custom SQL source:
```bash
.venv/bin/python main.py \
  --mode categorize_to_api \
  --source-sql "SELECT id, name, note FROM public.org" \
  --raw-id-column id \
  --raw-columns "id,name,note" \
  --categorized-columns "note"
```

Direct manual mode:
```bash
.venv/bin/python main.py --mode manual
```

API checks from `main.py`:
```bash
.venv/bin/python main.py --mode api_check --api-check-mode me
.venv/bin/python main.py --mode api_check --api-check-mode datasets
```

## Environment contract (from `.env`)

**Database and source**
- `DB_URL` — PostgreSQL connection string for reads (not used by `api_check`)
- `SOURCE_QUERY` — SQL text for query-first modes; same role as `--source-sql` (CLI overrides env). Required unless you pass `--source-sql` for `raw_to_api` / `categorize_to_api`

**Target**
- `TARGET_TYPE` — `api` (HTTP POST) or `json` (append JSONL to a file)
- `TARGET_API_BASE_URL`, `TARGET_ACCOUNT_UID`, `TARGET_DATASET_UID`, `TARGET_ACCESS_TOKEN` — required when `TARGET_TYPE=api`
- `TARGET_API_PATH_INSERT_DATA` — optional insert path template for `TARGET_TYPE=api` (default: `/api/v1/accounts/{account_uid}/datasets/{dataset_uid}/data`)
- `TARGET_API_TIMEOUT_SECONDS` — optional timeout for API inserts (default `30`)
- `TARGET_OUTPUT_PATH` — required when `TARGET_TYPE=json`

**Processing**
- `BATCH_PROCESS` — rows per load batch (default `10`)
- `DO_CATEGORIZE` — boolean (`true` / `false`, default `false`)
- `CATEGORIZE_COLUMNS` — optional comma-separated column names
- `CATEGORIZE_MAX_RETRIES` — integer, default `3`
- `LOAD_MAX_RETRIES` — optional retry count for API load batches (default `3`)
- `LOAD_RETRY_DELAY_SECONDS` — optional delay between API load retries (default `2`)
- `DEAD_LETTER_PATH` — optional dead-letter output path for failed records (default `outputs/dead_letter_records.jsonl`)

**LLM**

`load_neutral_runtime_config_from_env()` always reads `LLM_MODEL` and `LLM_API_KEY` for any flow that loads the full neutral env (query-first `raw_to_api` / `categorize_to_api`, interactive defaults, and `manual`). Placeholders are fine if you never run categorization, but the keys must be non-empty strings. The LLM is **called** only when categorization is actually applied (`DO_CATEGORIZE=true` with columns for `categorize_to_api`, or `categorize_only` / manual categorization paths). Override at runtime with `--llm-api-key`, `--llm-base-url`, and `--llm-model` where supported.

**API check (optional)**
- `TARGET_API_PATH_AUTH_ME` — override path for `api_check me` (default `/api/v1/auth/me`)
- `TARGET_API_PATH_ACCOUNT` — override path for `api_check account` (default `/api/v1/accounts/{account_uid}`)
- `TARGET_API_PATH_DATASETS` — override path for `api_check datasets` (default `/api/v1/accounts/{account_uid}/datasets`)
- `API_CHECK_TIMEOUT_SECONDS` — timeout for `api_check` when CLI flag is omitted (default `30`)
- `API_CHECK_OUTPUT_DIR` — output directory for `api_check` JSON results when CLI flag is omitted (default `outputs/api`)

`api_check` does not load the neutral runtime block above: it does **not** require `DB_URL`, `TARGET_TYPE`, `LLM_*`, or insert/load settings—only API base URL, bearer token, optional account UID (for `account` / `datasets`), and the path overrides listed here (or their defaults).

## Notes
- Source priority for query-first CLI flows: `--source-sql` overrides `SOURCE_QUERY`; there is no table-based fallback for `raw_to_api` / `categorize_to_api`.
- For custom SQL, aliases must match `--raw-columns`, `--raw-id-column`, and `--categorized-columns`.
- Categorized output columns like `note_categorized` are sent as JSON string values (for string-typed datasets).
- If your destination schema supports array/json types, remove JSON-string serialization and send list values directly.
- In file mode, output is reset once per ETL run, then each batch is appended.
- Failed records are written to `DEAD_LETTER_PATH` (default `outputs/dead_letter_records.jsonl`).
