# Vendor-Agnostic ETL Categorization

This project reads raw values from PostgreSQL, categorizes them with an LLM, and loads results to a configurable sink (`file`, `http`, or `db`).

## Setup
```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env
```

Fill `.env` first.

Minimal HTTP env:
```env
SINK_TYPE=http
API_BASE_URL=https://example-api.test
ACCOUNT_UID=replace_me
DATASET_UID=replace_me
API_PATH_INSERT_DATA=/api/v1/accounts/{account_uid}/datasets/{dataset_uid}/data
SINK_HTTP_PATH={API_PATH_INSERT_DATA}
SINK_HTTP_AUTH_TOKEN=replace_me
# Optional source override:
# SOURCE_SQL=SELECT id, name, note FROM public.org
```

## Run (recommended)
Interactive mode:
```bash
.venv/bin/python main.py
```

## Choose a Flow
- `raw_to_api`: read selected raw columns from DB and send directly to destination API
- `categorize_to_api`: read DB, run LLM categorization, then send to destination API
- `manual`: prompt-driven mode for ad-hoc runs (`raw_to_api`, `categorize_to_api`, or `categorize_only`)
- `api_check`: run GET checks (`me`, `account`, `datasets`) from `main.py`

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

## Sink config (from `.env`)
- `SINK_TYPE=file|http|db`
- File: `SINK_FILE_PATH`, `SINK_FILE_FORMAT`
- HTTP base URL: `SINK_HTTP_BASE_URL` (or `API_BASE_URL`)
- HTTP path: `SINK_HTTP_PATH` (or fallback `API_PATH_INSERT_DATA`)
- HTTP auth: `SINK_HTTP_AUTH_TOKEN`
- DB: `SINK_DB_URL`, `SINK_DB_SCHEMA`, `SINK_DB_TABLE`

API path templates:
- `API_PATH_AUTH_ME`
- `API_PATH_ACCOUNT`
- `API_PATH_DATASETS`
- `API_PATH_INSERT_DATA`

## Notes
- Source priority: `--source-sql` or `SOURCE_SQL` (if set) overrides schema/table extraction.
- For custom SQL, aliases must match `--raw-columns`, `--raw-id-column`, and `--categorized-columns`.
- Categorized output columns like `note_categorized` are sent as JSON string values (for string-typed datasets).
- If your destination schema supports array/json types, remove JSON-string serialization and send list values directly.
- Use placeholders in env paths (for example `{account_uid}`, `{dataset_uid}`).
- In file mode, output is reset once per ETL run, then each batch is appended.
- Retry config: `LOAD_BATCH_SIZE`, `LOAD_MAX_RETRIES`, `LOAD_RETRY_DELAY_SECONDS`
- Failed records are written to `LOAD_DEAD_LETTER_PATH`.
