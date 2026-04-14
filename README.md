# Vendor-Agnostic ETL Categorization

This project reads raw values from PostgreSQL, categorizes them with an LLM, and loads results to a configurable sink (`file`, `http`, or `db`).

## Setup
```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
cp .env.example .env
```

Fill `.env` first.

## Run (recommended)
Interactive mode:
```bash
.venv/bin/python main.py
```

Direct ETL mode:
```bash
.venv/bin/python main.py --mode etl
```

Direct manual mode (categorization only):
```bash
.venv/bin/python main.py --mode manual --target-columns-json '[{"name":"raw_value","description":"Raw procurement text"}]'
```

## Sink config (from `.env`)
- `SINK_TYPE=file|http|db`
- File: `SINK_FILE_PATH`, `SINK_FILE_FORMAT`
- HTTP: `SINK_HTTP_BASE_URL`, `SINK_HTTP_PATH`, `SINK_HTTP_AUTH_TOKEN`
- DB: `SINK_DB_URL`, `SINK_DB_SCHEMA`, `SINK_DB_TABLE`

## Notes
- In file mode, output is reset once per ETL run, then each batch is appended.
- Retry config: `LOAD_BATCH_SIZE`, `LOAD_MAX_RETRIES`, `LOAD_RETRY_DELAY_SECONDS`
- Failed records are written to `LOAD_DEAD_LETTER_PATH`.
