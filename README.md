# Column Categorization Pipeline

Python pipeline to read values from a source table, categorize them with an LLM provider, and return JSON output.

## Input
- `database_url`
- `target_columns`: array of objects with:
  - `name` (database column name)
  - `description` (nullable, used as LLM context)

## Output
- JSON categorization result
- No insert/create table operation

## Setup
```bash
python -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

Create `.env`:
```env
DATABASE_URL=postgresql://user:pass@host:5432/db_name
NIM_API_KEY=your_api_key
NIM_BASE_URL=https://your-llm-endpoint/v1
NIM_MODEL=your-model-name
```

## Run
```bash
.venv/bin/python scripts/run_column_categorization.py \
  --target-columns-json '[{"name":"your_column_name","description":"Optional context"}]' \
  --batch-size 10
```

## Notes
- `batch-size` is the number of values per LLM request, not number of columns.
- Use real database column names for `target_columns[].name`.
