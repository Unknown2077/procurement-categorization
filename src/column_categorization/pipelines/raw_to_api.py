from __future__ import annotations

import time
from dataclasses import dataclass

from column_categorization.config import EtlLoadConfig
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.schemas.load import LoadResult
from column_categorization.sinks.http_api_sink import HttpApiSink
from column_categorization.utils.batching import chunked


@dataclass(frozen=True)
class RawToApiRequest:
    schema_name: str
    table_name: str
    columns: list[str]
    order_by_column: str | None
    dry_run: bool


@dataclass(frozen=True)
class RawToApiResult:
    total_rows: int
    dry_run: bool
    sample_rows: list[dict[str, object]]
    load_result: LoadResult | None


class RawDbToApiPipeline:
    def __init__(self, reader: PostgresReader, sink: HttpApiSink, load_config: EtlLoadConfig) -> None:
        self._reader = reader
        self._sink = sink
        self._load_config = load_config

    def run(self, request: RawToApiRequest) -> RawToApiResult:
        rows = self._reader.fetch_table_rows(
            schema_name=request.schema_name,
            table_name=request.table_name,
            column_names=request.columns,
            order_by_column_name=request.order_by_column,
        )
        sample_rows = rows[:3]
        if request.dry_run:
            return RawToApiResult(total_rows=len(rows), dry_run=True, sample_rows=sample_rows, load_result=None)

        failures: list[LoadFailure] = []
        loaded_records = 0
        for rows_batch in chunked(rows, self._load_config.load_batch_size):
            batch_result = self._load_with_retry(rows_batch)
            loaded_records += batch_result.loaded_records
            failures.extend(batch_result.failures)
        load_result = LoadResult(
            sink_type="http",
            total_records=len(rows),
            loaded_records=loaded_records,
            failed_records=len(failures),
            failures=failures,
        )
        return RawToApiResult(total_rows=len(rows), dry_run=False, sample_rows=sample_rows, load_result=load_result)

    def _load_with_retry(self, rows_batch: list[dict[str, object]]) -> LoadResult:
        last_error: Exception | None = None
        for attempt in range(self._load_config.load_max_retries + 1):
            try:
                return self._sink.load_rows(rows_batch)
            except Exception as error:
                last_error = error
                if attempt >= self._load_config.load_max_retries:
                    break
                if self._load_config.load_retry_delay_seconds > 0:
                    time.sleep(self._load_config.load_retry_delay_seconds)
        if last_error is None:
            raise RuntimeError("Load retry finished without result and without exception")
        raise RuntimeError(
            "Raw API load failed after "
            f"{self._load_config.load_max_retries + 1} attempts: {last_error}"
        )
