from __future__ import annotations

import json
import time
from pathlib import Path

from column_categorization.config import EtlLoadConfig
from column_categorization.schemas.categorization import CategorizedRecord
from column_categorization.schemas.load import EtlExecutionResult, LoadFailure, LoadResult
from column_categorization.sinks.base import Sink
from column_categorization.sinks.file_sink import FileSink
from column_categorization.utils.batching import chunked


class EtlExecutionPipeline:
    def __init__(self, sink: Sink, load_config: EtlLoadConfig) -> None:
        self._sink = sink
        self._load_config = load_config

    def run(self, records: list[CategorizedRecord], categorization_error_count: int) -> EtlExecutionResult:
        self._prepare_sink_for_run()
        aggregate_failures: list[LoadFailure] = []
        dead_letter_records: list[CategorizedRecord] = []
        loaded_records = 0
        for records_batch in chunked(records, self._load_config.load_batch_size):
            batch_result = self._load_with_retry(records_batch)
            loaded_records += batch_result.loaded_records
            aggregate_failures.extend(batch_result.failures)
            if batch_result.failed_records > 0:
                failed_source_event_ids = {failure.source_event_id for failure in batch_result.failures}
                dead_letter_records.extend(
                    [record for record in records_batch if record.source_event_id in failed_source_event_ids]
                )
        aggregate_result = LoadResult(
            sink_type=self._resolve_sink_type(),
            total_records=len(records),
            loaded_records=loaded_records,
            failed_records=len(aggregate_failures),
            failures=aggregate_failures,
        )
        if dead_letter_records:
            self._write_dead_letter_records(dead_letter_records)
        return EtlExecutionResult(
            total_categorized_records=len(records),
            total_errors=categorization_error_count + len(aggregate_failures),
            load_result=aggregate_result,
            dead_letter_records=dead_letter_records,
        )

    def _load_with_retry(self, records_batch: list[CategorizedRecord]) -> LoadResult:
        last_error: Exception | None = None
        for attempt in range(self._load_config.load_max_retries + 1):
            try:
                return self._sink.load_records(records_batch)
            except Exception as error:
                last_error = error
                if attempt >= self._load_config.load_max_retries:
                    break
                if self._load_config.load_retry_delay_seconds > 0:
                    time.sleep(self._load_config.load_retry_delay_seconds)
        if last_error is None:
            raise RuntimeError("Load retry finished without result and without exception")
        failures = [
            LoadFailure(source_event_id=record.source_event_id, error_message=str(last_error))
            for record in records_batch
        ]
        return LoadResult(
            sink_type=self._resolve_sink_type(),
            total_records=len(records_batch),
            loaded_records=0,
            failed_records=len(records_batch),
            failures=failures,
        )

    def _write_dead_letter_records(self, records: list[CategorizedRecord]) -> None:
        dead_letter_file = Path(self._load_config.dead_letter_path)
        dead_letter_file.parent.mkdir(parents=True, exist_ok=True)
        with dead_letter_file.open("w", encoding="utf-8") as output_file:
            for record in records:
                output_file.write(f"{json.dumps(record.model_dump(mode='json'), ensure_ascii=False)}\n")

    def _resolve_sink_type(self) -> str:
        return type(self._sink).__name__.removesuffix("Sink").lower()

    def _prepare_sink_for_run(self) -> None:
        if isinstance(self._sink, FileSink):
            self._sink.reset_output()
