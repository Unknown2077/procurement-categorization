from __future__ import annotations

import csv
import json
from pathlib import Path

from column_categorization.schemas.categorization import CategorizedRecord
from column_categorization.schemas.load import LoadResult
from column_categorization.sinks.http_api_sink import _to_json_safe


class FileSink:
    def __init__(self, output_path: str, output_format: str) -> None:
        if not output_path.strip():
            raise ValueError("output_path must not be empty")
        normalized_format = output_format.strip().lower()
        if normalized_format not in {"jsonl", "csv"}:
            raise ValueError("output_format must be one of: jsonl, csv")
        self._output_path = Path(output_path)
        self._output_format = normalized_format

    def load_records(self, records: list[CategorizedRecord]) -> LoadResult:
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        if self._output_format == "jsonl":
            self._write_jsonl(records)
        else:
            self._write_csv(records)
        return LoadResult(
            sink_type="file",
            total_records=len(records),
            loaded_records=len(records),
            failed_records=0,
            failures=[],
        )

    def load_rows(self, rows: list[dict[str, object]]) -> LoadResult:
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        if self._output_format != "jsonl":
            raise ValueError("load_rows currently supports jsonl output only")
        with self._output_path.open("a", encoding="utf-8") as output_file:
            for row in rows:
                output_file.write(f"{json.dumps(_to_json_safe(row), ensure_ascii=False)}\n")
        return LoadResult(
            sink_type="file",
            total_records=len(rows),
            loaded_records=len(rows),
            failed_records=0,
            failures=[],
        )

    def reset_output(self) -> None:
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        if self._output_path.exists():
            self._output_path.unlink()

    def _write_jsonl(self, records: list[CategorizedRecord]) -> None:
        with self._output_path.open("a", encoding="utf-8") as output_file:
            for record in records:
                output_file.write(f"{json.dumps(record.model_dump(mode='json'), ensure_ascii=False)}\n")

    def _write_csv(self, records: list[CategorizedRecord]) -> None:
        should_write_header = (not self._output_path.exists()) or self._output_path.stat().st_size == 0
        with self._output_path.open("a", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(
                output_file,
                fieldnames=[
                    "source_event_id",
                    "raw_value",
                    "labels",
                    "confidence_score",
                    "model_name",
                    "categorized_at",
                ],
            )
            if should_write_header:
                writer.writeheader()
            for record in records:
                writer.writerow(
                    {
                        "source_event_id": record.source_event_id,
                        "raw_value": record.raw_value,
                        "labels": json.dumps(record.labels, ensure_ascii=False),
                        "confidence_score": record.confidence_score,
                        "model_name": record.model_name,
                        "categorized_at": record.categorized_at.isoformat(),
                    }
                )
