from __future__ import annotations

from column_categorization.config import EtlLoadConfig
from column_categorization.pipelines.raw_to_api import RawDbToApiPipeline, RawToApiRequest
from column_categorization.schemas.load import LoadResult


class _FakeReader:
    def fetch_table_rows(
        self,
        schema_name: str,
        table_name: str,
        column_names: list[str],
        order_by_column_name: str | None,
    ) -> list[dict[str, object]]:
        assert schema_name == "public"
        assert table_name == "event_raw_staging"
        assert column_names == ["source_event_id", "raw_value"]
        assert order_by_column_name == "source_event_id"
        return [
            {"source_event_id": 1, "raw_value": "alpha"},
            {"source_event_id": 2, "raw_value": "beta"},
        ]


class _FakeHttpSink:
    def __init__(self) -> None:
        self.loaded_batches: list[list[dict[str, object]]] = []

    def load_rows(self, rows: list[dict[str, object]]) -> LoadResult:
        self.loaded_batches.append(rows)
        return LoadResult(
            sink_type="http",
            total_records=len(rows),
            loaded_records=len(rows),
            failed_records=0,
            failures=[],
        )


def test_raw_db_to_api_pipeline_loads_all_rows() -> None:
    pipeline = RawDbToApiPipeline(
        reader=_FakeReader(),  # type: ignore[arg-type]
        sink=_FakeHttpSink(),  # type: ignore[arg-type]
        load_config=EtlLoadConfig(
            load_batch_size=10,
            load_max_retries=0,
            load_retry_delay_seconds=0,
            dead_letter_path="outputs/dead_letter_records.jsonl",
        ),
    )
    request = RawToApiRequest(
        schema_name="public",
        table_name="event_raw_staging",
        columns=["source_event_id", "raw_value"],
        order_by_column="source_event_id",
        dry_run=False,
    )
    result = pipeline.run(request)
    assert result.total_rows == 2
    assert result.dry_run is False
    assert result.load_result is not None
    assert result.load_result.loaded_records == 2


def test_raw_db_to_api_pipeline_dry_run_skips_send() -> None:
    sink = _FakeHttpSink()
    pipeline = RawDbToApiPipeline(
        reader=_FakeReader(),  # type: ignore[arg-type]
        sink=sink,  # type: ignore[arg-type]
        load_config=EtlLoadConfig(
            load_batch_size=10,
            load_max_retries=0,
            load_retry_delay_seconds=0,
            dead_letter_path="outputs/dead_letter_records.jsonl",
        ),
    )
    request = RawToApiRequest(
        schema_name="public",
        table_name="event_raw_staging",
        columns=["source_event_id", "raw_value"],
        order_by_column="source_event_id",
        dry_run=True,
    )
    result = pipeline.run(request)
    assert result.total_rows == 2
    assert result.dry_run is True
    assert result.load_result is None
    assert sink.loaded_batches == []
