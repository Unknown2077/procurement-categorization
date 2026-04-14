from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import urlparse

from column_categorization.categorization.llm_categorizer import BatchCategorizer
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.schemas.categorization import (
    CategorizedRecord,
    BatchSummary,
    CategorizationRequest,
    CategorizationResponse,
    ColumnCategorizationResult,
    PipelineError,
    RawRecord,
    RecordCategorizationRequest,
    RecordCategorizationResponse,
    SourceInfo,
    ValueMapping,
)
from column_categorization.utils.batching import chunked


def _build_source_info(database_url: str, schema_name: str, table_name: str) -> SourceInfo:
    parsed_url = urlparse(database_url)
    if not parsed_url.path or parsed_url.path == "/":
        raise ValueError("database_url must include database name in path")
    return SourceInfo(database=parsed_url.path.removeprefix("/"), schema_name=schema_name, table=table_name)


class ColumnCategorizationPipeline:
    def __init__(self, reader: PostgresReader, categorizer: BatchCategorizer) -> None:
        self._reader = reader
        self._categorizer = categorizer

    def run(self, request: CategorizationRequest) -> CategorizationResponse:
        source = _build_source_info(
            database_url=request.database_url,
            schema_name=request.schema_name,
            table_name=request.table_name,
        )
        columns: list[ColumnCategorizationResult] = []
        errors: list[PipelineError] = []

        for target_column in request.target_columns:
            raw_values = self._reader.fetch_distinct_values(
                schema_name=request.schema_name,
                table_name=request.table_name,
                column_name=target_column.name,
            )
            batch_summaries: list[BatchSummary] = []
            merged_mappings: dict[str, ValueMapping] = {}
            resolved_column_description: str | None = None

            for batch_index, values_batch in enumerate(chunked(raw_values, request.batch_size), start=1):
                try:
                    categorized_batch = self._categorizer.categorize_batch(
                        column_name=target_column.name,
                        column_description=target_column.description,
                        raw_values=values_batch,
                    )
                    if resolved_column_description is None:
                        resolved_column_description = categorized_batch.column_description
                    for mapping in categorized_batch.mappings:
                        merged_mappings[mapping.raw_value] = mapping
                    batch_summaries.append(BatchSummary(index=batch_index, size=len(values_batch), status="ok"))
                except Exception as error:
                    batch_summaries.append(
                        BatchSummary(
                            index=batch_index,
                            size=len(values_batch),
                            status="error",
                            error_message=str(error),
                        )
                    )
                    errors.append(
                        PipelineError(
                            error_type=type(error).__name__,
                            message=str(error),
                            column_name=target_column.name,
                            batch_index=batch_index,
                        )
                    )

            columns.append(
                ColumnCategorizationResult(
                    name=target_column.name,
                    input_description=target_column.description,
                    column_description=resolved_column_description,
                    total_values=len(raw_values),
                    batches=batch_summaries,
                    mappings=sorted(merged_mappings.values(), key=lambda item: item.raw_value),
                )
            )

        return CategorizationResponse(source=source, columns=columns, errors=errors)


class RecordCategorizationPipeline:
    def __init__(self, reader: PostgresReader, categorizer: BatchCategorizer) -> None:
        self._reader = reader
        self._categorizer = categorizer

    def run(self, request: RecordCategorizationRequest) -> RecordCategorizationResponse:
        source = _build_source_info(
            database_url=request.database_url,
            schema_name=request.schema_name,
            table_name=request.table_name,
        )
        raw_records = self._reader.fetch_raw_records(
            schema_name=request.schema_name,
            table_name=request.table_name,
            id_column_name=request.id_column_name,
            value_column_name=request.raw_value_column_name,
        )
        distinct_values = sorted({record.raw_value for record in raw_records})
        mapping_by_raw_value, errors = self._categorize_values(
            values=distinct_values,
            batch_size=request.batch_size,
            raw_value_column_name=request.raw_value_column_name,
        )
        categorized_records = self._build_categorized_records(
            raw_records=raw_records,
            mapping_by_raw_value=mapping_by_raw_value,
            model_name=request.model_name,
        )
        return RecordCategorizationResponse(
            source=source,
            total_input_rows=len(raw_records),
            total_distinct_values=len(distinct_values),
            records=categorized_records,
            errors=errors,
        )

    def _categorize_values(
        self,
        values: list[str],
        batch_size: int,
        raw_value_column_name: str,
    ) -> tuple[dict[str, ValueMapping], list[PipelineError]]:
        merged_mappings: dict[str, ValueMapping] = {}
        errors: list[PipelineError] = []
        for batch_index, values_batch in enumerate(chunked(values, batch_size), start=1):
            try:
                categorized_batch = self._categorizer.categorize_batch(
                    column_name=raw_value_column_name,
                    column_description=None,
                    raw_values=values_batch,
                )
                for mapping in categorized_batch.mappings:
                    merged_mappings[mapping.raw_value] = mapping
            except Exception as error:
                errors.append(
                    PipelineError(
                        error_type=type(error).__name__,
                        message=str(error),
                        column_name=raw_value_column_name,
                        batch_index=batch_index,
                    )
                )
        return merged_mappings, errors

    def _build_categorized_records(
        self,
        raw_records: list[RawRecord],
        mapping_by_raw_value: dict[str, ValueMapping],
        model_name: str,
    ) -> list[CategorizedRecord]:
        categorized_at = datetime.now(tz=UTC)
        categorized_records: list[CategorizedRecord] = []
        for raw_record in raw_records:
            mapping = mapping_by_raw_value.get(raw_record.raw_value)
            if mapping is None:
                continue
            categorized_records.append(
                CategorizedRecord(
                    source_event_id=raw_record.source_event_id,
                    raw_value=raw_record.raw_value,
                    labels=mapping.labels,
                    confidence_score=None,
                    model_name=model_name,
                    categorized_at=categorized_at,
                )
            )
        return categorized_records
