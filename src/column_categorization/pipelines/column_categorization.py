from __future__ import annotations

from urllib.parse import urlparse

from column_categorization.categorization.llm_categorizer import BatchCategorizer
from column_categorization.db.postgres_reader import PostgresReader
from column_categorization.schemas.categorization import (
    BatchSummary,
    CategorizationRequest,
    CategorizationResponse,
    ColumnCategorizationResult,
    PipelineError,
    SourceInfo,
    ValueMapping,
)
from column_categorization.utils.batching import chunked


class ColumnCategorizationPipeline:
    def __init__(self, reader: PostgresReader, categorizer: BatchCategorizer) -> None:
        self._reader = reader
        self._categorizer = categorizer

    def run(self, request: CategorizationRequest) -> CategorizationResponse:
        parsed_url = urlparse(request.database_url)
        if not parsed_url.path or parsed_url.path == "/":
            raise ValueError("database_url must include database name in path")

        source = SourceInfo(
            database=parsed_url.path.removeprefix("/"),
            schema_name=request.schema_name,
            table=request.table_name,
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
