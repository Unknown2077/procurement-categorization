from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TargetColumn(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1)
    description: str | None = None


class CategorizationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    database_url: str = Field(min_length=1)
    target_columns: list[TargetColumn] = Field(min_length=1)
    batch_size: int = Field(default=100, ge=1, le=1000)
    schema_name: str = Field(default="public", min_length=1)
    table_name: str = Field(default="original_data", min_length=1)
    source_query: str | None = Field(default=None, min_length=1)
    source_row_limit: int | None = Field(default=None, ge=1)
    prefetched_query_rows: list[dict[str, object]] | None = None
    prefetched_query_columns: list[str] | None = None

    @model_validator(mode="after")
    def validate_unique_column_names(self) -> "CategorizationRequest":
        seen_columns: set[str] = set()
        for target_column in self.target_columns:
            normalized_name = target_column.name.lower()
            if normalized_name in seen_columns:
                raise ValueError(f"Duplicate target column name: {target_column.name}")
            seen_columns.add(normalized_name)
        return self

    @model_validator(mode="after")
    def validate_prefetch_consistency(self) -> "CategorizationRequest":
        rows_set = self.prefetched_query_rows is not None
        columns_set = self.prefetched_query_columns is not None
        if rows_set != columns_set:
            raise ValueError("prefetched_query_rows and prefetched_query_columns must be set together or both omitted")
        return self


class RecordCategorizationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    database_url: str = Field(min_length=1)
    schema_name: str = Field(default="public", min_length=1)
    table_name: str = Field(default="original_data", min_length=1)
    id_column_name: str = Field(default="id", min_length=1)
    raw_value_column_name: str = Field(default="raw_value", min_length=1)
    batch_size: int = Field(default=100, ge=1, le=1000)
    model_name: str = Field(min_length=1)


class ValueMapping(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    raw_value: str = Field(min_length=1)
    labels: list[str] = Field(min_length=1)


class BatchSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: int = Field(ge=1)
    size: int = Field(ge=0)
    status: str = Field(pattern="^(ok|error)$")
    error_message: str | None = None


class ColumnCategorizationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1)
    input_description: str | None = None
    column_description: str | None = None
    total_values: int = Field(ge=0)
    batches: list[BatchSummary]
    mappings: list[ValueMapping]


class PipelineError(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    error_type: str = Field(min_length=1)
    message: str = Field(min_length=1)
    column_name: str | None = None
    batch_index: int | None = Field(default=None, ge=1)


class SourceInfo(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, populate_by_name=True)

    database: str = Field(min_length=1)
    schema_name: str = Field(alias="schema", serialization_alias="schema", min_length=1)
    table: str = Field(min_length=1)


class CategorizationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: SourceInfo
    columns: list[ColumnCategorizationResult]
    errors: list[PipelineError]


class RawRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source_event_id: str = Field(min_length=1)
    raw_value: str = Field(min_length=1)


class CategorizedRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source_event_id: str = Field(min_length=1)
    raw_value: str = Field(min_length=1)
    labels: list[str] = Field(min_length=1)
    confidence_score: float | None = Field(default=None, ge=0, le=1)
    model_name: str = Field(min_length=1)
    categorized_at: datetime


class RecordCategorizationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: SourceInfo
    total_input_rows: int = Field(ge=0)
    total_distinct_values: int = Field(ge=0)
    records: list[CategorizedRecord]
    errors: list[PipelineError]
