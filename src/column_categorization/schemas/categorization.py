from __future__ import annotations

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

    @model_validator(mode="after")
    def validate_unique_column_names(self) -> "CategorizationRequest":
        seen_columns: set[str] = set()
        for target_column in self.target_columns:
            normalized_name = target_column.name.lower()
            if normalized_name in seen_columns:
                raise ValueError(f"Duplicate target column name: {target_column.name}")
            seen_columns.add(normalized_name)
        return self


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
