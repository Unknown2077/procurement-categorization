from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from column_categorization.schemas.categorization import CategorizedRecord


class LoadFailure(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source_event_id: str = Field(min_length=1)
    error_message: str = Field(min_length=1)


class LoadResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sink_type: str = Field(min_length=1)
    total_records: int = Field(ge=0)
    loaded_records: int = Field(ge=0)
    failed_records: int = Field(ge=0)
    failures: list[LoadFailure]


class EtlExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_categorized_records: int = Field(ge=0)
    total_errors: int = Field(ge=0)
    load_result: LoadResult
    dead_letter_records: list[CategorizedRecord]
