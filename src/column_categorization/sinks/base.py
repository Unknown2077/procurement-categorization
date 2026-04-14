from __future__ import annotations

from typing import Protocol

from column_categorization.schemas.categorization import CategorizedRecord
from column_categorization.schemas.load import LoadResult


class Sink(Protocol):
    def load_records(self, records: list[CategorizedRecord]) -> LoadResult: ...
