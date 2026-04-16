from __future__ import annotations

import pytest

from column_categorization.db import postgres_reader


class _FakeColumn:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeCursor:
    def __init__(self) -> None:
        self.description = [_FakeColumn("id"), _FakeColumn("name"), _FakeColumn("note")]
        self._rows = [
            (1, "PT ABC", "Pengadaan jasa survei"),
            (2, "CV XYZ", "Pengadaan perangkat server"),
        ]
        self.executed_query: str | None = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def execute(self, query: str) -> None:
        self.executed_query = query

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _FakeConnection:
    def __init__(self) -> None:
        self.cursor_instance = _FakeCursor()

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self.cursor_instance


def test_fetch_rows_and_columns_by_sql_maps_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_connection = _FakeConnection()
    monkeypatch.setattr(postgres_reader, "connect", lambda *_args, **_kwargs: fake_connection)

    reader = postgres_reader.PostgresReader(database_url="postgresql://demo:demo@localhost:5432/demo")
    rows, columns = reader.fetch_rows_and_columns_by_sql("SELECT id, name, note FROM public.org")

    assert columns == ["id", "name", "note"]
    assert rows == [
        {"id": 1, "name": "PT ABC", "note": "Pengadaan jasa survei"},
        {"id": 2, "name": "CV XYZ", "note": "Pengadaan perangkat server"},
    ]
    assert fake_connection.cursor_instance.executed_query == "SELECT id, name, note FROM public.org"


def test_fetch_rows_and_columns_by_sql_rejects_empty_query() -> None:
    reader = postgres_reader.PostgresReader(database_url="postgresql://demo:demo@localhost:5432/demo")
    with pytest.raises(ValueError, match="source_sql must not be empty"):
        reader.fetch_rows_and_columns_by_sql("   ")
