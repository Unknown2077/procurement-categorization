from __future__ import annotations

import json

from psycopg import connect, sql

from column_categorization.schemas.categorization import CategorizedRecord
from column_categorization.schemas.load import LoadFailure, LoadResult


class DatabaseSink:
    def __init__(self, database_url: str, schema_name: str, table_name: str) -> None:
        if not database_url.strip():
            raise ValueError("database_url must not be empty")
        if not schema_name.strip():
            raise ValueError("schema_name must not be empty")
        if not table_name.strip():
            raise ValueError("table_name must not be empty")
        self._database_url = database_url
        self._schema_name = schema_name
        self._table_name = table_name

    def load_records(self, records: list[CategorizedRecord]) -> LoadResult:
        insert_query = sql.SQL(
            """
            INSERT INTO {schema}.{table}
            (
                source_event_id,
                raw_value,
                labels,
                confidence_score,
                model_name,
                categorized_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """
        ).format(schema=sql.Identifier(self._schema_name), table=sql.Identifier(self._table_name))
        failures: list[LoadFailure] = []
        loaded_records = 0
        with connect(self._database_url) as connection:
            with connection.cursor() as cursor:
                for record in records:
                    try:
                        cursor.execute("SAVEPOINT record_insert")
                        cursor.execute(
                            insert_query,
                            (
                                record.source_event_id,
                                record.raw_value,
                                json.dumps(record.labels, ensure_ascii=False),
                                record.confidence_score,
                                record.model_name,
                                record.categorized_at,
                            ),
                        )
                        cursor.execute("RELEASE SAVEPOINT record_insert")
                        loaded_records += 1
                    except Exception as error:
                        cursor.execute("ROLLBACK TO SAVEPOINT record_insert")
                        cursor.execute("RELEASE SAVEPOINT record_insert")
                        failures.append(
                            LoadFailure(source_event_id=record.source_event_id, error_message=str(error))
                        )
            connection.commit()
        return LoadResult(
            sink_type="db",
            total_records=len(records),
            loaded_records=loaded_records,
            failed_records=len(failures),
            failures=failures,
        )
