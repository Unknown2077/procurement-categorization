from __future__ import annotations

from collections.abc import Sequence

from psycopg import Connection, connect, sql

from column_categorization.schemas.categorization import RawRecord


class PostgresReader:
    def __init__(self, database_url: str) -> None:
        if not database_url.strip():
            raise ValueError("database_url must not be empty")
        self._database_url = database_url

    def fetch_distinct_values(self, schema_name: str, table_name: str, column_name: str) -> list[str]:
        if not schema_name.strip():
            raise ValueError("schema_name must not be empty")
        if not table_name.strip():
            raise ValueError("table_name must not be empty")
        if not column_name.strip():
            raise ValueError("column_name must not be empty")

        with connect(self._database_url, options="-c default_transaction_read_only=on") as connection:
            self._ensure_column_exists(connection, schema_name=schema_name, table_name=table_name, column_name=column_name)
            query = self._build_distinct_query(schema_name=schema_name, table_name=table_name, column_name=column_name)
            rows = connection.execute(query).fetchall()

        return [row[0] for row in rows if isinstance(row[0], str) and row[0].strip()]

    def fetch_raw_records(
        self,
        schema_name: str,
        table_name: str,
        id_column_name: str,
        value_column_name: str,
    ) -> list[RawRecord]:
        if not schema_name.strip():
            raise ValueError("schema_name must not be empty")
        if not table_name.strip():
            raise ValueError("table_name must not be empty")
        if not id_column_name.strip():
            raise ValueError("id_column_name must not be empty")
        if not value_column_name.strip():
            raise ValueError("value_column_name must not be empty")

        with connect(self._database_url, options="-c default_transaction_read_only=on") as connection:
            self._ensure_column_exists(
                connection=connection,
                schema_name=schema_name,
                table_name=table_name,
                column_name=id_column_name,
            )
            self._ensure_column_exists(
                connection=connection,
                schema_name=schema_name,
                table_name=table_name,
                column_name=value_column_name,
            )
            query = self._build_raw_records_query(
                schema_name=schema_name,
                table_name=table_name,
                id_column_name=id_column_name,
                value_column_name=value_column_name,
            )
            rows = connection.execute(query).fetchall()

        output_records: list[RawRecord] = []
        for row in rows:
            source_event_id = str(row[0]).strip() if row[0] is not None else ""
            raw_value = row[1].strip() if isinstance(row[1], str) else ""
            if not source_event_id or not raw_value:
                continue
            output_records.append(RawRecord(source_event_id=source_event_id, raw_value=raw_value))
        return output_records

    def _ensure_column_exists(
        self,
        connection: Connection,
        schema_name: str,
        table_name: str,
        column_name: str,
    ) -> None:
        check_query = sql.SQL(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
            """
        )
        row = connection.execute(check_query, (schema_name, table_name, column_name)).fetchone()
        if row is None:
            raise ValueError(f"Column '{column_name}' not found in {schema_name}.{table_name}")

    def _build_distinct_query(self, schema_name: str, table_name: str, column_name: str) -> sql.Composed:
        return sql.SQL(
            """
            SELECT DISTINCT TRIM({column}::text) AS value
            FROM {schema}.{table}
            WHERE {column} IS NOT NULL
              AND TRIM({column}::text) <> ''
            ORDER BY value
            """
        ).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            column=sql.Identifier(column_name),
        )

    def _build_raw_records_query(
        self,
        schema_name: str,
        table_name: str,
        id_column_name: str,
        value_column_name: str,
    ) -> sql.Composed:
        return sql.SQL(
            """
            SELECT {id_column}, TRIM({value_column}::text) AS raw_value
            FROM {schema}.{table}
            WHERE {id_column} IS NOT NULL
              AND {value_column} IS NOT NULL
              AND TRIM({value_column}::text) <> ''
            ORDER BY {id_column}
            """
        ).format(
            id_column=sql.Identifier(id_column_name),
            value_column=sql.Identifier(value_column_name),
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        )

    def fetch_table_rows(
        self,
        schema_name: str,
        table_name: str,
        column_names: Sequence[str],
        order_by_column_name: str | None = None,
    ) -> list[dict[str, object]]:
        if not schema_name.strip():
            raise ValueError("schema_name must not be empty")
        if not table_name.strip():
            raise ValueError("table_name must not be empty")
        if not column_names:
            raise ValueError("column_names must not be empty")

        normalized_columns: list[str] = []
        for column_name in column_names:
            normalized_name = column_name.strip()
            if not normalized_name:
                raise ValueError("column_names must not contain empty values")
            normalized_columns.append(normalized_name)

        with connect(self._database_url, options="-c default_transaction_read_only=on") as connection:
            for column_name in normalized_columns:
                self._ensure_column_exists(
                    connection=connection,
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=column_name,
                )
            if order_by_column_name is not None:
                self._ensure_column_exists(
                    connection=connection,
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=order_by_column_name,
                )
            query = self._build_table_rows_query(
                schema_name=schema_name,
                table_name=table_name,
                column_names=normalized_columns,
                order_by_column_name=order_by_column_name,
            )
            rows = connection.execute(query).fetchall()

        output_rows: list[dict[str, object]] = []
        for row in rows:
            output_rows.append({column_name: row[index] for index, column_name in enumerate(normalized_columns)})
        return output_rows

    def _build_table_rows_query(
        self,
        schema_name: str,
        table_name: str,
        column_names: Sequence[str],
        order_by_column_name: str | None,
    ) -> sql.Composed:
        selected_columns = sql.SQL(", ").join(sql.Identifier(column_name) for column_name in column_names)
        base_query = sql.SQL("SELECT {columns} FROM {schema}.{table}").format(
            columns=selected_columns,
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        )
        if order_by_column_name is None:
            return base_query
        return sql.SQL("{base} ORDER BY {order_column}").format(
            base=base_query,
            order_column=sql.Identifier(order_by_column_name),
        )
