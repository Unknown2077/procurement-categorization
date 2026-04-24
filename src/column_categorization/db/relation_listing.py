from __future__ import annotations

from column_categorization.db.postgres_reader import PostgresReader


def list_accessible_relations(reader: PostgresReader) -> list[str]:
    relation_query = (
        "SELECT table_schema, table_name "
        "FROM information_schema.tables "
        "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
        "ORDER BY table_schema, table_name"
    )
    relation_rows, _ = reader.fetch_rows_and_columns_by_sql(relation_query)
    relation_names: list[str] = []
    for relation_row in relation_rows:
        schema_name = relation_row.get("table_schema")
        table_name = relation_row.get("table_name")
        if isinstance(schema_name, str) and isinstance(table_name, str):
            relation_names.append(f"{schema_name}.{table_name}")
    return relation_names
