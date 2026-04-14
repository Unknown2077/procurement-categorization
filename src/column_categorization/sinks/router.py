from __future__ import annotations

from column_categorization.config import EtlSinkConfig
from column_categorization.sinks.base import Sink
from column_categorization.sinks.database_sink import DatabaseSink
from column_categorization.sinks.file_sink import FileSink
from column_categorization.sinks.http_api_sink import HttpApiSink


class SinkRouter:
    def __init__(self, sink_config: EtlSinkConfig) -> None:
        self._sink_config = sink_config

    def build_sink(self) -> Sink:
        if self._sink_config.sink_type == "http":
            if self._sink_config.http_base_url is None:
                raise ValueError("SINK_HTTP_BASE_URL is required for SINK_TYPE=http")
            if self._sink_config.http_path is None:
                raise ValueError("SINK_HTTP_PATH is required for SINK_TYPE=http")
            return HttpApiSink(
                base_url=self._sink_config.http_base_url,
                path=self._sink_config.http_path,
                auth_token=self._sink_config.http_auth_token,
                timeout_seconds=self._sink_config.http_timeout_seconds,
            )
        if self._sink_config.sink_type == "db":
            if self._sink_config.db_url is None:
                raise ValueError("SINK_DB_URL is required for SINK_TYPE=db")
            if self._sink_config.db_schema is None:
                raise ValueError("SINK_DB_SCHEMA is required for SINK_TYPE=db")
            if self._sink_config.db_table is None:
                raise ValueError("SINK_DB_TABLE is required for SINK_TYPE=db")
            return DatabaseSink(
                database_url=self._sink_config.db_url,
                schema_name=self._sink_config.db_schema,
                table_name=self._sink_config.db_table,
            )
        if self._sink_config.file_path is None:
            raise ValueError("SINK_FILE_PATH is required for SINK_TYPE=file")
        return FileSink(output_path=self._sink_config.file_path, output_format=self._sink_config.file_format)
