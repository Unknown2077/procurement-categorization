from __future__ import annotations

from column_categorization.config import EtlSinkConfig
from column_categorization.sinks.base import Sink
from column_categorization.sinks.file_sink import FileSink
from column_categorization.sinks.http_api_sink import HttpApiSink


class SinkRouter:
    def __init__(self, sink_config: EtlSinkConfig) -> None:
        self._sink_config = sink_config

    def build_sink(self) -> Sink:
        if self._sink_config.target_type == "api":
            if self._sink_config.http_base_url is None:
                raise ValueError("TARGET_API_BASE_URL is required when TARGET_TYPE=api")
            if self._sink_config.http_path is None:
                raise ValueError("Resolved insert path is missing for TARGET_TYPE=api")
            return HttpApiSink(
                base_url=self._sink_config.http_base_url,
                path=self._sink_config.http_path,
                auth_token=self._sink_config.http_auth_token,
                timeout_seconds=self._sink_config.http_timeout_seconds,
            )
        if self._sink_config.file_path is None:
            raise ValueError("TARGET_OUTPUT_PATH is required when TARGET_TYPE=json")
        return FileSink(output_path=self._sink_config.file_path, output_format=self._sink_config.file_format)
