from __future__ import annotations

import json
from urllib import error, request

from column_categorization.schemas.categorization import CategorizedRecord
from column_categorization.schemas.load import LoadFailure, LoadResult


class HttpApiSink:
    def __init__(self, base_url: str, path: str, auth_token: str | None, timeout_seconds: int = 30) -> None:
        if not base_url.strip():
            raise ValueError("base_url must not be empty")
        if not path.strip():
            raise ValueError("path must not be empty")
        self._url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"
        self._auth_token = auth_token.strip() if auth_token else None
        self._timeout_seconds = timeout_seconds

    def load_records(self, records: list[CategorizedRecord]) -> LoadResult:
        payload = {"rows": [record.model_dump(mode="json") for record in records]}
        response_status = self._post_payload(payload)
        if response_status < 200 or response_status >= 300:
            failure = LoadFailure(
                source_event_id="batch",
                error_message=f"HTTP sink returned unexpected status code: {response_status}",
            )
            return LoadResult(
                sink_type="http",
                total_records=len(records),
                loaded_records=0,
                failed_records=len(records),
                failures=[failure],
            )
        return LoadResult(
            sink_type="http",
            total_records=len(records),
            loaded_records=len(records),
            failed_records=0,
            failures=[],
        )

    def _post_payload(self, payload: dict[str, object]) -> int:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers = {"Content-Type": "application/json"}
        if self._auth_token:
            request_headers["Authorization"] = f"Bearer {self._auth_token}"
        http_request = request.Request(self._url, data=body, headers=request_headers, method="POST")
        try:
            with request.urlopen(http_request, timeout=self._timeout_seconds) as response:
                return response.status
        except error.HTTPError as http_error:
            raise ValueError(f"HTTP sink error: status={http_error.code}, reason={http_error.reason}") from http_error
        except error.URLError as url_error:
            raise ValueError(f"HTTP sink connection error: {url_error.reason}") from url_error
