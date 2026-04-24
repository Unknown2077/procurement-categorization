from __future__ import annotations

from column_categorization.sinks.http_api_sink import _normalize_row_keys


def test_payload_key_normalization_for_api_rows() -> None:
    normalized = _normalize_row_keys(
        {
            "Source Event ID": 1,
            "Raw-Value": "alpha",
            "123 Invalid Key": "x",
            "": "fallback",
        }
    )
    assert normalized == {
        "source_event_id": 1,
        "raw_value": "alpha",
        "col_123_invalid_key": "x",
        "column": "fallback",
    }
