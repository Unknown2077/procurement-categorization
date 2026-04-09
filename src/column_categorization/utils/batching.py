from __future__ import annotations

from collections.abc import Iterator, Sequence


def chunked(values: Sequence[str], batch_size: int) -> Iterator[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    for start_index in range(0, len(values), batch_size):
        end_index = start_index + batch_size
        yield list(values[start_index:end_index])
