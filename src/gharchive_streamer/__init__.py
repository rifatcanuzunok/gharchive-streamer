from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from ._cache import CachedFetcher
from ._client import Fetcher, HttpFetcher, RetryingFetcher
from ._exceptions import (
    DataUnavailableError,
    DecompressionError,
    GHArchiveError,
    NetworkError,
)
from ._gharchive_streamer import GHArchiveStreamer
from ._models import generate_timestamps

logger = logging.getLogger(__name__)

__all__ = [
    "CachedFetcher",
    "DataUnavailableError",
    "DecompressionError",
    "GHArchiveError",
    "GHArchiveStreamer",
    "HttpFetcher",
    "NetworkError",
    "RetryingFetcher",
    "stream_events",
]


def stream_events(
    start: datetime,
    end: datetime,
    *,
    use_cache: bool = False,
    cache_dir: str = ".gharchive_cache",
    fetcher: Fetcher | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Iterator[dict[str, Any]]:
    if not fetcher:
        fetcher = HttpFetcher()
    if max_retries > 0:
        fetcher = RetryingFetcher(fetcher, max_retries=max_retries, retry_delay=retry_delay)
    if use_cache:
        fetcher = CachedFetcher(fetcher, cache_dir=cache_dir)

    streamer = GHArchiveStreamer(fetcher)

    for ts in generate_timestamps(start, end):
        try:
            yield from streamer.stream_hour(ts)
        except DataUnavailableError:
            logger.warning("No data found, skipping: %s", ts.to_url())
