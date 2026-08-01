from __future__ import annotations

import logging
from collections.abc import Generator
from datetime import datetime
from typing import Any

from ._cache import CachedFetcher
from ._client import Fetcher, HttpFetcher, RetryingFetcher
from ._exceptions import DataUnavailableError
from ._gharchive_streamer import GHArchiveStreamer
from ._models import generate_timestamps

logger = logging.getLogger(__name__)


def stream_events(
    start: datetime,
    end: datetime,
    *,
    use_cache: bool = False,
    cache_dir: str = ".gharchive_cache",
    fetcher: Fetcher | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    timeout: float | None = None,
) -> Generator[dict[str, Any], None, None]:
    own_fetcher = fetcher is None
    if not fetcher:
        fetcher = HttpFetcher(timeout=timeout)
    if max_retries > 0:
        fetcher = RetryingFetcher(
            fetcher, max_retries=max_retries, retry_delay=retry_delay
        )
    if use_cache:
        fetcher = CachedFetcher(fetcher, cache_dir=cache_dir)

    streamer = GHArchiveStreamer(fetcher)

    try:
        for ts in generate_timestamps(start, end):
            try:
                yield from streamer.stream_hour(ts)
            except DataUnavailableError:
                logger.warning("No data found, skipping: %s", ts.to_url())
    finally:
        if own_fetcher:
            fetcher.close()
