from __future__ import annotations

import logging
import random
import time
from collections.abc import Generator
from datetime import datetime
from typing import Any

from ._cache import CachedFetcher
from ._client import Fetcher, HttpFetcher
from ._exceptions import DataUnavailableError, NetworkError
from ._gharchive_streamer import GHArchiveStreamer
from ._models import generate_timestamps

logger = logging.getLogger(__name__)

_BACKOFF_FACTOR = 2.0


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
    if use_cache:
        fetcher = CachedFetcher(fetcher, cache_dir=cache_dir)

    streamer = GHArchiveStreamer(fetcher)

    try:
        for ts in generate_timestamps(start, end):
            attempts = 0
            while True:
                try:
                    yield from streamer.stream_hour(ts)
                    break
                except DataUnavailableError:
                    logger.warning("No data found, skipping: %s", ts.to_url())
                    break
                except NetworkError as e:
                    attempts += 1
                    if attempts > max_retries:
                        raise
                    delay = retry_delay * (_BACKOFF_FACTOR ** (attempts - 1))
                    delay *= random.uniform(0.5, 1.5)  # noqa: S311
                    logger.warning(
                        "Network error for %s (retry %d/%d), retrying in %.2fs: %s",
                        ts.to_url(),
                        attempts,
                        max_retries,
                        delay,
                        e,
                    )
                    time.sleep(delay)
    finally:
        if own_fetcher:
            fetcher.close()
