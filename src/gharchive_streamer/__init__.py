from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from ._cache import CachedFetcher
from ._client import Fetcher, HttpFetcher
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
    "stream_events",
]


def stream_events(
    start: datetime,
    end: datetime,
    *,
    use_cache: bool = False,
    cache_dir: str = ".gharchive_cache",
    fetcher: Fetcher | None = None,
) -> Iterator[dict[str, Any]]:
    if not fetcher:
        fetcher = HttpFetcher()
    if use_cache:
        fetcher = CachedFetcher(fetcher, cache_dir=cache_dir)

    streamer = GHArchiveStreamer(fetcher)

    for ts in generate_timestamps(start, end):
        try:
            yield from streamer.stream_hour(ts)
        except DataUnavailableError:
            logger.warning("No data found, skipping: %s", ts.to_url())
        except NetworkError as e:
            logger.error("Network error for %s: %s", ts.to_url(), e)
        except Exception as e:
            logger.error("Unexpected error for %s: %s", ts.to_url(), e)
