from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

from ._client import Fetcher

logger = logging.getLogger(__name__)


class CachedFetcher(Fetcher):
    def __init__(
        self, base_fetcher: Fetcher, cache_dir: str = ".gharchive_cache"
    ) -> None:
        self._fetcher = base_fetcher
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, url: str) -> Iterator[bytes]:
        filename = self._cache_dir / url.split("/")[-1]

        if filename.exists():
            logger.debug(f"Cache HIT: {url}")

            with open(filename, "rb") as f:
                while chunk := f.read(8192):
                    yield chunk

        else:
            logger.debug(f"Cache MISS: {url}")
            chunks = list(self._fetcher.fetch(url))
            with open(filename, "wb") as f:
                for chunk in chunks:
                    f.write(chunk)
            yield from chunks
