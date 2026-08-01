from __future__ import annotations

import hashlib
import logging
import os
import tempfile
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

    def close(self) -> None:
        self._fetcher.close()

    def _cache_path(self, url: str) -> Path:
        name = url.rsplit("/", 1)[-1]
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
        return self._cache_dir / f"{digest}-{name}"

    def fetch(self, url: str) -> Iterator[bytes]:
        filename = self._cache_path(url)

        if filename.exists():
            logger.debug("Cache HIT: %s", url)
            with open(filename, "rb") as f:
                while chunk := f.read(8192):
                    yield chunk
            return

        logger.debug("Cache MISS: %s", url)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f"{filename.name}.", suffix=".part", dir=self._cache_dir
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as f:
                for chunk in self._fetcher.fetch(url):
                    f.write(chunk)
                    yield chunk
            os.replace(tmp_path, filename)
        finally:
            tmp_path.unlink(missing_ok=True)
