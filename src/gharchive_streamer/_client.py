from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator

import httpx

from ._exceptions import DataUnavailableError, NetworkError

logger = logging.getLogger(__name__)


class Fetcher(ABC):
    @abstractmethod
    def fetch(self, url: str) -> Iterator[bytes]: ...

    def close(self) -> None:
        pass


class HttpFetcher(Fetcher):
    def __init__(self, client: httpx.Client | None = None):
        self._owns_client = client is None
        if client is None:
            client = httpx.Client()
        self._client = client

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> HttpFetcher:  # noqa: PYI034 - Self requires Python 3.11
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:  # noqa: BLE001, S110 - __del__ must never raise or log
            pass

    def fetch(self, url: str) -> Iterator[bytes]:
        try:
            with self._client.stream("GET", url) as response:
                response.raise_for_status()
                yield from response.iter_bytes(chunk_size=8192)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise DataUnavailableError(f"HTTP 404: {url}") from e
            raise NetworkError(f"HTTP {e.response.status_code}: {url}") from e
        except httpx.RequestError as e:
            raise NetworkError(f"Request failed: {url}: {e}") from e


class RetryingFetcher(Fetcher):
    def __init__(
        self,
        base_fetcher: Fetcher,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        backoff_factor: float = 2.0,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self._fetcher = base_fetcher
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._backoff_factor = backoff_factor

    def close(self) -> None:
        self._fetcher.close()

    def fetch(self, url: str) -> Iterator[bytes]:
        attempts = 0
        while True:
            try:
                attempts += 1
                yield from self._fetcher.fetch(url)
                return
            except DataUnavailableError:
                raise
            except NetworkError as e:
                if attempts > self._max_retries:
                    raise
                delay = self._retry_delay * (self._backoff_factor ** (attempts - 1))
                delay *= random.uniform(0.5, 1.5)
                logger.warning(
                    "Network error for %s (retry %d/%d), retrying in %.2fs: %s",
                    url,
                    attempts,
                    self._max_retries,
                    delay,
                    e,
                )
                time.sleep(delay)
