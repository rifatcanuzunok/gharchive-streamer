from abc import ABC, abstractmethod
from collections.abc import Iterator

import httpx

from ._exceptions import DataUnavailableError, NetworkError


class Fetcher(ABC):
    @abstractmethod
    def fetch(self, url: str) -> Iterator[bytes]: ...


class HttpFetcher(Fetcher):
    def __init__(self, client: httpx.Client | None = None):
        self._client = client or httpx.Client()

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
