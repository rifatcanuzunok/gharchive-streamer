from abc import ABC, abstractmethod
from collections.abc import Iterator

import httpx


class Fetcher(ABC):
    @abstractmethod
    def fetch(self, url: str) -> Iterator[bytes]: ...


class HttpFetcher(Fetcher):
    def __init__(self, client: httpx.Client | None = None):
        self._client = client or httpx.Client()

    def fetch(self, url: str) -> Iterator[bytes]:
        with self._client.stream("GET", url) as response:
            response.raise_for_status()
            yield from response.iter_bytes(chunk_size=8192)
