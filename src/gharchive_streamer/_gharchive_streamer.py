import gzip
import json
import logging
from collections.abc import Iterator
from typing import Any

from gharchive_streamer._client import Fetcher
from gharchive_streamer._iter_stream import IterStream
from gharchive_streamer._models import GHTimestamp

logger = logging.getLogger(__name__)


class GHArchiveStreamer:
    def __init__(self, fetcher: Fetcher) -> None:
        self._fetcher = fetcher

    def stream_hour(self, timestamp: GHTimestamp) -> Iterator[dict[str, Any]]:
        url = timestamp.to_url()
        logger.info(f"Fetching {url}")
        byte_iterator = self._fetcher.fetch(url=url)
        stream = IterStream(byte_iterator)
        with gzip.GzipFile(fileobj=stream, mode="rb") as gz:
            for line in gz:
                if line.strip():
                    yield json.loads(line)
