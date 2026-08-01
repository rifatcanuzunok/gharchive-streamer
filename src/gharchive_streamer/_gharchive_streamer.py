import gzip
import json
import logging
from collections.abc import Iterator
from typing import Any

from gharchive_streamer._client import Fetcher
from gharchive_streamer._exceptions import DecompressionError
from gharchive_streamer._iter_stream import IterStream
from gharchive_streamer._models import GHTimestamp

logger = logging.getLogger(__name__)


class GHArchiveStreamer:
    def __init__(self, fetcher: Fetcher) -> None:
        self._fetcher = fetcher

    def stream_hour(self, timestamp: GHTimestamp) -> Iterator[dict[str, Any]]:
        url = timestamp.to_url()
        logger.info("Fetching %s", url)
        byte_iterator = self._fetcher.fetch(url=url)
        stream = IterStream(byte_iterator)
        try:
            with gzip.GzipFile(fileobj=stream, mode="rb") as gz:
                for line in gz:
                    if not line.strip():
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning("Skipping malformed JSON line in %s: %s", url, e)
        except (gzip.BadGzipFile, EOFError, OSError) as e:
            raise DecompressionError(f"Failed to decompress {url}: {e}") from e
