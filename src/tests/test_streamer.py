import gzip
import io
import json

import pytest

from gharchive_streamer._client import Fetcher
from gharchive_streamer._exceptions import DecompressionError
from gharchive_streamer._gharchive_streamer import GHArchiveStreamer
from gharchive_streamer._iter_stream import IterStream
from gharchive_streamer._models import GHTimestamp


def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data)
    return buf.getvalue()


class MockFetcher(Fetcher):
    def __init__(self, chunks):
        self.chunks = chunks

    def fetch(self, url: str):
        yield from self.chunks


class TestIterStream:
    def test_basic_read(self):
        it = iter([b"hello", b" world"])
        stream = IterStream(it)
        assert stream.read() == b"hello world"

    def test_chunked_read(self):
        it = iter([b"abcdefgh"])
        stream = IterStream(it)
        assert stream.read(3) == b"abc"
        assert stream.read(2) == b"de"
        assert stream.read() == b"fgh"


class TestGHArchiveStreamer:
    def test_stream_hour_basic(self):
        events = [
            {"type": "PushEvent", "id": 1},
            {"type": "IssuesEvent", "id": 2},
        ]
        lines = "\n".join(json.dumps(e) for e in events).encode("utf-8") + b"\n"
        compressed = gzip_bytes(lines)

        mock_fetcher = MockFetcher([compressed])
        streamer = GHArchiveStreamer(mock_fetcher)
        ts = GHTimestamp(2023, 1, 1, 0)

        result = list(streamer.stream_hour(ts))
        assert result == events

    def test_stream_hour_empty(self):
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            pass
        mock_fetcher = MockFetcher([buf.getvalue()])
        streamer = GHArchiveStreamer(mock_fetcher)
        ts = GHTimestamp(2023, 1, 1, 0)

        result = list(streamer.stream_hour(ts))
        assert result == []


class TestDecompressionError:
    def test_non_gzip_data_raises_decompression_error(self):
        mock_fetcher = MockFetcher([b"this is not gzip data"])
        streamer = GHArchiveStreamer(mock_fetcher)
        ts = GHTimestamp(2023, 1, 1, 0)

        with pytest.raises(DecompressionError):
            list(streamer.stream_hour(ts))

    def test_truncated_gzip_raises_decompression_error(self):
        full = gzip_bytes(b'{"id": 1}\n')
        mock_fetcher = MockFetcher([full[:16]])
        streamer = GHArchiveStreamer(mock_fetcher)
        ts = GHTimestamp(2023, 1, 1, 0)

        with pytest.raises(DecompressionError):
            list(streamer.stream_hour(ts))
