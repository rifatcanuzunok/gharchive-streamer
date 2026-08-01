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

    def test_empty_chunks_skipped(self):
        it = iter([b"a", b"", b"bc"])
        stream = IterStream(it)
        assert stream.read() == b"abc"

    def test_empty_chunks_skipped_sized_read(self):
        it = iter([b"a", b"", b"bc"])
        stream = IterStream(it)
        assert stream.read(2) == b"ab"
        assert stream.read() == b"c"

    def test_empty_chunks_before_eof(self):
        it = iter([b"", b"", b"data"])
        stream = IterStream(it)
        assert stream.read(4) == b"data"
        assert stream.read() == b""


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
        with gzip.GzipFile(fileobj=buf, mode="wb"):
            pass
        mock_fetcher = MockFetcher([buf.getvalue()])
        streamer = GHArchiveStreamer(mock_fetcher)
        ts = GHTimestamp(2023, 1, 1, 0)

        result = list(streamer.stream_hour(ts))
        assert result == []


class TestMalformedJsonLines:
    def test_corrupt_lines_are_skipped(self):
        events = [{"type": "PushEvent", "id": 1}, {"type": "IssuesEvent", "id": 2}]
        lines = (
            json.dumps(events[0]) + "\n"
            + "NOT-JSON-LINE\n"
            + json.dumps(events[1]) + "\n"
        ).encode("utf-8")

        mock_fetcher = MockFetcher([gzip_bytes(lines)])
        streamer = GHArchiveStreamer(mock_fetcher)
        ts = GHTimestamp(2023, 1, 1, 0)

        assert list(streamer.stream_hour(ts)) == events

    def test_all_corrupt_lines_yield_nothing(self):
        lines = b"NOT-JSON-LINE\n{also not json\n"

        mock_fetcher = MockFetcher([gzip_bytes(lines)])
        streamer = GHArchiveStreamer(mock_fetcher)
        ts = GHTimestamp(2023, 1, 1, 0)

        assert list(streamer.stream_hour(ts)) == []


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
