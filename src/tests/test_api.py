import gzip
import io
from datetime import datetime, timezone

import pytest

from gharchive_streamer._api import stream_events
from gharchive_streamer._client import Fetcher
from gharchive_streamer._exceptions import NetworkError
from gharchive_streamer._models import GHTimestamp

UTC = timezone.utc


def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data)
    return buf.getvalue()


def make_fetcher(data: dict[str, bytes]) -> Fetcher:
    class MapFetcher(Fetcher):
        def __init__(self, data):
            self.data = data
            self.calls: dict[str, int] = {}

        def fetch(self, url):
            self.calls[url] = self.calls.get(url, 0) + 1
            yield self.data[url]

    return MapFetcher(data)


class DropMidBodyFetcher(Fetcher):
    """Serves half the body on the first call, then the connection drops;
    full body on subsequent calls."""

    def __init__(self, url: str, data: bytes):
        self.url = url
        self.data = data
        self.calls = 0

    def fetch(self, url):
        self.calls += 1
        if url == self.url and self.calls == 1:
            yield self.data[: len(self.data) // 2]
            raise NetworkError("connection dropped mid-body")
        yield self.data


class AlwaysFailFetcher(Fetcher):
    def __init__(self):
        self.calls = 0

    def fetch(self, url):
        self.calls += 1
        raise NetworkError(url)


class TestStreamEventsRetry:
    def test_mid_body_drop_retries_whole_hour(self):
        url = GHTimestamp(2023, 1, 1, 0).to_url()
        data = gzip_bytes(b'{"id": 1}\n{"id": 2}\n')
        fetcher = DropMidBodyFetcher(url, data)

        result = list(
            stream_events(
                datetime(2023, 1, 1, 0, tzinfo=UTC),
                datetime(2023, 1, 1, 1, tzinfo=UTC),
                fetcher=fetcher,
                max_retries=2,
                retry_delay=0.0,
            )
        )

        assert result == [{"id": 1}, {"id": 2}]
        assert fetcher.calls == 2

    def test_network_error_raised_after_retries_exhausted(self):
        fetcher = AlwaysFailFetcher()

        with pytest.raises(NetworkError):
            list(
                stream_events(
                    datetime(2023, 1, 1, 0, tzinfo=UTC),
                    datetime(2023, 1, 1, 1, tzinfo=UTC),
                    fetcher=fetcher,
                    max_retries=2,
                    retry_delay=0.0,
                )
            )

        assert fetcher.calls == 3

    def test_no_retry_when_max_retries_zero(self):
        fetcher = AlwaysFailFetcher()

        with pytest.raises(NetworkError):
            list(
                stream_events(
                    datetime(2023, 1, 1, 0, tzinfo=UTC),
                    datetime(2023, 1, 1, 1, tzinfo=UTC),
                    fetcher=fetcher,
                    max_retries=0,
                )
            )

        assert fetcher.calls == 1

    def test_retry_does_not_cross_hour_boundaries(self):
        url0 = GHTimestamp(2023, 1, 1, 0).to_url()
        data = gzip_bytes(b'{"id": 1}\n')

        class FailFirstHourFetcher(Fetcher):
            def __init__(self):
                self.failures_left = 1

            def fetch(self, url):
                if url == url0 and self.failures_left > 0:
                    self.failures_left -= 1
                    raise NetworkError(url)
                yield data

        fetcher = FailFirstHourFetcher()
        result = list(
            stream_events(
                datetime(2023, 1, 1, 0, tzinfo=UTC),
                datetime(2023, 1, 1, 2, tzinfo=UTC),
                fetcher=fetcher,
                max_retries=1,
                retry_delay=0.0,
            )
        )

        assert result == [{"id": 1}, {"id": 1}]
