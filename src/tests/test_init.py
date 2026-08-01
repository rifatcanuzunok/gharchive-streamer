import gzip
import io
import json
from datetime import datetime, timezone

from gharchive_streamer import stream_events
from gharchive_streamer._client import Fetcher
from gharchive_streamer._exceptions import DataUnavailableError, NetworkError
from gharchive_streamer._models import GHTimestamp

UTC = timezone.utc


def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data)
    return buf.getvalue()


class MapFetcher(Fetcher):
    def __init__(self, data: dict[str, bytes]):
        self.data = data

    def fetch(self, url: str):
        yield self.data[url]


class SkipMissingFetcher(MapFetcher):
    def fetch(self, url: str):
        if url not in self.data:
            raise DataUnavailableError(url)
        yield from super().fetch(url)


def hour_url(hour: int) -> str:
    return GHTimestamp(2023, 1, 1, hour).to_url()


class TestStreamEvents:
    def test_yields_events_across_hours(self):
        events = [{"id": 1, "type": "PushEvent"}, {"id": 2, "type": "IssuesEvent"}]
        lines = "\n".join(json.dumps(e) for e in events).encode("utf-8") + b"\n"
        fetcher = MapFetcher({hour_url(0): gzip_bytes(lines), hour_url(1): gzip_bytes(lines)})

        result = list(
            stream_events(
                datetime(2023, 1, 1, 0, tzinfo=UTC),
                datetime(2023, 1, 1, 1, tzinfo=UTC),
                fetcher=fetcher,
            )
        )

        assert result == events * 2

    def test_missing_hour_is_skipped(self):
        events = [{"id": 1}]
        lines = "\n".join(json.dumps(e) for e in events).encode("utf-8") + b"\n"
        fetcher = SkipMissingFetcher({hour_url(1): gzip_bytes(lines)})

        result = list(
            stream_events(
                datetime(2023, 1, 1, 0, tzinfo=UTC),
                datetime(2023, 1, 1, 1, tzinfo=UTC),
                fetcher=fetcher,
            )
        )

        assert result == events

    def test_network_error_is_skipped(self):
        events = [{"id": 1}]
        lines = "\n".join(json.dumps(e) for e in events).encode("utf-8") + b"\n"

        class FlakyFetcher(MapFetcher):
            def fetch(self, url):
                if url == hour_url(0):
                    raise NetworkError(url)
                yield self.data[url]

        fetcher = FlakyFetcher({hour_url(1): gzip_bytes(lines)})

        result = list(
            stream_events(
                datetime(2023, 1, 1, 0, tzinfo=UTC),
                datetime(2023, 1, 1, 1, tzinfo=UTC),
                fetcher=fetcher,
            )
        )

        assert result == events
