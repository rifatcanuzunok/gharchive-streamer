import gzip
import io
import json
from datetime import datetime, timezone

import pytest

from gharchive_streamer._client import Fetcher
from gharchive_streamer._exceptions import DataUnavailableError
from gharchive_streamer._models import GHTimestamp
from gharchive_streamer._parallel import (
    _split_range,
    _stream_chunk,
    parallel_stream_events,
    streaming_parallel_stream_events,
)

UTC = timezone.utc


def dt(hour: int, day: int = 1) -> datetime:
    return datetime(2023, 1, day, hour, tzinfo=UTC)


def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(data)
    return buf.getvalue()


def make_events(hour: int, count: int = 3) -> list[dict]:
    return [{"id": hour * 100 + i, "type": "PushEvent"} for i in range(count)]


def events_bytes(events: list[dict]) -> bytes:
    lines = "\n".join(json.dumps(e) for e in events).encode("utf-8") + b"\n"
    return gzip_bytes(lines)


class MapFetcher(Fetcher):
    def __init__(self, data: dict[str, bytes]):
        self.data = data

    def fetch(self, url: str):
        if url not in self.data:
            raise DataUnavailableError(url)
        yield self.data[url]


def make_fetcher(hours: list[int], events_per_hour: int = 3) -> MapFetcher:
    data = {
        GHTimestamp(2023, 1, 1, h).to_url(): events_bytes(make_events(h, events_per_hour))
        for h in hours
    }
    return MapFetcher(data)


def expected_events(hours: list[int], events_per_hour: int = 3) -> list[dict]:
    return [e for h in hours for e in make_events(h, events_per_hour)]


def sorted_by_id(result: list[dict]) -> list[dict]:
    return sorted(result, key=lambda e: e["id"])


class TestSplitRange:
    def test_chunks_cover_range_without_overlap_or_gap(self):
        assert _split_range(dt(0), dt(4), chunk_hours=2) == [
            (dt(0), dt(1)),
            (dt(2), dt(3)),
            (dt(4), dt(4)),
        ]

    def test_single_chunk_shorter_than_chunk_hours(self):
        assert _split_range(dt(0), dt(1), chunk_hours=24) == [(dt(0), dt(1))]

    def test_hourly_chunks(self):
        assert _split_range(dt(0), dt(2), chunk_hours=1) == [
            (dt(0), dt(0)),
            (dt(1), dt(1)),
            (dt(2), dt(2)),
        ]

    def test_single_hour_range(self):
        assert _split_range(dt(1), dt(1), chunk_hours=1) == [(dt(1), dt(1))]

    def test_no_chunks_when_start_after_end(self):
        assert _split_range(dt(2), dt(1), chunk_hours=1) == []

    def test_invalid_chunk_hours(self):
        with pytest.raises(ValueError):
            _split_range(dt(0), dt(4), chunk_hours=0)


class TestStreamChunk:
    def test_returns_events_for_range(self):
        fetcher = make_fetcher([0, 1])
        events = _stream_chunk(dt(0), dt(1), fetcher=fetcher)
        assert events == expected_events([0, 1])

    def test_includes_end_hour(self):
        fetcher = make_fetcher([0, 1, 2])
        events = _stream_chunk(dt(0), dt(2), fetcher=fetcher)
        assert events == expected_events([0, 1, 2])

    def test_single_hour(self):
        fetcher = make_fetcher([1])
        assert _stream_chunk(dt(1), dt(1), fetcher=fetcher) == expected_events([1])

    def test_missing_hour_is_skipped(self):
        fetcher = make_fetcher([1])
        assert _stream_chunk(dt(0), dt(1), fetcher=fetcher) == expected_events([1])


class TestParallelStreamEvents:
    def test_yields_each_event_once(self):
        fetcher = make_fetcher([0, 1, 2])
        result = list(
            parallel_stream_events(dt(0), dt(2), chunk_hours=1, fetcher=fetcher)
        )
        assert sorted_by_id(result) == expected_events([0, 1, 2])

    def test_missing_hour_is_skipped(self):
        fetcher = make_fetcher([1])
        result = list(
            parallel_stream_events(dt(0), dt(1), chunk_hours=1, fetcher=fetcher)
        )
        assert result == expected_events([1])

    def test_single_hour_range(self):
        result = list(
            parallel_stream_events(dt(1), dt(1), fetcher=make_fetcher([1]))
        )
        assert result == expected_events([1])

    def test_empty_range(self):
        result = list(
            parallel_stream_events(dt(1), dt(0), fetcher=make_fetcher([1]))
        )
        assert result == []


class TestStreamingParallelStreamEvents:
    def test_yields_each_event_once(self):
        fetcher = make_fetcher([0, 1, 2, 3])
        result = list(
            streaming_parallel_stream_events(
                dt(0), dt(3), chunk_hours=1, queue_maxsize=2, fetcher=fetcher
            )
        )
        assert sorted_by_id(result) == expected_events([0, 1, 2, 3])

    def test_missing_hour_is_skipped(self):
        fetcher = make_fetcher([2])
        result = list(
            streaming_parallel_stream_events(dt(0), dt(2), chunk_hours=1, fetcher=fetcher)
        )
        assert result == expected_events([2])

    def test_single_hour_range(self):
        result = list(
            streaming_parallel_stream_events(dt(1), dt(1), fetcher=make_fetcher([1]))
        )
        assert result == expected_events([1])

    def test_backpressure_with_small_queue(self):
        fetcher = make_fetcher([0, 1, 2], events_per_hour=50)
        gen = streaming_parallel_stream_events(
            dt(0), dt(2), chunk_hours=1, queue_maxsize=1, fetcher=fetcher
        )
        first = next(gen)
        assert first in make_events(first["id"] // 100)
        remaining = list(gen)
        assert len(remaining) == 149
