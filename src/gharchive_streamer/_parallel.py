from __future__ import annotations

import logging
import queue
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from ._api import stream_events
from ._models import GHTimestamp

logger = logging.getLogger(__name__)

_PUT_TIMEOUT = 0.2
_REPORT_TIMEOUT = 0.5


@dataclass(frozen=True)
class ChunkError:
    start: datetime
    end: datetime
    exception: BaseException


def parallel_stream_events(
    start: datetime,
    end: datetime,
    max_workers: int = 4,
    chunk_hours: int = 6,
    queue_maxsize: int = 500,
    on_chunk_error: Callable[[ChunkError], None] | None = None,
    **kwargs,
) -> Iterator[dict[str, Any]]:
    if queue_maxsize < 1:
        raise ValueError("queue_maxsize must be >= 1")
    chunks = _split_range(start, end, chunk_hours)
    event_queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=queue_maxsize)
    stop_event = threading.Event()

    executor = ThreadPoolExecutor(max_workers=max_workers)
    futures: list[Future] = [
        executor.submit(
            _stream_chunk_to_queue, c_start, c_end, event_queue, stop_event, **kwargs
        )
        for c_start, c_end in chunks
    ]

    completed = False
    try:
        finished = 0
        while finished < len(futures):
            item = event_queue.get()
            if item is None:
                finished += 1
            else:
                yield item
        completed = True
    finally:
        stop_event.set()
        _drain(event_queue)
        executor.shutdown(wait=False, cancel_futures=True)
        _report_failures(
            chunks,
            futures,
            on_chunk_error,
            raise_on_total=completed,
            wait_timeout=None if completed else _REPORT_TIMEOUT,
        )


def _report_failures(
    chunks: list[tuple[datetime, datetime]],
    futures: list[Future],
    on_chunk_error: Callable[[ChunkError], None] | None,
    *,
    raise_on_total: bool,
    wait_timeout: float | None,
) -> None:
    done, _ = wait(futures, timeout=wait_timeout)
    failures: list[BaseException] = []
    for chunk, future in zip(chunks, futures, strict=True):
        if future not in done or future.cancelled():
            continue
        exc = future.exception()
        if exc is None:
            continue
        failures.append(exc)
        logger.error("Chunk failed: %s", exc)
        if on_chunk_error is not None:
            on_chunk_error(ChunkError(chunk[0], chunk[1], exc))
    if raise_on_total and failures and len(failures) == len(futures):
        raise failures[0]


def _drain(q: queue.Queue[dict[str, Any] | None]) -> None:
    while True:
        try:
            q.get_nowait()
        except queue.Empty:
            return


def _put(
    q: queue.Queue[dict[str, Any] | None],
    item: dict[str, Any] | None,
    stop_event: threading.Event,
) -> bool:
    while not stop_event.is_set():
        try:
            q.put_nowait(item)
            return True
        except queue.Full:
            try:
                q.put(item, timeout=_PUT_TIMEOUT)
                return True
            except queue.Full:
                continue
    return False


def _stream_chunk_to_queue(
    start: datetime,
    end: datetime,
    q: queue.Queue[dict[str, Any] | None],
    stop_event: threading.Event,
    **kwargs,
) -> None:
    try:
        events = stream_events(start, end, **kwargs)
        for event in events:
            if stop_event.is_set() or not _put(q, event, stop_event):
                events.close()
                return
    except Exception as e:
        logger.error("Chunk %s - %s failed: %s", start, end, e)
        raise
    finally:
        _put(q, None, stop_event)


def _split_range(
    start: datetime, end: datetime, chunk_hours: int
) -> list[tuple[datetime, datetime]]:
    if chunk_hours < 1:
        raise ValueError("chunk_hours must be >= 1")
    first = GHTimestamp.from_datetime(start)
    last = GHTimestamp.from_datetime(end - timedelta(microseconds=1))
    if first > last:
        return []
    current = datetime(
        first.year, first.month, first.day, first.hour, tzinfo=timezone.utc
    )
    last_dt = datetime(last.year, last.month, last.day, last.hour, tzinfo=timezone.utc)
    chunks = []
    while current <= last_dt:
        chunk_end = min(
            current + timedelta(hours=chunk_hours), last_dt + timedelta(hours=1)
        )
        chunks.append((current, chunk_end))
        current = chunk_end
    return chunks
