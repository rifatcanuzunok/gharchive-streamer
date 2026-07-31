from __future__ import annotations

import logging
import queue
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any

from . import stream_events

logger = logging.getLogger(__name__)


def parallel_stream_events(
    start: datetime,
    end: datetime,
    max_workers: int = 4,
    chunk_hours: int = 24,
    **kwargs,
) -> Iterator[dict[str, Any]]:
    chunks = _split_range(start, end, chunk_hours)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_stream_chunk, c_start, c_end, **kwargs): (c_start, c_end)
            for c_start, c_end in chunks
        }
        for future in as_completed(futures):
            try:
                yield from future.result()
            except Exception as e:
                c_start, c_end = futures[future]
                logger.error("Chunk %s - %s failed: %s", c_start, c_end, e)


def streaming_parallel_stream_events(
    start: datetime,
    end: datetime,
    max_workers: int = 4,
    chunk_hours: int = 6,
    queue_maxsize: int = 500,
    **kwargs,
) -> Iterator[Dict[str, Any]]:
    chunks = _split_range(start, end, chunk_hours)
    event_queue = queue.Queue(maxsize=queue_maxsize)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: List[Future] = []
        for c_start, c_end in chunks:
            future = executor.submit(
                _stream_chunk_to_queue, c_start, c_end, event_queue, **kwargs
            )
            futures.append(future)

        finished = 0
        while finished < len(futures):
            try:
                item = event_queue.get(timeout=0.2)
                if item is None:
                    finished += 1
                else:
                    yield item
            except queue.Empty:
                continue

        for future in futures:
            if future.exception():
                logger.error("Chunk failed: %s", future.exception())


def _stream_chunk_to_queue(start, end, q, **kwargs):
    try:
        for event in stream_events(start, end, **kwargs):
            q.put(event)
    except Exception as e:
        logger.error("Chunk %s - %s failed: %s", start, end, e)
    finally:
        q.put(None)


def _split_range(start, end, chunk_hours):
    chunks = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(hours=chunk_hours), end)
        chunks.append((current, chunk_end))
        current = chunk_end
    return chunks
