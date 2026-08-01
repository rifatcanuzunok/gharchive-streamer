# gharchive-streamer

Stream [GitHub Archive](https://www.gharchive.org/) data with a minimal, constant memory footprint. Gzipped hourly files are decompressed on the fly and yielded one event at a time — no temporary files, no full-file buffering.

## Installation

```bash
pip install gharchive-streamer
```

## Usage

### Sequential streaming

Processes hours one at a time. Ideal for cache hits and CPU-bound local processing.

`end` is exclusive: `stream_events(Jan 1 00:00, Jan 2 00:00)` covers exactly the 24 hours of January 1st.

```python
from datetime import datetime, timezone

from gharchive_streamer import stream_events

for event in stream_events(
    datetime(2023, 1, 1, tzinfo=timezone.utc),
    datetime(2023, 1, 2, tzinfo=timezone.utc),
):
    print(event["type"], event["repo"]["name"])
```

### Parallel ingestion (for Kafka feeding / historical load)

Downloads multiple chunks concurrently using a thread pool with bounded backpressure. Use this when network I/O dominates (the real ingestion case) — it is **not** faster than sequential for cached, CPU-bound reruns.

Events are yielded as chunks complete: no chronological ordering is guaranteed.

```python
from datetime import datetime, timezone

from gharchive_streamer import parallel_stream_events

for event in parallel_stream_events(
    datetime(2023, 1, 1, tzinfo=timezone.utc),
    datetime(2023, 1, 2, tzinfo=timezone.utc),
    max_workers=4,
    chunk_hours=6,
):
    producer.send("github-events", event)
```

Partially failed chunks are logged and the stream continues. If **every** chunk fails, the first error is re-raised. Failed chunks are reported via `on_chunk_error` even when the stream is closed early (best-effort). Hook per-chunk failures for monitoring:

```python
from gharchive_streamer import ChunkError, parallel_stream_events

def alert(err: ChunkError) -> None:
    logger.error("Chunk %s -> %s lost: %s", err.start, err.end, err.exception)

for event in parallel_stream_events(start, end, on_chunk_error=alert):
    ...
```

## Caching (development only)

`use_cache=True` stores downloaded files on disk so repeat runs hit the local cache instead of the network. Designed for dev cycles — not for production ingestion.

```python
for event in stream_events(start, end, use_cache=True, cache_dir=".gharchive_cache"):
    ...
```

## Resilience & errors

- Missing hours (`404`) are logged and skipped.
- `DataUnavailableError` — hour not found.
- `NetworkError` — network/HTTP failures, retried per-hour with jittered exponential backoff (`max_retries`, `retry_delay`). A mid-body connection drop restarts the whole hour; events already delivered from the failed attempt may be replayed (at-least-once semantics).
- `DecompressionError` — corrupt or truncated gzip.
- Malformed JSON lines are warned about and skipped.

Tune downloads with `max_retries`, `retry_delay`, and `timeout` (seconds). Timestamps are normalized to UTC — naive datetimes are treated as UTC.

## Development

```bash
uv sync --extra dev   # install with dev extras (pytest, mypy, ruff)
uv run pytest         # run tests
uv run ruff check .   # lint
uv run mypy src       # type check
```

CI (`.github/workflows/ci.yml`) runs these checks on every push and PR.

## License

MIT
