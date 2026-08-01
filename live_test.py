from __future__ import annotations

import argparse
import time
from collections import Counter
from datetime import datetime

from gharchive_streamer import parallel_stream_events


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def run(
    start: str,
    end: str,
    max_workers: int,
    chunk_hours: int,
    max_retries: int,
    queue_maxsize: int,
    use_cache: bool,
    cache_dir: str,
    timeout: float | None,
) -> None:
    start_dt = parse_iso(start)
    end_dt = parse_iso(end)

    print(
        f"Streaming {start_dt.isoformat()} -> {end_dt.isoformat()} "
        f"(workers={max_workers}, chunk_hours={chunk_hours}, "
        f"queue_maxsize={queue_maxsize}, max_retries={max_retries}, "
        f"cache={use_cache}, timeout={timeout})"
    )

    total = 0
    types: Counter[str] = Counter()
    samples: list[dict] = []
    started = time.monotonic()

    for event in parallel_stream_events(
        start_dt,
        end_dt,
        max_workers=max_workers,
        chunk_hours=chunk_hours,
        queue_maxsize=queue_maxsize,
        max_retries=max_retries,
        use_cache=use_cache,
        cache_dir=cache_dir,
        timeout=timeout,
    ):
        total += 1
        types[event["type"]] += 1
        if len(samples) < 3:
            samples.append(event)

    elapsed = time.monotonic() - started

    print(
        f"\nTotal events: {total:,} in {elapsed:.1f}s ({total / elapsed:,.0f} events/s)"
    )
    print("\nSample events:")
    for event in samples:
        repo = event.get("repo", {}).get("name", "?")
        actor = event.get("actor", {}).get("login", "?")
        print(f"  {event.get('type', '?')}: {actor} -> {repo}")
    print("\nTop event types:")
    for event_type, count in types.most_common(10):
        print(f"  {event_type}: {count:,}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stream real GitHub Archive events with parallel ingestion."
    )
    parser.add_argument("--start", default="2026-01-01T00:00:00Z")
    parser.add_argument("--end", default="2026-01-01T01:00:00Z")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--chunk-hours", type=int, default=1)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--queue-maxsize", type=int, default=500)
    parser.add_argument("--use-cache", action="store_true")
    parser.add_argument("--cache-dir", default=".gharchive_cache")
    parser.add_argument("--timeout", type=float, default=None)
    args = parser.parse_args()

    run(
        args.start,
        args.end,
        args.max_workers,
        args.chunk_hours,
        args.max_retries,
        args.queue_maxsize,
        args.use_cache,
        args.cache_dir,
        args.timeout,
    )


if __name__ == "__main__":
    main()
