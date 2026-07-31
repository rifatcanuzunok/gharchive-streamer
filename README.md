# gharchive-streamer

Stream Github Archive data with minimal memory footprint.

## Installation

```bash
pip install gharchive-stream
```

## Usage

```python
for event in streaming_parallel_stream_events(
    datetime(2026, 1, 1),
    datetime(2026, 1, 2),
    max_workers=4,
    chunk_hours=6,
):
    print(event["type"])
```

## License

MIT
