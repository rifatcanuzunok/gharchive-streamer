# gharchive-streamer

Stream Github Archive data with minimal memory footprint.

## Installation

```bash
pip install gharchive-stream
```

## Usage

```python
from datetime import datetime
from gharchive_streamer import stream_events

for event in stream_events(datetime(2026, 1, 1, 0), datetime(2026, 1, 1, 2)):
    print(event["type"])
```

## License

MIT
