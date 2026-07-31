from gharchive_streamer._cache import CachedFetcher
from gharchive_streamer._client import Fetcher


class CountingFetcher(Fetcher):
    def __init__(self, data=b""):
        self.data = data
        self.call_count = 0

    def fetch(self, url: str):
        self.call_count += 1
        yield self.data


class TestCachedFetcher:
    def test_cache_miss_then_hit(self, tmp_path):
        cache_dir = tmp_path / "cache"
        url = "https://data.gharchive.org/2015-01-01-15.json.gz"
        original_data = b"compressed-xyz"

        base = CountingFetcher(original_data)
        cached = CachedFetcher(base, cache_dir=str(cache_dir))

        result1 = b"".join(cached.fetch(url))
        assert result1 == original_data
        assert base.call_count == 1
        assert (cache_dir / "2015-01-01-15.json.gz").exists()

        result2 = b"".join(cached.fetch(url))
        assert result2 == original_data
        assert base.call_count == 1

    def test_chunked_cache_read(self, tmp_path):
        cache_dir = tmp_path / "cache"
        url = "https://example.com/data.gz"
        file_path = cache_dir / "data.gz"
        cache_dir.mkdir()
        file_path.write_bytes(b"chunk1chunk2")

        class DummyFetcher(Fetcher):
            def fetch(self, url):
                raise RuntimeError("should not be called")

        cached = CachedFetcher(DummyFetcher(), str(cache_dir))
        chunks = list(cached.fetch(url))
        assert b"".join(chunks) == b"chunk1chunk2"
        assert len(chunks) == 1
