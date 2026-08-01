import pytest

from gharchive_streamer._cache import CachedFetcher
from gharchive_streamer._client import Fetcher


class CountingFetcher(Fetcher):
    def __init__(self, data=b""):
        self.data = data
        self.call_count = 0

    def fetch(self, url: str):
        self.call_count += 1
        yield self.data


    def test_close_delegates_to_base_fetcher(self, tmp_path):
        class ClosableCountingFetcher(CountingFetcher):
            def __init__(self):
                super().__init__()
                self.closed = False

            def close(self):
                self.closed = True

        base = ClosableCountingFetcher()
        cached = CachedFetcher(base, str(tmp_path / "cache"))

        cached.close()

        assert base.closed is True


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

    def test_chunks_yielded_while_writing(self, tmp_path):
        cache_dir = tmp_path / "cache"
        url = "https://data.gharchive.org/2015-01-01-15.json.gz"

        class ChunkedFetcher(Fetcher):
            def fetch(self, url):
                yield b"a" * 8192
                yield b"b" * 8192
                yield b"c" * 8192

        cached = CachedFetcher(ChunkedFetcher(), str(cache_dir))
        chunks = list(cached.fetch(url))

        assert b"".join(chunks) == b"a" * 8192 + b"b" * 8192 + b"c" * 8192
        assert len(chunks) == 3

    def test_failed_fetch_leaves_no_cache_file(self, tmp_path):
        cache_dir = tmp_path / "cache"
        url = "https://data.gharchive.org/2015-01-01-15.json.gz"

        class FailingFetcher(Fetcher):
            def fetch(self, url):
                yield b"partial-data"
                raise RuntimeError("connection dropped")

        cached = CachedFetcher(FailingFetcher(), str(cache_dir))

        with pytest.raises(RuntimeError):
            list(cached.fetch(url))

        assert list(cache_dir.iterdir()) == []

    def test_failed_fetch_does_not_poison_cache(self, tmp_path):
        cache_dir = tmp_path / "cache"
        url = "https://data.gharchive.org/2015-01-01-15.json.gz"

        class FlakyFetcher(Fetcher):
            def __init__(self):
                self.calls = 0

            def fetch(self, url):
                self.calls += 1
                if self.calls == 1:
                    yield b"x"
                    raise RuntimeError("boom")
                yield b"complete-data"

        base = FlakyFetcher()
        cached = CachedFetcher(base, str(cache_dir))

        with pytest.raises(RuntimeError):
            list(cached.fetch(url))

        assert b"".join(cached.fetch(url)) == b"complete-data"
        assert base.calls == 2

    def test_abandoned_stream_does_not_cache_partial(self, tmp_path):
        cache_dir = tmp_path / "cache"
        url = "https://data.gharchive.org/2015-01-01-15.json.gz"

        class BigFetcher(Fetcher):
            def fetch(self, url):
                for _ in range(1000):
                    yield b"0123456789"

        cached = CachedFetcher(BigFetcher(), str(cache_dir))

        gen = cached.fetch(url)
        first = next(gen)
        assert first == b"0123456789"
        gen.close()

        assert list(cache_dir.iterdir()) == []

        result = b"".join(cached.fetch(url))
        assert result == b"0123456789" * 1000
        assert list(cache_dir.iterdir()) == [cache_dir / "2015-01-01-15.json.gz"]
