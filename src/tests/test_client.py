import httpx
import pytest

from gharchive_streamer._client import HttpFetcher, RetryingFetcher
from gharchive_streamer._exceptions import DataUnavailableError, NetworkError


class FlakyFetcher:
    def __init__(self, failures: int, data: bytes = b"ok"):
        self.failures = failures
        self.data = data
        self.call_count = 0

    def fetch(self, url: str):
        self.call_count += 1
        if self.call_count <= self.failures:
            raise NetworkError(f"boom {self.call_count}")
        yield self.data


class TestHttpFetcher:
    def test_fetch_yields_chunks(self, httpx_mock):
        url = "https://data.gharchive.org/2023-01-01-0.json.gz"
        chunk1 = b'{"id":1}\n'
        chunk2 = b'{"id":2}\n'

        httpx_mock.add_response(url=url, content=chunk1 + chunk2)

        fetcher = HttpFetcher()
        chunks = list(fetcher.fetch(url))

        assert b"".join(chunks) == chunk1 + chunk2
        assert len(chunks) > 0

    def test_fetch_404_raises_data_unavailable(self, httpx_mock):
        url = "https://data.gharchive.org/missing.json.gz"
        httpx_mock.add_response(url=url, status_code=404)

        fetcher = HttpFetcher()
        with pytest.raises(DataUnavailableError):
            list(fetcher.fetch(url))

    def test_fetch_500_raises_network_error(self, httpx_mock):
        url = "https://data.gharchive.org/error.json.gz"
        httpx_mock.add_response(url=url, status_code=500)

        fetcher = HttpFetcher()
        with pytest.raises(NetworkError):
            list(fetcher.fetch(url))

    def test_fetch_connection_error_raises_network_error(self, httpx_mock):
        url = "https://data.gharchive.org/down.json.gz"
        httpx_mock.add_exception(
            httpx.ConnectError("connection refused", request=httpx.Request("GET", url))
        )

        fetcher = HttpFetcher()
        with pytest.raises(NetworkError):
            list(fetcher.fetch(url))

    def test_fetch_passes_client_injection(self, httpx_mock):
        url = "https://example.com/data.gz"
        httpx_mock.add_response(url=url, content=b"hello")

        custom_client = httpx.Client()
        fetcher = HttpFetcher(client=custom_client)
        result = b"".join(fetcher.fetch(url))
        assert result == b"hello"
        custom_client.close()


class TestHttpFetcherLifecycle:
    def test_close_closes_owned_client(self):
        fetcher = HttpFetcher()
        client = fetcher._client

        fetcher.close()

        assert client.is_closed

    def test_close_does_not_close_injected_client(self):
        client = httpx.Client()
        fetcher = HttpFetcher(client=client)

        fetcher.close()

        assert not client.is_closed
        client.close()

    def test_context_manager_closes_client(self):
        with HttpFetcher() as fetcher:
            client = fetcher._client
            assert not client.is_closed

        assert client.is_closed

    def test_del_closes_owned_client(self):
        fetcher = HttpFetcher()
        client = fetcher._client

        del fetcher

        assert client.is_closed


class ClosingFetcher:
    def __init__(self):
        self.closed = False

    def fetch(self, url):
        yield b""

    def close(self):
        self.closed = True


class TestCloseDelegation:
    def test_retrying_fetcher_close_delegates(self):
        base = ClosingFetcher()
        retrying = RetryingFetcher(base, max_retries=0)

        retrying.close()

        assert base.closed


class TestRetryingFetcher:
    def test_retries_until_success(self):
        base = FlakyFetcher(failures=2)
        retrying = RetryingFetcher(base, max_retries=3, retry_delay=0.01)

        result = b"".join(retrying.fetch("https://example.com/x.gz"))

        assert result == b"ok"
        assert base.call_count == 3

    def test_raises_after_retries_exhausted(self):
        base = FlakyFetcher(failures=5)
        retrying = RetryingFetcher(base, max_retries=2, retry_delay=0.01)

        with pytest.raises(NetworkError):
            list(retrying.fetch("https://example.com/x.gz"))

        assert base.call_count == 3

    def test_no_retry_when_max_retries_zero(self):
        base = FlakyFetcher(failures=1)
        retrying = RetryingFetcher(base, max_retries=0)

        with pytest.raises(NetworkError):
            list(retrying.fetch("https://example.com/x.gz"))

        assert base.call_count == 1

    def test_data_unavailable_not_retried(self):
        class NotFoundFetcher:
            def __init__(self):
                self.call_count = 0

            def fetch(self, url):
                self.call_count += 1
                raise DataUnavailableError(url)

        base = NotFoundFetcher()
        retrying = RetryingFetcher(base, max_retries=3, retry_delay=0.01)

        with pytest.raises(DataUnavailableError):
            list(retrying.fetch("https://example.com/x.gz"))

        assert base.call_count == 1

    def test_negative_max_retries_rejected(self):
        with pytest.raises(ValueError):
            RetryingFetcher(FlakyFetcher(0), max_retries=-1)
