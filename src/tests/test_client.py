import httpx
import pytest

from gharchive_streamer._client import HttpFetcher
from gharchive_streamer._exceptions import DataUnavailableError, NetworkError


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
