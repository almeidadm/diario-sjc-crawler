"""Tests for HttpClient class."""

from pathlib import Path

import httpx
import pytest
import vcr

from diario_crawler.http.client import HttpClient

# VCR configuration
vcr_dir = Path(__file__).parent / "fixtures" / "vcr_cassettes"
vcr_config = vcr.VCR(
    cassette_library_dir=str(vcr_dir),
    record_mode="once",
    match_on=["method", "scheme", "host", "port", "path", "query"],
    filter_headers=["authorization", "cookie"],
    ignore_localhost=True,
    decode_compressed_response=True,
)


class TestHttpClient:
    """Test suite for HttpClient."""

    def test_init_default(self):
        """Test HttpClient initialization with default values."""
        client = HttpClient()
        assert client.headers == HttpClient.DEFAULT_HEADERS
        assert client.timeout == HttpClient.DEFAULT_TIMEOUT

    def test_init_custom_headers(self):
        """Test HttpClient initialization with custom headers."""
        custom_headers = {"User-Agent": "TestAgent/1.0", "Custom-Header": "value"}
        client = HttpClient(headers=custom_headers)
        assert client.headers["User-Agent"] == "TestAgent/1.0"
        assert client.headers["Custom-Header"] == "value"
        # Should still have default headers merged
        assert "Accept" in client.headers

    def test_init_custom_timeout(self):
        """Test HttpClient initialization with custom timeout."""
        custom_timeout = httpx.Timeout(
            connect=10.0, read=60.0, write=15.0, pool=5.0
        )
        client = HttpClient(timeout=custom_timeout)
        assert client.timeout.connect == 10.0
        assert client.timeout.read == 60.0
        assert client.timeout.write == 15.0
        assert client.timeout.pool == 5.0

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_fetch_success.yaml")
    async def test_fetch_success(self):
        """Test successful HTTP fetch."""
        client = HttpClient()
        test_url = "https://httpbin.org/get"

        async with httpx.AsyncClient() as http_client:
            response = await client.fetch(test_url, http_client)

        assert response is not None
        assert response.status_code == 200
        assert "url" in response.json()

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_fetch_404.yaml")
    async def test_fetch_404_error(self):
        """Test fetch with 404 error."""
        client = HttpClient()
        test_url = "https://httpbin.org/status/404"

        async with httpx.AsyncClient() as http_client:
            response = await client.fetch(test_url, http_client)

        assert response is None


    @pytest.mark.asyncio
    async def test_fetch_invalid_url(self):
        """Test fetch with invalid URL."""
        client = HttpClient()
        test_url = "https://invalid-domain-that-does-not-exist-12345.com"

        async with httpx.AsyncClient() as http_client:
            response = await client.fetch(test_url, http_client)

        assert response is None

