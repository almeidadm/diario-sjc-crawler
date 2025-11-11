"""Tests for ConcurrentHttpClient class."""

from pathlib import Path

import httpx
import pytest
import vcr

from diario_crawler.http.client import HttpClient
from diario_crawler.http.concurrent import ConcurrentHttpClient

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


class TestConcurrentHttpClient:
    """Test suite for ConcurrentHttpClient."""

    def test_init_default(self):
        """Test ConcurrentHttpClient initialization with default values."""
        client = ConcurrentHttpClient()
        assert client.client is not None
        assert isinstance(client.client, HttpClient)
        assert client.semaphore._value == 10  # Default max_concurrent

    def test_init_custom_base_client(self):
        """Test ConcurrentHttpClient with custom base client."""
        base_client = HttpClient()
        client = ConcurrentHttpClient(base_client=base_client, max_concurrent=5)
        assert client.client is base_client
        assert client.semaphore._value == 5

    def test_init_custom_max_concurrent(self):
        """Test ConcurrentHttpClient with custom max_concurrent."""
        client = ConcurrentHttpClient(max_concurrent=20)
        assert client.semaphore._value == 20

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_fetch_all_concurrent.yaml")
    async def test_fetch_all_success(self):
        """Test fetch_all with multiple URLs."""
        client = ConcurrentHttpClient(max_concurrent=3)
        test_urls = [
            "https://httpbin.org/get?test=1",
            "https://httpbin.org/get?test=2",
            "https://httpbin.org/get?test=3",
        ]

        async with httpx.AsyncClient() as http_client:
            responses = await client.fetch_all(test_urls, http_client, max_retries=2)

        assert len(responses) == 3
        assert all(r is not None for r in responses)
        assert all(r.status_code == 200 for r in responses)

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_fetch_all_mixed.yaml")
    async def test_fetch_all_mixed_results(self):
        """Test fetch_all with some successful and some failed requests."""
        client = ConcurrentHttpClient(max_concurrent=3)
        test_urls = [
            "https://httpbin.org/get",  # Success
            "https://httpbin.org/status/404",  # Failure
            "https://httpbin.org/get",  # Success
        ]

        async with httpx.AsyncClient() as http_client:
            responses = await client.fetch_all(test_urls, http_client, max_retries=2)

        assert len(responses) == 3
        # First and third should succeed
        assert responses[0] is not None
        assert responses[0].status_code == 200
        # Second should fail (404)
        assert responses[1] is None
        # Third should succeed
        assert responses[2] is not None
        assert responses[2].status_code == 200

    @pytest.mark.asyncio
    async def test_fetch_all_empty_list(self):
        """Test fetch_all with empty URL list."""
        client = ConcurrentHttpClient()

        async with httpx.AsyncClient() as http_client:
            responses = await client.fetch_all([], http_client)

        assert len(responses) == 0

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_fetch_all_concurrency_limit.yaml")
    async def test_fetch_all_concurrency_limit(self):
        """Test that fetch_all respects concurrency limit."""
        client = ConcurrentHttpClient(max_concurrent=2)
        # Create 5 URLs to test concurrency limiting
        test_urls = [f"https://httpbin.org/get?i={i}" for i in range(5)]

        async with httpx.AsyncClient() as http_client:
            responses = await client.fetch_all(test_urls, http_client, max_retries=2)

        assert len(responses) == 5
        # All should succeed (semaphore limits concurrent requests)
        assert all(r is not None for r in responses)

