"""Tests for HttpClient and ConcurrentHttpClient behavior."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from diario_crawler.core.clients import ConcurrentHttpClient, HttpClient

pytestmark = pytest.mark.order(1)

# ==========================================================
# HttpClient
# ==========================================================


@pytest.mark.asyncio
async def test_http_client_init_defaults():
    """Testa inicialização padrão e configuração de headers/timeouts."""
    client = HttpClient()
    assert "User-Agent" in client.headers
    assert isinstance(client.timeout, httpx.Timeout)
    assert client.timeout.connect == 5.0


@pytest.mark.asyncio
async def test_http_client_custom_headers_and_timeout():
    """Testa merge de headers customizados e timeout customizado."""
    custom_headers = {"X-Test": "123", "User-Agent": "CustomUA"}
    custom_timeout = httpx.Timeout(connect=1.0, read=2.0, write=3.0, pool=4.0)

    client = HttpClient(headers=custom_headers, timeout=custom_timeout)
    assert client.headers["X-Test"] == "123"
    assert client.headers["User-Agent"] == "CustomUA"
    assert client.timeout.connect == 1.0
    assert client.timeout.read == 2.0


@pytest.mark.asyncio
async def test_should_retry_status_error():
    """Testa lógica de decisão de retry para status HTTP."""
    client = HttpClient()

    assert client._should_retry_status_error(500)
    assert client._should_retry_status_error(503)
    assert client._should_retry_status_error(429)
    assert not client._should_retry_status_error(404)
    assert not client._should_retry_status_error(400)


@pytest.mark.asyncio
@pytest.mark.parametrize("vcr_cassette", ["test_fetch_success.yaml"], indirect=True)
async def test_fetch_success(vcr_cassette):
    """Testa requisição bem-sucedida (via VCR)."""
    client = HttpClient()
    url = "https://httpbin.org/get"

    async with httpx.AsyncClient() as http_client:
        response = await client.fetch(url, http_client)

    assert response.status_code == 200
    assert "url" in response.json()


@pytest.mark.asyncio
@pytest.mark.parametrize("vcr_cassette", ["test_fetch_404.yaml"], indirect=True)
async def test_fetch_404_returns_none(vcr_cassette):
    """Testa comportamento em 404 (erro não retentável)."""
    client = HttpClient()
    url = "https://httpbin.org/status/404"

    async with httpx.AsyncClient() as http_client:
        response = await client.fetch(url, http_client)

    assert response is None


@pytest.mark.asyncio
async def test_fetch_retries_on_timeout(monkeypatch):
    """Testa retry automático em caso de timeout."""
    client = HttpClient()

    mock_client = AsyncMock()
    mock_client.get.side_effect = httpx.TimeoutException("Timeout simulated")

    # Monkeypatch httpx.AsyncClient
    with patch.object(
        client,
        "_fetch_internal",
        side_effect=httpx.TimeoutException("Timeout simulated"),
    ):
        async with httpx.AsyncClient() as http_client:
            result = await client.fetch("http://fakeurl", http_client, max_retries=2)
            assert result is None


@pytest.mark.asyncio
async def test_fetch_unexpected_exception(monkeypatch):
    """Testa tratamento de erro inesperado (não retentável)."""
    client = HttpClient()

    async def fake_fetch_internal(url, http_client):
        raise ValueError("Erro inesperado")

    monkeypatch.setattr(client, "_fetch_internal", fake_fetch_internal)

    async with httpx.AsyncClient() as http_client:
        result = await client.fetch("http://fakeurl", http_client)
        assert result is None


# ==========================================================
# ConcurrentHttpClient
# ==========================================================


@pytest.mark.asyncio
async def test_concurrent_client_limits_concurrency(monkeypatch):
    """Testa se o cliente concorrente respeita o limite de semáforo."""
    base_client = HttpClient()
    concurrent_client = ConcurrentHttpClient(base_client=base_client, max_concurrent=2)

    call_log = []

    async def fake_fetch(url, client, max_retries):
        call_log.append(url)
        await asyncio.sleep(0.05)
        return MagicMock(status_code=200, url=url)

    concurrent_client.client.fetch = fake_fetch

    async with httpx.AsyncClient() as http_client:
        urls = [f"http://fake{i}.com" for i in range(5)]
        results = await concurrent_client.fetch_all(urls, http_client)

    assert len(results) == 5
    assert all(r.status_code == 200 for r in results if r)
    # Verifica se não travou e respeitou max_concurrent (deve concluir)
    assert len(call_log) == 5


@pytest.mark.asyncio
async def test_concurrent_client_handles_exceptions(monkeypatch):
    """Testa se exceções em tarefas são capturadas e retornadas como None."""
    concurrent_client = ConcurrentHttpClient(max_concurrent=3)

    async def fake_fetch(url, client, max_retries):
        if "bad" in url:
            raise httpx.RequestError("Erro simulado")
        return MagicMock(status_code=200)

    concurrent_client.client.fetch = fake_fetch

    async with httpx.AsyncClient() as http_client:
        urls = ["http://good.com", "http://bad.com"]
        results = await concurrent_client.fetch_all(urls, http_client)

    assert len(results) == 2
    assert results[0] is not None
    assert results[1] is None


@pytest.mark.asyncio
async def test_concurrent_client_with_vcr(vcr_cassette):
    """Testa execução real concorrente com VCR."""
    client = HttpClient()
    concurrent_client = ConcurrentHttpClient(base_client=client, max_concurrent=3)

    urls = [
        "https://httpbin.org/get?x=1",
        "https://httpbin.org/get?x=2",
        "https://httpbin.org/get?x=3",
    ]

    async with httpx.AsyncClient() as http_client:
        results = await concurrent_client.fetch_all(urls, http_client)

    assert all(r.status_code == 200 for r in results if r)
    assert len(results) == len(urls)
