"""Cliente HTTP para requisições concorrentes com retry mechanism."""

import asyncio
import logging

import httpx

from .client import HttpClient

logger = logging.getLogger(__name__)


class ConcurrentHttpClient:
    """Cliente HTTP para requisições concorrentes com limitação e retry."""

    def __init__(
        self,
        base_client: HttpClient | None = None,
        max_concurrent: int = 10,
    ):
        """
        Args:
            base_client: Cliente HTTP base (usará o padrão se None)
            max_concurrent: Número máximo de requisições concorrentes
        """
        self.client = base_client or HttpClient()
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_with_retry(
        self,
        url: str,
        client: httpx.AsyncClient,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> httpx.Response | None:
        """
        Realiza requisição com mecanismo de retry.

        Args:
            url: URL alvo
            client: Cliente httpx compartilhado
            max_retries: Número máximo de tentativas
            retry_delay: Delay entre tentativas (segundos)

        Returns:
            Response HTTP ou None após todas as tentativas
        """
        for attempt in range(max_retries):
            response = await self.client.fetch(url, client)

            if response is not None:
                return response

            if attempt < max_retries - 1:
                logger.info(f"Tentativa {attempt + 1} falhou, retry em {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff

        logger.error(f"Todas as {max_retries} tentativas falharam para {url}")
        return None

    async def fetch_all(
        self,
        urls: list[str],
        client: httpx.AsyncClient,
        max_retries: int = 3,
    ) -> list[httpx.Response | None]:
        """
        Realiza múltiplas requisições concorrentes com limitação.

        Args:
            urls: Lista de URLs para requisitar
            client: Cliente httpx compartilhado
            max_retries: Número máximo de retries por URL

        Returns:
            Lista de respostas (ou None para falhas) na mesma ordem das URLs
        """

        async def fetch_with_semaphore(url: str) -> httpx.Response | None:
            async with self.semaphore:
                return await self.fetch_with_retry(url, client, max_retries)

        tasks = [fetch_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Processa resultados, convertendo exceções em None
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Erro durante requisição concorrente: {result}")
                processed_results.append(None)
            else:
                processed_results.append(result)

        logger.info(
            f"Concluídas {len([r for r in processed_results if r])}/{len(urls)} requisições"
        )
        return processed_results
