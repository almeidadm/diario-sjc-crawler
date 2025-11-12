"""Cliente HTTP para requisições concorrentes com limitação via asyncio.Semaphore."""

import asyncio
import logging

import httpx

from .client import HttpClient

logger = logging.getLogger(__name__)


class ConcurrentHttpClient:
    """
    Cliente HTTP para requisições concorrentes com limitação via asyncio.Semaphore.

    O retry é gerenciado pelo HttpClient base usando tenacity.
    Esta classe apenas gerencia a concorrência com semáforo.
    """

    def __init__(
        self,
        base_client: HttpClient | None = None,
        max_concurrent: int = 10,
    ):
        """
        Args:
            base_client: Cliente HTTP base (usará o padrão se None)
            max_concurrent: Número máximo de requisições concorrentes (controlado por asyncio.Semaphore)
        """
        self.client = base_client or HttpClient()
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def fetch_all(
        self,
        urls: list[str],
        client: httpx.AsyncClient,
        max_retries: int = 3,
    ) -> list[httpx.Response | None]:
        """
        Realiza múltiplas requisições concorrentes com limitação via asyncio.Semaphore.

        O retry é gerenciado pelo HttpClient base usando tenacity.

        Args:
            urls: Lista de URLs para requisitar
            client: Cliente httpx compartilhado
            max_retries: Número máximo de retries por URL (passado para HttpClient.fetch)

        Returns:
            Lista de respostas (ou None para falhas) na mesma ordem das URLs
        """

        async def fetch_with_semaphore(url: str) -> httpx.Response | None:
            """
            Realiza uma requisição respeitando o limite de concorrência do semáforo.
            O retry é gerenciado internamente pelo HttpClient via tenacity.
            """
            async with self.semaphore:
                return await self.client.fetch(url, client, max_retries=max_retries)

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

        successful = len([r for r in processed_results if r])
        logger.info(f"Concluídas {successful}/{len(urls)} requisições com sucesso")
        return processed_results
