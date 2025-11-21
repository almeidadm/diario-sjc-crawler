"""Cliente HTTP básico para requisições."""

import asyncio
import logging

import httpx
from tenacity import (
    after_log,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class HttpClient:
    """Cliente HTTP com tratamento de erros e timeouts configurados."""

    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "*/*",
        "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
        "Referer": "https://diariodomunicipio.sjc.sp.gov.br/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-GPC": "1",
        "Priority": "u=0",
    }

    DEFAULT_TIMEOUT = httpx.Timeout(
        connect=5.0,  # Estabelecer conexão
        read=30.0,  # Ler resposta
        write=10.0,  # Enviar dados
        pool=5.0,  # Obter conexão do pool
    )

    def __init__(
        self,
        headers: dict | None = None,
        timeout: httpx.Timeout | None = None,
    ):
        """
        Args:
            headers: Headers customizados (merge com DEFAULT_HEADERS)
            timeout: Configuração de timeout customizada
        """
        self.headers = {**self.DEFAULT_HEADERS, **(headers or {})}
        self.timeout = timeout or self.DEFAULT_TIMEOUT

    async def _fetch_internal(
        self,
        url: str,
        client: httpx.AsyncClient,
    ) -> httpx.Response:
        """
        Realiza uma requisição HTTP GET assíncrona (internal, raises exceptions).

        Args:
            url: URL alvo
            client: Cliente httpx compartilhado

        Returns:
            Response HTTP

        Raises:
            httpx.HTTPStatusError: Em caso de erro de status HTTP
            httpx.TimeoutException: Em caso de timeout
            httpx.RequestError: Em caso de erro de requisição
        """
        response = await client.get(
            url,
            headers=self.headers,
            timeout=self.timeout,
            follow_redirects=True,
        )

        # Verifica status code (raise_for_status levanta exceção em 4xx/5xx)
        response.raise_for_status()

        logger.debug(
            f"Requisição bem-sucedida para {url} - Status: {response.status_code}"
        )
        return response

    def _should_retry_status_error(self, status_code: int) -> bool:
        """Determina se um erro de status HTTP deve ser retentado."""
        # Retenta em 5xx (erros do servidor)
        if 500 <= status_code < 600:
            return True
        # Retenta em alguns 4xx específicos
        if status_code in (408, 429):  # Request Timeout, Too Many Requests
            return True
        # Não retenta em outros 4xx (erros do cliente)
        return False

    async def fetch(
        self,
        url: str,
        client: httpx.AsyncClient,
        max_retries: int = 3,
    ) -> httpx.Response | None:
        """
        Realiza uma requisição HTTP GET assíncrona com retry automático via tenacity.

        Args:
            url: URL alvo
            client: Cliente httpx compartilhado
            max_retries: Número máximo de tentativas (padrão: 3)

        Returns:
            Response HTTP ou None em caso de erro não recuperável
        """

        # Cria uma função wrapper com retry específica para esta URL
        @retry(
            retry=retry_if_exception_type(
                (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError)
            ),
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            after=after_log(logger, logging.ERROR),
            reraise=False,
        )
        async def _fetch_with_retry() -> httpx.Response | None:
            try:
                return await self._fetch_internal(url, client)
            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                if not self._should_retry_status_error(status_code):
                    # Erro não retentável, retorna None (não será retentado)
                    logger.warning(
                        f"Erro HTTP {status_code} em {url}: {e} (não retentável)"
                    )
                    return None
                # Erro retentável, propaga exceção para tenacity retentar
                logger.warning(f"Erro HTTP {status_code} em {url}: {e} (retentando)")
                raise

            except (httpx.TimeoutException, httpx.RequestError) as e:
                logger.warning(f"Erro de requisição em {url}: {e} (retentando)")
                raise

            except Exception as e:
                # Erros inesperados não são retentados
                logger.error(f"Erro inesperado em {url}: {e}")
                return None

        try:
            result = await _fetch_with_retry()
            if result is None:
                logger.error(
                    f"Falha ao obter resposta para {url} após {max_retries} tentativas"
                )
            return result
        except Exception as e:
            # Captura qualquer exceção não tratada após todos os retries
            logger.error(f"Erro crítico após retries em {url}: {e}")
            return None


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
