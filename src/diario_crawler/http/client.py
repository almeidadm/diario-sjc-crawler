"""Cliente HTTP básico para requisições."""

import asyncio
import logging

import httpx

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

    async def fetch(
        self,
        url: str,
        client: httpx.AsyncClient,
    ) -> httpx.Response | None:
        """
        Realiza uma requisição HTTP GET assíncrona.

        Args:
            url: URL alvo
            client: Cliente httpx compartilhado

        Returns:
            Response HTTP ou None em caso de erro
        """
        try:
            response = await client.get(
                url,
                headers=self.headers,
                timeout=self.timeout,
                follow_redirects=True,
            )

            # Verifica status code
            response.raise_for_status()

            logger.debug(
                f"Requisição bem-sucedida para {url} - Status: {response.status_code}"
            )
            return response

        except httpx.HTTPStatusError as e:
            logger.warning(f"Erro HTTP {e.response.status_code} em {url}: {e}")
            return None

        except httpx.TimeoutException as e:
            logger.error(f"Timeout em {url}: {e}")
            return None

        except httpx.RequestError as e:
            logger.error(f"Erro de requisição em {url}: {e}")
            return None

        except Exception as e:
            logger.error(f"Erro inesperado em {url}: {e}")
            return None

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
            response = await self.fetch(url, client)

            if response is not None:
                return response

            if attempt < max_retries - 1:
                logger.info(f"Tentativa {attempt + 1} falhou, retry em {retry_delay}s")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2

        logger.error(f"Todas as {max_retries} tentativas falharam para {url}")
        return None
