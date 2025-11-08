"""Orquestrador principal do crawler."""

import asyncio
from typing import AsyncIterator

import httpx

from diario_crawler.core.config import CrawlerConfig
from diario_crawler.http import ConcurrentHttpClient, HttpClient
from diario_crawler.models import ArticleMetadata, GazetteEdition
from diario_crawler.parsers import ContentParser, HtmlStructureParser, MetadataParser
from diario_crawler.processors import DataProcessor
from diario_crawler.storage import BaseStorage, ParquetStorage
from diario_crawler.utils import get_logger, get_workdays

logger = get_logger(__name__)


class GazetteCrawler:
    """
    Orquestrador para crawling de edições do Diário do Município.
    Processa dados em lotes para otimizar uso de memória.
    """

    def __init__(
        self, config: CrawlerConfig | None = None, storage: BaseStorage | None = None
    ):
        """
        Args:
            config: Configuração do crawler (usa padrão se None)
        """
        self.config = config or CrawlerConfig()

        self.http_client = HttpClient()
        self.concurrent_client = ConcurrentHttpClient(
            base_client=self.http_client,
            max_concurrent=self.config.max_concurrent,
        )
        self.metadata_parser = MetadataParser()
        self.structure_parser = HtmlStructureParser()
        self.content_parser = ContentParser()
        self.data_processor = DataProcessor()
        self.storage = storage or ParquetStorage()

    def __repr__(self) -> str:
        return f"<GazetteCrawler start={self.config.start_date} end={self.config.end_date}>"

    def create_metadata_urls(self) -> list[str]:
        """Gera URLs para download dos metadados das edições."""
        dates = get_workdays(start=self.config.start_date, end=self.config.end_date)
        return [f"{self.config.METADATA_BASE_URL}{dt:%Y-%m-%d}.json" for dt in dates]

    async def fetch_metadata_batch(
        self, urls: list[str]
    ) -> tuple[list, dict[str, str]]:
        """
        Fase 1: Download dos metadados das edições.

        Returns:
            Tupla com (lista de metadados, mapa de edition_id para URL)
        """
        logger.info(f"Baixando {len(urls)} metadados...")

        async with httpx.AsyncClient() as client:
            responses = await self.concurrent_client.fetch_all(urls, client)

        all_metadata = []
        gazette_id_map = {}

        for response in responses:
            if not response:
                continue

            try:
                metadata_list = self.metadata_parser.parse(response)

                for metadata in metadata_list:
                    gazette_id_map[metadata.edition_id] = metadata.pdf_url

                all_metadata.extend(metadata_list)
                logger.debug(f"Metadados extraídos: {len(metadata_list)}")

            except Exception as e:
                logger.error(f"Erro ao processar metadados: {e}")
                continue

        logger.info(f"Extraídos {len(all_metadata)} metadados de {len(urls)} URLs")
        return all_metadata, gazette_id_map

    async def fetch_structure_batch(self, metadata_list: list) -> list[dict]:
        """
        Fase 2: Download das páginas HTML de estrutura.

        Returns:
            Lista de dicionários com {edition_id, html, url}
        """
        if not metadata_list:
            logger.warning("Nenhum metadado para buscar estrutura HTML")
            return []

        urls = [
            f"{self.config.HTML_BASE_URL}{metadata.edition_id}"
            for metadata in metadata_list
        ]

        logger.info(f"Baixando {len(urls)} estruturas HTML...")

        async with httpx.AsyncClient() as client:
            responses = await self.concurrent_client.fetch_all(urls, client)

        html_results = []

        for metadata, response in zip(metadata_list, responses):
            if not response:
                continue

            html_results.append(
                {
                    "edition_id": metadata.edition_id,
                    "html": response.text,
                    "url": str(response.url),
                }
            )

        logger.info(f"Obtidas {len(html_results)} estruturas HTML")
        return html_results

    def parse_articles_from_html(
        self, html_results: list[dict]
    ) -> list[ArticleMetadata]:
        """
        Parseia artigos das estruturas HTML usando HtmlStructureParser.

        Args:
            html_results: Lista de resultados HTML da fase 2

        Returns:
            Lista de ArticleMetadata deduplicados
        """
        all_articles = []

        for html_data in html_results:
            try:
                articles = self.structure_parser.parse(
                    html=html_data["html"], edition_id=html_data["edition_id"]
                )
                all_articles.extend(articles)
                logger.debug(
                    f"Extraídos {len(articles)} artigos da edição {html_data['edition_id']}"
                )

            except Exception as e:
                logger.error(
                    f"Erro ao parsear HTML da edição {html_data['edition_id']}: {e}"
                )
                continue

        # Remove duplicatas mantendo os artigos mais profundos
        unique_articles = self.structure_parser.deduplicate_keep_deepest(all_articles)

        logger.info(
            f"Parseados {len(unique_articles)} artigos únicos de {len(html_results)} estruturas HTML"
        )
        return unique_articles

    async def fetch_content_batch(self, articles: list[ArticleMetadata]) -> list[dict]:
        """
        Fase 3: Busca o conteúdo das matérias.

        Args:
            articles: Lista de ArticleMetadata

        Returns:
            Lista de dicionários com {article_metadata, content}
        """
        if not articles:
            logger.warning("Nenhum artigo para buscar conteúdo")
            return []

        urls = [
            f"{self.config.CONTENT_BASE_URL}{article.identifier}"
            for article in articles
        ]

        logger.info(f"Baixando {len(urls)} conteúdos de artigos...")

        async with httpx.AsyncClient() as client:
            responses = await self.concurrent_client.fetch_all(urls, client)

        content_results = []

        for article, response in zip(articles, responses):
            if not response:
                continue

            try:
                content = self.content_parser.parse(response)
                if content:
                    content_results.append(
                        {
                            "article_metadata": article,
                            "content": content,
                        }
                    )

            except Exception as e:
                logger.error(
                    f"Erro ao processar conteúdo do artigo {article.article_id}: {e}"
                )
                continue

        logger.info(f"Obtidos {len(content_results)} conteúdos de artigos")
        return content_results

    async def process_batch(self, urls: list[str]) -> list[GazetteEdition]:
        """
        Processa um lote de URLs e retorna edições completas.

        Returns:
            Lista de GazetteEdition com artigos e conteúdos
        """
        # Fase 1: Metadados das edições
        metadata_list, gazette_id_map = await self.fetch_metadata_batch(urls)

        if not metadata_list:
            logger.warning("Lote sem metadados válidos")
            return []

        # Fase 2: Estrutura HTML e parse de artigos
        html_results = await self.fetch_structure_batch(metadata_list)

        if not html_results:
            logger.warning("Lote sem estruturas HTML válidas")
            return []

        # Parse dos artigos do HTML
        articles = self.parse_articles_from_html(html_results)

        if not articles:
            logger.warning("Lote sem artigos válidos")
            return []

        # Fase 3: Conteúdo dos artigos
        content_results = await self.fetch_content_batch(articles)

        # Agrega os resultados em GazetteEdition
        editions = self.data_processor.aggregate_editions(
            metadata_list, content_results
        )

        logger.info(f"Processado lote com {len(editions)} edições")
        return editions

    async def run_batched(self) -> AsyncIterator[list[GazetteEdition]]:
        """
        Executa o crawling em lotes, gerando edições progressivamente.

        Yields:
            Listas de GazetteEdition por lote
        """
        all_urls = self.create_metadata_urls()
        total_urls = len(all_urls)

        logger.info(f"Iniciando crawling em lotes de {self.config.batch_size}")
        logger.info(f"Total de {total_urls} URLs para processar")

        for i in range(0, total_urls, self.config.batch_size):
            batch_urls = all_urls[i : i + self.config.batch_size]
            batch_num = (i // self.config.batch_size) + 1
            total_batches = (
                total_urls + self.config.batch_size - 1
            ) // self.config.batch_size

            logger.info(f"Processando lote {batch_num}/{total_batches}")

            batch_editions = await self.process_batch(batch_urls)
            yield batch_editions

            # Pausa entre lotes para não sobrecarregar o servidor
            if i + self.config.batch_size < total_urls:
                await asyncio.sleep(1)

        logger.info("Crawling em lotes concluído")

    async def run(self) -> int:
        """
        Executa o crawling completo e retorna todas as edições.

        Returns:
            Lista com todas as GazetteEdition processadas
        """
        n_editions = 0
        n_articles = 0

        async for batch in self.run_batched():
            self.storage.save_editions(batch)
            n_editions += len(batch)
            n_articles += sum([len(g.articles) for g in batch])

        logger.info(f"Total: {n_editions} edições processadas")
        logger.info(f"Total: {n_articles} artigos processadas")

        return n_editions, n_articles
