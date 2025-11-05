"""Processador para agregação de dados em GazetteEdition."""

import logging
from typing import Any

from ..models import Article, GazetteEdition, GazetteMetadata, ArticleMetadata
from ..utils import get_logger

logger = get_logger(__name__)


class DataProcessor:
    """Processador para agregação de artigos por edição."""

    @staticmethod
    def aggregate_editions(
        metadata_list: list[GazetteMetadata],
        articles_with_content: list[dict[str, Any]],
    ) -> list[GazetteEdition]:
        """
        Agrega artigos por edição e constrói objetos GazetteEdition.

        Args:
            metadata_list: Lista de metadados das edições
            articles_with_content: Lista de artigos com seus conteúdos

        Returns:
            Lista de GazetteEdition com artigos agregados
        """
        # Cria um dicionário de metadados por edition_id
        metadata_by_id: dict[str, GazetteMetadata] = {
            metadata.edition_id: metadata for metadata in metadata_list
        }

        logger.debug(f"Metadados disponíveis: {list(metadata_by_id.keys())}")

        # Agrupa artigos por edition_id
        articles_by_edition: dict[str, list[Article]] = {}
        
        for item in articles_with_content:
            article_metadata = item["article_metadata"]
            edition_id = article_metadata.edition_id

            # Log para debug
            logger.debug(f"Processando artigo {article_metadata.article_id} para edição {edition_id}")

            # Verifica se a edição do artigo existe nos metadados
            if edition_id not in metadata_by_id:
                logger.warning(
                    f"Artigo {article_metadata.article_id} referencia edição {edition_id} "
                    f"que não existe nos metadados. Metadados disponíveis: {list(metadata_by_id.keys())}"
                )
                continue

            # Cria o objeto Article
            article = Article(
                metadata=article_metadata,
                content=item["content"],
            )

            if edition_id not in articles_by_edition:
                articles_by_edition[edition_id] = []
            articles_by_edition[edition_id].append(article)

        logger.debug(f"Artigos agrupados por edição: { {k: len(v) for k, v in articles_by_edition.items()} }")

        # Constrói as edições
        editions = []
        for edition_id, metadata in metadata_by_id.items():
            articles = articles_by_edition.get(edition_id, [])
            edition = GazetteEdition(metadata=metadata, articles=articles)
            editions.append(edition)
            logger.info(
                f"Edição {edition_id} agregada com {len(articles)} artigos"
            )

        # Log de edições sem artigos
        editions_without_articles = [e for e in editions if not e.articles]
        if editions_without_articles:
            logger.warning(
                f"{len(editions_without_articles)} edições sem artigos: "
                f"{[e.edition_id for e in editions_without_articles]}"
            )

        logger.info(f"Total de {len(editions)} edições agregadas")
        return editions

    @staticmethod
    def create_article(
        article_metadata: ArticleMetadata, 
        article_content: Any
    ) -> Article:
        """
        Cria um artigo completo a partir de metadados e conteúdo.
        
        Args:
            article_metadata: Metadados do artigo
            article_content: Conteúdo do artigo
            
        Returns:
            Artigo completo
        """
        return Article(
            metadata=article_metadata,
            content=article_content,
        )

    @staticmethod
    def create_gazette_edition(
        gazette_metadata: GazetteMetadata,
        articles: list[Article] | None = None,
    ) -> GazetteEdition:
        """
        Cria uma edição completa do diário.
        
        Args:
            gazette_metadata: Metadados da edição
            articles: Lista de artigos (opcional)
            
        Returns:
            Edição completa do diário
        """
        return GazetteEdition(
            metadata=gazette_metadata,
            articles=articles or [],
        )