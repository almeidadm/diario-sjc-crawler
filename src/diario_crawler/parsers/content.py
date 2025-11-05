"""Parser para conteúdo dos artigos (Fase 3)."""

import logging

import httpx

from ..models import ArticleContent, ContentType

logger = logging.getLogger(__name__)


class ContentParser:
    """Parseia conteúdo final dos artigos."""

    @staticmethod
    def parse(response: httpx.Response) -> ArticleContent | None:
        """
        Extrai e processa conteúdo de um artigo.

        Args:
            response: Resposta HTTP com conteúdo do artigo

        Returns:
            ArticleContent processado ou None se inválido
        """
        content_type_header = response.headers.get("content-type", "").lower()

        try:
            # Detecta tipo de conteúdo
            if "application/pdf" in content_type_header:
                return ArticleContent(
                    raw_content=response.content,
                    content_type=ContentType.PDF,
                )

            elif "application/json" in content_type_header:
                # Assume que o JSON contém HTML no campo 'conteudo'
                data = response.json()
                html_content = data.get("conteudo", "")

                return ArticleContent(
                    raw_content=html_content,
                    content_type=ContentType.HTML,
                )

            elif "text/html" in content_type_header:
                return ArticleContent(
                    raw_content=response.text,
                    content_type=ContentType.HTML,
                )

            else:
                # Conteúdo texto genérico
                return ArticleContent(
                    raw_content=response.text,
                    content_type=ContentType.TEXT,
                )

        except Exception as e:
            logger.error(f"Erro ao processar conteúdo: {e}")
            return None
