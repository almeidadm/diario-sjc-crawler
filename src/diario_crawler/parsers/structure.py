"""Parser para estrutura HTML de navegação (Fase 2)."""

import logging
from typing import Any

from selectolax.lexbor import LexborHTMLParser, LexborNode

from ..models import ArticleMetadata

logger = logging.getLogger(__name__)


class HtmlStructureParser:
    """Parseia HTML da árvore de navegação de artigos."""

    @staticmethod
    def parse(html: str, edition_id: str) -> list[ArticleMetadata]:
        """
        Extrai metadados de artigos da árvore HTML de navegação.
        
        Args:
            html: HTML da estrutura de navegação
            edition_id: ID da edição (para associar aos artigos)
            
        Returns:
            Lista de ArticleMetadata com hierarquia completa
        """
        tree = LexborHTMLParser(html)
        root_ul = tree.css_first("ul#tree")
        
        if not root_ul:
            logger.warning(f"Árvore de navegação não encontrada para edição {edition_id}")
            return []

        articles = []

        def parse_node(li: LexborNode, path: list[str]) -> None:
            """Recursivamente parseia nós da árvore."""
            # Verifica se é uma pasta/categoria
            folder_span = li.css_first("span.folder")
            if folder_span:
                folder_name = folder_span.text(strip=True)
                sub_ul = li.css_first("ul")
                new_path = path + [folder_name]
                
                if sub_ul:
                    for sub_li in sub_ul.css("li"):
                        parse_node(sub_li, new_path)
            else:
                # É um link de artigo
                for link in li.css("a.linkMateria"):
                    try:
                        # CORREÇÃO: Sempre usa o edition_id da gazette, não do data-id
                        article = ArticleMetadata(
                            article_id=link.attributes.get("data-materia-id", ""),
                            edition_id=edition_id,  # Usa o edition_id passado, não do data-id
                            hierarchy_path=path.copy(),
                            title=link.text(strip=True),
                            identifier=link.attributes.get("identificador"),
                            protocol=link.attributes.get("data-protocolo"),
                        )
                        articles.append(article)
                        
                    except Exception as e:
                        logger.error(f"Erro ao parsear link de artigo: {e}")

        # Processa todos os nós de primeiro nível
        for li in root_ul.css("li"):
            parse_node(li, path=[])

        logger.debug(f"Extraídos {len(articles)} artigos da estrutura HTML")
        return articles

    @staticmethod
    def deduplicate_keep_deepest(
        articles: list[ArticleMetadata],
    ) -> list[ArticleMetadata]:
        """
        Remove artigos duplicados, mantendo o de maior profundidade.
        
        Útil quando um mesmo artigo aparece em múltiplas categorias.
        
        Args:
            articles: Lista de artigos para deduplicar
            
        Returns:
            Lista de artigos únicos
        """
        unique: dict[str, ArticleMetadata] = {}

        for article in articles:
            key = article.article_id
            
            if key in unique:
                # Mantém o de maior profundidade
                if article.depth > unique[key].depth:
                    unique[key] = article
            else:
                unique[key] = article

        deduplicated = list(unique.values())
        
        if len(deduplicated) < len(articles):
            logger.info(
                f"Removidas {len(articles) - len(deduplicated)} duplicatas de artigos"
            )

        return deduplicated