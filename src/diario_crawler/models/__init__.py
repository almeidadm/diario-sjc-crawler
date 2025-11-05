"""Modelos de dados do crawler."""

from .article import Article, ArticleContent, ArticleMetadata
from .content import ContentType
from .gazette import GazetteEdition, GazetteMetadata

__all__ = [
    "GazetteEdition",
    "GazetteMetadata",
    "Article",
    "ArticleMetadata",
    "ArticleContent",
    "ContentType",
]
