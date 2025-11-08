"""Modelos para artigos do diário."""

from dataclasses import dataclass

from .content import ContentType


@dataclass
class ArticleMetadata:
    """Metadados de um artigo."""

    article_id: str
    edition_id: str
    hierarchy_path: list[str]
    title: str
    identifier: str
    protocol: str | None = None

    @property
    def depth(self) -> int:
        return len(self.hierarchy_path)

    def __repr__(self) -> str:
        return f"<ArticleMetadata id={self.article_id} title='{self.title}'>"


@dataclass
class ArticleContent:
    """Conteúdo processado de um artigo."""

    raw_content: str | bytes
    content_type: ContentType

    def __repr__(self) -> str:
        return f"<ArticleContent type={self.content_type} size={len(self.raw_content)}>"


@dataclass
class Article:
    """Artigo completo com metadados e conteúdo."""

    metadata: ArticleMetadata
    content: ArticleContent

    @property
    def article_id(self) -> str:
        return self.metadata.article_id

    @property
    def title(self) -> str:
        return self.metadata.title

    @property
    def hierarchy_path(self) -> list[str]:
        return self.metadata.hierarchy_path

    @property
    def depth(self) -> int:
        return self.metadata.depth

    @property
    def raw_content(self) -> str | bytes:
        return self.content.raw_content

    @property
    def content_type(self) -> ContentType:
        return self.content.content_type

    def __repr__(self) -> str:
        return f"<Article id={self.article_id} title='{self.title}'>"
