"""Modelos para edições do diário."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .article import Article


@dataclass
class GazetteMetadata:
    """Metadados de uma edição do diário."""

    edition_id: str
    publication_date: str
    edition_number: int
    supplement: bool
    edition_type_id: int
    edition_type_name: str
    pdf_url: str

    def __repr__(self) -> str:
        return f"<GazetteMetadata id={self.edition_id} date={self.publication_date}>"


@dataclass
class GazetteEdition:
    """Edição completa do diário com seus artigos."""

    metadata: GazetteMetadata
    articles: list["Article"] = field(default_factory=list)

    @property
    def edition_id(self) -> str:
        return self.metadata.edition_id

    @property
    def publication_date(self) -> str:
        return self.metadata.publication_date

    @property
    def total_articles(self) -> int:
        return len(self.articles)

    def __repr__(self) -> str:
        return (
            f"<GazetteEdition id={self.edition_id} "
            f"date={self.publication_date} "
            f"articles={self.total_articles}>"
        )
