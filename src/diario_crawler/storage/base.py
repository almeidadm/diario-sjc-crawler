"""Interface base para storage."""

from abc import ABC, abstractmethod
from typing import Any

from diario_crawler.models import GazetteEdition


class BaseStorage(ABC):
    """Interface base para sistemas de storage."""

    @abstractmethod
    def save_editions(self, editions: list[GazetteEdition], **kwargs: Any) -> None:
        """Salva edições no storage."""
        pass

    @abstractmethod
    def load_editions(self, **kwargs: Any) -> list[GazetteEdition]:
        """Carrega edições do storage."""
        pass
