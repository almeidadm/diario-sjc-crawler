from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa

from diario_crawler.utils import get_logger

logger = get_logger(__name__)


# ============================================================================
# Configurações e Constantes
# ============================================================================


PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 9

# Schema explícito para melhor performance e compatibilidade
EDITIONS_SCHEMA = pa.schema(
    [
        ("municipality", pa.string()),
        ("edition_id", pa.string()),
        ("publication_date", pa.date32()),
        ("edition_number", pa.int32()),
        ("supplement", pa.bool_()),
        ("edition_type_id", pa.int32()),
        ("edition_type_name", pa.string()),
        ("pdf_url", pa.string()),
        ("total_articles", pa.int32()),
        ("processed_at", pa.timestamp("us")),
        ("edition_hash", pa.string()),
        ("batch_id", pa.string()),
        ("year", pa.int32()),
        ("month", pa.int32()),
        ("day", pa.int32()),
    ]
)

ARTICLES_SCHEMA = pa.schema(
    [
        ("municipality", pa.string()),
        ("article_id", pa.string()),
        ("edition_id", pa.string()),
        ("edition_hash", pa.string()),
        ("publication_date", pa.date32()),
        ("title", pa.string()),
        ("hierarchy_path", pa.string()),  # JSON serializado
        ("identifier", pa.string()),
        ("protocol", pa.string()),
        ("depth", pa.int32()),
        ("content_type", pa.string()),
        ("content_size", pa.int64()),
        ("content_hash", pa.string()),
        ("content_path", pa.string()),
        ("inline_text", pa.string()),
        ("processed_at", pa.timestamp("us")),
        ("batch_id", pa.string()),
        ("year", pa.int32()),
        ("month", pa.int32()),
        ("day", pa.int32()),
    ]
)


# ============================================================================
# Interfaces e Classes Base
# ============================================================================


class StorageBackend(ABC):
    """Interface abstrata para backends de armazenamento."""

    @abstractmethod
    def write_bytes(
        self, path: str, data: bytes, metadata: dict[str, Any] | None = None
    ) -> str:
        """Escreve bytes e retorna o path/URI final."""
        pass

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Lê bytes de um path/URI."""
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Verifica se path existe."""
        pass

    @abstractmethod
    def write_parquet(self, path: str, table: pa.Table, **kwargs: Any) -> str:
        """Escreve tabela Parquet."""
        pass

    @abstractmethod
    def read_parquet(self, path: str, columns: list[str] | None = None) -> pa.Table:
        """Lê tabela Parquet."""
        pass

    @abstractmethod
    def list_files(self, prefix: str, suffix: str | None = None) -> list[str]:
        """Lista arquivos com prefixo opcional."""
        pass

    @abstractmethod
    def get_uri(self, path: str) -> str:
        """Retorna URI completo para o path."""
        pass
