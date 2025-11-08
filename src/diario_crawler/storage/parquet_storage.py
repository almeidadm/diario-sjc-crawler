"""Storage local otimizado com PyArrow (parquet particionado), DuckDB (consulta seletiva)
e armazenamento de blobs de conteúdo em arquivos externos referenciados por hash.

Decisões:
- Particionamento por year/month/day (Hive-style) para pruning eficiente.
- Conteúdo textual/binary grande armazenado em data/raw/content/<hash>.txt (ou .bin).
- Metadados em Parquet compactados com ZSTD e encoding por dicionário quando possível.
- Leitura seletiva usando DuckDB (executa SQL sobre arquivos parquet).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from ..models import ContentType, GazetteEdition
from ..utils import get_logger
from .base import BaseStorage

logger = get_logger(__name__)


CONTENT_INLINE_THRESHOLD = 2000
PARQUET_COMPRESSION = "zstd"
CONTENT_DIRNAME = "content"


class ParquetStorage(BaseStorage):
    """Storage otimizado: escreve Parquet particionado via PyArrow, consulta via DuckDB."""

    def __init__(
        self,
        base_path: Path | str = "data/raw",
        partition_by: str = "day",  # "day", "month", "year"
        duckdb_path: Path | str | None = None,
    ):
        self.base_path = Path(base_path)
        self.partition_by = partition_by
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Diretório para blobs de conteúdo (textos grandes, pdfs)
        self.content_path = self.base_path / CONTENT_DIRNAME
        self.content_path.mkdir(parents=True, exist_ok=True)

        # DuckDB: catálogo local (file-backed DB opcional)
        # Se duckdb_path for None, usa in-memory catalog; para persistência, informe um arquivo.
        self.duckdb_path = str(duckdb_path) if duckdb_path else ":memory:"
        self._duck_conn = duckdb.connect(self.duckdb_path)

        logger.debug(
            f"ParquetStorage iniciado em {self.base_path} (duckdb={self.duckdb_path})"
        )

    # -----------------------
    # Helpers
    # -----------------------
    def _generate_edition_hash(self, edition: GazetteEdition) -> str:
        content = f"{edition.metadata.edition_id}_{edition.metadata.publication_date}_{len(edition.articles)}"
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    def _write_content_blob(self, raw: str | bytes) -> dict[str, Any]:
        """Se necessário, persiste conteúdo pesado em disco e retorna meta (path, size, hash)."""
        if isinstance(raw, str):
            raw_bytes = raw.encode("utf-8")
        else:
            raw_bytes = raw

        size = len(raw_bytes)
        content_hash = hashlib.sha256(raw_bytes).hexdigest()
        filename = f"{content_hash}.bin"
        path = self.content_path / filename

        # Escreve só se ainda não existir (idempotente)
        if not path.exists():
            path.write_bytes(raw_bytes)

        return {
            "content_hash": content_hash,
            "content_path": str(path.relative_to(self.base_path)),
            "content_size": size,
        }

    def _publication_date_parts(self, publication_date: str) -> dict[str, int]:
        """Retorna year/month/day extraídos da data (espera YYYY-MM-DD)."""
        try:
            d = datetime.strptime(publication_date, "%Y-%m-%d").date()
            return {"year": d.year, "month": d.month, "day": d.day}
        except Exception:
            today = datetime.now().date()
            return {"year": today.year, "month": today.month, "day": today.day}

    # -----------------------
    # Serialização (escrita)
    # -----------------------
    def save_editions(self, editions: list[GazetteEdition], **kwargs: Any) -> None:
        """
        Escreve edições e artigos:
         - metadados das edições -> data/raw/gazettes/ (parquet)
         - artigos (sem blobs grandes inline) -> data/raw/articles/ (parquet, particionado por date)
         - conteúdo pesado -> data/raw/content/<hash>.bin
        """
        if not editions:
            logger.warning("Nenhuma edição para salvar")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_id = f"batch_{timestamp}"

        editions_rows: list[dict[str, Any]] = []
        articles_rows: list[dict[str, Any]] = []
        relationships_rows: list[dict[str, Any]] = []

        for edition in editions:
            meta = edition.metadata
            edition_hash = self._generate_edition_hash(edition)
            pub_parts = self._publication_date_parts(meta.publication_date)

            editions_rows.append(
                {
                    "edition_id": str(meta.edition_id),
                    "publication_date": meta.publication_date,
                    "edition_number": int(meta.edition_number),
                    "supplement": bool(meta.supplement),
                    "edition_type_id": int(meta.edition_type_id),
                    "edition_type_name": str(meta.edition_type_name),
                    "pdf_url": str(meta.pdf_url),
                    "total_articles": len(edition.articles),
                    "processed_at": datetime.now().isoformat(),
                    "edition_hash": edition_hash,
                    "batch_id": batch_id,
                    "year": pub_parts["year"],
                    "month": pub_parts["month"],
                    "day": pub_parts["day"],
                }
            )

            for article in edition.articles:
                # Normaliza/obtém conteúdo (de dataclass Article)
                # Suporta Article.content.raw_content (str/bytes) e Article.content.content_type
                raw_content = article.content.raw_content
                if isinstance(raw_content, bytes):
                    try:
                        # tenta decodificar para UTF-8; se falhar, mantemos bytes e armazenamos em binário
                        raw_text = raw_content.decode("utf-8")
                        raw_bytes = raw_content
                    except Exception:
                        raw_text = None
                        raw_bytes = raw_content
                else:
                    raw_text = str(raw_content) if raw_content is not None else ""
                    raw_bytes = raw_text.encode("utf-8")

                # Se conteúdo for grande, salva blob e referencia
                if raw_text is None:
                    content_meta = self._write_content_blob(raw_bytes)
                    inline_text = None
                else:
                    if len(raw_text) > CONTENT_INLINE_THRESHOLD:
                        content_meta = self._write_content_blob(raw_text)
                        inline_text = None
                    else:
                        content_meta = {
                            "content_hash": None,
                            "content_path": None,
                            "content_size": len(raw_bytes),
                        }
                        inline_text = raw_text

                hierarchy = getattr(article.metadata, "hierarchy_path", []) or []
                identifier = getattr(article.metadata, "identifier", None)
                protocol = getattr(article.metadata, "protocol", None)
                depth = len(hierarchy)

                pub_date = meta.publication_date
                pub_parts = self._publication_date_parts(pub_date)

                articles_rows.append(
                    {
                        "article_id": str(article.metadata.article_id),
                        "edition_id": str(article.metadata.edition_id),
                        "edition_hash": edition_hash,
                        "publication_date": pub_date,
                        "title": getattr(article.metadata, "title", "") or "",
                        "hierarchy_path": json.dumps(hierarchy),
                        "identifier": str(identifier) if identifier else None,
                        "protocol": str(protocol) if protocol else None,
                        "depth": depth,
                        "content_type": (
                            article.content.content_type.value
                            if isinstance(article.content.content_type, ContentType)
                            else str(article.content.content_type)
                        ),
                        "content_size": content_meta.get("content_size", 0),
                        "content_hash": content_meta.get("content_hash"),
                        "content_path": content_meta.get("content_path"),
                        "inline_text": inline_text,
                        "processed_at": datetime.now().isoformat(),
                        "batch_id": batch_id,
                        "year": pub_parts["year"],
                        "month": pub_parts["month"],
                        "day": pub_parts["day"],
                    }
                )

                relationships_rows.append(
                    {
                        "edition_id": str(meta.edition_id),
                        "article_id": str(article.metadata.article_id),
                        "edition_hash": edition_hash,
                        "publication_date": meta.publication_date,
                        "batch_id": batch_id,
                        "processed_at": datetime.now().isoformat(),
                    }
                )

        # --- Escreve usando PyArrow dataset (particiona por year/month/day) ---
        try:
            # Escreve editions (particiona por year/month para reduzir granularidade)
            if editions_rows:
                editions_table = pa.Table.from_pylist(editions_rows)
                editions_out = self.base_path / "gazettes"
                ds.write_dataset(
                    data=editions_table,
                    base_dir=str(editions_out),
                    format="parquet",
                    partitioning=["year", "month"],
                    existing_data_behavior="overwrite_or_ignore",
                    file_options=ds.ParquetFileFormat().make_write_options(
                        compression=PARQUET_COMPRESSION
                    ),
                )
                logger.info(f"Gravadas {len(editions_rows)} edições em {editions_out}")

            # Escreve articles (particiona por year/month/day para bom pruning)
            if articles_rows:
                articles_table = pa.Table.from_pylist(articles_rows)
                articles_out = self.base_path / "articles"
                ds.write_dataset(
                    data=articles_table,
                    base_dir=str(articles_out),
                    format="parquet",
                    partitioning=["year", "month", "day"],
                    existing_data_behavior="overwrite_or_ignore",
                    file_options=ds.ParquetFileFormat().make_write_options(
                        compression=PARQUET_COMPRESSION
                    ),
                )
                logger.info(f"Gravados {len(articles_rows)} artigos em {articles_out}")

            # Escreve relationships (simples parquet sem partição)
            if relationships_rows:
                rel_table = pa.Table.from_pylist(relationships_rows)
                rel_out = self.base_path / "relationships"
                rel_out.mkdir(parents=True, exist_ok=True)
                rel_path = rel_out / f"edition_article_rel_{timestamp}.parquet"
                pq.write_table(
                    rel_table, str(rel_path), compression=PARQUET_COMPRESSION
                )
                logger.info(
                    f"Gravadas {len(relationships_rows)} relações em {rel_path}"
                )

        except Exception as exc:
            logger.error(f"Erro ao salvar datasets: {exc}")
            raise
