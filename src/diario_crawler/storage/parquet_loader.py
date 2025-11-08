"""Classe de leitura otimizada (Parquet + DuckDB + Polars) com suporte a filtros de data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import duckdb
import polars as pl

from ..models import (
    Article,
    ArticleContent,
    ArticleMetadata,
    ContentType,
    GazetteEdition,
    GazetteMetadata,
)
from ..utils import get_logger

logger = get_logger(__name__)


class ParquetLoader:
    """Responsável por carregar edições e artigos armazenados em Parquet."""

    def __init__(
        self, base_path: Path | str = "data/raw", duckdb_path: str | None = None
    ):
        self.base_path = Path(base_path)
        self.duckdb_path = duckdb_path or ":memory:"
        self._duck_conn = duckdb.connect(self.duckdb_path)

    def _duck_query_to_polars(self, sql: str) -> pl.DataFrame | pl.Series:
        """Executa query DuckDB e converte resultado em Polars DataFrame."""
        try:
            table = self._duck_conn.execute(sql).arrow()
            return pl.from_arrow(table)
        except Exception as e:
            logger.error(f"Erro ao executar query DuckDB: {e}\nSQL: {sql}")
            return pl.DataFrame()

    def _build_date_filter(self, start_date: str | None, end_date: str | None) -> str:
        """Gera cláusula SQL WHERE para filtrar por intervalo de datas."""
        filters = []
        if start_date:
            filters.append(f"publication_date >= '{start_date}'")
        if end_date:
            filters.append(f"publication_date <= '{end_date}'")
        return " AND ".join(filters)

    def load_edition_with_articles(self, edition_id: str) -> GazetteEdition | None:
        """Carrega uma edição e todos os seus artigos."""
        try:
            gazettes_glob = str(self.base_path / "gazettes" / "**" / "*.parquet")
            articles_glob = str(self.base_path / "articles" / "**" / "*.parquet")

            q_edition = f"""
                SELECT * FROM read_parquet('{gazettes_glob}')
                WHERE edition_id = '{edition_id}'
                LIMIT 1
            """
            edition_df = self._duck_query_to_polars(q_edition)

            if edition_df.is_empty():
                logger.debug(f"Edição {edition_id} não encontrada.")
                return None

            row = edition_df.to_dicts()[0]
            metadata = GazetteMetadata(
                edition_id=str(row["edition_id"]),
                publication_date=str(row["publication_date"]),
                edition_number=int(row["edition_number"]),
                supplement=bool(row["supplement"]),
                edition_type_id=int(row["edition_type_id"]),
                edition_type_name=str(row["edition_type_name"]),
                pdf_url=str(row.get("pdf_url", "")),
            )
            edition_hash = str(row["edition_hash"])

            q_articles = f"""
                SELECT * FROM read_parquet('{articles_glob}', union_by_name=True)
                WHERE edition_hash = '{edition_hash}'
                ORDER BY depth, article_id
            """
            articles_df = self._duck_query_to_polars(q_articles)

            articles: list[Article] = []
            for row in articles_df.to_dicts():
                hierarchy_path = json.loads(row.get("hierarchy_path") or "[]")
                identifier = row.get("identifier") or None
                protocol = row.get("protocol") or None

                article_metadata = ArticleMetadata(
                    article_id=str(row["article_id"]),
                    edition_id=str(row["edition_id"]),
                    hierarchy_path=hierarchy_path,
                    title=str(row.get("title") or ""),
                    identifier=str(identifier),
                    protocol=str(protocol),
                )

                inline_text = row.get("inline_text")
                raw_content = None
                if inline_text:
                    raw_content = inline_text
                else:
                    content_path = row.get("content_path")
                    if content_path:
                        full_path = self.base_path / content_path
                        try:
                            content_bytes = full_path.read_bytes()
                            try:
                                raw_content = content_bytes.decode("utf-8")
                            except UnicodeDecodeError:
                                raw_content = content_bytes
                        except Exception as e:
                            logger.warning(f"Erro lendo conteúdo {full_path}: {e}")
                            raw_content = ""
                    else:
                        raw_content = ""

                content_type_val = row.get("content_type")
                content_type = (
                    ContentType(content_type_val)
                    if content_type_val in ContentType._value2member_map_
                    else ContentType.TEXT
                )

                article_content = ArticleContent(
                    raw_content=raw_content, content_type=content_type
                )
                articles.append(
                    Article(metadata=article_metadata, content=article_content)
                )

            return GazetteEdition(metadata=metadata, articles=articles)

        except Exception as e:
            logger.error(f"Erro ao carregar edição {edition_id}: {e}")
            return None

    def load_editions(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> list[GazetteEdition]:
        """
        Carrega edições filtradas por intervalo de datas (inclusive artigos).
        """
        editions: list[GazetteEdition] = []
        try:
            gazettes_glob = str(self.base_path / "gazettes" / "**" / "*.parquet")
            where_clause = self._build_date_filter(start_date, end_date)
            where_sql = f"WHERE {where_clause}" if where_clause else ""

            q = f"""
                SELECT DISTINCT edition_id
                FROM read_parquet('{gazettes_glob}')
                {where_sql}
                ORDER BY publication_date
            """
            df = self._duck_query_to_polars(q)
            edition_ids = df["edition_id"].to_list()

            for eid in edition_ids:
                edition = self.load_edition_with_articles(str(eid))
                if edition:
                    editions.append(edition)

        except Exception as e:
            logger.error(f"Erro ao listar/carregar edições: {e}")
        return editions

    def iter_editions(
        self, start_date: str | None = None, end_date: str | None = None
    ) -> Iterator[GazetteEdition]:
        """
        Iterador lazy para grandes volumes de dados, com filtro opcional de datas.
        """
        try:
            gazettes_glob = str(self.base_path / "gazettes" / "**" / "*.parquet")
            where_clause = self._build_date_filter(start_date, end_date)
            where_sql = f"WHERE {where_clause}" if where_clause else ""

            q = f"""
                SELECT DISTINCT edition_id
                FROM read_parquet('{gazettes_glob}')
                {where_sql}
                ORDER BY publication_date
            """
            df = self._duck_query_to_polars(q)

            for eid in df["edition_id"].to_list():
                edition = self.load_edition_with_articles(str(eid))
                if edition:
                    yield edition
        except Exception as e:
            logger.error(f"Erro ao iterar edições: {e}")

    def close(self) -> None:
        """Fecha conexão DuckDB."""
        try:
            self._duck_conn.close()
        except Exception:
            pass
