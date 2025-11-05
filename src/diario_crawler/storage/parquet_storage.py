"""Storage local em formato Parquet."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..models import GazetteEdition, GazetteMetadata, Article, ArticleMetadata, ArticleContent, ContentType
from ..utils import get_logger
from .base import BaseStorage

logger = get_logger(__name__)


class ParquetStorage(BaseStorage):
    """Storage local usando formato Parquet com relações preservadas."""
    
    def __init__(
        self,
        base_path: Path | str = "data/raw",
        partition_by: str = "day",  # "day", "month", "year"
    ):
        """
        Args:
            base_path: Diretório base para armazenamento
            partition_by: Nível de partição temporal
        """
        self.base_path = Path(base_path)
        self.partition_by = partition_by
        self.base_path.mkdir(parents=True, exist_ok=True)
        
    def _editions_to_dataframe(self, editions: list[GazetteEdition]) -> pd.DataFrame:
        """Converte edições para DataFrame."""
        editions_data = []
        
        for edition in editions:
            metadata = edition.metadata
            
            editions_data.append({
                "edition_id": str(metadata.edition_id),
                "publication_date": str(metadata.publication_date),
                "edition_number": int(metadata.edition_number),
                "supplement": bool(metadata.supplement),
                "edition_type_id": int(metadata.edition_type_id),
                "edition_type_name": str(metadata.edition_type_name),
                "pdf_url": str(metadata.pdf_url),
                "total_articles": int(len(edition.articles)),
                "processed_at": str(datetime.now().isoformat()),
                # NOVO: Hash único para correlacionar com artigos
                "edition_hash": self._generate_edition_hash(edition),
            })
            
        return pd.DataFrame(editions_data)
    
    def _articles_to_dataframe(self, editions: list[GazetteEdition]) -> pd.DataFrame:
        """Converte artigos para DataFrame com referência à edição."""
        articles_data = []
        
        for edition in editions:
            edition_hash = self._generate_edition_hash(edition)
            
            for article in edition.articles:
                # Converte conteúdo para formato armazenável
                try:
                    raw_content = (
                        article.raw_content.decode('utf-8', errors='ignore') 
                        if isinstance(article.raw_content, bytes) 
                        else str(article.raw_content)
                    )
                except Exception as e:
                    logger.warning(f"Erro ao decodificar conteúdo do artigo {article.article_id}: {e}")
                    raw_content = ""
                
                content_data = {
                    "raw_content": raw_content,
                    "content_type": str(article.content_type.value),
                    "content_size": int(len(raw_content)),
                }
                
                # CORREÇÃO: Inclui edition_hash para correlacionar
                articles_data.append({
                    "article_id": str(article.article_id),
                    "edition_id": str(article.metadata.edition_id),
                    "edition_hash": edition_hash,  # CHAVE DE CORRELAÇÃO
                    "publication_date": str(edition.publication_date),
                    "title": str(article.title) if article.title else "",
                    "hierarchy_path": json.dumps(article.hierarchy_path) if article.hierarchy_path else "[]",
                    "identifier": str(article.metadata.identifier) if article.metadata.identifier else "",
                    "protocol": str(article.metadata.protocol) if article.metadata.protocol else "",
                    "depth": int(article.depth),
                    "content_data": json.dumps(content_data),
                    "processed_at": str(datetime.now().isoformat()),
                })
                
        return pd.DataFrame(articles_data)
    
    def _generate_edition_hash(self, edition: GazetteEdition) -> str:
        """Gera hash único para uma edição baseado em seus identificadores."""
        import hashlib
        
        content = f"{edition.edition_id}_{edition.publication_date}_{len(edition.articles)}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def _get_partition_path(self, publication_date: str) -> Path:
        """Gera caminho de partição baseado na data."""
        try:
            date_obj = datetime.strptime(publication_date, "%Y-%m-%d").date()
            
            if self.partition_by == "day":
                return Path(f"year={date_obj.year}/month={date_obj.month:02d}/day={date_obj.day:02d}")
            elif self.partition_by == "month":
                return Path(f"year={date_obj.year}/month={date_obj.month:02d}")
            else:  # year
                return Path(f"year={date_obj.year}")
                
        except ValueError:
            today = datetime.now().date()
            return Path(f"year={today.year}/month={today.month:02d}/day={today.day:02d}")
    
    def save_editions(self, editions: list[GazetteEdition], **kwargs: Any) -> None:
        """Salva edições em formato Parquet com relações preservadas."""
        if not editions:
            logger.warning("Nenhuma edição para salvar")
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_id = f"batch_{timestamp}"
        
        try:
            # Salva edições com batch_id para correlacionar
            editions_df = self._editions_to_dataframe(editions)
            if not editions_df.empty:
                editions_df["batch_id"] = batch_id  # NOVO: ID do lote
                editions_path = self.base_path / "gazettes" / f"editions_{timestamp}.parquet"
                editions_path.parent.mkdir(parents=True, exist_ok=True)
                
                self._ensure_dataframe_types(editions_df)
                editions_df.to_parquet(editions_path, index=False)
                logger.info(f"Salvas {len(editions_df)} edições em {editions_path}")
            
            # Salva artigos com mesma batch_id
            articles_df = self._articles_to_dataframe(editions)
            if not articles_df.empty:
                articles_df["batch_id"] = batch_id  # NOVO: Mesmo ID do lote
                self._ensure_dataframe_types(articles_df)
                
                # Agrupa por data de publicação para partição
                for publication_date, group_df in articles_df.groupby('publication_date'):
                    partition_path = self._get_partition_path(publication_date)
                    articles_path = (
                        self.base_path / "articles" / partition_path / 
                        f"articles_{timestamp}.parquet"
                    )
                    articles_path.parent.mkdir(parents=True, exist_ok=True)
                    group_df.to_parquet(articles_path, index=False)
                    
                logger.info(f"Salvos {len(articles_df)} artigos particionados")
            
            # NOVO: Salva relação entre edições e artigos
            self._save_edition_article_relationship(editions, batch_id, timestamp)
                
        except Exception as e:
            logger.error(f"Erro ao salvar dados em Parquet: {e}")
            raise
    
    def _save_edition_article_relationship(self, editions: list[GazetteEdition], batch_id: str, timestamp: str) -> None:
        """Salva tabela de relação entre edições e artigos."""
        relationship_data = []
        
        for edition in editions:
            edition_hash = self._generate_edition_hash(edition)
            
            for article in edition.articles:
                relationship_data.append({
                    "edition_id": str(edition.edition_id),
                    "article_id": str(article.article_id),
                    "edition_hash": edition_hash,
                    "publication_date": str(edition.publication_date),
                    "batch_id": batch_id,
                    "processed_at": str(datetime.now().isoformat()),
                })
        
        if relationship_data:
            relationship_df = pd.DataFrame(relationship_data)
            self._ensure_dataframe_types(relationship_df)
            
            relationship_path = self.base_path / "relationships" / f"edition_articles_{timestamp}.parquet"
            relationship_path.parent.mkdir(parents=True, exist_ok=True)
            relationship_df.to_parquet(relationship_path, index=False)
            logger.info(f"Salvas {len(relationship_df)} relações edição-artigo")
    
    def _ensure_dataframe_types(self, df: pd.DataFrame) -> None:
        """Garante que os tipos do DataFrame sejam compatíveis com Parquet."""
        type_mapping = {
            'object': 'string',
            'bool': 'boolean',
            'int64': 'int64',
            'float64': 'float64',
        }
        
        for column in df.columns:
            dtype = str(df[column].dtype)
            if dtype in type_mapping:
                try:
                    if dtype == 'object':
                        df[column] = df[column].astype('string')
                    elif dtype == 'bool':
                        df[column] = df[column].astype('boolean')
                except Exception as e:
                    logger.warning(f"Erro ao converter coluna {column} de {dtype}: {e}")
    
    def load_edition_with_articles(self, edition_id: str) -> GazetteEdition | None:
        """
        Carrega uma edição completa com todos os seus artigos.
        
        Args:
            edition_id: ID da edição para carregar
            
        Returns:
            GazetteEdition completa ou None se não encontrada
        """
        try:
            # Carrega metadados da edição
            editions_path = self.base_path / "gazettes"
            if not editions_path.exists():
                return None
                
            editions_files = list(editions_path.glob("*.parquet"))
            if not editions_files:
                return None
            
            # Encontra a edição
            edition_df = pd.concat([pd.read_parquet(f) for f in editions_files])
            edition_row = edition_df[edition_df['edition_id'] == edition_id]
            
            if edition_row.empty:
                return None
                
            row = edition_row.iloc[0]
            edition_hash = row['edition_hash']
            
            # Reconstrói GazetteMetadata
            metadata = GazetteMetadata(
                edition_id=str(row['edition_id']),
                publication_date=str(row['publication_date']),
                edition_number=int(row['edition_number']),
                supplement=bool(row['supplement']),
                edition_type_id=int(row['edition_type_id']),
                edition_type_name=str(row['edition_type_name']),
                pdf_url=str(row['pdf_url']),
            )
            
            # Carrega artigos relacionados
            articles = self._load_articles_for_edition(edition_hash)
            
            return GazetteEdition(metadata=metadata, articles=articles)
            
        except Exception as e:
            logger.error(f"Erro ao carregar edição {edition_id}: {e}")
            return None
    
    def _load_articles_for_edition(self, edition_hash: str) -> list[Article]:
        """Carrega todos os artigos de uma edição específica."""
        articles = []
        
        try:
            articles_path = self.base_path / "articles"
            if not articles_path.exists():
                return []
                
            # Procura em todas as partições
            articles_files = list(articles_path.glob("**/*.parquet"))
            if not articles_files:
                return []
            
            articles_df = pd.concat([pd.read_parquet(f) for f in articles_files])
            edition_articles_df = articles_df[articles_df['edition_hash'] == edition_hash]
            
            for _, article_row in edition_articles_df.iterrows():
                try:
                    content_data = json.loads(article_row['content_data'])
                    
                    article_metadata = ArticleMetadata(
                        article_id=str(article_row['article_id']),
                        edition_id=str(article_row['edition_id']),
                        hierarchy_path=json.loads(article_row['hierarchy_path']),
                        title=str(article_row['title']),
                        identifier=str(article_row['identifier']) if article_row['identifier'] else None,
                        protocol=str(article_row['protocol']) if article_row['protocol'] else None,
                    )
                    
                    article_content = ArticleContent(
                        raw_content=content_data['raw_content'],
                        content_type=ContentType(content_data['content_type']),
                    )
                    
                    article = Article(
                        metadata=article_metadata,
                        content=article_content,
                    )
                    articles.append(article)
                    
                except Exception as e:
                    logger.error(f"Erro ao carregar artigo {article_row['article_id']}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Erro ao carregar artigos para edition_hash {edition_hash}: {e}")
            
        return articles
    
    def load_editions(self, **kwargs: Any) -> list[GazetteEdition]:
        """Carrega todas as edições do storage."""
        editions = []
        
        try:
            editions_path = self.base_path / "gazettes"
            if not editions_path.exists():
                return []
                
            editions_files = list(editions_path.glob("*.parquet"))
            if not editions_files:
                return []
            
            editions_df = pd.concat([pd.read_parquet(f) for f in editions_files])
            
            for edition_id in editions_df['edition_id'].unique():
                edition = self.load_edition_with_articles(str(edition_id))
                if edition:
                    editions.append(edition)
                    
        except Exception as e:
            logger.error(f"Erro ao carregar edições: {e}")
            
        return editions