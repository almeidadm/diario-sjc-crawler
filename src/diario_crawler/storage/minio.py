from __future__ import annotations

from io import BytesIO
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from diario_crawler.storage.base import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    StorageBackend,
)
from diario_crawler.utils import get_logger

logger = get_logger(__name__)


class MinIOBackend(StorageBackend):
    """Backend otimizado para MinIO/S3."""

    def __init__(
        self,
        endpoint: str,
        bucket: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool = True,
        region: str = "us-east-1",
        prefix: str = "",
    ):

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.region = region

        # Parse endpoint
        parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
        self.endpoint = parsed.netloc or parsed.path

        # Inicializa cliente MinIO
        self.client = Minio(
            self.endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
        )

        # Garante que bucket existe
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket, location=region)
                logger.info(f"Bucket '{self.bucket}' criado")
        except S3Error as e:
            logger.warning(f"Erro ao verificar/criar bucket: {e}")

        logger.info(
            f"MinIOBackend inicializado: {self.endpoint}/{self.bucket}/{self.prefix}"
        )

    def _full_key(self, path: str) -> str:
        """Retorna chave completa com prefix."""
        path = path.lstrip("/")
        if self.prefix:
            return f"{self.prefix}/{path}"
        return path

    def write_bytes(
        self, path: str, data: bytes, metadata: dict[str, Any] | None = None
    ) -> str:
        key = self._full_key(path)

        # MinIO aceita metadados customizados
        meta = {}
        if metadata:
            for k, v in metadata.items():
                # Converte para string (S3 metadata aceita só strings)
                meta[f"x-amz-meta-{k}"] = str(v)

        self.client.put_object(
            self.bucket,
            key,
            BytesIO(data),
            length=len(data),
            metadata=meta,
        )
        return key

    def read_bytes(self, path: str) -> bytes:
        key = self._full_key(path)
        response = self.client.get_object(self.bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def exists(self, path: str) -> bool:
        key = self._full_key(path)
        try:
            self.client.stat_object(self.bucket, key)
            return True
        except S3Error:
            return False

    def write_parquet(self, path: str, table: pa.Table, **kwargs: Any) -> str:
        # Serializa parquet para memória
        buf = BytesIO()
        pq.write_table(
            table,
            buf,
            compression=kwargs.get("compression", PARQUET_COMPRESSION),
            compression_level=kwargs.get(
                "compression_level", PARQUET_COMPRESSION_LEVEL
            ),
            use_dictionary=kwargs.get("use_dictionary", True),
            write_statistics=kwargs.get("write_statistics", True),
        )

        # Upload
        buf.seek(0)
        key = self._full_key(path)
        self.client.put_object(
            self.bucket,
            key,
            buf,
            length=buf.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        return key

    def read_parquet(self, path: str, columns: list[str] | None = None) -> pa.Table:
        data = self.read_bytes(path)
        buf = BytesIO(data)
        return pq.read_table(buf, columns=columns)

    def list_files(self, prefix: str, suffix: str | None = None) -> list[str]:
        full_prefix = self._full_key(prefix)

        objects = self.client.list_objects(
            self.bucket,
            prefix=full_prefix,
            recursive=True,
        )

        files = []
        for obj in objects:
            key = obj.object_name
            # Remove prefix base
            if self.prefix and key.startswith(self.prefix + "/"):
                key = key[len(self.prefix) + 1 :]

            if suffix is None or key.endswith(suffix):
                files.append(key)

        return sorted(files)

    def get_uri(self, path: str) -> str:
        key = self._full_key(path)
        protocol = "https" if self.client._base_url.is_ssl else "http"
        return f"{protocol}://{self.endpoint}/{self.bucket}/{key}"
