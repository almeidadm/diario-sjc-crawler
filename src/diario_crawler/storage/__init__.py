from diario_crawler.storage.base import StorageBackend
from diario_crawler.storage.local import LocalBackend
from diario_crawler.storage.minio import MinIOBackend
from diario_crawler.storage.parquet import MockStorage, ParquetStorage

__all__ = [
    "StorageBackend",
    "LocalBackend",
    "MinIOBackend",
    "ParquetStorage",
    "MockStorage",
]
