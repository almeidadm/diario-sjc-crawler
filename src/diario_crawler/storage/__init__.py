"""Módulo de storage para dados do crawler."""

from .base import BaseStorage
from .parquet_storage import ParquetStorage

__all__ = ["ParquetStorage", "BaseStorage"]
