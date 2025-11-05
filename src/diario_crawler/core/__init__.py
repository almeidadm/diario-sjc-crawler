"""Módulo core do crawler."""

from .config import CrawlerConfig
from .crawler import GazetteCrawler

__all__ = ["CrawlerConfig", "GazetteCrawler"]
