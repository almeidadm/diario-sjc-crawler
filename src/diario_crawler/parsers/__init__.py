"""Módulo de parsers para diferentes fases do crawling."""

from .content import ContentParser
from .metadata import MetadataParser
from .structure import HtmlStructureParser

__all__ = ["ContentParser", "MetadataParser", "HtmlStructureParser"]
