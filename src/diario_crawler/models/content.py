"""Tipos de conteúdo suportados."""

from enum import Enum


class ContentType(Enum):
    """Tipos de conteúdo que podem ser processados."""

    HTML = "html"
    PDF = "pdf"
    TEXT = "text"
