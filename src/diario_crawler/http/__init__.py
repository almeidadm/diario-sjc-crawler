"""Módulo HTTP para cliente e operações concorrentes."""

from .client import HttpClient
from .concurrent import ConcurrentHttpClient

__all__ = ["HttpClient", "ConcurrentHttpClient"]
