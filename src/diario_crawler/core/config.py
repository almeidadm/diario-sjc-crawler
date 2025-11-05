"""Configurações e constantes do crawler."""

from datetime import date
from pathlib import Path


class CrawlerConfig:
    """Configurações do crawler."""

    # Datas
    DEFAULT_START_DATE = date(2022, 8, 15)

    # Diretórios
    BASE_DATA_DIR = Path("data")
    METADATA_DIR = BASE_DATA_DIR / "metadata"
    CONTENT_DIR = BASE_DATA_DIR / "content"

    # Limites
    DEFAULT_BATCH_SIZE = 30
    MAX_CONCURRENT_REQUESTS = 10
    MAX_RETRIES = 3

    # URLs base
    METADATA_BASE_URL = "https://diariodomunicipio.sjc.sp.gov.br/apifront/portal/edicoes/edicoes_from_data/"
    HTML_BASE_URL = (
        "https://diariodomunicipio.sjc.sp.gov.br/portal/visualizacoes/view_html_diario/"
    )
    CONTENT_BASE_URL = "https://diariodomunicipio.sjc.sp.gov.br/apifront/portal/edicoes/publicacoes_ver_conteudo/"

    # Timeouts (em segundos)
    CONNECT_TIMEOUT = 10.0
    READ_TIMEOUT = 30.0

    def __init__(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        batch_size: int | None = None,
        max_concurrent: int | None = None,
    ):
        """
        Args:
            start_date: Data inicial do crawling
            end_date: Data final do crawling
            batch_size: Tamanho do lote para processamento
            max_concurrent: Número máximo de requisições concorrentes
        """
        self.start_date = start_date or self.DEFAULT_START_DATE
        self.end_date = end_date or date.today()
        self.batch_size = batch_size or self.DEFAULT_BATCH_SIZE
        self.max_concurrent = max_concurrent or self.MAX_CONCURRENT_REQUESTS

        self._validate_config()

        # Cria diretórios
        # self.METADATA_DIR.mkdir(parents=True, exist_ok=True)
        # self.CONTENT_DIR.mkdir(parents=True, exist_ok=True)

    def _validate_config(self) -> None:
        """Valida as configurações."""
        if self.start_date < self.DEFAULT_START_DATE:
            raise ValueError(
                f"Data inicial ({self.start_date}) não pode ser anterior a {self.DEFAULT_START_DATE}"
            )
        if self.end_date < self.start_date:
            raise ValueError(
                f"Data final ({self.end_date}) deve ser >= data inicial ({self.start_date})"
            )
        if self.batch_size <= 0:
            raise ValueError(f"Batch size deve ser positivo: {self.batch_size}")
        if self.max_concurrent <= 0:
            raise ValueError(
                f"Concorrência máxima deve ser positiva: {self.max_concurrent}"
            )
