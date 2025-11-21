from datetime import date

from diario_crawler.crawler_configs.base import BaseCrawlerConfig


class SpSaoJoseDosCampos(BaseCrawlerConfig):
    NAME = "sp_sao_jose_dos_campos"
    DEFAULT_START_DATE = date(2022, 8, 15)
    DOMAIN_URL = "https://diariodomunicipio.sjc.sp.gov.br"
