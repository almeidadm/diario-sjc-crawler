from datetime import date

from diario_crawler.crawler_configs.base import BaseCrawlerConfig


class RjRioDeJaneiro(BaseCrawlerConfig):
    NAME = "rj_rio_de_janeiro"
    DEFAULT_START_DATE = date(2012, 5, 29)
    DOMAIN_URL = "https://doweb.rio.rj.gov.br"
