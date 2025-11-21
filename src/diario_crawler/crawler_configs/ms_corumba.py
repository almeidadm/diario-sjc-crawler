from datetime import date

from diario_crawler.crawler_configs.base import BaseCrawlerConfig


class MsCorumba(BaseCrawlerConfig):
    NAME = "ms_corumba"
    DEFAULT_START_DATE = date(2012, 6, 26)
    DOMAIN_URL = "https://do.corumba.ms.gov.br"
