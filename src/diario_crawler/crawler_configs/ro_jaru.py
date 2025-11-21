from datetime import date

from diario_crawler.crawler_configs.base import BaseCrawlerConfig


class RoJaru(BaseCrawlerConfig):
    NAME = "ro_jaru"
    DEFAULT_START_DATE = date(2022, 1, 1)
    DOMAIN_URL = "https://doe.jaru.ro.gov.br"
