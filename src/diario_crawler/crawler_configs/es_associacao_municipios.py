from datetime import date

from diario_crawler.crawler_configs.base import BaseCrawlerConfig


class EsAssociacaoMunicipios(BaseCrawlerConfig):
    NAME = "es_associacao_municipios"
    DEFAULT_START_DATE = date(2021, 1, 2)
    DOMAIN_URL = "https://ioes.dio.es.gov.br"
