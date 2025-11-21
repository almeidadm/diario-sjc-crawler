"""Pytest configuration and fixtures for crawler tests."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import vcr
from vcr.record_mode import RecordMode

from diario_crawler.crawler_configs.base import BaseCrawlerConfig
from diario_crawler.crawler_configs.es_associacao_municipios import (
    EsAssociacaoMunicipios,
)
from diario_crawler.crawler_configs.ms_corumba import MsCorumba
from diario_crawler.crawler_configs.rj_rio_de_janeiro import RjRioDeJaneiro
from diario_crawler.crawler_configs.ro_jaru import RoJaru
from diario_crawler.crawler_configs.sp_sao_jose_dos_campos import SpSaoJoseDosCampos
from diario_crawler.storage.base import StorageBackend
from diario_crawler.storage.local import LocalBackend

vcr_dir = Path(__file__).parent / "fixtures" / "vcr_cassettes"
vcr_dir.mkdir(parents=True, exist_ok=True)

vcr_config = vcr.VCR(
    cassette_library_dir=str(vcr_dir),
    record_mode=RecordMode.ONCE,
    match_on=["method", "scheme", "host", "port", "path", "query"],
    filter_headers=["authorization", "cookie"],
    filter_query_parameters=["token"],
    ignore_localhost=True,
    decode_compressed_response=True,
)


@pytest.fixture
def vcr_cassette(request):
    """Fixture to use VCR cassettes in tests."""
    cassette_name = getattr(request, "param", None)
    if not cassette_name:
        cassette_name = f"{request.node.name}.yaml"

    with vcr_config.use_cassette(cassette_name):
        yield


@pytest.fixture
def mock_storage() -> StorageBackend:
    """Mock storage that doesn't write to disk."""
    storage = MagicMock(spec=LocalBackend("data/raw"))
    storage.save_editions = MagicMock()
    return storage


@pytest.fixture(
    params=[
        (SpSaoJoseDosCampos, date(2024, 1, 3), date(2024, 1, 5)),
        (EsAssociacaoMunicipios, date(2023, 1, 2), date(2023, 1, 4)),
        (MsCorumba, date(2012, 6, 26), date(2012, 6, 30)),
        (RjRioDeJaneiro, date(2012, 5, 29), date(2012, 6, 4)),
        (RoJaru, date(2022, 1, 1), date(2022, 1, 5)),
    ]
)
def test_config(request) -> BaseCrawlerConfig:
    """Configuração parametrizada do Crawler para múltiplos intervalos."""
    cfg_class, start_date, end_date = request.param
    return cfg_class(
        start_date=start_date,
        end_date=end_date,
        batch_size=5,
        max_concurrent=3,
    )


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Temporary directory for test data."""
    return tmp_path / "test_data"
