"""Pytest configuration and fixtures for crawler tests."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import vcr

from diario_crawler.core.config import CrawlerConfig
from diario_crawler.storage.base import BaseStorage

# Configure VCR to record HTTP interactions
vcr_dir = Path(__file__).parent / "fixtures" / "vcr_cassettes"
vcr_dir.mkdir(parents=True, exist_ok=True)

# VCR configuration for httpx
vcr_config = vcr.VCR(
    cassette_library_dir=str(vcr_dir),
    record_mode="once",  # Record once, then use recorded responses
    match_on=["method", "scheme", "host", "port", "path", "query"],
    filter_headers=["authorization", "cookie"],  # Don't record sensitive headers
    filter_query_parameters=["token"],  # Don't record sensitive query params
    ignore_localhost=True,
    decode_compressed_response=True,
)


@pytest.fixture
def vcr_cassette(request):
    """Fixture to use VCR cassettes in tests."""
    cassette_name = getattr(request, "param", None)
    if not cassette_name:
        # Generate cassette name from test function name
        cassette_name = f"{request.node.name}.yaml"

    with vcr_config.use_cassette(cassette_name):
        yield


@pytest.fixture
def mock_storage() -> BaseStorage:
    """Mock storage that doesn't write to disk."""
    storage = MagicMock(spec=BaseStorage)
    storage.save_editions = MagicMock()
    return storage


@pytest.fixture
def test_config() -> CrawlerConfig:
    """Test configuration with small date range."""
    from datetime import date, timedelta

    # Use a small date range for testing (2-3 days)
    end_date = date(2024, 1, 5)
    start_date = end_date - timedelta(days=2)

    return CrawlerConfig(
        start_date=start_date,
        end_date=end_date,
        batch_size=5,
        max_concurrent=3,
    )


@pytest.fixture
def temp_data_dir(tmp_path: Path) -> Path:
    """Temporary directory for test data."""
    return tmp_path / "test_data"

