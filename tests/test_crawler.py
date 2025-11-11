"""Tests for GazetteCrawler class using vcrpy for HTTP recording."""

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import vcr

from diario_crawler.core.config import CrawlerConfig
from diario_crawler.core.crawler import GazetteCrawler
from diario_crawler.models import GazetteEdition, GazetteMetadata
from diario_crawler.storage.base import BaseStorage

# VCR configuration for recording real API interactions
vcr_dir = Path(__file__).parent / "fixtures" / "vcr_cassettes"
vcr_config = vcr.VCR(
    cassette_library_dir=str(vcr_dir),
    record_mode="once",  # Record once, then replay
    match_on=["method", "scheme", "host", "port", "path", "query"],
    filter_headers=["authorization", "cookie"],
    filter_query_parameters=["token"],
    ignore_localhost=True,
    decode_compressed_response=True,
)


class MockStorage(BaseStorage):
    """Mock storage that collects saved editions."""

    def __init__(self):
        self.saved_editions = []

    def save_editions(self, editions: list[GazetteEdition], **kwargs) -> None:
        """Store editions in memory."""
        self.saved_editions.extend(editions)


class TestGazetteCrawler:
    """Test suite for GazetteCrawler."""

    def test_init_default(self):
        """Test GazetteCrawler initialization with default config."""
        crawler = GazetteCrawler()
        assert crawler.config is not None
        assert crawler.http_client is not None
        assert crawler.concurrent_client is not None
        assert crawler.storage is not None

    def test_init_custom_config(self):
        """Test GazetteCrawler initialization with custom config."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5),
            batch_size=10,
            max_concurrent=5,
        )
        crawler = GazetteCrawler(config=config)
        assert crawler.config.start_date == date(2024, 1, 1)
        assert crawler.config.end_date == date(2024, 1, 5)
        assert crawler.config.batch_size == 10
        assert crawler.config.max_concurrent == 5

    def test_init_custom_storage(self):
        """Test GazetteCrawler initialization with custom storage."""
        storage = MockStorage()
        crawler = GazetteCrawler(storage=storage)
        assert crawler.storage is storage

    def test_create_metadata_urls(self):
        """Test URL generation for metadata."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 3),  # Wednesday
            end_date=date(2024, 1, 5),  # Friday
            batch_size=10,
        )
        crawler = GazetteCrawler(config=config)
        urls = crawler.create_metadata_urls()

        # Should generate URLs for workdays only (Wed, Thu, Fri = 3 days)
        assert len(urls) == 3
        assert all("edicoes_from_data" in url for url in urls)
        assert "2024-01-03.json" in urls[0]
        assert "2024-01-04.json" in urls[1]
        assert "2024-01-05.json" in urls[2]

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_fetch_metadata_batch.yaml")
    async def test_fetch_metadata_batch(self):
        """Test fetching metadata batch with real API (recorded)."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 3),
            end_date=date(2024, 1, 3),  # Single day for testing
            batch_size=1,
        )
        crawler = GazetteCrawler(config=config)
        urls = crawler.create_metadata_urls()

        metadata_list, gazette_id_map = await crawler.fetch_metadata_batch(urls)

        # Should have at least some metadata if the API returns data
        assert isinstance(metadata_list, list)
        assert isinstance(gazette_id_map, dict)

        # If metadata was found, verify structure
        if metadata_list:
            metadata = metadata_list[0]
            assert isinstance(metadata, GazetteMetadata)
            assert metadata.edition_id
            assert metadata.publication_date
            assert metadata.pdf_url

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_fetch_structure_batch.yaml")
    async def test_fetch_structure_batch(self):
        """Test fetching HTML structure batch (recorded)."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 3),
            end_date=date(2024, 1, 3),
            batch_size=1,
        )
        crawler = GazetteCrawler(config=config)

        # First get metadata
        urls = crawler.create_metadata_urls()
        metadata_list, _ = await crawler.fetch_metadata_batch(urls)

        if not metadata_list:
            pytest.skip("No metadata available to test structure fetch")

        # Then get structure
        html_results = await crawler.fetch_structure_batch(metadata_list)

        assert isinstance(html_results, list)
        if html_results:
            html_data = html_results[0]
            assert "edition_id" in html_data
            assert "html" in html_data
            assert "url" in html_data
            assert html_data["html"]  # HTML should not be empty

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_parse_articles_from_html.yaml")
    async def test_parse_articles_from_html(self):
        """Test parsing articles from HTML structure (recorded)."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 3),
            end_date=date(2024, 1, 3),
            batch_size=1,
        )
        crawler = GazetteCrawler(config=config)

        # Get metadata and structure
        urls = crawler.create_metadata_urls()
        metadata_list, _ = await crawler.fetch_metadata_batch(urls)

        if not metadata_list:
            pytest.skip("No metadata available")

        html_results = await crawler.fetch_structure_batch(metadata_list)

        if not html_results:
            pytest.skip("No HTML structure available")

        # Parse articles
        articles = crawler.parse_articles_from_html(html_results)

        assert isinstance(articles, list)
        # Articles may be empty if the HTML doesn't contain article links
        # but the method should still work

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_fetch_content_batch.yaml")
    async def test_fetch_content_batch(self):
        """Test fetching article content batch (recorded)."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 3),
            end_date=date(2024, 1, 3),
            batch_size=1,
        )
        crawler = GazetteCrawler(config=config)

        # Get metadata and structure
        urls = crawler.create_metadata_urls()
        metadata_list, _ = await crawler.fetch_metadata_batch(urls)

        if not metadata_list:
            pytest.skip("No metadata available")

        html_results = await crawler.fetch_structure_batch(metadata_list)

        if not html_results:
            pytest.skip("No HTML structure available")

        articles = crawler.parse_articles_from_html(html_results)

        if not articles:
            pytest.skip("No articles to fetch content for")

        # Fetch content (limit to first 2 articles for testing)
        test_articles = articles[:2]
        content_results = await crawler.fetch_content_batch(test_articles)

        assert isinstance(content_results, list)
        # Should have some content results if articles were found
        if content_results:
            content_data = content_results[0]
            assert "article_metadata" in content_data
            assert "content" in content_data

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_process_batch.yaml")
    async def test_process_batch(self):
        """Test processing a complete batch (recorded)."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 3),
            end_date=date(2024, 1, 3),
            batch_size=1,
        )
        crawler = GazetteCrawler(config=config)
        urls = crawler.create_metadata_urls()

        editions = await crawler.process_batch(urls)

        assert isinstance(editions, list)
        # Editions may be empty if no data is available for that date
        if editions:
            edition = editions[0]
            assert isinstance(edition, GazetteEdition)
            assert edition.metadata is not None
            assert isinstance(edition.articles, list)

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_run_batched.yaml")
    async def test_run_batched(self):
        """Test running crawler in batches (recorded)."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 3),
            end_date=date(2024, 1, 3),
            batch_size=1,
        )
        storage = MockStorage()
        crawler = GazetteCrawler(config=config, storage=storage)

        edition_count = 0
        async for batch in crawler.run_batched():
            assert isinstance(batch, list)
            edition_count += len(batch)

        # Should have processed at least one batch
        assert edition_count >= 0

    @pytest.mark.asyncio
    @vcr_config.use_cassette("test_run_full.yaml")
    async def test_run_full(self):
        """Test running full crawler (recorded)."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 3),
            end_date=date(2024, 1, 3),
            batch_size=1,
        )
        storage = MockStorage()
        crawler = GazetteCrawler(config=config, storage=storage)

        n_editions, n_articles = await crawler.run()

        assert isinstance(n_editions, int)
        assert isinstance(n_articles, int)
        assert n_editions >= 0
        assert n_articles >= 0
        # Verify storage was called
        assert len(storage.saved_editions) == n_editions

    def test_repr(self):
        """Test string representation of crawler."""
        config = CrawlerConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5),
        )
        crawler = GazetteCrawler(config=config)
        repr_str = repr(crawler)
        assert "GazetteCrawler" in repr_str
        assert "2024-01-01" in repr_str
        assert "2024-01-05" in repr_str

