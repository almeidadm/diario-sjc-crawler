"""Test suite for the GazetteCrawler orchestrator."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from diario_crawler.core.crawler import GazetteCrawler
from diario_crawler.models import ArticleMetadata, GazetteEdition

pytestmark = pytest.mark.order(3)


@pytest.mark.asyncio
async def test_init_default(test_config, mock_storage):
    """Testa inicialização com configurações padrão."""
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)

    assert crawler.config == test_config
    assert crawler.storage == mock_storage
    assert crawler.http_client is not None
    assert crawler.concurrent_client is not None
    assert crawler.metadata_parser is not None
    assert crawler.structure_parser is not None
    assert crawler.content_parser is not None
    assert crawler.data_processor is not None


@pytest.mark.asyncio
async def test_create_metadata_urls(test_config, mock_storage):
    """Garante que as URLs de metadados são geradas corretamente."""
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)
    urls = crawler.create_metadata_urls()

    assert urls, "Nenhuma URL gerada"
    assert all(url.endswith(".json") for url in urls)
    assert all(test_config.METADATA_URL in url for url in urls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "vcr_cassette", ["test_fetch_metadata_batch.yaml"], indirect=True
)
async def test_fetch_metadata_batch(vcr_cassette, test_config, mock_storage):
    """
    Testa a fase de download e parse dos metadados (Fase 1).
    Utiliza o cassette gravado para evitar chamadas reais.
    """
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)
    urls = crawler.create_metadata_urls()

    metadata_list = await crawler.fetch_metadata_batch(urls)

    assert isinstance(metadata_list, list)
    assert all(m.edition_id for m in metadata_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "vcr_cassette", ["test_fetch_structure_batch.yaml"], indirect=True
)
async def test_fetch_structure_batch(vcr_cassette, test_config, mock_storage):
    """Testa a fase 2: busca das estruturas HTML."""
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)
    urls = crawler.create_metadata_urls()

    metadata_list = await crawler.fetch_metadata_batch(urls)
    html_results = await crawler.fetch_structure_batch(metadata_list)

    assert isinstance(html_results, list)
    assert html_results and "html" in html_results[0]
    assert all("edition_id" in r for r in html_results)


def test_parse_articles_from_html(test_config, mock_storage):
    """
    Testa a fase 2.1: parse de HTMLs de estrutura.
    Mocka o HtmlStructureParser para garantir integração.
    """
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)

    fake_articles = [
        ArticleMetadata(
            article_id="1",
            edition_id="E123",
            hierarchy_path=["Root"],
            title="Artigo de Teste",
            identifier="A-001",
            protocol=None,
        ),
        ArticleMetadata(
            article_id="2",
            edition_id="E123",
            hierarchy_path=["Root", "Sub"],
            title="Outro Artigo",
            identifier="A-002",
            protocol=None,
        ),
    ]

    # Mocka parser interno
    mock_parser = MagicMock()
    mock_parser.parse.return_value = fake_articles
    mock_parser.deduplicate_keep_deepest.return_value = fake_articles
    crawler.structure_parser = mock_parser

    html_results = [{"edition_id": "E123", "html": "<ul></ul>"}]
    articles = crawler.parse_articles_from_html(html_results)

    assert len(articles) == 2
    mock_parser.parse.assert_called_once()
    mock_parser.deduplicate_keep_deepest.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_content_batch(test_config, mock_storage):
    """Testa a fase 3: fetch dos conteúdos das matérias."""
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)

    articles = [
        ArticleMetadata(
            article_id="1",
            edition_id="E123",
            hierarchy_path=["Root"],
            title="Matéria de Teste",
            identifier="A-001",
            protocol=None,
        )
    ]

    # Mocka o concurrent client e parser de conteúdo
    crawler.concurrent_client.fetch_all = AsyncMock(
        return_value=[
            MagicMock(text="conteúdo da matéria", url="http://fakeurl", status_code=200)
        ]
    )
    crawler.content_parser.parse = MagicMock(return_value="texto processado")

    results = await crawler.fetch_content_batch(articles)

    assert len(results) == 1
    assert "content" in results[0]
    crawler.content_parser.parse.assert_called_once()


@pytest.mark.asyncio
async def test_process_batch(test_config, mock_storage):
    """Testa o pipeline completo de processamento de um lote."""
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)

    # Mocka as três fases
    crawler.fetch_metadata_batch = AsyncMock(
        return_value=([MagicMock(edition_id="E123", pdf_url="pdf")], {})
    )
    crawler.fetch_structure_batch = AsyncMock(
        return_value=[{"edition_id": "E123", "html": "<ul></ul>", "url": "http"}]
    )
    crawler.parse_articles_from_html = MagicMock(
        return_value=[
            ArticleMetadata(
                article_id="1",
                edition_id="E123",
                hierarchy_path=["Root"],
                title="Artigo",
                identifier="A-001",
                protocol=None,
            )
        ]
    )
    crawler.fetch_content_batch = AsyncMock(
        return_value=[{"article_metadata": MagicMock(), "content": "texto"}]
    )

    # Mocka agregador final
    fake_edition = MagicMock(spec=GazetteEdition)
    crawler.data_processor.aggregate_editions = MagicMock(return_value=[fake_edition])

    result = await crawler.process_batch(["fake_url"])
    assert isinstance(result, list)
    assert len(result) == 1
    crawler.data_processor.aggregate_editions.assert_called_once()


@pytest.mark.asyncio
async def test_run_batched(test_config, mock_storage):
    """Testa a execução em lotes (run_batched)."""
    crawler = GazetteCrawler(test_config, mock_storage)
    fake_edition = MagicMock(spec=GazetteEdition)

    crawler.create_metadata_urls = MagicMock(return_value=["url1", "url2", "url3"])
    crawler.process_batch = AsyncMock(side_effect=[[fake_edition], [fake_edition]])

    batches = []
    async for batch in crawler.run_batched():
        batches.append(batch)

    assert len(batches) >= 1
    assert all(isinstance(b, list) for b in batches)
    crawler.process_batch.assert_called()


@pytest.mark.asyncio
async def test_run(test_config, mock_storage):
    """Testa a execução completa (run), incluindo armazenamento."""
    crawler = GazetteCrawler(test_config, storage=mock_storage)
    fake_edition = MagicMock(spec=GazetteEdition, articles=[MagicMock(), MagicMock()])

    async def fake_batches():
        yield [fake_edition]
        yield [fake_edition]

    crawler.run_batched = fake_batches
    crawler.storage.save_editions = MagicMock()

    n_editions, n_articles = await crawler.run()

    assert n_editions == 2
    assert n_articles == 4
    crawler.storage.save_editions.assert_called()
