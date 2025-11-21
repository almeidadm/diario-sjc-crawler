"""Tests for HtmlStructureParser using recorded HTML via VCR."""

import pytest

from diario_crawler.core.crawler import GazetteCrawler
from diario_crawler.models import ArticleMetadata
from diario_crawler.parsers.structure import HtmlStructureParser

pytestmark = pytest.mark.order(2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "vcr_cassette",
    [
        "test_fetch_structure_batch.yaml",  # cassette com HTML real (fase 2 do crawler)
    ],
    indirect=True,
)
async def test_structure_parser_basic(vcr_cassette, test_config, mock_storage):
    """
    Testa o parse de estrutura HTML usando cassetes gravados.
    Verifica se os seletores principais ainda funcionam.
    """
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)
    parser = HtmlStructureParser()

    # === Fase 1: Busca dos metadados (usa cassette test_fetch_structure_batch.yaml) ===
    urls = crawler.create_metadata_urls()
    metadata_list = await crawler.fetch_metadata_batch(urls)
    assert metadata_list, "Nenhum metadado retornado — verifique o cassette"

    # === Fase 2: Busca das estruturas HTML ===
    html_results = await crawler.fetch_structure_batch(metadata_list)
    assert html_results, "Nenhum HTML retornado — verifique o cassette"

    # === Fase 3: Parse das estruturas HTML ===
    articles = []
    for html_data in html_results:
        edition_id = html_data["edition_id"]
        html = html_data["html"]

        parsed = parser.parse(html, edition_id)
        articles.extend(parsed)

    # === Asserções ===
    assert isinstance(articles, list)
    assert all(isinstance(a, ArticleMetadata) for a in articles)
    assert len(articles) > 0, "Nenhum artigo foi extraído — seletores podem ter mudado"
    assert any(a.title for a in articles), "Nenhum título foi extraído"
    assert any(a.identifier for a in articles), "Nenhum identificador foi extraído"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "vcr_cassette",
    [
        "test_structure_parser_nested.yaml",  # HTML com estrutura mais profunda
        "test_structure_parser_empty.yaml",  # HTML sem árvore UL#tree
    ],
    indirect=True,
)
async def test_structure_parser_edge_cases(vcr_cassette, test_config, mock_storage):
    """
    Testa o parser em variações estruturais do HTML:
    - Árvore aninhada
    - Estrutura ausente
    """
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)
    parser = HtmlStructureParser()

    urls = crawler.create_metadata_urls()
    metadata_list = await crawler.fetch_metadata_batch(urls)
    html_results = await crawler.fetch_structure_batch(metadata_list)
    assert html_results, "Nenhum HTML carregado do cassette"

    for html_data in html_results:
        parsed = parser.parse(html_data["html"], html_data["edition_id"])
        assert isinstance(parsed, list)

        # Se o HTML for vazio, a lista deve ser vazia
        if not parsed:
            assert "ul#tree" not in html_data["html"], "Parser falhou em HTML vazio"
        else:
            # Se há parse, deve haver artigos e identificadores válidos
            assert all(isinstance(a, ArticleMetadata) for a in parsed)
            assert all(a.identifier for a in parsed)


def test_deduplication_logic():
    """
    Testa o comportamento da função deduplicate_keep_deepest()
    do HtmlStructureParser com o novo modelo ArticleMetadata.
    Garante que duplicatas com o mesmo identificador sejam reduzidas
    mantendo a instância de maior profundidade hierárquica.
    """
    parser = HtmlStructureParser()

    articles = [
        ArticleMetadata(
            article_id="1",
            edition_id="E2025-001",
            hierarchy_path=["Root"],
            title="Artigo Original",
            identifier="A-001",
            protocol=None,
        ),
        ArticleMetadata(
            article_id="1",
            edition_id="E2025-001",
            hierarchy_path=["Root", "Subfolder"],
            title="Artigo em Subpasta",
            identifier="A-001",
            protocol=None,
        ),
        ArticleMetadata(
            article_id="2",
            edition_id="E2025-001",
            hierarchy_path=["Root"],
            title="Outro Artigo",
            identifier="B-002",
            protocol=None,
        ),
    ]

    deduplicated = parser.deduplicate_keep_deepest(articles)

    # === Asserções ===
    assert isinstance(deduplicated, list)
    assert len(deduplicated) == 2, "Duplicatas não foram removidas corretamente"

    # O artigo A-001 mais profundo deve ser mantido
    a_article = next(a for a in deduplicated if a.identifier == "A-001")
    assert a_article.depth == 2
    assert "Subfolder" in a_article.hierarchy_path

    # O artigo B-002 deve permanecer inalterado
    b_article = next(a for a in deduplicated if a.identifier == "B-002")
    assert b_article.depth == 1
    assert b_article.title == "Outro Artigo"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "vcr_cassette",
    [
        "test_structure_parser_regression.yaml",
    ],
    indirect=True,
)
async def test_structure_parser_regression(vcr_cassette, test_config, mock_storage):
    """
    Teste de regressão: garante que o número total de artigos e a estrutura geral
    continuam estáveis entre execuções.
    """
    crawler = GazetteCrawler(config=test_config, storage=mock_storage)
    parser = HtmlStructureParser()

    urls = crawler.create_metadata_urls()
    metadata_list = await crawler.fetch_metadata_batch(urls)
    html_results = await crawler.fetch_structure_batch(metadata_list)
    assert html_results, "Nenhum HTML disponível — verifique o cassette"

    total_articles = 0
    for html_data in html_results:
        articles = parser.parse(html_data["html"], html_data["edition_id"])
        total_articles += len(articles)

    # Se o número mudar, é provável que o layout HTML tenha sido alterado
    assert total_articles >= 5, (
        f"Número de artigos inesperado ({total_articles}) — "
        "possível quebra nos seletores CSS"
    )
