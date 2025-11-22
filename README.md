# 📘 Diário SJC Crawler

![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)
![Packaging: Poetry](https://img.shields.io/badge/packaging-poetry-cyan.svg)
![Async/Await](https://img.shields.io/badge/async-await-green.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-lightgrey.svg)


Diário SJC Crawler é um crawler assíncrono multi-município para coleta estruturada de Diários Oficiais no Brasil.
A aplicação é baseada no formato de disponibilização dos Diários Oficiais fornecido pela IONews (https://ionews.com.br/), cuja publicação segue um modelo hierárquico em HTML, permitindo extração sistemática de seções, categorias e conteúdo textual.

Ele foi projetado para uso em pipelines de ETL, RAG e análise documental em larga escala.

**Exemplo:**

- São José dos Campos: https://diariodomunicipio.sjc.sp.gov.br/

---

## A aplicação fornece:

Coleta paralela de edições por período ou janela móvel.

Armazenamento local ou em MinIO/S3 com particionamento.

Suporte opcional a DuckDB para consultas rápidas.

CLI robusto para orquestração de crawlers específicos por município.

A interface oficial do projeto é fornecida via entrypoint Poetry:

```bash
cli  # mapeado para diario_crawler.cli.run_crawler:main
```

---

## 🚀 Instalação

```bash
git clone https://github.com/almeidadm/diario-sjc-crawler
cd diario-sjc-crawler
poetry install
```

---

## 🧭 Uso Básico

Liste os crawlers disponíveis:

```bash
cli --list-crawlers
```
Execute para um município específico (últimos 7 dias):
```bash
cli --municipality sp_sao_jose_dos_campos --days 7
```
Baixe um período específico e salve no MinIO:
```bash
cli \
  --municipality rj_rio_de_janeiro \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --storage minio
```

Migrar dados locais para MinIO:
```bash
cli --municipality sp_sao_jose_dos_campos --migrate-to-minio
```

---

## ⚙️ Principais Parâmetros

Município

```bash
--municipality {sp_sao_jose_dos_campos,rj_rio_de_janeiro,es_associacao_municipios,ro_jaru,ms_corumba}
```

Janela temporal

- --start-date YYYY-MM-DD

- --end-date YYYY-MM-DD

- --days N (padrão: 7)

Concorrência e desempenho

- --batch-size (padrão: 30)

- --max-concurrent (padrão: 10)

Armazenamento

Local, MinIO ou S3 (--storage)

- --output-dir para storage local

- --partition-by {day,month,year}

MinIO/S3

Inclui endpoint, bucket, credenciais e prefixos configuráveis.

DuckDB

- --enable-duckdb

- --duckdb-path (arquivo ou in-memory)

## 📂 Estrutura e Dependências

O projeto é organizado como um pacote Poetry:

Pacote principal: diario_crawler/ (definido em [tool.poetry])

CLI: diario_crawler.cli.run_crawler

Dependências principais:

httpx[http2] para requisições assíncronas

selectolax para parsing HTML eficiente

pandas, polars, pyarrow, duckdb para processamento tabular

typer para a interface CLI

vcrpy para testes reprodutíveis

Ambiente de desenvolvimento inclui: pytest, flake8, black, isort, matplotlib, seaborn, entre outros.

## 🧪 Testes

Execute toda a suíte:
```bash
task test
```

## 📄 Licença
Este projeto está licenciado sob a MIT License.
Consulte o arquivo LICENSE para mais detalhes.
