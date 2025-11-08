# Diário Oficial Crawler - São José dos Campos

![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)
![Packaging: Poetry](https://img.shields.io/badge/packaging-poetry-cyan.svg)
![Async/Await](https://img.shields.io/badge/async-await-green.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-lightgrey.svg)

Um **crawler assíncrono e eficiente** para capturar edições e artigos do Diário Oficial de São José dos Campos.  
Projetado para **processamento em larga escala**, com **armazenamento otimizado em Parquet/DuckDB** e **análises integradas com Polars e Pandas**.

---

## 🚀 Características Principais

- **Crawler Assíncrono** — uso de `httpx[http2]` e `asyncio` para alta concorrência.  
- **Pipeline Trifásico** — Metadados → Estrutura HTML → Conteúdo completo.  
- **Armazenamento Eficiente** — suporte a `Parquet` e `DuckDB` com particionamento temporal.  
- **Resiliência** — controle de *retries* e logs enriquecidos via `rich`.  
- **CLI Completo** — interface via `scripts/` e integração com `taskipy`.  
- **Análises e Visualização** — integração com `matplotlib`, `seaborn`, `polars` e `pandas`.  
- **Gerenciamento com Poetry** — ambientes isolados e consistentes.  

---

## 📁 Estrutura do Projeto

```text
rag-diario-sjc-crawler/
├── pyproject.toml              # Configuração Poetry e dependências
├── poetry.lock                 # Lock file
├── src/diario_crawler/         # Código-fonte principal
│   ├── core/                   # Orquestração principal
│   ├── http/                   # Cliente HTTP
│   ├── parsers/                # Parsing e extração
│   ├── processors/             # Processamento e agregação
│   ├── storage/                # Armazenamento (Parquet/DuckDB)
│   ├── utils/                  # Funções auxiliares e logging
│   └── models/                 # Estruturas de dados
├── scripts/                    # Scripts executáveis (CLI)
├── notebooks/                  # Notebooks para exploração e análise
├── tests/                      # Testes unitários (TODO)
├── data/                       # Dados locais (não versionados)
└── README.md
```

---

## ⚡ Instalação

### Pré-requisitos

- Python **3.12+**
- Poetry instalado globalmente

### Passos

```bash
# Clone o repositório
git clone <repository-url>
cd rag-diario-sjc-crawler

# Instale dependências
poetry install

# Ative o ambiente virtual
poetry shell
```

Ou execute diretamente sem ativar o shell:

```bash
poetry run python scripts/run_crawler.py --days 7
```

---

## 🎯 Uso Básico

### Últimos 7 dias

```bash
poetry run python scripts/run_crawler.py --days 7
```

### Período Específico

```bash
poetry run python scripts/run_crawler.py --start-date 2025-01-01 --end-date 2025-01-31
```

### Logs Detalhados

```bash
poetry run python scripts/run_crawler.py --days 30 --log-level DEBUG --log-file logs/crawler.log
```

---

## 🔧 Uso Avançado

### Processamento em Lotes

```bash
poetry run python scripts/batch_process.py   --start-date 2025-01-01   --end-date 2025-12-31   --batch-days 15   --max-retries 5
```

### Desenvolvimento

```bash
# Dependências de desenvolvimento
poetry install --with dev

# Lint e formatação
task lint
task format
```

---

## 📦 Dependências Principais

### Runtime

| Pacote         | Descrição                                   |
| -------------- | ------------------------------------------- |
| `httpx[http2]` | Cliente HTTP assíncrono de alta performance |
| `selectolax`   | Parser HTML rápido baseado em lexbor        |
| `rich`         | Saída de logs e console colorido            |
| `pandas`       | Manipulação tabular                         |
| `pyarrow`      | Suporte ao formato Parquet                  |
| `duckdb`       | Engine analítica em memória e on-disk       |
| `polars`       | DataFrame de alto desempenho                |

### Desenvolvimento (`--with dev`)

| Pacote                  | Função                                  |
| ----------------------- | --------------------------------------- |
| `black`                 | Formatação de código                    |
| `isort`                 | Organização de imports                  |
| `flake8`                | Linter                                  |
| `taskipy`               | Definição de tarefas CLI                |
| `matplotlib`, `seaborn` | Visualização                            |
| `tqdm[notebook]`        | Barra de progresso em terminal/notebook |
| `ipykernel`, `notebook` | Ambiente interativo Jupyter             |

---

## 🧩 Tarefas Taskipy

```toml
[tool.taskipy.tasks]
lint = "flake8 src"
format = "isort src && black src"
check = "task format && task lint"
```

Uso:

```bash
task lint    # Verifica código
task format  # Formata código
task check   # Executa lint e formatação
```

---

## 🏗️ Exemplo Programático

```python
from datetime import date
import asyncio
from diario_crawler.core import GazetteCrawler, CrawlerConfig
from diario_crawler.storage import ParquetStorage

async def main():
    config = CrawlerConfig(
        start_date=date(2025, 11, 1),
        end_date=date(2025, 11, 5)
    )
    storage = ParquetStorage()
    crawler = GazetteCrawler(config=config, storage=storage)

    editions = await crawler.run_and_save()
    print(f"Processadas {len(editions)} edições")

asyncio.run(main())
```

---

## 💾 Estrutura de Armazenamento

```text
data/
├── raw/
│   ├── gazettes/        # Metadados das edições
│   ├── articles/        # Artigos particionados por data
│   └── relationships/   # Relações edição-artigo
└── checkpoints/         # Checkpoints de batch
```

### Leitura de Dados

```python
from diario_crawler.storage import ParquetStorage

storage = ParquetStorage(base_path="data/raw")
edition = storage.load_edition_with_articles("2555")
```

---

## 📄 Licença

Este projeto está licenciado sob a **MIT License**.  
Consulte o arquivo `LICENSE` para mais detalhes.
