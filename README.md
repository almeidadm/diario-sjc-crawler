# Diário Oficial Crawler - São José dos Campos

![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)
![Packaging: Poetry](https://img.shields.io/badge/packaging-poetry-cyan.svg)
![Async/Await](https://img.shields.io/badge/async-await-green.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-lightgrey.svg)

Um crawler assíncrono e eficiente para capturar edições e artigos do Diário Oficial de São José dos Campos. Desenvolvido para processamento em larga escala com armazenamento otimizado em formato Parquet. Gerenciado com Poetry para dependências consistentes e empacotamento.
🚀 Características Principais

    Crawler Assíncrono: Processamento concorrente com httpx e asyncio

    Pipeline Trifásico: Metadados → Estrutura HTML → Conteúdo

    Armazenamento Eficiente: Dados salvos em Parquet com partição temporal

    Processamento em Lotes: Suporte a intervalos longos com checkpoint

    Resiliência a Falhas: Mecanismos de retry e tratamento de erros robusto

    CLI Completo: Interface de linha de comando para todas as operações

    Gerenciamento com Poetry: Dependências consistentes e ambiente isolado

📁 Estrutura do Projeto
```text
rag-diario-sjc-crawler/
├── pyproject.toml              # Configuração Poetry e dependências
├── poetry.lock                 # Lock file das dependências
├── src/diario_crawler/         # Código fonte do pacote
│   ├── core/                   # Orquestração principal
│   ├── http/                   # Cliente HTTP
│   ├── parsers/                # Processamento de dados
│   ├── processors/             # Agregação de dados
│   ├── storage/                # Armazenamento
│   ├── utils/                  # Utilitários
│   └── models/                 # Modelos de dados
├── scripts/                    # Scripts executáveis
├── notebooks/                  # Jupyter notebooks para análise
├── tests/                      # Testes unitários
├── data/                       # Dados gerados (não versionado)
└── README.md                   # Este arquivo
```

⚡ Instalação Rápida
Pré-requisitos

    Python 3.11 ou superior

    Poetry instalado globalmente

Instalação com Poetry

```bash
# Clone o repositório
git clone <repository-url>
cd rag-diario-sjc-crawler
```

# Instale as dependências com Poetry
poetry install

# Ative o ambiente virtual
poetry shell

Instalação sem Poetry Shell

```bash
# Execute comandos diretamente sem ativar o shell
poetry run python scripts/run_crawler.py --days 7
```

🎯 Uso Básico
Execução Simples (Últimos 7 dias)
```bash
poetry run python scripts/run_crawler.py --days 7
```

Período Específico
```bash
poetry run python scripts/run_crawler.py --start-date 2025-01-01 --end-date 2025-01-31
```

Com Logs Detalhados
```bash
poetry run python scripts/run_crawler.py --days 30 --log-level DEBUG --log-file logs/crawler.log
```

🔧 Uso Avançado
Processamento em Lotes (Longo Período)
```bash
poetry run python scripts/batch_process.py \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --batch-days 15 \
  --max-retries 5
```

Desenvolvimento e Contribuição
```bash
# Instalar dependências de desenvolvimento
poetry install --with dev

# Executar testes
poetry run pytest

# Formatação de código
poetry run black src/ scripts/
poetry run isort src/ scripts/

# Verificação de tipos
poetry run mypy src/
```

📦 Dependências Principais

O projeto utiliza Poetry para gerenciamento de dependências. Principais pacotes:
Runtime

```toml
httpx = ">=0.24.0"           # Cliente HTTP assíncrono
pandas = ">=2.0.0"           # Manipulação de dados
pyarrow = ">=12.0.0"         # Formato Parquet
selectolax = ">=0.3.0"       # Parsing HTML rápido
python-dateutil = ">=2.8.0"  # Utilitários de data
```

Desenvolvimento
```toml
pytest = ">=7.0.0"           # Testes
black = ">=23.0.0"           # Formatação
isort = ">=5.12.0"           # Organização de imports
mypy = ">=1.0.0"            # Verificação de tipos
jupyter = ">=1.0.0"          # Notebooks para análise
```

🏗️ Estrutura do Código
Módulos Principais
```python
# Crawler principal
from diario_crawler.core import GazetteCrawler, CrawlerConfig

# Clientes HTTP
from diario_crawler.http import HttpClient, ConcurrentHttpClient

# Parsers
from diario_crawler.parsers import MetadataParser, HtmlStructureParser, ContentParser

# Storage
from diario_crawler.storage import ParquetStorage

# Utilitários
from diario_crawler.utils import get_workdays, setup_logging
```

Exemplo de Uso Programático
```python
from datetime import date
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

# Execute com asyncio
import asyncio
asyncio.run(main())
```

💾 Armazenamento de Dados
Estrutura com Poetry

Os dados são armazenados no diretório data/ com a seguinte estrutura:

```text
data/
├── raw/
│   ├── gazettes/            # Metadados das edições
│   ├── articles/            # Artigos particionados por data
│   └── relationships/       # Relações edição-artigo
└── checkpoints/             # Checkpoints para processamento em lote
```

Recuperação de Dados
```python
from diario_crawler.storage import ParquetStorage

storage = ParquetStorage(base_path="data/raw")

# Carregar edição específica com todos os artigos
edition = storage.load_edition_with_articles("2555")

# Carregar todas as edições
all_editions = storage.load_editions()
```

📊 Comandos Úteis do Poetry

```bash
# Adicionar nova dependência
poetry add nome-do-pacote

# Adicionar dependência de desenvolvimento
poetry add --group dev nome-do-pacote

# Atualizar dependências
poetry update

# Verificar ambiente
poetry env info

# Exportar requirements.txt (se necessário)
poetry export -f requirements.txt --output requirements.txt
```

🚀 Deploy e Produção
Build do Pacote
```bash
# Build do pacote distribuível
poetry build

# Instalação em produção (sem dependências de dev)
poetry install --without dev
```

Execução em Produção

```bash
# Usando o ambiente Poetry
poetry run python scripts/run_crawler.py --days 1

# Ou instalando globalmente
pip install .
python scripts/run_crawler.py --days 1
```

🐛 Solução de Problemas
Problemas Comuns

Erro de importação

```bash
# Certifique-se de estar no ambiente Poetry
poetry shell

# Ou use poetry run
poetry run python seu_script.py
```

Dependências faltando

```bash
# Atualize o ambiente
poetry install
```

Logs e Debug

```bash
# Logs detalhados
poetry run python scripts/run_crawler.py --log-level DEBUG

# Log para arquivo
poetry run python scripts/run_crawler.py --log-file crawler.log
```

🤝 Contribuição

    Configuração do Ambiente

```bash
git clone <repo>
cd rag-diario-sjc-crawler
poetry install --with dev
```
    Padrões de Código

```bash
# Formatação automática
poetry run black src/ scripts/ tests/
poetry run isort src/ scripts/ tests/

# Verificação de tipos
poetry run mypy src/
```
Testes

```bash

poetry run pytest
```

📄 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo LICENSE para detalhes.
🆘 Suporte

Em caso de problemas:

    Verifique se o ambiente Poetry está ativo: poetry env info

    Execute com logs detalhados: --log-level DEBUG

    Consulte as issues abertas no repositório

    Crie uma nova issue com detalhes do problema e output do comando poetry env info

Desenvolvido com Poetry para dependências consistentes e ambiente reproduzível 📦🐍
