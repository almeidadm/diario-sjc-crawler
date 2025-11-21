"""Script CLI principal para execução do crawler do Diário Oficial."""

import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Type

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from diario_crawler.core import GazetteCrawler
from diario_crawler.crawler_configs.base import BaseCrawlerConfig
from diario_crawler.storage import (
    LocalBackend,
    MinIOBackend,
    MockStorage,
    ParquetStorage,
)
from diario_crawler.utils import get_logger, setup_logging

logger = get_logger(__name__)
console = Console()


# Registro de crawlers disponíveis
AVAILABLE_CRAWLERS = {
    "sp_sao_jose_dos_campos": "diario_crawler.crawler_configs.sp_sao_jose_dos_campos.SpSaoJoseDosCampos",
    "rj_rio_de_janeiro": "diario_crawler.crawler_configs.rj_rio_de_janeiro.RjRioDeJaneiro",
    "es_associacao_municipios": "diario_crawler.crawler_configs.es_associacao_municipios.EsAssociacaoMunicipios",
    "ro_jaru": "diario_crawler.crawler_configs.ro_jaru.RoJaru",
    "ms_corumba": "diario_crawler.crawler_configs.ms_corumba.MsCorumba",
}


def load_crawler_config(municipality: str) -> Type[BaseCrawlerConfig]:
    """Carrega dinamicamente a configuração do crawler para o município."""
    if municipality not in AVAILABLE_CRAWLERS:
        available = ", ".join(AVAILABLE_CRAWLERS.keys())
        raise ValueError(
            f"Município '{municipality}' não encontrado. " f"Disponíveis: {available}"
        )

    # Import dinâmico do módulo
    module_path = AVAILABLE_CRAWLERS[municipality]
    module_name, class_name = module_path.rsplit(".", 1)

    try:
        import importlib

        module = importlib.import_module(module_name)
        config_class = getattr(module, class_name)
        return config_class
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Erro ao carregar configuração para '{municipality}': {e}")


def list_available_crawlers():
    """Lista crawlers disponíveis."""
    table = Table(title="🏛️  Crawlers Disponíveis", show_header=True)
    table.add_column("ID", style="cyan", width=30)
    table.add_column("Classe", style="green")

    for crawler_id, class_path in AVAILABLE_CRAWLERS.items():
        class_name = class_path.split(".")[-1]
        table.add_row(crawler_id, class_name)

    console.print(table)
    console.print()


def parse_arguments():
    """Parse argumentos de linha de comando."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Crawler de Diários Oficiais Multi-Município",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Listar crawlers disponíveis
  python run_crawler.py --list-crawlers
  
  # Crawler de São José dos Campos (últimos 7 dias, local)
  python run_crawler.py --municipality sp_sao_jose_dos_campos --days 7
  
  # Crawler do Rio de Janeiro (período específico, MinIO)
  python run_crawler.py \
    --municipality rj_rio_de_janeiro \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --storage minio
  
  # Crawler ES (com output customizado)
  python run_crawler.py \
    --municipality es_associacao_municipios \
    --days 30 \
    --output-dir /mnt/data/diario \
    --log-level DEBUG
  
  # Migrar dados para MinIO
  python run_crawler.py --municipality sp_sao_jose_dos_campos --migrate-to-minio
        """,
    )

    # Seleção de município (obrigatório, exceto para --list-crawlers)
    parser.add_argument(
        "--municipality",
        "--m",
        type=str,
        choices=list(AVAILABLE_CRAWLERS.keys()),
        help="Município/região para crawler (obrigatório)",
    )

    # Utilitários
    parser.add_argument(
        "--list-crawlers",
        action="store_true",
        help="Listar crawlers disponíveis e sair",
    )

    # Grupo de datas
    date_group = parser.add_argument_group("Configurações de Data")
    date_group.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Data de início (YYYY-MM-DD)",
    )
    date_group.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Data de fim (YYYY-MM-DD)",
    )
    date_group.add_argument(
        "--days",
        type=int,
        default=7,
        help="Número de dias para retroceder a partir de hoje (padrão: 7)",
    )

    # Grupo de configuração do crawler
    config_group = parser.add_argument_group("Configurações do Crawler")
    config_group.add_argument(
        "--batch-size",
        type=int,
        default=30,
        help="Tamanho do lote de processamento (padrão: 30)",
    )
    config_group.add_argument(
        "--max-concurrent",
        type=int,
        default=10,
        help="Número máximo de requisições concorrentes (padrão: 10)",
    )

    # Grupo de storage
    storage_group = parser.add_argument_group("Configurações de Storage")
    storage_group.add_argument(
        "--storage",
        choices=["local", "minio", "s3"],
        default=os.getenv("STORAGE_TYPE", "local"),
        help="Tipo de storage (padrão: local ou $STORAGE_TYPE)",
    )
    storage_group.add_argument(
        "--output-dir",
        type=Path,
        default=os.getenv("STORAGE_PATH", "data/raw"),
        help="Diretório de saída (local) (padrão: data/raw ou $STORAGE_PATH)",
    )
    storage_group.add_argument(
        "--partition-by",
        choices=["day", "month", "year"],
        default="day",
        help="Nível de partição dos dados (padrão: day)",
    )

    # Grupo MinIO/S3
    minio_group = parser.add_argument_group("Configurações MinIO/S3")
    minio_group.add_argument(
        "--minio-endpoint",
        default=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        help="Endpoint do MinIO (padrão: localhost:9000 ou $MINIO_ENDPOINT)",
    )
    minio_group.add_argument(
        "--minio-bucket",
        default=os.getenv("MINIO_BUCKET", "gazettes"),
        help="Bucket do MinIO (padrão: gazettes ou $MINIO_BUCKET)",
    )
    minio_group.add_argument(
        "--minio-access-key",
        default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        help="Access key do MinIO (padrão: $MINIO_ACCESS_KEY)",
    )
    minio_group.add_argument(
        "--minio-secret-key",
        default=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        help="Secret key do MinIO (padrão: $MINIO_SECRET_KEY)",
    )
    minio_group.add_argument(
        "--minio-secure",
        action="store_true",
        default=os.getenv("MINIO_SECURE", "false").lower() == "true",
        help="Usar HTTPS para MinIO (padrão: False ou $MINIO_SECURE)",
    )
    minio_group.add_argument(
        "--minio-region",
        default=os.getenv("MINIO_REGION", "us-east-1"),
        help="Região do MinIO/S3 (padrão: us-east-1 ou $MINIO_REGION)",
    )
    minio_group.add_argument(
        "--minio-prefix",
        default=os.getenv("MINIO_PREFIX", ""),
        help='Prefixo para chaves no MinIO (padrão: "" ou $MINIO_PREFIX)',
    )

    # Grupo de DuckDB
    duckdb_group = parser.add_argument_group("Configurações DuckDB")
    duckdb_group.add_argument(
        "--enable-duckdb",
        action="store_true",
        default=os.getenv("ENABLE_DUCKDB", "true").lower() == "true",
        help="Habilitar DuckDB para consultas (padrão: True)",
    )
    duckdb_group.add_argument(
        "--duckdb-path",
        type=Path,
        default=os.getenv("DUCKDB_PATH"),
        help="Caminho para arquivo DuckDB (padrão: in-memory)",
    )

    # Grupo de logging
    log_group = parser.add_argument_group("Configurações de Log")
    log_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Nível de logging (padrão: INFO)",
    )
    log_group.add_argument(
        "--log-file", type=Path, help="Arquivo para salvar logs (opcional)"
    )

    # Grupo de utilitários
    util_group = parser.add_argument_group("Utilitários")
    util_group.add_argument(
        "--migrate-to-minio", action="store_true", help="Migrar dados locais para MinIO"
    )
    util_group.add_argument(
        "--show-stats", action="store_true", help="Mostrar estatísticas do storage"
    )
    util_group.add_argument(
        "--dry-run", action="store_true", help="Simular execução sem salvar dados"
    )

    return parser.parse_args()


def validate_arguments(args) -> bool:
    """Valida os argumentos fornecidos."""
    errors = []

    # Lista crawlers não precisa de outros argumentos
    if args.list_crawlers:
        return True

    # Município é obrigatório para operações principais
    if not args.municipality and not (args.show_stats and args.storage):
        errors.append(
            "Argumento --municipality é obrigatório. "
            "Use --list-crawlers para ver opções disponíveis."
        )
        return False

    # Carrega configuração do município para validar datas
    if args.municipality:
        try:
            ConfigClass = load_crawler_config(args.municipality)
            min_date = ConfigClass.DEFAULT_START_DATE
        except Exception as e:
            errors.append(f"Erro ao carregar configuração: {e}")
            return False
    else:
        # Para show-stats sem município, usa data padrão
        min_date = date(2020, 1, 1)

    # Valida datas
    if args.start_date and args.end_date:
        if args.start_date > args.end_date:
            errors.append("Data inicial não pode ser maior que data final")

    # Valida data mínima
    if args.start_date and args.start_date < min_date:
        errors.append(
            f"Data inicial ({args.start_date}) não pode ser anterior a {min_date} "
            f"para {args.municipality}"
        )

    # Valida números
    if args.batch_size <= 0:
        errors.append("Batch size deve ser positivo")

    if args.max_concurrent <= 0:
        errors.append("Número de requisições concorrentes deve ser positivo")

    if args.days < 0:
        errors.append("Número de dias deve ser não-negativo")

    # Valida configurações de storage
    if args.storage in ["minio", "s3"]:
        if not args.minio_access_key or not args.minio_secret_key:
            errors.append("MinIO/S3 requer access_key e secret_key")

    if errors:
        for error in errors:
            logger.error(f"❌ {error}")
        return False

    return True


def calculate_dates(args) -> tuple[date, date]:
    """Calcula as datas de início e fim baseado nos argumentos."""
    end_date = args.end_date or date.today()

    if args.start_date:
        start_date = args.start_date
    elif args.days > 0:
        start_date = end_date - timedelta(days=args.days - 1)
    else:
        # Se days=0 e sem start_date, usa apenas hoje
        start_date = end_date

    return start_date, end_date


def create_storage_backend(args):
    """Cria o backend de storage apropriado."""
    if args.storage == "local":
        logger.info(f"📁 Usando storage local: {args.output_dir}")
        return LocalBackend(base_path=args.output_dir)

    elif args.storage in ["minio", "s3"]:
        endpoint = args.minio_endpoint

        # Para S3 AWS, ajusta endpoint
        if args.storage == "s3":
            endpoint = f"s3.{args.minio_region}.amazonaws.com"

        logger.info(f"☁️  Usando {args.storage.upper()}: {endpoint}/{args.minio_bucket}")

        try:
            return MinIOBackend(
                endpoint=endpoint,
                bucket=args.minio_bucket,
                access_key=args.minio_access_key,
                secret_key=args.minio_secret_key,
                secure=args.minio_secure,
                region=args.minio_region,
                prefix=args.minio_prefix,
            )
        except ImportError:
            logger.error("❌ MinIO não disponível. Instale: pip install minio")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Erro ao conectar ao MinIO: {e}")
            sys.exit(1)

    else:
        raise ValueError(f"Storage type não suportado: {args.storage}")


def create_storage(args) -> ParquetStorage:
    """Cria instância do ParquetStorage."""
    backend = create_storage_backend(args)

    duckdb_path = str(args.duckdb_path) if args.duckdb_path else None

    storage = ParquetStorage(
        backend=backend,
        partition_by=args.partition_by,
        enable_duckdb=args.enable_duckdb,
        duckdb_path=duckdb_path,
    )

    return storage


def display_config_summary(
    args, config_class: Type[BaseCrawlerConfig], start_date: date, end_date: date
):
    """Exibe resumo da configuração usando Rich."""
    table = Table(title="⚙️  Configuração do Crawler", show_header=False)
    table.add_column("Parâmetro", style="cyan", width=25)
    table.add_column("Valor", style="green")

    # Município
    table.add_row("🏛️  Município", args.municipality)
    table.add_row("🌐 Domínio", config_class.DOMAIN_URL)
    table.add_row("📅 Data mínima", str(config_class.DEFAULT_START_DATE))

    # Período
    table.add_row("", "")  # Separador
    table.add_row("📅 Período", f"{start_date} até {end_date}")
    total_days = (end_date - start_date).days + 1
    table.add_row("📊 Total de dias", str(total_days))

    # Crawler
    table.add_row("", "")
    table.add_row("📦 Batch size", str(args.batch_size))
    table.add_row("⚡ Concorrência", str(args.max_concurrent))

    # Storage
    table.add_row("", "")
    storage_info = args.storage.upper()
    if args.storage == "local":
        storage_info += f" ({args.output_dir})"
    else:
        storage_info += f" ({args.minio_endpoint}/{args.minio_bucket})"
        if args.minio_prefix:
            storage_info += f"/{args.minio_prefix}"
    table.add_row("💾 Storage", storage_info)
    table.add_row("🗂️  Partição", args.partition_by)

    # DuckDB
    table.add_row("", "")
    duckdb_status = "✅ Habilitado" if args.enable_duckdb else "❌ Desabilitado"
    table.add_row("🦆 DuckDB", duckdb_status)

    console.print(table)
    console.print()


def display_results(stats: dict, execution_time: float):
    """Exibe resultados da execução usando Rich."""
    panel = Panel.fit(
        f"""
[bold green]✅ EXECUÇÃO CONCLUÍDA[/bold green]

[cyan]Estatísticas:[/cyan]
  • Edições processadas: [bold]{stats.get('editions', 0)}[/bold]
  • Artigos processados: [bold]{stats.get('articles', 0)}[/bold]
  • Relacionamentos: [bold]{stats.get('relationships', 0)}[/bold]
  • Batch ID: [dim]{stats.get('batch_id', 'N/A')}[/dim]

[cyan]Performance:[/cyan]
  • Tempo total: [bold]{execution_time:.2f}s[/bold]
  • Taxa edições: [bold]{stats.get('editions', 0)/execution_time:.2f}[/bold] ed/s
  • Taxa artigos: [bold]{stats.get('articles', 0)/execution_time:.2f}[/bold] art/s

[cyan]Timestamp:[/cyan]
  • Início: [dim]{stats.get('start_time', 'N/A')}[/dim]
  • Fim: [dim]{stats.get('end_time', 'N/A')}[/dim]
        """,
        title="📊 Resultados",
        border_style="green",
    )
    console.print(panel)


async def migrate_to_minio(args):
    """Migra dados locais para MinIO."""
    console.print("\n[bold cyan]🔄 Iniciando migração Local → MinIO[/bold cyan]\n")

    # Storage origem (local)
    local_backend = LocalBackend(base_path=args.output_dir)

    # Storage destino (MinIO)
    minio_backend = MinIOBackend(
        endpoint=args.minio_endpoint,
        bucket=args.minio_bucket,
        access_key=args.minio_access_key,
        secret_key=args.minio_secret_key,
        secure=args.minio_secure,
        region=args.minio_region,
        prefix=args.minio_prefix,
    )

    datasets = ["gazettes", "articles", "relationships", "content"]
    total_files = 0
    total_bytes = 0

    for dataset in datasets:
        files = local_backend.list_files(
            dataset, suffix=".parquet" if dataset != "content" else None
        )

        if not files:
            console.print(f"  [dim]⊘ {dataset}: sem arquivos[/dim]")
            continue

        console.print(f"  [cyan]📦 {dataset}: {len(files)} arquivos[/cyan]")

        for file_path in files:
            try:
                data = local_backend.read_bytes(file_path)
                minio_backend.write_bytes(file_path, data)
                total_files += 1
                total_bytes += len(data)
            except Exception as e:
                logger.error(f"Erro ao migrar {file_path}: {e}")

    console.print("\n[bold green]✅ Migração concluída![/bold green]")
    console.print(f"  • Arquivos migrados: {total_files}")
    console.print(
        f"  • Bytes transferidos: {total_bytes:,} ({total_bytes/1024/1024:.2f} MB)"
    )


def show_storage_stats(storage: ParquetStorage):
    """Exibe estatísticas do storage."""
    console.print("\n[bold cyan]📊 Estatísticas do Storage[/bold cyan]\n")

    stats = storage.get_stats()

    table = Table(show_header=False)
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor", style="green")

    table.add_row("Backend", stats.get("backend", "N/A"))
    table.add_row("Arquivos de gazettes", str(stats.get("editions_files", 0)))
    table.add_row("Arquivos de articles", str(stats.get("articles_files", 0)))
    table.add_row("Arquivos de content", str(stats.get("content_files", 0)))

    console.print(table)
    console.print()


async def cli():
    """Função principal."""
    args = parse_arguments()

    # Configura logging
    setup_logging(level=args.log_level, log_file=args.log_file)

    # Banner
    console.print(
        Panel.fit(
            "[bold blue]Crawler de Diários Oficiais Multi-Município[/bold blue]\n"
            "[dim]Sistema unificado de coleta de publicações oficiais[/dim]",
            border_style="blue",
        )
    )
    console.print()

    # Lista crawlers disponíveis
    if args.list_crawlers:
        list_available_crawlers()
        return

    # Validação
    if not validate_arguments(args):
        sys.exit(1)

    # Carrega configuração do município
    try:
        ConfigClass = load_crawler_config(args.municipality)
        logger.info(f"✅ Configuração carregada: {ConfigClass.NAME}")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar configuração: {e}")
        sys.exit(1)

    # Cria storage
    try:
        storage = create_storage(args)
    except Exception as e:
        logger.error(f"❌ Erro ao criar storage: {e}")
        sys.exit(1)

    # Utilitários
    if args.show_stats:
        show_storage_stats(storage)
        return

    if args.migrate_to_minio:
        await migrate_to_minio(args)
        return

    # Calcula datas
    start_date, end_date = calculate_dates(args)

    # Exibe configuração
    display_config_summary(args, ConfigClass, start_date, end_date)

    # Confirmação para dry-run
    if args.dry_run:
        console.print("[yellow]⚠️  Modo DRY-RUN: dados não serão salvos[/yellow]\n")

    try:
        # Cria configuração do crawler com a classe específica do município
        config = ConfigClass(
            start_date=start_date,
            end_date=end_date,
            batch_size=args.batch_size,
            max_concurrent=args.max_concurrent,
        )

        # Cria crawler
        crawler = GazetteCrawler(
            config=config, storage=storage if not args.dry_run else MockStorage()
        )

        # Executa
        console.print(
            f"[bold green]🚀 Iniciando crawler para {ConfigClass.NAME}...[/bold green]\n"
        )

        start_time = datetime.now()
        n_editions, n_articles = await crawler.run()
        end_time = datetime.now()

        execution_time = (end_time - start_time).total_seconds()

        # Prepara estatísticas
        if args.dry_run:
            stats = {
                "editions": n_editions,
                "articles": n_articles,
                # 'relationships': sum(len(ed.articles) for ed in editions_list),
                "batch_id": "DRY-RUN",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "municipality": args.municipality,
            }
        else:
            # Pega estatísticas do último batch salvo
            stats = {
                "editions": n_editions,
                "articles": n_articles,
                # 'relationships': sum(len(ed.articles) for ed in editions_list),
                "batch_id": f"batch_{start_time.strftime('%Y%m%d_%H%M%S')}",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "municipality": args.municipality,
            }

        # Exibe resultados
        display_results(stats, execution_time)

        # Mostra estatísticas do storage se solicitado
        if not args.dry_run:
            console.print(
                "\n[dim]💡 Use --show-stats para ver estatísticas detalhadas do storage[/dim]"
            )

    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️  Execução interrompida pelo usuário[/yellow]")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"❌ Erro durante execução: {e}")
        console.print(f"\n[bold red]❌ Erro: {e}[/bold red]")
        sys.exit(1)


def main():
    asyncio.run(cli())
