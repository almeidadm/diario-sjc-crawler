#!/usr/bin/env python3
"""Script CLI principal para execução do crawler do Diário Oficial."""

import asyncio
import sys
from datetime import date, datetime
from pathlib import Path

src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from diario_crawler.core import GazetteCrawler, CrawlerConfig
from diario_crawler.storage import ParquetStorage
from diario_crawler.utils import setup_logging, get_logger

logger = get_logger(__name__)


def parse_arguments():
    """Parse argumentos de linha de comando."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Crawler do Diário Oficial de São José dos Campos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Crawler dos últimos 7 dias
  python run_crawler.py --days 7
  
  # Crawler de período específico
  python run_crawler.py --start-date 2025-01-01 --end-date 2025-01-31
  
  # Crawler com output customizado
  python run_crawler.py --days 30 --output-dir /mnt/data/diario --log-level DEBUG
        """
    )
    
    # Grupo de datas
    date_group = parser.add_argument_group('Configurações de Data')
    date_group.add_argument(
        '--start-date',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='Data de início (YYYY-MM-DD)'
    )
    date_group.add_argument(
        '--end-date', 
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').date(),
        help='Data de fim (YYYY-MM-DD)'
    )
    date_group.add_argument(
        '--days',
        type=int,
        default=7,
        help='Número de dias para retroceder a partir de hoje (padrão: 7)'
    )
    
    # Grupo de configuração
    config_group = parser.add_argument_group('Configurações do Crawler')
    config_group.add_argument(
        '--batch-size',
        type=int,
        default=30,
        help='Tamanho do lote de processamento (padrão: 30)'
    )
    config_group.add_argument(
        '--max-concurrent',
        type=int,
        default=10,
        help='Número máximo de requisições concorrentes (padrão: 10)'
    )
    
    # Grupo de output
    output_group = parser.add_argument_group('Configurações de Saída')
    output_group.add_argument(
        '--output-dir',
        type=Path,
        default='data/raw',
        help='Diretório de saída para os dados (padrão: data/raw)'
    )
    output_group.add_argument(
        '--partition-by',
        choices=['day', 'month', 'year'],
        default='day',
        help='Nível de partição dos dados (padrão: day)'
    )
    
    # Grupo de logging
    log_group = parser.add_argument_group('Configurações de Log')
    log_group.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Nível de logging (padrão: INFO)'
    )
    log_group.add_argument(
        '--log-file',
        type=Path,
        help='Arquivo para salvar logs (opcional)'
    )
    
    return parser.parse_args()


def validate_arguments(args):
    """Valida os argumentos fornecidos."""
    errors = []
    
    # Valida datas
    if args.start_date and args.end_date:
        if args.start_date > args.end_date:
            errors.append("Data inicial não pode ser maior que data final")
    
    if args.start_date and args.start_date < date(2022, 8, 15):
        errors.append(f"Data inicial não pode ser anterior a 2025-08-15")
    
    # Valida números
    if args.batch_size <= 0:
        errors.append("Batch size deve ser positivo")
    
    if args.max_concurrent <= 0:
        errors.append("Número de requisições concorrentes deve ser positivo")
    
    if args.days <= 0:
        errors.append("Número de dias deve ser positivo")
    
    if errors:
        for error in errors:
            logger.error(error)
        return False
    
    return True


def calculate_dates(args):
    """Calcula as datas de início e fim baseado nos argumentos."""
    end_date = args.end_date or date.today()
    
    if args.start_date:
        start_date = args.start_date
    else:
        start_date = end_date - timedelta(days=args.days - 1)
    
    return start_date, end_date


async def main():
    """Função principal."""
    args = parse_arguments()
    
    # Configura logging
    setup_logging(level=args.log_level, log_file=args.log_file)
    
    if not validate_arguments(args):
        sys.exit(1)
    
    # Calcula datas
    start_date, end_date = calculate_dates(args)
    
    logger.info("Iniciando crawler do Diário Oficial")
    logger.info(f"Período: {start_date} a {end_date}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Requisições concorrentes: {args.max_concurrent}")
    logger.info(f"Diretório de saída: {args.output_dir}")
    
    try:
        # Configuração do crawler
        config = CrawlerConfig(
            start_date=start_date,
            end_date=end_date,
            batch_size=args.batch_size,
            max_concurrent=args.max_concurrent,
        )
        
        # Storage
        storage = ParquetStorage(
            base_path=args.output_dir,
            partition_by=args.partition_by,
        )
        
        # Crawler
        crawler = GazetteCrawler(config=config, storage=storage)
        
        # Executa e salva
        start_time = datetime.now()
        editions = await crawler.run_and_save()
        end_time = datetime.now()
        
        # Estatísticas
        total_articles = sum(len(edition.articles) for edition in editions)
        execution_time = (end_time - start_time).total_seconds()
        
        logger.info("=" * 50)
        logger.info("EXECUÇÃO CONCLUÍDA")
        logger.info("=" * 50)
        logger.info(f"Edições processadas: {len(editions)}")
        logger.info(f"Artigos processados: {total_articles}")
        logger.info(f"Tempo de execução: {execution_time:.2f} segundos")
        logger.info(f"Taxa: {total_articles/execution_time:.2f} artigos/segundo")
        
        # Log de edições específicas
        for edition in editions:
            logger.info(f" - {edition.edition_id}: {len(edition.articles)} artigos")
            
    except KeyboardInterrupt:
        logger.info("Execução interrompida pelo usuário")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro durante execução: {e}")
        sys.exit(1)


if __name__ == "__main__":
    from datetime import timedelta  # Move import para evitar circular
    
    asyncio.run(main())