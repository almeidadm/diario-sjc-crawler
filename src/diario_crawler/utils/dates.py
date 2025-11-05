"""Utilitários para manipulação de datas."""

from datetime import date, datetime
from typing import Iterable

from dateutil.rrule import DAILY, FR, MO, TH, TU, WE, rrule, rruleset


def get_workdays(
    start: datetime | date,
    end: datetime | date | None = None,
    holidays: Iterable[date] | None = None,
) -> list[date]:
    """
    Retorna todas as datas úteis (segunda a sexta) entre duas datas,
    opcionalmente excluindo feriados específicos.

    Args:
        start: Data inicial do intervalo
        end: Data final do intervalo (usa data atual se None)
        holidays: Conjunto ou lista de feriados a serem excluídos

    Returns:
        Lista de datas úteis entre start e end, excluindo finais de semana
        e (se fornecidos) feriados
    """
    if end is None:
        end = datetime.today().date()

    if isinstance(start, datetime):
        start = start.date()
    if isinstance(end, datetime):
        end = end.date()

    holidays = set(holidays or [])

    rules = rruleset()
    rules.rrule(rrule(DAILY, byweekday=(MO, TU, WE, TH, FR), dtstart=start, until=end))

    for h in holidays:
        rules.exdate(datetime.combine(h, datetime.min.time()))

    return [d.date() for d in rules]


def parse_date(date_str: str, format: str = "%Y-%m-%d") -> date | None:
    """
    Converte string para objeto date.

    Args:
        date_str: String representando a data
        format: Formato da string (padrão: YYYY-MM-DD)

    Returns:
        Objeto date ou None se conversão falhar
    """
    try:
        return datetime.strptime(date_str, format).date()
    except (ValueError, TypeError):
        return None


def format_date(date_obj: date | datetime, format: str = "%Y-%m-%d") -> str:
    """
    Formata objeto date/datetime para string.

    Args:
        date_obj: Objeto date ou datetime
        format: Formato de saída (padrão: YYYY-MM-DD)

    Returns:
        String formatada
    """
    if isinstance(date_obj, datetime):
        date_obj = date_obj.date()
    return date_obj.strftime(format)
