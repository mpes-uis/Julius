"""Generate ExtractionJob list from provider + prefeituras + date range."""

from collections.abc import Iterator
from datetime import date

from julius.models.endpoint import Frequency
from julius.models.job import ExtractionJob
from julius.models.prefeitura import Prefeitura
from julius.providers.base import Provider


def _iter_months(start: date, end: date) -> Iterator[tuple[int, int]]:
    """Yield (year, month) tuples from start to end inclusive."""
    year, month = start.year, start.month
    end_year, end_month = end.year, end.month
    while (year, month) <= (end_year, end_month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def _iter_years(start: date, end: date) -> Iterator[tuple[int, None]]:
    """Yield (year, None) tuples from start.year to end.year inclusive."""
    for year in range(start.year, end.year + 1):
        yield year, None


def iter_jobs(
    provider: Provider,
    prefeituras: list[Prefeitura],
    *,
    start: date,
    end: date,
) -> Iterator[ExtractionJob]:
    """Yield one ExtractionJob per (prefeitura, endpoint, period) combination."""
    endpoints = provider.list_endpoints()
    for prefeitura in prefeituras:
        for endpoint in endpoints:
            if endpoint.frequency == Frequency.MONTHLY:
                periods = _iter_months(start, end)
            else:
                periods = _iter_years(start, end)  # type: ignore[assignment]

            for year, month in periods:
                yield ExtractionJob(
                    prefeitura=prefeitura,
                    endpoint=endpoint,
                    year=year,
                    month=month,
                )
