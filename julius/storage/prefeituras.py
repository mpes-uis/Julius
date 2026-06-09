"""Load and filter Prefeitura records from the CSV."""

import csv
from pathlib import Path

from julius.models.prefeitura import Prefeitura, Provider


def load_prefeituras(
    path: Path | None = None,
    *,
    provider: str | None = None,
    slug: str | None = None,
) -> list[Prefeitura]:
    """Read prefeituras.csv and return validated Prefeitura objects."""
    if path is None:
        path = Path(__file__).parent.parent.parent / "data" / "prefeituras.csv"

    results: list[Prefeitura] = []
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                p = Prefeitura.model_validate(row)
            except Exception:
                continue  # skip rows that fail validation (e.g. unknown provider)
            if provider and p.empresa != Provider(provider):
                continue
            if slug and p.slug != slug:
                continue
            results.append(p)

    return results
