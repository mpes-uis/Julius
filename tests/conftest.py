"""Shared pytest fixtures."""

import pytest

from julius.models.prefeitura import Prefeitura, Provider
from julius.storage.duckdb_store import DuckDBStore


@pytest.fixture
def in_memory_store() -> DuckDBStore:
    return DuckDBStore(":memory:")


@pytest.fixture
def sample_prefeitura() -> Prefeitura:
    return Prefeitura(
        id=1,
        prefeitura="Prefeitura Municipal de Afonso Cláudio",
        municipio="afonso claudio",
        url="https://afonsoclaudio-es.portaltp.com.br/api",
        empresa=Provider.PORTALTP,
    )
