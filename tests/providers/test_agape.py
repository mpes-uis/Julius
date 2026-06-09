"""Tests for the Agape and Alphatec providers."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from julius.models.endpoint import EndpointSpec, Frequency
from julius.models.prefeitura import Prefeitura, Provider
from julius.providers.agape import Agape
from julius.providers.alphatec import Alphatec
from julius.providers.registry import get_provider

FIXTURES = Path(__file__).parent.parent / "fixtures" / "agape"


def _make_page(registros: list[dict], pagina_atual: int, pagina_total: int) -> bytes:
    return json.dumps(
        {
            "pagina_total": pagina_total,
            "pagina_atual": pagina_atual,
            "registro_total": pagina_total * len(registros),
            "registros": registros,
            "erros": [],
        }
    ).encode()


@pytest.fixture
def agape() -> Agape:
    return Agape()


@pytest.fixture
def alphatec() -> Alphatec:
    return Alphatec()


@pytest.fixture
def cachoeiro() -> Prefeitura:
    return Prefeitura(
        id=16,
        prefeitura="Prefeitura Municipal de Cachoeiro de Itapemirim",
        municipio="cachoeiro de itapemirim",
        url="https://transparencia.cachoeiro.es.gov.br/transparencia/api",
        empresa=Provider.AGAPE,
    )


@pytest.fixture
def sooretama() -> Prefeitura:
    return Prefeitura(
        id=71,
        prefeitura="Prefeitura Municipal de Sooretama",
        municipio="sooretama",
        url="https://www.sooretama.es.gov.br/transparencia/api",
        empresa=Provider.ALPHATEC,
    )


class TestRegistry:
    def test_agape_registered(self) -> None:
        assert isinstance(get_provider("agape"), Agape)

    def test_alphatec_registered(self) -> None:
        assert isinstance(get_provider("alphatec"), Alphatec)

    def test_agape_is_paginated(self, agape: Agape) -> None:
        assert agape.paginated is True

    def test_alphatec_is_paginated(self, alphatec: Alphatec) -> None:
        assert alphatec.paginated is True


class TestListEndpoints:
    def test_agape_has_required(self, agape: Agape) -> None:
        names = {e.name for e in agape.list_endpoints()}
        for ep in ("empenhos", "pagamentos", "receitas", "contratos", "servidores"):
            assert ep in names

    def test_alphatec_has_same_structure(self, alphatec: Alphatec) -> None:
        a_names = {e.name for e in Agape().list_endpoints()}
        t_names = {e.name for e in alphatec.list_endpoints()}
        assert a_names == t_names

    def test_monthly_endpoints(self, agape: Agape) -> None:
        eps = {e.name: e for e in agape.list_endpoints()}
        assert eps["empenhos"].frequency == Frequency.MONTHLY
        assert eps["pagamentos"].frequency == Frequency.MONTHLY

    def test_yearly_endpoints(self, agape: Agape) -> None:
        eps = {e.name: e for e in agape.list_endpoints()}
        assert eps["contratos"].frequency == Frequency.YEARLY
        assert eps["servidores"].frequency == Frequency.YEARLY


class TestBuildUrl:
    def test_monthly_url(self, agape: Agape, cachoeiro: Prefeitura) -> None:
        ep = EndpointSpec(name="empenhos", path="empenhos", frequency=Frequency.MONTHLY)
        url = agape.build_url(cachoeiro, ep, year=2024, month=3)
        assert url == (
            "https://transparencia.cachoeiro.es.gov.br/transparencia/api/empenhos"
            "?ano=2024&mes=03"
        )

    def test_yearly_url_no_mes(self, agape: Agape, cachoeiro: Prefeitura) -> None:
        ep = EndpointSpec(name="contratos", path="contratos", frequency=Frequency.YEARLY)
        url = agape.build_url(cachoeiro, ep, year=2024, month=None)
        assert "ano=2024" in url
        assert "mes" not in url

    def test_alphatec_url(self, alphatec: Alphatec, sooretama: Prefeitura) -> None:
        ep = EndpointSpec(name="receitas", path="receitas", frequency=Frequency.MONTHLY)
        url = alphatec.build_url(sooretama, ep, year=2024, month=1)
        assert "sooretama" in url
        assert "mes=01" in url


class TestParse:
    def test_parse_page_fixture(self, agape: Agape) -> None:
        payload = (FIXTURES / "empenhos_2024_01_p1.json").read_bytes()
        records = agape.parse(payload)
        assert len(records) == 10

    def test_parse_extracts_registros_key(self, agape: Agape) -> None:
        payload = _make_page([{"id": 1}, {"id": 2}], 1, 1)
        records = agape.parse(payload)
        assert records == [{"id": 1}, {"id": 2}]

    def test_parse_empty(self, agape: Agape) -> None:
        payload = _make_page([], 1, 0)
        assert agape.parse(payload) == []

    def test_parse_plain_list(self, agape: Agape) -> None:
        payload = json.dumps([{"a": 1}]).encode()
        assert agape.parse(payload) == [{"a": 1}]


class TestFetchAll:
    @pytest.mark.asyncio
    async def test_single_page(self, agape: Agape) -> None:
        page = _make_page([{"id": i} for i in range(10)], 1, 1)
        client = MagicMock()
        client.get = AsyncMock(return_value=MagicMock(
            status_code=200,
            content=page,
            raise_for_status=lambda: None,
        ))
        records = await agape.fetch_all(client, "https://x.com/api/empenhos?ano=2024&mes=01")
        assert len(records) == 10
        assert client.get.call_count == 1

    @pytest.mark.asyncio
    async def test_multi_page(self, agape: Agape) -> None:
        pages = {
            1: _make_page([{"id": 1}], 1, 3),
            2: _make_page([{"id": 2}], 2, 3),
            3: _make_page([{"id": 3}], 3, 3),
        }
        call_count = 0

        async def _mock_get(url: str) -> MagicMock:
            nonlocal call_count
            call_count += 1
            page_num = int(url.split("pagina=")[1]) if "pagina=" in url else 1
            return MagicMock(
                status_code=200,
                content=pages[page_num],
                raise_for_status=lambda: None,
            )

        client = MagicMock()
        client.get = _mock_get
        records = await agape.fetch_all(client, "https://x.com/api/contratos?ano=2024")
        assert len(records) == 3
        assert call_count == 3
