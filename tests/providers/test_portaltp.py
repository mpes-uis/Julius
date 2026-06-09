"""Tests for the PortalTP provider."""

import json
from pathlib import Path

import pytest

from julius.models.endpoint import EndpointSpec, Frequency
from julius.models.prefeitura import Prefeitura, Provider
from julius.providers.portaltp import PortalTP
from julius.providers.registry import get_provider

FIXTURES = Path(__file__).parent.parent / "fixtures" / "portaltp"


@pytest.fixture
def portaltp() -> PortalTP:
    return PortalTP()


@pytest.fixture
def afonso_claudio() -> Prefeitura:
    return Prefeitura(
        id=1,
        prefeitura="Prefeitura Municipal de Afonso Cláudio",
        municipio="afonso claudio",
        url="https://afonsoclaudio-es.portaltp.com.br/api",
        empresa=Provider.PORTALTP,
    )


class TestRegistry:
    def test_registered(self) -> None:
        prov = get_provider("portaltp")
        assert isinstance(prov, PortalTP)

    def test_name(self, portaltp: PortalTP) -> None:
        assert portaltp.name == "portaltp"


class TestListEndpoints:
    def test_returns_endpoints(self, portaltp: PortalTP) -> None:
        endpoints = portaltp.list_endpoints()
        assert len(endpoints) > 0

    def test_has_licitacoes(self, portaltp: PortalTP) -> None:
        names = [e.name for e in portaltp.list_endpoints()]
        assert "licitacoes" in names

    def test_frequencies(self, portaltp: PortalTP) -> None:
        eps = {e.name: e for e in portaltp.list_endpoints()}
        assert eps["licitacoes"].frequency == Frequency.MONTHLY
        assert eps["bens_imoveis"].frequency == Frequency.YEARLY
        assert eps["orcamento_despesa"].frequency == Frequency.YEARLY

    def test_all_have_path(self, portaltp: PortalTP) -> None:
        for ep in portaltp.list_endpoints():
            assert ep.path, f"Endpoint {ep.name!r} has empty path"


class TestBuildUrl:
    def test_monthly_url(self, portaltp: PortalTP, afonso_claudio: Prefeitura) -> None:
        ep = EndpointSpec(
            name="licitacoes", path="Compras/GetLicitacoes", frequency=Frequency.MONTHLY
        )
        url = portaltp.build_url(afonso_claudio, ep, year=2024, month=1)
        assert (
            url
            == "https://afonsoclaudio-es.portaltp.com.br/api/Compras/GetLicitacoes?ano=2024&mes=01"
        )

    def test_monthly_zero_pads_month(self, portaltp: PortalTP, afonso_claudio: Prefeitura) -> None:
        ep = EndpointSpec(
            name="empenhos", path="Despesas/GetEmpenhosFavorecidos", frequency=Frequency.MONTHLY
        )
        url = portaltp.build_url(afonso_claudio, ep, year=2024, month=3)
        assert "mes=03" in url

    def test_yearly_url_omits_mes(self, portaltp: PortalTP, afonso_claudio: Prefeitura) -> None:
        ep = EndpointSpec(
            name="bens_imoveis", path="MateriasBens/GetBensImoveis", frequency=Frequency.YEARLY
        )
        url = portaltp.build_url(afonso_claudio, ep, year=2024, month=None)
        assert "mes" not in url
        assert "ano=2024" in url

    def test_no_trailing_slash_in_base(self, portaltp: PortalTP) -> None:
        pref = Prefeitura(
            id=99,
            prefeitura="Test",
            municipio="test",
            url="https://test-es.portaltp.com.br/api/",  # trailing slash
            empresa=Provider.PORTALTP,
        )
        ep = EndpointSpec(name="x", path="Compras/GetLicitacoes", frequency=Frequency.MONTHLY)
        url = portaltp.build_url(pref, ep, year=2024, month=1)
        assert "//" not in url.replace("https://", "")


class TestParse:
    def test_parse_fixture(self, portaltp: PortalTP) -> None:
        payload = (FIXTURES / "licitacoes_2024_01.json").read_bytes()
        records = portaltp.parse(payload)
        assert len(records) == 3
        assert "licitacao" in records[0]
        assert "valor_homologado" in records[0]

    def test_parse_empty_response(self, portaltp: PortalTP) -> None:
        records = portaltp.parse(b"[]")
        assert records == []

    def test_parse_empty_bytes(self, portaltp: PortalTP) -> None:
        records = portaltp.parse(b"")
        assert records == []

    def test_parse_bom_prefix(self, portaltp: PortalTP) -> None:
        data = json.dumps([{"key": "value"}]).encode("utf-8-sig")
        records = portaltp.parse(data)
        assert records == [{"key": "value"}]

    def test_parse_single_dict(self, portaltp: PortalTP) -> None:
        data = json.dumps({"key": "value"}).encode()
        records = portaltp.parse(data)
        assert records == [{"key": "value"}]
