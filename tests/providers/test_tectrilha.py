"""Tests for the Tectrilha provider."""

import json
from pathlib import Path

import pytest

from julius.models.endpoint import EndpointSpec, Frequency
from julius.models.prefeitura import Prefeitura, Provider
from julius.providers.registry import get_provider
from julius.providers.tectrilha import Tectrilha

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tectrilha"


@pytest.fixture
def tectrilha() -> Tectrilha:
    return Tectrilha()


@pytest.fixture
def vitoria() -> Prefeitura:
    return Prefeitura(
        id=78,
        prefeitura="Prefeitura Municipal de Vitória",
        municipio="vitória",
        url="https://wstransparencia.vitoria.es.gov.br/api",
        empresa=Provider.TECTRILHA,
        unidadegestora=6,
    )


@pytest.fixture
def serra() -> Prefeitura:
    return Prefeitura(
        id=70,
        prefeitura="Prefeitura Municipal da Serra",
        municipio="serra",
        url="https://wstransparencia.serra.es.gov.br/api",
        empresa=Provider.TECTRILHA,
        unidadegestora=1,
    )


class TestRegistry:
    def test_registered(self) -> None:
        prov = get_provider("tectrilha")
        assert isinstance(prov, Tectrilha)

    def test_name(self, tectrilha: Tectrilha) -> None:
        assert tectrilha.name == "tectrilha"


class TestListEndpoints:
    def test_returns_endpoints(self, tectrilha: Tectrilha) -> None:
        eps = tectrilha.list_endpoints()
        assert len(eps) >= 10

    def test_has_required_endpoints(self, tectrilha: Tectrilha) -> None:
        names = {e.name for e in tectrilha.list_endpoints()}
        for required in ("contratos", "despesa", "receitas", "licitacoes"):
            assert required in names, f"Missing endpoint: {required}"

    def test_licitacoes_is_post(self, tectrilha: Tectrilha) -> None:
        eps = {e.name: e for e in tectrilha.list_endpoints()}
        assert eps["licitacoes"].method == "POST"

    def test_bens_imoveis_no_year_param(self, tectrilha: Tectrilha) -> None:
        eps = {e.name: e for e in tectrilha.list_endpoints()}
        assert eps["bens_imoveis"].no_year_param is True

    def test_pessoal_is_monthly(self, tectrilha: Tectrilha) -> None:
        eps = {e.name: e for e in tectrilha.list_endpoints()}
        assert eps["pessoal"].frequency == Frequency.MONTHLY


class TestBuildUrl:
    def test_strips_api_suffix_from_base(self, tectrilha: Tectrilha, vitoria: Prefeitura) -> None:
        ep = EndpointSpec(name="despesa", path="despesa", frequency=Frequency.YEARLY)
        url = tectrilha.build_url(vitoria, ep, year=2024)
        assert url.startswith("https://wstransparencia.vitoria.es.gov.br/api/despesa")
        assert "//api" not in url

    def test_yearly_without_ug(self, tectrilha: Tectrilha, vitoria: Prefeitura) -> None:
        ep = EndpointSpec(name="despesa", path="despesa", frequency=Frequency.YEARLY)
        url = tectrilha.build_url(vitoria, ep, year=2024)
        assert "exercicio=2024" in url
        assert "unidadeGestoraId" not in url

    def test_yearly_with_ug(self, tectrilha: Tectrilha, vitoria: Prefeitura) -> None:
        ep = EndpointSpec(
            name="contratos", path="contratos", frequency=Frequency.YEARLY, requires_ug=True
        )
        url = tectrilha.build_url(vitoria, ep, year=2024)
        assert "exercicio=2024" in url
        assert "unidadeGestoraId=6" in url

    def test_no_year_param(self, tectrilha: Tectrilha, vitoria: Prefeitura) -> None:
        ep = EndpointSpec(
            name="bens_imoveis", path="bensImoveis",
            frequency=Frequency.YEARLY, no_year_param=True
        )
        url = tectrilha.build_url(vitoria, ep, year=2024)
        assert "exercicio" not in url

    def test_monthly_adds_periodo(self, tectrilha: Tectrilha, vitoria: Prefeitura) -> None:
        ep = EndpointSpec(name="pessoal", path="pessoal", frequency=Frequency.MONTHLY)
        url = tectrilha.build_url(vitoria, ep, year=2024, month=3)
        assert "exercicio=2024" in url
        assert "periodo=3" in url

    def test_post_endpoint_params_in_url(self, tectrilha: Tectrilha, serra: Prefeitura) -> None:
        ep = EndpointSpec(
            name="licitacoes", path="licitacoes",
            frequency=Frequency.YEARLY, requires_ug=True, method="POST"
        )
        url = tectrilha.build_url(serra, ep, year=2024)
        # POST: params still in URL (fetch() strips them to body)
        assert "exercicio=2024" in url
        assert "unidadeGestoraId=1" in url

    def test_different_ug(self, tectrilha: Tectrilha, serra: Prefeitura) -> None:
        ep = EndpointSpec(
            name="receitas", path="receitas", frequency=Frequency.YEARLY, requires_ug=True
        )
        url = tectrilha.build_url(serra, ep, year=2023)
        assert "unidadeGestoraId=1" in url
        assert "exercicio=2023" in url


class TestParse:
    def test_parse_contratos_fixture(self, tectrilha: Tectrilha) -> None:
        payload = (FIXTURES / "contratos_2023.json").read_bytes()
        records = tectrilha.parse(payload)
        assert len(records) == 2
        assert "NumeroContrato" in records[0]
        assert "ValorContratado" in records[0]

    def test_parse_licitacoes_fixture(self, tectrilha: Tectrilha) -> None:
        payload = (FIXTURES / "licitacoes_2024.json").read_bytes()
        records = tectrilha.parse(payload)
        assert len(records) == 3

    def test_parse_bens_imoveis_fixture(self, tectrilha: Tectrilha) -> None:
        payload = (FIXTURES / "bens_imoveis.json").read_bytes()
        records = tectrilha.parse(payload)
        assert len(records) == 3

    def test_parse_empty(self, tectrilha: Tectrilha) -> None:
        assert tectrilha.parse(b"[]") == []
        assert tectrilha.parse(b"") == []

    def test_parse_bom(self, tectrilha: Tectrilha) -> None:
        data = json.dumps([{"k": "v"}]).encode("utf-8-sig")
        assert tectrilha.parse(data) == [{"k": "v"}]

    def test_parse_wrapped_dict(self, tectrilha: Tectrilha) -> None:
        data = json.dumps({"itens": [{"a": 1}, {"b": 2}], "total": 2}).encode()
        records = tectrilha.parse(data)
        assert records == [{"a": 1}, {"b": 2}]
