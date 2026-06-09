"""Tests for the extraction scheduler."""

from datetime import date

import pytest

from julius.extractor.scheduler import iter_jobs
from julius.models.endpoint import EndpointSpec, Frequency
from julius.models.prefeitura import Prefeitura, Provider
from julius.providers.portaltp import PortalTP


@pytest.fixture
def pref() -> Prefeitura:
    return Prefeitura(
        id=1,
        prefeitura="Test",
        municipio="test",
        url="https://test-es.portaltp.com.br/api",
        empresa=Provider.PORTALTP,
    )


class TestIterJobs:
    def test_monthly_generates_one_per_month(self, pref: Prefeitura) -> None:
        prov = PortalTP()
        # Filter to just one monthly endpoint
        monthly_ep = next(e for e in prov.list_endpoints() if e.frequency == Frequency.MONTHLY)

        class _SingleEndpoint(PortalTP):
            def list_endpoints(self) -> list[EndpointSpec]:
                return [monthly_ep]

        jobs = list(
            iter_jobs(
                _SingleEndpoint(),
                [pref],
                start=date(2024, 1, 1),
                end=date(2024, 3, 1),
            )
        )
        assert len(jobs) == 3
        assert [(j.year, j.month) for j in jobs] == [(2024, 1), (2024, 2), (2024, 3)]

    def test_yearly_generates_one_per_year(self, pref: Prefeitura) -> None:
        yearly_ep = EndpointSpec(name="bens", path="X/Y", frequency=Frequency.YEARLY)

        class _YearlyOnly(PortalTP):
            def list_endpoints(self) -> list[EndpointSpec]:
                return [yearly_ep]

        jobs = list(
            iter_jobs(
                _YearlyOnly(),
                [pref],
                start=date(2022, 1, 1),
                end=date(2024, 12, 1),
            )
        )
        assert len(jobs) == 3
        assert [j.year for j in jobs] == [2022, 2023, 2024]
        assert all(j.month is None for j in jobs)

    def test_job_key_is_unique_per_combination(self, pref: Prefeitura) -> None:
        prov = PortalTP()
        jobs = list(iter_jobs(prov, [pref], start=date(2024, 1, 1), end=date(2024, 1, 1)))
        keys = [j.key for j in jobs]
        assert len(keys) == len(set(keys))
