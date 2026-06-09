"""Agape transparency portal provider.

URL pattern: {base}/{path}?ano={year}[&mes={month:02d}][&pagina={page}]
Response: {"pagina_total": N, "registro_total": M, "registros": [...], "erros": [...], ...}
Page size: 10 records. Pagination via &pagina=N (starting at 1).
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import ClassVar

import httpx
import yaml
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from julius.config import settings
from julius.models.endpoint import EndpointSpec, Frequency
from julius.models.prefeitura import Prefeitura
from julius.providers.base import Provider
from julius.providers.registry import register

_ENDPOINTS_FILE = Path(__file__).parent.parent.parent / "data" / "endpoints" / "agape.yaml"
_MAX_PAGES = 5000  # hard cap: 50,000 records per job
_log = logging.getLogger(__name__)


def _agape_should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException | httpx.ConnectError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return False


@register
class Agape(Provider):
    name: ClassVar[str] = "agape"
    paginated: ClassVar[bool] = True

    def _endpoints_file(self) -> Path:
        return _ENDPOINTS_FILE

    def list_endpoints(self) -> list[EndpointSpec]:
        raw = yaml.safe_load(self._endpoints_file().read_text(encoding="utf-8"))
        return [
            EndpointSpec(
                name=ep["name"],
                path=ep["path"],
                frequency=Frequency(ep.get("frequency", "monthly")),
                description=ep.get("description", ""),
            )
            for ep in raw["endpoints"]
        ]

    def build_url(
        self,
        prefeitura: Prefeitura,
        endpoint: EndpointSpec,
        *,
        year: int,
        month: int | None = None,
    ) -> str:
        base = prefeitura.url  # already has /api, no trailing slash
        url = f"{base}/{endpoint.path}?ano={year}"
        if endpoint.frequency == Frequency.MONTHLY and month is not None:
            url += f"&mes={month:02d}"
        return url

    def parse(self, payload: bytes) -> list[dict[str, object]]:
        text = payload.decode("utf-8-sig")
        if not text.strip():
            return []
        data = json.loads(text)
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            records = data.get("registros", [])
            if isinstance(records, list):
                return [r for r in records if isinstance(r, dict)]
        return []

    async def fetch_all(
        self, client: httpx.AsyncClient, url: str
    ) -> list[dict[str, object]]:
        """Fetch all pages, retrying each page individually on transient errors."""
        all_records: list[dict[str, object]] = []
        page = 1

        while page <= _MAX_PAGES:
            page_url = f"{url}&pagina={page}"

            @retry(
                retry=retry_if_exception(_agape_should_retry),
                stop=stop_after_attempt(settings.retry_max_attempts),
                wait=wait_exponential(
                    multiplier=settings.retry_backoff_base,
                    min=settings.retry_backoff_base,
                    max=settings.retry_backoff_base * 8,
                ),
                reraise=True,
            )
            async def _fetch_page(u: str = page_url) -> bytes:
                resp = await client.get(u)
                resp.raise_for_status()
                return resp.content

            payload = await _fetch_page()
            await asyncio.sleep(settings.http_per_host_delay)

            text = payload.decode("utf-8-sig")
            if not text.strip():
                break
            data = json.loads(text)

            pagina_total = data.get("pagina_total", 1) or 1
            records = data.get("registros", [])
            batch = [r for r in records if isinstance(r, dict)]
            all_records.extend(batch)

            if page >= pagina_total:
                break

            if page == _MAX_PAGES:
                _log.warning(
                    "agape.fetch_all.max_pages_hit url=%s pages=%d records=%d",
                    url, page, len(all_records),
                )

            page += 1

        return all_records
