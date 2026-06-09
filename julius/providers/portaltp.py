"""PortalTP transparency portal provider.

URL pattern: {base}/{path}?ano={year}&mes={month:02d}
Base URL already contains /api (e.g. https://afonsoclaudio-es.portaltp.com.br/api).
Yearly endpoints omit the mes parameter.
Response: JSON array, occasionally BOM-prefixed (utf-8-sig).
"""

import json
from pathlib import Path
from typing import ClassVar

import yaml

from julius.models.endpoint import EndpointSpec, Frequency
from julius.models.prefeitura import Prefeitura
from julius.providers.base import Provider
from julius.providers.registry import register

_ENDPOINTS_FILE = Path(__file__).parent.parent.parent / "data" / "endpoints" / "portaltp.yaml"


@register
class PortalTP(Provider):
    name: ClassVar[str] = "portaltp"

    def list_endpoints(self) -> list[EndpointSpec]:
        raw = yaml.safe_load(_ENDPOINTS_FILE.read_text(encoding="utf-8"))
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
        base = prefeitura.url  # already normalised, no trailing slash
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
            return [data]
        return []
