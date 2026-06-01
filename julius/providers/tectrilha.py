"""Tectrilha transparency portal provider.

URL pattern: {base}/api/{path}?exercicio={year}[&unidadeGestoraId={ug}]
Base URL in prefeituras.csv ends in /api — we strip it here.
Frequency: mostly yearly; pessoal is monthly via &periodo={month}.
licitacoes is a POST endpoint with JSON body {unidadeGestoraId, exercicio}.
bensImoveis is a snapshot with no year param.
"""

import json
from pathlib import Path
from typing import ClassVar
from urllib.parse import parse_qs, urlparse

import httpx
import yaml

from julius.models.endpoint import EndpointSpec, Frequency
from julius.models.prefeitura import Prefeitura
from julius.providers.base import Provider
from julius.providers.registry import register

_ENDPOINTS_FILE = Path(__file__).parent.parent.parent / "data" / "endpoints" / "tectrilha.yaml"


@register
class Tectrilha(Provider):
    name: ClassVar[str] = "tectrilha"

    def list_endpoints(self) -> list[EndpointSpec]:
        raw = yaml.safe_load(_ENDPOINTS_FILE.read_text(encoding="utf-8"))
        return [
            EndpointSpec(
                name=ep["name"],
                path=ep["path"],
                frequency=Frequency(ep.get("frequency", "yearly")),
                description=ep.get("description", ""),
                method=ep.get("method", "GET"),
                requires_ug=ep.get("requires_ug", False),
                no_year_param=ep.get("no_year_param", False),
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
        # Strip /api suffix — it's re-added below
        base = prefeitura.url.removesuffix("/api")
        url = f"{base}/api/{endpoint.path}"

        params: dict[str, str | int] = {}

        if not endpoint.no_year_param:
            params["exercicio"] = year

        if endpoint.requires_ug and prefeitura.unidadegestora is not None:
            params["unidadeGestoraId"] = prefeitura.unidadegestora

        if endpoint.frequency == Frequency.MONTHLY and month is not None:
            params["periodo"] = month

        if params:
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{qs}"

        return url

    async def fetch(self, client: httpx.AsyncClient, url: str) -> bytes:
        """GET by default; POST for endpoints that require it (detected from registered paths)."""
        parsed = urlparse(url)
        path_tail = parsed.path.rstrip("/").rsplit("/", 1)[-1].lower()

        if path_tail in self._POST_PATHS:
            # Move query-string params to JSON body
            body = {
                k: int(v[0]) if v[0].isdigit() else v[0]
                for k, v in parse_qs(parsed.query).items()
            }
            base_url = url.split("?")[0]
            response = await client.post(base_url, json=body)
        else:
            response = await client.get(url)

        response.raise_for_status()
        return response.content

    _POST_PATHS: ClassVar[frozenset[str]] = frozenset({"licitacoes"})

    def parse(self, payload: bytes) -> list[dict[str, object]]:
        text = payload.decode("utf-8-sig")
        if not text.strip():
            return []
        data = json.loads(text)
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            # Some endpoints wrap in {"itens": [...], "total": N}
            for key in ("itens", "data", "resultado"):
                if key in data and isinstance(data[key], list):
                    return [r for r in data[key] if isinstance(r, dict)]
            return [data]
        return []
