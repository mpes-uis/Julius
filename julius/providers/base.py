"""Abstract base class for all transparency portal API providers."""

from abc import ABC, abstractmethod
from typing import ClassVar

import httpx

from julius.models.endpoint import EndpointSpec
from julius.models.prefeitura import Prefeitura


class Provider(ABC):
    """Encapsulates the API quirks of a single transparency portal provider."""

    name: ClassVar[str]

    # Set to True for providers whose fetch_all() handles pagination internally
    # and manages its own per-page retry. The runner skips the outer retry wrapper.
    paginated: ClassVar[bool] = False

    @abstractmethod
    def list_endpoints(self) -> list[EndpointSpec]:
        """Return all endpoints available for this provider."""
        ...

    @abstractmethod
    def build_url(
        self,
        prefeitura: Prefeitura,
        endpoint: EndpointSpec,
        *,
        year: int,
        month: int | None = None,
    ) -> str:
        """Construct the full request URL for a given job."""
        ...

    async def fetch(self, client: httpx.AsyncClient, url: str) -> bytes:
        """Perform the HTTP GET and return raw bytes. Override for non-standard auth/methods."""
        response = await client.get(url)
        response.raise_for_status()
        return response.content

    @abstractmethod
    def parse(self, payload: bytes) -> list[dict[str, object]]:
        """Decode bytes into a list of records (one dict per row)."""
        ...

    async def fetch_all(
        self, client: httpx.AsyncClient, url: str
    ) -> list[dict[str, object]]:
        """Fetch all records for this URL, handling pagination if needed.

        Default implementation: single fetch + parse.
        Paginated providers override this to loop through pages.
        """
        return self.parse(await self.fetch(client, url))
