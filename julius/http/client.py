"""httpx AsyncClient factory."""

import httpx

from julius.config import settings


def make_client() -> httpx.AsyncClient:
    """Return a configured AsyncClient for transparency portal requests."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(settings.http_timeout),
        headers={"User-Agent": settings.http_user_agent},
        follow_redirects=True,
        limits=httpx.Limits(
            max_connections=settings.http_concurrency + 10,
            max_keepalive_connections=settings.http_concurrency,
        ),
    )
