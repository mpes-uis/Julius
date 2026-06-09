"""Retry policy using tenacity."""

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from julius.config import settings


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException | httpx.ConnectError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return False


fetch_with_retry = retry(
    retry=retry_if_exception(_should_retry),
    stop=stop_after_attempt(settings.retry_max_attempts),
    wait=wait_exponential(
        multiplier=settings.retry_backoff_base,
        min=settings.retry_backoff_base,
        max=settings.retry_backoff_base * 8,
    ),
    reraise=True,
)
