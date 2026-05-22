"""Async extraction runner with per-host rate limiting."""

import asyncio
import time
from collections.abc import Iterator

import structlog

from julius.config import settings
from julius.extractor.checkpoint import Checkpoint
from julius.http.client import make_client
from julius.http.retry import fetch_with_retry
from julius.models.job import ExtractionJob, ExtractionResult, JobStatus
from julius.providers.base import Provider
from julius.storage.duckdb_store import DuckDBStore

log = structlog.get_logger(__name__)


class Runner:
    def __init__(self, store: DuckDBStore, checkpoint: Checkpoint) -> None:
        self._store = store
        self._checkpoint = checkpoint
        self._global_sem = asyncio.Semaphore(settings.http_concurrency)
        # One semaphore per host — ensures 1 request at a time per portal
        self._host_sems: dict[str, asyncio.Semaphore] = {}

    def _host_sem(self, url: str) -> asyncio.Semaphore:
        from urllib.parse import urlparse

        host = urlparse(url).netloc
        if host not in self._host_sems:
            self._host_sems[host] = asyncio.Semaphore(1)
        return self._host_sems[host]

    async def _run_job(
        self,
        provider: Provider,
        job: ExtractionJob,
        client: "asyncio.Future[object]",  # actually httpx.AsyncClient, typed loosely for stub
    ) -> ExtractionResult:
        import httpx as _httpx

        if self._checkpoint.already_done(job):
            log.debug("extraction.job.skipped", key=job.key)
            return ExtractionResult(job=job, status=JobStatus.SKIPPED)

        url = provider.build_url(job.prefeitura, job.endpoint, year=job.year, month=job.month)
        t0 = time.monotonic()

        try:
            async with self._global_sem:
                host_sem = self._host_sem(url)
                async with host_sem:
                    log.debug("http.request", url=url)

                    @fetch_with_retry
                    async def _do_fetch() -> bytes:
                        assert isinstance(client, _httpx.AsyncClient)
                        return await provider.fetch(client, url)

                    payload = await _do_fetch()
                    await asyncio.sleep(settings.http_per_host_delay)

            records = provider.parse(payload)
            duration = (time.monotonic() - t0) * 1000

            if not records:
                log.info("extraction.job.empty", key=job.key, url=url)
                return ExtractionResult(job=job, status=JobStatus.EMPTY, duration_ms=duration)

            meta = {
                "municipio": job.prefeitura.municipio,
                "prefeitura": job.prefeitura.prefeitura,
                "provider": str(job.prefeitura.empresa),
                "ano": job.year,
                "mes": job.month,
            }
            self._store.write_records(job.endpoint.name, records, meta)
            self._checkpoint.mark_done(job, records=len(records))

            log.info("extraction.job.success", key=job.key, records=len(records))
            return ExtractionResult(
                job=job, status=JobStatus.SUCCESS, records_count=len(records), duration_ms=duration
            )

        except Exception as exc:
            duration = (time.monotonic() - t0) * 1000
            error = f"{type(exc).__name__}: {exc}"
            self._checkpoint.mark_failed(job, error=error)
            log.warning("extraction.job.failed", key=job.key, url=url, error=error)
            return ExtractionResult(
                job=job, status=JobStatus.FAILED, error=error, duration_ms=duration
            )

    async def run(
        self, provider: Provider, jobs: Iterator[ExtractionJob]
    ) -> list[ExtractionResult]:
        """Run all jobs concurrently within the configured semaphore limits."""
        job_list = list(jobs)
        log.info("extraction.started", total=len(job_list), provider=provider.name)

        async with make_client() as client:
            tasks = [self._run_job(provider, job, client) for job in job_list]  # type: ignore[arg-type]
            results = await asyncio.gather(*tasks)

        summary = {s.value: 0 for s in JobStatus}
        for r in results:
            summary[r.status] += 1
        log.info("extraction.finished", provider=provider.name, **summary)

        return list(results)
