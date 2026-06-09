"""ExtractionJob and ExtractionResult models."""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field

from julius.models.endpoint import EndpointSpec
from julius.models.prefeitura import Prefeitura


class JobStatus(StrEnum):
    SUCCESS = "success"
    EMPTY = "empty"
    FAILED = "failed"
    SKIPPED = "skipped"


class ExtractionJob(BaseModel):
    prefeitura: Prefeitura
    endpoint: EndpointSpec
    year: int
    month: int | None = None

    @property
    def key(self) -> str:
        """Unique identifier for checkpoint lookup."""
        parts = [self.prefeitura.empresa, self.prefeitura.slug, self.endpoint.name, str(self.year)]
        if self.month is not None:
            parts.append(f"{self.month:02d}")
        return "|".join(parts)


class ExtractionResult(BaseModel):
    job: ExtractionJob
    status: JobStatus
    records_count: int = 0
    error: str | None = None
    duration_ms: float = 0.0
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = Field(default_factory=dict)
