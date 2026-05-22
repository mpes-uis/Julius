"""EndpointSpec — describes a single API endpoint for a provider."""

from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class Frequency(StrEnum):
    MONTHLY = "monthly"
    YEARLY = "yearly"


class ParamSpec(BaseModel):
    name: str
    required: bool = True
    default: Any = None


class EndpointSpec(BaseModel):
    name: str
    path: str
    frequency: Frequency = Frequency.MONTHLY
    params: list[ParamSpec] = []
    description: str = ""
