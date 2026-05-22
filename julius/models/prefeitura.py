"""Prefeitura and Provider models."""

from enum import StrEnum

from pydantic import BaseModel, field_validator, model_validator


class Provider(StrEnum):
    PORTALTP = "portaltp"
    TECTRILHA = "tectrilha"
    AGAPE = "agape"
    ALPHATEC = "alphatec"
    CR2 = "cr2"
    PORTALFACIL = "portalfacil"


class Prefeitura(BaseModel):
    id: int
    prefeitura: str
    municipio: str
    url: str
    empresa: Provider
    unidadegestora: int | None = None

    @field_validator("url")
    @classmethod
    def normalize_url(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith("http"):
            v = "https://" + v.lstrip("/")
        return v.rstrip("/")

    @field_validator("empresa", mode="before")
    @classmethod
    def lowercase_empresa(cls, v: object) -> object:
        if isinstance(v, str):
            return v.strip().lower()
        return v

    @field_validator("unidadegestora", mode="before")
    @classmethod
    def coerce_ug(cls, v: object) -> object:
        if v == "" or v is None:
            return None
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            return int(float(s))
        return v

    @model_validator(mode="after")
    def tectrilha_requires_ug(self) -> "Prefeitura":
        if self.empresa == Provider.TECTRILHA and self.unidadegestora is None:
            raise ValueError(f"Tectrilha prefeitura '{self.municipio}' is missing unidadegestora")
        return self

    @property
    def slug(self) -> str:
        """URL-friendly identifier derived from municipio."""
        import re
        import unicodedata

        normalized = unicodedata.normalize("NFD", self.municipio)
        ascii_str = normalized.encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
