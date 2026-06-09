"""Application settings loaded from environment variables / .env file."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="JULIUS_", env_file=".env", extra="ignore")

    # Paths
    data_dir: Path = Path("data")
    db_path: Path = Path("data/julius.duckdb")
    log_dir: Path = Path("logs")

    # HTTP
    http_timeout: float = 30.0
    http_concurrency: int = 20
    http_per_host_delay: float = 1.0
    http_user_agent: str = "Julius/2.0 (github.com/pedropberger/Julius; transparency data research)"

    # Retry
    retry_max_attempts: int = 3
    retry_backoff_base: float = 1.0

    # Logging
    log_level: str = "INFO"


settings = Settings()
