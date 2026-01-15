"""Configuration module using Pydantic settings."""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Required settings
    db_url: str
    google_api_key: str

    # Optional settings with defaults
    table_prefix: str = "api_"
    db_schema: str = "public"

    # Request settings
    request_timeout: int = 30


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
