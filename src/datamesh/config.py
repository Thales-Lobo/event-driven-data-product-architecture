"""Centralized runtime configuration.

Settings are sourced from environment variables so the *same* image can back
both the API and the orchestrator containers, differing only by process command.
This keeps infrastructure endpoints (Kafka, Postgres) out of the code and honors
the twelve-factor "config in the environment" principle.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Strongly-typed application settings validated at startup."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- PostgreSQL (active metadata + data persistence) ---
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_user: str = "datamesh"
    postgres_password: str = "datamesh"
    postgres_db: str = "datamesh"

    # --- Kafka (event-driven control plane) ---
    kafka_bootstrap_servers: str = "kafka:9092"
    topic_base_data_updated: str = "base_data.updated"
    topic_data_product_recalculated: str = "data_product.recalculated"
    consumer_group_orchestrator: str = "orchestrator-service"

    @property
    def async_database_url(self) -> str:
        """Async SQLAlchemy DSN using the asyncpg driver."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    """Return a process-wide singleton of the settings object."""
    return Settings()