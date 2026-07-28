"""Shared FastAPI application state (producer, registry, services).

Held on ``app.state`` and initialized in the lifespan so heavy resources
(the Kafka producer connection) are created once per process, not per request.
"""

from __future__ import annotations

from dataclasses import dataclass

from datamesh.adapters.sources.base import SourceConnector
from datamesh.application.ingestion_service import IngestionService


@dataclass
class AppState:
    """Container for long-lived, per-process API resources."""

    ingestion_service: IngestionService
    connectors: dict[str, SourceConnector]