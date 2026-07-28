"""FastAPI application entrypoint (the API / "Data Shop" container).

Lifespan responsibilities:
* create tables (prototype convenience),
* seed the GDP product definition,
* start/stop the shared Kafka producer.

This process deliberately does NOT run the Kafka consumer -- reactive
recalculation is isolated in a separate orchestrator container (see compose).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI

from datamesh.adapters.sources.registry import build_registry
from datamesh.api.dependencies import AppState
from datamesh.api.routes import router
from datamesh.api.seed import seed_definitions
from datamesh.application.ingestion_service import IngestionService
from datamesh.config import get_settings
from datamesh.infrastructure.database import async_session_factory, init_db
from datamesh.infrastructure.kafka_producer import EventProducer


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()

    await init_db()
    await seed_definitions(async_session_factory)

    producer = EventProducer(settings.kafka_bootstrap_servers)
    await producer.start()

    app.state.app_state = AppState(
        ingestion_service=IngestionService(
            session_factory=async_session_factory,
            producer=producer,
            settings=settings,
        ),
        connectors=build_registry(),
    )
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(
    title="Event-Driven Data Product Platform",
    version="0.1.0",
    lifespan=lifespan,
)
app.include_router(router)