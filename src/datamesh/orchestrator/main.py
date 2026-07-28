"""Orchestrator container: the reactive control plane.

Runs in its OWN process/container (never inside the API). It subscribes to the
``base_data.updated`` topic and, for each event, triggers recomputation of every
dependent Data Product.

Resilience choices tied to the paper's fault-tolerance argument:
* ``enable_auto_commit=False`` + explicit commit *after* successful processing
  gives at-least-once delivery -- a crash mid-processing re-delivers the event
  rather than losing it, so a product is never "orphaned" from an update.
* ``auto_offset_reset="earliest"`` lets a freshly started orchestrator catch up
  on events produced before it joined.
"""

from __future__ import annotations

import asyncio
import logging

from aiokafka import AIOKafkaConsumer

from datamesh.application.product_service import ProductRecalculationService
from datamesh.config import get_settings
from datamesh.domain.events import BaseDataUpdatedEvent
from datamesh.infrastructure.database import async_session_factory, init_db
from datamesh.infrastructure.kafka_producer import EventProducer

# Short HH:MM:SS timestamp, no redundant "[orchestrator]" tag -- the
# dedicated VS Code terminal running this container is already labeled,
# so repeating it on every line was just noise.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


async def _run() -> None:
    settings = get_settings()

    # Ensure schema exists even if the orchestrator wins the startup race.
    await init_db()

    producer = EventProducer(settings.kafka_bootstrap_servers)
    await producer.start()

    service = ProductRecalculationService(
        session_factory=async_session_factory,
        producer=producer,
        settings=settings,
    )

    consumer = AIOKafkaConsumer(
        settings.topic_base_data_updated,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group_orchestrator,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info("Orchestrator listening on '%s'", settings.topic_base_data_updated)

    try:
        async for message in consumer:
            try:
                event = BaseDataUpdatedEvent.model_validate_json(message.value)
                logger.info(
                    "Received update: %s v%s (value=%s)",
                    event.base_data_id,
                    event.version,
                    event.value,
                )
                results = await service.recalculate_for_base_data(event.base_data_id)
                for result in results:
                    logger.info(
                        "Recalculated %s -> v%s = %s (lineage: %s)",
                        result.product_id,
                        result.version,
                        result.value,
                        [
                            f"{e.base_data_id}@v{e.version}"
                            for e in result.composition_lineage
                        ],
                    )
                # Commit only after full, successful processing (at-least-once).
                await consumer.commit()
            except Exception:  # noqa: BLE001 - keep the consumer loop alive
                logger.exception("Failed to process message; offset NOT committed.")
                # Offset is not committed: the broker will redeliver on restart.
    finally:
        await consumer.stop()
        await producer.stop()


def main() -> None:
    """Console entrypoint (``python -m datamesh.orchestrator.main``)."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
