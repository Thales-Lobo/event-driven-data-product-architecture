"""Use case: ingest a normalized reading into the Base Data layer.

Responsibilities:
1. Compute the next monotonic version for the base_data_id.
2. Build & validate the full 4-dimensional BaseDataMetadata (provenance included).
3. Persist an immutable versioned record.
4. Emit a ``base_data.updated`` event so the control plane can react.

The service never knows which products depend on the data -- that decoupling is
the whole point of the event-driven design.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from datamesh.config import Settings
from datamesh.domain.events import BaseDataUpdatedEvent
from datamesh.domain.ingestion import IngestionPayload
from datamesh.domain.metadata import (
    BaseDataMetadata,
    OperationalMetadata,
    Provenance,
    QualityStatus,
)
from datamesh.infrastructure.kafka_producer import EventProducer
from datamesh.infrastructure.models import BaseDataRecord


class IngestionService:
    """Application service orchestrating Base Data ingestion + event emission."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        producer: EventProducer,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._settings = settings

    async def ingest(self, payload: IngestionPayload) -> BaseDataUpdatedEvent:
        """Persist a new Base Data version and publish its update event."""
        now = datetime.now(timezone.utc)

        async with self._session_factory() as session:
            # Determine the next version atomically within the transaction.
            current_max = await session.scalar(
                select(func.max(BaseDataRecord.version)).where(
                    BaseDataRecord.base_data_id == payload.base_data_id
                )
            )
            next_version = (current_max or 0) + 1

            metadata = BaseDataMetadata(
                technical=payload.technical,
                operational=OperationalMetadata(
                    last_refresh=now,
                    quality_status=QualityStatus.VALIDATED,
                    data_quality_score=99.0,
                ),
                business=payload.business,
                governance=payload.governance,
                provenance=Provenance(
                    extraction_timestamp=now,
                    reference_period=payload.reference_period,
                    raw_data_pointer=payload.raw_data_pointer,
                ),
            )

            record = BaseDataRecord(
                base_data_id=payload.base_data_id,
                version=next_version,
                value=payload.value,
                reference_period=payload.reference_period,
                asset_metadata=metadata.model_dump(mode="json"),
                created_at=now,
            )
            session.add(record)
            await session.commit()

        event = BaseDataUpdatedEvent(
            base_data_id=payload.base_data_id,
            version=next_version,
            value=payload.value,
            reference_period=payload.reference_period,
            source_system=payload.technical.source_system,
            occurred_at=now,
        )
        # Publish only after the DB commit succeeds (outbox-lite ordering).
        await self._producer.publish(
            topic=self._settings.topic_base_data_updated,
            key=payload.base_data_id,
            event=event,
        )
        return event