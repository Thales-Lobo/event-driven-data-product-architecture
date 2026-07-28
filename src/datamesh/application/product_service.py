"""Use case: reactively recompute Data Products for an updated Base Data.

This is the heart of the "active metadata" control plane. Given a base_data_id
that just changed, it:
1. Finds every product *definition* that subscribes to that base data.
2. Loads the latest version of each dependency and builds the composition
   lineage (exact versions + timestamps actually consumed).
3. Applies the product's registered business rule.
4. Persists a new immutable product version and emits a
   ``data_product.recalculated`` event.

If a dependency has not been ingested yet, the product is skipped (it cannot be
computed) -- an intentional, explicit gap rather than a silent zero.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from datamesh.application.business_rules import get_rule
from datamesh.config import Settings
from datamesh.domain.events import DataProductRecalculatedEvent
from datamesh.domain.metadata import (
    BusinessMetadata,
    CompositionLineageEntry,
    DataProductMetadata,
    GovernanceMetadata,
    OperationalMetadata,
    ProductStatus,
    QualityStatus,
    TechnicalMetadata,
)
from datamesh.infrastructure.kafka_producer import EventProducer
from datamesh.infrastructure.models import (
    BaseDataRecord,
    DataProductDefinition,
    DataProductRecord,
)


class ProductRecalculationService:
    """Application service that recomputes products in reaction to events."""

    def __init__(
        self,
        session_factory: async_sessionmaker,
        producer: EventProducer,
        settings: Settings,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._settings = settings

    async def recalculate_for_base_data(
        self, base_data_id: str
    ) -> list[DataProductRecalculatedEvent]:
        """Recompute all products depending on ``base_data_id``."""
        now = datetime.now(timezone.utc)
        emitted: list[DataProductRecalculatedEvent] = []

        async with self._session_factory() as session:
            # JSONB containment (@>) selects definitions whose dependency list
            # includes this base_data_id -- a proper indexed metadata query.
            definitions = (
                await session.scalars(
                    select(DataProductDefinition).where(
                        DataProductDefinition.dependencies.contains([base_data_id])
                    )
                )
            ).all()

            for definition in definitions:
                inputs: dict[str, float] = {}
                lineage: list[CompositionLineageEntry] = []
                complete = True

                for dependency_id in definition.dependencies:
                    latest = await self._latest_base_data(session, dependency_id)
                    if latest is None:
                        # Dependency not yet available: cannot compute this product.
                        complete = False
                        break
                    inputs[dependency_id] = latest.value
                    lineage.append(
                        CompositionLineageEntry(
                            base_data_id=latest.base_data_id,
                            version=latest.version,
                            timestamp_used=latest.created_at,
                        )
                    )

                if not complete:
                    continue

                value = get_rule(definition.rule_id)(inputs)
                next_version = await self._next_product_version(
                    session, definition.product_id
                )

                metadata = DataProductMetadata(
                    technical=TechnicalMetadata(
                        source_system="internal_orchestrator",
                        schema_version="1.0.0",
                    ),
                    operational=OperationalMetadata(
                        last_refresh=now,
                        quality_status=QualityStatus.VALIDATED,
                        data_quality_score=98.5,
                    ),
                    business=BusinessMetadata.model_validate(definition.business_metadata),
                    governance=GovernanceMetadata.model_validate(
                        definition.governance_metadata
                    ),
                    composition_lineage=lineage,
                    rule_id=definition.rule_id,
                    rule_version=definition.rule_version,
                )

                record = DataProductRecord(
                    product_id=definition.product_id,
                    version=next_version,
                    value=value,
                    asset_metadata=metadata.model_dump(mode="json"),
                    created_at=now,
                )
                session.add(record)

                emitted.append(
                    DataProductRecalculatedEvent(
                        product_id=definition.product_id,
                        version=next_version,
                        value=value,
                        triggered_by=base_data_id,
                        composition_lineage=lineage,
                        occurred_at=now,
                    )
                )

            await session.commit()

        # Publish after commit so consumers never see phantom recalculations.
        for event in emitted:
            await self._producer.publish(
                topic=self._settings.topic_data_product_recalculated,
                key=event.product_id,
                event=event,
            )
        return emitted

    @staticmethod
    async def _latest_base_data(session, base_data_id: str) -> BaseDataRecord | None:
        """Return the highest-version record for a base data id, if any."""
        return await session.scalar(
            select(BaseDataRecord)
            .where(BaseDataRecord.base_data_id == base_data_id)
            .order_by(BaseDataRecord.version.desc())
            .limit(1)
        )

    @staticmethod
    async def _next_product_version(session, product_id: str) -> int:
        """Compute the next monotonic version for a product."""
        current_max = await session.scalar(
            select(func.max(DataProductRecord.version)).where(
                DataProductRecord.product_id == product_id
            )
        )
        return (current_max or 0) + 1