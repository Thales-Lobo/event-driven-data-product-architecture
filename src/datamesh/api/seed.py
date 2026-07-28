"""Idempotent seeding of Data Product *definitions*.

The orchestrator can only react to base-data events if it knows which products
subscribe to which base data. We register the ``monthly_gdp`` definition here so
the demo is self-contained. Real domains would self-register their products via
an authenticated endpoint.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from datamesh.domain.metadata import (
    BusinessMetadata,
    GovernanceMetadata,
    SensitivityLevel,
)
from datamesh.infrastructure.models import DataProductDefinition

_GDP_DEFINITION = DataProductDefinition(
    product_id="monthly_gdp",
    rule_id="calc_gdp_weighted_avg",
    rule_version="v2.1",
    dependencies=["base_industrial_production", "base_services_revenue"],
    business_metadata=BusinessMetadata(
        domain="macroeconomics",
        description="Simplified Monthly GDP from industrial production and services.",
        semantic_tags=["gdp", "macroeconomics", "indicator"],
    ).model_dump(mode="json"),
    governance_metadata=GovernanceMetadata(
        owner_domain="macroeconomics",
        sensitivity_level=SensitivityLevel.INTERNAL,
        access_control="restricted_internal",
        compliance_tags=["LGPD_compliant"],
    ).model_dump(mode="json"),
)


async def seed_definitions(session_factory: async_sessionmaker) -> None:
    """Insert the GDP product definition if it is not already present."""
    async with session_factory() as session:
        existing = await session.scalar(
            select(DataProductDefinition).where(
                DataProductDefinition.product_id == _GDP_DEFINITION.product_id
            )
        )
        if existing is None:
            session.add(_GDP_DEFINITION)
            await session.commit()