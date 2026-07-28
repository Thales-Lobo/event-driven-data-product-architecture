"""Persistence models: relational tables + JSONB active metadata.

Design rationale (mirroring the dissertation's storage discussion):
* The *shape* of an asset (ids, versions, numeric value) lives in typed columns
  so we get referential integrity and fast versioned queries.
* The rich, evolving 4-dimensional metadata lives in a ``JSONB`` column so the
  schema can evolve per-domain without DDL migrations -- Postgres gives us a
  single, consolidated, queryable metadata catalog.

We deliberately separate a static ``DataProductDefinition`` (the subscription:
which base data ids and rule compose a product) from the versioned
``DataProductRecord`` (each computed output). The orchestrator reads definitions
to know *what* to recompute; records store *what was computed and from what*.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Column, DateTime, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


def _utcnow() -> datetime:
    """Timezone-aware UTC timestamp (avoids naive-datetime ambiguity)."""
    return datetime.now(timezone.utc)


class BaseDataRecord(SQLModel, table=True):
    """An immutable, versioned Base Data reading."""

    __tablename__ = "base_data"
    __table_args__ = (
        UniqueConstraint("base_data_id", "version", name="uq_base_data_version"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    base_data_id: str = Field(index=True)
    version: int = Field(default=1, index=True)
    value: float
    reference_period: str
    # 4-dimensional BaseDataMetadata serialized as JSONB.
    asset_metadata: dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))
    created_at: datetime = Field(
        sa_column=Column(DateTime(timezone=True), nullable=False, default=_utcnow)
    )


class DataProductDefinition(SQLModel, table=True):
    """Static declaration of a product: its dependencies and business rule.

    ``dependencies`` (a JSONB list of base_data_ids) is the subscription the
    orchestrator matches against when a ``base_data.updated`` event arrives.
    """

    __tablename__ = "data_product_definition"

    product_id: str = Field(primary_key=True)
    rule_id: str
    rule_version: str = Field(default="v1.0")
    dependencies: list[str] = Field(sa_column=Column(JSONB, nullable=False))
    business_metadata: dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))
    governance_metadata: dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))


class DataProductRecord(SQLModel, table=True):
    """An immutable, versioned computed Data Product output."""

    __tablename__ = "data_product"
    __table_args__ = (
        UniqueConstraint("product_id", "version", name="uq_data_product_version"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    product_id: str = Field(index=True)
    version: int = Field(default=1, index=True)
    value: float
    # Full DataProductMetadata (including composition_lineage) as JSONB.
    asset_metadata: dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))
    created_at: datetime = Field(
        sa_column=Column(DateTime(timezone=True), nullable=False, default=_utcnow)
    )