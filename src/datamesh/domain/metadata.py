"""Four-dimensional active-metadata model (the "connective tissue").

The dissertation categorizes metadata into four dimensions -- technical,
operational, business, and governance -- plus provenance/lineage. We model each
dimension as an isolated, strictly-validated Pydantic block (``extra="forbid"``)
so that malformed or unexpected fields are rejected *before* the record ever
reaches Postgres or the message broker. This is the computational enforcement
the paper argues cannot rely on "organizational discipline" alone.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class SensitivityLevel(str, Enum):
    """Governance sensitivity, driving API gateway routing/restriction."""

    PUBLIC = "public"
    INTERNAL = "internal"
    RESTRICTED = "restricted"


class QualityStatus(str, Enum):
    """Operational health signal exposed to downstream consumers."""

    PENDING = "pending"
    VALIDATED = "validated"
    FAILED = "failed"


class ProductStatus(str, Enum):
    """Lifecycle status of a computed data product version."""

    STABLE = "stable"
    STALE = "stale"
    RECALCULATING = "recalculating"


class TechnicalMetadata(BaseModel):
    """Schema/storage details that make an asset machine-readable."""

    model_config = ConfigDict(extra="forbid")

    source_system: str = Field(description="External system that produced the asset.")
    storage_format: str = Field(default="json")
    compression: str | None = None
    schema_version: str = Field(default="1.0.0")


class OperationalMetadata(BaseModel):
    """Freshness and observability metrics."""

    model_config = ConfigDict(extra="forbid")

    update_frequency: str = Field(default="on_demand")
    last_refresh: datetime | None = None
    data_quality_score: float | None = Field(default=None, ge=0, le=100)
    quality_status: QualityStatus = QualityStatus.PENDING


class BusinessMetadata(BaseModel):
    """Contextual/semantic information bridging data and its real-world meaning."""

    model_config = ConfigDict(extra="forbid")

    domain: str
    description: str
    semantic_tags: list[str] = Field(default_factory=list)


class GovernanceMetadata(BaseModel):
    """Access-control, ownership and compliance markers."""

    model_config = ConfigDict(extra="forbid")

    owner_domain: str
    sensitivity_level: SensitivityLevel = SensitivityLevel.INTERNAL
    access_control: str = "restricted_internal"
    compliance_tags: list[str] = Field(default_factory=list)


class Provenance(BaseModel):
    """Immutable pointer to raw origin -- the foundation of Traceability (TR)."""

    model_config = ConfigDict(extra="forbid")

    extraction_timestamp: datetime
    reference_period: str
    raw_data_pointer: str = Field(
        description="Immutable pointer to the exact raw file/record ingested."
    )


class CompositionLineageEntry(BaseModel):
    """A single upstream dependency actually consumed by a product version.

    Captures not merely *which* base data was used, but the exact ``version`` and
    ``timestamp_used``. This is what enables re-runability: an auditor can prove
    why an indicator held value X at moment Y even after sources were revised.
    """

    model_config = ConfigDict(extra="forbid")

    base_data_id: str
    version: int
    timestamp_used: datetime


class BaseDataMetadata(BaseModel):
    """Full 4-dimensional metadata envelope for a Base Data asset.

    ``provenance`` + ``technical.source_system`` are mandatory, structurally
    guaranteeing that every Base Data record tracks its external source of origin.
    """

    model_config = ConfigDict(extra="forbid")

    technical: TechnicalMetadata
    operational: OperationalMetadata
    business: BusinessMetadata
    governance: GovernanceMetadata
    provenance: Provenance


class DataProductMetadata(BaseModel):
    """Full 4-dimensional metadata envelope for a Data Product.

    ``composition_lineage`` is enforced with ``min_length=1``: a product cannot
    be persisted without declaring at least one upstream dependency, making the
    lineage a hard structural invariant rather than optional documentation.
    """

    model_config = ConfigDict(extra="forbid")

    technical: TechnicalMetadata
    operational: OperationalMetadata
    business: BusinessMetadata
    governance: GovernanceMetadata
    composition_lineage: list[CompositionLineageEntry] = Field(min_length=1)
    rule_id: str
    rule_version: str = "v1.0"