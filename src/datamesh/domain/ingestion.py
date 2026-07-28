"""Domain contract for ingestion payloads produced by source connectors.

A ``SourceConnector`` (see ``adapters/sources``) is responsible for reading a
heterogeneous external format (API/CSV/TXT/...) and normalizing it into this
single, validated shape. The ingestion service then enriches it with provenance
and operational metadata before persistence -- decoupling *how data is read*
from *how it is governed*.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from datamesh.domain.metadata import (
    BusinessMetadata,
    GovernanceMetadata,
    TechnicalMetadata,
)


class IngestionPayload(BaseModel):
    """Normalized, connector-agnostic representation of one Base Data reading."""

    model_config = ConfigDict(extra="forbid")

    base_data_id: str
    value: float
    reference_period: str
    raw_data_pointer: str
    technical: TechnicalMetadata
    business: BusinessMetadata
    governance: GovernanceMetadata