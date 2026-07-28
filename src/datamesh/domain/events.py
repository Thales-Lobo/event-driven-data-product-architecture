"""Event payloads for the event-driven control plane.

Events are intentionally *thin*: they carry identifiers and the minimum state
needed to trigger downstream reactions, never the full asset. This preserves the
structural decoupling the paper emphasizes -- the producing domain publishes
"base data X changed" and remains ignorant of how many products depend on it.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from datamesh.domain.metadata import CompositionLineageEntry


class BaseDataUpdatedEvent(BaseModel):
    """Emitted by the ingestion service whenever a Base Data version is created."""

    model_config = ConfigDict(extra="forbid")

    event_type: Literal["base_data.updated"] = "base_data.updated"
    base_data_id: str
    version: int
    value: float
    reference_period: str
    source_system: str
    occurred_at: datetime


class DataProductRecalculatedEvent(BaseModel):
    """Emitted by the orchestrator after a product is reactively recomputed."""

    model_config = ConfigDict(extra="forbid")

    event_type: Literal["data_product.recalculated"] = "data_product.recalculated"
    product_id: str
    version: int
    value: float
    triggered_by: str = Field(description="base_data_id that triggered the recalculation.")
    composition_lineage: list[CompositionLineageEntry]
    occurred_at: datetime