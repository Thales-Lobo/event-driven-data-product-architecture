"""API connector -> base_industrial_production (alternate live source).

Mocks pulling a JSON payload from an external HTTP API. In a real deployment
this would issue an ``httpx`` GET; here we return a canned response so the demo
runs fully offline. Re-ingesting industrial production via this source is what
we use to *trigger* a live GDP recalculation end-to-end.
"""

from __future__ import annotations

from typing import Any

from datamesh.domain.ingestion import IngestionPayload
from datamesh.domain.metadata import (
    BusinessMetadata,
    GovernanceMetadata,
    SensitivityLevel,
    TechnicalMetadata,
)


class ApiIndustrialProductionSource:
    """Ingests an updated industrial production reading from a mock JSON API."""

    name = "api_industrial"

    def __init__(self, value: float = 121.7, reference_period: str = "2026-02") -> None:
        # Parameterized so the trigger script can simulate a new monthly reading.
        self._value = value
        self._reference_period = reference_period

    def _fetch(self) -> dict[str, Any]:
        """Simulate an HTTP GET returning JSON.

        Real implementation:
            async with httpx.AsyncClient() as c:
                return (await c.get(url)).json()
        """
        return {
            "reference_period": self._reference_period,
            "industrial_production_index": self._value,
        }

    def read(self) -> IngestionPayload:
        raw = self._fetch()
        return IngestionPayload(
            base_data_id="base_industrial_production",
            value=float(raw["industrial_production_index"]),
            reference_period=str(raw["reference_period"]),
            raw_data_pointer="https://mock-api.local/fiesp/industrial/latest",
            technical=TechnicalMetadata(
                source_system="FIESP_REST_API",
                storage_format="json",
                schema_version="2.0.0",
            ),
            business=BusinessMetadata(
                domain="economic_indicators",
                description="Monthly industrial production index (API feed).",
                semantic_tags=["economy", "industry", "manufacturing", "api"],
            ),
            governance=GovernanceMetadata(
                owner_domain="economic_indicators",
                sensitivity_level=SensitivityLevel.PUBLIC,
                access_control="public_read",
            ),
        )