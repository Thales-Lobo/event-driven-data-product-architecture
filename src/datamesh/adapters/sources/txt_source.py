"""TXT connector -> base_services_revenue.

Parses a pipe-delimited flat text file. Demonstrates that adding a new format is
purely local: no other layer changes.
"""

from __future__ import annotations

from pathlib import Path

from datamesh.domain.ingestion import IngestionPayload
from datamesh.domain.metadata import (
    BusinessMetadata,
    GovernanceMetadata,
    SensitivityLevel,
    TechnicalMetadata,
)


class TxtServicesRevenueSource:
    """Ingests services revenue from a ``key|period|value`` text file."""

    name = "txt_services"

    def __init__(self, file_path: Path) -> None:
        self._file_path = file_path

    def read(self) -> IngestionPayload:
        content = self._file_path.read_text(encoding="utf-8").strip().splitlines()
        if not content:
            raise ValueError(f"TXT source '{self._file_path}' is empty.")

        _, reference_period, raw_value = content[-1].split("|")
        return IngestionPayload(
            base_data_id="base_services_revenue",
            value=float(raw_value),
            reference_period=reference_period,
            raw_data_pointer=f"file://{self._file_path.resolve()}",
            technical=TechnicalMetadata(
                source_system="Treasury_Inbound_TXT",
                storage_format="txt",
                schema_version="1.0.0",
            ),
            business=BusinessMetadata(
                domain="economic_indicators",
                description="Monthly services sector revenue.",
                semantic_tags=["economy", "services", "revenue"],
            ),
            governance=GovernanceMetadata(
                owner_domain="fiscal_domain",
                sensitivity_level=SensitivityLevel.RESTRICTED,
                access_control="restricted_internal",
                compliance_tags=["LGPD_compliant"],
            ),
        )