"""CSV connector -> base_industrial_production.

Reads the most recent row of an industrial-production CSV. Represents a domain
that drops periodic flat files (a very common public-sector reality).
"""

from __future__ import annotations

import csv
from pathlib import Path

from datamesh.domain.ingestion import IngestionPayload
from datamesh.domain.metadata import (
    BusinessMetadata,
    GovernanceMetadata,
    SensitivityLevel,
    TechnicalMetadata,
)


class CsvIndustrialProductionSource:
    """Ingests the industrial production index from a CSV file."""

    name = "csv_industrial"

    def __init__(self, file_path: Path) -> None:
        self._file_path = file_path

    def read(self) -> IngestionPayload:
        with self._file_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        if not rows:
            raise ValueError(f"CSV source '{self._file_path}' is empty.")

        latest = rows[-1]  # convention: last row is the newest reading
        return IngestionPayload(
            base_data_id="base_industrial_production",
            value=float(latest["industrial_production_index"]),
            reference_period=latest["reference_period"],
            raw_data_pointer=f"file://{self._file_path.resolve()}",
            technical=TechnicalMetadata(
                source_system="FIESP_Inbound_CSV",
                storage_format="csv",
                schema_version="1.0.2",
            ),
            business=BusinessMetadata(
                domain="economic_indicators",
                description="Monthly industrial production index.",
                semantic_tags=["economy", "industry", "manufacturing"],
            ),
            governance=GovernanceMetadata(
                owner_domain="economic_indicators",
                sensitivity_level=SensitivityLevel.PUBLIC,
                access_control="public_read",
            ),
        )