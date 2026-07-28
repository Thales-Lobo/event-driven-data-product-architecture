"""Connector registry -- the single place that maps a name to a connector.

Adding a new heterogeneous source is a one-line change here plus a connector
class. Nothing in the API or orchestrator needs to know the concrete formats.
"""

from __future__ import annotations

from pathlib import Path

from datamesh.adapters.sources.api_source import ApiIndustrialProductionSource
from datamesh.adapters.sources.base import SourceConnector
from datamesh.adapters.sources.csv_source import CsvIndustrialProductionSource
from datamesh.adapters.sources.txt_source import TxtServicesRevenueSource

# Data files bundled with the container image (see Dockerfile COPY data ./data).
_DATA_DIR = Path(__file__).resolve().parents[4] / "data"


def build_registry() -> dict[str, SourceConnector]:
    """Construct the default connector registry for the mock ecosystem."""
    return {
        "csv_industrial": CsvIndustrialProductionSource(
            _DATA_DIR / "industrial_production.csv"
        ),
        "txt_services": TxtServicesRevenueSource(_DATA_DIR / "services_revenue.txt"),
        "api_industrial": ApiIndustrialProductionSource(),
    }