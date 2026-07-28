"""Source connector abstraction.

The platform must ingest N heterogeneous formats and remain open to new ones.
We express this with a ``Protocol`` (structural typing): any object exposing a
``name`` and a ``read() -> IngestionPayload`` is a valid connector. New formats
are added by writing a class and registering it -- no change to the ingestion
service or orchestrator (Open/Closed Principle).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from datamesh.domain.ingestion import IngestionPayload


@runtime_checkable
class SourceConnector(Protocol):
    """Reads one external source and normalizes it into an IngestionPayload."""

    name: str

    def read(self) -> IngestionPayload:
        """Fetch + parse the source, returning a validated payload."""
        ...