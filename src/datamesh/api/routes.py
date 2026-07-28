"""HTTP routes: the Data Product "Data Shop" and ingestion trigger.

Note the strict separation of concerns: routes only translate HTTP <-> domain
calls. Ingestion emits events; the *reactive recalculation* happens out-of-band
in the orchestrator container -- never inside a request cycle.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datamesh.api.dependencies import AppState
from datamesh.domain.events import BaseDataUpdatedEvent
from datamesh.infrastructure.database import get_session
from datamesh.infrastructure.models import BaseDataRecord, DataProductRecord

router = APIRouter()


def _state(request: Request) -> AppState:
    """Retrieve the typed application state from the FastAPI app."""
    return request.app.state.app_state  # type: ignore[no-any-return]


@router.get("/health", tags=["system"])
async def health() -> dict[str, str]:
    """Liveness probe used by docker-compose healthchecks."""
    return {"status": "ok"}


@router.get("/connectors", tags=["ingestion"])
async def list_connectors(request: Request) -> dict[str, list[str]]:
    """Expose the available heterogeneous source connectors."""
    return {"connectors": sorted(_state(request).connectors.keys())}


@router.post(
    "/ingest/{connector_name}",
    tags=["ingestion"],
    status_code=status.HTTP_202_ACCEPTED,
    response_model=BaseDataUpdatedEvent,
)
async def ingest(connector_name: str, request: Request) -> BaseDataUpdatedEvent:
    """Run the named connector, persist a Base Data version, emit its event."""
    state = _state(request)
    connector = state.connectors.get(connector_name)
    if connector is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown connector '{connector_name}'.",
        )
    payload = connector.read()
    return await state.ingestion_service.ingest(payload)


@router.get("/base-data/{base_data_id}", tags=["catalog"])
async def get_base_data(
    base_data_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, Any]:
    """Return the latest version of a Base Data asset with its metadata."""
    record = await session.scalar(
        select(BaseDataRecord)
        .where(BaseDataRecord.base_data_id == base_data_id)
        .order_by(BaseDataRecord.version.desc())
        .limit(1)
    )
    if record is None:
        raise HTTPException(status_code=404, detail="Base data not found.")
    return {
        "base_data_id": record.base_data_id,
        "version": record.version,
        "value": record.value,
        "reference_period": record.reference_period,
        "metadata": record.asset_metadata,
        "created_at": record.created_at,
    }


@router.get("/data-products/{product_id}", tags=["catalog"])
async def get_data_product(
    product_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, Any]:
    """Return the latest computed version of a Data Product (the GDP indicator)."""
    record = await session.scalar(
        select(DataProductRecord)
        .where(DataProductRecord.product_id == product_id)
        .order_by(DataProductRecord.version.desc())
        .limit(1)
    )
    if record is None:
        raise HTTPException(status_code=404, detail="Data product not yet computed.")
    return {
        "product_id": record.product_id,
        "version": record.version,
        "value": record.value,
        "composition_lineage": record.asset_metadata["composition_lineage"],
        "metadata": record.asset_metadata,
        "created_at": record.created_at,
    }


@router.get("/data-products/{product_id}/history", tags=["catalog"])
async def get_data_product_history(
    product_id: str,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[dict[str, Any]]:
    """Return every computed version -- the auditable trail of the indicator."""
    records = (
        await session.scalars(
            select(DataProductRecord)
            .where(DataProductRecord.product_id == product_id)
            .order_by(DataProductRecord.version.asc())
        )
    ).all()
    return [
        {
            "version": r.version,
            "value": r.value,
            "composition_lineage": r.asset_metadata["composition_lineage"],
            "created_at": r.created_at,
        }
        for r in records
    ]