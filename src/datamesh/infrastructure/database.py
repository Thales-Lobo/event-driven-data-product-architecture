"""Async database engine and session factory.

We use SQLAlchemy's async engine (asyncpg driver) with SQLModel table classes.
``expire_on_commit=False`` lets us safely read attributes off ORM objects after
commit inside the same request/handler without triggering lazy IO.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlmodel import SQLModel

from datamesh.config import get_settings

# Import models so their tables register on SQLModel.metadata before create_all.
from datamesh.infrastructure import models  # noqa: F401

_settings = get_settings()

engine: AsyncEngine = create_async_engine(
    _settings.async_database_url,
    echo=False,
    pool_pre_ping=True,
)

async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db() -> None:
    """Create tables if they do not yet exist (idempotent, prototype-friendly).

    In production this would be replaced by Alembic migrations; for the
    prototype, create_all keeps the developer loop fast.
    """
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a scoped async session."""
    async with async_session_factory() as session:
        yield session