"""Thin async Kafka producer wrapping aiokafka.

Events are serialized as JSON via Pydantic. Keys are the entity id so all events
for a given asset land on the same partition, preserving per-entity ordering --
critical for correct "latest version wins" reasoning downstream.
"""

from __future__ import annotations

from aiokafka import AIOKafkaProducer
from pydantic import BaseModel


class EventProducer:
    """Lifecycle-managed singleton producer shared across a process."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            enable_idempotence=True,  # exactly-once *produce* semantics
            acks="all",
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish(self, topic: str, key: str, event: BaseModel) -> None:
        """Publish a Pydantic event, blocking until the broker acknowledges."""
        if self._producer is None:  # pragma: no cover - guard rail
            raise RuntimeError("EventProducer.start() must be called first.")
        await self._producer.send_and_wait(
            topic,
            key=key.encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
        )