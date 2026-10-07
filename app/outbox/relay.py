import asyncio
import contextlib
import logging
import signal

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.messaging import ATTEMPT_HEADER, create_broker, declare_topology, payments_exchange
from app.models import OutboxEvent, utcnow

logger = logging.getLogger("outbox.relay")


async def publish_pending(
    session_factory: async_sessionmaker, broker: RabbitBroker, batch_size: int
) -> int:
    published = 0
    async with session_factory() as session, session.begin():
        events = await session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        for event in events:
            try:
                await broker.publish(
                    event.payload,
                    exchange=payments_exchange,
                    routing_key=event.routing_key,
                    message_id=str(event.id),
                    message_type=event.event_type,
                    headers={ATTEMPT_HEADER: 1},
                    persist=True,
                )
            except Exception as exc:
                logger.warning("Failed to publish outbox event %s: %r", event.id, exc)
                event.attempts += 1
                event.last_error = repr(exc)
                break
            event.published_at = utcnow()
            event.attempts += 1
            event.last_error = None
            published += 1
    return published


async def run(settings: Settings) -> None:
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    broker = create_broker(settings)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await broker.start()
    await declare_topology(broker, settings)
    logger.info("Outbox relay started")
    try:
        while not stop.is_set():
            try:
                published = await publish_pending(
                    session_factory, broker, settings.outbox_batch_size
                )
            except Exception:
                logger.exception("Outbox relay iteration failed")
                published = 0
            if published:
                logger.info("Published %d outbox event(s)", published)
            if published < settings.outbox_batch_size:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval)
    finally:
        await broker.stop()
        await engine.dispose()
        logger.info("Outbox relay stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run(get_settings()))
