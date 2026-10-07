import asyncio
import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from app.config import Settings, get_settings
from app.consumer.gateway import EmulatedPaymentGateway
from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.consumer.webhook import WebhookNotifier
from app.db import create_engine, create_session_factory
from app.messaging import (
    ATTEMPT_HEADER,
    ERROR_HEADER,
    PAYMENTS_DLQ,
    create_broker,
    dead_letter_exchange,
    declare_topology,
    payments_exchange,
    payments_new_queue,
    retry_delay,
    retry_queue,
)
from app.schemas import PaymentCreatedEvent

logger = logging.getLogger("consumer")


def build_app(settings: Settings) -> FastStream:
    broker = create_broker(settings)
    state: dict[str, object] = {}

    async def on_startup() -> None:
        engine = create_engine(settings.database_url)
        http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
        state.update(engine=engine, http_client=http_client)
        state["processor"] = PaymentProcessor(
            create_session_factory(engine),
            EmulatedPaymentGateway(
                settings.gateway_min_delay,
                settings.gateway_max_delay,
                settings.gateway_success_rate,
            ),
            WebhookNotifier(http_client),
        )

    async def after_startup() -> None:
        await declare_topology(broker, settings)

    async def on_shutdown() -> None:
        await state["http_client"].aclose()
        await state["engine"].dispose()

    @broker.subscriber(
        payments_new_queue,
        payments_exchange,
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment(event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        attempt = int(message.headers.get(ATTEMPT_HEADER, 1))
        try:
            await state["processor"].process(event.payment_id)
        except PaymentNotFoundError as exc:
            await dead_letter(broker, event, message, attempt, exc)
        except Exception as exc:
            if attempt >= settings.max_attempts:
                await dead_letter(broker, event, message, attempt, exc)
            else:
                await schedule_retry(broker, settings, event, message, attempt, exc)

    return FastStream(
        broker,
        on_startup=[on_startup],
        after_startup=[after_startup],
        on_shutdown=[on_shutdown],
    )


async def schedule_retry(
    broker: RabbitBroker,
    settings: Settings,
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    attempt: int,
    exc: Exception,
) -> None:
    delay = retry_delay(attempt, settings.retry_base_delay)
    logger.warning(
        "Payment %s attempt %d/%d failed: %r. Retrying in %.1fs",
        event.payment_id,
        attempt,
        settings.max_attempts,
        exc,
        delay,
    )
    await broker.publish(
        event.model_dump(mode="json"),
        queue=retry_queue(attempt, settings),
        message_id=message.message_id,
        headers={ATTEMPT_HEADER: attempt + 1, ERROR_HEADER: repr(exc)[:1000]},
        persist=True,
    )


async def dead_letter(
    broker: RabbitBroker,
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    attempt: int,
    exc: Exception,
) -> None:
    logger.error(
        "Payment %s failed after %d attempt(s): %r. Moving to DLQ", event.payment_id, attempt, exc
    )
    await broker.publish(
        event.model_dump(mode="json"),
        exchange=dead_letter_exchange,
        routing_key=PAYMENTS_DLQ,
        message_id=message.message_id,
        headers={ATTEMPT_HEADER: attempt, ERROR_HEADER: repr(exc)[:1000]},
        persist=True,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(build_app(get_settings()).run())
