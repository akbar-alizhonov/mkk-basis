from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import Settings

PAYMENTS_NEW = "payments.new"
PAYMENTS_DLQ = "payments.new.dlq"
ATTEMPT_HEADER = "x-attempt"
ERROR_HEADER = "x-last-error"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

payments_new_queue = RabbitQueue(
    PAYMENTS_NEW,
    durable=True,
    routing_key=PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": PAYMENTS_DLQ,
    },
)
dead_letter_queue = RabbitQueue(PAYMENTS_DLQ, durable=True, routing_key=PAYMENTS_DLQ)


def retry_delay(attempt: int, base_delay: float) -> float:
    return base_delay * 2 ** (attempt - 1)


def retry_queue(attempt: int, settings: Settings) -> RabbitQueue:
    return RabbitQueue(
        f"{PAYMENTS_NEW}.retry.{attempt}",
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay(attempt, settings.retry_base_delay) * 1000),
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
        },
    )


def create_broker(settings: Settings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            prefetch_count=settings.consumer_prefetch,
            publisher_confirms=True,
            on_return_raises=True,
        ),
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    exchange = await broker.declare_exchange(payments_exchange)
    dlx = await broker.declare_exchange(dead_letter_exchange)

    main_queue = await broker.declare_queue(payments_new_queue)
    await main_queue.bind(exchange, routing_key=PAYMENTS_NEW)

    dlq = await broker.declare_queue(dead_letter_queue)
    await dlq.bind(dlx, routing_key=PAYMENTS_DLQ)

    for attempt in range(1, settings.max_attempts):
        await broker.declare_queue(retry_queue(attempt, settings))
