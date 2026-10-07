import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.messaging import retry_delay
from app.models import Currency, OutboxEvent, Payment, PaymentStatus
from app.outbox.relay import publish_pending


class FakeGateway:
    def __init__(self, result: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        self.result = result
        self.calls = 0

    async def charge(self, payment: Payment) -> PaymentStatus:
        self.calls += 1
        return self.result


class FakeNotifier:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[Payment] = []

    async def notify(self, payment: Payment) -> None:
        if self.fail:
            raise ConnectionError("webhook is down")
        self.sent.append(payment)


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict] = []

    async def publish(self, message, **kwargs) -> None:
        if self.fail:
            raise ConnectionError("broker is down")
        self.published.append({"message": message, **kwargs})


async def add_payment(session_factory, status=PaymentStatus.PENDING) -> Payment:
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="test",
        metadata_={},
        status=status,
        idempotency_key=str(uuid.uuid4()),
        request_hash="x" * 64,
        webhook_url="http://example.com/hook",
    )
    async with session_factory() as session, session.begin():
        session.add(payment)
        session.add(
            OutboxEvent(
                aggregate_id=payment.id,
                event_type="payment.created",
                routing_key="payments.new",
                payload={"payment_id": str(payment.id)},
            )
        )
    return payment


async def get_status(session_factory, payment_id) -> Payment:
    async with session_factory() as session:
        return await session.get(Payment, payment_id)


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_pending_payment_is_charged_and_notified(session_factory, result):
    payment = await add_payment(session_factory)
    gateway, notifier = FakeGateway(result), FakeNotifier()

    await PaymentProcessor(session_factory, gateway, notifier).process(payment.id)

    stored = await get_status(session_factory, payment.id)
    assert stored.status == result
    assert stored.processed_at is not None
    assert gateway.calls == 1
    assert [p.status for p in notifier.sent] == [result]


async def test_processed_payment_is_not_charged_again(session_factory):
    payment = await add_payment(session_factory, status=PaymentStatus.SUCCEEDED)
    gateway, notifier = FakeGateway(PaymentStatus.FAILED), FakeNotifier()

    await PaymentProcessor(session_factory, gateway, notifier).process(payment.id)

    assert gateway.calls == 0
    assert (await get_status(session_factory, payment.id)).status == PaymentStatus.SUCCEEDED
    assert len(notifier.sent) == 1


async def test_webhook_failure_keeps_result_for_retry(session_factory):
    payment = await add_payment(session_factory)
    gateway = FakeGateway()
    processor = PaymentProcessor(session_factory, gateway, FakeNotifier(fail=True))

    with pytest.raises(ConnectionError):
        await processor.process(payment.id)
    assert (await get_status(session_factory, payment.id)).status == PaymentStatus.SUCCEEDED

    retry_notifier = FakeNotifier()
    await PaymentProcessor(session_factory, gateway, retry_notifier).process(payment.id)
    assert gateway.calls == 1
    assert len(retry_notifier.sent) == 1


async def test_unknown_payment_raises_not_found(session_factory):
    processor = PaymentProcessor(session_factory, FakeGateway(), FakeNotifier())
    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid.uuid4())


async def test_relay_publishes_and_marks_events(session_factory):
    payment = await add_payment(session_factory)
    broker = FakeBroker()

    assert await publish_pending(session_factory, broker, batch_size=10) == 1
    assert await publish_pending(session_factory, broker, batch_size=10) == 0

    assert len(broker.published) == 1
    assert broker.published[0]["message"] == {"payment_id": str(payment.id)}
    assert broker.published[0]["routing_key"] == "payments.new"
    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent))
    assert event.published_at is not None


async def test_relay_keeps_event_when_broker_fails(session_factory):
    await add_payment(session_factory)

    assert await publish_pending(session_factory, FakeBroker(fail=True), batch_size=10) == 0

    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent))
    assert event.published_at is None
    assert event.attempts == 1
    assert "broker is down" in event.last_error


def test_retry_delay_is_exponential():
    assert [retry_delay(n, 2.0) for n in (1, 2, 3)] == [2.0, 4.0, 8.0]
