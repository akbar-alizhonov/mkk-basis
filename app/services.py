import hashlib
import json
import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging import PAYMENTS_NEW
from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate, PaymentCreatedEvent

PAYMENT_CREATED = "payment.created"


class IdempotencyConflictError(Exception):
    """The idempotency key was already used with a different request body."""


def request_fingerprint(data: PaymentCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def _get_by_idempotency_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


def _ensure_same_request(payment: Payment, request_hash: str) -> Payment:
    if payment.request_hash != request_hash:
        raise IdempotencyConflictError
    return payment


async def create_payment(
    session: AsyncSession, data: PaymentCreate, idempotency_key: str
) -> tuple[Payment, bool]:
    """Creates a payment and an outbox event in one transaction. Returns (payment, created)."""
    request_hash = request_fingerprint(data)

    existing = await _get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return _ensure_same_request(existing, request_hash), False

    payment = Payment(
        id=uuid.uuid4(),
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        metadata_=data.metadata,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        webhook_url=str(data.webhook_url),
    )
    event = OutboxEvent(
        aggregate_id=payment.id,
        event_type=PAYMENT_CREATED,
        routing_key=PAYMENTS_NEW,
        payload=PaymentCreatedEvent(payment_id=payment.id).model_dump(mode="json"),
    )
    session.add_all([payment, event])
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        existing = await _get_by_idempotency_key(session, idempotency_key)
        if existing is None:
            raise
        return _ensure_same_request(existing, request_hash), False
    return payment, True
