import logging
import uuid
from typing import Protocol

from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.models import Payment, PaymentStatus, utcnow

logger = logging.getLogger("consumer.processor")


class PaymentNotFoundError(Exception):
    pass


class Gateway(Protocol):
    async def charge(self, payment: Payment) -> PaymentStatus: ...


class Notifier(Protocol):
    async def notify(self, payment: Payment) -> None: ...


class PaymentProcessor:
    def __init__(
        self, session_factory: async_sessionmaker, gateway: Gateway, notifier: Notifier
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._load(payment_id)

        if payment.status == PaymentStatus.PENDING:
            result = await self._gateway.charge(payment)
            payment = await self._finalize(payment_id, result)
            logger.info("Payment %s processed: %s", payment_id, payment.status.value)
        else:
            logger.info("Payment %s already %s, skipping charge", payment_id, payment.status.value)

        await self._notifier.notify(payment)
        logger.info("Webhook for payment %s delivered to %s", payment_id, payment.webhook_url)
        return payment

    async def _load(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"Payment {payment_id} not found")
        return payment

    async def _finalize(self, payment_id: uuid.UUID, result: PaymentStatus) -> Payment:
        async with self._session_factory() as session, session.begin():
            payment = await session.scalar(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
                .values(status=result, processed_at=utcnow())
                .returning(Payment)
            )
        if payment is None:
            payment = await self._load(payment_id)
        return payment
