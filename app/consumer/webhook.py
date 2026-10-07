import httpx

from app.models import Payment
from app.schemas import WebhookPayload


class WebhookNotifier:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def notify(self, payment: Payment) -> None:
        payload = WebhookPayload(
            event=f"payment.{payment.status.value}",
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
        response = await self._client.post(
            payment.webhook_url,
            content=payload.model_dump_json(),
            headers={
                "Content-Type": "application/json",
                "X-Payment-Id": str(payment.id),
                "X-Event": payload.event,
            },
        )
        response.raise_for_status()
