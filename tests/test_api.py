import asyncio
import uuid

from sqlalchemy import func, select

from app.messaging import PAYMENTS_NEW
from app.models import OutboxEvent, Payment

URL = "/api/v1/payments"
BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://example.com/webhook",
}


def key() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def count(session_factory, model) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def test_create_payment_writes_payment_and_outbox_event(client, session_factory):
    response = await client.post(URL, json=BODY, headers=key())

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert {"payment_id", "created_at"} <= body.keys()

    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent))
    assert event.routing_key == PAYMENTS_NEW
    assert event.payload == {"payment_id": body["payment_id"]}
    assert event.published_at is None


async def test_same_idempotency_key_returns_same_payment(client, session_factory):
    headers = key()
    first = await client.post(URL, json=BODY, headers=headers)
    second = await client.post(URL, json=BODY, headers=headers)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_same_idempotency_key_with_other_body_conflicts(client):
    headers = key()
    await client.post(URL, json=BODY, headers=headers)
    response = await client.post(URL, json={**BODY, "amount": "1.00"}, headers=headers)

    assert response.status_code == 409


async def test_concurrent_requests_with_same_key_create_one_payment(client, session_factory):
    headers = key()
    requests = (client.post(URL, json=BODY, headers=headers) for _ in range(5))
    responses = await asyncio.gather(*requests)

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxEvent) == 1


async def test_idempotency_key_is_required(client):
    response = await client.post(URL, json=BODY)
    assert response.status_code == 422


async def test_invalid_body_is_rejected(client):
    for bad in (
        {**BODY, "amount": "0"},
        {**BODY, "amount": "1.001"},
        {**BODY, "currency": "GBP"},
        {**BODY, "webhook_url": "not-a-url"},
    ):
        response = await client.post(URL, json=bad, headers=key())
        assert response.status_code == 422, bad


async def test_api_key_is_required(client):
    created = await client.post(URL, json=BODY, headers=key())
    payment_url = f"{URL}/{created.json()['payment_id']}"

    for headers in ({"X-API-Key": ""}, {"X-API-Key": "wrong"}):
        assert (await client.post(URL, json=BODY, headers={**key(), **headers})).status_code == 401
        assert (await client.get(payment_url, headers=headers)).status_code == 401


async def test_get_payment(client):
    created = (await client.post(URL, json=BODY, headers=key())).json()

    response = await client.get(f"{URL}/{created['payment_id']}")

    assert response.status_code == 200
    payment = response.json()
    assert payment["id"] == created["payment_id"]
    assert payment["amount"] == "100.50"
    assert payment["currency"] == "RUB"
    assert payment["metadata"] == {"order_id": 42}
    assert payment["status"] == "pending"
    assert payment["processed_at"] is None


async def test_get_unknown_payment_returns_404(client):
    response = await client.get(f"{URL}/{uuid.uuid4()}")
    assert response.status_code == 404
