import logging
from typing import Any

from fastapi import FastAPI, Request, Response

logger = logging.getLogger("uvicorn.error")
app = FastAPI(title="Webhook receiver (demo)")
received: list[dict[str, Any]] = []


@app.post("/webhook")
async def receive(request: Request) -> dict[str, str]:
    payload = await request.json()
    received.append(payload)
    logger.info("Webhook received: %s", payload)
    return {"status": "ok"}


@app.post("/webhook/fail")
async def receive_and_fail(request: Request) -> Response:
    logger.info("Webhook received (answering 500): %s", await request.body())
    return Response(status_code=500)


@app.get("/webhook")
async def list_received() -> list[dict[str, Any]]:
    return received
