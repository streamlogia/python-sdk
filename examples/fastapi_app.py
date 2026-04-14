"""
Example FastAPI app using the Log Ingestor SDK.

Install:  pip install fastapi uvicorn
Run:      LOGINGESTOR_API_KEY=... LOGINGESTOR_PROJECT_ID=... uvicorn fastapi_app:app
"""

import logging
import os
import sys

import logingestor
from fastapi import FastAPI
from pydantic import BaseModel

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

app = FastAPI()
client = logingestor.init(app, source="order-service", console=True)

logger = logging.getLogger("order-service")


class CreateOrderRequest(BaseModel):
    customer_id: str
    amount: float
    currency: str = "USD"


@app.post("/orders", status_code=201)
async def create_order(body: CreateOrderRequest):
    order_id = f"ord_{os.urandom(4).hex()}"

    client.info("order created", meta={
        "order_id": order_id,
        "customer_id": body.customer_id,
        "amount": body.amount,
        "currency": body.currency,
    }, tags=["orders", f"customer:{body.customer_id}"])

    logger.info("order persisted", extra={"meta": {"order_id": order_id}})
    return {"orderId": order_id}


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    return {"orderId": order_id, "status": "confirmed"}


@app.get("/health")
async def health():
    return {"status": "ok"}
