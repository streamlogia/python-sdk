"""
Example FastAPI app using the Log Ingestor SDK.

Install:  pip install fastapi uvicorn
Run:      LOGINGESTOR_API_KEY=... LOGINGESTOR_PROJECT_ID=... uvicorn fastapi_app:app
"""

from logingestor import LogIngestorClient
import logging
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── 1. Create the client ──────────────────────────────────────────────────────
# console=True  — logs are written to stdout/stderr AND sent to the ingestor.
# When running under systemd, stdout is captured by the journal and you can
# tail logs on the server with: journalctl -u your-service -f
#
# console=False (default) — logs go to the ingestor only. Nothing will appear
# in the terminal or journalctl on your server machine.
client = LogIngestorClient(
    api_key=os.environ["LOGINGESTOR_API_KEY"],
    project_id=os.environ["LOGINGESTOR_PROJECT_ID"],
    source="order-service",
    console=True,
)

# ── 2. Plug in the stdlib logger → ingestor ───────────────────────────────────
# The logging_handler routes all logger.xxx() calls through the client, so the
# client's console=True setting applies here too — no separate StreamHandler
# needed. Adding one would cause every log line to print twice.
logger = logging.getLogger("order-service")
logger.setLevel(logging.DEBUG)
logger.addHandler(client.logging_handler())


# ── 3. Lifespan: flush on shutdown ───────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("server starting")
    yield
    client.close()  # flush remaining logs before the process exits


# ── 4. Register the ASGI middleware once ─────────────────────────────────────
app = FastAPI(lifespan=lifespan)
app.add_middleware(client.asgi_middleware())


# ── 5. Route handlers ────────────────────────────────────────────────────────
class CreateOrderRequest(BaseModel):
    customer_id: str
    amount: float
    currency: str = "USD"


@app.post("/orders", status_code=201)
async def create_order(body: CreateOrderRequest):
    order_id = f"ord_{os.urandom(4).hex()}"

    # Business event — supplements the automatic access log.
    client.info("order created", meta={
        "order_id": order_id,
        "customer_id": body.customer_id,
        "amount": body.amount,
        "currency": body.currency,
    }, tags=["orders", f"customer:{body.customer_id}"])

    # Or via the stdlib logger.
    logger.info("order persisted", extra={"meta": {"order_id": order_id}})

    return {"orderId": order_id}


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    # Middleware already logs this — no manual log needed unless adding context.
    return {"orderId": order_id, "status": "confirmed"}


@app.get("/health")
async def health():
    return {"status": "ok"}
