"""
Example Flask app using the Log Ingestor SDK.

Install:  pip install flask
Run:      LOGINGESTOR_API_KEY=... LOGINGESTOR_PROJECT_ID=... python flask_app.py
"""

from logingestor import LogIngestorClient
import logging
import os
import signal
import sys

from flask import Flask, jsonify, request

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
    source="payment-service",
    console=True,
)

# Flush on shutdown so buffered logs are not lost.
signal.signal(signal.SIGTERM, lambda *_: (client.close(), sys.exit(0)))

# ── 2. Plug in the stdlib logger → ingestor ───────────────────────────────────
# The logging_handler routes all logger.xxx() calls through the client, so the
# client's console=True setting applies here too — no separate StreamHandler
# needed. Adding one would cause every log line to print twice.
logger = logging.getLogger("payment-service")
logger.setLevel(logging.DEBUG)
logger.addHandler(client.logging_handler())

# ── 3. Register the middleware once — auto-logs every HTTP request ────────────
app = Flask(__name__)
client.flask_middleware(app)

# ── 4. Route handlers ─────────────────────────────────────────────────────────


@app.post("/payments")
def create_payment():
    data = request.get_json()
    customer_id = data.get("customerId")
    amount = data.get("amount")

    payment_id = f"pay_{os.urandom(4).hex()}"

    # Business event — supplements the automatic access log with domain context.
    client.info("payment processed", meta={
        "payment_id": payment_id,
        "customer_id": customer_id,
        "amount": amount,
    }, tags=["payments", f"customer:{customer_id}"])

    # Or use the stdlib logger (goes to the same ingestor via the handler above).
    logger.info("payment record persisted", extra={
                "meta": {"payment_id": payment_id}})

    return jsonify({"paymentId": payment_id}), 201


@app.get("/payments/<payment_id>")
def get_payment(payment_id):
    # Middleware already logs this request automatically — no manual log needed
    # unless you want to add business context.
    return jsonify({"paymentId": payment_id, "status": "completed"})


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    logger.info("server starting", extra={"meta": {"port": 5000}})
    app.run(port=5000)
