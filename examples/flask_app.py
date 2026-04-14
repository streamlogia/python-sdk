"""
Example Flask app using the Log Ingestor SDK.

Install:  pip install flask
Run:      LOGINGESTOR_API_KEY=... LOGINGESTOR_PROJECT_ID=... python flask_app.py
"""

import logging
import os
import sys

import logingestor
from flask import Flask, jsonify, request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

app = Flask(__name__)
client = logingestor.init(app, source="payment-service", console=True)

logger = logging.getLogger("payment-service")


@app.post("/payments")
def create_payment():
    data = request.get_json()
    customer_id = data.get("customerId")
    amount = data.get("amount")

    payment_id = f"pay_{os.urandom(4).hex()}"

    client.info("payment processed", meta={
        "payment_id": payment_id,
        "customer_id": customer_id,
        "amount": amount,
    }, tags=["payments", f"customer:{customer_id}"])

    logger.info("payment record persisted", extra={
                "meta": {"payment_id": payment_id}})
    return jsonify({"paymentId": payment_id}), 201


@app.get("/payments/<payment_id>")
def get_payment(payment_id):
    return jsonify({"paymentId": payment_id, "status": "completed"})


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    logger.info("server starting", extra={"meta": {"port": 5000}})
    app.run(port=5000)
