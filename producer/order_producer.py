"""Simulates an order system.

Each change is written to the source Postgres table (for the batch path) and
published as a change event to Kafka (for the streaming path), similar to an
outbox/CDC feed. A small share of events are deliberately duplicated or
malformed so the data-quality path has something to catch.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("ORDERS_TOPIC", "orders.cdc")
SOURCE_DSN = os.getenv("SOURCE_DSN", "dbname=shop user=etl password=etl host=postgres")
EVENTS_PER_SEC = float(os.getenv("EVENTS_PER_SEC", "5"))

NEXT_STATUS = {"CREATED": "PAID", "PAID": "SHIPPED", "SHIPPED": "DELIVERED"}

UPSERT = """
INSERT INTO orders (order_id, customer_id, status, amount, currency, updated_at)
VALUES (%(order_id)s, %(customer_id)s, %(status)s, %(amount)s, %(currency)s, %(updated_at)s)
ON CONFLICT (order_id) DO UPDATE
SET status = EXCLUDED.status, updated_at = EXCLUDED.updated_at
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_order() -> dict:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"c-{random.randint(1, 500):04d}",
        "status": "CREATED",
        "amount": f"{random.uniform(99, 25000):.2f}",
        "currency": "INR",
        "updated_at": now_iso(),
    }


def main() -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    conn = psycopg2.connect(SOURCE_DSN)
    conn.autocommit = True
    open_orders: dict[str, dict] = {}

    while True:
        if open_orders and random.random() < 0.6:
            order = open_orders[random.choice(list(open_orders))]
            if random.random() < 0.05:
                order["status"] = "CANCELLED"
            else:
                order["status"] = NEXT_STATUS[order["status"]]
            order["updated_at"] = now_iso()
            if order["status"] in ("DELIVERED", "CANCELLED"):
                open_orders.pop(order["order_id"])
        else:
            order = new_order()
            open_orders[order["order_id"]] = order

        with conn.cursor() as cur:
            cur.execute(UPSERT, order)

        event = dict(order, op="u" if order["status"] != "CREATED" else "c")
        payload = json.dumps(event)
        if random.random() < 0.02:          # malformed event -> dead-letter table
            payload = json.dumps(dict(event, status="LOST"))
        producer.produce(TOPIC, key=order["order_id"], value=payload)
        if random.random() < 0.03:          # duplicate delivery -> deduplicated downstream
            producer.produce(TOPIC, key=order["order_id"], value=payload)
        producer.poll(0)
        time.sleep(1 / EVENTS_PER_SEC)


if __name__ == "__main__":
    main()
