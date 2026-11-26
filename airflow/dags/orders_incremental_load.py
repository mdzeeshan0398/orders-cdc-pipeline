"""Batch path: watermark-based incremental load, source Postgres -> warehouse.

1. read the last high-water mark for this pipeline
2. extract only rows changed since then (up to a cutoff), validate them,
   keep the latest version per order and merge into dw.fact_orders
3. reconcile: extracted = loaded + rejected, and staged rows match the
   fact table on count and amount total
4. advance the watermark only if reconciliation passed
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

sys.path.insert(0, "/opt/project")  # repo root is mounted here (see docker-compose.yml)
from pipeline.transforms import latest_per_key, reconcile, split_valid  # noqa: E402

PIPELINE = "orders_batch"
SOURCE_CONN = "shop_source"
WAREHOUSE_CONN = "warehouse"

MERGE_SQL = """
INSERT INTO dw.fact_orders (order_id, customer_id, status, amount, currency, updated_at, source)
SELECT order_id, customer_id, status, amount, currency, updated_at, 'batch'
FROM dw.stg_orders_batch
ON CONFLICT (order_id) DO UPDATE
SET status = EXCLUDED.status,
    amount = EXCLUDED.amount,
    currency = EXCLUDED.currency,
    updated_at = EXCLUDED.updated_at,
    source = EXCLUDED.source,
    loaded_at = now()
WHERE dw.fact_orders.updated_at < EXCLUDED.updated_at;
"""


@dag(
    dag_id="orders_incremental_load",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "incremental", "reconciliation"],
)
def orders_incremental_load():

    @task
    def get_window() -> dict:
        wh = PostgresHook(postgres_conn_id=WAREHOUSE_CONN)
        last = wh.get_first(
            "SELECT last_value FROM dw.etl_watermark WHERE pipeline = %s", parameters=(PIPELINE,)
        )[0]
        # small lag so rows still being written are picked up next run
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=1)
        return {"from": last.isoformat(), "to": cutoff.isoformat()}

    @task
    def extract_and_load(window: dict) -> dict:
        src = PostgresHook(postgres_conn_id=SOURCE_CONN)
        rows = src.get_records(
            """SELECT order_id, customer_id, status, amount, currency, updated_at
               FROM orders WHERE updated_at > %s AND updated_at <= %s
               ORDER BY updated_at""",
            parameters=(window["from"], window["to"]),
        )
        cols = ("order_id", "customer_id", "status", "amount", "currency", "updated_at")
        good, bad = split_valid(dict(zip(cols, r)) for r in rows)
        latest = list(latest_per_key(good).values())

        wh = PostgresHook(postgres_conn_id=WAREHOUSE_CONN)
        with wh.get_conn() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE dw.stg_orders_batch")
            cur.executemany(
                """INSERT INTO dw.stg_orders_batch
                   (order_id, customer_id, status, amount, currency, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                [(o.order_id, o.customer_id, o.status, o.amount, o.currency, o.updated_at)
                 for o in latest],
            )
            cur.executemany(
                "INSERT INTO dw.dead_letter (raw, reason) VALUES (%s, %s)",
                [(str(raw), reason) for raw, reason in bad],
            )
            cur.execute(MERGE_SQL)
        return {"extracted": len(rows), "loaded": len(latest), "rejected": len(bad)}

    @task
    def reconcile_window(stats: dict) -> dict:
        # 1) source -> staging: every extracted row was either loaded or rejected
        #    (orders.order_id is the source primary key, so no rows collapse)
        if stats["extracted"] != stats["loaded"] + stats["rejected"]:
            raise ValueError(f"extract/load mismatch: {stats}")

        # 2) staging -> fact: every staged order is in the fact table at the same
        #    or a newer version, and amount totals agree
        wh = PostgresHook(postgres_conn_id=WAREHOUSE_CONN)
        s_count, s_sum = wh.get_first(
            "SELECT count(*), coalesce(sum(amount), 0) FROM dw.stg_orders_batch"
        )
        t_count, t_sum = wh.get_first(
            """SELECT count(*), coalesce(sum(f.amount), 0)
               FROM dw.stg_orders_batch s
               JOIN dw.fact_orders f
                 ON f.order_id = s.order_id AND f.updated_at >= s.updated_at"""
        )
        result = reconcile(s_count, t_count, Decimal(s_sum), Decimal(t_sum))
        if not result["matched"]:
            raise ValueError(f"reconciliation failed: {result}")
        return {k: str(v) for k, v in result.items()}

    @task
    def advance_watermark(window: dict, _recon: dict) -> str:
        wh = PostgresHook(postgres_conn_id=WAREHOUSE_CONN)
        wh.run(
            "UPDATE dw.etl_watermark SET last_value = %s, updated_at = now() WHERE pipeline = %s",
            parameters=(window["to"], PIPELINE),
        )
        return window["to"]

    window = get_window()
    stats = extract_and_load(window)
    recon = reconcile_window(stats)
    advance_watermark(window, recon)


orders_incremental_load()
