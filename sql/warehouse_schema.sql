-- Warehouse database
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.fact_orders (
    order_id     TEXT PRIMARY KEY,
    customer_id  TEXT          NOT NULL,
    status       TEXT          NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    currency     CHAR(3)       NOT NULL,
    updated_at   TIMESTAMPTZ   NOT NULL,
    source       TEXT          NOT NULL,          -- 'stream' or 'batch'
    loaded_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- staging tables, truncated and reloaded on every micro-batch / DAG run
CREATE TABLE IF NOT EXISTS dw.stg_orders (
    order_id TEXT, customer_id TEXT, status TEXT, amount NUMERIC(12,2),
    currency CHAR(3), updated_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS dw.stg_orders_batch (LIKE dw.stg_orders);

-- records that failed data-quality rules
CREATE TABLE IF NOT EXISTS dw.dead_letter (
    id          BIGSERIAL PRIMARY KEY,
    raw         TEXT,
    reason      TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- high-water marks for incremental loads
CREATE TABLE IF NOT EXISTS dw.etl_watermark (
    pipeline   TEXT PRIMARY KEY,
    last_value TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO dw.etl_watermark (pipeline, last_value)
VALUES ('orders_batch', '1970-01-01T00:00:00Z')
ON CONFLICT (pipeline) DO NOTHING;

-- reporting view
CREATE OR REPLACE VIEW dw.v_daily_revenue AS
SELECT date_trunc('day', updated_at)::date AS day,
       count(*)                            AS orders,
       sum(amount)                         AS revenue
FROM dw.fact_orders
WHERE status IN ('PAID', 'SHIPPED', 'DELIVERED')
GROUP BY 1
ORDER BY 1;
