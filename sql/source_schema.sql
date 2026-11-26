-- Source OLTP database (shop)
CREATE TABLE IF NOT EXISTS orders (
    order_id     TEXT PRIMARY KEY,
    customer_id  TEXT          NOT NULL,
    status       TEXT          NOT NULL,
    amount       NUMERIC(12,2) NOT NULL,
    currency     CHAR(3)       NOT NULL DEFAULT 'INR',
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- incremental extracts filter on updated_at
CREATE INDEX IF NOT EXISTS ix_orders_updated_at ON orders (updated_at);
