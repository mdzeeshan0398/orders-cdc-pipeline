"""Streaming path: Kafka -> Spark Structured Streaming -> Postgres warehouse.

* parses order change events from Kafka
* drops exact replays with a watermark-bounded dropDuplicates
* sends records that fail data-quality rules to dw.dead_letter
* keeps only the latest version of each order per micro-batch
* upserts into dw.fact_orders, ignoring events older than what is stored
"""
import os

import psycopg2
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("ORDERS_TOPIC", "orders.cdc")
JDBC_URL = os.getenv("WAREHOUSE_JDBC", "jdbc:postgresql://postgres:5432/warehouse")
DB_USER = os.getenv("DB_USER", "etl")
DB_PASSWORD = os.getenv("DB_PASSWORD", "etl")
PG_DSN = os.getenv("WAREHOUSE_DSN", "dbname=warehouse user=etl password=etl host=postgres")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/orders")

VALID_STATUSES = ["CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]

SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("status", StringType()),
    StructField("amount", StringType()),
    StructField("currency", StringType()),
    StructField("updated_at", StringType()),
    StructField("op", StringType()),
])

MERGE_SQL = """
INSERT INTO dw.fact_orders (order_id, customer_id, status, amount, currency, updated_at, source)
SELECT order_id, customer_id, status, amount, currency, updated_at, 'stream'
FROM dw.stg_orders
ON CONFLICT (order_id) DO UPDATE
SET status = EXCLUDED.status,
    amount = EXCLUDED.amount,
    currency = EXCLUDED.currency,
    updated_at = EXCLUDED.updated_at,
    source = EXCLUDED.source,
    loaded_at = now()
WHERE dw.fact_orders.updated_at < EXCLUDED.updated_at;
"""

JDBC_OPTS = {"url": JDBC_URL, "user": DB_USER, "password": DB_PASSWORD,
             "driver": "org.postgresql.Driver"}


def parse(raw: DataFrame) -> DataFrame:
    parsed = raw.select(
        F.col("value").cast("string").alias("raw"),
        F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
    ).select("raw", "e.*")
    return (parsed
            .withColumn("status", F.upper(F.trim("status")))
            .withColumn("currency", F.upper(F.coalesce(F.col("currency"), F.lit("INR"))))
            .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
            .withColumn("updated_at", F.to_timestamp("updated_at")))


def quality_flag(df: DataFrame) -> DataFrame:
    reason = (F.when(F.col("order_id").isNull(), "missing order_id")
              .when(F.col("customer_id").isNull(), "missing customer_id")
              .when(F.col("updated_at").isNull(), "bad updated_at")
              .when(F.col("amount").isNull() | (F.col("amount") < 0), "bad amount")
              .when(~F.col("status").isin(VALID_STATUSES), "unknown status"))
    return df.withColumn("reject_reason", reason)


def write_batch(batch: DataFrame, batch_id: int) -> None:
    batch = batch.persist()

    (batch.filter("reject_reason IS NOT NULL")
          .select("raw", F.col("reject_reason").alias("reason"))
          .write.mode("append").format("jdbc")
          .options(dbtable="dw.dead_letter", **JDBC_OPTS).save())

    latest = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    clean = (batch.filter("reject_reason IS NULL")
                  .withColumn("rn", F.row_number().over(latest))
                  .filter("rn = 1")
                  .select("order_id", "customer_id", "status", "amount", "currency", "updated_at"))

    if clean.take(1):
        (clean.write.mode("overwrite").option("truncate", "true").format("jdbc")
              .options(dbtable="dw.stg_orders", **JDBC_OPTS).save())
        with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
            cur.execute(MERGE_SQL)
    batch.unpersist()


def main() -> None:
    spark = SparkSession.builder.appName("orders-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .load())

    events = (quality_flag(parse(raw))
              .withWatermark("updated_at", "10 minutes")
              .dropDuplicates(["order_id", "updated_at", "status"]))

    (events.writeStream
           .foreachBatch(write_batch)
           .option("checkpointLocation", CHECKPOINT)
           .trigger(processingTime="10 seconds")
           .start()
           .awaitTermination())


if __name__ == "__main__":
    main()
