import sys
import os
import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

HADOOP_HOME = "C:\\hadoop"
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] += os.pathsep + os.path.join(HADOOP_HOME, "bin")

KAFKA_BROKER = "localhost:9092"
TOPIC = "stock_ticks_realtime"

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
}

CHECKPOINT_DIR = os.path.join(os.getcwd(), "checkpoints")

TICK_SCHEMA = StructType([
    StructField("time", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("symbol", StringType()),
    StructField("ingested_at", StringType()),
])

UPSERT_SQL = """
    INSERT INTO stock_candles
        (symbol, window_start, window_end,
         open_price, high_price, low_price, close_price,
         volume, tick_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, window_start)
    DO UPDATE SET
        window_end  = EXCLUDED.window_end,
        open_price  = EXCLUDED.open_price,
        high_price  = EXCLUDED.high_price,
        low_price   = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume      = EXCLUDED.volume,
        tick_count  = EXCLUDED.tick_count
"""


def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    print(f">>> [BATCH {batch_id}] Upserting {len(rows)} candles...", flush=True)

    conn = psycopg2.connect(**PG_CONFIG)
    try:
        cur = conn.cursor()
        cur.executemany(UPSERT_SQL, [
            (
                r["symbol"], r["window_start"], r["window_end"],
                r["open_price"], r["high_price"], r["low_price"], r["close_price"],
                r["volume"], r["tick_count"],
            )
            for r in rows
        ])
        conn.commit()
        cur.close()
        print(f">>> [BATCH {batch_id}] OK.", flush=True)
    except Exception as e:
        conn.rollback()
        print(f">>> [BATCH {batch_id}] FAILED: {e}", flush=True)
    finally:
        conn.close()


def main():
    print(">>> [1/5] KHOI TAO SPARK LOCAL...", flush=True)
    try:
        spark = (
            SparkSession.builder
            .appName("StockLocal")
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.streaming.backpressure.enabled", "true")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            )
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        print(">>> [2/5] SPARK SESSION OK!", flush=True)
    except Exception as e:
        print(f">>> [ERROR] LOI KHOI TAO SPARK: {e}", flush=True)
        return

    print(f">>> [3/5] DANG KET NOI KAFKA: {TOPIC}...", flush=True)
    try:
        raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 50000)
            .load()
        )
        print(">>> [3/5] KET NOI KAFKA OK!", flush=True)
    except Exception as e:
        print(f">>> [ERROR] LOI DOC KAFKA: {e}", flush=True)
        return

    ticks = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), TICK_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("price").isNotNull())
        .withColumn("event_time", F.to_timestamp("ingested_at"))
    )

    candles = (
        ticks
        .withWatermark("event_time", "1 minute")
        .groupBy(F.col("symbol"), F.window("event_time", "1 minute"))
        .agg(
            F.first("price").alias("open_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.last("price").alias("close_price"),
            F.sum("volume").alias("volume"),
            F.count("*").alias("tick_count"),
        )
        .select(
            "symbol",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "open_price", "high_price", "low_price", "close_price",
            "volume", "tick_count",
        )
    )

    print(">>> [4/5] BAT DAU STREAMING -> POSTGRES...", flush=True)
    query = (
        candles.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="15 seconds")
        .start()
    )

    print(">>> [5/5] DANG CHAY... (Bam Ctrl+C de dung)", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
