import sys
import os
import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from dotenv import load_dotenv

# --- NẠP FILE .ENV ---
load_dotenv()

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
TOPIC = os.environ.get("TOPIC_NAME", "stock_ticks_realtime")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

if not DB_PASSWORD:
    raise ValueError("❌ CẢNH BÁO: Không tìm thấy POSTGRES_PASSWORD!")

PG_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": DB_PASSWORD,
}

CHECKPOINT_DIR = "/tmp/spark_checkpoints"

# BỔ SUNG CỘT "id" ĐỂ PHỤC VỤ DROP DUPLICATES
TICK_SCHEMA = StructType([
    StructField("id", StringType()),  # Thêm ID
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
    print(">>> [1/5] KHOI TAO SPARK DOCKER...", flush=True)
    try:
        spark = (
            SparkSession.builder
            .appName("StockStreamingDocker")
            .master("local[*]") 
            # [ĐÃ SỬA] Đã xóa dòng .config("spark.driver.memory", "2g") vì không có tác dụng
            # TỐI ƯU 1: Đổi thành 3 để khớp với 3 Partitions của Kafka
            .config("spark.sql.shuffle.partitions", "3") 
            # TỐI ƯU 2: Cài đặt đúng múi giờ Việt Nam
            .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") 
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") 
            .getOrCreate()
        )
        # [ĐÃ SỬA] Chuyển thành INFO để bạn có thể xem log theo dõi bộ nhớ qua từng batch
        spark.sparkContext.setLogLevel("INFO")
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
        
        # [ĐÃ SỬA] Chuyển đổi định dạng thời gian đúng chuẩn để Spark tiến được Watermark
        .withColumn("event_time", F.to_timestamp(F.col("time"), "yyyy-MM-dd HH:mm:ssXXX"))
        
        # TỐI ƯU 3: Đặt Watermark 2 phút và loại bỏ trùng lặp dựa trên ID giao dịch
        .withWatermark("event_time", "2 minutes")
        
        # [ĐÃ SỬA] Thêm event_time vào dropDuplicates để kích hoạt cơ chế tự động dọn RAM
        .dropDuplicates(["symbol", "id", "event_time"]) 
    )

    candles = (
        ticks
        # Gom nhóm nến 1 phút dựa trên giờ Việt Nam chuẩn xác
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