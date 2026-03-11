from __future__ import annotations
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd
from confluent_kafka import KafkaError, Producer
from pydantic_settings import BaseSettings, SettingsConfigDict
from vnstock import Vnstock

# --- CẤU HÌNH ---
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra='ignore')
    
    # Kafka
    kafka_broker: str = "127.0.0.1:9092"
    topic_name: str = "stock_ticks_realtime"
    
    # Application
    symbols: str = "FPT,SSI,HPG"
    poll_interval_seconds: int = 15  # Chu kỳ request dữ liệu
    
    # Kafka Producer Tuning
    kafka_queue_buffering_max_messages: int = 200_000
    kafka_batch_num_messages: int = 1_000 # Gom 1000 tin nhắn rồi mới gửi
    kafka_linger_ms: int = 100            # Hoặc đợi 100ms rồi gửi
    kafka_compression_type: str = "lz4"   # Nén dữ liệu để tiết kiệm băng thông
    kafka_acks: str = "all"
    kafka_retries: int = 5
    kafka_delivery_timeout_ms: int = 60_000

# --- KAFKA CLIENT ---
class KafkaProducerClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._logger = logging.getLogger(self.__class__.__name__)
        self._producer = self._build_producer()
        self._total_delivered = 0

    def _build_producer(self) -> Producer:
        conf = {
            "bootstrap.servers": self._settings.kafka_broker,
            "queue.buffering.max.messages": self._settings.kafka_queue_buffering_max_messages,
            "batch.num.messages": self._settings.kafka_batch_num_messages,
            "linger.ms": self._settings.kafka_linger_ms,
            "compression.type": self._settings.kafka_compression_type,
            "acks": self._settings.kafka_acks,
            "retries": self._settings.kafka_retries,
            "delivery.timeout.ms": self._settings.kafka_delivery_timeout_ms,
        }
        return Producer(conf)

    def _delivery_callback(self, err: KafkaError | None, msg: Any) -> None:
        """Callback được gọi khi Kafka Broker xác nhận đã nhận tin nhắn"""
        if err:
            self._logger.error("Message failed delivery: %s", err)
        else:
            self._total_delivered += 1
            # Debug log nếu cần thiết (cẩn thận spam log)
            # self._logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def produce(self, topic: str, key: str, value: bytes) -> None:
        """Gửi tin nhắn bất đồng bộ (Async)"""
        try:
            self._producer.poll(0) # Trigger callbacks
            self._producer.produce(
                topic=topic, 
                key=key, 
                value=value, 
                callback=self._delivery_callback
            )
        except BufferError:
            self._logger.warning("Local buffer full, waiting for free space...")
            self._producer.poll(1) # Chờ 1s để giải phóng buffer
            self._producer.produce(topic=topic, key=key, value=value, callback=self._delivery_callback)

    def flush(self, timeout: float = 10.0) -> int:
        """Đẩy toàn bộ tin nhắn còn tồn đọng đi"""
        self._logger.info("Flushing remaining messages...")
        return self._producer.flush(timeout)

# --- LOGIC LẤY DỮ LIỆU ---
class VnStockPoller:
    def __init__(self, settings: Settings, kafka_client: KafkaProducerClient) -> None:
        self._settings = settings
        self._kafka = kafka_client
        self._logger = logging.getLogger(self.__class__.__name__)
        self._running = True
        self._symbol_list = [s.strip() for s in self._settings.symbols.split(',')]
        
        # Dictionary lưu mốc thời gian (hoặc ID) cuối cùng của từng mã
        # Key: Symbol, Value: Time string (e.g., '14:30:00')
        self._watermark: Dict[str, str] = {s: "" for s in self._symbol_list}

    def start(self) -> None:
        self._logger.info(">>> Starting ingestion for: %s", self._symbol_list)
        
        while self._running:
            for sym in self._symbol_list:
                if not self._running: 
                    break
                
                try:
                    self._process_symbol(sym)
                except Exception as e:
                    self._logger.error(f"Unexpected error processing {sym}: {e}")
            
            # Nghỉ giữa các chu kỳ quét
            if self._running:
                # self._logger.info(f"Sleeping for {self._settings.poll_interval_seconds}s...")
                time.sleep(self._settings.poll_interval_seconds)

    def _process_symbol(self, sym: str):
        """Hàm xử lý logic cho từng mã chứng khoán"""
        try:
            # Gọi API lấy dữ liệu Intraday
            stock_client = Vnstock().stock(symbol=sym, source='VCI')
            df_data = stock_client.quote.intraday()

            if df_data is None or df_data.empty:
                return

            # Sắp xếp theo thời gian tăng dần để xử lý đúng thứ tự
            if 'time' in df_data.columns:
                df_data = df_data.sort_values(by='time')
            
            # --- LOGIC DEDUPLICATION (CHỐNG TRÙNG) ---
            last_processed_time = self._watermark.get(sym, "")
            
            # Chỉ lấy các dòng có thời gian lớn hơn mốc đã lưu
            if last_processed_time:
                df_new = df_data[df_data['time'] > last_processed_time]
            else:
                df_new = df_data # Lần chạy đầu tiên lấy hết (hoặc lấy 100 records mới nhất)

            if df_new.empty:
                # self._logger.info(f"[{sym}] No new data updates.")
                return

            # --- GỬI VÀO KAFKA ---
            records = df_new.to_dict(orient="records")
            count = 0
            for record in records:
                # Thêm timestamp ingest để đo độ trễ (latency) sau này
                record["ingested_at"] = datetime.now(timezone.utc).isoformat()
                record["symbol"] = sym
                
                # Tạo key cho Kafka (để đảm bảo ordering trong cùng partition)
                # Dùng Symbol làm Key để tất cả dữ liệu của 1 mã vào cùng 1 partition
                msg_key = sym 
                
                self._kafka.produce(
                    topic=self._settings.topic_name, 
                    key=msg_key, 
                    value=json.dumps(record, default=str).encode("utf-8")
                )
                count += 1

            # Cập nhật Watermark mới nhất
            self._watermark[sym] = df_new['time'].max()
            self._logger.info(f"[{sym}] Pushed {count} NEW records. Last time: {self._watermark[sym]}")

        except Exception as e:
            # Bắt lỗi API (ví dụ 502) để không làm crash vòng lặp
            self._logger.warning(f"Error fetching {sym}: {e}")

    def stop(self) -> None:
        self._logger.info("Stopping poller...")
        self._running = False

# --- ENTRY POINT ---
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    settings = Settings()
    client = KafkaProducerClient(settings)
    poller = VnStockPoller(settings, client)

    # Xử lý Signal để tắt chương trình mượt mà (Graceful Shutdown)
    def handle_exit(sig, frame):
        logging.info("\nReceived termination signal. Shutting down...")
        poller.stop()
    
    signal.signal(signal.SIGINT, handle_exit)  # Ctrl+C
    signal.signal(signal.SIGTERM, handle_exit) # Docker Stop

    try:
        poller.start()
    except Exception as e:
        logging.critical(f"Fatal error in main loop: {e}")
    finally:
        # Đảm bảo gửi hết tin nhắn còn trong hàng đợi trước khi tắt hẳn
        leftover = client.flush(timeout=10.0)
        logging.info(f"Producer flushed. {leftover} messages remaining locally.")

if __name__ == "__main__":
    main()