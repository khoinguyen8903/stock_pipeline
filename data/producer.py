from __future__ import annotations
import os
import sys

# --- FIX LỖI VNSTOCK LOGGING (FINAL) ---
# vnstock sẽ ghi log vào thư mục hiện tại.
# Chuyển về /tmp để đảm bảo luôn có quyền ghi mà không ảnh hưởng logic khác.
try:
    os.chdir("/tmp")
except:
    pass
# ---------------------------------------

import json
import logging
import signal
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
    
    # KAFKA HOST TRONG DOCKER
    kafka_broker: str = "kafka:29092"
    topic_name: str = "stock_ticks_realtime"
    
    # ĐÃ SỬA: Danh sách rổ VN30 (30 công ty vốn hóa lớn nhất thị trường)
    symbols: str = "ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,PLX,POW,SAB,SHB,SSB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
    poll_interval_seconds: int = 15
    
    # Kafka Producer Tuning
    kafka_queue_buffering_max_messages: int = 200_000
    kafka_batch_num_messages: int = 1_000
    kafka_linger_ms: int = 100
    kafka_compression_type: str = "lz4"
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
        if err:
            self._logger.error("Message failed delivery: %s", err)
        else:
            self._total_delivered += 1

    def produce(self, topic: str, key: str, value: bytes) -> None:
        try:
            self._producer.poll(0)
            self._producer.produce(topic=topic, key=key, value=value, callback=self._delivery_callback)
        except BufferError:
            self._logger.warning("Buffer full, waiting...")
            self._producer.poll(1)
            self._producer.produce(topic=topic, key=key, value=value, callback=self._delivery_callback)

    def flush(self, timeout: float = 10.0) -> int:
        self._logger.info("Flushing...")
        return self._producer.flush(timeout)

# --- LOGIC LẤY DỮ LIỆU ---
class VnStockPoller:
    def __init__(self, settings: Settings, kafka_client: KafkaProducerClient) -> None:
        self._settings = settings
        self._kafka = kafka_client
        self._logger = logging.getLogger(self.__class__.__name__)
        self._running = True
        self._symbol_list = [s.strip() for s in self._settings.symbols.split(',')]
        self._watermark: Dict[str, Any] = {s: None for s in self._symbol_list}

    def start(self) -> None:
        self._logger.info(">>> Starting ingestion for: %s", self._symbol_list)
        while self._running:
            for sym in self._symbol_list:
                if not self._running: break
                try:
                    self._process_symbol(sym)
                except Exception as e:
                    self._logger.error(f"Error {sym}: {e}")
                
                # ĐÃ SỬA: Quãng nghỉ 1 giây sau MỖI mã để tránh bị API đánh dấu là tấn công DDoS
                if self._running:
                    time.sleep(1)
            
            if self._running:
                # Sau khi xong 1 vòng 30 mã (tốn ~30s), nghỉ thêm 15s rồi lặp lại
                time.sleep(self._settings.poll_interval_seconds)

    def _process_symbol(self, sym: str):
        try:
            stock_client = Vnstock().stock(symbol=sym, source='VCI')
            df_data = stock_client.quote.intraday()

            if df_data is None or df_data.empty: return

            if 'time' in df_data.columns:
                df_data['time'] = pd.to_datetime(df_data['time'])
                df_data = df_data.sort_values(by='time')
            
            last_processed_time = self._watermark.get(sym, None)
            if last_processed_time is not None:
                df_new = df_data[df_data['time'] > last_processed_time]
            else:
                df_new = df_data

            if df_new.empty: return

            records = df_new.to_dict(orient="records")
            count = 0
            for record in records:
                record["ingested_at"] = datetime.now(timezone.utc).isoformat()
                record["symbol"] = sym
                self._kafka.produce(
                    topic=self._settings.topic_name, 
                    key=sym, 
                    value=json.dumps(record, default=str).encode("utf-8")
                )
                count += 1

            self._watermark[sym] = df_new['time'].max()
            self._logger.info(f"[{sym}] Pushed {count} NEW records. Last time: {self._watermark[sym]}")

        except Exception as e:
            self._logger.warning(f"Fetch error {sym}: {e}")

    def stop(self) -> None:
        self._logger.info("Stopping...")
        self._running = False

def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    settings = Settings()
    client = KafkaProducerClient(settings)
    poller = VnStockPoller(settings, client)

    def handle_exit(sig, frame):
        poller.stop()
    
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        poller.start()
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
    finally:
        client.flush()

if __name__ == "__main__":
    main()