from __future__ import annotations
import os
import sys
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, Set

import pandas as pd
from confluent_kafka import KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic_settings import BaseSettings, SettingsConfigDict
from vnstock import Vnstock

# --- BỔ SUNG: LỚP GIÁP BẢO VỆ MẠNG (CHỐNG LỖI 502 VÀ TIMEOUT) ---
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import vnstock.core.utils.client as vn_client

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)
vn_client.session = session 
# -----------------------------------------------------------------

# --- CẤU HÌNH ---
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file_encoding="utf-8", extra='ignore')
    
    kafka_broker: str = "kafka:29092"
    topic_name: str = "stock_ticks_realtime"
    
    symbols: str = "ACB,BCM,BID,BVH,CTG,FPT,GAS,GVR,HDB,HPG,MBB,MSN,MWG,PLX,POW,SAB,SHB,SSB,SSI,STB,TCB,TPB,VCB,VHM,VIB,VIC,VJC,VNM,VPB,VRE"
    # poll_interval_seconds sẽ là thời gian nghỉ sau khi quét XONG 1 vòng 30 mã
    poll_interval_seconds: int = 5 
    
    kafka_queue_buffering_max_messages: int = 200_000
    kafka_batch_num_messages: int = 1_000
    kafka_linger_ms: int = 100
    kafka_compression_type: str = "lz4"
    kafka_acks: str = "all"
    kafka_retries: int = 5
    kafka_delivery_timeout_ms: int = 60_000


# --- HÀM KHAI SINH TOPIC ---
def ensure_topic_exists(settings: Settings):
    admin_client = AdminClient({'bootstrap.servers': settings.kafka_broker})
    topic_name = settings.topic_name
    logging.info(f"🔍 Kiểm tra sự tồn tại của Topic: '{topic_name}'...")
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            logging.info(f"🛠️ Topic '{topic_name}' chưa tồn tại. Bắt đầu khởi tạo...")
            new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                try:
                    f.result() 
                    logging.info(f"✅ Đã tạo thành công topic: {topic}")
                except Exception as e:
                    logging.error(f"❌ Lỗi khi tạo topic {topic}: {e}")
        else:
            logging.info(f"✅ Topic '{topic_name}' đã tồn tại.")
    except Exception as e:
        logging.error(f"❌ Lỗi kết nối Kafka Admin: {e}")


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
        self._logger.info("Flushing Kafka...")
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
        
        # BỔ SUNG: Cache để lưu các ID đã đẩy vào Kafka của giây cuối cùng (để chống trùng khi dùng >=)
        self._processed_ids_cache: Dict[str, Set[str]] = {s: set() for s in self._symbol_list}

    def start(self) -> None:
        self._logger.info(">>> Starting ingestion for %d symbols. Rate limit: ~60req/min", len(self._symbol_list))
        while self._running:
            for sym in self._symbol_list:
                if not self._running: break
                try:
                    self._process_symbol(sym)
                except Exception as e:
                    self._logger.error(f"Error processing {sym}: {e}")
                
                # ĐIỀU TIẾT: Nghỉ 1s giữa mỗi mã. Với 30 mã, đi hết 1 vòng mất 30s.
                # Tổng cộng ~60 requests/phút, vừa khít hạn mức của bạn.
                if self._running:
                    time.sleep(1)
            
            # Nghỉ thêm một chút sau khi xong 1 vòng nếu cần
            if self._running:
                time.sleep(self._settings.poll_interval_seconds)

    def _process_symbol(self, sym: str):
        try:
            # Tăng page_size lên 2000 để bao phủ tốt hơn nếu thị trường biến động mạnh
            stock_client = Vnstock().stock(symbol=sym, source='VCI')
            df_data = stock_client.quote.intraday(page_size=2000)

            if df_data is None or df_data.empty:
                return

            if 'time' in df_data.columns:
                df_data['time'] = pd.to_datetime(df_data['time'])
                df_data = df_data.sort_values(by='time')
            
            last_processed_time = self._watermark.get(sym)
            
            # SỬA LỖI: Dùng >= để không bỏ sót các lệnh cùng timestamp phát sinh sau
            if last_processed_time is not None:
                df_new = df_data[df_data['time'] >= last_processed_time].copy()
                
                # Lọc bỏ những ID đã được đẩy đi ở lần quét trước dựa vào Cache
                if not df_new.empty and 'id' in df_new.columns:
                    target_cache = self._processed_ids_cache[sym]
                    df_new = df_new[~df_new['id'].astype(str).isin(target_cache)]
            else:
                df_new = df_data

            if df_new.empty:
                return

            # Đẩy dữ liệu mới vào Kafka
            records = df_new.to_dict(orient="records")
            for record in records:
                record["ingested_at"] = datetime.now(timezone.utc).isoformat()
                record["symbol"] = sym
                self._kafka.produce(
                    topic=self._settings.topic_name, 
                    key=sym, 
                    value=json.dumps(record, default=str).encode("utf-8")
                )

            # CẬP NHẬT WATERMARK VÀ CACHE
            new_max_time = df_new['time'].max()
            self._watermark[sym] = new_max_time
            
            # Cập nhật cache: Chỉ lưu ID của những bản ghi có timestamp bằng timestamp lớn nhất
            # Điều này giúp chống trùng cho lần quét tới mà không tốn RAM lưu cả ngày.
            self._processed_ids_cache[sym] = {
                str(r['id']) for r in records if r['time'] == new_max_time
            }

            self._logger.info(f"[{sym}] Pushed {len(records)} NEW records. Watermark: {new_max_time}")

        except Exception as e:
            self._logger.warning(f"Fetch error for {sym}: {e}")

    def stop(self) -> None:
        self._logger.info("Stopping Poller...")
        self._running = False


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    settings = Settings()

    if os.environ.get("VNSTOCK_API_KEY"):
        logging.info("VNSTOCK_API_KEY detected.")
    else:
        logging.warning("API KEY not found! Running as Guest.")

    ensure_topic_exists(settings)

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