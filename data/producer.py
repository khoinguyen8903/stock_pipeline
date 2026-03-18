from __future__ import annotations
import os
import sys
import json
import logging
import signal
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Set, List

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
    
    # CẤU HÌNH SONG SONG & RATE LIMIT
    num_threads: int = 3
    num_partitions: int = 3
    poll_interval_seconds: int = 0  # Không cần nghỉ thêm sau vòng lặp vì đã quản lý nghỉ theo Thread
    
    kafka_queue_buffering_max_messages: int = 200_000
    kafka_batch_num_messages: int = 1_000
    kafka_linger_ms: int = 100
    kafka_compression_type: str = "lz4"
    kafka_acks: str = "1" 
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
            logging.info(f"🛠️ Topic '{topic_name}' chưa tồn tại. Bắt đầu tạo với {settings.num_partitions} partitions...")
            new_topic = NewTopic(topic_name, num_partitions=settings.num_partitions, replication_factor=1)
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


# --- LOGIC LẤY DỮ LIỆU ĐA LUỒNG ---
class VnStockPoller:
    def __init__(self, settings: Settings, kafka_client: KafkaProducerClient) -> None:
        self._settings = settings
        self._kafka = kafka_client
        self._logger = logging.getLogger(self.__class__.__name__)
        self._running = True
        
        self._symbol_list = [s.strip() for s in self._settings.symbols.split(',')]
        self._watermark: Dict[str, Any] = {s: None for s in self._symbol_list}
        self._processed_ids_cache: Dict[str, Set[str]] = {s: set() for s in self._symbol_list}
        
        self._threads: List[threading.Thread] = []

    def start(self) -> None:
        num_threads = self._settings.num_threads
        self._logger.info(f">>> Khởi động {num_threads} luồng quét an toàn cho {len(self._symbol_list)} mã (Rate limit: 60/min)...")
        
        # CHIA NHỎ DANH SÁCH MÃ CHỨNG KHOÁN CHO TỪNG LUỒNG
        chunks = [self._symbol_list[i::num_threads] for i in range(num_threads)]
        
        for i, chunk in enumerate(chunks):
            if not chunk: continue
            t = threading.Thread(target=self._worker_loop, args=(i, chunk), name=f"PollerThread-{i}")
            self._threads.append(t)
            t.start()
            
        # Luồng chính duy trì hệ thống
        while self._running:
            time.sleep(1)

    def _worker_loop(self, thread_id: int, symbols_chunk: List[str]):
        """Vòng lặp riêng của từng luồng (Shipper)"""
        self._logger.info(f"[Luồng {thread_id}] Phụ trách: {symbols_chunk}")
        
        # SO LE KHỞI ĐỘNG: Giúp 3 luồng không gửi request vào đúng một tích tắc
        time.sleep(thread_id * 1.0) 
        
        while self._running:
            for sym in symbols_chunk:
                if not self._running: break
                try:
                    self._process_symbol(sym)
                except Exception as e:
                    self._logger.error(f"[Luồng {thread_id}] Lỗi xử lý {sym}: {e}")
                
                # KHÓA RATE LIMIT: Bắt buộc nghỉ 3 giây sau mỗi mã
                # Đảm bảo tổng 3 luồng chỉ bắn ra tối đa 1 request mỗi giây (60 rq/phút)
                if self._running:
                    time.sleep(3) 

    def _process_symbol(self, sym: str):
        try:
            stock_client = Vnstock().stock(symbol=sym, source='VCI')
            df_data = stock_client.quote.intraday(page_size=2000)

            if df_data is None or df_data.empty:
                return

            if 'time' in df_data.columns:
                df_data['time'] = pd.to_datetime(df_data['time'])
                df_data = df_data.sort_values(by=['time', 'id'])
            
            last_processed_time = self._watermark.get(sym)
            
            # CHIẾN LƯỢC LỌC THÔNG MINH
            if last_processed_time is not None:
                df_potential = df_data[df_data['time'] >= last_processed_time].copy()
                
                if not df_potential.empty:
                    target_cache = self._processed_ids_cache[sym]
                    
                    mask_overlap = df_potential['time'] == last_processed_time
                    df_overlap = df_potential[mask_overlap]
                    df_new_records = df_potential[df_potential['time'] > last_processed_time]
                    
                    df_overlap_filtered = df_overlap[~df_overlap['id'].astype(str).isin(target_cache)]
                    df_final = pd.concat([df_overlap_filtered, df_new_records])
                else:
                    df_final = df_potential
            else:
                df_final = df_data

            if df_final.empty:
                return

            # Đẩy vào Kafka
            records = df_final.to_dict(orient="records")
            for record in records:
                record["ingested_at"] = datetime.now(timezone.utc).isoformat()
                record["symbol"] = sym
                self._kafka.produce(
                    topic=self._settings.topic_name, 
                    # QUAN TRỌNG: Key bằng 'sym' giúp Kafka đưa đúng mã vào đúng Partition
                    key=sym, 
                    value=json.dumps(record, default=str).encode("utf-8")
                )

            # CẬP NHẬT WATERMARK VÀ CACHE
            new_max_time = df_final['time'].max()
            self._watermark[sym] = new_max_time
            
            self._processed_ids_cache[sym] = {
                str(r['id']) for r in records if r['time'] == new_max_time
            }

            self._logger.info(f"[{sym}] Pushed {len(records)} records. Watermark: {new_max_time}")

        except Exception as e:
            self._logger.warning(f"Fetch error for {sym}: {e}")

    def stop(self) -> None:
        self._logger.info("Đang dừng hệ thống quét, chờ các luồng hoàn tất...")
        self._running = False
        for t in self._threads:
            if t.is_alive():
                t.join()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    settings = Settings()
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