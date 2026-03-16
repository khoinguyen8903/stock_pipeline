import os
import sys
import json
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer
from google.cloud import storage

# ==========================================
# 1. CẤU HÌNH MÔI TRƯỜNG & BIẾN
# ==========================================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/data/gcpkey.json"
BUCKET_NAME = "stock-datalake-raw-khoinguyen"
KAFKA_BROKER = "kafka:29092" 
TOPIC_NAME = "stock_ticks_realtime"
GROUP_ID = "datalake_batch_saver"

def upload_to_gcs(df, filename):
    """Hàm lưu DataFrame thành Parquet và đẩy lên GCS"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    
    temp_file = "/tmp/temp_raw.parquet"
    
    # FIX LỖI NANOSECOND: Ép thời gian về chuẩn Microsecond (us) cho BigQuery
    df.to_parquet(
        temp_file, 
        engine="pyarrow", 
        index=False,
        coerce_timestamps='us',
        allow_truncated_timestamps=True
    )
    
    blob.upload_from_filename(temp_file)
    os.remove(temp_file) 
    print(f"✅ Đã upload thành công {len(df)} dòng dữ liệu lên gs://{BUCKET_NAME}/{filename}")

def consume_and_upload(target_date_folder=None):
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False 
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])

    print(f"⏳ Bắt đầu rà soát topic '{TOPIC_NAME}' để lấy dữ liệu mới...")
    messages = []
    
    try:
        while True:
            msg = consumer.poll(10.0)
            
            if msg is None:
                print("🛑 Đã hút cạn dữ liệu hiện có trong Kafka. Tiến hành đóng gói...")
                break 
                
            if msg.error():
                print(f"❌ Lỗi Kafka: {msg.error()}")
                continue

            data = json.loads(msg.value().decode('utf-8'))
            messages.append(data)

        if messages:
            df = pd.DataFrame(messages)
            
            # ==================================================
            # BƯỚC QUAN TRỌNG: ÉP KIỂU CHUẨN ELT CHO TICK DATA
            # ==================================================
            # 1. Ép thời gian sang Datetime (Để BigQuery Partition được)
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
            if 'ingested_at' in df.columns:
                df['ingested_at'] = pd.to_datetime(df['ingested_at'])
                
            # 2. Ép các cột còn lại sang STRING an toàn
            cols_to_string = ['price', 'volume', 'match_type', 'id', 'symbol']
            for col in cols_to_string:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            # ==================================================

            now = datetime.now()
            if target_date_folder:
                folder_path = f"raw/{target_date_folder}"
            else:
                folder_path = now.strftime("raw/%Y/%m/%d")
                
            file_name = now.strftime("stock_raw_%H%M%S.parquet")
            full_path = f"{folder_path}/{file_name}"
            
            upload_to_gcs(df, full_path)
            
            consumer.commit()
            print(f"🎉 Job Data Lake Batch hoàn tất (Zero Data Loss)! Lưu tại: {full_path}")
        else:
            print("🤷‍♂️ Hiện tại không có dữ liệu giao dịch mới nào để đẩy lên mây.")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")
    finally:
        consumer.close()

if __name__ == '__main__':
    passed_date = sys.argv[1] if len(sys.argv) > 1 else None
    consume_and_upload(passed_date)