import os
import sys
import json
import pandas as pd
from datetime import datetime
from confluent_kafka import Consumer
from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/data/gcpkey.json"
BUCKET_NAME = "stock-datalake-raw-khoinguyen"
KAFKA_BROKER = "kafka:29092" 
TOPIC_NAME = "stock_ticks_realtime"
GROUP_ID = "datalake_batch_saver"

def upload_to_gcs(df, full_gcs_path):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(full_gcs_path)
    temp_file = "/tmp/temp_raw.parquet"
    
    # 1. Ép kiểu các cột thời gian sang chuẩn Datetime để BigQuery làm Partition
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
    if 'ingested_at' in df.columns:
        df['ingested_at'] = pd.to_datetime(df['ingested_at'], errors='coerce')
        
    # 2. Ép các cột còn lại sang Chuỗi (STRING) để tránh lỗi số liệu lệch chuẩn
    cols_to_string = ['price', 'volume', 'match_type', 'id', 'symbol']
    for col in cols_to_string:
        if col in df.columns:
            df[col] = df[col].astype(str)
            
    # Ép thời gian về chuẩn Microsecond (us) tránh lỗi Nanosecond của Pandas/PyArrow
    df.to_parquet(
        temp_file, 
        engine="pyarrow", 
        index=False,
        coerce_timestamps='us',
        allow_truncated_timestamps=True
    )
    
    blob.upload_from_filename(temp_file)
    os.remove(temp_file) 
    print(f"✅ Đã upload thành công {len(df)} dòng dữ liệu lên gs://{BUCKET_NAME}/{full_gcs_path}")

# ... (Hàm consume_and_upload giữ nguyên phần hút Kafka và gọi batch_id) ...
def consume_and_upload(target_date_folder, batch_id):
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
            msg = consumer.poll(5.0)
            if msg is None:
                print("🛑 Đã hút cạn dữ liệu hiện có. Tiến hành đóng gói...")
                break 
            if msg.error():
                print(f"❌ Lỗi Kafka: {msg.error()}")
                continue
            data = json.loads(msg.value().decode('utf-8'))
            messages.append(data)

        if messages:
            df = pd.DataFrame(messages)
            full_path = f"raw/{target_date_folder}/stock_raw_{batch_id}.parquet"
            upload_to_gcs(df, full_path)
            consumer.commit()
            print(f"🎉 Job Data Lake Batch hoàn tất! Lưu tại: {full_path}")
        else:
            print("🤷‍♂️ Không có dữ liệu giao dịch mới.")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")
    finally:
        consumer.close()

if __name__ == '__main__':
    passed_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y/%m/%d")
    batch_id = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime("%H%M%S")
    consume_and_upload(passed_date, batch_id)