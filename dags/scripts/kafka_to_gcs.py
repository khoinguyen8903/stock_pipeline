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
# Đường dẫn tuyệt đối trỏ tới file Key nằm bên trong container Airflow
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/data/gcpkey.json"

BUCKET_NAME = "stock-datalake-raw-khoinguyen"
# Vì script chạy trong Docker, phải gọi Kafka bằng tên service trong mạng nội bộ (thường là kafka:29092)
KAFKA_BROKER = "kafka:29092" 
TOPIC_NAME = "stock_ticks_realtime"
GROUP_ID = "datalake_batch_saver"

def upload_to_gcs(df, filename):
    """Hàm lưu DataFrame thành Parquet và đẩy lên GCS"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    
    # Lưu ra file tạm ở thư mục /tmp của Linux (an toàn trong Docker)
    temp_file = "/tmp/temp_raw.parquet"
    df.to_parquet(temp_file, engine="pyarrow", index=False)
    
    # Upload lên Cloud
    blob.upload_from_filename(temp_file)
    os.remove(temp_file) # Dọn rác
    
    print(f"✅ Đã upload thành công {len(df)} dòng dữ liệu lên Data Lake: gs://{BUCKET_NAME}/{filename}")

# THÊM THAM SỐ target_date_folder
def consume_and_upload(target_date_folder=None):
    """Hàm lấy sạch dữ liệu từ Kafka rồi tự tắt"""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        # Tắt tự động commit để đảm bảo dữ liệu lên GCS an toàn rồi mới đánh dấu là đã đọc
        'enable.auto.commit': False 
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])

    print(f"⏳ Bắt đầu rà soát topic '{TOPIC_NAME}' để lấy dữ liệu mới...")
    messages = []
    
    try:
        while True:
            # Chờ 10 giây. Nếu không có tin nhắn nào lọt vào Kafka, ngầm hiểu là đã hút cạn dữ liệu
            msg = consumer.poll(10.0)
            
            if msg is None:
                print("🛑 Đã hút cạn dữ liệu hiện có trong Kafka. Tiến hành đóng gói...")
                break 
                
            if msg.error():
                print(f"❌ Lỗi Kafka: {msg.error()}")
                continue

            # Đọc và gom dữ liệu
            data = json.loads(msg.value().decode('utf-8'))
            messages.append(data)

        # Nếu gom được dữ liệu, tiến hành upload
        if messages:
            df = pd.DataFrame(messages)
            now = datetime.now()
            
            # --- SỬA LOGIC NGÀY THÁNG Ở ĐÂY ---
            # Ưu tiên lấy ngày do Airflow cấp. Nếu không có mới tự động lấy ngày hiện tại.
            if target_date_folder:
                folder_path = f"raw/{target_date_folder}"
            else:
                folder_path = now.strftime("raw/%Y/%m/%d")
                
            # Tên file lấy giờ thực tế để không bị trùng nếu chạy lại nhiều lần trong ngày
            file_name = now.strftime("stock_raw_%H%M%S.parquet")
            full_path = f"{folder_path}/{file_name}"
            
            upload_to_gcs(df, full_path)
            
            # CỰC QUAN TRỌNG: Chỉ xác nhận (commit) với Kafka sau khi đã upload GCS thành công
            consumer.commit()
            print(f"🎉 Job Data Lake Batch đã hoàn tất trọn vẹn (Zero Data Loss)! Lưu tại: {full_path}")
        else:
            print("🤷‍♂️ Hiện tại không có dữ liệu giao dịch mới nào để đẩy lên mây.")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng trong quá trình chạy: {e}")
    finally:
        consumer.close()

if __name__ == '__main__':
    # BẮT THAM SỐ TỪ AIRFLOW TRUYỀN XUỐNG DÒNG LỆNH
    passed_date = sys.argv[1] if len(sys.argv) > 1 else None
    consume_and_upload(passed_date)