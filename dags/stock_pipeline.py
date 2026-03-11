from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta
import pendulum

# Cấu hình múi giờ Việt Nam
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'Khoi Nguyen',
    'depends_on_past': False,
    'email_on_failure': False,
    # Cấu hình mặc định vẫn để retry 3 lần cho các task thông thường (như wait_for_data bị lỗi mạng)
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# --- HÀM KIỂM TRA ĐIỀU KIỆN (CẦU DAO) ---
def enforce_time_limit():
    now = pendulum.now("Asia/Ho_Chi_Minh")
    
    # Nếu bật máy sau 14:50 chiều (sắp đến giờ shutdown) -> Ngắt cầu dao (False)
    if now.hour >= 15 or (now.hour == 14 and now.minute >= 50):
        print(f">>> Bật máy lúc {now.format('HH:mm')}. Đã quá giờ giao dịch an toàn.")
        print(">>> NGẮT CẦU DAO: Hệ thống sẽ tự động HỦY lượt chạy bù này để tránh rác RAM!")
        return False
        
    print(f">>> Bật máy lúc {now.format('HH:mm')}. Thời gian hợp lệ, đóng cầu dao cho phép kéo dữ liệu...")
    return True

with DAG(
    'vietstock_auto_pipeline',
    default_args=default_args,
    description='Hệ thống tự động theo giờ giao dịch sàn HOSE/HNX (9h - 15h)',
    # CHUẨN: Chạy lúc 8:55 sáng từ Thứ 2 đến Thứ 6
    schedule_interval='55 8 * * 1-5', 
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    catchup=False,
    tags=['stock', 'vietnam', 'realtime'],
) as dag:

    # 0. LẮP CẦU DAO TỰ ĐỘNG Ở ĐÂY
    check_time_limit = ShortCircuitOperator(
        task_id='check_time_limit',
        python_callable=enforce_time_limit,
    )

    # 1. Khởi động Producer (Chạy liên tục, không đợi kết thúc)
    start_producer = BashOperator(
        task_id='start_vnstock_producer',
        cwd='/opt/airflow', 
        bash_command='python /opt/airflow/data/producer.py',
        # Ghi đè retries = 0 để không bị Airflow "hồi sinh" lúc 15:05
        retries=0, 
    )

    # 2. Đợi 30 giây để Kafka có dữ liệu mồi
    wait_for_data = BashOperator(
        task_id='wait_for_kafka_data',
        bash_command='sleep 30',
    )

    # 3. Chạy Spark Streaming (Chạy song song với Producer)
    start_spark = BashOperator(
        task_id='start_spark_processing',
        cwd='/opt/airflow',
        bash_command='''
        spark-submit \
        --master local[*] \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        /opt/airflow/data/spark.py
        ''',
        # Ghi đè retries = 0 để Spark chết hẳn khi DAG Shutdown gọi lệnh pkill
        retries=0,
    )

    # --- ĐỊNH NGHĨA LUỒNG CHẠY SONG SONG CÓ BẢO VỆ ---
    
    # Cầu dao phải nằm trước tất cả. Nếu cầu dao đóng (True) thì mới chạy 2 nhánh dưới:
    check_time_limit >> start_producer 
    check_time_limit >> wait_for_data >> start_spark