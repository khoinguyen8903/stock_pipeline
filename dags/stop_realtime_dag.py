import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'Khoi Nguyen',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0, # SỬA LỖI: Không cho phép retry để tránh treo hệ thống
}

def force_kill_zombies():
    print(">>> 1. Đang truy quét và tiêu diệt Producer...")
    # Dùng lệnh kill nguyên thủy của Linux, an toàn hơn pkill rất nhiều
    os.system("ps aux | grep '[p]roducer.py' | awk '{print $2}' | xargs -r kill -9")

    print(">>> 2. Đang truy quét và tiêu diệt Spark (Java)...")
    os.system("ps aux | grep '[s]park.py' | awk '{print $2}' | xargs -r kill -9")
    os.system("ps aux | grep '[j]ava' | awk '{print $2}' | xargs -r kill -9")

    print(">>> 3. Đang dọn dẹp Checkpoint lock...")
    # Thêm 2>/dev/null để chặn các thông báo lỗi rác làm treo log
    os.system("find /opt/airflow/data/checkpoints -name '*.lock' -type f -delete 2>/dev/null || true")
    os.system("find /tmp/spark_checkpoints -name '*.lock' -type f -delete 2>/dev/null || true")
    
    print(">>> HOÀN TẤT DỌN DẸP HỆ THỐNG!")

with DAG(
    'stock_realtime_shutdown',
    default_args=default_args,
    description='Tự động dọn dẹp hệ thống sau khi sàn đóng cửa bằng Python',
    schedule_interval='05 15 * * 1-5', 
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    catchup=False,
    tags=['stock', 'realtime', 'shutdown'],
) as dag:

    # Sử dụng PythonOperator thay vì BashOperator
    kill_zombies_task = PythonOperator(
        task_id='kill_producer_and_spark',
        python_callable=force_kill_zombies,
    )