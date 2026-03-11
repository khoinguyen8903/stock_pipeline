from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'khoi_nguyen',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
}

with DAG(
    dag_id='stock_batch_to_datalake',
    default_args=default_args,
    description='Gom dữ liệu Kafka đẩy lên GCS, sau đó nạp vào BigQuery',
    schedule_interval='30 15 * * 1-5', 
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    catchup=False,
    tags=['batch', 'datalake', 'gcs', 'bigquery'],
) as dag:

    # Task 1: Gom dữ liệu từ Kafka đẩy lên GCS (Giữ nguyên)
    run_kafka_to_gcs = BashOperator(
        task_id='extract_kafka_load_gcs',
        bash_command="python /opt/airflow/dags/scripts/kafka_to_gcs.py {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y/%m/%d') }}",
    )

    # Task 2: Đẩy dữ liệu từ GCS nạp vào BigQuery
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket='stock-datalake-raw-khoinguyen',
        # Vẫn dùng Macro để lấy đúng thư mục của ngày đang chạy
        source_objects=["raw/{{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y/%m/%d') }}/*.parquet"],
        # CẤU HÌNH ĐÍCH ĐẾN: Tên Project . Tên Dataset . Tên Bảng
        destination_project_dataset_table='stock-lambda-project.stock_data_warehouse.stock_raw_daily',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',      # Chạy mỗi ngày sẽ nối thêm dữ liệu vào dưới
        create_disposition='CREATE_IF_NEEDED', # Nếu bảng chưa tồn tại thì tự động tạo mới
        autodetect=True,                       # Tự động nhận diện cấu trúc cột từ file Parquet
        # Mặc định Operator sẽ dùng connection 'google_cloud_default' mà ta vừa tạo ở Bước 2
    )

    # Nối luồng chạy
    run_kafka_to_gcs >> load_gcs_to_bigquery