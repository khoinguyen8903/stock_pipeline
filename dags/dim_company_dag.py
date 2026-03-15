from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'Khoi Nguyen',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2, 
    'retry_delay': timedelta(minutes=5), 
}

with DAG(
    dag_id='update_dim_company_weekly',
    default_args=default_args,
    description='Cào hồ sơ 1600+ doanh nghiệp và nạp vào bảng Dimension (Chạy 2h sáng CN hàng tuần)',
    # Chạy vào lúc 2:00 sáng, ngày Chủ Nhật (0) hàng tuần
    schedule_interval='0 2 * * 0', 
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    catchup=False,
    tags=['batch', 'dimension', 'gcs', 'bigquery'],
) as dag:

    # Task 1: Chạy script Python để cào dữ liệu và đẩy lên GCS
    # ĐÃ SỬA: Thêm cờ -u vào sau python để ép hiển thị log theo thời gian thực
    fetch_company_data = BashOperator(
        task_id='fetch_and_upload_profiles_to_gcs',
        bash_command='python -u /opt/airflow/dags/scripts/fetch_dim_company.py',
    )

    # Task 2: Kéo file từ GCS đổ vào BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq_dim_company',
        bucket='stock-datalake-raw-khoinguyen',
        source_objects=['dimension/dim_company.parquet'],
        destination_project_dataset_table='stock-lambda-project.stock_data_warehouse.dim_company',
        source_format='PARQUET',
        # CỰC QUAN TRỌNG: Ghi đè (Xóa cũ - Nạp mới) để chống trùng lặp dữ liệu Dimension
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True, 
    )

    # Nối luồng chạy
    fetch_company_data >> load_to_bigquery