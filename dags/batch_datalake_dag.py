from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.datasets import Dataset # [THÊM MỚI 1]: Import Dataset
from datetime import datetime, timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'khoi_nguyen',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1, 
    'retry_delay': timedelta(minutes=2), 
}

with DAG(
    'stock_microbatch_to_bq',
    default_args=default_args,
    description='Hút dữ liệu Kafka lên BQ mỗi 10 phút, giữ nguyên Partition',
    schedule_interval='*/10 9-15 * * 1-5', 
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1, 
    tags=['batch', 'datalake', 'kafka', 'tick_data'],
) as dag:

    # 1. Truyền biến thời gian chuẩn xác xuống file Python
    date_folder = "{{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y/%m/%d') }}"
    batch_id = "{{ ts_nodash }}"
    exact_gcs_file = f"raw/{date_folder}/stock_raw_{batch_id}.parquet"

    run_kafka_to_gcs = BashOperator(
        task_id='extract_kafka_load_gcs',
        bash_command=f"python /opt/airflow/dags/scripts/kafka_to_gcs.py {date_folder} {batch_id}",
    )

    # 2. KHÔI PHỤC SCHEMA NGUYÊN BẢN CỦA BẠN (time = TIMESTAMP)
    ORIGINAL_TICK_SCHEMA = [
        {'name': 'time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'volume', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'match_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ingested_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]

    # 3. Đẩy file lên đúng bảng cũ, giữ nguyên lớp bảo vệ
    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket='stock-datalake-raw-khoinguyen',
        source_objects=[exact_gcs_file], 
        destination_project_dataset_table='stock-lambda-project.stock_data_warehouse.stock_raw_daily',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',      
        autodetect=False, 
        schema_fields=ORIGINAL_TICK_SCHEMA, 
        time_partitioning={"type": "DAY", "field": "time"}, 
        cluster_fields=['symbol'], 
        schema_update_options=['ALLOW_FIELD_ADDITION'],
        
        # [THÊM MỚI 2]: Khai báo Outlet (Cái loa phát tín hiệu)
        outlets=[Dataset("bigquery://stock_raw_daily")]
    )

    run_kafka_to_gcs >> load_gcs_to_bigquery