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
    description='Gom Tick Data Kafka đẩy lên GCS, nạp vào BigQuery',
    schedule_interval='30 15 * * 1-5', 
    start_date=datetime(2026, 3, 1, tzinfo=local_tz),
    catchup=False,
    tags=['batch', 'datalake', 'kafka', 'tick_data'],
) as dag:

    run_kafka_to_gcs = BashOperator(
        task_id='extract_kafka_load_gcs',
        bash_command="python /opt/airflow/dags/scripts/kafka_to_gcs.py {{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y/%m/%d') }}",
    )

    # ĐỊNH NGHĨA SCHEMA CỨNG CHO TICK DATA
    TICK_SCHEMA = [
        {'name': 'time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'volume', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'match_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ingested_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]

    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket='stock-datalake-raw-khoinguyen',
        source_objects=["raw/{{ data_interval_end.in_timezone('Asia/Ho_Chi_Minh').strftime('%Y/%m/%d') }}/*.parquet"],
        destination_project_dataset_table='stock-lambda-project.stock_data_warehouse.stock_raw_daily',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',      
        
        # --- CÁC CẤU HÌNH BẢO MẬT & TỐI ƯU ---
        autodetect=False, # Tắt tự động nhận diện
        schema_fields=TICK_SCHEMA, # Áp schema cứng
        time_partitioning={"type": "DAY", "field": "time"}, # Lớp khiên 1: Cắt theo ngày
        cluster_fields=['symbol'], # Lớp khiên 2: Gom cụm theo mã cổ phiếu
        schema_update_options=['ALLOW_FIELD_ADDITION'],
    )

    run_kafka_to_gcs >> load_gcs_to_bigquery