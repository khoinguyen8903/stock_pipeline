from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import pandas as pd
import time
import os
from vnstock import stock_historical_data

PROJECT_ID = 'stock-lambda-project'
GCS_BUCKET = 'stock-datalake-raw-khoinguyen'
BQ_DATASET = 'stock_data_warehouse'
GCP_CONN_ID = 'google_cloud_default'

VN30_TICKERS = [
    'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 
    'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSI', 'SSB', 'STB', 
    'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'
]

TODAY_STR = datetime.now().strftime('%Y-%m-%d')

default_args = {
    'owner': 'khoi_nguyen',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

def upload_to_gcs(df, file_name, destination_blob_name):
    if df.empty: return
    temp_path = f"/tmp/{file_name}"
    df['time'] = pd.to_datetime(df['time']) # Đảm bảo kiểu datetime
    df['symbol'] = df['symbol'].astype(str)
    df['ingestion_timestamp'] = pd.Timestamp.now(tz='UTC') 
    df.to_parquet(temp_path, index=False)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    gcs_hook.upload(bucket_name=GCS_BUCKET, object_name=destination_blob_name, filename=temp_path)
    os.remove(temp_path)

def fetch_today_daily(**kwargs):
    all_data = []
    for ticker in VN30_TICKERS:
        try:
            df = stock_historical_data(symbol=ticker, start_date=TODAY_STR, end_date=TODAY_STR, resolution='1D', type='stock')
            if df is not None and not df.empty:
                df['symbol'] = ticker
                all_data.append(df)
            time.sleep(1)
        except Exception as e: print(f"Lỗi: {e}")
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        upload_to_gcs(final_df, "today_daily.parquet", f"bronze/daily_run/{TODAY_STR}_daily.parquet")

def fetch_today_1m(**kwargs):
    all_data = []
    for ticker in VN30_TICKERS:
        try:
            df = stock_historical_data(symbol=ticker, start_date=TODAY_STR, end_date=TODAY_STR, resolution='1', type='stock')
            if df is not None and not df.empty:
                df['symbol'] = ticker
                all_data.append(df)
            time.sleep(1)
        except Exception as e: print(f"Lỗi: {e}")
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        upload_to_gcs(final_df, "today_1m.parquet", f"bronze/daily_run/{TODAY_STR}_1m.parquet")

with DAG(
    'vn30_daily_incremental',
    default_args=default_args,
    description='Daily Incremental: Append vào Partitioned Tables',
    schedule_interval='0 10 * * 1-5',
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=['stock', 'daily', 'bronze'],
) as dag:

    task_extract_daily = PythonOperator(task_id='extract_today_daily', python_callable=fetch_today_daily)
    task_extract_1m = PythonOperator(task_id='extract_today_1m', python_callable=fetch_today_1m)

    task_append_daily_bq = GCSToBigQueryOperator(
        task_id='append_daily_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[f'bronze/daily_run/{TODAY_STR}_daily.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.bronze_historical_daily',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        # CHO PHÉP TỰ CẬP NHẬT CỘT MỚI NẾU CẦN
        schema_update_options=['ALLOW_FIELD_ADDITION'],
        gcp_conn_id=GCP_CONN_ID,
    )

    task_append_1m_bq = GCSToBigQueryOperator(
        task_id='append_1m_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[f'bronze/daily_run/{TODAY_STR}_1m.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.bronze_historical_1m',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        schema_update_options=['ALLOW_FIELD_ADDITION'],
        gcp_conn_id=GCP_CONN_ID,
    )

    task_extract_daily >> task_append_daily_bq
    task_extract_1m >> task_append_1m_bq