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
    
    # 1. ÉP KIỂU THỜI GIAN
    df['time'] = pd.to_datetime(df['time'])
    df['ingestion_timestamp'] = pd.Timestamp.now(tz='UTC') 
    
    # 2. ÉP KIỂU STRING
    cols_to_string = ['open', 'high', 'low', 'close', 'volume', 'symbol', 'ticker']
    for col in cols_to_string:
        if col in df.columns:
            df[col] = df[col].astype(str)
            
    # 3. FIX LỖI NANOSECOND
    df.to_parquet(
        temp_path, 
        index=False, 
        engine='pyarrow',
        coerce_timestamps='us',
        allow_truncated_timestamps=True
    )
    
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
    schedule_interval='0 10 * * 1-5',
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=['stock', 'daily', 'bronze', 'elt'],
) as dag:

    t1 = PythonOperator(task_id='extract_today_daily', python_callable=fetch_today_daily)
    t2 = PythonOperator(task_id='extract_today_1m', python_callable=fetch_today_1m)

    ELT_SCHEMA = [
        {'name': 'time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'open', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'high', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'low', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'close', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'volume', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ticker', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
    ]

    t3 = GCSToBigQueryOperator(
        task_id='append_daily_bq',
        bucket=GCS_BUCKET,
        source_objects=[f'bronze/daily_run/{TODAY_STR}_daily.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.bronze_historical_daily',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND', # DAG Daily thì dùng APPEND
        autodetect=False,
        schema_fields=ELT_SCHEMA,
        schema_update_options=['ALLOW_FIELD_ADDITION'],
        gcp_conn_id=GCP_CONN_ID,
    )

    t4 = GCSToBigQueryOperator(
        task_id='append_1m_bq',
        bucket=GCS_BUCKET,
        source_objects=[f'bronze/daily_run/{TODAY_STR}_1m.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.bronze_historical_1m',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        autodetect=False,
        schema_fields=ELT_SCHEMA,
        schema_update_options=['ALLOW_FIELD_ADDITION'],
        gcp_conn_id=GCP_CONN_ID,
    )

    t1 >> t3
    t2 >> t4