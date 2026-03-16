from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import pandas as pd
import time
import os
from dateutil.relativedelta import relativedelta
from vnstock import stock_historical_data

# --- CẤU HÌNH DỰ ÁN ---
PROJECT_ID = 'stock-lambda-project'
GCS_BUCKET = 'stock-datalake-raw-khoinguyen'
BQ_DATASET = 'stock_data_warehouse'
GCP_CONN_ID = 'google_cloud_default'

VN30_TICKERS = [
    'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 
    'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSI', 'SSB', 'STB', 
    'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'
]

default_args = {
    'owner': 'khoi_nguyen',
    'depends_on_past': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
}

def upload_to_gcs(df, file_name, destination_blob_name):
    if df.empty:
        return
    temp_path = f"/tmp/{file_name}"
    
    # ÉP KIỂU DATETIME để BigQuery nhận diện được Partition
    df['time'] = pd.to_datetime(df['time'])
    df['symbol'] = df['symbol'].astype(str)
    df['ingestion_timestamp'] = pd.Timestamp.now(tz='UTC')
    
    df.to_parquet(temp_path, index=False)
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    gcs_hook.upload(bucket_name=GCS_BUCKET, object_name=destination_blob_name, filename=temp_path)
    os.remove(temp_path)

def fetch_historical_daily(**kwargs):
    print("BẮT ĐẦU CÀO NẾN NGÀY (TỪ 2000)...")
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = '2000-01-01'
    all_daily_data = []
    for ticker in VN30_TICKERS:
        try:
            df = stock_historical_data(symbol=ticker, start_date=start_date, end_date=end_date, resolution='1D', type='stock')
            if df is not None and not df.empty:
                df['symbol'] = ticker
                all_daily_data.append(df)
            time.sleep(1) 
        except Exception as e:
            print(f"Lỗi {ticker}: {e}")
    if all_daily_data:
        final_df = pd.concat(all_daily_data, ignore_index=True)
        upload_to_gcs(final_df, "historical_daily.parquet", "bronze/historical_daily/data.parquet")

def fetch_historical_1m(**kwargs):
    print("BẮT ĐẦU CÀO NẾN 1 PHÚT (3 THÁNG GẦN NHẤT)...")
    end_date_obj = datetime.now()
    start_date_obj = end_date_obj - relativedelta(months=3)
    all_1m_data = []
    for ticker in VN30_TICKERS:
        current_start = start_date_obj
        while current_start < end_date_obj:
            current_end = current_start + relativedelta(days=30)
            if current_end > end_date_obj: current_end = end_date_obj
            str_start = current_start.strftime('%Y-%m-%d')
            str_end = current_end.strftime('%Y-%m-%d')
            try:
                df = stock_historical_data(symbol=ticker, start_date=str_start, end_date=str_end, resolution='1', type='stock')
                if df is not None and not df.empty:
                    df['symbol'] = ticker
                    all_1m_data.append(df)
                time.sleep(1.5) 
            except Exception as e:
                print(f"Lỗi {ticker}: {e}")
            current_start = current_end + relativedelta(days=1)
    if all_1m_data:
        final_df = pd.concat(all_1m_data, ignore_index=True)
        upload_to_gcs(final_df, "historical_1m.parquet", "bronze/historical_1m/data.parquet")

with DAG(
    'vn30_historical_bootstrap',
    default_args=default_args,
    description='Bootstrap: Nạp lịch sử & Tạo bảng Partitioned',
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stock', 'bootstrap', 'partition'],
) as dag:

    task_extract_daily = PythonOperator(task_id='extract_daily_to_gcs', python_callable=fetch_historical_daily)
    task_extract_1m = PythonOperator(task_id='extract_1m_to_gcs', python_callable=fetch_historical_1m)

    task_load_daily_bq = GCSToBigQueryOperator(
        task_id='load_daily_to_bq',
        bucket=GCS_BUCKET,
        source_objects=['bronze/historical_daily/*.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.bronze_historical_daily',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED',
        # CẤU HÌNH PARTITION THEO NGÀY
        time_partitioning={"type": "DAY", "field": "time"},
        gcp_conn_id=GCP_CONN_ID,
    )

    task_load_1m_bq = GCSToBigQueryOperator(
        task_id='load_1m_to_bq',
        bucket=GCS_BUCKET,
        source_objects=['bronze/historical_1m/*.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.bronze_historical_1m',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        # CẤU HÌNH PARTITION THEO THÁNG (Cho nến 1p)
        time_partitioning={"type": "MONTH", "field": "time"},
        gcp_conn_id=GCP_CONN_ID,
    )

    task_extract_daily >> task_load_daily_bq
    task_extract_1m >> task_load_1m_bq