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

# Rổ VN30 (Đã thêm SSI)
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
    """Hàm phụ trợ: Lưu DataFrame thành Parquet và đẩy lên GCS bằng Airflow Hook"""
    if df.empty:
        print(f"DataFrame rỗng, bỏ qua upload cho {destination_blob_name}")
        return
    
    temp_path = f"/tmp/{file_name}"
    
    # 1. Ép kiểu chuẩn cho cột symbol
    df['symbol'] = df['symbol'].astype(str)
    
    # 2. THÊM CỘT TIMESTAMP (Đồng bộ với DAG Daily để tránh lỗi Schema)
    df['ingestion_timestamp'] = pd.Timestamp.now(tz='UTC')
    
    # 3. Lưu file tạm
    df.to_parquet(temp_path, index=False)
    
    # 4. Upload bằng GCSHook
    gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=destination_blob_name,
        filename=temp_path
    )
    
    os.remove(temp_path)
    print(f"Đã upload thành công: gs://{GCS_BUCKET}/{destination_blob_name}")

def fetch_historical_daily(**kwargs):
    """Cào nến Ngày từ năm 2000 đến nay"""
    print("BẮT ĐẦU CÀO NẾN NGÀY (TỪ 2000)...")
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = '2000-01-01'
    
    all_daily_data = []
    
    for ticker in VN30_TICKERS:
        print(f"Đang cào nến Ngày cho: {ticker}")
        try:
            df = stock_historical_data(symbol=ticker, start_date=start_date, end_date=end_date, resolution='1D', type='stock')
            if df is not None and not df.empty:
                df['symbol'] = ticker
                all_daily_data.append(df)
            time.sleep(1) 
        except Exception as e:
            print(f"Lỗi khi cào nến Ngày {ticker}: {e}")
            
    if not all_daily_data:
        return

    final_df = pd.concat(all_daily_data, ignore_index=True)
    upload_to_gcs(final_df, "historical_daily.parquet", "bronze/historical_daily/data.parquet")

def fetch_historical_1m(**kwargs):
    """Cào nến 1 Phút trong 3 tháng gần nhất (Chia nhỏ từng tháng để tránh sập API)"""
    print("BẮT ĐẦU CÀO NẾN 1 PHÚT (3 THÁNG GẦN NHẤT)...")
    end_date_obj = datetime.now()
    start_date_obj = end_date_obj - relativedelta(months=3)
    
    all_1m_data = []
    
    for ticker in VN30_TICKERS:
        print(f"--- Đang xử lý nến 1p cho: {ticker} ---")
        current_start = start_date_obj
        
        while current_start < end_date_obj:
            current_end = current_start + relativedelta(days=30)
            if current_end > end_date_obj:
                current_end = end_date_obj
                
            str_start = current_start.strftime('%Y-%m-%d')
            str_end = current_end.strftime('%Y-%m-%d')
            
            print(f"  -> Cào {ticker} từ {str_start} đến {str_end}")
            try:
                df = stock_historical_data(symbol=ticker, start_date=str_start, end_date=str_end, resolution='1', type='stock')
                if df is not None and not df.empty:
                    df['symbol'] = ticker
                    all_1m_data.append(df)
                time.sleep(1.5) 
            except Exception as e:
                print(f"  -> Lỗi cào {ticker} ({str_start} - {str_end}): {e}")
            
            current_start = current_end + relativedelta(days=1)
            
    if not all_1m_data:
        return

    final_df = pd.concat(all_1m_data, ignore_index=True)
    upload_to_gcs(final_df, "historical_1m.parquet", "bronze/historical_1m/data.parquet")

# --- ĐỊNH NGHĨA DAG ---
with DAG(
    'vn30_historical_bootstrap',
    default_args=default_args,
    description='Chạy 1 lần: Nạp toàn bộ lịch sử VN30 có Timestamp',
    schedule_interval=None, 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stock', 'bootstrap', 'bronze'],
) as dag:

    # 1. Task cào dữ liệu đẩy lên GCS
    task_extract_daily = PythonOperator(
        task_id='extract_daily_to_gcs',
        python_callable=fetch_historical_daily,
    )

    task_extract_1m = PythonOperator(
        task_id='extract_1m_to_gcs',
        python_callable=fetch_historical_1m,
    )

    # 2. Task load từ GCS vào BigQuery (Sử dụng WRITE_TRUNCATE để làm sạch bảng)
    task_load_daily_bq = GCSToBigQueryOperator(
        task_id='load_daily_to_bq',
        bucket=GCS_BUCKET,
        source_objects=['bronze/historical_daily/*.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.bronze_historical_daily',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE', 
        create_disposition='CREATE_IF_NEEDED',
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
        gcp_conn_id=GCP_CONN_ID,
    )

    # --- THỨ TỰ THỰC THI ---
    task_extract_daily >> task_load_daily_bq
    task_extract_1m >> task_load_1m_bq