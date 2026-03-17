from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
import pandas as pd
import time
import os
import logging
import re

# --- SỬA LẠI IMPORT THEO CHUẨN VNSTOCK 3 ---
from vnstock import Finance

PROJECT_ID = 'stock-lambda-project'
GCS_BUCKET = 'stock-datalake-raw-khoinguyen'
BQ_DATASET = 'stock_data_warehouse'
GCP_CONN_ID = 'google_cloud_default'

VN30_TICKERS = [
    'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG', 
    'MBB', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSI', 'SSB', 'STB', 
    'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'
]

# DAG chạy hàng tuần, lấy ngày chủ nhật làm tên file
RUN_DATE_STR = datetime.now().strftime('%Y-%m-%d')

default_args = {
    'owner': 'khoi_nguyen',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def upload_to_gcs(df, file_name, destination_blob_name):
    if df is None or df.empty: 
        logging.warning("DataFrame rỗng, bỏ qua bước upload lên GCS.")
        return
        
    # --- BƯỚC MỚI: CHUẨN HÓA TÊN CỘT CHO BIGQUERY ---
    # Thay thế mọi ký tự không phải chữ/số (như khoảng trắng, ngoặc, chấm) thành dấu gạch dưới "_"
    # Ví dụ: "Revenue (Bn. VND)" -> "Revenue__Bn__VND"
    df.columns = df.columns.str.replace(r'\W+', '_', regex=True).str.strip('_')
    
    temp_path = f"/tmp/{file_name}"
    
    # Ép thời gian lấy dữ liệu
    df['ingestion_timestamp'] = pd.Timestamp.now(tz='UTC') 
    
    # TƯ DUY ELT: Ép toàn bộ các cột (trừ ingestion_timestamp) sang STRING
    for col in df.columns:
        if col != 'ingestion_timestamp':
            df[col] = df[col].astype(str)
            
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
    logging.info(f"Đã upload {len(df)} dòng lên GCS: {destination_blob_name}")

def fetch_financial_statements(**kwargs):
    all_data = []
    failed_tickers = []
    
    for ticker in VN30_TICKERS:
        try:
            logging.info(f"Đang kéo BCTC của: {ticker}")
            
            # 1. KHỞI TẠO ĐÚNG CLASS FINANCE CỦA VNSTOCK 3
            finance = Finance(symbol=ticker, source='VCI')
            
            # 2. CHỌN LOẠI BÁO CÁO CẦN LẤY
            df_finance = finance.income_statement(period='quarter')
            
            if df_finance is not None and not df_finance.empty:
                df_finance['symbol'] = ticker
                all_data.append(df_finance)
            else:
                logging.warning(f"Cảnh báo: API trả về dữ liệu rỗng cho mã {ticker}")
                
            time.sleep(1.5) # Nghỉ ngơi để tránh bị chặn IP
            
        except Exception as e: 
            logging.error(f"Lỗi khi kéo BCTC {ticker}: {e}")
            failed_tickers.append(ticker)
            
    # 3. CHẶN LỖI NGẦM (SILENT FAILURE)
    if failed_tickers:
        raise RuntimeError(f"Task thất bại. Không thể kéo dữ liệu cho các mã: {failed_tickers}")
        
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # Lưu vào một thư mục riêng biệt cho dữ liệu tĩnh
        upload_to_gcs(final_df, "weekly_finance.parquet", "bronze/financials/financial_statements.parquet")
    else:
        raise ValueError("Lỗi nghiêm trọng: Không có bất kỳ dữ liệu nào được kéo thành công.")

with DAG(
    'vn30_financial_statements_weekly',
    default_args=default_args,
    description='Kéo BCTC toàn bộ lịch sử mỗi tuần 1 lần',
    schedule_interval='0 6 * * 0', # 6h sáng mỗi Chủ Nhật
    start_date=datetime(2024, 3, 1),
    catchup=False,
    tags=['stock', 'weekly', 'financials', 'bronze', 'elt'],
) as dag:

    extract_finance = PythonOperator(
        task_id='extract_financial_statements', 
        python_callable=fetch_financial_statements
    )

    load_bq = GCSToBigQueryOperator(
        task_id='load_finance_bq',
        bucket=GCS_BUCKET,
        source_objects=['bronze/financials/financial_statements.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.bronze_financial_statements',
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE', # CHIẾN THUẬT: Xóa sạch bảng cũ, tạo bảng mới
        autodetect=True, 
        cluster_fields=['symbol'], 
        gcp_conn_id=GCP_CONN_ID,
    )

    extract_finance >> load_bq