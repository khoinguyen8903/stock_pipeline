import os
import sys
import time
import pandas as pd
from vnstock import Vnstock
from google.cloud import storage

# --- CẤU HÌNH ---
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/data/gcpkey.json"
BUCKET_NAME = "stock-datalake-raw-khoinguyen"
DESTINATION_BLOB_NAME = "dimension/dim_company.parquet"

def get_all_companies():
    print("⏳ Đang lấy danh sách toàn bộ mã chứng khoán trên 3 sàn (HOSE, HNX, UPCOM)...")
    try:
        stock = Vnstock().stock(symbol='FPT', source='VCI')
        df_tickers = stock.listing.all_symbols()
        
        # IN RA ĐỂ DEBUG: Xem thư viện thực sự trả về những cột tên là gì
        print(f"🔍 Cấu trúc bảng trả về có các cột: {df_tickers.columns.tolist()}")
        
        # TỰ ĐỘNG DÒ TÌM TÊN CỘT CHỨA MÃ CHỨNG KHOÁN
        if 'ticker' in df_tickers.columns:
            all_symbols = df_tickers['ticker'].tolist()
        elif 'symbol' in df_tickers.columns:
            all_symbols = df_tickers['symbol'].tolist()
        else:
            # Rơi vào đường cùng: Ép lấy dữ liệu của cột đầu tiên
            all_symbols = df_tickers.iloc[:, 0].tolist()
            
        print(f"✅ Đã tìm thấy {len(all_symbols)} mã chứng khoán. Bắt đầu thu thập hồ sơ...")
    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng khi lấy danh sách mã: {e}")
        sys.exit(1) 

    company_profiles = []
    
    # Đã gỡ bỏ [:10], hệ thống sẽ cào TOÀN BỘ danh sách thị trường
    for i, sym in enumerate(all_symbols):
        try:
            profile = Vnstock().stock(symbol=sym, source='VCI').company.overview()
            if profile is not None and not profile.empty:
                profile['symbol'] = sym 
                company_profiles.append(profile)
            
            if (i + 1) % 100 == 0:
                print(f"🔄 Đã cào được {i + 1}/{len(all_symbols)} công ty...")
        except Exception as e:
            pass # Bỏ qua mã rác
        
        time.sleep(1)

    if company_profiles:
        df_final = pd.concat(company_profiles, ignore_index=True)
        return df_final
    else:
        print("❌ Cảnh báo: Thu thập xong nhưng không có dữ liệu doanh nghiệp nào!")
        sys.exit(1) 

def upload_to_gcs(df):
    if df.empty:
        print("🛑 Dữ liệu rỗng. Hủy quá trình upload.")
        sys.exit(1) 

    df = df.astype(str)

    temp_file = "/tmp/dim_company.parquet"
    df.to_parquet(temp_file, engine="pyarrow", index=False)
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB_NAME)
    
    blob.upload_from_filename(temp_file)
    os.remove(temp_file)
    print(f"🎉 Đã upload thành công {len(df)} hồ sơ công ty lên gs://{BUCKET_NAME}/{DESTINATION_BLOB_NAME}")

if __name__ == "__main__":
    df_companies = get_all_companies()
    upload_to_gcs(df_companies)