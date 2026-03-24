"""
Chạy file này để kiểm tra kết nối trước khi start app:
  cd streamlit_app
  python test_connections.py
Xóa file này sau khi test thành công.
"""
import sys
import os
sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, ".")

try:
    import tomllib
except ImportError:
    import tomli as tomllib

with open(".streamlit/secrets.toml", "rb") as f:
    secrets = tomllib.load(f)

# === TEST POSTGRES ===
print("=" * 40)
print("Testing Postgres connection...")
try:
    from sqlalchemy import create_engine, text
    cfg = secrets["postgres"]
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stock_candles"))
        count = result.scalar()
    print(f"✅ Postgres OK — stock_candles có {count:,} rows")

    with engine.connect() as conn:
        result2 = conn.execute(
            text("SELECT DISTINCT symbol FROM stock_candles ORDER BY symbol LIMIT 5")
        )
        symbols = [row[0] for row in result2]
    print(f"   Sample symbols: {symbols}")
except Exception as e:
    print(f"❌ Postgres FAIL: {e}")
    print("   Kiểm tra: Docker đang chạy? POSTGRES_PASSWORD đúng?")

# === TEST BIGQUERY ===
print()
print("Testing BigQuery connection...")
try:
    from google.cloud import bigquery
    client = bigquery.Client(project=secrets["bigquery"]["project"])

    df = client.query("""
        SELECT COUNT(*) as cnt
        FROM `stock-lambda-project.stock_data_warehouse.mart_daily_stock_performance`
    """).to_dataframe()
    print(f"✅ BigQuery OK — mart_daily_stock_performance có {df['cnt'][0]:,} rows")

    df2 = client.query("""
        SELECT COUNT(*) as cnt
        FROM `stock-lambda-project.stock_data_warehouse.mart_intraday_whale`
    """).to_dataframe()
    print(f"✅ BigQuery OK — mart_intraday_whale có {df2['cnt'][0]:,} rows")
except Exception as e:
    print(f"❌ BigQuery FAIL: {e}")
    print("   Kiểm tra: GOOGLE_APPLICATION_CREDENTIALS đã set?")
    print(f"   Hiện tại: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'CHƯA SET')}")

print()
print("=" * 40)
print("Nếu cả 2 đều ✅, xóa file này và chạy: streamlit run app.py")
