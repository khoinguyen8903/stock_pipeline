import streamlit as st
from sqlalchemy import create_engine, text
import pandas as pd


@st.cache_resource
def get_pg_engine():
    """
    Tạo connection pool SQLAlchemy, cache suốt vòng đời app.
    pool_size=5: tối đa 5 kết nối đồng thời
    pool_pre_ping=True: tự kiểm tra kết nối chết trước khi dùng
    pool_recycle=300: recycle kết nối sau 5 phút tránh timeout
    """
    cfg = st.secrets["postgres"]
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    return create_engine(
        url,
        pool_size=5,
        max_overflow=2,
        pool_pre_ping=True,
        pool_recycle=300,
    )


@st.cache_data(ttl=15)
def get_candles(symbol: str, limit: int = 200) -> pd.DataFrame:
    """
    Lấy nến 1 phút từ Postgres.
    ttl=15: cache 15 giây, tự động fetch lại khi auto-refresh trigger.
    Trả về DataFrame đã sort tăng dần theo thời gian (chuẩn cho Plotly chart).
    """
    engine = get_pg_engine()
    query = text("""
        SELECT symbol, window_start, window_end,
               open_price, high_price, low_price, close_price,
               volume, tick_count
        FROM stock_candles
        WHERE symbol = :symbol
        ORDER BY window_start DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol, "limit": limit})
    return df.sort_values("window_start").reset_index(drop=True)


@st.cache_data(ttl=15)
def get_available_symbols() -> list[str]:
    """Lấy danh sách tất cả symbol đang có trong Postgres."""
    engine = get_pg_engine()
    query = text("SELECT DISTINCT symbol FROM stock_candles ORDER BY symbol")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df["symbol"].tolist()
