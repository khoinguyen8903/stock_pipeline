# ============================================================
# IMPORTS
# ============================================================
import datetime
import os
import pathlib

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Hệ Thống Phân Tích Dòng Tiền",
    page_icon="💹",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# CONSTANTS
# ============================================================
BQ_PROJECT: str = os.getenv("PROJECT_ID", "stock-lambda-project")
BQ_DATASET: str = os.getenv("BQ_DATASET", "stock_data_warehouse")
GCP_KEY_PATH: pathlib.Path = pathlib.Path(
    os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        str(pathlib.Path(__file__).resolve().parents[1] / "data" / "gcpkey.json"),
    )
)

COLOR_UP = "#00c2a8"
COLOR_DOWN = "#ff2e51"
COLOR_MA20 = "#FFA726"
COLOR_MA50 = "#42A5F5"


# ============================================================
# CONNECTIONS — BigQuery (Cold Path)
# ============================================================
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(
        str(GCP_KEY_PATH),
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    return bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )


# ============================================================
# CONNECTIONS — PostgreSQL (Hot Path)
# ============================================================
@st.cache_resource
def get_pg_engine() -> Engine:
    pg_user = os.getenv("POSTGRES_USER") or st.secrets["postgres"]["user"]
    pg_pass = os.getenv("POSTGRES_PASSWORD") or st.secrets["postgres"]["password"]
    pg_host = os.getenv("POSTGRES_HOST") or st.secrets["postgres"]["host"]
    pg_port = os.getenv("POSTGRES_PORT", "") or st.secrets["postgres"]["port"]
    pg_db = os.getenv("POSTGRES_DB") or st.secrets["postgres"]["database"]
    url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    return create_engine(
        url,
        pool_size=5,
        max_overflow=2,
        pool_pre_ping=True,
        pool_recycle=300,
    )


# ============================================================
# DATA FETCHING — Symbol list (from BigQuery mart)
# ============================================================
@st.cache_data(ttl=3600)
def fetch_symbol_list() -> pd.DataFrame:
    """Lấy danh sách symbol + sector từ mart, chỉ giữ symbol — không lấy mô tả."""
    client = get_bq_client()
    sql = f"""
        SELECT DISTINCT symbol, sector
        FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_daily_stock_performance`
        WHERE symbol IS NOT NULL AND sector IS NOT NULL
        ORDER BY sector, symbol
    """
    return client.query(sql).to_dataframe()


# ============================================================
# DATA FETCHING — Cold Path (BigQuery marts, cached 10 min)
# ============================================================
@st.cache_data(ttl=600)
def fetch_daily_data(symbol: str) -> pd.DataFrame:
    """Lấy TOÀN BỘ dữ liệu daily từ mart_daily_stock_performance (từ 2000)."""
    client = get_bq_client()
    sql = f"""
        SELECT
            trading_date, symbol,
            open_price, high_price, low_price, close_price,
            volume, pct_change, traded_value,
            ma_20, ma_50, rsi_14
        FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_daily_stock_performance`
        WHERE symbol = @symbol
        ORDER BY trading_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    num_cols = [
        "open_price", "high_price", "low_price", "close_price",
        "volume", "pct_change", "traded_value", "ma_20", "ma_50", "rsi_14",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values("trading_date").reset_index(drop=True)


@st.cache_data(ttl=600)
def fetch_whale_data(
    symbol: str, target_date: datetime.date
) -> pd.DataFrame:
    """Lấy dữ liệu cá mập từ mart_intraday_whale."""
    client = get_bq_client()
    sql = f"""
        SELECT
            whale_category,
            SUM(trade_value) AS total_value,
            COUNT(*) AS total_trades
        FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_intraday_whale`
        WHERE symbol = @symbol AND trading_date = @target_date
        GROUP BY whale_category
        ORDER BY total_value DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    for col in ("total_value", "total_trades"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=600)
def fetch_pressure_data(
    symbol: str, target_date: datetime.date
) -> pd.DataFrame:
    """Lấy dữ liệu áp lực mua/bán 1 ngày (cho KPI cards)."""
    client = get_bq_client()
    sql = f"""
        SELECT
            match_type,
            SUM(volume) AS total_volume,
            COUNT(*) AS total_trades
        FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_intraday_pressure`
        WHERE symbol = @symbol
          AND match_type IN ('Buy', 'Sell')
          AND trading_date = @target_date
        GROUP BY match_type
        ORDER BY match_type
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    for col in ("total_volume", "total_trades"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


@st.cache_data(ttl=600)
def fetch_pressure_7d(
    symbol: str, end_date: datetime.date
) -> pd.DataFrame:
    """Lấy áp lực mua/bán 7 ngày giao dịch gần nhất (cho biểu đồ xu hướng)."""
    client = get_bq_client()
    sql = f"""
        WITH recent_days AS (
            SELECT DISTINCT trading_date
            FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_intraday_pressure`
            WHERE symbol = @symbol
              AND trading_date <= @end_date
              AND EXTRACT(DAYOFWEEK FROM trading_date) NOT IN (1, 7)
            ORDER BY trading_date DESC
            LIMIT 7
        )
        SELECT
            p.trading_date,
            p.match_type,
            SUM(p.volume) AS total_volume
        FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_intraday_pressure` p
        INNER JOIN recent_days r ON p.trading_date = r.trading_date
        WHERE p.symbol = @symbol
          AND p.match_type IN ('Buy', 'Sell')
        GROUP BY p.trading_date, p.match_type
        ORDER BY p.trading_date, p.match_type
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    if "total_volume" in df.columns:
        df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce")
    return df.sort_values("trading_date").reset_index(drop=True)


# ============================================================
# DATA FETCHING — Tick data / Sổ lệnh (BigQuery staging)
# ============================================================
@st.cache_data(ttl=600)
def fetch_tick_data(
    symbol: str, target_date: datetime.date
) -> pd.DataFrame:
    """Lấy toàn bộ sổ lệnh chi tiết từ stg_stock_ticks theo ngày, giờ VN."""
    client = get_bq_client()
    sql = f"""
        SELECT
            DATETIME(time, 'Asia/Ho_Chi_Minh') AS time_vn,
            price,
            volume,
            match_type
        FROM `{BQ_PROJECT}.{BQ_DATASET}.stg_stock_ticks`
        WHERE symbol = @symbol
          AND DATE(time, 'Asia/Ho_Chi_Minh') = @target_date
        ORDER BY time DESC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    for col in ("price", "volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ============================================================
# DATA FETCHING — Cold Path: Nến 1 phút lịch sử (BigQuery)
# ============================================================
@st.cache_data(ttl=600)
def fetch_bq_1m_data(
    symbol: str, target_date: datetime.date
) -> pd.DataFrame:
    """Lấy nến 1 phút lịch sử từ mart_intraday_price (Cold Path)."""
    client = get_bq_client()
    sql = f"""
        SELECT
            trading_time AS window_start,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        FROM `{BQ_PROJECT}.{BQ_DATASET}.mart_intraday_price`
        WHERE symbol = @symbol
          AND trading_date = @target_date
        ORDER BY trading_time
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
        ]
    )
    df = client.query(sql, job_config=job_config).to_dataframe()
    num_cols = ["open_price", "high_price", "low_price", "close_price", "volume"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if not df.empty:
        df["window_start"] = pd.to_datetime(df["window_start"])
    return df.sort_values("window_start").reset_index(drop=True) if not df.empty else df


# ============================================================
# DATA FETCHING — Hot Path (PostgreSQL, NO CACHE — realtime)
# ============================================================
def fetch_minute_candles(symbol: str, limit: int = 200) -> pd.DataFrame:
    """Lấy nến 1 phút realtime từ Postgres (stock_candles)."""
    engine = get_pg_engine()
    sql = text("""
        SELECT symbol, window_start, window_end,
               open_price, high_price, low_price, close_price,
               volume, tick_count
        FROM stock_candles
        WHERE symbol = :symbol
        ORDER BY window_start DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"symbol": symbol, "limit": limit})
    if df.empty:
        return df
    for col in ("window_start", "window_end"):
        ts = pd.to_datetime(df[col])
        if ts.dt.tz is None:
            df[col] = ts.tz_localize("UTC").dt.tz_convert("Asia/Ho_Chi_Minh")
        else:
            df[col] = ts.dt.tz_convert("Asia/Ho_Chi_Minh")
    return df.sort_values("window_start").reset_index(drop=True)


# ============================================================
# UI — Daily Candlestick + Volume chart
# ============================================================
def build_candlestick_chart(df: pd.DataFrame) -> go.Figure:
    x_dates = pd.to_datetime(df["trading_date"])
    x_labels = x_dates.dt.strftime("%Y-%m-%d")

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0,
        row_heights=[0.8, 0.2],
    )

    fig.add_trace(
        go.Candlestick(
            x=x_labels,
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            name="OHLC",
            increasing_line_color=COLOR_UP,
            decreasing_line_color=COLOR_DOWN,
            increasing_fillcolor=COLOR_UP,
            decreasing_fillcolor=COLOR_DOWN,
        ),
        row=1, col=1,
    )

    ma20 = df["ma_20"].dropna()
    if not ma20.empty:
        fig.add_trace(
            go.Scatter(
                x=x_labels[ma20.index], y=ma20,
                mode="lines", name="MA 20",
                line=dict(color=COLOR_MA20, width=1.2),
            ),
            row=1, col=1,
        )

    ma50 = df["ma_50"].dropna()
    if not ma50.empty:
        fig.add_trace(
            go.Scatter(
                x=x_labels[ma50.index], y=ma50,
                mode="lines", name="MA 50",
                line=dict(color=COLOR_MA50, width=1.2),
            ),
            row=1, col=1,
        )

    vol_colors = [
        COLOR_UP if c >= o else COLOR_DOWN
        for o, c in zip(df["open_price"], df["close_price"])
    ]
    fig.add_trace(
        go.Bar(
            x=x_labels, y=df["volume"],
            marker_color=vol_colors,
            name="Volume", showlegend=False,
        ),
        row=2, col=1,
    )

    n = len(df)
    visible = min(n, 250)
    default_range = [n - visible - 0.5, n - 0.5]

    fig.update_layout(
        template="plotly_dark",
        height=560,
        dragmode="pan",
        margin=dict(l=0, r=0, t=30, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#0e1117",
        xaxis_rangeslider_visible=False,
        bargap=0,
        legend=dict(
            orientation="h", y=1.02, x=0.5, xanchor="center",
            font=dict(size=10),
        ),
        xaxis=dict(
            type="category",
            showgrid=False, showticklabels=False,
            range=default_range,
        ),
        xaxis2=dict(
            type="category",
            showgrid=False,
            tickfont=dict(size=9),
            nticks=20,
            range=default_range,
        ),
        yaxis=dict(
            gridcolor="#222", showgrid=True,
            tickfont=dict(size=9), side="right",
        ),
        yaxis2=dict(
            gridcolor="#222", showgrid=True,
            tickfont=dict(size=9), side="right",
        ),
    )

    return fig


# ============================================================
# UI — Minute Candlestick chart (Real-time from Postgres)
# ============================================================
def build_minute_chart(df: pd.DataFrame) -> go.Figure:
    x_labels = pd.to_datetime(df["window_start"]).dt.strftime("%H:%M")

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0,
        row_heights=[0.8, 0.2],
    )

    fig.add_trace(
        go.Candlestick(
            x=x_labels,
            open=df["open_price"],
            high=df["high_price"],
            low=df["low_price"],
            close=df["close_price"],
            name="OHLC",
            increasing_line_color=COLOR_UP,
            decreasing_line_color=COLOR_DOWN,
            increasing_fillcolor=COLOR_UP,
            decreasing_fillcolor=COLOR_DOWN,
        ),
        row=1, col=1,
    )

    vol_colors = [
        COLOR_UP if c >= o else COLOR_DOWN
        for o, c in zip(df["open_price"], df["close_price"])
    ]
    fig.add_trace(
        go.Bar(
            x=x_labels, y=df["volume"],
            marker_color=vol_colors,
            name="Volume", showlegend=False,
        ),
        row=2, col=1,
    )

    fig.update_layout(
        template="plotly_dark",
        height=560,
        dragmode="pan",
        margin=dict(l=0, r=0, t=30, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#0e1117",
        xaxis_rangeslider_visible=False,
        bargap=0,
        legend=dict(
            orientation="h", y=1.02, x=0.5, xanchor="center",
            font=dict(size=10),
        ),
        xaxis=dict(
            type="category",
            showgrid=False, showticklabels=False,
        ),
        xaxis2=dict(
            type="category",
            showgrid=False,
            tickfont=dict(size=9),
            nticks=20,
        ),
        yaxis=dict(
            gridcolor="#222", showgrid=True,
            tickfont=dict(size=9), side="right",
        ),
        yaxis2=dict(
            gridcolor="#222", showgrid=True,
            tickfont=dict(size=9), side="right",
        ),
    )

    return fig


# ============================================================
# UI — Price Info Panel (native Streamlit components)
# ============================================================
def render_price_panel(row: pd.Series) -> None:
    def fmt(val: object) -> str:
        """Giữ nguyên giá trị gốc từ DB, không làm tròn."""
        if pd.isna(val):
            return "–"
        if isinstance(val, float) and val == int(val):
            return f"{int(val):,}"
        return f"{val:,}"

    symbol = row.get("symbol", "")
    pct = row.get("pct_change")
    close = row.get("close_price")
    traded = row.get("traded_value")
    rsi = row.get("rsi_14")

    trading_date = row.get("trading_date")
    date_str = ""
    if trading_date is not None:
        date_str = pd.to_datetime(trading_date).strftime("%d/%m/%Y")

    pct_str = f"{pct:+.2f}" if pd.notna(pct) else "N/A"
    is_up = pd.notna(pct) and pct >= 0
    accent = COLOR_UP if is_up else COLOR_DOWN
    arrow = "▲" if is_up else "▼"

    items = [
        ("Mở cửa", fmt(row.get("open_price")), "#e6edf3"),
        ("Cao nhất", fmt(row.get("high_price")), COLOR_UP),
        ("Thấp nhất", fmt(row.get("low_price")), COLOR_DOWN),
        ("Khối lượng", fmt(row.get("volume")), "#e6edf3"),
        ("GTGD", f"{traded / 1e9:,.2f} tỷ" if pd.notna(traded) and traded else "–", "#e6edf3"),
        ("MA 20", fmt(row.get("ma_20")), COLOR_MA20),
        ("MA 50", fmt(row.get("ma_50")), COLOR_MA50),
        ("RSI (14)", fmt(rsi), "#e6edf3"),
    ]

    rows_html = "".join(
        f'<tr style="border-bottom:1px solid #21262d;">'
        f'<td style="padding:8px 6px;color:#8b949e;font-size:13px;">{label}</td>'
        f'<td style="padding:8px 6px;color:{color};text-align:right;font-size:13px;font-weight:500;">{value}</td>'
        f'</tr>'
        for label, value, color in items
    )

    st.markdown(
        f'<div style="background:#161b22;border:1px solid #30363d;border-radius:10px;'
        f'padding:16px 14px;font-family:Segoe UI,sans-serif;">'
        f'<div style="color:#8b949e;font-size:12px;margin-bottom:2px;">📅 {date_str}</div>'
        f'<div style="color:#e6edf3;font-size:17px;font-weight:700;margin-bottom:10px;">{symbol}</div>'
        f'<div style="text-align:center;margin-bottom:14px;">'
        f'<span style="font-size:30px;font-weight:700;color:{accent};">{fmt(close)}</span>'
        f'<div style="font-size:14px;color:{accent};margin-top:2px;">{arrow} {pct_str}%</div>'
        f'</div>'
        f'<table style="width:100%;border-collapse:collapse;">{rows_html}</table>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ============================================================
# UI — Order Book / Sổ lệnh chi tiết (from stg_stock_ticks)
# ============================================================
def render_order_book(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Chưa có dữ liệu sổ lệnh cho ngày này.")
        return

    display = pd.DataFrame({
        "Thời gian": pd.to_datetime(df["time_vn"]).dt.strftime("%H:%M:%S"),
        "Giá": df["price"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "–"),
        "KL": df["volume"],
        "Lệnh": df["match_type"],
    })

    def _color_row(row: pd.Series) -> list[str]:
        mt = str(row["Lệnh"]).strip().upper()
        if mt in ("BU", "BUY", "B"):
            return [f"color: {COLOR_UP}"] * len(row)
        if mt in ("SD", "SELL", "S"):
            return [f"color: {COLOR_DOWN}"] * len(row)
        return [""] * len(row)

    styled = display.style.apply(_color_row, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True, height=520)


# ============================================================
# UI — Whale Donut Chart
# ============================================================
def _build_whale_donut(df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = go.Figure(
        go.Pie(
            labels=df["whale_category"],
            values=df["total_value"],
            hole=0.4,
            marker=dict(colors=[
                "#ab47bc", "#42A5F5", "#FFA726", "#00c2a8", "#ff2e51", "#78909c",
            ][: len(df)]),
            textinfo="percent",
            textposition="inside",
            textfont=dict(size=12, color="#fff"),
            hovertemplate=(
                "<b>%{label}</b><br>"
                "Giá trị: %{value:,.0f} VNĐ<br>"
                "Tỷ trọng: %{percent}<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        template="plotly_dark",
        height=380,
        margin=dict(l=10, r=10, t=40, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        title=dict(text=f"🐋 Dòng tiền Cá mập — {symbol}", x=0.5, xanchor="center"),
        showlegend=True,
        legend=dict(
            orientation="h",
            y=-0.15,
            x=0.5,
            xanchor="center",
            font=dict(size=11),
        ),
    )
    return fig


# ============================================================
# UI — Buy/Sell Pressure 7-day Grouped Bar Chart
# ============================================================
def _build_pressure_7d_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    buy_df = df[df["match_type"] == "Buy"].set_index("trading_date")
    sell_df = df[df["match_type"] == "Sell"].set_index("trading_date")
    all_dates = sorted(df["trading_date"].unique())
    n_sessions = len(all_dates)

    buy_vals = [int(buy_df.loc[d, "total_volume"]) if d in buy_df.index else 0 for d in all_dates]
    sell_vals = [int(sell_df.loc[d, "total_volume"]) if d in sell_df.index else 0 for d in all_dates]
    labels = [pd.to_datetime(d).strftime("%d/%m") for d in all_dates]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=labels, y=buy_vals,
            name="Mua chủ động",
            marker_color="#00C853",
            hovertemplate="Mua: %{y:,}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Bar(
            x=labels, y=sell_vals,
            name="Bán chủ động",
            marker_color="#FF5252",
            hovertemplate="Bán: %{y:,}<extra></extra>",
        )
    )

    fig.update_layout(
        barmode="group",
        template="plotly_dark",
        height=380,
        margin=dict(l=0, r=0, t=40, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        title=dict(
            text=f"📊 Áp lực Mua / Bán {n_sessions} phiên — {symbol}",
            x=0.5, xanchor="center",
        ),
        legend=dict(
            orientation="h", y=-0.15, x=0.5, xanchor="center",
            font=dict(size=11),
        ),
        xaxis=dict(
            type="category",
            showgrid=False,
            tickfont=dict(size=10),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="#222",
            tickfont=dict(size=9),
            side="right",
        ),
        bargap=0.25,
        bargroupgap=0.08,
    )
    return fig


# ============================================================
# UI — Micro-batch Dashboard (Whale + Pressure)
# ============================================================
def render_microbatch_charts(
    symbol: str, target_date: datetime.date
) -> None:
    try:
        df_whale = fetch_whale_data(symbol, target_date)
    except Exception as exc:
        st.error(f"Lỗi tải dữ liệu Cá mập: {exc}")
        df_whale = pd.DataFrame()

    try:
        df_pressure = fetch_pressure_data(symbol, target_date)
    except Exception as exc:
        st.error(f"Lỗi tải dữ liệu áp lực Mua/Bán: {exc}")
        df_pressure = pd.DataFrame()

    if df_whale.empty and df_pressure.empty:
        st.warning(
            f"Chưa có dữ liệu dòng tiền cho **{symbol}** "
            f"ngày **{target_date:%d/%m/%Y}**. "
            "Có thể là ngày nghỉ hoặc dữ liệu chưa được cập nhật."
        )
        return

    # --- Extract KPI values ---
    mt = df_pressure["match_type"].astype(str) if not df_pressure.empty else pd.Series(dtype=str)
    buy_vol_s = df_pressure.loc[mt == "Buy", "total_volume"] if not df_pressure.empty else pd.Series(dtype=float)
    sell_vol_s = df_pressure.loc[mt == "Sell", "total_volume"] if not df_pressure.empty else pd.Series(dtype=float)
    buy_vol = int(buy_vol_s.iloc[0]) if not buy_vol_s.empty else 0
    sell_vol = int(sell_vol_s.iloc[0]) if not sell_vol_s.empty else 0
    net_vol = buy_vol - sell_vol
    whale_total = int(df_whale["total_value"].sum()) if not df_whale.empty else 0

    # =================== TẦNG 1: KPI Cards ===================
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("🟢 Tổng Mua Chủ Động", f"{buy_vol:,}")
    k2.metric("🔴 Tổng Bán Chủ Động", f"{sell_vol:,}")
    k3.metric(
        "📐 Độ Lệch (Net Volume)",
        f"{net_vol:,}",
        delta=f"{net_vol:+,}",
        delta_color="normal",
    )
    whale_b = whale_total / 1e9
    k4.metric("🐋 Tổng Dòng Tiền Cá Mập", f"{whale_b:,.2f} B VNĐ")

    st.markdown("")

    # =================== TẦNG 2: Charts + Detail =============
    col_left, col_right = st.columns(2)

    # --- Khối trái: Donut Cá mập + Bảng chi tiết ---
    with col_left:
        if df_whale.empty:
            st.info(f"Chưa có dữ liệu Cá mập cho **{symbol}**.")
        else:
            st.plotly_chart(
                _build_whale_donut(df_whale, symbol),
                use_container_width=True,
            )
            whale_display = df_whale.copy()
            whale_display["total_value"] = whale_display["total_value"].map(
                lambda x: f"{x:,.0f}" if pd.notna(x) else "–"
            )
            whale_display["total_trades"] = whale_display["total_trades"].map(
                lambda x: f"{x:,.0f}" if pd.notna(x) else "–"
            )
            whale_display.columns = ["Phân loại", "Giá trị (VNĐ)", "Số lệnh"]
            st.dataframe(
                whale_display,
                use_container_width=True,
                hide_index=True,
            )

    # --- Khối phải: Biểu đồ xu hướng 7 phiên ---
    with col_right:
        try:
            df_pressure_7d = fetch_pressure_7d(symbol, target_date)
            if df_pressure_7d.empty:
                st.info(f"Chưa có dữ liệu áp lực Mua/Bán cho **{symbol}**.")
            else:
                st.plotly_chart(
                    _build_pressure_7d_chart(df_pressure_7d, symbol),
                    use_container_width=True,
                )
        except Exception as exc:
            st.error(f"Lỗi tải dữ liệu áp lực 7 phiên: {exc}")


# ============================================================
# UI — Nến 1 phút Real-time (Hot Path — PostgreSQL)
# ============================================================
def render_realtime_1m_chart(symbol: str) -> None:
    @st.fragment(run_every="15s")
    def _live_fragment() -> None:
        try:
            df_candles = fetch_minute_candles(symbol)
            if df_candles.empty:
                st.info(f"Chưa có dữ liệu nến phút cho **{symbol}**.")
                return

            latest_close = df_candles["close_price"].iloc[-1]
            day_high = df_candles["high_price"].max()
            day_low = df_candles["low_price"].min()
            total_volume = int(df_candles["volume"].sum())

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Close", f"{latest_close:,}")
            c2.metric("High", f"{day_high:,}")
            c3.metric("Low", f"{day_low:,}")
            c4.metric("Volume", f"{total_volume:,}")

            fig = build_minute_chart(df_candles)
            st.plotly_chart(
                fig,
                use_container_width=True,
                config={"scrollZoom": True, "displayModeBar": False},
            )
        except Exception as exc:
            st.error(f"Lỗi kết nối PostgreSQL: {exc}")

    _live_fragment()


# ============================================================
# UI — Nến 1 phút Lịch sử (Cold Path — BigQuery)
# ============================================================
def render_historical_1m_chart(
    symbol: str, selected_date: datetime.date
) -> None:
    try:
        df_1m = fetch_bq_1m_data(symbol, selected_date)
        if df_1m.empty:
            st.warning(
                f"Không có dữ liệu nến 1 phút cho **{symbol}** "
                f"ngày **{selected_date:%d/%m/%Y}**."
            )
            return

        latest_close = df_1m["close_price"].iloc[-1]
        day_high = df_1m["high_price"].max()
        day_low = df_1m["low_price"].min()
        total_volume = int(df_1m["volume"].sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Close", f"{latest_close:,}")
        c2.metric("High", f"{day_high:,}")
        c3.metric("Low", f"{day_low:,}")
        c4.metric("Volume", f"{total_volume:,}")

        fig = build_minute_chart(df_1m)
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"scrollZoom": True, "displayModeBar": False},
        )
    except Exception as exc:
        st.error(f"Lỗi kết nối BigQuery: {exc}")


# ============================================================
# MAIN APP
# ============================================================
def main() -> None:
    st.markdown(
        """<style>
        [data-testid="stMetric"] {
            background: rgba(22, 27, 34, 0.8);
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 10px 14px;
        }
        [data-testid="stTabs"] [data-baseweb="tab-list"] {
            gap: 0px;
        }
        [data-testid="stTabs"] [data-baseweb="tab"] {
            padding: 8px 20px;
        }
        </style>""",
        unsafe_allow_html=True,
    )

    st.markdown(
        "<h2 style='text-align:center; margin-bottom:0;'>"
        "DASHBOARD"
        "</h2>",
        unsafe_allow_html=True,
    )
    st.caption("Cold Path: BigQuery Marts  ·  Hot Path: Kafka → Spark → PostgreSQL")

    # ------------------------------------------------------------------
    # SIDEBAR — Symbol picker (by sector, symbol only) + Single date
    # ------------------------------------------------------------------
    with st.sidebar:
        st.header("⚙️ Bộ lọc")

        try:
            df_symbols = fetch_symbol_list()
            sectors = sorted(df_symbols["sector"].unique())
        except Exception:
            df_symbols = pd.DataFrame(columns=["symbol", "sector"])
            sectors = []

        chosen_sector = st.selectbox("Nhóm ngành", sectors) if sectors else None

        if chosen_sector and not df_symbols.empty:
            sector_df = df_symbols[
                df_symbols["sector"] == chosen_sector
            ].sort_values("symbol")
            symbol_options = sector_df["symbol"].tolist()
            symbol: str = (
                st.selectbox("Mã cổ phiếu", symbol_options)
                if symbol_options
                else ""
            )
        else:
            symbol = st.text_input("Mã cổ phiếu", value="VIC").upper().strip()

        if not symbol:
            st.warning("Vui lòng chọn mã cổ phiếu.")
            st.stop()

        st.markdown("---")

        selected_date = st.date_input(
            "Ngày xem thông số",
            value=datetime.date.today(),
        )

    # ------------------------------------------------------------------
    # TABS
    # ------------------------------------------------------------------
    tab_tech, tab_flow, tab_live = st.tabs(
        ["📈 Phân tích Kỹ thuật", "🐳 Dòng tiền (10p)", "🕯️ Nến 1 phút"]
    )

    # ====================== TAB TECH ==============================
    with tab_tech:
        try:
            df_daily = fetch_daily_data(symbol)

            if df_daily.empty:
                st.warning(f"Không có dữ liệu cho **{symbol}**.")
            else:
                col_chart, col_info = st.columns([7.5, 2.5])

                with col_chart:
                    fig = build_candlestick_chart(df_daily)
                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        config={"scrollZoom": True, "displayModeBar": False},
                    )

                with col_info:
                    panel_mode = st.radio(
                        "panel_mode",
                        ["📊 Thông tin", "📋 Sổ lệnh"],
                        horizontal=True,
                        label_visibility="collapsed",
                    )

                    if panel_mode == "📊 Thông tin":
                        dates = pd.to_datetime(df_daily["trading_date"]).dt.date
                        mask = dates == selected_date
                        if mask.any():
                            panel_row = df_daily.loc[mask].iloc[-1]
                        else:
                            before = df_daily[dates <= selected_date]
                            panel_row = (
                                before.iloc[-1]
                                if not before.empty
                                else df_daily.iloc[-1]
                            )
                        render_price_panel(panel_row)
                    else:
                        df_ticks = fetch_tick_data(symbol, selected_date)
                        render_order_book(df_ticks)

        except Exception as exc:
            st.error(f"Lỗi kết nối BigQuery: {exc}")

    # ====================== TAB FLOW ==============================
    with tab_flow:
        is_today = selected_date == datetime.date.today()

        if is_today:
            @st.fragment(run_every="600s")
            def _microbatch() -> None:
                render_microbatch_charts(symbol, selected_date)

            _microbatch()
        else:
            render_microbatch_charts(symbol, selected_date)

    # ====================== TAB LIVE (Minute candles — Routing) ======
    with tab_live:
        is_live = selected_date == datetime.date.today()

        if is_live:
            st.subheader(f"📈 {symbol} — Nến 1 phút (Real-time)")
            st.info("⚡ Đang hiển thị dữ liệu Real-time (Luồng Nóng — PostgreSQL)")
            render_realtime_1m_chart(symbol)
        else:
            st.subheader(f"📈 {symbol} — Nến 1 phút ({selected_date:%d/%m/%Y})")
            st.info(
                f"📊 Đang hiển thị dữ liệu Lịch sử ngày "
                f"**{selected_date:%d/%m/%Y}** (Luồng Lạnh — BigQuery)"
            )
            render_historical_1m_chart(symbol, selected_date)


# ============================================================
# ENTRYPOINT
# ============================================================
main()
