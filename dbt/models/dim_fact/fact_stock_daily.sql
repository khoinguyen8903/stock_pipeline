{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['symbol', 'trading_date'],
    description='Final layer: OHLCV + Technical Indicators (MA20, MA50, RSI 14). Backfill 60 ngày để tính window function chính xác.'
) }}

WITH base_daily AS (
    SELECT * FROM {{ ref('fact_stock_daily_base') }}
    {% if is_incremental() %}
        -- Backfill 60 ngày để tính đủ lịch sử cho MA50 và RSI14
        WHERE trading_date >= (SELECT DATE_SUB(MAX(trading_date), INTERVAL 60 DAY) FROM {{ this }})
    {% endif %}
),

price_deltas AS (
    -- Tính toán delta giá (change) và category (gain/loss) cho RSI
    SELECT
        *,
        close_price - LAG(close_price) OVER(PARTITION BY symbol ORDER BY trading_date) AS price_delta,
        CASE 
            WHEN close_price - LAG(close_price) OVER(PARTITION BY symbol ORDER BY trading_date) > 0 
            THEN close_price - LAG(close_price) OVER(PARTITION BY symbol ORDER BY trading_date)
            ELSE 0 
        END AS gain,
        CASE 
            WHEN close_price - LAG(close_price) OVER(PARTITION BY symbol ORDER BY trading_date) < 0 
            THEN ABS(close_price - LAG(close_price) OVER(PARTITION BY symbol ORDER BY trading_date))
            ELSE 0 
        END AS loss
    FROM base_daily
),

rsi_components AS (
    -- Tính Average Gain/Loss cho RSI14 (14 ngày = trading days)
    SELECT
        *,
        -- Lấy average gain/loss của 14 ngày gần nhất (có thể điều chỉnh kích thước window)
        AVG(gain) OVER(PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER(PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM price_deltas
),

final_metrics AS (
    -- Tính toán MA20, MA50, RSI14
    SELECT
        company_sk,
        symbol,
        trading_date,
        trading_day_seq,
        is_trading_day,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        pct_change,
        traded_value,
        
        -- MA 20 (trung bình 20 ngày gần nhất)
        ROUND(
            AVG(close_price) OVER(PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
            2
        ) AS ma_20,
        
        -- MA 50 (trung bình 50 ngày gần nhất)
        ROUND(
            AVG(close_price) OVER(PARTITION BY symbol ORDER BY trading_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW),
            2
        ) AS ma_50,
        
        -- RSI 14
        ROUND(
            100 - (100 / (1 + NULLIF(avg_gain / NULLIF(avg_loss, 0), 0))),
            2
        ) AS rsi_14
        
    FROM rsi_components
)

SELECT
    company_sk,
    symbol,
    trading_date,
    trading_day_seq,
    is_trading_day,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    pct_change,
    traded_value,
    ma_20,
    ma_50,
    rsi_14

FROM final_metrics

-- Giữ lại filter is_trading_day nếu cần
WHERE is_trading_day = TRUE

-- Tránh trả về dòng có MA/RSI NULL ở đầu (vì cần 20/50/14 ngày dữ liệu)
QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol ORDER BY trading_date) != 1 OR ma_20 IS NOT NULL