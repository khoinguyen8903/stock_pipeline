{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['symbol', 'trading_date'],
    description='Base layer: Daily candle OHLCV + fundamental metrics. Đã chuẩn hóa giá nhân 1000.'
) }}

WITH daily_silver AS (
    SELECT * FROM {{ ref('stg_historical_daily') }}
    {% if is_incremental() %}
        WHERE DATE(time) >= (SELECT DATE_SUB(MAX(trading_date), INTERVAL 2 DAY) FROM {{ this }})
    {% endif %}
),

dim_comp AS (
    SELECT company_sk, symbol FROM {{ ref('dim_company_gold') }}
),

dim_cal AS (
    SELECT trading_date, trading_day_seq, is_trading_day 
    FROM {{ ref('dim_trading_calendar') }}
)

SELECT
    c.company_sk,
    d.symbol,
    DATE(d.time) AS trading_date, 
    cal.trading_day_seq,
    cal.is_trading_day,

    d.open_price,
    d.high_price,
    d.low_price,
    d.close_price,
    d.volume,
    
    -- Tính % thay đổi Intraday (Biến động trong phiên)
    ROUND((d.close_price - d.open_price) / NULLIF(d.open_price, 0) * 100, 2) AS pct_change,
    
    -- SỬA LỖI ĐƠN VỊ: Nhân 1000 cho giá để ra đúng VNĐ
    (d.close_price * 1000 * d.volume) AS traded_value

FROM daily_silver d

LEFT JOIN dim_comp c 
    ON d.symbol = c.symbol

LEFT JOIN dim_cal cal 
    ON DATE(d.time) = cal.trading_date

WHERE cal.is_trading_day = TRUE

-- Chốt chặn cuối: tránh nhân bản nếu có dòng cùng ngày, cùng mã
QUALIFY ROW_NUMBER() OVER(PARTITION BY d.symbol, DATE(d.time) ORDER BY d.time DESC) = 1