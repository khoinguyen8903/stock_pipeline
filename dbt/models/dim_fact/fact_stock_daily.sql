{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['symbol', 'trading_date']
) }}

WITH base_daily AS (
    SELECT * FROM {{ ref('fact_stock_daily_base') }}
    WHERE is_trading_day = TRUE
    
    {% if is_incremental() %}
        AND trading_date >= (SELECT DATE_SUB(MAX(trading_date), INTERVAL 60 DAY) FROM {{ this }})
    {% endif %}
),

price_deltas AS (
    SELECT
        *,
        close_price - LAG(close_price) OVER(PARTITION BY symbol ORDER BY trading_day_seq) AS price_delta
    FROM base_daily
),

gains_losses AS (
    SELECT 
        *,
        CASE WHEN price_delta > 0 THEN price_delta ELSE 0 END AS gain,
        CASE WHEN price_delta < 0 THEN ABS(price_delta) ELSE 0 END AS loss
    FROM price_deltas
),

rsi_components AS (
    SELECT
        *,
        AVG(gain) OVER(PARTITION BY symbol ORDER BY trading_day_seq ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER(PARTITION BY symbol ORDER BY trading_day_seq ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
    FROM gains_losses
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
    
    ROUND(AVG(close_price) OVER(PARTITION BY symbol ORDER BY trading_day_seq ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 2) AS ma_20,
    ROUND(AVG(close_price) OVER(PARTITION BY symbol ORDER BY trading_day_seq ROWS BETWEEN 49 PRECEDING AND CURRENT ROW), 2) AS ma_50,
    
    CASE 
        WHEN avg_loss = 0 THEN 100.0 
        ELSE ROUND(100.0 - (100.0 / (1.0 + (avg_gain / avg_loss))), 2)
    END AS rsi_14

FROM rsi_components