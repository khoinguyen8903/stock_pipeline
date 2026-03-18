{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['symbol', 'trading_date']
) }}

WITH daily_silver AS (
    SELECT * FROM {{ ref('stg_historical_daily') }}
    {% if is_incremental() %}
        WHERE DATE(time) >= (SELECT DATE_SUB(MAX(trading_date), INTERVAL 2 DAY) FROM {{ this }})
    {% endif %}
),

dim_comp AS (
    SELECT company_sk, symbol FROM {{ ref('dim_company_gold') }}
)

SELECT
    c.company_sk,
    d.symbol,
    DATE(d.time) AS trading_date, -- Ép Timestamp thành Date cho dễ nhóm
    d.open_price,
    d.high_price,
    d.low_price,
    d.close_price,
    d.volume,
    
    -- Các chỉ số phái sinh (Enriched Metrics)
    ROUND((d.close_price - d.open_price) / NULLIF(d.open_price, 0) * 100, 2) AS pct_change,
    (d.close_price * d.volume) AS traded_value

FROM daily_silver d
LEFT JOIN dim_comp c ON d.symbol = c.symbol