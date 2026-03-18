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
    
    ROUND((d.close_price - d.open_price) / NULLIF(d.open_price, 0) * 100, 2) AS pct_change,
    (d.close_price * d.volume) AS traded_value

FROM daily_silver d

LEFT JOIN dim_comp c 
    ON d.symbol = c.symbol

LEFT JOIN dim_cal cal 
    ON DATE(d.time) = cal.trading_date

WHERE cal.is_trading_day = TRUE

-- [MỚI] CHỐT CHẶN CUỐI CÙNG: Ép buộc tính Unique
-- Nếu có nhiều dòng cùng ngày, cùng mã, chỉ lấy dòng có timestamp mới nhất
QUALIFY ROW_NUMBER() OVER(PARTITION BY d.symbol, DATE(d.time) ORDER BY d.time DESC) = 1