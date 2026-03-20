{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['id'],
    description='Whale-level transactions: Lọc lệnh khớp có giá trị >= 500M VNĐ. Đã chuẩn hóa giá nhân 1000.'
) }}

WITH tick_source AS (
    SELECT * FROM {{ ref('stg_stock_ticks') }}
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
),

with_trade_value AS (
    -- Tính giá trị giao dịch (VNĐ) - LƯU Ý: Giá (price) phải nhân 1000
    SELECT
        t.*,
        (t.price * 1000 * t.volume) AS trade_value,
        DATE(t.time) AS trading_date
    FROM tick_source t
),

whale_transactions AS (
    -- Lọc lệnh >= 500M VNĐ và phân cấp Whale
    SELECT
        w.*,
        CASE
            WHEN w.trade_value >= 10000000000 THEN 'Whale Level 4 (Mega >= 10B)'
            WHEN w.trade_value >= 5000000000 THEN 'Whale Level 3 (Huge >= 5B)'
            WHEN w.trade_value >= 1000000000 THEN 'Whale Level 2 (Large >= 1B)'
            WHEN w.trade_value >= 500000000 THEN 'Whale Level 1 (Medium >= 500M)'
        END AS whale_category
    FROM with_trade_value w
    WHERE (w.price * 1000 * w.volume) >= 500000000  -- Filter chuẩn xác
),

with_dimensions AS (
    -- Join dimension tables
    SELECT
        wt.id,
        wt.symbol,
        c.company_sk,
        wt.time,
        wt.trading_date,
        cal.trading_day_seq,
        cal.is_trading_day,
        
        wt.price,
        wt.volume,
        wt.trade_value,
        wt.whale_category,
        wt.match_type,
        wt.ingested_at
    
    FROM whale_transactions wt
    LEFT JOIN dim_comp c ON wt.symbol = c.symbol
    LEFT JOIN dim_cal cal ON wt.trading_date = cal.trading_date
)

SELECT *
FROM with_dimensions
WHERE is_trading_day = TRUE
  AND whale_category IS NOT NULL
-- Dedupe an toàn
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY ingested_at DESC) = 1