{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['id'],
    description='Whale-level transactions: Lọc lệnh khớp có giá trị >= 500M VNĐ, phân cấp thành Whale Level 1/2/3 dựa trên giá trị giao dịch.'
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
    -- Tính giá trị giao dịch (VNĐ)
    SELECT
        t.*,
        (t.price * t.volume) AS trade_value,
        DATE(t.time) AS trading_date
    FROM tick_source t
),

whale_transactions AS (
    -- Lọc lệnh >= 500M VNĐ và phân cấp Whale
    SELECT
        w.*,
        CASE
            WHEN w.trade_value >= 5000000000 THEN 'Whale Level 3'    -- >= 5B
            WHEN w.trade_value >= 1000000000 THEN 'Whale Level 2'    -- >= 1B
            WHEN w.trade_value >= 500000000 THEN 'Whale Level 1'     -- >= 500M
            ELSE NULL
        END AS whale_level,
        -- Phân loại chi tiết theo tầng giá trị
        CASE
            WHEN w.trade_value >= 10000000000 THEN 'Mega (>= 10B)'
            WHEN w.trade_value >= 5000000000 THEN 'Huge (5B - 10B)'
            WHEN w.trade_value >= 2000000000 THEN 'Very Large (2B - 5B)'
            WHEN w.trade_value >= 1000000000 THEN 'Large (1B - 2B)'
            WHEN w.trade_value >= 500000000 THEN 'Medium-Large (500M - 1B)'
        END AS whale_category
    FROM with_trade_value w
    WHERE (w.price * w.volume) >= 500000000  -- Filter: chỉ lấy lệnh >= 500M
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
        wt.whale_level,
        wt.whale_category,
        wt.match_type,
        wt.ingested_at
    
    FROM whale_transactions wt
    LEFT JOIN dim_comp c ON wt.symbol = c.symbol
    LEFT JOIN dim_cal cal ON wt.trading_date = cal.trading_date
)

SELECT
    id,
    symbol,
    company_sk,
    time,
    trading_date,
    trading_day_seq,
    is_trading_day,
    price,
    volume,
    trade_value,
    whale_level,
    whale_category,
    match_type,
    ingested_at

FROM with_dimensions

-- Lọc chỉ ngày giao dịch
WHERE is_trading_day = TRUE

-- Đảm bảo whale_level không NULL (safety check - vì đã filter trong where ở trên nhưng thêm an toàn)
  AND whale_level IS NOT NULL

-- Dedupe: nếu có tick trùng, lấy ingested_at mới nhất
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY ingested_at DESC) = 1

