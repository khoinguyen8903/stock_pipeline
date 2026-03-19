{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['id'],
    description='Tick-level data: Phân tích lực mua/bán dựa trên Tick Test (so sánh giá hiện tại vs giá trước). Tính direction (+1=buy push, -1=sell push, 0=neutral) và signed_volume.'
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

with_lagged_price AS (
    -- Lấy giá tick trước cùng symbol, đảm bảo không nhảy qua ngày
    SELECT
        t.*,
        LAG(price) OVER(PARTITION BY t.symbol, DATE(t.time) ORDER BY t.time) AS prev_price,
        LAG(volume) OVER(PARTITION BY t.symbol, DATE(t.time) ORDER BY t.time) AS prev_volume,
        ROW_NUMBER() OVER(PARTITION BY t.symbol, DATE(t.time) ORDER BY t.time) AS tick_seq_per_day
    FROM tick_source t
),

tick_direction AS (
    -- Tính direction dựa trên so sánh giá
    SELECT
        *,
        CASE 
            WHEN price > prev_price THEN 1      -- Giá tăng = buy push
            WHEN price < prev_price THEN -1     -- Giá giảm = sell push
            ELSE 0                              -- Giá bằng = neutral
        END AS direction,
        price - prev_price AS price_delta
    FROM with_lagged_price
    WHERE tick_seq_per_day > 1  -- Bỏ tick đầu tiên của ngày (không có prev_price)
),

with_signed_volume AS (
    -- Tính signed volume (volume * direction)
    SELECT
        w.*,
        (w.volume * w.direction) AS signed_volume,
        DATE(w.time) AS trading_date
    FROM tick_direction w
),

with_dimensions AS (
    -- Join dimension tables
    SELECT
        ws.id,
        ws.symbol,
        c.company_sk,
        ws.time,
        ws.trading_date,
        cal.trading_day_seq,
        cal.is_trading_day,
        
        ws.price,
        ws.prev_price,
        ws.price_delta,
        ws.volume,
        ws.prev_volume,
        ws.match_type,
        ws.direction,
        ws.signed_volume,
        ws.ingested_at
    
    FROM with_signed_volume ws
    LEFT JOIN dim_comp c ON ws.symbol = c.symbol
    LEFT JOIN dim_cal cal ON ws.trading_date = cal.trading_date
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
    prev_price,
    price_delta,
    volume,
    prev_volume,
    match_type,
    direction,
    signed_volume,
    ingested_at

FROM with_dimensions

-- Lọc chỉ ngày giao dịch
WHERE is_trading_day = TRUE

-- Dedupe: nếu có tick trùng, lấy ingested_at mới nhất
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY ingested_at DESC) = 1
