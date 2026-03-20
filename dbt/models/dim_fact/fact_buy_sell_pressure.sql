{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['id'],
    description='Tick-level data: Phân loại dòng tiền chủ động chuẩn xác dựa trên nhãn match_type của sàn. Tách biệt rõ ATO/ATC.'
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

tick_classification AS (
    SELECT
        t.*,
        DATE(t.time) AS trading_date,
        -- Chuyển đổi match_type thành direction chuẩn (dùng để vẽ chart tích lũy)
        CASE 
            WHEN t.match_type = 'Buy' THEN 1
            WHEN t.match_type = 'Sell' THEN -1
            ELSE 0 -- Gồm ATO, ATC, Unknown
        END AS direction,
        
        -- Tính signed volume CHỈ cho các lệnh khớp liên tục
        CASE 
            WHEN t.match_type = 'Buy' THEN t.volume
            WHEN t.match_type = 'Sell' THEN -t.volume
            ELSE 0 
        END AS signed_volume
    FROM tick_source t
),

with_dimensions AS (
    SELECT
        ws.id,
        ws.symbol,
        c.company_sk,
        ws.time,
        ws.trading_date,
        cal.trading_day_seq,
        cal.is_trading_day,
        
        ws.price,
        ws.volume,
        ws.match_type,
        ws.direction,
        ws.signed_volume,
        ws.ingested_at
    FROM tick_classification ws
    LEFT JOIN dim_comp c ON ws.symbol = c.symbol
    LEFT JOIN dim_cal cal ON ws.trading_date = cal.trading_date
)

SELECT *
FROM with_dimensions
WHERE is_trading_day = TRUE
-- Dedupe an toàn
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY ingested_at DESC) = 1