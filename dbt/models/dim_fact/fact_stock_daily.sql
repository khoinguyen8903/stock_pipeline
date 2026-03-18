{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['symbol', 'trading_date'],
    description='Bảng Fact trung tâm lưu trữ toàn bộ lịch sử giao dịch ngày của các mã chứng khoán'
) }}

WITH daily_silver AS (
    SELECT * FROM {{ ref('stg_historical_daily') }}
    
    -- LOGIC QUÉT DỮ LIỆU TỐI ƯU
    {% if is_incremental() %}
        -- Quét lùi 2 ngày để vét sạch dữ liệu cập nhật muộn
        WHERE DATE(time) >= (SELECT DATE_SUB(MAX(trading_date), INTERVAL 2 DAY) FROM {{ this }})
    {% endif %}
),

dim_comp AS (
    -- Lấy Surrogate Key từ bảng Công ty
    SELECT company_sk, symbol FROM {{ ref('dim_company_gold') }}
),

dim_cal AS (
    -- Lấy thông tin từ bảng Lịch vừa tạo ở trên
    SELECT trading_date, trading_day_seq, is_trading_day 
    FROM {{ ref('dim_trading_calendar') }}
)

SELECT
    -- 1. Các cột Khóa (Dùng để JOIN khi lên Dashboard)
    c.company_sk,
    d.symbol,
    DATE(d.time) AS trading_date,
    cal.trading_day_seq,

    -- 2. Dữ liệu gốc (Đã được làm sạch từ Silver)
    d.open_price,
    d.high_price,
    d.low_price,
    d.close_price,
    d.volume,
    
    -- 3. Chỉ số phái sinh (Enriched)
    ROUND((d.close_price - d.open_price) / NULLIF(d.open_price, 0) * 100, 2) AS pct_change,
    (d.close_price * d.volume) AS traded_value

FROM daily_silver d

-- JOIN với Công ty
LEFT JOIN dim_comp c 
    ON d.symbol = c.symbol
    
-- JOIN với Lịch Giao Dịch
LEFT JOIN dim_cal cal 
    ON DATE(d.time) = cal.trading_date

-- BƯỚC PHÒNG THỦ: Loại bỏ những dòng dữ liệu giá (nếu có) vô tình rơi vào ngày cuối tuần
WHERE cal.is_trading_day = TRUE