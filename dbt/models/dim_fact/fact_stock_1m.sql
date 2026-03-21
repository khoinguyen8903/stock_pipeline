{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['symbol', 'trading_time'],
    description='Bảng Fact lưu trữ nến 1 phút, phục vụ vẽ biểu đồ Intraday'
) }}

WITH minute_silver AS (
    SELECT * FROM {{ ref('stg_historical_1m') }}
    
    -- Logic Incremental: Quét lùi 2 ngày để vét sạch dữ liệu cập nhật muộn
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
    -- 1. Các Khóa Liên Kết (Surrogate Keys)
    c.company_sk,
    m.symbol,
    
    -- 2. Xử lý Thời gian
    m.time AS trading_time,                 -- Giữ nguyên mốc thời gian chi tiết (VD: 14:05:00)
    DATE(m.time) AS trading_date,           -- Tách riêng cột Ngày để dbt làm Partition và Join
    
    -- 3. Thông tin chu kỳ từ bảng Lịch
    cal.trading_day_seq,
    cal.is_trading_day,

    -- 4. Dữ liệu nến (OHLCV)
    m.open_price,
    m.high_price,
    m.low_price,
    m.close_price,
    m.volume,
    
    -- 5. Chỉ số phái sinh
    (m.close_price * 1000 * m.volume) AS traded_value

FROM minute_silver m

-- Kéo mã khóa Công ty
LEFT JOIN dim_comp c 
    ON m.symbol = c.symbol

-- Kéo thông tin Lịch giao dịch
LEFT JOIN dim_cal cal 
    ON DATE(m.time) = cal.trading_date

-- BỘ LỌC BẢO VỆ
-- 1. Loại bỏ dữ liệu rác nếu API vô tình trả về vào ngày cuối tuần
WHERE cal.is_trading_day = TRUE

-- 2. Chống nhân bản (Fan-out): Nếu có 2 dòng trùng mã, trùng từng phút, lấy dòng mới nhất
QUALIFY ROW_NUMBER() OVER(PARTITION BY m.symbol, m.time ORDER BY m.time DESC) = 1