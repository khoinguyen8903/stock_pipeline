{{ config(
    materialized='incremental',
    partition_by={"field": "trading_date", "data_type": "date"},
    cluster_by=['company_sk', 'symbol'],
    unique_key=['symbol', 'trading_date'],
    description='Daily Indicators: Tính MA20, MA50, RSI_14. Áp dụng kỹ thuật Kéo Rộng - Ghi Hẹp để tính Window Function chuẩn xác.'
) }}

WITH base_daily AS (
    SELECT * FROM {{ ref('fact_stock_daily_base') }}
    WHERE is_trading_day = TRUE
    
    {% if is_incremental() %}
        -- KÉO RỘNG: Lấy 100 ngày lịch để đảm bảo luôn có dư 50 ngày giao dịch (phục vụ MA_50)
        AND trading_date >= (SELECT DATE_SUB(MAX(trading_date), INTERVAL 100 DAY) FROM {{ this }})
    {% endif %}
),

price_deltas AS (
    SELECT
        *,
        -- Tính chênh lệch giá so với ngày GIAO DỊCH liền trước
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

indicator_calculations AS (
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
        
        -- RSI Components (Cutler's RSI 14 ngày)
        AVG(gain) OVER(PARTITION BY symbol ORDER BY trading_day_seq ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
        AVG(loss) OVER(PARTITION BY symbol ORDER BY trading_day_seq ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss,
        
        -- Moving Averages (20 và 50 ngày)
        ROUND(AVG(close_price) OVER(PARTITION BY symbol ORDER BY trading_day_seq ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 2) AS ma_20,
        ROUND(AVG(close_price) OVER(PARTITION BY symbol ORDER BY trading_day_seq ROWS BETWEEN 49 PRECEDING AND CURRENT ROW), 2) AS ma_50

    FROM gains_losses
),

final_output AS (
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
        ma_20,
        ma_50,
        CASE 
            WHEN avg_loss = 0 THEN 100.0 
            ELSE ROUND(100.0 - (100.0 / (1.0 + (avg_gain / avg_loss))), 2)
        END AS rsi_14
    FROM indicator_calculations
)

SELECT * FROM final_output
{% if is_incremental() %}
    -- GHI HẸP: Chỉ xuất 3 ngày gần nhất để cập nhật/ghi đè vào DB.
    -- Loại bỏ 97 ngày "nháp" để không làm biến dạng dữ liệu lịch sử đã lưu.
    WHERE trading_date >= (SELECT DATE_SUB(MAX(trading_date), INTERVAL 3 DAY) FROM {{ this }})
{% endif %}