{{ config(
    materialized='table',
    description='Bảng chiều (Dimension) quản lý lịch giao dịch, dùng để tính toán các chu kỳ T+ hoặc đường MA'
) }}

WITH date_spine AS (
  -- Tạo ra dải ngày liên tục (Bạn có thể mở rộng năm tùy ý)
  SELECT date
  FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31')) AS date
),

base_calendar AS (
  SELECT
    date AS trading_date,
    FORMAT_DATE('%A', date) AS day_name,
    EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
    -- Trong BigQuery: Chủ nhật = 1, Thứ 7 = 7
    CASE 
      WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN TRUE 
      ELSE FALSE 
    END AS is_weekend
  FROM date_spine
),

final_calendar AS (
  SELECT
    *,
    
    CASE 
      WHEN is_weekend IS FALSE THEN TRUE 
      ELSE FALSE 
    END AS is_trading_day
  FROM base_calendar
)

SELECT
  trading_date,
  day_name,
  is_weekend,
  is_trading_day,
  
  CASE 
    WHEN is_trading_day THEN SUM(CASE WHEN is_trading_day THEN 1 ELSE 0 END) OVER (ORDER BY trading_date)
    ELSE NULL 
  END AS trading_day_seq
FROM final_calendar