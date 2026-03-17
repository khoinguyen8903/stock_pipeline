{{ 
    config(
        materialized='table', 
        partition_by={
            "field": "time",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['symbol']
    ) 
}}

WITH source_data AS (
    SELECT * FROM {{ source('bq_raw', 'stock_raw_daily') }}
),

casted_data AS (
    SELECT
        -- 1. Giữ nguyên các cột định danh
        id,
        symbol,
        
        -- 2. Ép kiểu dữ liệu (Data Type Casting)
        CAST(price AS FLOAT64) AS price,
        CAST(volume AS INT64) AS volume,
        
        -- 3. Các cột thời gian và phân loại
        time,
        match_type,
        ingested_at
    FROM source_data
    -- Bỏ qua những dòng bị lỗi hoàn toàn (nếu id hoặc symbol bị null)
    WHERE id IS NOT NULL 
      AND symbol IS NOT NULL
)

-- 4. Khử trùng lặp (Deduplication) bằng cửa sổ window function
SELECT *
FROM casted_data
-- Lệnh QUALIFY là "đặc sản" của BigQuery giúp lọc trùng cực nhanh
-- Ý nghĩa: Nhóm theo mã và id, xếp theo thời gian ingest mới nhất, và chỉ lấy dòng số 1
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol, id 
    ORDER BY ingested_at DESC
) = 1