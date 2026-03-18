{{ 
    config(
        materialized='incremental', 
        partition_by={
            "field": "time",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=['symbol'],
        -- Định nghĩa cột dùng để xác định dữ liệu mới khi chạy incremental
        unique_key=['symbol', 'id'] 
    ) 
}}

WITH source_data AS (
    SELECT * FROM {{ source('bq_raw', 'stock_raw_daily') }}
    
    -- LOGIC INCREMENTAL: Chỉ lấy dữ liệu mới hơn thời gian cập nhật gần nhất
    {% if is_incremental() %}
        -- Lấy lùi lại 1 ngày để đảm bảo không miss dữ liệu nếu có late-arriving data
        WHERE ingested_at >= (SELECT TIMESTAMP_SUB(MAX(ingested_at), INTERVAL 1 DAY) FROM {{ this }})
    {% endif %}
),

casted_data AS (
    SELECT
        id,
        symbol,
        -- Sửa thành NUMERIC cho dữ liệu tài chính
        CAST(price AS NUMERIC) AS price,
        CAST(volume AS INT64) AS volume,
        time,
        match_type,
        ingested_at
    FROM source_data
    WHERE id IS NOT NULL 
      AND symbol IS NOT NULL
)

SELECT *
FROM casted_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol, id 
    ORDER BY ingested_at DESC
) = 1