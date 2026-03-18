{{ config(
    materialized='incremental',
    partition_by={"field": "time", "data_type": "timestamp", "granularity": "day"},
    cluster_by=['symbol'],
    unique_key=['symbol', 'time']
) }}

WITH source_data AS (
    SELECT * FROM {{ source('bq_raw', 'bronze_historical_1m') }}
    {% if is_incremental() %}
        -- Quét lùi 2 ngày để đảm bảo vét sạch dữ liệu cập nhật muộn
        WHERE ingestion_timestamp >= (SELECT TIMESTAMP_SUB(MAX(ingestion_timestamp), INTERVAL 2 DAY) FROM {{ this }})
    {% endif %}
),

casted_data AS (
    SELECT
        time,
        symbol,
        -- Chuyển STRING thành NUMERIC và INT64
        CAST(open AS NUMERIC) AS open_price,
        CAST(high AS NUMERIC) AS high_price,
        CAST(low AS NUMERIC) AS low_price,
        CAST(close AS NUMERIC) AS close_price,
        CAST(volume AS INT64) AS volume,
        ingestion_timestamp
    FROM source_data
    WHERE symbol IS NOT NULL AND time IS NOT NULL
)

SELECT *
FROM casted_data
-- Lọc trùng lặp theo mã và thời gian, lấy dòng có ingestion_timestamp mới nhất
QUALIFY ROW_NUMBER() OVER(PARTITION BY symbol, time ORDER BY ingestion_timestamp DESC) = 1