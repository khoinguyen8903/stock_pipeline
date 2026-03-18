{{ config(materialized='view') }}

SELECT
    TRIM(UPPER(symbol)) AS symbol,
    id AS company_id,
    company_profile,
    history,
    icb_name2 AS sector,       -- Ngành cấp 1
    icb_name3 AS industry,     -- Ngành cấp 2
    icb_name4 AS sub_industry, -- Ngành chi tiết
    CAST(charter_capital AS NUMERIC) AS charter_capital,
    CAST(issue_share AS NUMERIC) AS issue_share
FROM {{ source('bq_raw', 'dim_company') }}
WHERE symbol IS NOT NULL