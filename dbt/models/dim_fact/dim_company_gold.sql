{{ config(
    materialized='table',
    cluster_by=['symbol']
) }}

SELECT
    -- Tạo Surrogate Key từ mã chứng khoán
    CAST(FARM_FINGERPRINT(symbol) AS INT64) AS company_sk,
    symbol,
    company_id,
    company_profile,
    sector,
    industry,
    charter_capital,
    issue_share
FROM {{ ref('stg_company') }}