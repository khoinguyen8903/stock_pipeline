{{ config(materialized='view') }}

SELECT
    w.id,
    w.time,
    w.trading_date,
    w.symbol,
    c.company_profile,
    c.sector,
    c.industry,
    w.price,
    w.volume,
    w.trade_value,
    w.whale_category,
    w.match_type

FROM {{ ref('fact_whale_transactions') }} w
LEFT JOIN {{ ref('dim_company_gold') }} c ON w.company_sk = c.company_sk