{{ config(materialized='view') }}

SELECT
    p.id,
    p.time,
    p.trading_date,
    p.symbol,
    c.company_profile,
    c.sector,
    c.industry,
    p.price,
    p.volume,
    p.match_type,
    p.direction,
    p.signed_volume

FROM {{ ref('fact_buy_sell_pressure') }} p
LEFT JOIN {{ ref('dim_company_gold') }} c ON p.company_sk = c.company_sk