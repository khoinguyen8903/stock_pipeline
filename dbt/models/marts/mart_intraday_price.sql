{{ config(materialized='view') }}

SELECT
    m.trading_time,
    m.trading_date,
    m.symbol,
    c.company_profile,
    c.sector,
    c.industry,
    m.open_price,
    m.high_price,
    m.low_price,
    m.close_price,
    m.volume,
    m.traded_value

FROM {{ ref('fact_stock_1m') }} m
LEFT JOIN {{ ref('dim_company_gold') }} c ON m.company_sk = c.company_sk