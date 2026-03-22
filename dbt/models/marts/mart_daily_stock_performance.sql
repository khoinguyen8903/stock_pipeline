{{ config(materialized='view') }}

SELECT
    f.trading_date,
    f.symbol,
    c.company_profile,
    c.sector,
    c.industry,
    f.open_price,
    f.high_price,
    f.low_price,
    f.close_price,
    f.volume,
    f.pct_change,
    f.traded_value,
    f.ma_20,
    f.ma_50,
    f.rsi_14

FROM {{ ref('fact_stock_daily') }} f
LEFT JOIN {{ ref('dim_company_gold') }} c ON f.company_sk = c.company_sk