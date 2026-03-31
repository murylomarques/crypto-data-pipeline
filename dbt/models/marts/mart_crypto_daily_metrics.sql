{{ config(materialized='table') }}

SELECT
    coin_id,
    symbol,
    name,
    snapshot_date,
    AVG(current_price) AS avg_price,
    MAX(current_price) AS max_price,
    MIN(current_price) AS min_price,
    SUM(total_volume) AS total_volume,
    MAX(market_cap) AS max_market_cap,
    AVG(price_change_percentage_24h) AS avg_price_change_percentage_24h
FROM {{ ref('stg_crypto_market') }}
GROUP BY 1, 2, 3, 4
