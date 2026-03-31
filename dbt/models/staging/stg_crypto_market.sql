{{ config(materialized='view') }}

SELECT
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    price_change_percentage_24h,
    market_cap_rank,
    snapshot_at,
    DATE(snapshot_at) AS snapshot_date,
    loaded_at
FROM raw.crypto_market
