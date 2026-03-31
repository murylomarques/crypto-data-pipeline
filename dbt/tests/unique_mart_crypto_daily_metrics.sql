SELECT
    coin_id,
    snapshot_date,
    COUNT(*) AS row_count
FROM {{ ref('mart_crypto_daily_metrics') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
