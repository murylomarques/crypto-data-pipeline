-- 1) Evolucao de preco medio diario por ativo
SELECT
    snapshot_date,
    coin_id,
    symbol,
    avg_price
FROM analytics.mart_crypto_daily_metrics
ORDER BY snapshot_date DESC, avg_price DESC;

-- 2) Top 10 ativos por market cap maximo no dia mais recente
WITH latest_day AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM analytics.mart_crypto_daily_metrics
)
SELECT
    m.snapshot_date,
    m.coin_id,
    m.symbol,
    m.max_market_cap,
    m.total_volume
FROM analytics.mart_crypto_daily_metrics m
JOIN latest_day d ON m.snapshot_date = d.snapshot_date
ORDER BY m.max_market_cap DESC
LIMIT 10;
