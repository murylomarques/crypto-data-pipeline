CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.crypto_market (
    coin_id TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    current_price NUMERIC,
    market_cap NUMERIC,
    total_volume NUMERIC,
    price_change_percentage_24h NUMERIC,
    market_cap_rank INTEGER,
    snapshot_at TIMESTAMPTZ NOT NULL,
    source_file TEXT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (coin_id, snapshot_at)
);
