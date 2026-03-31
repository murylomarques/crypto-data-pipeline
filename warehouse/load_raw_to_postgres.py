import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

LOGGER = logging.getLogger(__name__)


def _find_latest_raw_file(data_lake_root: str) -> Path:
    root = Path(data_lake_root) / "raw" / "coingecko" / "market_snapshot"
    candidates = sorted(root.glob("ingestion_date=*/ingestion_hour=*/*.json"))
    if not candidates:
        raise FileNotFoundError(f"No raw snapshot found under: {root}")
    return candidates[-1]


def _read_snapshot(path: Path) -> tuple[list[dict[str, Any]], str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "data" not in payload:
        raise ValueError(f"Invalid raw file format: {path}")
    records = payload["data"]
    snapshot_at = payload.get("metadata", {}).get("ingested_at_utc")
    if not isinstance(records, list) or not snapshot_at:
        raise ValueError(f"Missing required fields in raw file: {path}")
    return records, snapshot_at


def _db_url_from_env() -> str:
    load_dotenv()
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "crypto_dw")
    user = os.getenv("POSTGRES_USER", "crypto_user")
    password = os.getenv("POSTGRES_PASSWORD", "crypto_pass")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def load_raw_file_to_postgres(raw_file: Path, db_url: str) -> int:
    records, snapshot_at = _read_snapshot(raw_file)
    engine = create_engine(db_url)

    insert_sql = text(
        """
        INSERT INTO raw.crypto_market (
            coin_id,
            symbol,
            name,
            current_price,
            market_cap,
            total_volume,
            price_change_percentage_24h,
            market_cap_rank,
            snapshot_at,
            source_file
        ) VALUES (
            :coin_id,
            :symbol,
            :name,
            :current_price,
            :market_cap,
            :total_volume,
            :price_change_percentage_24h,
            :market_cap_rank,
            :snapshot_at,
            :source_file
        )
        ON CONFLICT (coin_id, snapshot_at) DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            current_price = EXCLUDED.current_price,
            market_cap = EXCLUDED.market_cap,
            total_volume = EXCLUDED.total_volume,
            price_change_percentage_24h = EXCLUDED.price_change_percentage_24h,
            market_cap_rank = EXCLUDED.market_cap_rank,
            source_file = EXCLUDED.source_file,
            loaded_at = NOW()
        """
    )

    rows = []
    for record in records:
        rows.append(
            {
                "coin_id": record.get("id"),
                "symbol": record.get("symbol"),
                "name": record.get("name"),
                "current_price": record.get("current_price"),
                "market_cap": record.get("market_cap"),
                "total_volume": record.get("total_volume"),
                "price_change_percentage_24h": record.get("price_change_percentage_24h"),
                "market_cap_rank": record.get("market_cap_rank"),
                "snapshot_at": snapshot_at,
                "source_file": str(raw_file),
            }
        )

    with engine.begin() as connection:
        connection.execute(insert_sql, rows)

    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load CoinGecko raw snapshot into PostgreSQL.")
    parser.add_argument("--raw-file", type=str, default=None, help="Path to a raw snapshot file.")
    parser.add_argument("--data-lake-root", type=str, default=None, help="Override data lake root.")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    args = parse_args()

    load_dotenv()
    data_lake_root = args.data_lake_root or os.getenv("DATA_LAKE_ROOT", "./data_lake")
    raw_file = Path(args.raw_file) if args.raw_file else _find_latest_raw_file(data_lake_root)

    LOGGER.info("Loading raw file to PostgreSQL: %s", raw_file)
    count = load_raw_file_to_postgres(raw_file=raw_file, db_url=_db_url_from_env())
    LOGGER.info("Loaded %s records into raw.crypto_market", count)


if __name__ == "__main__":
    main()
