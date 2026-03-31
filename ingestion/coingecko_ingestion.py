import argparse
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


LOGGER = logging.getLogger(__name__)


@dataclass
class CoinGeckoConfig:
    base_url: str
    vs_currency: str
    per_page: int
    page: int
    timeout_seconds: int
    data_lake_root: str


def load_config() -> CoinGeckoConfig:
    load_dotenv()
    return CoinGeckoConfig(
        base_url=os.getenv("COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"),
        vs_currency=os.getenv("COINGECKO_VS_CURRENCY", "usd"),
        per_page=int(os.getenv("COINGECKO_PER_PAGE", "50")),
        page=int(os.getenv("COINGECKO_PAGE", "1")),
        timeout_seconds=int(os.getenv("COINGECKO_TIMEOUT_SECONDS", "30")),
        data_lake_root=os.getenv("DATA_LAKE_ROOT", "./data_lake"),
    )


def fetch_market_snapshot(config: CoinGeckoConfig) -> list[dict[str, Any]]:
    endpoint = f"{config.base_url}/coins/markets"
    params = {
        "vs_currency": config.vs_currency,
        "order": "market_cap_desc",
        "per_page": config.per_page,
        "page": config.page,
        "sparkline": "false",
    }

    response = requests.get(endpoint, params=params, timeout=config.timeout_seconds)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError("CoinGecko response is not a list.")
    return data


def persist_raw_snapshot(
    rows: list[dict[str, Any]],
    data_lake_root: str,
    source: str = "coingecko",
    dataset: str = "market_snapshot",
) -> Path:
    now = datetime.now(timezone.utc)
    date_partition = f"ingestion_date={now.strftime('%Y-%m-%d')}"
    hour_partition = f"ingestion_hour={now.strftime('%H')}"
    target_dir = (
        Path(data_lake_root).resolve()
        / "raw"
        / source
        / dataset
        / date_partition
        / hour_partition
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"{dataset}_{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    target_file = target_dir / file_name

    payload = {
        "metadata": {
            "source": source,
            "dataset": dataset,
            "ingested_at_utc": now.isoformat(),
            "record_count": len(rows),
        },
        "data": rows,
    }
    target_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch crypto market data from CoinGecko.")
    parser.add_argument("--page", type=int, default=None, help="Pagination page number.")
    parser.add_argument("--per-page", type=int, default=None, help="Number of assets per page.")
    parser.add_argument(
        "--output-root",
        type=str,
        default=None,
        help="Override local data lake root path.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    args = parse_args()
    config = load_config()

    if args.page is not None:
        config.page = args.page
    if args.per_page is not None:
        config.per_page = args.per_page
    if args.output_root is not None:
        config.data_lake_root = args.output_root

    LOGGER.info(
        "Fetching CoinGecko market snapshot (currency=%s, page=%s, per_page=%s).",
        config.vs_currency,
        config.page,
        config.per_page,
    )

    try:
        data = fetch_market_snapshot(config)
    except requests.RequestException as exc:
        LOGGER.error("Request to CoinGecko failed: %s", exc)
        raise SystemExit(1) from exc
    except ValueError as exc:
        LOGGER.error("Unexpected response format: %s", exc)
        raise SystemExit(1) from exc

    raw_path = persist_raw_snapshot(data, data_lake_root=config.data_lake_root)
    LOGGER.info("Fetched %s records from CoinGecko.", len(data))
    LOGGER.info("Raw snapshot persisted at: %s", raw_path)
    print(
        json.dumps(
            {
                "record_count": len(data),
                "raw_file": str(raw_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
