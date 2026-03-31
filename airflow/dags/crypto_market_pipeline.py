from __future__ import annotations

from datetime import datetime, timedelta
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator


def _run_script(command: list[str]) -> None:
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed: {' '.join(command)}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


def run_ingestion() -> None:
    _run_script(["python", "/opt/project/ingestion/coingecko_ingestion.py"])


def run_load_postgres() -> None:
    _run_script(["python", "/opt/project/warehouse/load_raw_to_postgres.py"])


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_market_pipeline",
    description="Ingest CoinGecko market data and load into PostgreSQL raw layer",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["crypto", "coingecko", "raw"],
) as dag:
    ingest_market_snapshot = PythonOperator(
        task_id="ingest_market_snapshot",
        python_callable=run_ingestion,
    )

    load_raw_to_postgres = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=run_load_postgres,
    )

    ingest_market_snapshot >> load_raw_to_postgres
