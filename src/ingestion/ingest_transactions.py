from __future__ import annotations

import os
import sys

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from config.db_config import engine
from src.utils.logger import get_logger

logger = get_logger(__name__, "ingestion.log")

RAW_DIR = "data/raw"


def load_csv(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    logger.info("Loading CSV file: %s", file_path)
    return pd.read_csv(file_path)


def ingest_table(df: pd.DataFrame, table_name: str) -> None:
    try:
        with engine.begin() as conn:
            if engine.dialect.name == "sqlite":
                conn.execute(text(f"DELETE FROM {table_name}"))
            else:
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))

        df.to_sql(table_name, engine, if_exists="append", index=False)
        logger.info("Loaded %s rows into '%s'", len(df), table_name)
    except SQLAlchemyError as exc:
        logger.exception("Failed loading table '%s'", table_name)
        raise RuntimeError(f"Failed loading table '{table_name}'") from exc


def main() -> None:
    try:
        logger.info("Starting ingestion pipeline")

        transactions_file = os.path.join(RAW_DIR, "transactions.csv")
        positions_file = os.path.join(RAW_DIR, "portfolio_positions_reported.csv")
        cash_file = os.path.join(RAW_DIR, "cash_balances.csv")

        transactions_df = load_csv(transactions_file)
        positions_df = load_csv(positions_file)
        cash_df = load_csv(cash_file)

        transactions_df["transaction_date"] = pd.to_datetime(
            transactions_df["transaction_date"], errors="coerce"
        )
        positions_df["valuation_date"] = pd.to_datetime(
            positions_df["valuation_date"], errors="coerce"
        ).dt.date
        cash_df["balance_date"] = pd.to_datetime(
            cash_df["balance_date"], errors="coerce"
        ).dt.date

        ingest_table(transactions_df, "transactions")
        ingest_table(positions_df, "portfolio_positions_reported")
        ingest_table(cash_df, "cash_balances")

        logger.info("Ingestion pipeline completed successfully")

    except Exception as exc:
        logger.exception("Ingestion pipeline failed")
        print(f"Ingestion failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()