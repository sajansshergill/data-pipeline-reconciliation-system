import importlib.util
import os
from pathlib import Path

# Load db_config from project root (works regardless of cwd or PYTHONPATH)
_project_root = Path(__file__).resolve().parent.parent.parent
_db_config_path = _project_root / "config" / "db_config.py"
_spec = importlib.util.spec_from_file_location("db_config", _db_config_path)
_db_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_db_config)
engine = _db_config.engine

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

RAW_DIR = "data/raw"


def load_csv(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    return pd.read_csv(file_path)


def ingest_table(df: pd.DataFrame, table_name: str) -> None:
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"Loaded {len(df)} rows into '{table_name}'")
    except SQLAlchemyError as exc:
        print(f"Failed loading table '{table_name}': {exc}")
        raise


def main() -> None:
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

    print("Ingestion complete.")


if __name__ == "__main__":
    main()