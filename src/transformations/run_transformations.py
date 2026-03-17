from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

from config.db_config import engine
from src.utils.logger import get_logger

logger = get_logger(__name__, "transformations.log")


def main() -> None:
    try:
        logger.info("Starting transformation pipeline")

        sql_file = Path("sql/transformations.sql")

        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        sql_text = sql_file.read_text()

        with engine.begin() as connection:
            if engine.dialect.name == "sqlite":
                raw = connection.connection  # DBAPI connection
                raw.executescript(sql_text)
            else:
                statements = [s.strip() for s in sql_text.split(";") if s.strip()]
                for stmt in statements:
                    connection.execute(text(stmt))

        logger.info("Transformation models executed successfully")

        print("Transformation models executed successfully.")
        print("Created tables:")
        print("- stg_transactions")
        print("- mart_portfolio_exposure")
        print("- mart_reconciliation_summary")
        print("- mart_account_cash_summary")

    except Exception as exc:
        logger.exception("Transformation pipeline failed")
        print(f"Transformation failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()