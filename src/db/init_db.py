from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import text

from config.db_config import engine
from src.utils.logger import get_logger

logger = get_logger(__name__, "db_init.log")


def main() -> None:
    try:
        sql_file = Path("sql/creates_tables.sql")
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        sql_text = sql_file.read_text()
        with engine.begin() as connection:
            # `sql/creates_tables.sql` contains multiple statements.
            # SQLite requires executescript(); other DBs can run statement-by-statement.
            if engine.dialect.name == "sqlite":
                raw = connection.connection  # DBAPI connection
                raw.executescript(sql_text)
            else:
                statements = [s.strip() for s in sql_text.split(";") if s.strip()]
                for stmt in statements:
                    connection.execute(text(stmt))

        logger.info("Database tables created successfully")
        print("Database tables created successfully.")

    except Exception as exc:
        logger.exception("Database init failed")
        print(f"Database init failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()

