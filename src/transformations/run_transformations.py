from pathlib import Path
import importlib.util

from sqlalchemy import text

# Load db_config from project root (same pattern as other scripts)
_project_root = Path(__file__).resolve().parent.parent.parent
_db_config_path = _project_root / "config" / "db_config.py"
_spec = importlib.util.spec_from_file_location("db_config", _db_config_path)
_db_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_db_config)
engine = _db_config.engine


def main() -> None:
    sql_file = Path("sql/transformations.sql")

    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    sql_text = sql_file.read_text()

    with engine.begin() as connection:
        connection.execute(text(sql_text))

    print("Transformation models executed successfully.")
    print("Created tables:")
    print("- stg_transactions")
    print("- mart_portfolio_exposure")
    print("- mart_reconciliation_summary")
    print("- mart_account_cash_summary")


if __name__ == "__main__":
    main()
