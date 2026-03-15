from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd
from sqlalchemy import text

# Load db_config from project root (works regardless of cwd or PYTHONPATH)
_project_root = Path(__file__).resolve().parent.parent.parent
_db_config_path = _project_root / "config" / "db_config.py"
_spec = importlib.util.spec_from_file_location("db_config", _db_config_path)
_db_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_db_config)
engine = _db_config.engine


@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    failed_rows: int
    message: str


def fetch_transactions() -> pd.DataFrame:
    query = """
        SELECT
            transaction_id,
            account_id,
            asset_symbol,
            transaction_type,
            quantity,
            price,
            transaction_date
        FROM transactions
    """
    return pd.read_sql(query, engine)


def fetch_reported_positions() -> pd.DataFrame:
    query = """
        SELECT
            account_id,
            asset_symbol,
            reported_quantity,
            valuation_date
        FROM portfolio_positions_reported
    """
    return pd.read_sql(query, engine)


def fetch_cash_balances() -> pd.DataFrame:
    query = """
        SELECT
            account_id,
            cash_balance,
            balance_date
        FROM cash_balances
    """
    return pd.read_sql(query, engine)


def check_nulls_transactions(df: pd.DataFrame) -> ValidationResult:
    required_cols = [
        "transaction_id",
        "account_id",
        "asset_symbol",
        "transaction_type",
        "quantity",
        "price",
        "transaction_date",
    ]
    failed = df[required_cols].isnull().any(axis=1).sum()

    return ValidationResult(
        check_name="transactions_required_fields_not_null",
        passed=failed == 0,
        failed_rows=int(failed),
        message="Checks for missing required fields in transactions table.",
    )


def check_transaction_type_values(df: pd.DataFrame) -> ValidationResult:
    valid_types = {"BUY", "SELL"}
    failed = (~df["transaction_type"].isin(valid_types)).sum()

    return ValidationResult(
        check_name="transactions_valid_transaction_type",
        passed=failed == 0,
        failed_rows=int(failed),
        message="Checks that transaction_type is only BUY or SELL.",
    )


def check_positive_quantity(df: pd.DataFrame) -> ValidationResult:
    failed = (df["quantity"] <= 0).sum()

    return ValidationResult(
        check_name="transactions_positive_quantity",
        passed=failed == 0,
        failed_rows=int(failed),
        message="Checks that quantity is greater than zero.",
    )


def check_positive_price(df: pd.DataFrame) -> ValidationResult:
    failed = (df["price"] <= 0).sum()

    return ValidationResult(
        check_name="transactions_positive_price",
        passed=failed == 0,
        failed_rows=int(failed),
        message="Checks that price is greater than zero.",
    )


def check_duplicate_transaction_ids(df: pd.DataFrame) -> ValidationResult:
    failed = df["transaction_id"].duplicated().sum()

    return ValidationResult(
        check_name="transactions_unique_transaction_id",
        passed=failed == 0,
        failed_rows=int(failed),
        message="Checks that transaction_id values are unique.",
    )


def check_future_transaction_dates(df: pd.DataFrame) -> ValidationResult:
    today = pd.Timestamp.today()
    tx_dates = pd.to_datetime(df["transaction_date"], errors="coerce")
    failed = (tx_dates > today).sum()

    return ValidationResult(
        check_name="transactions_no_future_dates",
        passed=failed == 0,
        failed_rows=int(failed),
        message="Checks that transaction_date is not in the future.",
    )


def check_reported_positions_non_null(df: pd.DataFrame) -> ValidationResult:
    required_cols = ["account_id", "asset_symbol", "reported_quantity", "valuation_date"]
    failed = df[required_cols].isnull().any(axis=1).sum()

    return ValidationResult(
        check_name="reported_positions_required_fields_not_null",
        passed=failed == 0,
        failed_rows=int(failed),
        message="Checks for missing required fields in reported positions.",
    )


def check_cash_balances_non_negative(df: pd.DataFrame) -> ValidationResult:
    failed = (df["cash_balance"] < 0).sum()

    return ValidationResult(
        check_name="cash_balances_non_negative",
        passed=failed == 0,
        failed_rows=int(failed),
        message="Checks that cash balances are not negative.",
    )


def run_all_validations() -> List[ValidationResult]:
    transactions_df = fetch_transactions()
    reported_positions_df = fetch_reported_positions()
    cash_balances_df = fetch_cash_balances()

    results = [
        check_nulls_transactions(transactions_df),
        check_transaction_type_values(transactions_df),
        check_positive_quantity(transactions_df),
        check_positive_price(transactions_df),
        check_duplicate_transaction_ids(transactions_df),
        check_future_transaction_dates(transactions_df),
        check_reported_positions_non_null(reported_positions_df),
        check_cash_balances_non_negative(cash_balances_df),
    ]

    return results


def results_to_dataframe(results: List[ValidationResult]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "check_name": result.check_name,
                "passed": result.passed,
                "failed_rows": result.failed_rows,
                "message": result.message,
            }
            for result in results
        ]
    )


def persist_validation_results(results_df: pd.DataFrame) -> None:
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS data_quality_results (
        run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        check_name TEXT,
        passed BOOLEAN,
        failed_rows INTEGER,
        message TEXT
    )
    """

    with engine.begin() as connection:
        connection.execute(text(create_table_sql))

    results_df.to_sql(
        "data_quality_results",
        engine,
        if_exists="append",
        index=False,
    )


def summarize_results(results: List[ValidationResult]) -> Dict[str, int]:
    total_checks = len(results)
    passed_checks = sum(result.passed for result in results)
    failed_checks = total_checks - passed_checks

    return {
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
    }