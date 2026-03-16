from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.db_config import engine


@dataclass
class ReconciliationSummary:
    total_records_compared: int
    matched_records: int
    mismatched_records: int
    match_rate: float


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


def compute_positions_from_transactions(transactions_df: pd.DataFrame) -> pd.DataFrame:
    df = transactions_df.copy()

    df["signed_quantity"] = np.where(
        df["transaction_type"] == "BUY",
        df["quantity"],
        -df["quantity"],
    )

    computed_positions = (
        df.groupby(["account_id", "asset_symbol"], as_index=False)["signed_quantity"]
        .sum()
        .rename(columns={"signed_quantity": "computed_quantity"})
    )

    return computed_positions


def reconcile_positions(
    computed_df: pd.DataFrame,
    reported_df: pd.DataFrame,
    tolerance: float = 0.01,
) -> pd.DataFrame:
    reported_clean = reported_df.copy()

    reconciliation_df = computed_df.merge(
        reported_clean[["account_id", "asset_symbol", "reported_quantity", "valuation_date"]],
        on=["account_id", "asset_symbol"],
        how="outer",
    )

    reconciliation_df["computed_quantity"] = reconciliation_df["computed_quantity"].fillna(0.0)
    reconciliation_df["reported_quantity"] = reconciliation_df["reported_quantity"].fillna(0.0)

    reconciliation_df["quantity_difference"] = (
        reconciliation_df["computed_quantity"] - reconciliation_df["reported_quantity"]
    ).round(4)

    reconciliation_df["status"] = np.where(
        reconciliation_df["quantity_difference"].abs() <= tolerance,
        "MATCH",
        "MISMATCH",
    )

    reconciliation_df["abs_difference"] = reconciliation_df["quantity_difference"].abs().round(4)

    reconciliation_df = reconciliation_df[
        [
            "account_id",
            "asset_symbol",
            "computed_quantity",
            "reported_quantity",
            "quantity_difference",
            "abs_difference",
            "valuation_date",
            "status",
        ]
    ].sort_values(["status", "account_id", "asset_symbol"], ascending=[True, True, True])

    return reconciliation_df


def summarize_reconciliation(reconciliation_df: pd.DataFrame) -> ReconciliationSummary:
    total_records = len(reconciliation_df)
    matched_records = int((reconciliation_df["status"] == "MATCH").sum())
    mismatched_records = int((reconciliation_df["status"] == "MISMATCH").sum())
    match_rate = round((matched_records / total_records) * 100, 2) if total_records > 0 else 0.0

    return ReconciliationSummary(
        total_records_compared=total_records,
        matched_records=matched_records,
        mismatched_records=mismatched_records,
        match_rate=match_rate,
    )


def persist_computed_positions(computed_df: pd.DataFrame) -> None:
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS portfolio_positions_computed (
        account_id TEXT NOT NULL,
        asset_symbol TEXT NOT NULL,
        computed_quantity NUMERIC(18,4) NOT NULL
    )
    """

    with engine.begin() as connection:
        connection.execute(text(create_table_sql))
        connection.execute(text("TRUNCATE TABLE portfolio_positions_computed"))

    computed_df.to_sql(
        "portfolio_positions_computed",
        engine,
        if_exists="append",
        index=False,
    )


def persist_reconciliation_results(reconciliation_df: pd.DataFrame) -> None:
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS reconciliation_results (
        run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        account_id TEXT NOT NULL,
        asset_symbol TEXT NOT NULL,
        computed_quantity NUMERIC(18,4) NOT NULL,
        reported_quantity NUMERIC(18,4) NOT NULL,
        quantity_difference NUMERIC(18,4) NOT NULL,
        abs_difference NUMERIC(18,4) NOT NULL,
        valuation_date DATE,
        status TEXT NOT NULL
    )
    """

    with engine.begin() as connection:
        connection.execute(text(create_table_sql))
        connection.execute(text("TRUNCATE TABLE reconciliation_results"))

    reconciliation_df.to_sql(
        "reconciliation_results",
        engine,
        if_exists="append",
        index=False,
    )


def run_reconciliation_pipeline(tolerance: float = 0.01) -> Tuple[pd.DataFrame, pd.DataFrame, ReconciliationSummary]:
    transactions_df = fetch_transactions()
    reported_df = fetch_reported_positions()

    computed_df = compute_positions_from_transactions(transactions_df)
    reconciliation_df = reconcile_positions(
        computed_df=computed_df,
        reported_df=reported_df,
        tolerance=tolerance,
    )
    summary = summarize_reconciliation(reconciliation_df)

    persist_computed_positions(computed_df)
    persist_reconciliation_results(reconciliation_df)

    return computed_df, reconciliation_df, summary