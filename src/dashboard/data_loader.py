from __future__ import annotations

from pathlib import Path
import importlib.util
from datetime import datetime, timezone

import pandas as pd
import numpy as np
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Load db_config from project root (works regardless of cwd/PYTHONPATH)
_project_root = Path(__file__).resolve().parent.parent.parent
_db_config_path = _project_root / "config" / "db_config.py"
_spec = importlib.util.spec_from_file_location("db_config", _db_config_path)
_db_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_db_config)
engine = _db_config.engine


RAW_DIR = _project_root / "data" / "raw"
REPORTS_DIR = _project_root / "reports"
RECON_TOLERANCE = 0.01


def _utc_now_timestamp() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc))


def _db_is_available() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def get_backend_info() -> dict[str, str]:
    """
    Returns a small dict describing the active backend for the dashboard.
    This is meant for UI messaging (e.g. "Postgres" vs "offline CSV mode").
    """
    if _db_is_available():
        return {"backend": "postgres", "details": "Connected to configured DATABASE_URL"}
    return {
        "backend": "offline",
        "details": "Postgres unavailable; using local CSVs in data/raw (and computing marts in-memory).",
    }


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required data file: {path}")
    return pd.read_csv(path)


def _load_raw_transactions() -> pd.DataFrame:
    df = _read_csv(RAW_DIR / "transactions.csv")
    df["transaction_date"] = pd.to_datetime(df.get("transaction_date"), errors="coerce")
    # Normalize some fields to match SQL transformations intent
    df["account_id"] = df.get("account_id").astype(str).str.strip()
    df["asset_symbol"] = df.get("asset_symbol").astype(str).str.strip().str.upper()
    df["transaction_type"] = df.get("transaction_type").astype(str).str.strip().str.upper()
    return df


def _load_raw_reported_positions() -> pd.DataFrame:
    df = _read_csv(RAW_DIR / "portfolio_positions_reported.csv")
    df["valuation_date"] = pd.to_datetime(df.get("valuation_date"), errors="coerce").dt.date
    df["account_id"] = df.get("account_id").astype(str).str.strip()
    df["asset_symbol"] = df.get("asset_symbol").astype(str).str.strip().str.upper()
    return df


def _load_raw_cash_balances() -> pd.DataFrame:
    df = _read_csv(RAW_DIR / "cash_balances.csv")
    df["balance_date"] = pd.to_datetime(df.get("balance_date"), errors="coerce").dt.date
    df["account_id"] = df.get("account_id").astype(str).str.strip()
    return df


def _compute_positions_from_transactions(transactions_df: pd.DataFrame) -> pd.DataFrame:
    df = transactions_df.copy()
    df["quantity"] = pd.to_numeric(df.get("quantity"), errors="coerce")
    df["signed_quantity"] = np.where(
        df["transaction_type"] == "BUY",
        df["quantity"],
        -df["quantity"],
    )
    computed = (
        df.groupby(["account_id", "asset_symbol"], as_index=False)["signed_quantity"]
        .sum()
        .rename(columns={"signed_quantity": "computed_quantity"})
    )
    computed["computed_quantity"] = pd.to_numeric(computed["computed_quantity"], errors="coerce").fillna(0.0).round(4)
    return computed


def _reconcile_positions(
    computed_df: pd.DataFrame, reported_df: pd.DataFrame, tolerance: float = RECON_TOLERANCE
) -> pd.DataFrame:
    r = reported_df.copy()
    r["reported_quantity"] = pd.to_numeric(r.get("reported_quantity"), errors="coerce")

    recon = computed_df.merge(
        r[["account_id", "asset_symbol", "reported_quantity", "valuation_date"]],
        on=["account_id", "asset_symbol"],
        how="outer",
    )
    recon["computed_quantity"] = pd.to_numeric(recon["computed_quantity"], errors="coerce").fillna(0.0)
    recon["reported_quantity"] = pd.to_numeric(recon["reported_quantity"], errors="coerce").fillna(0.0)
    recon["quantity_difference"] = (recon["computed_quantity"] - recon["reported_quantity"]).round(4)
    recon["abs_difference"] = recon["quantity_difference"].abs().round(4)
    recon["status"] = np.where(recon["abs_difference"] <= tolerance, "MATCH", "MISMATCH")

    return recon[
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


def _compute_mart_reconciliation_summary(reconciliation_df: pd.DataFrame) -> pd.DataFrame:
    g = reconciliation_df.groupby("account_id", as_index=False)
    out = g.agg(
        assets_compared=("asset_symbol", "count"),
        matched_assets=("status", lambda s: int((s == "MATCH").sum())),
        mismatched_assets=("status", lambda s: int((s == "MISMATCH").sum())),
        avg_abs_difference=("abs_difference", "mean"),
        max_abs_difference=("abs_difference", "max"),
    )
    out["avg_abs_difference"] = pd.to_numeric(out["avg_abs_difference"], errors="coerce").fillna(0.0).round(4)
    out["max_abs_difference"] = pd.to_numeric(out["max_abs_difference"], errors="coerce").fillna(0.0).round(4)
    return out


def _compute_mart_portfolio_exposure(
    computed_positions_df: pd.DataFrame, transactions_df: pd.DataFrame
) -> pd.DataFrame:
    t = transactions_df.copy()
    t["quantity"] = pd.to_numeric(t.get("quantity"), errors="coerce")
    t["price"] = pd.to_numeric(t.get("price"), errors="coerce")

    # Mimic stg_transactions filter from SQL
    t = t[
        t["transaction_id"].notna()
        & t["account_id"].notna()
        & t["asset_symbol"].notna()
        & t["transaction_type"].isin(["BUY", "SELL"])
        & (t["quantity"] > 0)
        & (t["price"] > 0)
    ].copy()

    avg_price = (
        t.groupby(["account_id", "asset_symbol"], as_index=False)["price"]
        .mean()
        .rename(columns={"price": "avg_traded_price"})
    )
    avg_price["avg_traded_price"] = pd.to_numeric(avg_price["avg_traded_price"], errors="coerce").fillna(0.0).round(4)

    out = computed_positions_df.merge(avg_price, on=["account_id", "asset_symbol"], how="left")
    out["avg_traded_price"] = pd.to_numeric(out["avg_traded_price"], errors="coerce").fillna(0.0).round(4)
    out["estimated_market_value"] = (out["computed_quantity"] * out["avg_traded_price"]).round(4)
    return out[["account_id", "asset_symbol", "computed_quantity", "avg_traded_price", "estimated_market_value"]]


def _compute_mart_account_cash_summary(
    cash_balances_df: pd.DataFrame, portfolio_exposure_df: pd.DataFrame
) -> pd.DataFrame:
    exposure_sum = (
        portfolio_exposure_df.groupby("account_id", as_index=False)["estimated_market_value"]
        .sum()
        .rename(columns={"estimated_market_value": "estimated_portfolio_value"})
    )
    out = cash_balances_df.merge(exposure_sum, on="account_id", how="left")
    out["cash_balance"] = pd.to_numeric(out.get("cash_balance"), errors="coerce").fillna(0.0).round(4)
    out["estimated_portfolio_value"] = pd.to_numeric(out.get("estimated_portfolio_value"), errors="coerce").fillna(0.0).round(4)
    out["estimated_total_account_value"] = (out["cash_balance"] + out["estimated_portfolio_value"]).round(4)
    return out[
        ["account_id", "cash_balance", "estimated_portfolio_value", "estimated_total_account_value", "balance_date"]
    ]


def _compute_data_quality_results(
    transactions_df: pd.DataFrame, reported_positions_df: pd.DataFrame, cash_balances_df: pd.DataFrame
) -> pd.DataFrame:
    results: list[dict[str, object]] = []
    run_ts = _utc_now_timestamp()

    def add(check_name: str, passed: bool, failed_rows: int, message: str) -> None:
        results.append(
            {
                "run_timestamp": run_ts,
                "check_name": check_name,
                "passed": bool(passed),
                "failed_rows": int(failed_rows),
                "message": message,
            }
        )

    tx_required = [
        "transaction_id",
        "account_id",
        "asset_symbol",
        "transaction_type",
        "quantity",
        "price",
        "transaction_date",
    ]
    tx_failed_nulls = int(transactions_df[tx_required].isnull().any(axis=1).sum()) if all(
        c in transactions_df.columns for c in tx_required
    ) else len(transactions_df)
    add(
        "transactions_required_fields_not_null",
        tx_failed_nulls == 0,
        tx_failed_nulls,
        "Checks for missing required fields in transactions dataset.",
    )

    valid_types = {"BUY", "SELL"}
    tx_failed_types = int((~transactions_df["transaction_type"].isin(valid_types)).sum()) if "transaction_type" in transactions_df.columns else len(transactions_df)
    add(
        "transactions_valid_transaction_type",
        tx_failed_types == 0,
        tx_failed_types,
        "Checks that transaction_type is only BUY or SELL.",
    )

    qty = pd.to_numeric(transactions_df.get("quantity"), errors="coerce")
    tx_failed_qty = int((qty <= 0).sum())
    add(
        "transactions_positive_quantity",
        tx_failed_qty == 0,
        tx_failed_qty,
        "Checks that quantity is greater than zero.",
    )

    price = pd.to_numeric(transactions_df.get("price"), errors="coerce")
    tx_failed_price = int((price <= 0).sum())
    add(
        "transactions_positive_price",
        tx_failed_price == 0,
        tx_failed_price,
        "Checks that price is greater than zero.",
    )

    tx_failed_dupes = int(transactions_df.get("transaction_id").duplicated().sum()) if "transaction_id" in transactions_df.columns else len(transactions_df)
    add(
        "transactions_unique_transaction_id",
        tx_failed_dupes == 0,
        tx_failed_dupes,
        "Checks that transaction_id values are unique.",
    )

    tx_dates = pd.to_datetime(transactions_df.get("transaction_date"), errors="coerce")
    tx_failed_future = int((tx_dates > pd.Timestamp.now()).sum())
    add(
        "transactions_no_future_dates",
        tx_failed_future == 0,
        tx_failed_future,
        "Checks that transaction_date is not in the future.",
    )

    rp_required = ["account_id", "asset_symbol", "reported_quantity", "valuation_date"]
    rp_failed_nulls = int(reported_positions_df[rp_required].isnull().any(axis=1).sum()) if all(
        c in reported_positions_df.columns for c in rp_required
    ) else len(reported_positions_df)
    add(
        "reported_positions_required_fields_not_null",
        rp_failed_nulls == 0,
        rp_failed_nulls,
        "Checks for missing required fields in reported positions dataset.",
    )

    cb = pd.to_numeric(cash_balances_df.get("cash_balance"), errors="coerce")
    cb_failed_negative = int((cb < 0).sum())
    add(
        "cash_balances_non_negative",
        cb_failed_negative == 0,
        cb_failed_negative,
        "Checks that cash balances are not negative.",
    )

    return pd.DataFrame(results)


def load_reconciliation_results() -> pd.DataFrame:
    query = """
        SELECT
            account_id,
            asset_symbol,
            computed_quantity,
            reported_quantity,
            quantity_difference,
            abs_difference,
            valuation_date,
            status
        FROM reconciliation_results
    """
    try:
        return pd.read_sql(query, engine)
    except (OperationalError, SQLAlchemyError):
        tx = _load_raw_transactions()
        rp = _load_raw_reported_positions()
        computed = _compute_positions_from_transactions(tx)
        return _reconcile_positions(computed, rp)


def load_reconciliation_summary() -> pd.DataFrame:
    query = """
        SELECT
            account_id,
            assets_compared,
            matched_assets,
            mismatched_assets,
            avg_abs_difference,
            max_abs_difference
        FROM mart_reconciliation_summary
    """
    try:
        return pd.read_sql(query, engine)
    except (OperationalError, SQLAlchemyError):
        recon = load_reconciliation_results()
        return _compute_mart_reconciliation_summary(recon)


def load_portfolio_exposure() -> pd.DataFrame:
    query = """
        SELECT
            account_id,
            asset_symbol,
            computed_quantity,
            avg_traded_price,
            estimated_market_value
        FROM mart_portfolio_exposure
    """
    try:
        return pd.read_sql(query, engine)
    except (OperationalError, SQLAlchemyError):
        tx = _load_raw_transactions()
        computed = _compute_positions_from_transactions(tx)
        return _compute_mart_portfolio_exposure(computed, tx)


def load_account_value_summary() -> pd.DataFrame:
    query = """
        SELECT
            account_id,
            cash_balance,
            estimated_portfolio_value,
            estimated_total_account_value,
            balance_date
        FROM mart_account_cash_summary
    """
    try:
        return pd.read_sql(query, engine)
    except (OperationalError, SQLAlchemyError):
        tx = _load_raw_transactions()
        cb = _load_raw_cash_balances()
        computed = _compute_positions_from_transactions(tx)
        exposure = _compute_mart_portfolio_exposure(computed, tx)
        return _compute_mart_account_cash_summary(cb, exposure)


def load_data_quality_results() -> pd.DataFrame:
    query = """
        SELECT
            run_timestamp,
            check_name,
            passed,
            failed_rows,
            message
        FROM data_quality_results
        ORDER BY run_timestamp DESC
    """
    try:
        return pd.read_sql(query, engine)
    except (OperationalError, SQLAlchemyError):
        tx = _load_raw_transactions()
        rp = _load_raw_reported_positions()
        cb = _load_raw_cash_balances()
        return _compute_data_quality_results(tx, rp, cb).sort_values("run_timestamp", ascending=False)