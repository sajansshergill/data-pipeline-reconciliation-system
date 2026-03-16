from __future__ import annotations

from pathlib import Path

import pandas as pd

from config.db_config import engine


REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)


def fetch_table(query: str) -> pd.DataFrame:
    return pd.read_sql(query, engine)


def export_csv(df: pd.DataFrame, file_name: str) -> Path:
    output_path = REPORTS_DIR / file_name
    df.to_csv(output_path, index=False)
    return output_path


def generate_reconciliation_mismatch_report() -> Path:
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
        WHERE status = 'MISMATCH'
        ORDER BY abs_difference DESC, account_id, asset_symbol
    """
    df = fetch_table(query)
    return export_csv(df, "reconciliation_mismatches.csv")


def generate_account_reconciliation_summary_report() -> Path:
    query = """
        SELECT
            account_id,
            assets_compared,
            matched_assets,
            mismatched_assets,
            avg_abs_difference,
            max_abs_difference
        FROM mart_reconciliation_summary
        ORDER BY mismatched_assets DESC, max_abs_difference DESC, account_id
    """
    df = fetch_table(query)
    return export_csv(df, "account_reconciliation_summary.csv")


def generate_portfolio_exposure_report() -> Path:
    query = """
        SELECT
            account_id,
            asset_symbol,
            computed_quantity,
            avg_traded_price,
            estimated_market_value
        FROM mart_portfolio_exposure
        ORDER BY estimated_market_value DESC, account_id, asset_symbol
    """
    df = fetch_table(query)
    return export_csv(df, "portfolio_exposure_report.csv")


def generate_total_account_value_report() -> Path:
    query = """
        SELECT
            account_id,
            cash_balance,
            estimated_portfolio_market_value,
            estimated_total_account_value,
            balance_date
        FROM mart_account_cash_summary
        ORDER BY estimated_total_account_value DESC, account_id
    """
    df = fetch_table(query)
    return export_csv(df, "account_total_value_report.csv")


def generate_pipeline_summary_report() -> Path:
    dq_query = """
        SELECT
            check_name,
            passed,
            failed_rows,
            message
        FROM data_quality_results
        ORDER BY run_timestamp DESC
    """
    recon_query = """
        SELECT
            status,
            COUNT(*) AS record_count
        FROM reconciliation_results
        GROUP BY status
        ORDER BY status
    """

    dq_df = fetch_table(dq_query)
    recon_df = fetch_table(recon_query)

    summary_rows = []

    summary_rows.append(
        {
            "metric": "data_quality_total_checks",
            "value": len(dq_df),
        }
    )
    summary_rows.append(
        {
            "metric": "data_quality_failed_checks",
            "value": int((~dq_df["passed"]).sum()) if not dq_df.empty else 0,
        }
    )

    for _, row in recon_df.iterrows():
        summary_rows.append(
            {
                "metric": f"reconciliation_{str(row['status']).lower()}_records",
                "value": int(row["record_count"]),
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    return export_csv(summary_df, "pipeline_summary_report.csv")


def generate_all_reports() -> list[Path]:
    report_paths = [
        generate_reconciliation_mismatch_report(),
        generate_account_reconciliation_summary_report(),
        generate_portfolio_exposure_report(),
        generate_total_account_value_report(),
        generate_pipeline_summary_report(),
    ]
    return report_paths