from __future__ import annotations

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_step(step_name: str, command: list[str]) -> None:
    print(f"\n{'=' * 80}")
    print(f"RUNNING STEP: {step_name}")
    print(f"COMMAND: {' '.join(command)}")
    print(f"{'=' * 80}\n")

    result = subprocess.run(command, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        print(f"\nStep failed: {step_name}")
        sys.exit(result.returncode)

    print(f"\nCompleted: {step_name}")


def main() -> None:
    pipeline_steps = [
        (
            "Generate synthetic financial data",
            [sys.executable, "src/ingestion/generate_fake_data.py"],
        ),
        (
            "Ingest raw data into PostgreSQL",
            [sys.executable, "src/ingestion/ingest_transactions.py"],
        ),
        (
            "Run data quality validations",
            [sys.executable, "src/validation/run_validation.py"],
        ),
        (
            "Run portfolio reconciliation engine",
            [sys.executable, "src/reconciliation/run_reconciliation.py"],
        ),
        (
            "Run SQL transformation models",
            [sys.executable, "src/transformations/run_transformations.py"],
        ),
        (
            "Generate reporting outputs",
            [sys.executable, "src/reporting/run_reporting.py"],
        ),
    ]

    print("\nStarting Financial Portfolio Data Pipeline & Reconciliation System")

    for step_name, command in pipeline_steps:
        run_step(step_name, command)

    print(f"\n{'=' * 80}")
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("Created outputs include:")
    print("- transactions")
    print("- portfolio_positions_reported")
    print("- cash_balances")
    print("- data_quality_results")
    print("- portfolio_positions_computed")
    print("- reconciliation_results")
    print("- stg_transactions")
    print("- mart_portfolio_exposure")
    print("- mart_reconciliation_summary")
    print("- mart_account_cash_summary")
    print("- reports/reconciliation_mismatches.csv")
    print("- reports/account_reconciliation_summary.csv")
    print("- reports/portfolio_exposure_report.csv")
    print("- reports/account_total_value_report.csv")
    print("- reports/pipeline_summary_report.csv")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    main()