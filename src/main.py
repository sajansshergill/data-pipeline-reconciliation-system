from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from src.utils.logger import get_logger

logger = get_logger(__name__, "pipeline.log")

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_step(step_name: str, command: list[str]) -> None:
    logger.info("Running step: %s | command=%s", step_name, " ".join(command))

    print(f"\n{'=' * 80}")
    print(f"RUNNING STEP: {step_name}")
    print(f"COMMAND: {' '.join(command)}")
    print(f"{'=' * 80}\n")

    result = subprocess.run(command, cwd=PROJECT_ROOT)

    if result.returncode != 0:
        logger.error("Step failed: %s", step_name)
        print(f"\nStep failed: {step_name}")
        sys.exit(result.returncode)

    logger.info("Completed step: %s", step_name)
    print(f"\nCompleted: {step_name}")


def main() -> None:
    try:
        logger.info("Starting full pipeline run")

        pipeline_steps = [
            (
                "Generate synthetic financial data",
                [sys.executable, "-m", "src.ingestion.generate_fake_data"],
            ),
            (
                "Initialize database tables",
                [sys.executable, "-m", "src.db.init_db"],
            ),
            (
                "Ingest raw data into database",
                [sys.executable, "-m", "src.ingestion.ingest_transactions"],
            ),
            (
                "Run data quality validations",
                [sys.executable, "-m", "src.validation.run_validation"],
            ),
            (
                "Run portfolio reconciliation engine",
                [sys.executable, "-m", "src.reconciliation.run_reconciliation"],
            ),
            (
                "Run SQL transformation models",
                [sys.executable, "-m", "src.transformations.run_transformations"],
            ),
            (
                "Generate reporting outputs",
                [sys.executable, "-m", "src.reporting.run_reporting"],
            ),
        ]

        print("\nStarting Financial Portfolio Data Pipeline & Reconciliation System")

        for step_name, command in pipeline_steps:
            run_step(step_name, command)

        logger.info("Pipeline completed successfully")

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

    except Exception as exc:
        logger.exception("Full pipeline run failed")
        print(f"Pipeline failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()