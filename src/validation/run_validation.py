from __future__ import annotations

import sys

from src.utils.logger import get_logger
from src.validation.data_quality_checks import (
    persist_validation_results,
    results_to_dataframe,
    run_all_validations,
    summarize_results,
)

logger = get_logger(__name__, "validation.log")


def main() -> None:
    try:
        logger.info("Starting validation pipeline")

        results = run_all_validations()
        results_df = results_to_dataframe(results)
        summary = summarize_results(results)

        print("\nDATA QUALITY VALIDATION RESULTS")
        print("-" * 50)
        print(results_df.to_string(index=False))

        print("\nSUMMARY")
        print("-" * 50)
        print(f"Total checks : {summary['total_checks']}")
        print(f"Passed checks: {summary['passed_checks']}")
        print(f"Failed checks: {summary['failed_checks']}")

        persist_validation_results(results_df)

        logger.info(
            "Validation completed | total_checks=%s passed=%s failed=%s",
            summary["total_checks"],
            summary["passed_checks"],
            summary["failed_checks"],
        )

        print("\nValidation results saved to table: data_quality_results")

    except Exception as exc:
        logger.exception("Validation pipeline failed")
        print(f"Validation failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()