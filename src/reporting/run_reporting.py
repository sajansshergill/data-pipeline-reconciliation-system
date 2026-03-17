from __future__ import annotations

import sys

from src.reporting.generate_reports import generate_all_reports
from src.utils.logger import get_logger

logger = get_logger(__name__, "reporting.log")


def main() -> None:
    try:
        logger.info("Starting reporting pipeline")

        print("\nGENERATING REPORTS")
        print("-" * 60)

        report_paths = generate_all_reports()

        for path in report_paths:
            logger.info("Created report: %s", path)
            print(f"Created: {path}")

        logger.info("Reporting pipeline completed successfully")

        print("\nReporting complete.")
        print("Generated outputs can be found in the reports/ folder.")

    except Exception as exc:
        logger.exception("Reporting pipeline failed")
        print(f"Reporting failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()