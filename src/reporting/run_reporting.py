import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.reporting.generate_reports import generate_all_reports


def main() -> None:
    print("\nGENERATING REPORTS")
    print("-" * 60)

    report_paths = generate_all_reports()

    for path in report_paths:
        print(f"Created: {path}")

    print("\nReporting complete.")
    print("Generated outputs can be found in the reports/ folder.")


if __name__ == "__main__":
    main()