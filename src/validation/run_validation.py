import sys
from pathlib import Path

# Add project root so "src" and "config" resolve when run as script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.validation.data_quality_checks import (
    persist_validation_results,
    results_to_dataframe,
    run_all_validations,
    summarize_results,
)

def main() -> None:
    results = run_all_validations()
    results_df = results_to_dataframe(results)
    summary = summarize_results(results)
    
    print("\nDATA QUALITY VALIDATION RESULTS")
    print("-" * 50)
    print(results_df.to_string(index=False))
    
    print("\nSUMMARY")
    print("-" * 50)
    print(f"Total checks: {summary['total_checks']}")
    print(f"Passed checks: {summary['passed_checks']}")
    print(f"Failed checks: {summary['failed_checks']}")
    
    persist_validation_results(results_df)
    print("\nValidation results saved to table: data_quality_results")
    
if __name__ == "__main__":
    main()