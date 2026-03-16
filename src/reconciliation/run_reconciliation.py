from src.reconciliation.reconcile_portfolio import run_reconciliation_pipeline


def main() -> None:
    computed_df, reconciliation_df, summary = run_reconciliation_pipeline(tolerance=0.01)

    print("\nCOMPUTED POSITIONS SAMPLE")
    print("-" * 60)
    print(computed_df.head(10).to_string(index=False))

    print("\nRECONCILIATION RESULTS SAMPLE")
    print("-" * 60)
    print(reconciliation_df.head(20).to_string(index=False))

    mismatches = reconciliation_df[reconciliation_df["status"] == "MISMATCH"]

    print("\nRECONCILIATION SUMMARY")
    print("-" * 60)
    print(f"Total records compared : {summary.total_records_compared}")
    print(f"Matched records        : {summary.matched_records}")
    print(f"Mismatched records     : {summary.mismatched_records}")
    print(f"Match rate             : {summary.match_rate}%")

    print("\nTOP MISMATCHES")
    print("-" * 60)
    if mismatches.empty:
        print("No mismatches found.")
    else:
        print(
            mismatches.sort_values("abs_difference", ascending=False)
            .head(10)
            .to_string(index=False)
        )

    print("\nReconciliation results saved to:")
    print("- portfolio_positions_computed")
    print("- reconciliation_results")


if __name__ == "__main__":
    main()