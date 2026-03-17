from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from config.db_config import DATABASE_URL
from src.dashboard.data_loader import (
    get_backend_info,
    load_account_value_summary,
    load_data_quality_results,
    load_portfolio_exposure,
    load_reconciliation_results,
)


st.set_page_config(page_title="System status", layout="wide")


def _file_info(path: Path) -> dict[str, str]:
    if not path.exists():
        return {"path": str(path), "status": "missing", "size_kb": "0"}
    return {"path": str(path), "status": "ok", "size_kb": f"{path.stat().st_size / 1024:.1f}"}


def main() -> None:
    st.title("System Status")
    backend = get_backend_info()

    st.subheader("Backend")
    st.write(
        {
            "backend": backend["backend"],
            "details": backend["details"],
            "database_url": DATABASE_URL,
        }
    )

    st.subheader("Data health (quick)")
    recon = load_reconciliation_results()
    exposure = load_portfolio_exposure()
    values = load_account_value_summary()
    dq = load_data_quality_results()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("reconciliation_results", f"{len(recon):,}")
    c2.metric("mart_portfolio_exposure", f"{len(exposure):,}")
    c3.metric("mart_account_cash_summary", f"{len(values):,}")
    c4.metric("data_quality_results", f"{len(dq):,}")

    st.subheader("Artifacts")
    project_root = Path(__file__).resolve().parent.parent
    artifacts = [
        _file_info(project_root / "data" / "pipeline.db"),
        _file_info(project_root / "reports" / "pipeline_summary_report.csv"),
        _file_info(project_root / "reports" / "reconciliation_mismatches.csv"),
    ]
    st.dataframe(pd.DataFrame(artifacts), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()

