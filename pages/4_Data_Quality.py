from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.data_loader import get_backend_info, load_data_quality_results
from src.dashboard.ui_utils import download_csv_button


st.set_page_config(page_title="Data quality", layout="wide")


@st.cache_data
def get_dq() -> pd.DataFrame:
    return load_data_quality_results()


def main() -> None:
    st.title("Data Quality")
    backend = get_backend_info()
    st.caption(f"Backend: {backend['backend']} • {backend['details']}")

    df = get_dq()
    if df.empty:
        st.info("No data quality results available.")
        return

    st.sidebar.header("Filters")
    checks = sorted(df["check_name"].dropna().astype(str).unique().tolist()) if "check_name" in df.columns else []
    check = st.sidebar.selectbox("Check", options=["All"] + checks, index=0)
    view = df.copy()
    if check != "All":
        view = view[view["check_name"] == check]

    total = len(view)
    failed = int((~view["passed"]).sum()) if "passed" in view.columns else 0
    st.metric("Failed checks (rows)", f"{failed:,}")

    st.divider()

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Latest run results")
        latest_ts = view["run_timestamp"].max() if "run_timestamp" in view.columns else None
        latest = view[view["run_timestamp"] == latest_ts] if latest_ts is not None else view
        st.dataframe(
            latest.sort_values(["passed", "failed_rows"], ascending=[True, False]),
            use_container_width=True,
            height=360,
        )

    with right:
        st.subheader("Failures by check")
        by_check = (
            view.groupby("check_name", as_index=False)
            .agg(failed_rows=("failed_rows", "sum"), failed_checks=("passed", lambda s: int((~s).sum())))
            .sort_values(["failed_checks", "failed_rows"], ascending=[False, False])
        )
        st.plotly_chart(
            px.bar(by_check, x="check_name", y="failed_rows", title="Failed rows (sum)"),
            use_container_width=True,
        )

    st.divider()
    st.subheader("Data")
    download_csv_button(
        label="Download data quality results",
        df=view,
        file_name="data_quality_results.csv",
        key="dl_dq",
    )
    st.dataframe(view, use_container_width=True, height=520)


if __name__ == "__main__":
    main()

