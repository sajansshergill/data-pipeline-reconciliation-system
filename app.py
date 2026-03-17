from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.data_loader import (
    get_backend_info,
    load_account_value_summary,
    load_data_quality_results,
    load_portfolio_exposure,
    load_reconciliation_results,
    load_reconciliation_summary,
)
from src.dashboard.ui_utils import (
    apply_reconciliation_filters,
    download_csv_button,
    format_currency,
    render_sidebar_filters,
    utc_now,
)


st.set_page_config(
    page_title="Financial Portfolio Reconciliation Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_data
def get_reconciliation_results() -> pd.DataFrame:
    return load_reconciliation_results()


@st.cache_data
def get_reconciliation_summary() -> pd.DataFrame:
    return load_reconciliation_summary()


@st.cache_data
def get_portfolio_exposure() -> pd.DataFrame:
    return load_portfolio_exposure()


@st.cache_data
def get_account_values() -> pd.DataFrame:
    return load_account_value_summary()


@st.cache_data
def get_data_quality_results() -> pd.DataFrame:
    return load_data_quality_results()


def main() -> None:
    st.title("Financial Portfolio Reconciliation Dashboard")
    st.caption(
        "Reconciliation, exposure analytics, account valuation, and data quality monitoring."
    )

    backend = get_backend_info()
    st.sidebar.caption(f"Backend: **{backend['backend']}**")
    st.sidebar.caption(backend["details"])

    reconciliation_df = get_reconciliation_results()
    recon_summary_df = get_reconciliation_summary()
    exposure_df = get_portfolio_exposure()
    account_values_df = get_account_values()
    dq_df = get_data_quality_results()

    filters = render_sidebar_filters(reconciliation_df=reconciliation_df)
    reconciliation_filtered = apply_reconciliation_filters(reconciliation_df, filters)

    total_records = len(reconciliation_df)
    matched_records = int((reconciliation_df["status"] == "MATCH").sum())
    mismatched_records = int((reconciliation_df["status"] == "MISMATCH").sum())
    match_rate = round((matched_records / total_records) * 100, 2) if total_records else 0.0
    total_account_value = (
        float(account_values_df["estimated_total_account_value"].sum())
        if not account_values_df.empty
        else 0.0
    )
    failed_dq_checks = int((~dq_df["passed"]).sum()) if not dq_df.empty else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Records Compared", f"{total_records:,}")
    col2.metric("Matched Records", f"{matched_records:,}")
    col3.metric("Mismatched Records", f"{mismatched_records:,}")
    col4.metric("Match Rate", f"{match_rate}%")
    col5.metric("DQ Failed Checks", f"{failed_dq_checks:,}")

    meta_left, meta_right = st.columns([2, 1])
    with meta_left:
        st.caption(f"Last refreshed: {utc_now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    with meta_right:
        download_csv_button(
            label="Download reconciliation (filtered)",
            df=reconciliation_filtered,
            file_name="reconciliation_filtered.csv",
            key="dl_recon_filtered_home",
        )

    st.divider()

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Reconciliation Status Distribution")
        status_counts = (
            reconciliation_df["status"]
            .value_counts()
            .reset_index()
        )
        status_counts.columns = ["status", "count"]

        fig_status = px.bar(
            status_counts,
            x="status",
            y="count",
            title="Match vs Mismatch Records",
        )
        st.plotly_chart(fig_status, use_container_width=True)

    with right_col:
        st.subheader("Top Accounts by Mismatches")
        top_mismatch_accounts = (
            recon_summary_df.sort_values(
                ["mismatched_assets", "max_abs_difference"],
                ascending=[False, False],
            )
            .head(10)
        )

        fig_mismatch_accounts = px.bar(
            top_mismatch_accounts,
            x="account_id",
            y="mismatched_assets",
            title="Accounts with Highest Mismatched Assets",
        )
        st.plotly_chart(fig_mismatch_accounts, use_container_width=True)

    st.divider()

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Top Portfolio Exposures")
        top_exposures = exposure_df.sort_values(
            "estimated_market_value", ascending=False
        ).head(15)

        fig_exposure = px.bar(
            top_exposures,
            x="asset_symbol",
            y="estimated_market_value",
            hover_data=["account_id", "computed_quantity", "avg_traded_price"],
            title="Largest Estimated Market Values",
        )
        st.plotly_chart(fig_exposure, use_container_width=True)

    with right_col:
        st.subheader("Top Accounts by Estimated Total Value")
        top_accounts = account_values_df.sort_values(
            "estimated_total_account_value", ascending=False
        ).head(10)

        fig_account_value = px.bar(
            top_accounts,
            x="account_id",
            y="estimated_total_account_value",
            title="Highest Value Accounts",
        )
        st.plotly_chart(fig_account_value, use_container_width=True)

    st.divider()

    st.subheader("Reconciliation Results Explorer")

    st.dataframe(reconciliation_filtered, use_container_width=True, height=360)

    st.divider()

    st.subheader("Data Quality Results")
    st.dataframe(dq_df, use_container_width=True, height=260)

    st.divider()

    st.subheader("Account Value Summary")
    st.dataframe(
        account_values_df.sort_values("estimated_total_account_value", ascending=False),
        use_container_width=True,
        height=260,
    )

    st.caption(f"Estimated total account value across all accounts: {format_currency(total_account_value)}")


if __name__ == "__main__":
    main()