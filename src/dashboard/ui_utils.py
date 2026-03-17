from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd
import streamlit as st


def utc_now() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc))


def format_currency(value: float) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "$0.00"


def download_csv_button(*, label: str, df: pd.DataFrame, file_name: str, key: str) -> None:
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label=label,
        data=csv,
        file_name=file_name,
        mime="text/csv",
        key=key,
        use_container_width=True,
    )


@dataclass(frozen=True)
class GlobalFilters:
    account_id: str | None
    status: list[str] | None


def render_sidebar_filters(*, reconciliation_df: pd.DataFrame) -> GlobalFilters:
    st.sidebar.header("Filters")

    accounts = (
        sorted(reconciliation_df["account_id"].dropna().astype(str).unique().tolist())
        if "account_id" in reconciliation_df.columns
        else []
    )
    statuses = (
        sorted(reconciliation_df["status"].dropna().astype(str).unique().tolist())
        if "status" in reconciliation_df.columns
        else []
    )

    account = st.sidebar.selectbox("Account", options=["All"] + accounts, index=0)
    selected_statuses = st.sidebar.multiselect(
        "Status",
        options=statuses,
        default=statuses,
    )

    st.sidebar.divider()
    if st.sidebar.button("Clear cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    return GlobalFilters(
        account_id=None if account == "All" else account,
        status=selected_statuses if selected_statuses else None,
    )


def apply_reconciliation_filters(df: pd.DataFrame, filters: GlobalFilters) -> pd.DataFrame:
    out = df.copy()
    if filters.status is not None and "status" in out.columns:
        out = out[out["status"].isin(filters.status)]
    if filters.account_id is not None and "account_id" in out.columns:
        out = out[out["account_id"] == filters.account_id]
    return out

