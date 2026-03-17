from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.data_loader import get_backend_info, load_account_value_summary
from src.dashboard.ui_utils import download_csv_button, format_currency


st.set_page_config(page_title="Account valuation", layout="wide")


@st.cache_data
def get_values() -> pd.DataFrame:
    return load_account_value_summary()


def main() -> None:
    st.title("Account Valuation")
    backend = get_backend_info()
    st.caption(f"Backend: {backend['backend']} • {backend['details']}")

    df = get_values()
    if df.empty:
        st.info("No account valuation data available.")
        return

    st.sidebar.header("Filters")
    accounts = sorted(df["account_id"].dropna().astype(str).unique().tolist()) if "account_id" in df.columns else []
    account = st.sidebar.selectbox("Account", options=["All"] + accounts, index=0)
    top_n = st.sidebar.slider("Top N", 5, 50, 10, 5)

    view = df.copy()
    if account != "All":
        view = view[view["account_id"] == account]

    total = float(view["estimated_total_account_value"].sum()) if "estimated_total_account_value" in view.columns else 0.0
    st.metric("Total estimated account value", format_currency(total))

    st.divider()

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Top accounts")
        top = (
            view.sort_values("estimated_total_account_value", ascending=False)
            .head(top_n)
        )
        st.plotly_chart(
            px.bar(
                top,
                x="account_id",
                y="estimated_total_account_value",
                title=f"Top {top_n} accounts by total value",
            ),
            use_container_width=True,
        )

    with right:
        st.subheader("Cash vs portfolio")
        if {"cash_balance", "estimated_portfolio_value"}.issubset(view.columns):
            melt = view[["account_id", "cash_balance", "estimated_portfolio_value"]].melt(
                id_vars=["account_id"],
                value_vars=["cash_balance", "estimated_portfolio_value"],
                var_name="component",
                value_name="value",
            )
            melt = melt.groupby("component", as_index=False)["value"].sum()
            st.plotly_chart(
                px.pie(melt, names="component", values="value", title="Total value composition"),
                use_container_width=True,
            )

    st.divider()
    st.subheader("Data")
    download_csv_button(
        label="Download account values",
        df=view,
        file_name="account_values.csv",
        key="dl_account_values",
    )
    st.dataframe(view.sort_values("estimated_total_account_value", ascending=False), use_container_width=True, height=520)


if __name__ == "__main__":
    main()

