from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.data_loader import get_backend_info, load_portfolio_exposure
from src.dashboard.ui_utils import download_csv_button, format_currency


st.set_page_config(page_title="Exposure", layout="wide")


@st.cache_data
def get_exposure() -> pd.DataFrame:
    return load_portfolio_exposure()


def main() -> None:
    st.title("Portfolio Exposure")
    backend = get_backend_info()
    st.caption(f"Backend: {backend['backend']} • {backend['details']}")

    df = get_exposure()
    if df.empty:
        st.info("No exposure data available.")
        return

    st.sidebar.header("Filters")
    accounts = sorted(df["account_id"].dropna().astype(str).unique().tolist()) if "account_id" in df.columns else []
    symbols = sorted(df["asset_symbol"].dropna().astype(str).unique().tolist()) if "asset_symbol" in df.columns else []
    account = st.sidebar.selectbox("Account", options=["All"] + accounts, index=0)
    symbol = st.sidebar.selectbox("Asset", options=["All"] + symbols, index=0)
    top_n = st.sidebar.slider("Top N", min_value=5, max_value=50, value=15, step=5)

    view = df.copy()
    if account != "All":
        view = view[view["account_id"] == account]
    if symbol != "All":
        view = view[view["asset_symbol"] == symbol]

    total_mv = float(view["estimated_market_value"].sum()) if "estimated_market_value" in view.columns else 0.0
    st.metric("Total estimated market value", format_currency(total_mv))

    st.divider()

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Top exposures")
        top = view.sort_values("estimated_market_value", ascending=False).head(top_n)
        st.plotly_chart(
            px.bar(
                top,
                x="asset_symbol",
                y="estimated_market_value",
                hover_data=["account_id", "computed_quantity", "avg_traded_price"],
                title=f"Top {top_n} exposures by market value",
            ),
            use_container_width=True,
        )

    with right:
        st.subheader("Exposure by account")
        by_acct = (
            view.groupby("account_id", as_index=False)["estimated_market_value"]
            .sum()
            .sort_values("estimated_market_value", ascending=False)
            .head(20)
        )
        st.plotly_chart(
            px.bar(by_acct, x="account_id", y="estimated_market_value", title="Top accounts by exposure"),
            use_container_width=True,
        )

    st.divider()
    st.subheader("Data")
    download_csv_button(
        label="Download exposure",
        df=view,
        file_name="portfolio_exposure.csv",
        key="dl_exposure",
    )
    st.dataframe(view.sort_values("estimated_market_value", ascending=False), use_container_width=True, height=520)


if __name__ == "__main__":
    main()

