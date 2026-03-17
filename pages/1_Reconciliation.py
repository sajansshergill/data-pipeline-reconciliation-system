from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.data_loader import get_backend_info, load_reconciliation_results
from src.dashboard.ui_utils import (
    apply_reconciliation_filters,
    download_csv_button,
    render_sidebar_filters,
)


st.set_page_config(page_title="Reconciliation", layout="wide")


@st.cache_data
def get_recon() -> pd.DataFrame:
    return load_reconciliation_results()


def main() -> None:
    st.title("Reconciliation Explorer")
    backend = get_backend_info()
    st.caption(f"Backend: {backend['backend']} • {backend['details']}")

    df = get_recon()
    filters = render_sidebar_filters(reconciliation_df=df)
    filtered = apply_reconciliation_filters(df, filters)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Records", f"{len(filtered):,}")
    k2.metric("Matched", f"{int((filtered['status'] == 'MATCH').sum()):,}" if "status" in filtered.columns else "—")
    k3.metric("Mismatched", f"{int((filtered['status'] == 'MISMATCH').sum()):,}" if "status" in filtered.columns else "—")
    k4.metric("Max abs diff", f"{float(filtered['abs_difference'].max()):,.4f}" if "abs_difference" in filtered.columns and len(filtered) else "0.0000")

    st.divider()

    left, right = st.columns([1, 1])
    with left:
        st.subheader("Status distribution")
        if "status" in filtered.columns and not filtered.empty:
            status_counts = filtered["status"].value_counts().reset_index()
            status_counts.columns = ["status", "count"]
            st.plotly_chart(
                px.bar(status_counts, x="status", y="count", title="Match vs mismatch"),
                use_container_width=True,
            )
        else:
            st.info("No reconciliation data available for the selected filters.")

    with right:
        st.subheader("Largest differences")
        if "abs_difference" in filtered.columns and not filtered.empty:
            top = filtered.sort_values("abs_difference", ascending=False).head(20)
            st.plotly_chart(
                px.bar(
                    top,
                    x="asset_symbol",
                    y="abs_difference",
                    color="status" if "status" in top.columns else None,
                    hover_data=["account_id", "computed_quantity", "reported_quantity", "quantity_difference"],
                    title="Top mismatches (abs difference)",
                ),
                use_container_width=True,
            )

    st.divider()

    st.subheader("Records")
    download_csv_button(
        label="Download filtered reconciliation",
        df=filtered,
        file_name="reconciliation_filtered.csv",
        key="dl_recon_filtered_page",
    )
    st.dataframe(filtered, use_container_width=True, height=520)


if __name__ == "__main__":
    main()

