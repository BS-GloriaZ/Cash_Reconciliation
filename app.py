from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from cash_rec.config import load_config
from cash_rec.pipeline import run_cash_reconciliation

sys.path.insert(0, str(Path(__file__).parent))
from sync_inputs import sync_all


@st.cache_data(show_spinner=False)
def _run_reconciliation_cached(run_date: str, lookback_days: int) -> dict:
    return run_cash_reconciliation(run_date=run_date, lookback_days=lookback_days)

st.set_page_config(page_title="RecX", layout="wide")


def _fmt(df: pd.DataFrame):
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    return df.style.format({c: "{:,.2f}" for c in numeric_cols}, na_rep="")


def to_date_str(value) -> str:
    if pd.isna(value):
        return ""
    try:
        return pd.to_datetime(value).strftime("%Y-%m-%d")
    except Exception:
        return str(value)


def build_break_key(row: pd.Series) -> str:
    parts = [
        row.get("Type", ""),
        row.get("Display Date", ""),
        row.get("Mapping Fund", row.get("Fund", "")),
        row.get("Fund Type", ""),
        row.get("Custody", ""),
        row.get("Currency Code", ""),
        row.get("Tradar_Account", row.get("Tradar Accounts Used", "")),
        row.get("Source Account ID", row.get("Source SKAC", "")),
    ]
    return "|".join([str(x).strip() for x in parts])


def prepare_combined_detail(result: dict) -> pd.DataFrame:
    rec = result.get("rec_detail", pd.DataFrame()).copy()
    hi = result.get("hi_detail", pd.DataFrame()).copy()
    bbus = result.get("bbus_pdf_detail", pd.DataFrame()).copy()

    if not rec.empty:
        rec["Type"] = "Normal"
        rec["Display Date"] = rec["Date"].map(to_date_str) if "Date" in rec.columns else ""

        has_bbus_special = not bbus.empty

        if has_bbus_special:
            fund_col = rec["Mapping Fund"] if "Mapping Fund" in rec.columns else rec.get("Fund", "")
            custody_col = rec["Custody"] if "Custody" in rec.columns else ""
            mask = ~(
                fund_col.astype(str).str.strip().str.upper().eq("BBUS")
                & pd.Series(custody_col).astype(str).str.strip().str.upper().eq("BNP")
            )
            rec = rec.loc[mask].copy()

    if not hi.empty:
        hi["Type"] = "High Interest"
        hi["Display Date"] = hi["Target Date"].map(to_date_str) if "Target Date" in hi.columns else hi["Date"].map(to_date_str)

    if not bbus.empty:
        bbus["Type"] = "BNP Special"
        bbus["Display Date"] = bbus["Date"].map(to_date_str) if "Date" in bbus.columns else ""

    frames = [df for df in [rec, hi, bbus] if not df.empty]
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)

    if "Abs Variance" in combined.columns:
        combined["Abs Variance"] = pd.to_numeric(combined["Abs Variance"], errors="coerce").fillna(0.0)
    if "Variance" in combined.columns:
        combined["Variance"] = pd.to_numeric(combined["Variance"], errors="coerce")
    if "Source Ledger Balance" in combined.columns:
        combined["Source Ledger Balance"] = pd.to_numeric(combined["Source Ledger Balance"], errors="coerce")
    if "Tradar Balance" in combined.columns:
        combined["Tradar Balance"] = pd.to_numeric(combined["Tradar Balance"], errors="coerce")
    
    if "Fund Type" not in combined.columns:
        combined["Fund Type"] = ""

    combined["__break_key"] = combined.apply(build_break_key, axis=1)
    return combined


@st.cache_data
def _dashboard_trend(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("Display Date", dropna=False)["Abs Variance"]
        .sum()
        .reset_index()
        .sort_values("Display Date")
    )


@st.cache_data
def _dashboard_fund_type(df: pd.DataFrame) -> pd.DataFrame:
    local_abs = df["Local Variance"].abs().fillna(df["Abs Variance"]) if "Local Variance" in df.columns else df["Abs Variance"]
    return (
        df.assign(
            _local_abs=local_abs,
            _fund_type=df["Fund Type"].fillna("Unknown").replace("", "Unknown"),
        )
        .groupby("_fund_type")["_local_abs"]
        .sum()
        .reset_index()
        .rename(columns={"_fund_type": "Fund Type", "_local_abs": "Abs Variance (AUD)"})
        .sort_values("Abs Variance (AUD)", ascending=False)
    )


@st.cache_data
def _compute_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    # Use abs(Local Variance) where available (converted to AUD or NZD per fund type).
    # Fall back to Abs Variance for rows without a local-currency conversion
    # (HI / BNP Special rows, or rows whose currency has no FX mapping → treated as AUD).
    if "Local Variance" in df.columns:
        local_abs = df["Local Variance"].abs().fillna(df["Abs Variance"])
    else:
        local_abs = df["Abs Variance"]
    work = df.assign(_local_abs=local_abs)
    result = (
        work.groupby(["Display Date", "Custody", "Status"], dropna=False)
        .agg(Count=("Status", "size"), Total_Abs_Variance=("_local_abs", "sum"))
        .reset_index()
        .sort_values(["Display Date", "Custody", "Status"], ascending=[False, True, True])
        .reset_index(drop=True)
    )
    result["Count"] = result["Count"].astype(int)
    return result.rename(columns={"Display Date": "Settlement Date"})


def show_dashboard(df: pd.DataFrame) -> None:
    st.header("Page 1 · Dashboard")

    latest_date = df["Display Date"].dropna().astype(str).max() if not df.empty and "Display Date" in df.columns else None
    latest_df = df[df["Display Date"].astype(str) == latest_date] if latest_date else df

    total_rows = len(latest_df)
    break_rows = int(latest_df["Status"].astype(str).eq("Break").sum()) if "Status" in latest_df.columns else 0
    matched_rows = int(latest_df["Status"].astype(str).eq("Matched").sum()) if "Status" in latest_df.columns else 0
    if not latest_df.empty:
        if "Local Variance" in latest_df.columns:
            _local_abs = latest_df["Local Variance"].abs().fillna(pd.to_numeric(latest_df["Abs Variance"], errors="coerce"))
        else:
            _local_abs = pd.to_numeric(latest_df["Abs Variance"], errors="coerce")
        abs_variance_latest = _local_abs.fillna(0).sum()
    else:
        abs_variance_latest = 0

    st.caption(f"Metrics as at **{latest_date}**" if latest_date else "")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Rows", f"{total_rows:,}")
    c2.metric("Breaks", f"{break_rows:,}")
    c3.metric("Matched", f"{matched_rows:,}")
    c4.metric("Abs Variance (AUD)", f"A$ {abs_variance_latest:,.2f}")

    left, right = st.columns([2, 1])

    with left:
        st.subheader("Variance by Date")
        if not df.empty and "Display Date" in df.columns and "Abs Variance" in df.columns:
            trend_df = _dashboard_trend(df).reset_index()
            st.altair_chart(
                alt.Chart(trend_df).mark_line(point=True).encode(
                    x=alt.X("Display Date:O", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("Abs Variance:Q", title="Abs Variance"),
                ),
                use_container_width=True,
            )
        else:
            st.info("No trend data available.")

    with right:
        st.subheader("Variance by Fund Type")
        if not latest_df.empty and "Fund Type" in latest_df.columns and "Abs Variance" in latest_df.columns:
            fund_df = _dashboard_fund_type(latest_df).reset_index()
            st.altair_chart(
                alt.Chart(fund_df).mark_bar().encode(
                    x=alt.X("Fund Type:N", axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("Abs Variance (AUD):Q", title="Abs Variance (AUD)", axis=alt.Axis(format=",.0f")),
                    tooltip=[
                        alt.Tooltip("Fund Type:N"),
                        alt.Tooltip("Abs Variance (AUD):Q", format=",.2f", title="Abs Variance (AUD)"),
                    ],
                ),
                use_container_width=True,
            )
        else:
            st.info("No fund type data available.")

    st.subheader("Summary")
    if not df.empty and "Display Date" in df.columns and "Custody" in df.columns:
        _date_ranks = (
            df[["Custody", "Display Date"]].drop_duplicates()
            .assign(_rank=lambda d: d.groupby("Custody")["Display Date"]
                    .rank(method="dense", ascending=False))
        )
        _valid = _date_ranks[_date_ranks["_rank"] <= 5][["Custody", "Display Date"]]
        summary_df = df.merge(_valid, on=["Custody", "Display Date"])
    else:
        summary_df = df
    summary = _compute_summary(summary_df)
    if not summary.empty:
        st.dataframe(
            summary.style.format({"Count": "{:,d}", "Total_Abs_Variance": "{:,.2f}"}),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No summary data.")


def show_detail_page(df: pd.DataFrame) -> None:
    st.markdown(
        """
        <style>
        .block-container { padding-top: 1rem !important; }
        h1, h2 { margin-top: 0 !important; padding-top: 0 !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.subheader("Detailed Rec")

    if df.empty:
        st.warning("No detailed reconciliation data available.")
        return

    if "comments" not in st.session_state:
        st.session_state["comments"] = {}

    with st.sidebar:
        st.subheader("Filters")

        fund_source = "Mapping Fund" if "Mapping Fund" in df.columns else "Fund"
        fund_options = ["All"] + sorted([x for x in df[fund_source].dropna().astype(str).unique().tolist() if x])

        fund_type_options = ["All"] + sorted([x for x in df["Fund Type"].dropna().astype(str).unique().tolist() if x])
        custody_options = ["All"] + sorted([x for x in df["Custody"].dropna().astype(str).unique().tolist() if x]) if "Custody" in df.columns else ["All"]
        currency_options = ["All"] + sorted([x for x in df["Currency Code"].dropna().astype(str).unique().tolist() if x]) if "Currency Code" in df.columns else ["All"]
        type_options = ["All"] + sorted([x for x in df["Type"].dropna().astype(str).unique().tolist() if x])
        status_options = ["All"] + sorted([x for x in df["Status"].dropna().astype(str).unique().tolist() if x])
        _tradar_acct_col = "Tradar_Account" if "Tradar_Account" in df.columns else "Tradar Accounts Used" if "Tradar Accounts Used" in df.columns else None
        tradar_acct_options = ["All"] + sorted([x for x in df[_tradar_acct_col].dropna().astype(str).unique().tolist() if x]) if _tradar_acct_col else ["All"]

        _date_vals = sorted([x for x in df["Display Date"].dropna().astype(str).unique().tolist() if x])
        _date_vals_desc = list(reversed(_date_vals))
        _date_mode = st.radio("Date filter", ["Single", "Range"], horizontal=True)
        if _date_mode == "Single":
            _sel = st.selectbox("Date", ["All"] + _date_vals_desc)
            if _sel == "All":
                selected_date_range = (None, None)
            else:
                selected_date_range = (_sel, _sel)
        else:
            _from = st.selectbox("From", _date_vals, index=0)
            _to = st.selectbox("To", _date_vals_desc, index=0)
            if _from > _to:
                _from, _to = _to, _from
            selected_date_range = (_from, _to)
        selected_fund = st.selectbox("Fund", fund_options, index=0)
        selected_fund_type = st.selectbox("Fund Type", fund_type_options, index=0)
        selected_custody = st.selectbox("Custody", custody_options, index=0)
        selected_currency = st.selectbox("Currency", currency_options, index=0)
        selected_tradar_acct = st.selectbox("Tradar Account", tradar_acct_options, index=0)
        selected_type = st.selectbox("Type", type_options, index=0)
        selected_status = st.selectbox("Status", status_options, index=0)

        materiality_options = ["All", "HIGH", "MODERATE", "LOW", ""]
        selected_materiality = st.selectbox("Materiality", materiality_options, index=0)

        abs_variance_min = st.number_input("Absolute Variance Min", min_value=0.0, value=0.0, step=1000.0)
        materiality = st.number_input("Materiality", min_value=0.0, value=0.0, step=1000.0)
        search_text = st.text_input("Search")

    filtered = df

    date_start, date_end = selected_date_range
    if date_start is not None:
        filtered = filtered[
            (filtered["Display Date"].astype(str) >= date_start) &
            (filtered["Display Date"].astype(str) <= date_end)
        ]

    if selected_fund != "All":
        filtered = filtered[filtered[fund_source].astype(str) == selected_fund]

    if selected_fund_type != "All":
        filtered = filtered[filtered["Fund Type"].astype(str) == selected_fund_type]

    if selected_custody != "All" and "Custody" in filtered.columns:
        filtered = filtered[filtered["Custody"].astype(str) == selected_custody]

    if selected_currency != "All" and "Currency Code" in filtered.columns:
        filtered = filtered[filtered["Currency Code"].astype(str) == selected_currency]

    if selected_tradar_acct != "All" and _tradar_acct_col:
        filtered = filtered[filtered[_tradar_acct_col].astype(str) == selected_tradar_acct]

    if selected_type != "All":
        filtered = filtered[filtered["Type"].astype(str) == selected_type]

    if selected_status != "All":
        filtered = filtered[filtered["Status"].astype(str) == selected_status]

    if selected_materiality != "All" and "Materiality" in filtered.columns:
        filtered = filtered[filtered["Materiality"].fillna("").astype(str) == selected_materiality]

    threshold = max(abs_variance_min, materiality)
    filtered = filtered[pd.to_numeric(filtered["Abs Variance"], errors="coerce").fillna(0.0) >= threshold]

    if search_text:
        needle = search_text.strip().lower()
        str_df = filtered.astype(str)
        haystack = str_df.iloc[:, 0]
        for _col in str_df.columns[1:]:
            haystack = haystack + " " + str_df[_col]
        filtered = filtered[haystack.str.lower().str.contains(needle, regex=False, na=False)]

    st.caption(f"Showing {len(filtered):,} rows")

    display_cols = [
        "Display Date",
        "Type",
        "Mapping Fund",
        "Fund Type",
        "Custody",
        "Currency Code",
        "Tradar_Account",
        "Tradar Accounts Used",
        "Source Ledger Balance",
        "Tradar Balance",
        "Variance",
        "Abs Variance",
        "Local Variance",
        "BPS Impact",
        "Materiality",
        "Status",
    ]
    display_cols = [c for c in display_cols if c in filtered.columns]

    if filtered.empty:
        st.info("No rows match the current filters.")
    else:
        display_df = filtered[display_cols].copy().reset_index(drop=True).rename(columns={"Display Date": "Settlement Date"})
        break_keys = filtered["__break_key"].reset_index(drop=True)
        display_df["Comment"] = break_keys.map(
            lambda k: st.session_state["comments"].get(k, "")
        )

        disabled_cols = [c for c in display_df.columns if c != "Comment"]
        col_config: dict = {
            "Comment": st.column_config.TextColumn("Comment", width="medium"),
        }
        for col in display_df.columns:
            if col != "Comment" and pd.api.types.is_numeric_dtype(display_df[col]):
                col_config[col] = st.column_config.NumberColumn(col, format="%,.2f")

        filter_sig = "|".join([
            str(selected_date_range), str(selected_fund), str(selected_fund_type),
            str(selected_custody), str(selected_tradar_acct), str(selected_type), str(selected_status),
            str(selected_materiality), str(abs_variance_min), str(materiality),
            str(search_text),
        ])
        edited_df = st.data_editor(
            display_df,
            use_container_width=True,
            hide_index=True,
            disabled=disabled_cols,
            column_config=col_config,
            key=f"breaks_editor_{filter_sig}",
        )

        for i, row in edited_df.iterrows():
            bk = break_keys.iloc[i]
            st.session_state["comments"][bk] = str(row.get("Comment", "") or "")


def main() -> None:
    st.title("RecX")

    col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])

    with col1:
        run_date = st.date_input("Run Date", value=date.today())

    with col2:
        lookback_days = st.number_input("Lookback Days", min_value=1, value=60, step=1)

    with col3:
        st.write("")
        st.write("")
        sync_clicked = st.button("Sync Inputs", use_container_width=True, help="Copy new files from network share to local data/input")

    with col4:
        st.write("")
        st.write("")
        run_clicked = st.button("Run Reconciliation", use_container_width=True)

    with col5:
        st.write("")
        st.write("")
        if st.button("Clear Cache", use_container_width=True, help="Force re-read from source folders"):
            _run_reconciliation_cached.clear()
            st.session_state["result"] = None
            st.session_state["combined_df"] = None
            st.rerun()

    if sync_clicked:
        with st.spinner("Syncing input files from network share..."):
            try:
                config = load_config()
                total_copied, msgs = sync_all(config)
            except Exception as exc:
                st.error(f"Sync failed: {exc}")
                msgs = []
                total_copied = 0
        if total_copied > 0:
            st.success(f"Sync complete — {total_copied} file(s) copied.")
            _run_reconciliation_cached.clear()
            st.session_state["result"] = None
            st.session_state["combined_df"] = None
        else:
            st.info("Sync complete — all files already up to date.")
        with st.expander("Sync log", expanded=total_copied > 0):
            st.text("\n".join(msgs))

    if "result" not in st.session_state:
        st.session_state["result"] = None
    if "combined_df" not in st.session_state:
        st.session_state["combined_df"] = None

    if run_clicked:
        with st.spinner("Running reconciliation..."):
            result = _run_reconciliation_cached(str(run_date), int(lookback_days))
            combined_df = prepare_combined_detail(result)
            st.session_state["result"] = result
            st.session_state["combined_df"] = combined_df

        for warn in result.get("source_warnings", []):
            st.warning(f"Source folder not read-only: {warn}")

        timings = dict(result.get("timings", {}))
        total = timings.pop("total", sum(timings.values()))
        st.success(f"Reconciliation completed in **{total:.2f}s**")
        if timings:
            timing_df = (
                pd.DataFrame({"Step": list(timings.keys()), "Seconds": list(timings.values())})
                .assign(Pct=lambda d: (d["Seconds"] / total * 100).round(1))
            )
            with st.expander("Run time breakdown"):
                st.dataframe(
                    timing_df.style.format({"Seconds": "{:.3f}", "Pct": "{:.1f}%"}),
                    hide_index=True,
                    use_container_width=True,
                )

        with st.expander("Source file paths"):
            fp = result.get("file_paths", {})
            for src, path in fp.items():
                st.text(f"{src}: {path}")

    result = st.session_state.get("result")
    combined_df = st.session_state.get("combined_df")

    if result is None or combined_df is None:
        st.info("Click 'Run Reconciliation' to load the latest data directly from the pipeline.")
        return

    page = st.radio("Navigate", ["Dashboard", "Detailed Rec"], horizontal=True)

    if page == "Dashboard":
        show_dashboard(combined_df)
    else:
        show_detail_page(combined_df)


if __name__ == "__main__":
    main()