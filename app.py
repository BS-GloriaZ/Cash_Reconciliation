from __future__ import annotations

import hashlib
import json
import sys
from datetime import date, datetime
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from cash_rec.config import load_config
from cash_rec.pipeline import run_cash_reconciliation

sys.path.insert(0, str(Path(__file__).parent))
from sync_inputs import sync_all

_OVERRIDES_PATH = Path(__file__).parent / "data" / "output" / "manual_overrides.json"


def _load_overrides() -> dict:
    try:
        if _OVERRIDES_PATH.exists():
            return json.loads(_OVERRIDES_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_overrides(overrides: dict) -> None:
    _OVERRIDES_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OVERRIDES_PATH.write_text(json.dumps(overrides, indent=2, default=str), encoding="utf-8")


@st.cache_data(show_spinner=False)
def _assign_group_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Replace internal _gid column with a stable content-based 8-char Group ID.

    All rows sharing the same _gid receive the same Group ID.
    Key includes Fund/Custody/CCY (cross-fund uniqueness), settle dates,
    security IDs, types, amounts and descriptions (intra-day uniqueness).
    Status is intentionally excluded so the ID stays stable after overrides.
    """
    df = df.copy()
    gid_map: dict[int, str] = {}
    for gid_val in df["_gid"].unique():
        first = df.loc[df["_gid"] == gid_val].iloc[0]
        parts = [
            str(first.get("Fund", "")),
            str(first.get("Custody", "")),
            str(first.get("CCY", "")),
            str(first.get("Tradar Settle", "")),
            str(first.get("Security ID", "")),
            str(first.get("Type", "")),
            str(first.get("Tradar Amount", "")),
            str(first.get("Description", "")),
            str(first.get("Custodian Settle", "")),
            str(first.get("Custodian Amount", "")),
            str(first.get("Cust Description", "")),
        ]
        gid_map[gid_val] = hashlib.sha256("|".join(parts).encode()).hexdigest()[:8].upper()
    df.insert(0, "Group ID", df["_gid"].map(gid_map))
    return df.drop(columns=["_gid"])


def _full_rerun() -> None:
    """Rerun the full app. Works inside @st.fragment (Streamlit ≥1.37) and outside."""
    try:
        st.rerun(scope="app")
    except TypeError:
        st.rerun()


try:
    _fragment = st.fragment
except AttributeError:
    import functools
    def _fragment(func):          # type: ignore[misc]
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper


@_fragment
def _render_txn_table(df: pd.DataFrame, key: str) -> None:
    """Interactive transaction rec table with Group ID, comments, and mark-as-matched.

    Decorated with @st.fragment so checkbox ticks and the show-matched toggle
    only rerun this function, not the entire page.
    """
    if df.empty:
        return

    df = _assign_group_ids(df)
    overrides = _load_overrides()

    # Apply manual-match overrides
    df["Status"] = df.apply(
        lambda r: "ManualMatch"
        if overrides.get(r["Group ID"], {}).get("status") == "ManualMatch"
        else r["Status"],
        axis=1,
    )

    _RECONCILED = {"Matched", "ManualMatch"}

    # Save status lookup keyed by Group ID BEFORE any df transformations.
    # st.data_editor may not reliably return disabled-column values, so we
    # never trust edited["Status"] — we always derive it from this map.
    _gid_status: dict[str, str] = dict(zip(df["Group ID"], df["Status"]))

    n_breaks  = int(df["Status"].isin({"Tradar Only", "Custodian Only", "Break"}).sum())
    n_matched = int(df["Status"].isin(_RECONCILED).sum())
    show_matched = st.toggle(
        f"Include matched rows ({n_matched})",
        value=False, key=f"show_matched_{key}",
    )

    if not show_matched:
        df = df[~df["Status"].isin(_RECONCILED)].reset_index(drop=True)

    if df.empty:
        st.success(f"All {n_matched} transaction(s) matched — no breaks.")
        return

    def _fmt_amounts(d: pd.DataFrame) -> pd.DataFrame:
        d = d.copy()
        for _c in ["Tradar Amount", "Custodian Amount", "Amount Diff"]:
            if _c in d.columns:
                d[_c] = pd.to_numeric(d[_c], errors="coerce").apply(
                    lambda x: f"{x:,.2f}" if pd.notna(x) else ""
                )
        return d

    df["Comment"] = df["Group ID"].map(
        lambda gid: overrides.get(gid, {}).get("comment", "")
    )
    # Stash raw numerics for diff summary before string-formatting
    df["_raw_trd"]  = pd.to_numeric(df["Tradar Amount"],    errors="coerce").fillna(0.0)
    df["_raw_cust"] = pd.to_numeric(df["Custodian Amount"], errors="coerce").fillna(0.0)

    df = _fmt_amounts(df)

    ctrl1, ctrl2, _ = st.columns([1.2, 2.5, 4])
    select_all = ctrl1.checkbox("Select all", value=False, key=f"sel_all_{key}")
    tolerance = ctrl2.number_input(
        "Tolerance (mark-as-matched threshold)",
        min_value=0.0, value=1.0, step=0.01, format="%.2f",
        key=f"tol_{key}",
        help="'Mark as Matched' is enabled when |Custodian − Tradar| ≤ this value and no reconciled rows are selected.",
    )

    df.insert(1, "✓", select_all)

    disabled_cols = [c for c in df.columns if c not in ("✓", "Comment")]
    col_cfg: dict = {
        "✓":         st.column_config.CheckboxColumn("✓", default=False, width=35),
        "Group ID":  st.column_config.TextColumn("Group ID",  disabled=True, width=90),
        "Status":    st.column_config.TextColumn("Status",    disabled=True, width=110),
        "Comment":   st.column_config.TextColumn("Comment",   width=220),
        "_raw_trd":  st.column_config.NumberColumn(disabled=True),
        "_raw_cust": st.column_config.NumberColumn(disabled=True),
    }

    edited = st.data_editor(
        df, column_config=col_cfg, disabled=disabled_cols,
        column_order=[c for c in df.columns if c not in ("_raw_trd", "_raw_cust")],
        hide_index=True, use_container_width=True,
        key=f"txn_editor_{key}_{select_all}",
    )

    # ── Selected-row net diff summary ─────────────────────────────────────────
    selected = edited[edited["✓"] == True]
    within_tol = False
    has_reconciled = False
    net_diff = 0.0
    if not selected.empty:
        trd_total  = float(selected["_raw_trd"].sum())
        cust_total = float(selected["_raw_cust"].sum())
        net_diff   = cust_total - trd_total
        within_tol = abs(net_diff) <= tolerance
        # Use the saved mapping — never trust edited["Status"]
        has_reconciled = selected["Group ID"].map(_gid_status).isin(_RECONCILED).any()

        sc1, sc2, sc3 = st.columns(3)
        sc1.metric("Tradar Total (selected)",    f"{trd_total:,.2f}")
        sc2.metric("Custodian Total (selected)", f"{cust_total:,.2f}")
        sc3.metric("Net Diff (Cust − Trd)",      f"{net_diff:,.2f}",
                   delta=f"{'Within' if within_tol else 'Exceeds'} tolerance {tolerance:,.2f}",
                   delta_color="normal" if within_tol else "inverse")

        if has_reconciled:
            st.warning("Selection includes already-reconciled rows (Matched / ManualMatch) — uncheck them to enable Mark as Matched.")
    else:
        selected = pd.DataFrame()

    # Derive per-selection flags from the saved status map
    selected_manual_gids = (
        [gid for gid in selected["Group ID"].unique()
         if _gid_status.get(gid) == "ManualMatch"]
        if not selected.empty else []
    )
    has_manual_match = bool(selected_manual_gids)

    # ── Action buttons ────────────────────────────────────────────────────────
    c1, c2, c3, _ = st.columns([1.8, 1.6, 1.8, 2])
    with c1:
        mark_disabled = selected.empty or not within_tol or has_reconciled
        mark_help = (
            "Tick rows first." if selected.empty
            else "Uncheck already-reconciled rows." if has_reconciled
            else f"Net diff {net_diff:,.2f} exceeds tolerance {tolerance:,.2f}." if not within_tol
            else None
        )
        if st.button("✅ Mark as Matched", key=f"mark_{key}",
                     disabled=mark_disabled, help=mark_help):
            ovr = _load_overrides()
            for gid in selected["Group ID"].unique():
                comment = str(edited.loc[edited["Group ID"] == gid, "Comment"].iloc[0]).strip()
                ovr[gid] = {"status": "ManualMatch", "comment": comment,
                            "timestamp": datetime.now().isoformat()}
            _save_overrides(ovr)
            st.success(f"Marked {len(selected['Group ID'].unique())} group(s) as matched.")
            _full_rerun()
    with c2:
        if st.button("💾 Save Comments", key=f"save_cmt_{key}"):
            ovr = _load_overrides()
            saved = 0
            for _, r in edited.iterrows():
                gid = r["Group ID"]
                comment = str(r.get("Comment", "")).strip()
                if comment != ovr.get(gid, {}).get("comment", ""):
                    ovr.setdefault(gid, {})["comment"] = comment
                    saved += 1
            _save_overrides(ovr)
            st.success(f"Saved {saved} comment(s).")
    with c3:
        break_disabled = not has_manual_match
        break_help = (
            "Tick ManualMatch rows to break." if not has_manual_match else None
        )
        if st.button("↩ Break Manual Match", key=f"break_{key}",
                     disabled=break_disabled, help=break_help):
            ovr = _load_overrides()
            for gid in selected_manual_gids:
                ovr.pop(gid, None)
            _save_overrides(ovr)
            st.success(f"Removed manual match for {len(selected_manual_gids)} group(s).")
            _full_rerun()


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
    local_abs = df["Local Variance"].abs().fillna(df["Abs Variance"]) if "Local Variance" in df.columns else df["Abs Variance"]
    return (
        df.assign(_local_abs=local_abs)
        .groupby("Display Date", dropna=False)["_local_abs"]
        .sum()
        .reset_index()
        .rename(columns={"_local_abs": "Abs Variance (AUD)"})
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


def _waterfall_chart_data(
    raw_tradar_settled: pd.DataFrame,
    tradar_account: str,
    ccy: str,
    up_to_date: pd.Timestamp,
    opening_balance: float,
    tradar_fund: str = "",
) -> pd.DataFrame:
    # tradar_account may be "acct1 + acct2 + acct3" for multi-account rows (e.g. BBUS)
    accounts = [a.strip() for a in tradar_account.replace(" + ", ",").split(",") if a.strip() and a.strip() != "nan"]
    if accounts:
        mask = (
            raw_tradar_settled["Account"].str.strip().isin(accounts)
            & raw_tradar_settled["CCY"].str.strip().eq(ccy.strip())
            & raw_tradar_settled["Settles"].notna()
            & (raw_tradar_settled["Settles"] <= up_to_date)
        )
        if tradar_fund and "Fund" in raw_tradar_settled.columns:
            mask &= raw_tradar_settled["Fund"].str.strip().eq(tradar_fund)
    else:
        mask = pd.Series(False, index=raw_tradar_settled.index)
    txns = raw_tradar_settled[mask].sort_values(["Settles", "Date"]).copy()

    rows: list[dict] = []
    running = float(opening_balance) if pd.notna(opening_balance) else 0.0

    rows.append({
        "Order": 0, "Label": "Opening\nBalance",
        "From": 0.0, "To": running,
        "BarType": "total", "Amount": running,
        "Settle Date": "", "Description": "Opening Balance",
    })

    order = 1
    for _, txn in txns.iterrows():
        cf = pd.to_numeric(txn.get("Cashflow", 0), errors="coerce")
        if pd.isna(cf) or cf == 0:
            continue
        settle_str = pd.Timestamp(txn["Settles"]).strftime("%m/%d") if pd.notna(txn.get("Settles")) else ""
        txn_type = str(txn.get("Type", "")).strip()
        rows.append({
            "Order": order, "Label": settle_str + (f"\n{txn_type}" if txn_type else ""),
            "From": running, "To": running + cf,
            "BarType": "positive" if cf > 0 else "negative",
            "Amount": cf,
            "Settle Date": settle_str,
            "Description": str(txn.get("Description", "")).strip(),
        })
        running += cf
        order += 1

    rows.append({
        "Order": order, "Label": "Tradar\nBalance",
        "From": 0.0, "To": running,
        "BarType": "total", "Amount": running,
        "Settle Date": "", "Description": f"Tradar Closing Balance",
    })

    df = pd.DataFrame(rows)
    df["Start"] = df[["From", "To"]].min(axis=1)
    df["End"] = df[["From", "To"]].max(axis=1)
    return df


def _make_waterfall_chart(wf_df: pd.DataFrame, title: str, ccy: str, color_scale: alt.Scale) -> alt.Chart:
    return (
        alt.Chart(wf_df)
        .mark_bar(size=24)
        .encode(
            x=alt.X("Label:N", sort=alt.SortField("Order", order="ascending"), title="",
                    axis=alt.Axis(labelAngle=-30, labelLimit=120)),
            y=alt.Y("Start:Q", title=f"Balance ({ccy})", axis=alt.Axis(format=",.0f")),
            y2=alt.Y2("End:Q"),
            color=alt.Color("BarType:N", scale=color_scale, legend=None),
            tooltip=[
                alt.Tooltip("Label:N", title="Step"),
                alt.Tooltip("Description:N", title="Description"),
                alt.Tooltip("Amount:Q", format=",.2f", title="Amount / Balance"),
                alt.Tooltip("Settle Date:N", title="Date"),
            ],
        )
        .properties(height=320, title=title)
    )


@st.cache_data(show_spinner=False)
def _reconcile_txns(trd_df: pd.DataFrame, cust_df: pd.DataFrame, _v: int = 10) -> pd.DataFrame:
    """Match Tradar vs custodian transactions.

    Pass 1 — Dividend grouping:
      Tradar div rows (Type startswith "div") with the same SEDOL + settle date are
      summed and compared against the matching custodian group (Description contains
      "dividend"/"interest"/"income", same Security ID + settle date). Both Tradar
      and Custodian sides are summed independently before comparison.

    Pass 2 — Trade matching:
      Tradar "Purchase" → Custodian "Receipt vs Payt"; Tradar "Sell"/"Sale" →
      Custodian "SEC-DEL vs Payt". Matched by SEDOL + settle date.

    Pass 3 — Generic 1-to-1 greedy by exact signed amount.

    matched_groups tuple: (trd_idxs, cust_idxs, status, trd_sum, cust_sum)
    """
    trd = trd_df.reset_index(drop=True).copy()
    cust = cust_df.reset_index(drop=True).copy()

    trd_amt  = pd.to_numeric(trd.get("Cashflow", pd.Series(dtype=float)), errors="coerce").round(2)
    cust_amt = pd.to_numeric(cust.get("Amount",   pd.Series(dtype=float)), errors="coerce").round(2)

    matched_trd:  set[int] = set()
    matched_cust: set[int] = set()
    # each entry: (trd_idxs, cust_idxs, status, trd_sum, cust_sum)
    matched_groups: list[tuple] = []

    def _is_div(i: int) -> bool:
        if "Type" not in trd.columns:
            return False
        t = str(trd["Type"].iloc[i]).strip().casefold()
        return t.startswith("div") or t.startswith("cpn") or "coupon" in t

    def _is_trade(i: int) -> bool:
        if "Type" not in trd.columns:
            return False
        return str(trd["Type"].iloc[i]).strip().casefold() in ("purchase", "sell", "sale")

    def _direction(amt) -> str:
        return "" if pd.isna(amt) else ("In" if amt > 0 else "Out")

    _EMPTY_STRS = {"", "nan", "none", "n/a", "na", "<na>", "<nan>"}

    def _safe_str(val) -> str:
        """Convert a cell value to str, returning '' for any NA/null variant."""
        try:
            if pd.isna(val):
                return ""
        except (TypeError, ValueError):
            pass
        s = str(val).strip()
        return "" if s.casefold() in _EMPTY_STRS else s

    _BOND_SEC_TYPES    = {"government bond", "mtn bond", "term deposit", "corporate bond",
                          "bond", "fixed income", "floating rate note", "frn"}
    _EQUITY_SEC_TYPES  = {"equity", "common stock", "ordinary share", "ordinary shares",
                          "preference share", "preference shares", "reit"}
    _FX_SEC_TYPES      = {"exchrate"}

    def _trd_sec_id(row) -> str:
        sec_type = _safe_str(row.get("Security Type")).casefold()
        # Fixed income → ISIN only
        if any(bt in sec_type for bt in _BOND_SEC_TYPES):
            return _safe_str(row.get("Isin"))
        # Equity → SEDOL only
        if any(et in sec_type for et in _EQUITY_SEC_TYPES):
            return _safe_str(row.get("Sedol"))
        # FX / Exchrate → no security ID (routes to FX pass)
        if any(ft in sec_type for ft in _FX_SEC_TYPES):
            return ""
        # Unknown security type — SEDOL first, ISIN as fallback
        s = _safe_str(row.get("Sedol"))
        if s:
            return s
        return _safe_str(row.get("Isin"))

    def _cust_sec_id(row) -> str:
        return _safe_str(row.get("Security ID"))

    def _cust_has_id(cust_row, sec_id: str) -> bool:
        """True if sec_id matches any of the custodian's available identifiers."""
        if not sec_id:
            return False
        for col in ("ISIN", "SEDOL", "Security ID"):
            if _safe_str(cust_row.get(col)) == sec_id:
                return True
        return False

    def _norm_settle(ts) -> "pd.Timestamp | None":
        try:
            return pd.Timestamp(ts).normalize() if pd.notna(ts) else None
        except Exception:
            return None

    def _settle_eq(s1, s2) -> bool:
        n1 = _norm_settle(s1); n2 = _norm_settle(s2)
        return n1 is not None and n2 is not None and n1 == n2

    def _trade_cust_type_ok(trd_type: str, cust_desc: str) -> bool:
        t = trd_type.strip().casefold()
        d = cust_desc.strip().casefold()
        if t == "purchase":
            return any(kw in d for kw in ("receipt vs payt", "rvp", "receipt versus payment",
                                           "receipt vs payment", "security purchases", "securities purchases",
                                           "purchase of securities", "o/s purchase"))
        if t in ("sell", "sale"):
            return any(kw in d for kw in ("sec-del vs payt", "sec del vs payt", "dvp",
                                           "delivery vs payt", "deliver vs payt", "sec del vs pay",
                                           "sale of securities", "securities sale", "sell of securities",
                                           "o/s sale"))
        return False

    def _is_cust_div_desc(val) -> bool:
        d = _safe_str(val).casefold()
        return any(kw in d for kw in ("dividend", "interest", "income"))

    def _is_cust_trade_desc(val) -> bool:
        d = _safe_str(val).casefold()
        return any(kw in d for kw in ("receipt vs payt", "rvp", "security purchases",
                                       "purchase of securities", "o/s purchase",
                                       "sec-del vs payt", "sec del vs payt", "dvp",
                                       "delivery vs payt", "sale of securities",
                                       "securities sale", "o/s sale",
                                       "contractual settlement"))

    def _cust_type_label(desc: str) -> str:
        if _is_cust_trade_desc(desc):
            return "Trade"
        if _is_cust_div_desc(desc):
            return "Div/Income"
        return ""

    def _type_category(trd_type: str, trd_desc: str, cust_desc: str,
                       has_sec_id: bool, trd_sec_type: str = "") -> str:
        t   = trd_type.casefold()
        td  = trd_desc.casefold()
        cd  = cust_desc.casefold()
        st  = trd_sec_type.casefold()

        # FX — Security Type is most reliable; check first to avoid misclassifying
        # Exchrate purchase/sells as Trade
        if any(ft in st for ft in _FX_SEC_TYPES):
            return "FX"

        # Dividend / Coupon
        if t.startswith("div") or t.startswith("cpn") or "coupon" in t:
            return "Dividend/Coupon"
        if any(kw in td for kw in ("dividend", "coupon")):
            return "Dividend/Coupon"
        if any(kw in cd for kw in ("dividend", "coupon")):
            return "Dividend/Coupon"

        # Interest
        if "interest" in t or "interest" in td or "interest" in cd:
            return "Interest"

        # Tax / Withholding
        if any(kw in td for kw in ("tax", "withholding", "wht")):
            return "Tax"
        if any(kw in cd for kw in ("withholding", "wht", "tax")):
            return "Tax"

        # Trade (securities — has SEDOL or ISIN)
        if t in ("purchase", "sell", "sale") and has_sec_id:
            return "Trade"
        if has_sec_id and _is_cust_trade_desc(cust_desc):
            return "Trade"

        # Flow Cash (subscriptions, redemptions, capital calls, transfers)
        if any(kw in td for kw in ("subscription", "redemption", "transfer",
                                    "drawdown", "capital call", "inflow", "outflow")):
            return "Flow Cash"
        if any(kw in cd for kw in ("subscription", "redemption", "transfer",
                                    "drawdown", "capital call")):
            return "Flow Cash"

        # Custodian-only fallback: classify from description alone
        if not t and _is_cust_trade_desc(cust_desc):
            return "Trade"
        if not t and _is_cust_div_desc(cust_desc):
            return "Dividend/Coupon"

        return "Other"

    # ── Pre-build custodian dividend groups ──────────────────────────────────
    # Group custodian rows with div-like descriptions by (Security ID, settle_norm)
    # so their amounts can be summed and compared against Tradar div group sums.
    cust_div_groups: dict[tuple, list[int]] = {}
    for j in range(len(cust)):
        cr = cust.iloc[j]
        if not _is_cust_div_desc(cr.get("Description")):
            continue
        settle_norm = _norm_settle(cr.get("Settle Date"))
        seen: set[str] = set()
        for col in ("ISIN", "SEDOL", "Security ID"):
            sid = _safe_str(cr.get(col))
            if sid and sid not in seen:
                seen.add(sid)
                cust_div_groups.setdefault((sid, settle_norm), []).append(j)

    # ── Pre-build custodian indices ───────────────────────────────────────────
    # These replace O(n*m) full-scan loops in Sub-passes B/C and Pass 2 with O(1) lookups.
    _cust_settle_norms: list = [_norm_settle(cust.iloc[j].get("Settle Date")) for j in range(len(cust))]
    _cust_by_sec_id: dict[str, list[int]] = {}
    for j in range(len(cust)):
        cr = cust.iloc[j]
        _seen: set[str] = set()
        for _col in ("ISIN", "SEDOL", "Security ID"):
            _sid = _safe_str(cr.get(_col))
            if _sid and _sid not in _seen:
                _seen.add(_sid)
                _cust_by_sec_id.setdefault(_sid, []).append(j)
    _cust_by_settle: dict = {}
    for j, _sn in enumerate(_cust_settle_norms):
        if _sn is not None:
            _cust_by_settle.setdefault(_sn, []).append(j)

    # ── Pass 1: group Tradar div rows and match against custodian ─────────────
    trd_div_groups: dict[tuple, list[int]] = {}
    for i in range(len(trd)):
        if not _is_div(i):
            continue
        sec_id = _trd_sec_id(trd.iloc[i])
        if not sec_id:
            continue
        settle_norm = _norm_settle(trd["Settles"].iloc[i])
        trd_div_groups.setdefault((sec_id, settle_norm), []).append(i)

    for (sec_id, trd_settle_norm), trd_idxs in trd_div_groups.items():
        trd_sum = float(trd_amt.iloc[trd_idxs].sum().round(2))
        if pd.isna(trd_sum):
            continue

        paired = False

        # Sub-pass A: custodian div group with same (sec_id, settle_norm)
        cust_cands = [j for j in cust_div_groups.get((sec_id, trd_settle_norm), []) if j not in matched_cust]
        if cust_cands:
            cust_sum = float(cust_amt.iloc[cust_cands].sum().round(2))
            grp_status = "Matched" if (not pd.isna(cust_sum) and trd_sum == cust_sum) else "Break"
            for i in trd_idxs:
                matched_trd.add(i)
            for j in cust_cands:
                matched_cust.add(j)
            matched_groups.append((trd_idxs, cust_cands, grp_status, trd_sum, cust_sum))
            paired = True

        if paired:
            continue

        # Sub-pass B: any single custodian row with same sec_id + settle date
        # (catches rows whose description wasn't classified as div-like)
        # Exclude trade-type custodian rows — they must only match via Pass 2.
        for j in _cust_by_sec_id.get(sec_id, []):
            if j in matched_cust:
                continue
            if _cust_settle_norms[j] != trd_settle_norm:
                continue
            cr = cust.iloc[j]
            if _is_cust_trade_desc(cr.get("Description")):
                continue
            ca = float(cust_amt.iloc[j]) if pd.notna(cust_amt.iloc[j]) else float("nan")
            grp_status = "Matched" if (not pd.isna(ca) and trd_sum == ca) else "Break"
            for i in trd_idxs:
                matched_trd.add(i)
            matched_cust.add(j)
            matched_groups.append((trd_idxs, [j], grp_status, trd_sum, ca))
            paired = True
            break

        if paired:
            continue

        # Sub-pass C: fallback — same sec_id, same sign, settle within 20 days
        # Exclude trade-type custodian rows — they must only match via Pass 2.
        if trd_settle_norm is not None:
            for j in _cust_by_sec_id.get(sec_id, []):
                if j in matched_cust:
                    continue
                cj_sn = _cust_settle_norms[j]
                if cj_sn is None or abs((cj_sn - trd_settle_norm).days) > 20:
                    continue
                cr = cust.iloc[j]
                if _is_cust_trade_desc(cr.get("Description")):
                    continue
                ca = cust_amt.iloc[j]
                if pd.isna(ca):
                    continue
                if (trd_sum > 0) != (ca > 0):
                    continue
                for i in trd_idxs:
                    matched_trd.add(i)
                matched_cust.add(j)
                matched_groups.append((trd_idxs, [j], "Break", trd_sum, float(ca)))
                break

    # ── Pass 2: trade matching — Purchase/Sell ────────────────────────────────
    # 2a: Security trades (SEDOL/ISIN present) — match by sec_id + settle + description type
    # 2b: FX/cash trades (no SEDOL/ISIN) — match custodian "fx*" rows by settle + amount ±0.01
    for i in range(len(trd)):
        if i in matched_trd:
            continue
        if not _is_trade(i):
            continue
        tr = trd.iloc[i]
        ta = trd_amt.iloc[i]
        if pd.isna(ta) or ta == 0:
            continue
        trd_type = _safe_str(tr.get("Type"))
        sec_id = _trd_sec_id(tr)
        trd_settle_norm = _norm_settle(tr.get("Settles"))

        if sec_id:
            # 2a — security trade: sec_id + settle + description type
            for j in _cust_by_sec_id.get(sec_id, []):
                if j in matched_cust:
                    continue
                if _cust_settle_norms[j] != trd_settle_norm:
                    continue
                cr = cust.iloc[j]
                if not _trade_cust_type_ok(trd_type, _safe_str(cr.get("Description"))):
                    continue
                ca = float(cust_amt.iloc[j]) if pd.notna(cust_amt.iloc[j]) else float("nan")
                ta_f = float(ta)
                grp_status = "Matched" if (not pd.isna(ca) and ta_f == ca) else "Break"
                matched_trd.add(i)
                matched_cust.add(j)
                matched_groups.append(([i], [j], grp_status, ta_f, ca))
                break
        else:
            # 2b — FX/cash trade (no sec_id): settle date + amount within 0.01
            # Prefer exact match; accept ±0.01 rounding difference as a Break.
            best_j, best_diff = None, float("inf")
            for j in _cust_by_settle.get(trd_settle_norm, []):
                if j in matched_cust:
                    continue
                ca = cust_amt.iloc[j]
                if pd.isna(ca):
                    continue
                diff = abs(float(ta) - float(ca))
                if diff <= 0.01 and diff < best_diff:
                    best_diff = diff
                    best_j = j
                    if diff == 0:
                        break
            if best_j is not None:
                ca_f = float(cust_amt.iloc[best_j])
                ta_f = float(ta)
                grp_status = "Matched" if best_diff == 0 else "Break"
                matched_trd.add(i)
                matched_cust.add(best_j)
                matched_groups.append(([i], [best_j], grp_status, ta_f, ca_f))

    # ── Pass 3: 1-to-1 greedy by exact signed amount — O(n+m) via hash lookup ──
    # Use integer cents as key to avoid float-equality pitfalls.
    _cust_by_cents: dict[int, list[int]] = {}
    for j in range(len(cust)):
        if j in matched_cust:
            continue
        ca = cust_amt.iloc[j]
        if pd.isna(ca) or ca == 0:
            continue
        _cust_by_cents.setdefault(int(round(float(ca) * 100)), []).append(j)

    for i in range(len(trd)):
        if i in matched_trd:
            continue
        ta = trd_amt.iloc[i]
        if pd.isna(ta) or ta == 0:
            continue
        _cands = _cust_by_cents.get(int(round(float(ta) * 100)))
        if not _cands:
            continue
        while _cands and _cands[0] in matched_cust:
            _cands.pop(0)
        if not _cands:
            continue
        j = _cands.pop(0)
        matched_trd.add(i)
        matched_cust.add(j)
        matched_groups.append(([i], [j], "Matched", float(ta), float(cust_amt.iloc[j])))

    # ── Row building ──────────────────────────────────────────────────────────
    # Schema: one row per match group (or per Tradar row within a multi-Tradar group).
    # Counterparty = "Both" (matched/break pair), "Tradar" (unmatched Tradar),
    #               "Custody" (unmatched Custodian).
    # Settle date and amount kept as two separate columns per party; Security ID,
    # Direction, and Description are merged into single columns.
    rows: list[dict] = []
    _gid = 0

    for trd_idxs, cust_idxs, row_status, trd_sum, cust_sum in matched_groups:
        ci = cust_idxs[0]
        cr = cust.iloc[ci]

        try:
            _trd_s  = trd.iloc[trd_idxs[0]].get("Settles")
            _cust_s = cr.get("Settle Date")
            _settle_diff = int((pd.Timestamp(_trd_s).normalize() - pd.Timestamp(_cust_s).normalize()).days) if pd.notna(_trd_s) and pd.notna(_cust_s) else None
        except Exception:
            _settle_diff = None

        try:
            _amt_diff = round(float(trd_sum) - float(cust_sum), 2)
        except (TypeError, ValueError):
            _amt_diff = None

        try:
            _cust_disp = round(float(cust_sum), 2)
        except (TypeError, ValueError):
            _cust_disp = pd.NA

        for k, ti in enumerate(trd_idxs):
            tr   = trd.iloc[ti]
            ta   = trd_amt.iloc[ti]
            _tsid = _trd_sec_id(tr)
            _csid = _cust_sec_id(cr) if k == 0 else ""
            rows.append({
                "Counterparty":     "Both" if k == 0 else "Tradar",
                "Tradar Account":   _safe_str(tr.get("Account")),
                "Type Category":    _type_category(
                                        _safe_str(tr.get("Type")),
                                        _safe_str(tr.get("Description")),
                                        _safe_str(cr.get("Description")) if k == 0 else "",
                                        bool(_tsid),
                                        _safe_str(tr.get("Security Type")),
                                    ),
                "Type":             _safe_str(tr.get("Type")),
                "Security ID":      _tsid if _tsid else _csid,
                "Security Name":    _safe_str(cr.get("Security Name")) if k == 0 else "",
                "Tradar Settle":    tr.get("Settles"),
                "Custodian Settle": cr.get("Settle Date")  if k == 0 else pd.NaT,
                "Tradar Amount":    ta,
                "Custodian Amount": _cust_disp             if k == 0 else pd.NA,
                "Direction":        _direction(ta),
                "Description":      _safe_str(tr.get("Description")),
                "Cust Description": _safe_str(cr.get("Description")) if k == 0 else "",
                "Cust Type":        _cust_type_label(_safe_str(cr.get("Description"))) if k == 0 else "",
                "Amount Diff":      _amt_diff if k == 0 and _amt_diff != 0 else pd.NA,
                "Settle Date Diff": _settle_diff if k == 0 and _settle_diff != 0 else pd.NA,
                "Status":           row_status,
                "_gid": _gid, "_gpos": k,
            })
        _gid += 1

    # ── Tradar Only ───────────────────────────────────────────────────────────
    for i in range(len(trd)):
        if i in matched_trd:
            continue
        ta = trd_amt.iloc[i]
        if pd.isna(ta) or ta == 0:
            continue
        tr = trd.iloc[i]
        _tsid = _trd_sec_id(tr)
        rows.append({
            "Counterparty":     "Tradar",
            "Tradar Account":   str(tr.get("Account", "")),
            "Type Category":    _type_category(
                                    _safe_str(tr.get("Type")),
                                    _safe_str(tr.get("Description")),
                                    "",
                                    bool(_tsid),
                                    _safe_str(tr.get("Security Type")),
                                ),
            "Type":             str(tr.get("Type", "")),
            "Security ID":      _tsid,
            "Security Name":    "",
            "Tradar Settle":    tr.get("Settles"),
            "Custodian Settle": pd.NaT,
            "Tradar Amount":    ta,
            "Custodian Amount": pd.NA,
            "Direction":        _direction(ta),
            "Description":      str(tr.get("Description", "")),
            "Cust Description": "",
            "Cust Type":        "",
            "Amount Diff":      pd.NA,
            "Settle Date Diff": pd.NA,
            "Status":           "Tradar Only",
            "_gid": _gid, "_gpos": 0,
        })
        _gid += 1

    # ── Custodian Only ────────────────────────────────────────────────────────
    for j in range(len(cust)):
        if j in matched_cust:
            continue
        ca = cust_amt.iloc[j]
        if pd.isna(ca) or ca == 0:
            continue
        cr = cust.iloc[j]
        _cdesc = _safe_str(cr.get("Description"))
        rows.append({
            "Counterparty":     "Custody",
            "Tradar Account":   "",
            "Type Category":    _type_category("", "", _cdesc, bool(_cust_sec_id(cr))),
            "Type":             "",
            "Security ID":      _cust_sec_id(cr),
            "Security Name":    _safe_str(cr.get("Security Name")),
            "Tradar Settle":    pd.NaT,
            "Custodian Settle": cr.get("Settle Date"),
            "Tradar Amount":    pd.NA,
            "Custodian Amount": ca,
            "Direction":        _direction(ca),
            "Description":      "",
            "Cust Description": _cdesc,
            "Cust Type":        _cust_type_label(_cdesc),
            "Amount Diff":      pd.NA,
            "Settle Date Diff": pd.NA,
            "Status":           "Custodian Only",
            "_gid": _gid, "_gpos": 0,
        })
        _gid += 1

    _cols = ["Counterparty", "Tradar Account", "Type Category", "Type",
             "Security ID", "Security Name",
             "Tradar Settle", "Custodian Settle",
             "Tradar Amount", "Custodian Amount",
             "Direction",
             "Description", "Cust Description", "Cust Type",
             "Amount Diff", "Settle Date Diff", "Status"]
    if not rows:
        return pd.DataFrame(columns=_cols)

    df = pd.DataFrame(rows)
    df["Tradar Amount"]    = pd.to_numeric(df["Tradar Amount"],    errors="coerce")
    df["Custodian Amount"] = pd.to_numeric(df["Custodian Amount"], errors="coerce")
    df["Amount Diff"]      = pd.to_numeric(df["Amount Diff"],      errors="coerce")
    df["Settle Date Diff"] = pd.to_numeric(df["Settle Date Diff"], errors="coerce")
    for col in ["Tradar Settle", "Custodian Settle"]:
        df[col] = df[col].apply(
            lambda x: pd.Timestamp(x).strftime("%Y-%m-%d") if pd.notna(x) and x is not pd.NaT else ""
        )
    df["_sort"] = df["Status"].map({"Tradar Only": 0, "Custodian Only": 1, "Break": 0, "Matched": 2})
    df = (df.sort_values(["_sort", "_gid", "_gpos"])
            .drop(columns=["_sort", "_gpos"])   # keep _gid for Group ID assignment
            .reset_index(drop=True))
    return df



@st.cache_data(show_spinner=False)
def _build_all_txn_breaks(
    _break_rows: pd.DataFrame,
    _raw_tradar_settled: pd.DataFrame,
    _custody_txns: pd.DataFrame,
    version: int,
) -> pd.DataFrame:
    """Pool and reconcile transactions for every (fund, custody, ccy) break group.

    All DataFrame args use the _ prefix so they are NOT hashed.
    version is the only cache key — bumped on run_clicked so the result is
    always pre-computed once and reused for all filter/toggle interactions.
    """
    from collections import defaultdict

    groups: dict[tuple, dict] = defaultdict(lambda: {
        "trd_accts": set(), "trd_fund": "", "src_accts": set(),
        "custody": "", "dates": [],
    })

    for _, brow in _break_rows.iterrows():
        _tradar_acct = str(brow.get("Tradar_Account", "")).strip()
        if not _tradar_acct or _tradar_acct == "nan":
            _tradar_acct = str(brow.get("Tradar Accounts Used", "")).strip()
        _trd_accts = [a.strip() for a in _tradar_acct.replace(" + ", ",").split(",")
                      if a.strip() and a.strip() != "nan"]
        _raw_fund = str(brow.get("Portfolio", "")).strip()
        if not _raw_fund or _raw_fund == "nan":
            _raw_fund = str(brow.get("Mapping Fund", "")).strip()
        _trd_fund  = "" if _raw_fund == "nan" else _raw_fund
        _src_accts = [s.strip() for s in str(brow.get("Source Account ID", "")).split(",")
                      if s.strip() and s.strip() != "nan"]
        _custody   = str(brow.get("Custody", "")).strip().upper()
        _ccy       = str(brow.get("Currency Code", "")).strip()
        _fund_code = str(brow.get("Mapping Fund", "")).strip()
        _date_val  = brow.get("Date") if pd.notna(brow.get("Date", pd.NaT)) else brow.get("Display Date")
        _up_to     = pd.Timestamp(_date_val).normalize()
        key = (_fund_code, _custody, _ccy)
        g = groups[key]
        g["trd_accts"].update(_trd_accts)
        g["trd_fund"]  = _trd_fund
        g["src_accts"].update(_src_accts)
        g["custody"]   = _custody
        g["dates"].append(_up_to)

    all_txn_breaks: list[pd.DataFrame] = []
    for (fund_code, custody_key, ccy_key), g in groups.items():
        _trd_accts = list(g["trd_accts"])
        _trd_fund  = g["trd_fund"]
        _src_accts = list(g["src_accts"])
        min_date   = min(g["dates"])
        max_date   = max(g["dates"])

        _trd_mask = (
            (_raw_tradar_settled["Account"].str.strip().isin(_trd_accts)
             if _trd_accts else pd.Series(False, index=_raw_tradar_settled.index))
            & _raw_tradar_settled["CCY"].str.strip().eq(ccy_key)
            & _raw_tradar_settled["Settles"].notna()
            & (_raw_tradar_settled["Settles"] >= min_date)
            & (_raw_tradar_settled["Settles"] <= max_date)
        )
        if _trd_fund and "Fund" in _raw_tradar_settled.columns:
            _trd_mask &= _raw_tradar_settled["Fund"].str.strip().eq(_trd_fund)
        _trd_cols = [c for c in ["Account", "Settles", "Cashflow", "Type", "Security Type", "Sedol", "Isin", "Description"]
                     if c in _raw_tradar_settled.columns]
        _trd_pool = _raw_tradar_settled[_trd_mask][_trd_cols].reset_index(drop=True)

        _cust_pool = pd.DataFrame()
        if not _custody_txns.empty and _src_accts:
            _cm = (
                _custody_txns["Account ID"].str.strip().isin(_src_accts)
                & _custody_txns["Currency Code"].str.strip().eq(ccy_key)
                & _custody_txns["COB Date"].notna()
                & (_custody_txns["COB Date"] >= min_date)
                & (_custody_txns["COB Date"] <= max_date)
            )
            if custody_key in ("BNP", "BNPNZ", "CITI"):
                _cm &= _custody_txns["Custody"].eq(custody_key)
            _cust_pool = _custody_txns[_cm].copy()

        if not _cust_pool.empty or not _trd_pool.empty:
            _rec = _reconcile_txns(_trd_pool, _cust_pool)
            _rec.insert(0, "Fund",    fund_code)
            _rec.insert(1, "Custody", custody_key)
            _rec.insert(2, "CCY",     ccy_key)
            all_txn_breaks.append(_rec)

    if not all_txn_breaks:
        return pd.DataFrame()
    return pd.concat(all_txn_breaks, ignore_index=True)


def show_waterfall(
    breaks_df: pd.DataFrame,
    raw_tradar_settled: pd.DataFrame,
    custody_txns: pd.DataFrame | None = None,
) -> None:
    if raw_tradar_settled is None or raw_tradar_settled.empty:
        st.info("Transaction detail not available.")
        return

    break_rows = breaks_df[breaks_df["Status"].astype(str).eq("Break")].copy() if "Status" in breaks_df.columns else breaks_df.copy()
    if break_rows.empty:
        st.info("No breaks in current view. Filter Status = 'Break' above to see rows here.")
        return

    def _label(row):
        date = row.get("Display Date", "")
        fund = row.get("Mapping Fund", "")
        acct = row.get("Tradar_Account", "")
        ccy  = row.get("Currency Code", "")
        var  = pd.to_numeric(row.get("Variance", 0), errors="coerce") or 0
        return f"{date}  |  {fund}  |  {acct}  |  {ccy}  |  Var: {var:+,.2f}"

    labels = break_rows.apply(_label, axis=1).tolist()
    _ALL = -1
    sel_idx = st.selectbox(
        "Select break to analyse",
        [_ALL] + list(range(len(labels))),
        format_func=lambda i: "— All breaks —" if i == _ALL else labels[i],
    )

    if sel_idx == _ALL:
        # Use pre-computed result if available (populated on run_clicked).
        # Fall back to computing on the fly so the view still works if the
        # user hasn't re-run reconciliation since the last code deploy.
        full_result = st.session_state.get("all_txn_breaks_precomputed")
        if full_result is None:
            _cust_df2 = custody_txns if custody_txns is not None else pd.DataFrame()
            full_result = _build_all_txn_breaks(
                break_rows, raw_tradar_settled, _cust_df2,
                st.session_state.get("rec_version", 0),
            )

        if not full_result.empty:
            # Filter to groups that appear in the current filtered break_rows view
            _key_col = (
                break_rows["Mapping Fund"].astype(str).str.strip()
                + "|" + break_rows["Custody"].astype(str).str.strip().str.upper()
                + "|" + break_rows["Currency Code"].astype(str).str.strip()
            )
            relevant_keys = set(_key_col)
            _fr_key = (
                full_result["Fund"].astype(str).str.strip()
                + "|" + full_result["Custody"].astype(str).str.strip()
                + "|" + full_result["CCY"].astype(str).str.strip()
            )
            combined = full_result[_fr_key.isin(relevant_keys)].reset_index(drop=True)
        else:
            combined = pd.DataFrame()

        if not combined.empty:
            n_breaks  = int((combined["Status"] != "Matched").sum())
            n_matched = int((combined["Status"] == "Matched").sum())
            n_groups  = combined[["Fund", "Custody", "CCY"]].drop_duplicates().shape[0]
            st.caption(f"{n_breaks} break(s)  ·  {n_matched} matched  —  {n_groups} fund/custody/ccy group(s)")
            _render_txn_table(combined, "all_breaks")
        else:
            st.info("No transaction breaks found — run reconciliation first, or no custodian transaction data loaded.")
        return

    row = break_rows.iloc[sel_idx]
    _raw_tradar_acct = str(row.get("Tradar_Account", "")).strip()
    if not _raw_tradar_acct or _raw_tradar_acct == "nan":
        # Multi-account rows (e.g. BBUS special rec) store accounts in Tradar Accounts Used
        _raw_tradar_acct = str(row.get("Tradar Accounts Used", "")).strip()
    tradar_account   = _raw_tradar_acct
    trd_accounts     = [a.strip() for a in tradar_account.replace(" + ", ",").split(",") if a.strip() and a.strip() != "nan"]
    # Fund name used in the Tradar file — needed to avoid cross-fund account-name collisions.
    # Portfolio stores the actual Tradar file fund name (e.g. "BBUS-ETF"); Mapping Fund is the
    # shorter code (e.g. "BBUS") and may not match the Tradar header. Use Portfolio first.
    _raw_trd_fund = str(row.get("Portfolio", "")).strip()
    if not _raw_trd_fund or _raw_trd_fund == "nan":
        _raw_trd_fund = str(row.get("Mapping Fund", "")).strip()
    tradar_fund = "" if _raw_trd_fund == "nan" else _raw_trd_fund
    # Source Account ID may be comma-separated for aggregated rows (e.g. BBUS)
    _raw_src_acct    = str(row.get("Source Account ID", "")).strip()
    source_accounts  = [s.strip() for s in _raw_src_acct.split(",") if s.strip() and s.strip() != "nan"]
    source_account   = source_accounts[0] if len(source_accounts) == 1 else _raw_src_acct  # for display only
    custody          = str(row.get("Custody", "")).strip().upper()
    ccy              = str(row.get("Currency Code", "")).strip()
    date_val         = row.get("Date") if pd.notna(row.get("Date", pd.NaT)) else row.get("Display Date")
    up_to_date       = pd.Timestamp(date_val).normalize()
    trd_opening      = pd.to_numeric(row.get("Tradar Opening Balance", 0), errors="coerce") or 0.0
    src_opening      = pd.to_numeric(row.get("Source Opening Balance"), errors="coerce")
    source_balance   = pd.to_numeric(row.get("Source Ledger Balance"), errors="coerce")
    tradar_balance   = pd.to_numeric(row.get("Tradar Balance"), errors="coerce")
    variance         = pd.to_numeric(row.get("Variance"), errors="coerce")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tradar Opening", f"{trd_opening:,.2f} {ccy}")
    c2.metric("Tradar Balance", f"{tradar_balance:,.2f} {ccy}" if pd.notna(tradar_balance) else "N/A")
    c3.metric("Source Balance", f"{source_balance:,.2f} {ccy}" if pd.notna(source_balance) else "N/A")
    c4.metric("Variance", f"{variance:,.2f} {ccy}" if pd.notna(variance) else "N/A")

    color_scale = alt.Scale(
        domain=["positive", "negative", "total"],
        range=["#43A047", "#E53935", "#1565C0"],
    )

    # ── Tradar waterfall ──────────────────────────────────────────────────
    trd_wf = _waterfall_chart_data(raw_tradar_settled, tradar_account, ccy, up_to_date, trd_opening, tradar_fund=tradar_fund)
    trd_chart = _make_waterfall_chart(trd_wf, f"Tradar — {tradar_account}", ccy, color_scale)

    # ── Custodian waterfall ───────────────────────────────────────────────
    cust_chart = None
    cust_txn_detail = pd.DataFrame()
    has_cust_txns = custody_txns is not None and not custody_txns.empty

    if has_cust_txns and source_accounts:
        cust_mask = (
            custody_txns["Account ID"].str.strip().isin(source_accounts)
            & custody_txns["Currency Code"].str.strip().eq(ccy)
            & custody_txns["COB Date"].notna()
            & (custody_txns["COB Date"] == up_to_date)
        )
        if custody in ("BNP", "BNPNZ", "CITI"):
            cust_mask &= custody_txns["Custody"].eq(custody)
        cust_txn_detail = custody_txns[cust_mask].copy()

        # BNP carries opening balance in the transaction file itself; fall back to rec_detail
        file_opening = pd.to_numeric(
            cust_txn_detail["Opening Balance"].dropna().iloc[0] if "Opening Balance" in cust_txn_detail.columns and not cust_txn_detail["Opening Balance"].dropna().empty else None,
            errors="coerce",
        )
        cust_opening = float(file_opening) if pd.notna(file_opening) else (float(src_opening) if pd.notna(src_opening) else 0.0)
        cust_rows: list[dict] = []
        cust_rows.append({
            "Order": 0, "Label": "Opening\nBalance",
            "From": 0.0, "To": cust_opening,
            "BarType": "total", "Amount": cust_opening,
            "Settle Date": "", "Description": "Source Opening Balance",
        })
        running = cust_opening
        for i, (_, txn) in enumerate(cust_txn_detail.sort_values("Settle Date").iterrows(), start=1):
            amt = pd.to_numeric(txn.get("Amount", 0), errors="coerce")
            if pd.isna(amt) or amt == 0:
                continue
            settle_str = txn["Settle Date"].strftime("%m/%d") if pd.notna(txn.get("Settle Date")) else ""
            cust_rows.append({
                "Order": i, "Label": settle_str + (f"\n{str(txn.get('Description',''))[:20]}" if txn.get("Description") else ""),
                "From": running, "To": running + amt,
                "BarType": "positive" if amt > 0 else "negative",
                "Amount": amt,
                "Settle Date": settle_str,
                "Description": str(txn.get("Description", "")),
            })
            running += amt
        cust_rows.append({
            "Order": len(cust_rows), "Label": "Source\nBalance",
            "From": 0.0, "To": float(source_balance) if pd.notna(source_balance) else running,
            "BarType": "total", "Amount": float(source_balance) if pd.notna(source_balance) else running,
            "Settle Date": "", "Description": "Source Ledger Balance",
        })
        cust_wf = pd.DataFrame(cust_rows)
        cust_wf["Start"] = cust_wf[["From", "To"]].min(axis=1)
        cust_wf["End"]   = cust_wf[["From", "To"]].max(axis=1)
        cust_chart = _make_waterfall_chart(cust_wf, f"Custodian ({custody}) — {', '.join(source_accounts)}", ccy, color_scale)

    # ── Layout ────────────────────────────────────────────────────────────
    if cust_chart is not None:
        left, right = st.columns(2)
        with left:
            st.altair_chart(trd_chart, use_container_width=True)
        with right:
            st.altair_chart(cust_chart, use_container_width=True)
    else:
        st.altair_chart(trd_chart, use_container_width=True)
        if has_cust_txns and not source_accounts:
            st.caption("Custodian waterfall unavailable: no Source Account ID on this row.")
        elif not has_cust_txns:
            st.caption("Custodian transaction files not loaded. Run reconciliation after sync to see custodian waterfall.")

    # ── Transaction Reconciliation (single-day, Tradar vs Custodian) ─────
    # Include Account in Tradar side so user can see which sub-account each cashflow belongs to
    trd_day_cols = [c for c in ["Account", "Settles", "Cashflow", "Type", "Security Type", "Sedol", "Isin", "Description"] if c in raw_tradar_settled.columns]

    def _trd_for_date(settle_date: pd.Timestamp) -> pd.DataFrame:
        _mask = (
            (raw_tradar_settled["Account"].str.strip().isin(trd_accounts) if trd_accounts else pd.Series(False, index=raw_tradar_settled.index))
            & raw_tradar_settled["CCY"].str.strip().eq(ccy)
            & raw_tradar_settled["Settles"].notna()
            & (raw_tradar_settled["Settles"] == settle_date)
        )
        if tradar_fund and "Fund" in raw_tradar_settled.columns:
            _mask &= raw_tradar_settled["Fund"].str.strip().eq(tradar_fund)
        return raw_tradar_settled[_mask][trd_day_cols].sort_values(["Account", "Settles"]).reset_index(drop=True)

    trd_day = _trd_for_date(up_to_date)

    st.markdown(f"**Transaction Rec — {up_to_date.date()} · {ccy}**")

    if not cust_txn_detail.empty:
        rec_df = _reconcile_txns(trd_day, cust_txn_detail)
        # Prepend Fund/Custody/CCY so Group IDs are consistent with the "All breaks" view
        _fund_label = str(row.get("Mapping Fund", "")).strip()
        rec_df.insert(0, "Fund",    _fund_label)
        rec_df.insert(1, "Custody", custody)
        rec_df.insert(2, "CCY",     ccy)

        n_trd_only  = int((rec_df["Status"] == "Tradar Only").sum())
        n_cust_only = int((rec_df["Status"] == "Custodian Only").sum())
        n_div_break = int((rec_df["Status"] == "Break").sum())
        n_matched   = int((rec_df["Status"] == "Matched").sum())
        n_breaks    = n_trd_only + n_cust_only + n_div_break

        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric("Tradar txns", len(trd_day))
        col_b.metric("Custodian txns", len(cust_txn_detail))
        col_c.metric("Matched", n_matched)
        col_d.metric("Breaks", n_breaks, help=f"{n_trd_only} Tradar Only  ·  {n_cust_only} Custodian Only  ·  {n_div_break} Div Amount Break")

        _render_txn_table(rec_df, "txn_rec")
    else:
        if not trd_day.empty:
            st.caption(f"No custodian transactions on {up_to_date.date()} — {len(trd_day)} Tradar transaction(s) below")
            _trd_disp = trd_day.copy()
            if "Settles" in _trd_disp.columns:
                _trd_disp["Settles"] = _trd_disp["Settles"].apply(
                    lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else ""
                )
            if "Cashflow" in _trd_disp.columns:
                _trd_disp["Cashflow"] = pd.to_numeric(_trd_disp["Cashflow"], errors="coerce").apply(
                    lambda x: f"{x:,.2f}" if pd.notna(x) else ""
                )
            st.dataframe(_trd_disp, use_container_width=True, hide_index=True)
        else:
            if has_cust_txns:
                st.info(f"No new transactions on {up_to_date.date()} on either side — this break is a carry-forward of a prior position.")
            else:
                st.info("Custodian transaction files not yet loaded.")


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
                    y=alt.Y("Abs Variance (AUD):Q", title="Abs Variance (AUD)", axis=alt.Axis(format=",.0f")),
                    tooltip=[
                        alt.Tooltip("Display Date:O"),
                        alt.Tooltip("Abs Variance (AUD):Q", format=",.2f"),
                    ],
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


def show_detail_page(df: pd.DataFrame, raw_tradar_settled: pd.DataFrame | None = None, custody_txns: pd.DataFrame | None = None) -> None:
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

    # Waterfall drill-down — only shown when there are breaks in the current filtered view
    if raw_tradar_settled is not None and not raw_tradar_settled.empty:
        breaks_in_view = filtered[filtered["Status"].astype(str).eq("Break")] if "Status" in filtered.columns else pd.DataFrame()
        if not breaks_in_view.empty:
            st.markdown("---")
            if st.toggle("Break Waterfall Analysis", key="wf_toggle"):
                show_waterfall(breaks_in_view, raw_tradar_settled, custody_txns)


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
        st.session_state["rec_version"] = st.session_state.get("rec_version", 0) + 1
        with st.spinner("Running reconciliation..."):
            result = _run_reconciliation_cached(str(run_date), int(lookback_days))
            combined_df = prepare_combined_detail(result)
            st.session_state["result"] = result
            st.session_state["combined_df"] = combined_df

        # Pre-compute all transaction breaks so filter/toggle changes are instant.
        # Stored in session_state; show_waterfall reads and filters it cheaply.
        with st.spinner("Pre-computing transaction breaks..."):
            _all_break_rows = (
                combined_df[combined_df["Status"].astype(str).eq("Break")].copy()
                if combined_df is not None and not combined_df.empty and "Status" in combined_df.columns
                else pd.DataFrame()
            )
            _raw_df  = result.get("raw_tradar_settled", pd.DataFrame())
            _cust_txns = result.get("custody_txns")
            _cust_df = _cust_txns if _cust_txns is not None else pd.DataFrame()
            _ver     = st.session_state["rec_version"]
            st.session_state["all_txn_breaks_precomputed"] = _build_all_txn_breaks(
                _all_break_rows, _raw_df, _cust_df, _ver
            )

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
        raw_tradar   = result.get("raw_tradar_settled", pd.DataFrame()) if result else pd.DataFrame()
        custody_txns = result.get("custody_txns", pd.DataFrame()) if result else pd.DataFrame()
        show_detail_page(combined_df, raw_tradar, custody_txns)


if __name__ == "__main__":
    main()