from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def load_reference_data(
    reference_dir: Path, run_date: pd.Timestamp
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (fx_df, nav_df). Either may be an empty DataFrame if files are missing."""
    return _load_fx(reference_dir, run_date), _load_unit_prices(reference_dir)


def _load_fx(reference_dir: Path, run_date: pd.Timestamp) -> pd.DataFrame:
    files = sorted(reference_dir.glob("*_ffx_points.csv"))
    if not files:
        return pd.DataFrame()

    best = None
    for f in files:
        try:
            file_date = pd.to_datetime(f.name.split("_")[0])
            if file_date <= run_date:
                best = f
        except Exception:
            pass
    if best is None:
        best = files[-1]

    fx = pd.read_csv(best, dtype=str)
    fx.columns = fx.columns.str.strip()
    fx["LC"] = fx["LC"].str.strip().str.upper()
    fx["BASE_FX"] = pd.to_numeric(fx["BASE_FX"], errors="coerce")
    return (
        fx[["LC", "BASE_FX"]]
        .dropna(subset=["BASE_FX"])
        .drop_duplicates(subset="LC")
        .reset_index(drop=True)
    )


def _load_unit_prices(reference_dir: Path) -> pd.DataFrame:
    frames = []
    for f in reference_dir.glob("Unit Prices_*.csv"):
        df = pd.read_csv(f, dtype=str)
        df.columns = df.columns.str.strip()
        if "Exchange Code" in df.columns:
            df = df.drop(columns=["Exchange Code"])
        frames.append(df)
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined["Fund"] = combined["Fund"].str.strip().str.upper()
    combined["Date"] = pd.to_datetime(combined["Date"], errors="coerce").dt.normalize()
    combined["Total Assets"] = pd.to_numeric(combined["Total Assets"], errors="coerce")
    return (
        combined[["Fund", "Date", "Total Assets"]]
        .dropna(subset=["Fund", "Date", "Total Assets"])
        .drop_duplicates(subset=["Fund", "Date"])
        .reset_index(drop=True)
    )


def compute_materiality(
    rec_detail: pd.DataFrame,
    fx_df: pd.DataFrame,
    nav_df: pd.DataFrame,
) -> pd.DataFrame:
    """Add Local Variance, BPS Impact, and Materiality columns to rec_detail."""
    df = rec_detail.copy()

    if fx_df.empty or nav_df.empty:
        df["Local Variance"] = pd.NA
        df["BPS Impact"] = pd.NA
        df["Materiality"] = ""
        return df

    # BASE_FX[LC] = AUDUSD / LC_in_USD  →  dividing by BASE_FX converts LC → AUD
    # AUD itself is the base so BASE_FX[AUD] = 1.0
    base_fx: dict[str, float] = fx_df.set_index("LC")["BASE_FX"].to_dict()
    base_fx["AUD"] = 1.0

    # Local currency: NZD for NZ funds, AUD for everything else
    is_nz = (
        df.get("Fund Type", pd.Series("", index=df.index))
        .fillna("")
        .str.upper()
        .str.contains("NZ")
    )
    local_ccy = is_nz.map({True: "NZD", False: "AUD"})

    src_base_fx = df["Currency Code"].str.strip().str.upper().map(base_fx)
    local_base_fx = local_ccy.map(base_fx)

    # Local Variance = Variance × (local_BASE_FX / src_BASE_FX)
    variance = pd.to_numeric(df["Variance"], errors="coerce")
    df["Local Variance"] = variance * local_base_fx / src_base_fx

    # Join Total Assets on Mapping Fund + Date (exact match, then fall back to latest)
    fund_key = df["Mapping Fund"].str.strip().str.upper()
    _date_col = "Date" if "Date" in df.columns else "Target Date"
    date_key = pd.to_datetime(df[_date_col], errors="coerce").dt.normalize()

    exact = pd.merge(
        pd.DataFrame({"_fund": fund_key, "_date": date_key}),
        nav_df.rename(columns={"Fund": "_fund", "Date": "_date", "Total Assets": "_ta"}),
        on=["_fund", "_date"],
        how="left",
    )
    total_assets = exact["_ta"].values.copy().astype(float)

    # Fallback: for unmatched rows use the most recent available NAV for that fund
    missing = np.isnan(total_assets)
    if missing.any():
        latest_nav = (
            nav_df.sort_values("Date")
            .groupby("Fund")["Total Assets"]
            .last()
            .reset_index()
            .rename(columns={"Fund": "_fund", "Total Assets": "_ta_fb"})
        )
        fallback = pd.merge(
            pd.DataFrame({"_fund": fund_key[missing].values}),
            latest_nav,
            on="_fund",
            how="left",
        )
        total_assets[missing] = fallback["_ta_fb"].values

    # BPS Impact = (Local Variance / Total Assets) × 10 000
    local_var = df["Local Variance"].values.astype(float)
    with np.errstate(invalid="ignore", divide="ignore"):
        bps = np.where(total_assets > 0, (local_var / total_assets) * 10_000, np.nan)
    df["BPS Impact"] = bps

    # Materiality classification (matches Excel formula):
    # HIGH     |bps| > 1
    # MODERATE |bps| >= 0.5
    # LOW      (0.25 <= |bps| < 0.5)  OR  (|bps| < 0.25 AND |local_var| > 5 000)
    # ""       otherwise or BPS unavailable
    bps_abs = np.abs(bps)
    local_abs = np.abs(local_var)

    df["Materiality"] = np.select(
        [
            ~np.isnan(bps_abs) & (bps_abs > 1),
            ~np.isnan(bps_abs) & (bps_abs >= 0.5),
            ~np.isnan(bps_abs) & ((bps_abs >= 0.25) | ((bps_abs < 0.25) & (local_abs > 5_000))),
        ],
        ["HIGH", "MODERATE", "LOW"],
        default="",
    )

    return df
