from __future__ import annotations

import threading
import warnings
from pathlib import Path

import pandas as pd

_EXCEL_MAX_ROWS = 1_048_576


def _safe_to_excel(df: pd.DataFrame, writer, sheet_name: str, **kwargs) -> None:
    if len(df) > _EXCEL_MAX_ROWS - 1:  # -1 for header row
        warnings.warn(
            f"Sheet '{sheet_name}' has {len(df):,} rows — truncated to {_EXCEL_MAX_ROWS - 1:,} (Excel limit).",
            stacklevel=2,
        )
        df = df.iloc[: _EXCEL_MAX_ROWS - 1]
    df.to_excel(writer, sheet_name=sheet_name, **kwargs)


def export_excel_output(
    output_path: str | Path,
    summary_df: pd.DataFrame,
    rec_detail_df: pd.DataFrame,
    unmapped_df: pd.DataFrame,
    out_of_scope_df: pd.DataFrame,
    raw_tradar_settled_df: pd.DataFrame,
    hi_summary_df: pd.DataFrame | None = None,
    hi_rec_detail_df: pd.DataFrame | None = None,
    hi_unmapped_df: pd.DataFrame | None = None,
    bbus_pdf_summary_df: pd.DataFrame | None = None,
    bbus_pdf_detail_df: pd.DataFrame | None = None,
) -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _write() -> None:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            _safe_to_excel(summary_df, writer, sheet_name='Summary', index=False)
            _safe_to_excel(rec_detail_df, writer, sheet_name='Rec_Detail', index=False)
            _safe_to_excel(unmapped_df, writer, sheet_name='Unmapped_Custody', index=False)
            _safe_to_excel(out_of_scope_df, writer, sheet_name='Out_Of_Scope_Custody', index=False)
            _safe_to_excel(raw_tradar_settled_df, writer, sheet_name='Raw_Tradar_Settled', index=False)

            if hi_summary_df is not None:
                _safe_to_excel(hi_summary_df, writer, sheet_name='HI_Summary', index=False)
            if hi_rec_detail_df is not None:
                _safe_to_excel(hi_rec_detail_df, writer, sheet_name='HI_Rec_Detail', index=False)
            if hi_unmapped_df is not None:
                _safe_to_excel(hi_unmapped_df, writer, sheet_name='HI_Unmapped', index=False)
            if bbus_pdf_summary_df is not None:
                _safe_to_excel(bbus_pdf_summary_df, writer, sheet_name='BBUS_BNP_Adjustment', index=False)
            if bbus_pdf_detail_df is not None:
                _safe_to_excel(bbus_pdf_detail_df, writer, sheet_name='BBUS_BNP_Rec_Detail', index=False)

    threading.Thread(target=_write, daemon=True).start()
