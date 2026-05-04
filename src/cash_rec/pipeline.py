from __future__ import annotations

import time
from pathlib import Path
from typing import Any
import json
import pandas as pd

from cash_rec.config import load_config
from cash_rec.data_clean import (
    clean_tradar_file,
    load_and_clean_bnp,
    load_and_clean_bnp_nz,
    load_and_clean_citi,
    load_and_clean_citi_hi_balance,
    load_bnp_nz_transactions,
    load_bnp_transactions,
    load_citi_transactions,
    load_mapping,
    parse_bnp_margin_pdf,
    parse_title_date_range,
)
from cash_rec.data_input import read_pdf_text
from cash_rec.data_output import export_excel_output
from cash_rec.file_discovery import (
    default_output_path,
    resolve_input_paths,
    resolve_source_dir,
    validate_path_isolation,
)
from cash_rec.materiality import compute_materiality, load_reference_data
from cash_rec.reconciliation import (
    build_tradar_daily_balances,
    reconcile_balances,
    reconcile_bbus_bnp_total_balance,
    reconcile_high_interest_balances,
)


def run_cash_reconciliation(
    run_date: str,
    lookback_days: int | None = None,
    tradar: str | None = None,
    citi: str | None = None,
    citi_hi: str | None = None,
    bnp: str | None = None,
    bnp_nz: str | None = None,
    mapping: str | None = None,
    output: str | None = None,
    config_path: str | None = None,
    bnp_margin_pdf: str | None = None,
) -> dict[str, Any]:
    _t: dict[str, float] = {}
    _t0 = time.perf_counter()

    config = load_config(config_path)
    effective_lookback = int(lookback_days or config["reconciliation"]["default_lookback_days"])
    run_dt = pd.Timestamp(run_date).normalize()

    file_paths = resolve_input_paths(
        config,
        {
            "tradar": tradar,
            "citi": citi,
            "citi_hi": citi_hi,
            "bnp": bnp,
            "bnp_nz": bnp_nz,
            "mapping": mapping,
            "bnp_margin_pdf": bnp_margin_pdf,
        },
    )
    output_path = Path(output) if output else default_output_path(config["paths"]["output_root"])

    # Guard: ensure output directory does not overlap with any source folder
    validate_path_isolation(file_paths, output_path)
    source_warnings: list[str] = []

    report_start, report_end = parse_title_date_range(file_paths["tradar"])
    tradar_clean = clean_tradar_file(file_paths["tradar"])
    mapping_raw, mapping_accounts = load_mapping(file_paths["mapping"], config)
    _t["load_inputs"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    # Base allowed Tradar accounts from mapping
    allowed_accounts = (
        mapping_accounts["Tradar_Account"].dropna().astype(str).str.strip().unique().tolist()
        if "Tradar_Account" in mapping_accounts.columns
        else []
    )

    # Add special reconciliation Tradar accounts from config
    special_cfg = config.get("special_reconciliation", {})
    for _, rule in special_cfg.items():
        for acct in rule.get("tradar_accounts", []):
            acct = str(acct).strip()
            if acct and acct not in allowed_accounts:
                allowed_accounts.append(acct)

    tradar_daily, raw_tradar_settled = build_tradar_daily_balances(
        tradar_clean,
        run_date=run_dt,
        lookback_days=effective_lookback,
        allowed_accounts=allowed_accounts if allowed_accounts else None,
    )
    _t["build_tradar_daily"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    # citi_hi uses a directory (all matching CSVs), resolved via config path override support
    citi_hi_dir = resolve_source_dir(config, "citi_hi")

    custody_frames = []
    if "citi" in file_paths:
        custody_frames.append(load_and_clean_citi(file_paths["citi"], config))
    if citi_hi_dir.is_dir() and any(citi_hi_dir.glob("*.CSV")):
        custody_frames.append(load_and_clean_citi_hi_balance(
            citi_hi_dir, config, run_date=run_dt, lookback_days=effective_lookback
        ))

    bnp_dir = resolve_source_dir(config, "bnp")
    if bnp_dir.is_dir():
        bnp_data = load_and_clean_bnp(bnp_dir, config)
        if not bnp_data.empty:
            custody_frames.append(bnp_data)

    bnp_nz_dir = resolve_source_dir(config, "bnp_nz")
    if bnp_nz_dir.is_dir():
        bnp_nz_data = load_and_clean_bnp_nz(bnp_nz_dir, config)
        if not bnp_nz_data.empty:
            custody_frames.append(bnp_nz_data)

    source_balances = pd.concat(custody_frames, ignore_index=True) if custody_frames else pd.DataFrame()
    _t["load_custody"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    summary, rec_detail, unmapped, out_of_scope = reconcile_balances(
        source_balances_df=source_balances,
        tradar_daily_df=tradar_daily,
        mapping_accounts_df=mapping_accounts,
        run_date=run_dt,
        lookback_days=effective_lookback,
        config=config,
    )
    _t["reconcile_normal"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    reference_dir = Path(config["paths"]["input_root"]) / config.get("reference", {}).get("subdir", "reference")
    fx_df, nav_df = load_reference_data(reference_dir, run_dt)
    rec_detail = compute_materiality(rec_detail, fx_df, nav_df)
    _t["materiality"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    bbus_pdf_summary = pd.DataFrame()
    bbus_pdf_detail = pd.DataFrame()

    if "bnp_margin_pdf" in file_paths:
        pdf_text = read_pdf_text(file_paths["bnp_margin_pdf"])
        pdf_adjustment = parse_bnp_margin_pdf(pdf_text)

        bbus_pdf_summary, bbus_pdf_detail = reconcile_bbus_bnp_total_balance(
            bnp_source_df=source_balances,
            tradar_daily_df=tradar_daily,
            mapping_accounts_df=mapping_accounts,
            run_date=run_dt,
            pdf_adjustment=pdf_adjustment,
            config=config,
        )
        if not bbus_pdf_detail.empty:
            bbus_pdf_detail = compute_materiality(bbus_pdf_detail, fx_df, nav_df)
    _t["reconcile_bbus"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    hi_summary = pd.DataFrame()
    hi_detail = pd.DataFrame()
    hi_unmapped = pd.DataFrame()

    if citi_hi_dir.is_dir() and any(citi_hi_dir.glob("*.CSV")):
        hi_summary, hi_detail, hi_unmapped = reconcile_high_interest_balances(
            source_balances_df=source_balances,
            tradar_daily_df=tradar_daily,
            mapping_accounts_df=mapping_accounts,
            run_date=run_dt,
            config=config,
        )
        if not hi_detail.empty and "Target Date" in hi_detail.columns:
            hi_detail["Date"] = hi_detail["Target Date"]
        hi_detail = compute_materiality(hi_detail, fx_df, nav_df)
    _t["reconcile_hi"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    export_excel_output(
        output_path,
        summary,
        rec_detail,
        unmapped,
        out_of_scope,
        raw_tradar_settled,
        hi_summary_df=hi_summary,
        hi_rec_detail_df=hi_detail,
        hi_unmapped_df=hi_unmapped,
        bbus_pdf_summary_df=bbus_pdf_summary,
        bbus_pdf_detail_df=bbus_pdf_detail,
    )
    _t["export_excel"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    txn_frames = []
    citi_txn_dir = resolve_source_dir(config, "citi_txns")
    if citi_txn_dir.is_dir():
        txn_frames.append(load_citi_transactions(citi_txn_dir, config))
    bnp_txn_dir = resolve_source_dir(config, "bnp_txns")
    if bnp_txn_dir.is_dir():
        txn_frames.append(load_bnp_transactions(bnp_txn_dir, config))
    bnp_nz_txn_dir = resolve_source_dir(config, "bnp_nz_txns")
    if bnp_nz_txn_dir.is_dir():
        txn_frames.append(load_bnp_nz_transactions(bnp_nz_txn_dir, config))
    custody_txns = pd.concat([f for f in txn_frames if not f.empty], ignore_index=True) if txn_frames else pd.DataFrame()
    _t["load_txns"] = time.perf_counter() - _t0; _t0 = time.perf_counter()

    manifest_path = output_path.parent / "manifest.json"
    manifest = {
        "latest_file": output_path.name,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    _t["total"] = sum(_t.values())
    return {
        "file_paths": file_paths,
        "output_path": output_path,
        "report_start": report_start,
        "report_end": report_end,
        "run_date": run_dt,
        "lookback_days": effective_lookback,
        "summary": summary,
        "rec_detail": rec_detail,
        "unmapped": unmapped,
        "out_of_scope": out_of_scope,
        "hi_summary": hi_summary,
        "hi_detail": hi_detail,
        "hi_unmapped": hi_unmapped,
        "bbus_pdf_summary": bbus_pdf_summary,
        "bbus_pdf_detail": bbus_pdf_detail,
        "mapping_raw": mapping_raw,
        "raw_tradar_settled": raw_tradar_settled,
        "custody_txns": custody_txns,
        "source_warnings": source_warnings,
        "timings": _t,
    }
