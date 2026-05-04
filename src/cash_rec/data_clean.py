from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Optional
from pypdf import PdfReader
import pandas as pd

from cash_rec.data_input import read_tabular_file
from cash_rec.exceptions import CashRecError
from cash_rec.utils import clean_text, dedupe_columns, first_matching_column, normalize_identifier, to_number

TRADAR_BASE_COLUMNS = [
    'Trade', '', 'Type', 'Amount', 'Sedol', 'Isin', '_blank_1', 'Security Type',
    'Description', 'Price', 'Date', 'Settles', 'Cashflow', 'Cumulative Cashflow', 'Balance', 'Notes',
]


def parse_title_date_range(tradar_file: str | Path) -> tuple[pd.Timestamp, pd.Timestamp]:
    tradar_file = Path(tradar_file)
    with tradar_file.open('r', encoding='utf-8-sig', errors='replace', newline='') as handle:
        first_row = next(csv.reader(handle), [])
    joined = ' '.join(clean_text(x) for x in first_row if clean_text(x))
    match = re.search(r'(\d{1,2}\s+[A-Za-z]{3}\s+\d{2})\s*-\s*(\d{1,2}\s+[A-Za-z]{3}\s+\d{2})', joined)
    if not match:
        raise CashRecError(f'Could not parse Tradar title date range from first row: {joined!r}')
    start = pd.to_datetime(match.group(1), format='%d %b %y').normalize()
    end = pd.to_datetime(match.group(2), format='%d %b %y').normalize()
    return start, end


def clean_tradar_file(tradar_file: str | Path) -> pd.DataFrame:
    tradar_file = Path(tradar_file)
    with tradar_file.open('r', encoding='utf-8-sig', errors='replace', newline='') as handle:
        rows = list(csv.reader(handle))
    if len(rows) < 2:
        raise CashRecError('Tradar file does not contain enough rows.')

    raw_header = rows[1]
    header = []
    for idx, col in enumerate(raw_header):
        name = clean_text(col)
        if not name:
            name = TRADAR_BASE_COLUMNS[idx] if idx < len(TRADAR_BASE_COLUMNS) else f'Unnamed_{idx}'
        header.append(name)
    header = dedupe_columns(header)

    fund: Optional[str] = None
    account: Optional[str] = None
    ccy: Optional[str] = None
    cleaned_records: list[dict[str, object]] = []

    for row in rows[2:]:
        row = list(row) + [''] * max(0, len(header) - len(row))
        row = row[: len(header)]
        first = clean_text(row[0]) if row else ''

        if first.startswith('Fund:'):
            fund = first.split(':', 1)[1].strip()
            continue
        if first.startswith('Account:'):
            account = first.split(':', 1)[1].strip()
            continue
        if first.startswith('Ccy:'):
            ccy = first.split(':', 1)[1].strip()
            continue

        record = {header[i]: clean_text(row[i]) for i in range(len(header))}
        record = {'Fund': fund or '', 'Account': account or '', 'CCY': ccy or '', **record}
        meaningful = [v for k, v in record.items() if k not in {'Fund', 'Account', 'CCY'}]
        if not any(clean_text(v) for v in meaningful):
            continue
        cleaned_records.append(record)

    if not cleaned_records:
        raise CashRecError('No usable Tradar rows were found after cleaning.')

    df = pd.DataFrame(cleaned_records)
    for col in ['Fund', 'Account', 'CCY', 'Type']:
        if col in df.columns:
            df[col] = df[col].map(clean_text)
    df['Date'] = pd.to_datetime(df.get('Date', ''), format='%d %b %Y', errors='coerce').dt.normalize()
    df['Settles'] = pd.to_datetime(df.get('Settles', ''), format='%d %b %Y', errors='coerce').dt.normalize()
    for col in ['Amount', 'Cashflow', 'Cumulative Cashflow', 'Balance', 'Price']:
        if col in df.columns:
            df[col] = df[col].map(to_number)
    df['Is Opening Balance'] = df.get('Type', '').astype(str).str.casefold().eq('opening balance')
    return df


def load_and_clean_citi(citi_file: str | Path, config: dict) -> pd.DataFrame:
    df = read_tabular_file(citi_file, dtype=str)
    df.columns = dedupe_columns(df.columns)
    required = config['columns']['citi_required']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise CashRecError(f'Citi file is missing required columns: {missing}')

    citi = df[required].copy()
    citi['Account ID'] = citi['Account ID'].map(normalize_identifier)
    for col in ['Account Name', 'Currency Code']:
        citi[col] = citi[col].map(clean_text)
    citi['Date'] = pd.to_datetime(citi['Close of Business Date'], errors='coerce').dt.normalize()
    citi['As of Date Parsed'] = pd.to_datetime(citi['As of Date'], errors='coerce')
    citi['Balance Update Timestamp Parsed'] = pd.to_datetime(citi['Balance Update Timestamp'], errors='coerce')
    for col in ['Ledger Balance', 'Available Balance', 'Opening Balance']:
        citi[col] = citi[col].map(to_number)
    citi = citi.sort_values(['Date', 'Account ID', 'Currency Code', 'Balance Update Timestamp Parsed', 'As of Date Parsed'])
    citi = citi.drop_duplicates(subset=['Date', 'Account ID', 'Currency Code'], keep='last').reset_index(drop=True)
    return pd.DataFrame({
        'Date': citi['Date'],
        'Custody': config['mapping']['custody_values']['citi'],
        'Source Account ID': citi['Account ID'],
        'Source Account Name': citi['Account Name'],
        'Currency Code': citi['Currency Code'].str.upper(),
        'Source Ledger Balance': citi['Ledger Balance'],
        'Source Available Balance': citi['Available Balance'],
        'Source Opening Balance': citi['Opening Balance'],
        'Source Timestamp': citi['Balance Update Timestamp Parsed'],
    })

def _hi_settlement_date(file_path: Path) -> pd.Timestamp | None:
    """Derive settlement date from HI filename: YYMMDD in stem → that date."""
    match = re.search(r'(\d{6})', file_path.stem)
    if not match:
        return None
    try:
        return pd.Timestamp(f"20{match.group(1)[:2]}-{match.group(1)[2:4]}-{match.group(1)[4:6]}").normalize()
    except Exception:
        return None


def load_and_clean_citi_hi_balance(
    hi_path: str | Path,
    config: dict,
    run_date: pd.Timestamp | None = None,
    lookback_days: int | None = None,
) -> pd.DataFrame:
    hi_path = Path(hi_path)
    files = sorted(hi_path.glob("*.CSV"), key=lambda f: f.stem) if hi_path.is_dir() else [hi_path]

    if run_date is not None and lookback_days is not None:
        cutoff = pd.bdate_range(end=run_date, periods=lookback_days + 1)[0].normalize()
        files = [f for f in files if (s := _hi_settlement_date(f)) is not None and s >= cutoff]

    # Per settlement date, keep only the latest-arrived file (highest mtime).
    # Citi delivers both a same-day file and an AM file the next morning for the
    # same date (e.g. BSFFTMOUT_Positions_All_260428.CSV and
    # BSFFTMOUT_Positions_All_AM_260428.CSV). Loading both doubles every position.
    _date_to_file: dict = {}
    for f in files:
        s = _hi_settlement_date(f)
        if s is None:
            continue
        if s not in _date_to_file or f.stat().st_mtime > _date_to_file[s].stat().st_mtime:
            _date_to_file[s] = f
    files = sorted(_date_to_file.values(), key=lambda f: f.stem)

    frames = []
    for f in files:
        if f.stat().st_size == 0:
            continue
        settlement_date = _hi_settlement_date(f)
        try:
            df = read_tabular_file(f, dtype=str)
        except Exception:
            continue
        if df.empty:
            continue
        df.columns = dedupe_columns(df.columns)

        required = ['Account ID', 'Sec ID', 'Available Position']
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise CashRecError(f'High-interest Citi file {f.name} is missing required columns: {missing}')

        hi = df[required].copy()
        hi['Account ID'] = hi['Account ID'].map(normalize_identifier)
        hi['Sec ID'] = hi['Sec ID'].map(normalize_identifier)
        hi['Available Position'] = hi['Available Position'].map(to_number)
        hi = hi.dropna(subset=['Available Position']).reset_index(drop=True)

        frames.append(pd.DataFrame({
            'Date': settlement_date,
            'Custody': config['mapping']['custody_values']['citi'],
            'Source Type': 'CITI_HI',
            'Source SKAC': hi['Account ID'],
            'Source Account ID': hi['Sec ID'],
            'Source Account Name': pd.NA,
            'Currency Code': pd.NA,
            'Source Ledger Balance': hi['Available Position'],
            'Source Available Balance': hi['Available Position'],
            'Source Opening Balance': pd.NA,
            'Source Timestamp': pd.NaT,
        }))

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def load_and_clean_bnp(bnp_source: str | Path, config: dict) -> pd.DataFrame:
    source = Path(bnp_source)
    if source.is_dir():
        pattern = config['sources']['bnp']['filename_pattern']
        files = sorted(source.glob(pattern), key=lambda p: p.name)
    else:
        files = [source]

    required = config['columns']['bnp_required']
    raw_frames = []
    for f in files:
        try:
            df = read_tabular_file(f, dtype=str)
            df.columns = dedupe_columns(df.columns)
            if any(c not in df.columns for c in required):
                continue
            raw_frames.append(df[required].copy())
        except Exception:
            pass

    if not raw_frames:
        return pd.DataFrame()

    bnp = pd.concat(raw_frames, ignore_index=True)
    bnp['AsOfDate'] = pd.to_datetime(bnp['AsOfDate'].astype(str), format='%Y%m%d', errors='coerce').dt.normalize()
    bnp['ProcessDate'] = pd.to_datetime(bnp['ProcessDate'].astype(str), format='%Y%m%d %H:%M:%S', errors='coerce')
    bnp['AccountCode'] = bnp['AccountCode'].map(normalize_identifier)
    for col in ['AccountName', 'CurrencyCode']:
        bnp[col] = bnp[col].map(clean_text)
    bnp['SettleDateBalanceLocal'] = bnp['SettleDateBalanceLocal'].map(to_number)
    bnp['_row_order'] = range(len(bnp))
    bnp = bnp.sort_values(['AsOfDate', 'AccountCode', 'CurrencyCode', 'ProcessDate', '_row_order'])
    bnp = bnp.groupby(['AsOfDate', 'AccountCode', 'CurrencyCode'], as_index=False).tail(1).reset_index(drop=True)

    return pd.DataFrame({
        'Date': bnp['AsOfDate'],
        'Custody': config['mapping']['custody_values']['bnp'],
        'Source Account ID': bnp['AccountCode'],
        'Source Account Name': bnp['AccountName'],
        'Currency Code': bnp['CurrencyCode'].str.upper(),
        'Source Ledger Balance': bnp['SettleDateBalanceLocal'],
        'Source Available Balance': pd.NA,
        'Source Opening Balance': pd.NA,
        'Source Timestamp': bnp['ProcessDate'],
    })


def load_and_clean_bnp_nz(bnp_nz_source: str | Path, config: dict) -> pd.DataFrame:
    source = Path(bnp_nz_source)
    if source.is_dir():
        pattern = config['sources']['bnp_nz']['filename_pattern']
        files = sorted(source.glob(pattern), key=lambda p: p.name)
    else:
        files = [source]

    required = config['columns']['bnp_nz_required']
    raw_frames = []
    for f in files:
        try:
            df = read_tabular_file(f, dtype=str)
            df.columns = dedupe_columns(df.columns)
            if any(c not in df.columns for c in required):
                continue
            raw_frames.append(df[required].copy())
        except Exception:
            pass

    if not raw_frames:
        return pd.DataFrame()

    nz = pd.concat(raw_frames, ignore_index=True)
    nz['Contractual Settlement Date'] = pd.to_datetime(nz['Contractual Settlement Date'], errors='coerce').dt.normalize()
    nz['Created Timestamp'] = pd.to_datetime(nz['Created Timestamp'], errors='coerce')
    nz['Account ID'] = nz['Account ID'].map(normalize_identifier)
    for col in ['Account Name', 'Account Base Currency Code']:
        nz[col] = nz[col].map(clean_text)
    for col in ['Opening Balance', 'Closing Balance']:
        nz[col] = nz[col].map(to_number)
    nz['_row_order'] = range(len(nz))
    nz = nz.sort_values(['Contractual Settlement Date', 'Account ID', 'Account Base Currency Code', 'Created Timestamp', '_row_order'])
    nz = nz.groupby(['Contractual Settlement Date', 'Account ID', 'Account Base Currency Code'], as_index=False).tail(1).reset_index(drop=True)

    return pd.DataFrame({
        'Date': nz['Contractual Settlement Date'],
        'Custody': config['mapping']['custody_values']['bnp_nz'],
        'Source Account ID': nz['Account ID'],
        'Source Account Name': nz['Account Name'],
        'Currency Code': nz['Account Base Currency Code'].str.upper(),
        'Source Ledger Balance': nz['Closing Balance'],
        'Source Available Balance': pd.NA,
        'Source Opening Balance': nz['Opening Balance'],
        'Source Timestamp': nz['Created Timestamp'],
    })


def _normalize_mapping_columns(mapping_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    aliases = config['mapping']['column_aliases']
    resolved = mapping_df.copy()
    rename_map: dict[str, str] = {}
    fund_type: ['Fund Type', 'FundType']
    for target, candidates in aliases.items():
        col = first_matching_column(resolved, candidates, required=False)
        if col is not None:
            rename_map[col] = target
    resolved = resolved.rename(columns=rename_map)
    return resolved

def load_mapping(mapping_file: str | Path, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_sheets: dict = pd.read_excel(mapping_file, dtype=str, sheet_name=None)
    mapping_df = pd.concat(all_sheets.values(), ignore_index=True)
    mapping_df.columns = dedupe_columns(mapping_df.columns)
    mapping_df = _normalize_mapping_columns(mapping_df, config)

    required_core = ['fund', 'portfolio', 'account_id', 'currency_code', 'custody']
    missing = [c for c in required_core if c not in mapping_df.columns]
    if missing:
        raise CashRecError(f'Mapping file is missing required normalized columns: {missing}')

    if 'fund_type' not in mapping_df.columns:
        mapping_df['fund_type'] = ''
    if 'skac' not in mapping_df.columns:
        mapping_df['skac'] = ''
    if 'tradar_account' not in mapping_df.columns:
        mapping_df['tradar_account'] = ''

    for col in required_core + ['fund_type', 'skac', 'tradar_account']:
        mapping_df[col] = mapping_df[col].map(clean_text)

    mapping_df['account_id'] = mapping_df['account_id'].map(normalize_identifier)
    mapping_df['skac'] = mapping_df['skac'].map(normalize_identifier)
    mapping_df['fund'] = mapping_df['fund'].str.upper()
    mapping_df['currency_code'] = mapping_df['currency_code'].str.upper()
    mapping_df['custody'] = mapping_df['custody'].str.upper()

    mapping_df['In Scope'] = mapping_df['fund'].fillna('').ne('')

    normalized = mapping_df.rename(columns={
        'fund': 'Fund',
        'portfolio': 'Portfolio',
        'skac': 'SKAC',
        'account_id': 'Account ID',
        'currency_code': 'Currency Code',
        'custody': 'Custody',
        'tradar_account': 'Tradar_Account',
        'fund_type': 'Fund Type',
    }).copy()

    keep_cols = [
        'Fund',
        'Portfolio',
        'SKAC',
        'Account ID',
        'Currency Code',
        'Custody',
        'Tradar_Account',
        'Fund Type',
        'In Scope',
    ]
    normalized = normalized[[c for c in keep_cols if c in normalized.columns]].drop_duplicates().reset_index(drop=True)

    return mapping_df, normalized

def parse_bnp_margin_pdf(pdf_text: str) -> dict:
    import re

    def _to_float(raw: str) -> float:
        return float(raw.replace(",", "").strip())

    text = re.sub(r"\s+", " ", pdf_text)

    m_im = re.search(
        r"Inititial Margin Requirement\s+([\-0-9,]+\.\d{2})\s+USD",
        text,
        flags=re.IGNORECASE,
    )
    if not m_im:
        raise CashRecError("Could not find Initial Margin Requirement in BNP margin PDF.")
    initial_margin = _to_float(m_im.group(1))

    m_vm = re.search(
        r"Variation Margin\s+([\-0-9,]+\.\d{2})\s+USD\s+([\-0-9,]+\.\d{2})\s+USD\s+([\-0-9,]+\.\d{2})\s+USD\s+([\-0-9,]+\.\d{2})\s+USD\s+([\-0-9,]+\.\d{2})\s+USD",
        text,
        flags=re.IGNORECASE,
    )
    if not m_vm:
        raise CashRecError("Could not find Variation Margin row in BNP margin PDF.")

    prev_unrealized_trading = _to_float(m_vm.group(1))
    prev_unrealized_hedging = _to_float(m_vm.group(2))
    unrealized_trading = _to_float(m_vm.group(3))
    unrealized_hedging = _to_float(m_vm.group(4))
    mark_to_market = _to_float(m_vm.group(5))

    opposite_unrealized_trading = -unrealized_trading
    total_adjustment = initial_margin + opposite_unrealized_trading

    return {
        "Initial Margin Requirement USD": initial_margin,
        "Previous Unrealized Trading USD": prev_unrealized_trading,
        "Previous Unrealized Hedging USD": prev_unrealized_hedging,
        "Unrealized Trading USD": unrealized_trading,
        "Unrealized Hedging USD": unrealized_hedging,
        "Mark to Market USD": mark_to_market,
        "Opposite Unrealized Trading USD": opposite_unrealized_trading,
        "Total PDF Adjustment USD": total_adjustment,
    }


def read_pdf_text(pdf_file: str | Path) -> str:
    reader = PdfReader(str(pdf_file))
    pages = []
    for page in reader.pages:
        pages.append(page.extract_text() or "")
    return "\n".join(pages)


# ---------------------------------------------------------------------------
# Custody transaction loaders — produce a common schema:
#   Custody | Account ID | Currency Code | COB Date | Settle Date | Amount | Description
# ---------------------------------------------------------------------------

_TXN_OUT_COLS = ["Custody", "Account ID", "Currency Code", "COB Date", "Settle Date", "Amount", "Description", "Security ID", "ISIN", "SEDOL", "Security Name"]


def load_citi_transactions(source_dir: str | Path, config: dict) -> pd.DataFrame:
    """Load Citi DOD_CASH_TRANSACTIONS_V1_C30_*.csv files from a directory.

    Uses usecols so the 160-column file is not fully materialised in memory.
    Amount is pre-signed (negative = debit, positive = credit).
    """
    source = Path(source_dir)
    pattern = config.get("sources", {}).get("citi_txns", {}).get("filename_pattern", "DOD_CASH_TRANSACTIONS_V1_C30_*.csv")
    files = sorted(source.glob(pattern), key=lambda p: p.name)
    if not files:
        return pd.DataFrame(columns=_TXN_OUT_COLS)

    files = [files[-1]]  # latest file already contains full history; loading all doubles rows needlessly

    needed = {
        "Account ID", "Currency Code", "Amount",
        "Close of Business Date", "Contractual Settlement Date",
        "Transaction Description", "SEDOL", "ISIN", "Issue Description",
    }
    raw_frames = []
    for f in files:
        try:
            df = read_tabular_file(f, dtype=str, usecols=lambda c: c in needed)
            df.columns = dedupe_columns(df.columns)
            if "Account ID" not in df.columns or "Amount" not in df.columns:
                continue
            raw_frames.append(df)
        except Exception:
            pass

    if not raw_frames:
        return pd.DataFrame(columns=_TXN_OUT_COLS)

    txns = pd.concat(raw_frames, ignore_index=True)
    txns["COB Date"]    = pd.to_datetime(txns.get("Close of Business Date"),     errors="coerce").dt.normalize()
    txns["Settle Date"] = pd.to_datetime(txns.get("Contractual Settlement Date"), errors="coerce").dt.normalize()
    txns["Account ID"]  = txns["Account ID"].fillna("").astype(str).str.strip()
    txns["Currency Code"] = txns["Currency Code"].fillna("").astype(str).str.strip().str.upper()
    txns["Amount"]      = pd.to_numeric(txns["Amount"], errors="coerce")
    txns["Description"] = txns.get("Transaction Description", pd.Series("", index=txns.index)).fillna("").astype(str)
    sedol = txns.get("SEDOL", pd.Series("", index=txns.index)).fillna("").astype(str).str.strip()
    isin  = txns.get("ISIN",  pd.Series("", index=txns.index)).fillna("").astype(str).str.strip()
    txns["Security ID"]   = sedol.where(sedol.ne(""), isin)
    txns["SEDOL"]         = sedol
    txns["ISIN"]          = isin
    txns["Security Name"] = txns.get("Issue Description", pd.Series("", index=txns.index)).fillna("").astype(str)
    txns["Custody"]     = "CITI"
    return txns[[c for c in _TXN_OUT_COLS if c in txns.columns]].dropna(subset=["COB Date", "Amount"]).copy()


def load_bnp_transactions(source_dir: str | Path, config: dict) -> pd.DataFrame:
    """Load BNP *99X.CashLedgerSD.csv files from a directory.

    Account identifier is the 'ID' column (matches AccountCode / Source Account ID in mapping).
    Settlement date is 'Settle Date'. Transaction type is 'Activity'. Net Amount is pre-signed.
    """
    source = Path(source_dir)
    pattern = config.get("sources", {}).get("bnp_txns", {}).get("filename_pattern", "*99X.CashLedgerSD.csv")
    files = sorted(source.glob(pattern), key=lambda p: p.name)
    if not files:
        return pd.DataFrame(columns=_TXN_OUT_COLS)

    raw_frames = []
    for f in files:
        try:
            df = read_tabular_file(f, dtype=str)
            df.columns = dedupe_columns(df.columns)
            if "AsOfDate" not in df.columns or "Net Amount" not in df.columns:
                continue
            if df.empty:
                continue
            raw_frames.append(df)
        except Exception:
            pass

    if not raw_frames:
        return pd.DataFrame(columns=_TXN_OUT_COLS)

    txns = pd.concat(raw_frames, ignore_index=True)
    txns["COB Date"]      = pd.to_datetime(txns["AsOfDate"], errors="coerce").dt.normalize()
    # Settle Date is stored as YYYYMMDD integer string
    txns["Settle Date"]   = pd.to_datetime(txns.get("Settle Date"), format="%Y%m%d", errors="coerce").dt.normalize()
    txns["Account ID"]    = txns["ID"].fillna("").astype(str).str.strip()
    # 'Code' column holds the ISO currency code (e.g. AUD/USD); 'Currency' holds full names
    txns["Currency Code"] = txns.get("Code", pd.Series("", index=txns.index)).fillna("").astype(str).str.strip().str.upper()
    txns["Amount"]        = pd.to_numeric(txns["Net Amount"], errors="coerce")
    txns["Description"]   = txns.get("Activity", pd.Series("", index=txns.index)).fillna("").astype(str)
    sedol = txns.get("SEDOL", pd.Series("", index=txns.index)).fillna("").astype(str).str.strip()
    isin  = txns.get("ISIN",  pd.Series("", index=txns.index)).fillna("").astype(str).str.strip()
    txns["Security ID"]   = sedol.where(sedol.ne(""), isin)
    txns["SEDOL"]         = sedol
    txns["ISIN"]          = isin
    txns["Security Name"] = txns.get("Security Description", pd.Series("", index=txns.index)).fillna("").astype(str)
    txns["_opening"]      = pd.to_numeric(txns.get("Previous Local Opening Balance"), errors="coerce")
    txns["_closing"]      = pd.to_numeric(txns.get("Local Closing Balance"), errors="coerce")
    txns["Custody"]       = "BNP"

    # Balance summary rows (Amount=0, no Settle Date) carry the opening/closing balance
    # per account/currency for the day. Extract and join onto the transaction rows.
    bal_rows = txns[txns["Amount"].eq(0) | txns["Amount"].isna()].copy()
    balances = (
        bal_rows.groupby(["COB Date", "Account ID", "Currency Code"], dropna=False)
        .agg(Opening_Balance=("_opening", "first"), Closing_Balance=("_closing", "first"))
        .reset_index()
    )

    txn_rows = txns[txns["Amount"].notna() & txns["Amount"].ne(0)].copy()
    txn_rows = txn_rows.merge(balances, on=["COB Date", "Account ID", "Currency Code"], how="left")
    txn_rows = txn_rows.rename(columns={"Opening_Balance": "Opening Balance", "Closing_Balance": "Closing Balance"})

    out_cols = _TXN_OUT_COLS + ["Opening Balance", "Closing Balance"]
    return txn_rows[[c for c in out_cols if c in txn_rows.columns]].dropna(subset=["COB Date"]).reset_index(drop=True)


def load_bnp_nz_transactions(source_dir: str | Path, config: dict) -> pd.DataFrame:
    """Load BNP NZ *_BNPNZ_Custody_Cash.csv files from a directory.

    The file has many blank-named columns; we select by name after load.
    Amount is pre-signed.
    """
    source = Path(source_dir)
    pattern = config.get("sources", {}).get("bnp_nz_txns", {}).get("filename_pattern", "*_BNPNZ_Custody_Cash.csv")
    files = sorted(source.glob(pattern), key=lambda p: p.name)
    if not files:
        return pd.DataFrame(columns=_TXN_OUT_COLS)

    raw_frames = []
    for f in files:
        try:
            df = read_tabular_file(f, dtype=str)
            df.columns = dedupe_columns(df.columns)
            if "Account ID" not in df.columns or "Amount" not in df.columns:
                continue
            raw_frames.append(df)
        except Exception:
            pass

    if not raw_frames:
        return pd.DataFrame(columns=_TXN_OUT_COLS)

    txns = pd.concat(raw_frames, ignore_index=True)
    date_col            = next((c for c in ("Contractual Settlement Date", "Entry Date", "Value Date") if c in txns.columns), None)
    txns["COB Date"]    = pd.to_datetime(txns[date_col] if date_col else pd.NaT, errors="coerce").dt.normalize()
    txns["Settle Date"] = pd.to_datetime(txns.get("Value Date"), errors="coerce").dt.normalize()
    txns["Account ID"]  = txns["Account ID"].fillna("").astype(str).str.strip()
    ccy_col             = next((c for c in ("Account Base Currency Code", "Currency Code") if c in txns.columns), None)
    txns["Currency Code"] = txns[ccy_col].fillna("").astype(str).str.strip().str.upper() if ccy_col else ""
    txns["Amount"]      = pd.to_numeric(txns["Amount"], errors="coerce")
    txns["Description"] = txns.get("Cash Transaction Type", pd.Series("", index=txns.index)).fillna("").astype(str)
    sedol = txns.get("SEDOL", pd.Series("", index=txns.index)).fillna("").astype(str).str.strip()
    isin  = txns.get("ISIN",  pd.Series("", index=txns.index)).fillna("").astype(str).str.strip()
    txns["Security ID"]   = sedol.where(sedol.ne(""), isin)
    txns["SEDOL"]         = sedol
    txns["ISIN"]          = isin
    txns["Security Name"] = ""
    txns["Custody"]     = "BNPNZ"
    return txns[[c for c in _TXN_OUT_COLS if c in txns.columns]].dropna(subset=["COB Date", "Amount"]).copy()