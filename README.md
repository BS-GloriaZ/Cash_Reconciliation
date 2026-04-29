# RecX — Cash Reconciliation Pipeline

RecX reconciles daily cash balances from multiple custody feeds against Tradar system of record balances. It runs as a Streamlit web app and also exposes a Python/CLI interface for scripted use.

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Daily Workflow](#daily-workflow)
3. [Data Sources](#data-sources)
4. [Pipeline Internals](#pipeline-internals)
5. [Reconciliation Logic](#reconciliation-logic)
6. [Special Reconciliations](#special-reconciliations)
7. [Materiality Calculation](#materiality-calculation)
8. [Streamlit App](#streamlit-app)
9. [Configuration](#configuration)
10. [Project Structure](#project-structure)
11. [Setup and Installation](#setup-and-installation)
12. [Output](#output)

---

## Architecture Overview

```
W: Network Share (FTP delivery)
        │
   sync_inputs.ps1 / sync_inputs.py          ← Step 1: sync to local disk
        │
   data/input/  (local cache)
        │
   pipeline.py                                ← Step 2: run reconciliation
   ├── data_clean.py       (load & normalise each source)
   ├── reconciliation.py   (match custody → Tradar, compute variances)
   ├── materiality.py      (FX conversion, BPS impact, materiality bucket)
   └── data_output.py      (write Excel)
        │
   data/output/rec_results/cash_rec_YYYYMMDD_HHMMSS.xlsx
        │
   app.py (Streamlit)                         ← Step 3: review in UI
   ├── Dashboard  (variance trend chart, fund-type breakdown, summary table)
   └── Detailed Rec (filterable row-level break table with comments)
```

---

## Daily Workflow

### Step 1 — Sync input files from network share

Run **once each morning** after overnight FTP delivery.

**Windows PowerShell (recommended for Windows/WSL users):**
```powershell
.\sync_inputs.ps1           # incremental — copies only new files
.\sync_inputs.ps1 -Full     # full resync of the last 90 calendar days
```

**Python (WSL / Linux):**
```bash
python sync_inputs.py           # incremental
python sync_inputs.py --full    # full resync
```

The sync copies files from the mapped W: drive folders to `data/input/<source>/`. Sources that deliver one settlement-date file per day (Citi HI, BNP NZ) accumulate a history of files; single-delivery sources (Citi, BNP, Tradar, BNP PDF) keep only the latest.

### Step 2 — Run reconciliation

Launch the Streamlit app (or use the CLI) and click **Run Reconciliation**.

```bash
streamlit run app.py
```

Or via CLI:
```bash
cash-rec --run-date 2026-04-28 --lookback-days 60
```

### Step 3 — Review results in the app

- **Dashboard** — variance trend chart (AUD-converted), variance by fund type, summary table (breaks vs matched per custody/date).
- **Detailed Rec** — full row-level reconciliation with filters (date, fund, custody, currency, Tradar account, type, status, materiality). Users can add comments against individual breaks.

---

## Data Sources

| Source | Local folder | File pattern | Delivery |
|--------|-------------|--------------|---------|
| Tradar | `data/input/tradar/` | `*Cash Flow since One Month ago - all funds.csv` | One file per day (date-prefixed) |
| Citi | `data/input/citi/` | `DOD_CASH_BALANCES_*.csv` | One file per day |
| Citi HI (High Interest) | `data/input/citi_hi/` | `BSFFTMOUT_Positions_All_*.CSV` | Two files per day: regular (`_YYMMDD.CSV`) and next-morning AM (`_AM_YYMMDD.CSV`) |
| BNP | `data/input/bnp/` | `*GPBCash*.csv` | One file per day |
| BNP NZ | `data/input/bnp_nz/` | `*Bal_cash*.csv` | One file per day |
| BNP Margin PDF | `data/input/bnp_margin_pdf/` | `*DAILY_STAT.pdf` | One file per day |
| Mapping | `data/input/mapping/` | `mapping.xlsx` | Updated manually as needed |
| Reference (FX / NAV) | `data/input/reference/` | Various CSV | Updated manually / periodically |

### Tradar file format

The Tradar report is a cash-flow ledger with one row per transaction. Key parsing steps:
- Strip the title row; row 2 becomes the header.
- Fill down `Fund`, `Account`, `CCY` from context rows (they only appear on the first row of each group).
- Each row has a `Settles` date and a running `Balance`.
- The COB balance for a given date is the last `Balance` where `Settles <= COB date`. If no settled row exists yet, the opening balance is used.
- Tradar data is filtered to only `Account` values listed in the mapping (via `allowed_accounts`).

### Citi HI file format

Each CSV contains position-level data (SKAC + account + currency). Two variants are delivered daily:
- Regular file (`BSFFTMOUT_Positions_All_YYMMDD.CSV`) — settlement date = date in filename.
- AM file (`BSFFTMOUT_Positions_All_AM_YYMMDD.CSV`) — next-morning re-run of the prior day.

Deduplication: for each settlement date, only the **latest file by mtime** is used (AM file supersedes the regular file if received later).

---

## Pipeline Internals

`pipeline.py: run_cash_reconciliation()` orchestrates these steps:

1. **Resolve file paths** — `file_discovery.resolve_input_paths()` finds the latest matching file for each single-file source. BNP, BNP NZ, and Citi HI use directory-based loading (all files within lookback window).

2. **Parse Tradar date range** — reads report start/end from the file title row.

3. **Load & clean Tradar** — `clean_tradar_file()` normalises the ledger; `build_tradar_daily_balances()` projects a COB balance for every (Fund, CCY, Account) × business date in the lookback window using `merge_asof`.

4. **Load & clean custody sources** — each source loader (`load_and_clean_citi`, `load_and_clean_bnp`, etc.) normalises to a common schema:

   | Column | Description |
   |--------|-------------|
   | `Date` | Settlement / COB date |
   | `Custody` | `CITI` / `BNP` / `BNPNZ` |
   | `Source Account ID` | Normalised account code |
   | `Source Account Name` | Account description |
   | `Currency Code` | ISO currency |
   | `Source Ledger Balance` | Balance used for reconciliation |
   | `Source Available Balance` | Available balance (Citi only) |
   | `Source Opening Balance` | Opening balance (Citi / BNP NZ) |
   | `Source Timestamp` | File process timestamp |

5. **Filter by lookback window** — `_prepare_source()` keeps only rows whose `Date` falls within `[run_date − lookback_days, run_date]` (business days).

6. **Match to mapping** — `_combine_matches()` joins each custody's source data to the mapping file. BNP and Citi join on `Account ID`; BNP NZ derives a fund code from the account string. Rows without a mapping match go to `unmapped`. Rows matched but with `In Scope = False` go to `out_of_scope`.

7. **Merge with Tradar daily** — in-scope rows are joined to `tradar_daily_df` on `(Date, Fund/Portfolio, CCY, Tradar_Account)`. Supports both exact and wildcard (`*`) Tradar account rules.

8. **Compute variance** — `Variance = Source Ledger Balance − Tradar Balance`. `Status = Matched` if `round(Abs Variance, 2) == 0`, else `Break`.

9. **Materiality** — `compute_materiality()` converts variance to AUD (`Local Variance`) using daily FX rates, computes BPS impact against NAV, and assigns a materiality bucket (`HIGH / MODERATE / LOW`).

10. **Special reconciliations** — BBUS BNP total and Citi HI are run separately (see below).

11. **Export** — `export_excel_output()` writes all result frames to a timestamped Excel file. Sheets with > 1,048,575 rows are truncated with a warning.

12. **Manifest** — `data/output/rec_results/manifest.json` records the latest output filename for use by external consumers.

---

## Reconciliation Logic

### Mapping file (`mapping.xlsx`)

All sheets are concatenated. Column names are resolved via configurable aliases (see `defaults.yaml: mapping.column_aliases`). Key columns:

| Canonical name | Purpose |
|---------------|---------|
| `Fund` | Short fund code (e.g. `GEAR`, `BBUS`) — used for `In Scope` determination |
| `Portfolio` | Tradar fund name (e.g. `GEAR-ETF`) — used as `Tradar Match Fund` (takes priority over `Fund`) |
| `Account ID` | Custody account code |
| `Currency Code` | Optional currency filter on mapping row |
| `Custody` | `CITI` / `BNP` / `BNPNZ` |
| `Tradar_Account` | Tradar account name; supports wildcard suffix `*` for prefix matching |
| `SKAC` | Citi HI SKAC code (used for HI matching only) |
| `Fund Type` | Fund classification label |
| `In Scope` | `True` if `Fund` is non-empty |

### Tradar Match Fund

`fallback_fund_order: [Portfolio, Fund]` — `Portfolio` is used as the join key to `tradar_daily_df['Fund']` when populated; otherwise `Fund` is used. This allows the mapping to hold both the short fund name (for display) and the full Tradar fund name (for joining).

### Variance status

```
Status = "Matched"  if  round(abs(Source Ledger Balance − Tradar Balance), 2) == 0
Status = "Break"    otherwise
```

---

## Special Reconciliations

### BBUS BNP Total (`reconcile_bbus_bnp_total_balance`)

BBUS holds cash at BNP across multiple Tradar accounts (`RBC-CS`, `FUT USD`, `BTMU 11am`) plus a PDF-reported initial margin adjustment. The standard row-level reconciliation does not apply; instead:

1. BNP source rows for BBUS/USD are summed.
2. The initial margin figure is extracted from the daily `*DAILY_STAT.pdf` via `parse_bnp_margin_pdf()`.
3. `BNP Total = BNP Source + Initial Margin Adjustment`.
4. Tradar balance = sum of all three Tradar accounts filtered to USD.
5. A single variance row is produced per date.

BBUS/BNP rows are removed from the normal reconciliation and replaced by the BBUS Special rows in the UI.

### Citi High-Interest (`reconcile_high_interest_balances`)

Citi HI files carry positions at SKAC + account level rather than a simple ledger balance. The HI reconciliation:
1. Loads all `BSFFTMOUT_Positions_All_*.CSV` files (deduplicating to one file per settlement date).
2. Matches on `(Source SKAC, Source Account ID)` against the mapping.
3. Merges with Tradar daily on `(Target Date, Fund/Portfolio, Tradar_Account)`.
4. Produces a separate summary and detail output (`hi_summary`, `hi_detail`).

---

## Materiality Calculation

`materiality.py: compute_materiality()`

1. **FX conversion** — loads `data/input/reference/*ffx_points.csv` for daily FX rates. Variance in non-AUD currency is converted: `Local Variance = Variance / FX Rate`. NZD funds use a separate NZD NAV file.

2. **BPS impact** — `BPS = abs(Local Variance) / NAV × 10,000`. NAV loaded from `data/input/reference/Unit Prices_*.csv`.

3. **Materiality bucket**:
   - `HIGH` — BPS ≥ 5 or Local Variance ≥ $500,000
   - `MODERATE` — BPS ≥ 1 or Local Variance ≥ $100,000
   - `LOW` — otherwise

---

## Streamlit App

`app.py`

| Control | Description |
|---------|-------------|
| Run Date | COB date to reconcile. Defaults to today. |
| Lookback Days | Number of business days of history to include (default 60). |
| Sync Inputs | Copies new files from W: drive; clears cache if new files arrived. |
| Run Reconciliation | Executes the pipeline; result is cached by `(run_date, lookback_days)`. |
| Clear Cache | Forces a full re-run on next click of Run Reconciliation. |

### Pages

**Dashboard**
- Variance by Date trend chart — uses `Local Variance` (AUD-converted).
- Variance by Fund Type bar chart — latest date only.
- Summary table — break/match counts and total AUD variance per (date, custody), latest 5 dates per custody.

**Detailed Rec**
- Combined view of Normal, High Interest, and BNP Special reconciliation rows.
- Sidebar filters: Date (single or range), Fund, Fund Type, Custody, Currency, Tradar Account, Type, Status, Materiality, Abs Variance threshold, free-text search.
- Editable Comment column — comments persist in session state keyed by a break signature (date + fund + custody + currency + account).

---

## Configuration

### Two-layer config

| File | Purpose | In version control |
|------|---------|-------------------|
| `src/cash_rec/config/defaults.yaml` | Source patterns, column names, reconciliation rules, output column order | ✅ Yes |
| `src/cash_rec/config/config.local.yaml` | Network share paths (`ftp:` section), any local overrides | ❌ No (gitignored) |

### Setting up local config

```bash
cp src/cash_rec/config/config.local.yaml.example src/cash_rec/config/config.local.yaml
```

Edit `config.local.yaml` to set the W: drive mount paths for each FTP source. Example:

```yaml
ftp:
  tradar:       /mnt/w/Ops/Controls/Cash/Tradar Reports
  citi:         /mnt/w/Ops/Citi/Rec Files/Archive
  citi_hi:      /mnt/w/Ops/Citi/Rec Files
  bnp:          /mnt/w/Ops/BNP/Rec Files
  bnp_nz:       /mnt/w/Ops/BNP NZ/Rec Files/Recon cleaned
  bnp_margin_pdf: /mnt/w/Ops/BNP/Rec Files
```

---

## Project Structure

```
Cash-Reconciliation_V2/
├── app.py                          # Streamlit UI
├── cash_reconciliation.py          # CLI entry point
├── sync_inputs.py                  # Python sync script (WSL/Linux)
├── sync_inputs.ps1                 # PowerShell sync script (Windows)
├── requirements.txt
├── pyproject.toml
│
├── src/cash_rec/
│   ├── pipeline.py                 # Orchestration: load → reconcile → export
│   ├── data_clean.py               # Per-source loaders and normalisation
│   ├── data_input.py               # Low-level file readers (CSV, Excel, PDF)
│   ├── data_output.py              # Excel export with row-limit guard
│   ├── reconciliation.py           # Core rec logic: matching, variance, summary
│   ├── materiality.py              # FX conversion, BPS, materiality buckets
│   ├── file_discovery.py           # File/directory path resolution
│   ├── exceptions.py               # CashRecError
│   ├── utils.py                    # Text normalisation helpers
│   └── config/
│       ├── defaults.yaml           # All default settings
│       ├── config.local.yaml       # Machine-specific overrides (gitignored)
│       └── config.local.yaml.example
│
├── data/
│   ├── input/
│   │   ├── tradar/                 # Tradar cash flow reports
│   │   ├── citi/                   # Citi DOD balance files
│   │   ├── citi_hi/                # Citi high-interest position files
│   │   ├── bnp/                    # BNP GPBCash balance files
│   │   ├── bnp_nz/                 # BNP NZ balance files
│   │   ├── bnp_margin_pdf/         # BNP daily stat PDFs (BBUS margin)
│   │   ├── mapping/                # mapping.xlsx
│   │   └── reference/              # FX rates, NAV / unit price files
│   └── output/
│       └── rec_results/            # Timestamped Excel outputs + manifest.json
│
└── tests/
    └── test_loaders.py
```

---

## Setup and Installation

**Prerequisites:** Python 3.11+, WSL2 (for Windows users running sync against W: drive)

```bash
# 1. Create virtual environment
python -m venv .venv
source .venv/bin/activate          # Linux/WSL
# .venv\Scripts\activate           # Windows PowerShell

# 2. Install dependencies
pip install -r requirements.txt
pip install -e .

# 3. Set up local config
cp src/cash_rec/config/config.local.yaml.example src/cash_rec/config/config.local.yaml
# Edit config.local.yaml with your network share paths

# 4. Initial data sync (copies last 90 days from W: drive)
python sync_inputs.py --full
# or on Windows: .\sync_inputs.ps1 -Full

# 5. Launch the app
streamlit run app.py
```

---

## Output

### Excel file (`cash_rec_YYYYMMDD_HHMMSS.xlsx`)

| Sheet | Contents |
|-------|----------|
| `Summary` | Break/match counts and total abs variance per (date, custody, status) |
| `Rec_Detail` | Row-level normal reconciliation with variance, status, materiality |
| `HI_Summary` | High-interest reconciliation summary |
| `HI_Rec_Detail` | High-interest row-level detail |
| `BBUS_PDF_Summary` | BBUS special reconciliation summary |
| `BBUS_PDF_Detail` | BBUS special reconciliation detail |
| `Unmapped_Custody` | Source rows with no mapping match |
| `Out_Of_Scope_Custody` | Mapped rows where `In Scope = False` |
| `Raw_Tradar_Settled` | Filtered Tradar settled transactions used in the run |

Sheets exceeding 1,048,575 rows are truncated with a warning in the first row.

### `manifest.json`

Written alongside the Excel file. Records the filename of the most recent output for downstream consumers:

```json
{ "latest_file": "cash_rec_20260428_144813.xlsx" }
```
