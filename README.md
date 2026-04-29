# Cash reconciliation overlay

This package reconciles daily cash balances from multiple custody feeds to Tradar cash balances.

## Supported custody feeds
- Citi
- BNP
- BNP_NZ

All custody feeds are standardized into one common schema before reconciliation.

## Folder structure
```text
data/
  input/
    citi/
    bnp/
    bnp_nz/
    tradar/
    mapping/
  output/
    rec_results/
src/cash_rec/
  config/
```

## Run
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
python cash_reconciliation.py --run-date 2026-04-14 --lookback-days 60
```

Or:
```bash
cash-rec --run-date 2026-04-14 --lookback-days 60
```

## How file discovery works
For each enabled source, the script picks the latest modified file in the configured folder that matches the configured filename pattern.

It does **not** use the date in the filename for selection.

## Business rules implemented
- Tradar:
  - remove first title row
  - use second row as header
  - fill down Fund, Account, CCY from context rows
  - use `Balance`, not `Cumulative Cashflow`
  - use only `RBC-CS`
  - strict COB logic: last settled row where `Settles <= COB date`
  - if none settled yet, use opening balance
  - fill missing business days across the requested lookback window
- Citi:
  - rec on `Ledger Balance`
  - date = `Close of Business Date`
  - keep latest row by `Balance Update Timestamp`
- BNP:
  - date = `AsOfDate`
  - balance = last `SettleDateBalanceLocal` by `AsOfDate + AccountCode + CurrencyCode`
- BNP_NZ:
  - date = `Contractual Settlement Date`
  - balance = last `Closing Balance` by `Contractual Settlement Date + Account ID + Account Base Currency Code`
- Mapping:
  - only rows with a mapped `Fund` are in scope
  - custody-specific matching:
    - Citi uses `Citi Account ID`
    - BNP uses `BNP Account ID`
    - BNP_NZ uses `BNP NZ Account ID`

## Expected mapping file columns
The loader accepts common aliases, but the cleanest setup is:
- `Fund`
- `Portfolio`
- `Currency Code`
- `Custody`
- `Citi Account ID`
- `BNP Account ID`
- `BNP NZ Account ID`

Custody values are normalized to upper case. Recommended values:
- `CITI`
- `BNP`
- `BNP_NZ`

## Output tabs
- `Summary`
- `Rec_Detail`
- `Unmapped_Custody`
- `Out_Of_Scope_Custody`
- `Raw_Tradar_Settled`

## Local config override
Copy:
```bash
cp src/cash_rec/config/config.local.yaml.example src/cash_rec/config/config.local.yaml
```
Then edit local paths or patterns without touching shared defaults.
