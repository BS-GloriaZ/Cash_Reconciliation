from pathlib import Path

import pytest

from cash_rec.config import load_config
from cash_rec.data_clean import load_and_clean_bnp, load_and_clean_bnp_nz

CFG = load_config()
DATA_DIR = Path('/mnt/data')
BNP = DATA_DIR / '20260410164516.99X.GPBCash.csv'
BNP_NZ = DATA_DIR / '20260413_BNPNZ_Custody_Bal_cash.csv'


@pytest.mark.skipif(not BNP.exists(), reason='BNP sample file not present')
def test_bnp_loader_standardizes_sample() -> None:
    df = load_and_clean_bnp(BNP, CFG)
    assert {'Date', 'Custody', 'Source Account ID', 'Currency Code', 'Source Ledger Balance'}.issubset(df.columns)
    assert (df['Custody'] == 'BNP').all()


@pytest.mark.skipif(not BNP_NZ.exists(), reason='BNP_NZ sample file not present')
def test_bnp_nz_loader_standardizes_sample() -> None:
    df = load_and_clean_bnp_nz(BNP_NZ, CFG)
    assert {'Date', 'Custody', 'Source Account ID', 'Currency Code', 'Source Ledger Balance'}.issubset(df.columns)
    assert (df['Custody'] == 'BNP_NZ').all()
