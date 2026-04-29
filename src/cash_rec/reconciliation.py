from __future__ import annotations

import numpy as np
import pandas as pd

from cash_rec.exceptions import CashRecError
from cash_rec.utils import clean_text, normalize_identifier

def _clean_account_rule(value) -> str:
    return clean_text(value)


def _is_partial_account_rule(value) -> bool:
    return _clean_account_rule(value).endswith('*')


def _account_rule_prefix(value) -> str:
    return _clean_account_rule(value).rstrip('*').strip()


def _account_matches_rule(account_value: str, rule_value: str) -> bool:
    account = clean_text(account_value)
    rule = _clean_account_rule(rule_value)

    if rule == '':
        return False
    if rule.endswith('*'):
        return account.startswith(_account_rule_prefix(rule))
    return account == rule


def _merge_with_tradar_account_rule(
    left_df: pd.DataFrame,
    tradar_daily_df: pd.DataFrame,
    left_base_keys: list[str],
    right_base_keys: list[str],
) -> pd.DataFrame:
    left = left_df.copy()
    right = tradar_daily_df.copy()

    left['Tradar_Account'] = left['Tradar_Account'].fillna('').astype(str).str.strip()
    right['Tradar_Account'] = right['Tradar_Account'].fillna('').astype(str).str.strip()

    exact_left = left[~left['Tradar_Account'].str.endswith('*', na=False)].copy()
    partial_left = left[left['Tradar_Account'].str.endswith('*', na=False)].copy()

    out_frames = []

    if not exact_left.empty:
        exact_merge = exact_left.merge(
            right,
            how='left',
            left_on=left_base_keys + ['Tradar_Account'],
            right_on=right_base_keys + ['Tradar_Account'],
        )
        out_frames.append(exact_merge)

    if not partial_left.empty:
        partial_left['_tradar_prefix'] = partial_left['Tradar_Account'].str.rstrip('*').str.strip()

        partial_merge = partial_left.merge(
            right,
            how='left',
            left_on=left_base_keys,
            right_on=right_base_keys,
            suffixes=('', '_trd'),
        )

        partial_merge = partial_merge[
            partial_merge.apply(
                lambda r: clean_text(r.get('Tradar_Account_trd', r.get('Tradar_Account_y', ''))).startswith(
                    clean_text(r['_tradar_prefix'])
                ),
                axis=1,
            )
        ].copy()

        # normalize the matched Tradar account column name
        if 'Tradar_Account_trd' in partial_merge.columns:
            partial_merge = partial_merge.rename(columns={'Tradar_Account_trd': 'Tradar_Account_Matched'})
        elif 'Tradar_Account_y' in partial_merge.columns:
            partial_merge = partial_merge.rename(columns={'Tradar_Account_y': 'Tradar_Account_Matched'})
        else:
            partial_merge['Tradar_Account_Matched'] = pd.NA

        out_frames.append(partial_merge)

    if not out_frames:
        return pd.DataFrame()

    merged = pd.concat(out_frames, ignore_index=True, sort=False)

    if 'Tradar_Account_Matched' not in merged.columns and 'Tradar_Account' in merged.columns:
        merged['Tradar_Account_Matched'] = merged['Tradar_Account']

    return merged

def get_business_dates(run_date: pd.Timestamp, lookback_days: int) -> pd.DatetimeIndex:
    run_date = pd.Timestamp(run_date).normalize()
    return pd.bdate_range(end=run_date, periods=lookback_days)


def build_tradar_daily_balances(
    tradar_df: pd.DataFrame,
    run_date: pd.Timestamp,
    lookback_days: int,
    allowed_accounts: list[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
    business_days = get_business_dates(run_date, lookback_days)
    tradar = tradar_df.copy()
    for col in ['Account', 'Fund', 'CCY']:
        tradar[col] = tradar[col].fillna('').astype(str).str.strip()

    if allowed_accounts:
        allowed_rules = [clean_text(x) for x in allowed_accounts if clean_text(x)]

        def _allowed(account_value: str) -> bool:
            account = clean_text(account_value)
            return any(_account_matches_rule(account, rule) for rule in allowed_rules)

        settled = tradar[tradar['Account'].map(_allowed)].copy()
    else:
        settled = tradar.copy()

    if settled.empty:
        raise CashRecError('No Tradar rows found for mapped Tradar accounts.')

    # Opening balance per group (first row flagged as opening)
    opening = (
        settled[settled['Is Opening Balance']]
        .groupby(['Fund', 'CCY', 'Account'], dropna=False)['Balance']
        .first()
        .reset_index()
        .rename(columns={'Balance': 'Tradar Opening Balance'})
    )

    # Settled transactions only — last entry per (group, settle date) gives the final balance
    txns = (
        settled[~settled['Is Opening Balance'] & settled['Settles'].notna()]
        .sort_values(['Fund', 'CCY', 'Account', 'Settles', 'Date'])
        .groupby(['Fund', 'CCY', 'Account', 'Settles'], as_index=False)
        .last()
    )

    # Grid: every (Fund, CCY, Account) × every business date
    # merge_asof requires the join key (Date / Settles) to be globally sorted
    groups = settled[['Fund', 'CCY', 'Account']].drop_duplicates().reset_index(drop=True)
    dates_df = pd.DataFrame({'Date': pd.DatetimeIndex(business_days).normalize()})
    grid = groups.merge(dates_df, how='cross').sort_values('Date').reset_index(drop=True)

    # For each (group, cob_date) find the last transaction with Settles <= cob_date
    if not txns.empty:
        txns_right = txns[['Fund', 'CCY', 'Account', 'Settles', 'Balance']].sort_values('Settles').reset_index(drop=True)
        daily = pd.merge_asof(
            grid,
            txns_right,
            left_on='Date',
            right_on='Settles',
            by=['Fund', 'CCY', 'Account'],
            direction='backward',
        ).rename(columns={'Balance': 'Tradar Balance', 'Settles': 'Tradar Settles Used'})
    else:
        daily = grid.copy()
        daily['Tradar Balance'] = np.nan
        daily['Tradar Settles Used'] = pd.NaT

    # Attach opening balance; fall back to it when no eligible transaction exists
    daily = daily.merge(opening, on=['Fund', 'CCY', 'Account'], how='left')
    daily['Tradar Balance'] = daily['Tradar Balance'].fillna(daily['Tradar Opening Balance'])
    daily = daily.rename(columns={'Account': 'Tradar_Account'})
    daily = daily.sort_values(['Date', 'Fund', 'CCY']).reset_index(drop=True)
    raw_cols = ['Fund', 'Account', 'CCY', 'Trade', 'Type', 'Date', 'Settles', 'Amount', 'Cashflow', 'Cumulative Cashflow', 'Balance', 'Description', 'Notes']
    raw_settled = settled[[c for c in raw_cols if c in settled.columns]].copy()
    raw_settled = raw_settled.sort_values(['Fund', 'CCY', 'Settles', 'Date'], na_position='last').reset_index(drop=True)
    return daily, raw_settled


def _prepare_source(source_balances_df: pd.DataFrame, run_date: pd.Timestamp, lookback_days: int) -> pd.DataFrame:
    business_dates = get_business_dates(run_date, lookback_days)
    source_df = source_balances_df.copy()
    source_df['Date'] = pd.to_datetime(source_df['Date'], errors='coerce').dt.normalize()
    source_df['Currency Code'] = source_df['Currency Code'].map(clean_text).str.upper()
    source_df['Source Account ID'] = source_df['Source Account ID'].map(normalize_identifier)
    source_df['Custody'] = source_df['Custody'].map(clean_text).str.upper()
    return source_df[source_df['Date'].isin(business_dates)].copy()


def _prepare_mapping(mapping_df: pd.DataFrame) -> pd.DataFrame:
    mapping = mapping_df.copy()

    for col in ['Fund', 'Portfolio', 'Custody', 'Tradar_Account']:
        if col in mapping.columns:
            mapping[col] = mapping[col].map(clean_text)

    if 'Account ID' in mapping.columns:
        mapping['Account ID'] = mapping['Account ID'].map(normalize_identifier)

    if 'SKAC' in mapping.columns:
        mapping['SKAC'] = mapping['SKAC'].map(normalize_identifier)

    if 'Currency Code' in mapping.columns:
        mapping['Currency Code'] = mapping['Currency Code'].map(clean_text).str.upper()

    if 'Fund' in mapping.columns:
        mapping['Fund'] = mapping['Fund'].str.upper()

    if 'Custody' in mapping.columns:
        mapping['Custody'] = mapping['Custody'].str.upper().str.replace(r'\s+', '', regex=True)

    if 'Tradar_Account' in mapping.columns:
        mapping['Tradar_Account'] = mapping['Tradar_Account'].map(normalize_identifier)

    mapping['In Scope'] = mapping['In Scope'].fillna(False).astype(bool)
    mapping = mapping[mapping['Custody'].fillna('').ne('')].copy()
    return mapping


def _match_citi(source_df: pd.DataFrame, mapping_df: pd.DataFrame, custody_value: str) -> pd.DataFrame:
    source_type = source_df['Source Type'] if 'Source Type' in source_df.columns else pd.Series('', index=source_df.index)

    src = source_df[
        source_df['Custody'].eq(custody_value) &
        source_type.fillna('').ne('CITI_HI')
    ].copy()

    mp = mapping_df[
        mapping_df['Custody'].eq(custody_value) &
        mapping_df.get('Account ID', pd.Series('', index=mapping_df.index)).fillna('').ne('') &
        mapping_df.get('Currency Code', pd.Series('', index=mapping_df.index)).fillna('').ne('')
    ].copy()

    if src.empty:
        return src.assign(Fund=pd.NA, Portfolio=pd.NA, Tradar_Account=pd.NA, **{'In Scope': False, '_matched': False})
    if mp.empty:
        return src.assign(Fund=pd.NA, Portfolio=pd.NA, Tradar_Account=pd.NA, **{'In Scope': False, '_matched': False})

    mp_cols = ['Fund', 'Portfolio', 'Account ID', 'Currency Code', 'Tradar_Account', 'In Scope']
    if 'Fund Type' in mp.columns:
        mp_cols.append('Fund Type')
    merged = src.merge(
        mp[mp_cols],
        how='left',
        left_on=['Source Account ID', 'Currency Code'],
        right_on=['Account ID', 'Currency Code'],
    )
    merged['_matched'] = merged['Fund'].notna()
    return merged.drop(columns=[c for c in ['Account ID'] if c in merged.columns])

def _match_citi_hi(source_df: pd.DataFrame, mapping_df: pd.DataFrame, custody_value: str) -> pd.DataFrame:
    source_type = source_df['Source Type'] if 'Source Type' in source_df.columns else pd.Series('', index=source_df.index)

    src = source_df[
        source_df['Custody'].eq(custody_value) &
        source_type.fillna('').eq('CITI_HI')
    ].copy()

    account_id_series = mapping_df['Account ID'] if 'Account ID' in mapping_df.columns else pd.Series('', index=mapping_df.index)
    skac_series = mapping_df['SKAC'] if 'SKAC' in mapping_df.columns else pd.Series('', index=mapping_df.index)

    mp = mapping_df[
        mapping_df['Custody'].eq(custody_value) &
        skac_series.fillna('').ne('') &
        account_id_series.fillna('').ne('')
    ].copy()

    if 'Tradar_Account' in mp.columns:
        mp = mp[mp['Tradar_Account'].fillna('').ne('')].copy()

    if src.empty:
        return src.assign(Fund=pd.NA, Portfolio=pd.NA, Tradar_Account=pd.NA, **{'In Scope': False, '_matched': False})
    if mp.empty:
        return src.assign(Fund=pd.NA, Portfolio=pd.NA, Tradar_Account=pd.NA, **{'In Scope': False, '_matched': False})

    mp_cols = ['Fund', 'Portfolio', 'SKAC', 'Account ID', 'Tradar_Account', 'In Scope']
    if 'Fund Type' in mp.columns:
        mp_cols.append('Fund Type')
    merged = src.merge(
        mp[mp_cols],
        how='left',
        left_on=['Source SKAC', 'Source Account ID'],
        right_on=['SKAC', 'Account ID'],
    )
    merged['_matched'] = merged['Fund'].notna()
    return merged.drop(columns=[c for c in ['SKAC', 'Account ID'] if c in merged.columns])

def _match_bnp(source_df: pd.DataFrame, mapping_df: pd.DataFrame, custody_value: str) -> pd.DataFrame:
    src = source_df[source_df['Custody'].eq(custody_value)].copy()
    mp = mapping_df[
        mapping_df['Custody'].eq(custody_value) &
        mapping_df.get('Account ID', '').fillna('').ne('')
    ].copy()

    if src.empty:
        return src.assign(Fund=pd.NA, Portfolio=pd.NA, Tradar_Account=pd.NA, **{'In Scope': False, '_matched': False})
    if mp.empty:
        return src.assign(Fund=pd.NA, Portfolio=pd.NA, Tradar_Account=pd.NA, **{'In Scope': False, '_matched': False})

    mp_cols = ['Fund', 'Portfolio', 'Account ID', 'Currency Code', 'Tradar_Account', 'In Scope']
    if 'Fund Type' in mp.columns:
        mp_cols.append('Fund Type')
    merged = src.merge(
        mp[mp_cols],
        how='left',
        left_on='Source Account ID',
        right_on='Account ID',
    )

    if 'Currency Code_y' in merged.columns:
        currency_ok = merged['Currency Code_y'].fillna('').eq('') | merged['Currency Code_x'].eq(merged['Currency Code_y'])
        merged = merged[currency_ok].copy()
        merged['_currency_priority'] = np.where(merged['Currency Code_y'].fillna('').eq(''), 1, 0)
        merged = merged.sort_values(
            ['Date', 'Source Account ID', 'Currency Code_x', '_currency_priority', 'Fund'],
            ascending=[True, True, True, True, True],
        )
        merged = merged.drop_duplicates(subset=['Date', 'Source Account ID', 'Currency Code_x'], keep='first')
        merged = merged.rename(columns={'Currency Code_x': 'Currency Code'})
        merged = merged.drop(columns=['Currency Code_y', '_currency_priority'], errors='ignore')

    merged['_matched'] = merged['Fund'].notna()
    return merged.drop(columns=[c for c in ['Account ID'] if c in merged.columns])

def _match_bnp_nz(source_df: pd.DataFrame, mapping_df: pd.DataFrame, custody_value: str) -> pd.DataFrame:
    src = source_df[source_df['Custody'].eq(custody_value)].copy()
    mp = mapping_df[mapping_df['Custody'].eq(custody_value)].copy()

    if src.empty:
        return src.assign(Fund=pd.NA, Portfolio=pd.NA, Tradar_Account=pd.NA, **{'In Scope': False, '_matched': False})
    if mp.empty:
        return src.assign(Fund=pd.NA, Portfolio=pd.NA, Tradar_Account=pd.NA, **{'In Scope': False, '_matched': False})

    src['Source Account ID'] = src['Source Account ID'].fillna('').astype(str).str.strip()
    src['Currency Code'] = src['Currency Code'].fillna('').astype(str).str.strip().str.upper()

    src['Derived Fund'] = [
        acct[:-len(ccy)].upper() if ccy and acct.endswith(ccy) else acct.upper()
        for acct, ccy in zip(src['Source Account ID'], src['Currency Code'])
    ]

    mp_cols = ['Fund', 'Portfolio', 'Currency Code', 'Tradar_Account', 'In Scope']
    if 'Fund Type' in mp.columns:
        mp_cols.append('Fund Type')
    merged = src.merge(
        mp[mp_cols],
        how='left',
        left_on='Derived Fund',
        right_on='Fund',
        suffixes=('', '_map'),
    )

    if 'Currency Code_map' in merged.columns:
        currency_ok = merged['Currency Code_map'].fillna('').eq('') | merged['Currency Code'].eq(merged['Currency Code_map'])
        merged = merged[currency_ok].copy()
        merged['_currency_priority'] = np.where(merged['Currency Code_map'].fillna('').eq(''), 1, 0)
        merged = merged.sort_values(
            ['Date', 'Source Account ID', 'Currency Code', '_currency_priority', 'Fund'],
            ascending=[True, True, True, True, True],
        )
        merged = merged.drop_duplicates(subset=['Date', 'Source Account ID', 'Currency Code'], keep='first')
        merged = merged.drop(columns=['Currency Code_map', '_currency_priority'], errors='ignore')

    merged['_matched'] = merged['Fund'].notna()
    return merged


def _combine_matches(source_df: pd.DataFrame, mapping_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    custody_values = {k: str(v).upper() for k, v in config['mapping']['custody_values'].items()}
    frames = [
        _match_citi(source_df, mapping_df, custody_values['citi']),
        _match_citi_hi(source_df, mapping_df, custody_values['citi']),
        _match_bnp(source_df, mapping_df, custody_values['bnp']),
        _match_bnp_nz(source_df, mapping_df, custody_values['bnp_nz']),
    ]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=list(source_df.columns) + ['Fund', 'Portfolio', 'Tradar_Account', 'In Scope', '_matched'])
    return pd.concat(frames, ignore_index=True, sort=False)

def reconcile_balances(
    source_balances_df: pd.DataFrame,
    tradar_daily_df: pd.DataFrame,
    mapping_accounts_df: pd.DataFrame,
    run_date: pd.Timestamp,
    lookback_days: int,
    config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    source_df = _prepare_source(source_balances_df, run_date, lookback_days)
    mapping_df = _prepare_mapping(mapping_accounts_df)
    merged = _combine_matches(source_df, mapping_df, config)

    matched_mask = merged['_matched'].fillna(False).astype('boolean')

    unmapped = merged[~matched_mask].copy()
    matched = merged[matched_mask].copy()

    matched_in_scope_mask = matched['In Scope'].fillna(False).astype('boolean')
    out_of_scope = matched[~matched_in_scope_mask].copy()
    in_scope = matched[matched_in_scope_mask].copy()

    rec_base = in_scope if config['reconciliation'].get('mapped_funds_only', True) else matched
    if 'Tradar_Account' not in rec_base.columns:
        rec_base['Tradar_Account'] = ''
        rec_base['Tradar_Account'] = rec_base['Tradar_Account'].fillna('').astype(str).str.strip()
    if rec_base.empty:
        rec_detail = pd.DataFrame(columns=config['columns']['output_rec_detail'])
        summary = pd.DataFrame(columns=['Date', 'Custody', 'Status', 'Count', 'Total_Abs_Variance'])
        return summary, rec_detail, unmapped, out_of_scope

    rec_base = rec_base.copy()
    rec_base['Tradar Match Fund'] = ''
    for col in config['mapping']['fallback_fund_order']:
        if col in rec_base.columns:
            rec_base.loc[rec_base['Tradar Match Fund'].eq(''), 'Tradar Match Fund'] = rec_base[col].fillna('')

    balance_col = config['reconciliation']['source_balance_column']
    _grp_keys = [k for k in ['Date', 'Tradar Match Fund', 'Currency Code', 'Tradar_Account'] if k in rec_base.columns]
    _other_cols = [c for c in rec_base.columns if c not in _grp_keys]
    _agg = {c: ('sum' if c == balance_col else 'first') for c in _other_cols}
    rec_base = rec_base.groupby(_grp_keys, as_index=False, dropna=False).agg(_agg)

    rec = _merge_with_tradar_account_rule(
    rec_base,
    tradar_daily_df,
    left_base_keys=['Date', 'Tradar Match Fund', 'Currency Code'],
    right_base_keys=['Date', 'Fund', 'CCY'],
    )

    rec = rec[rec['Tradar Balance'].notna() & rec[balance_col].notna()].copy()
    rec['Variance'] = rec[balance_col] - rec['Tradar Balance']
    rec['Abs Variance'] = rec['Variance'].abs()
    round_dp = int(config['reconciliation'].get('status_round_dp', 2))
    match_value = float(config['reconciliation'].get('status_match_value', 0))
    rec['Status'] = np.where(rec['Abs Variance'].round(round_dp).eq(match_value), 'Matched', 'Break')
    if 'Fund_x' in rec.columns:
        fund_col = rec['Fund_x']
        if 'Fund' in rec.columns:
            fund_col = fund_col.combine_first(rec['Fund'])
        rec['Mapping Fund'] = fund_col
    else:
        rec['Mapping Fund'] = rec['Fund'] if 'Fund' in rec.columns else rec.get('Fund_y', '')
    if 'Portfolio' not in rec.columns:
        rec['Portfolio'] = rec.get('Mapping Fund', '')

    ordered_cols = [c for c in config['columns']['output_rec_detail'] if c in rec.columns]
    rec_detail = rec[ordered_cols].sort_values(['Date', 'Custody', 'Mapping Fund', 'Currency Code', 'Source Account ID']).reset_index(drop=True)

    summary = (
        rec_detail.groupby(['Date', 'Custody', 'Status'], dropna=False)
        .agg(Count=('Status', 'size'), Total_Abs_Variance=('Abs Variance', 'sum'))
        .reset_index()
        .sort_values(['Date', 'Custody', 'Status'])
    )
    
    return summary, rec_detail, unmapped, out_of_scope

def reconcile_high_interest_balances(
    source_balances_df: pd.DataFrame,
    tradar_daily_df: pd.DataFrame,
    mapping_accounts_df: pd.DataFrame,
    run_date: pd.Timestamp,
    config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    source_df = source_balances_df.copy()
    source_type = source_df['Source Type'] if 'Source Type' in source_df.columns else pd.Series('', index=source_df.index)
    source_df = source_df[source_type.fillna('').eq('CITI_HI')].copy()

    if source_df.empty:
        empty_summary = pd.DataFrame(columns=['Target Date', 'Status', 'Count', 'Total_Abs_Variance'])
        empty_detail = pd.DataFrame(columns=config['columns']['output_hi_rec_detail'])
        empty_unmapped = pd.DataFrame()
        return empty_summary, empty_detail, empty_unmapped

    mapping_df = _prepare_mapping(mapping_accounts_df)

    skac_series = mapping_df['SKAC'] if 'SKAC' in mapping_df.columns else pd.Series('', index=mapping_df.index)
    account_id_series = mapping_df['Account ID'] if 'Account ID' in mapping_df.columns else pd.Series('', index=mapping_df.index)

    mapping_df = mapping_df[
        mapping_df['Custody'].eq(config['mapping']['custody_values']['citi']) &
        skac_series.fillna('').ne('') &
        account_id_series.fillna('').ne('')
    ].copy()

    mp_hi_cols = ['Fund', 'Portfolio', 'SKAC', 'Account ID', 'Tradar_Account', 'In Scope']
    for _c in ['Currency Code', 'Fund Type']:
        if _c in mapping_df.columns:
            mp_hi_cols.append(_c)
    merged = source_df.merge(
        mapping_df[mp_hi_cols],
        how='left',
        left_on=['Source SKAC', 'Source Account ID'],
        right_on=['SKAC', 'Account ID'],
    )

    # HI source has no Currency Code — fill from mapping
    if 'Currency Code_y' in merged.columns:
        merged['Currency Code'] = merged['Currency Code_x'].fillna(merged['Currency Code_y'])
        merged = merged.drop(columns=['Currency Code_x', 'Currency Code_y'])
    elif 'Currency Code' not in merged.columns and 'Currency Code_x' in merged.columns:
        merged = merged.rename(columns={'Currency Code_x': 'Currency Code'})

    matched_mask = merged['Fund'].notna().fillna(False).astype('boolean')
    unmapped = merged[~matched_mask].copy()
    matched = merged[matched_mask].copy()

    if matched.empty:
        empty_summary = pd.DataFrame(columns=['Target Date', 'Status', 'Count', 'Total_Abs_Variance'])
        empty_detail = pd.DataFrame(columns=config['columns']['output_hi_rec_detail'])
        return empty_summary, empty_detail, unmapped

    in_scope_mask = matched['In Scope'].fillna(False).astype('boolean')
    in_scope = matched[in_scope_mask].copy()

    if in_scope.empty:
        empty_summary = pd.DataFrame(columns=['Target Date', 'Status', 'Count', 'Total_Abs_Variance'])
        empty_detail = pd.DataFrame(columns=config['columns']['output_hi_rec_detail'])
        return empty_summary, empty_detail, unmapped

    in_scope['Tradar Match Fund'] = ''
    for col in config['mapping']['fallback_fund_order']:
        if col in in_scope.columns:
            in_scope.loc[in_scope['Tradar Match Fund'].eq(''), 'Tradar Match Fund'] = in_scope[col].fillna('')

    in_scope['Target Date'] = pd.to_datetime(in_scope['Date'], errors='coerce').dt.normalize()
    in_scope['Tradar_Account'] = in_scope['Tradar_Account'].fillna('').astype(str).str.strip()

    _hi_grp_keys = [k for k in ['Target Date', 'Tradar Match Fund', 'Currency Code', 'Tradar_Account'] if k in in_scope.columns]
    _hi_other_cols = [c for c in in_scope.columns if c not in _hi_grp_keys]
    _hi_agg = {c: ('sum' if c == 'Source Ledger Balance' else 'first') for c in _hi_other_cols}
    in_scope = in_scope.groupby(_hi_grp_keys, as_index=False, dropna=False).agg(_hi_agg)

    rec = _merge_with_tradar_account_rule(
    in_scope,
    tradar_daily_df,
    left_base_keys=['Target Date', 'Tradar Match Fund'],
    right_base_keys=['Date', 'Fund'],
    )

    rec = rec[rec['Tradar Balance'].notna() & rec['Source Ledger Balance'].notna()].copy()

    if rec.empty:
        empty_summary = pd.DataFrame(columns=['Target Date', 'Status', 'Count', 'Total_Abs_Variance'])
        empty_detail = pd.DataFrame(columns=config['columns']['output_hi_rec_detail'])
        return empty_summary, empty_detail, unmapped

    rec['Variance'] = rec['Source Ledger Balance'] - rec['Tradar Balance']
    rec['Abs Variance'] = rec['Variance'].abs()
    round_dp = int(config['reconciliation'].get('status_round_dp', 2))
    match_value = float(config['reconciliation'].get('status_match_value', 0))
    rec['Status'] = np.where(rec['Abs Variance'].round(round_dp).eq(match_value), 'Matched', 'Break')

    if 'Fund_x' in rec.columns:
        fund_col = rec['Fund_x']
        if 'Fund' in rec.columns:
            fund_col = fund_col.combine_first(rec['Fund'])
        rec['Mapping Fund'] = fund_col
    else:
        rec['Mapping Fund'] = rec['Fund'] if 'Fund' in rec.columns else ''
    detail_cols = [c for c in config['columns']['output_hi_rec_detail'] if c in rec.columns]
    hi_detail = rec[detail_cols].sort_values(['Target Date', 'Mapping Fund', 'Source SKAC', 'Source Account ID']).reset_index(drop=True)

    hi_summary = (
        hi_detail.groupby(['Target Date', 'Status'], dropna=False)
        .agg(Count=('Status', 'size'), Total_Abs_Variance=('Abs Variance', 'sum'))
        .reset_index()
        .sort_values(['Target Date', 'Status'])
    )

    return hi_summary, hi_detail, unmapped

def reconcile_bbus_bnp_total_balance(
    bnp_source_df: pd.DataFrame,
    tradar_daily_df: pd.DataFrame,
    mapping_accounts_df: pd.DataFrame,
    run_date: pd.Timestamp,
    pdf_adjustment: dict,
    config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_date = pd.Timestamp(run_date).normalize()

    special_cfg = config.get("special_reconciliation", {})
    bbus_cfg = special_cfg.get("bbus_bnp_total", {})

    target_fund = str(bbus_cfg.get("fund", "BBUS")).strip().upper()
    target_custody = str(bbus_cfg.get("custody", "BNP")).strip().upper()
    target_currency = str(bbus_cfg.get("currency", "USD")).strip().upper()
    required_accounts = {
        str(x).strip()
        for x in bbus_cfg.get("tradar_accounts", [])
        if str(x).strip()
    }

    if not required_accounts:
        raise CashRecError("BBUS special reconciliation config is missing tradar_accounts.")

    # -----------------------------
    # Source side: BNP cash balances
    # Use latest available BNP date <= run_date
    # -----------------------------
    bnp = bnp_source_df.copy()
    bnp["Date"] = pd.to_datetime(bnp["Date"], errors="coerce").dt.normalize()
    bnp["Custody"] = bnp["Custody"].fillna("").astype(str).str.strip().str.upper()
    bnp["Currency Code"] = bnp["Currency Code"].fillna("").astype(str).str.strip().str.upper()

    bnp = bnp[bnp["Custody"].eq(target_custody)].copy()

    available_bnp_dates = (
        bnp["Date"]
        .dropna()
        .loc[lambda s: s.le(target_date)]
        .sort_values()
    )

    # -----------------------------
    # Mapping side
    # Be tolerant: BBUS may be in Fund, Portfolio, or both
    # -----------------------------
    mapping_df = _prepare_mapping(mapping_accounts_df).copy()
    mapping_df["Custody"] = mapping_df["Custody"].fillna("").astype(str).str.strip().str.upper()
    mapping_df["Fund"] = mapping_df["Fund"].fillna("").astype(str).str.strip().str.upper()

    if "Portfolio" in mapping_df.columns:
        mapping_df["Portfolio"] = mapping_df["Portfolio"].fillna("").astype(str).str.strip().str.upper()
    else:
        mapping_df["Portfolio"] = ""

    mapping_df = mapping_df[mapping_df["Custody"].eq(target_custody)].copy()

    fund_mask = mapping_df["Fund"].eq(target_fund) | mapping_df["Portfolio"].eq(f"{target_fund}-ETF")
    mapping_df = mapping_df[fund_mask].copy()

    print("==== BBUS SPECIAL DEBUG ====")
    print("target_date:", target_date)
    print("target_fund:", target_fund)
    print("target_custody:", target_custody)
    print("target_currency:", target_currency)
    print("required_accounts:", sorted(required_accounts))
    print("mapping rows after fund/custody filter:", len(mapping_df))

    if not mapping_df.empty:
        cols = [c for c in ["Fund", "Portfolio", "Account ID", "Currency Code", "Custody", "Tradar_Account", "Fund Type"] if c in mapping_df.columns]
        print("Mapping sample:")
        print(mapping_df[cols].head(10).to_string(index=False))

    if available_bnp_dates.empty:
        print("Returning empty: no BNP dates on or before run date")
        return pd.DataFrame(), pd.DataFrame()

    effective_bnp_date = available_bnp_dates.iloc[-1]
    print("effective_bnp_date used for BBUS:", effective_bnp_date)

    bnp = bnp[bnp["Date"].eq(effective_bnp_date)].copy()
    print("bnp rows after custody/effective date filter:", len(bnp))

    if not bnp.empty:
        cols = [c for c in ["Date", "Custody", "Source Account ID", "Currency Code", "Source Ledger Balance"] if c in bnp.columns]
        print("BNP sample:")
        print(bnp[cols].head(10).to_string(index=False))

    if bnp.empty:
        print("Returning empty: bnp empty after custody/effective date filter")
        return pd.DataFrame(), pd.DataFrame()

    if mapping_df.empty:
        print("Returning empty: mapping empty after BBUS filter")
        return pd.DataFrame(), pd.DataFrame()

    # -----------------------------
    # Match BNP rows to mapping
    # -----------------------------
    matched = _match_bnp(bnp, mapping_df, target_custody).copy()
    print("rows returned by _match_bnp:", len(matched))

    if not matched.empty:
        cols = [c for c in ["Date", "Fund", "Portfolio", "Source Account ID", "Currency Code", "Custody", "Tradar_Account", "Fund Type", "Source Ledger Balance", "_matched"] if c in matched.columns]
        print("Matched sample before _matched filter:")
        print(matched[cols].head(20).to_string(index=False))

    matched = matched[matched["_matched"].fillna(False).astype("boolean")].copy()
    print("matched rows after _matched filter:", len(matched))

    if matched.empty:
        print("Returning empty: matched empty after _matched filter")
        return pd.DataFrame(), pd.DataFrame()

    matched = matched[matched["Currency Code"].eq(target_currency)].copy()
    print("matched rows after currency filter:", len(matched))

    if not matched.empty:
        cols = [c for c in ["Date", "Fund", "Portfolio", "Source Account ID", "Currency Code", "Tradar_Account", "Fund Type", "Source Ledger Balance"] if c in matched.columns]
        print("Matched sample after currency filter:")
        print(matched[cols].head(20).to_string(index=False))

    if matched.empty:
        print("Returning empty: matched empty after currency filter")
        return pd.DataFrame(), pd.DataFrame()

    # -----------------------------
    # Derive Tradar match fund
    # Pick the first fallback candidate that actually exists in tradar_daily_df.
    # Portfolio ("BBUS-ETF") is tried before Fund ("BBUS"), but Tradar may only
    # carry the short name, so we validate against real data.
    # -----------------------------
    _tradar_funds = set(tradar_daily_df["Fund"].dropna().astype(str).str.strip().unique())
    print("Unique Tradar funds in tradar_daily_df (sample):", sorted(_tradar_funds)[:20])

    tradar_match_fund = ""
    for col in config["mapping"]["fallback_fund_order"]:
        if col in matched.columns:
            vals = matched[col].fillna("").astype(str).str.strip()
            vals = vals[vals.ne("")]
            if not vals.empty:
                candidate = vals.iloc[0]
                if candidate in _tradar_funds:
                    tradar_match_fund = candidate
                    break

    # Last resort: use the raw config fund name
    if not tradar_match_fund and target_fund in _tradar_funds:
        tradar_match_fund = target_fund

    print("tradar_match_fund:", tradar_match_fund)
    print("All Tradar funds available:", sorted(_tradar_funds))

    if not tradar_match_fund:
        print("WARNING: BBUS fund not found in Tradar data — skipping BBUS special rec.")
        return pd.DataFrame(), pd.DataFrame()

    # -----------------------------
    # Source ledger = BNP cash + PDF adjustment
    # -----------------------------
    bnp_cash_balance_usd = float(pd.to_numeric(matched["Source Ledger Balance"], errors="coerce").fillna(0).sum())
    total_pdf_adjustment_usd = float(pdf_adjustment["Total PDF Adjustment USD"])
    adjusted_source_ledger_balance = bnp_cash_balance_usd + total_pdf_adjustment_usd

    print("bnp_cash_balance_usd:", bnp_cash_balance_usd)
    print("total_pdf_adjustment_usd:", total_pdf_adjustment_usd)
    print("adjusted_source_ledger_balance:", adjusted_source_ledger_balance)

    # -----------------------------
    # Tradar side: USD only, sum required accounts
    # Use latest available Tradar date <= run_date
    # -----------------------------
    tradar = tradar_daily_df.copy()
    tradar["Date"] = pd.to_datetime(tradar["Date"], errors="coerce").dt.normalize()
    tradar["Fund"] = tradar["Fund"].fillna("").astype(str).str.strip()
    tradar["CCY"] = tradar["CCY"].fillna("").astype(str).str.strip().str.upper()
    tradar["Tradar_Account"] = tradar["Tradar_Account"].fillna("").astype(str).str.strip()
    tradar["Tradar Balance"] = pd.to_numeric(tradar["Tradar Balance"], errors="coerce").fillna(0.0)

    # Debug: show what Fund/CCY combos exist for this fund in tradar
    _bbus_combos = tradar[tradar["Fund"].eq(tradar_match_fund)][["Fund", "CCY"]].drop_duplicates()
    print("Tradar Fund/CCY combos for", tradar_match_fund, ":\n", _bbus_combos.to_string(index=False))

    # Show CCY breakdown for required accounts to aid diagnosis
    _acct_debug = tradar[
        tradar["Fund"].eq(tradar_match_fund) &
        tradar["Tradar_Account"].isin(required_accounts)
    ][["Fund", "CCY", "Tradar_Account", "Tradar Balance"]].drop_duplicates()
    print("Tradar accounts/CCY for required accounts:\n", _acct_debug.to_string(index=False))

    available_tradar_dates = (
        tradar.loc[
            tradar["Fund"].eq(tradar_match_fund) &
            tradar["CCY"].eq(target_currency) &
            tradar["Tradar_Account"].isin(required_accounts),
            "Date"
        ]
        .dropna()
        .loc[lambda s: s.le(target_date)]
        .sort_values()
    )

    if available_tradar_dates.empty:
        print("Returning empty: no Tradar USD rows found for required accounts — check CCY in Tradar file")
        return pd.DataFrame(), pd.DataFrame()

    effective_tradar_date = available_tradar_dates.iloc[-1]
    print("effective_tradar_date used for BBUS:", effective_tradar_date)

    tradar_bbus = tradar[
        tradar["Date"].eq(effective_tradar_date) &
        tradar["Fund"].eq(tradar_match_fund) &
        tradar["CCY"].eq(target_currency) &
        tradar["Tradar_Account"].isin(required_accounts)
    ].copy()

    print("tradar_bbus rows:", len(tradar_bbus))
    if not tradar_bbus.empty:
        print(tradar_bbus[["Date", "Fund", "CCY", "Tradar_Account", "Tradar Balance"]].to_string(index=False))

    found_accounts = set(tradar_bbus["Tradar_Account"].dropna().astype(str).str.strip())
    missing_accounts = required_accounts - found_accounts
    if missing_accounts:
        print("Returning empty / error: missing Tradar accounts:", sorted(missing_accounts))
        raise CashRecError(f"Missing required BBUS Tradar {target_currency} accounts: {sorted(missing_accounts)}")

    account_totals = (
        tradar_bbus.groupby("Tradar_Account", dropna=False)["Tradar Balance"]
        .sum()
        .reset_index()
        .sort_values("Tradar_Account")
        .reset_index(drop=True)
    )

    print("Tradar account totals:")
    print(account_totals.to_string(index=False))

    tradar_balance_usd = float(account_totals["Tradar Balance"].sum())
    variance = adjusted_source_ledger_balance - tradar_balance_usd

    print("tradar_balance_usd:", tradar_balance_usd)
    print("variance:", variance)

    # -----------------------------
    # Final output
    # -----------------------------
    detail = pd.DataFrame([{
        "Date": target_date,
        "Fund": target_fund,
        "Custody": target_custody,
        "Currency Code": target_currency,
        "Portfolio": tradar_match_fund,
        "Mapping Fund": target_fund,
        "Fund Type": matched["Fund Type"].iloc[0] if "Fund Type" in matched.columns and not matched.empty else "",
        "BNP Cash Balance USD": bnp_cash_balance_usd,
        "Initial Margin Requirement USD": float(pdf_adjustment["Initial Margin Requirement USD"]),
        "Unrealized Trading USD": float(pdf_adjustment["Unrealized Trading USD"]),
        "Opposite Unrealized Trading USD": float(pdf_adjustment["Opposite Unrealized Trading USD"]),
        "Total PDF Adjustment USD": total_pdf_adjustment_usd,
        "Source Ledger Balance": adjusted_source_ledger_balance,
        "Tradar Accounts Used": " + ".join(sorted(required_accounts)),
        "Tradar Balance": tradar_balance_usd,
        "Variance": variance,
        "Abs Variance": abs(variance),
        "Status": "Matched" if round(abs(variance), 2) == 0 else "Break",
    }])

    summary = detail[[
        "Date",
        "Fund",
        "Custody",
        "Currency Code",
        "Fund Type",
        "Source Ledger Balance",
        "Tradar Balance",
        "Variance",
        "Abs Variance",
        "Status",
    ]].copy()

    print("detail rows:", len(detail))
    print(detail.to_string(index=False))

    return summary, detail