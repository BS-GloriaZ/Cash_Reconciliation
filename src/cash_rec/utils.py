from __future__ import annotations

import math
import re
from pathlib import Path

import pandas as pd


def clean_text(value) -> str:
    if value is None:
        return ''
    if isinstance(value, float) and math.isnan(value):
        return ''
    return str(value).strip()


def normalize_identifier(value) -> str:
    text = clean_text(value)
    if text == '':
        return ''
    if re.fullmatch(r'-?\d+\.0+', text):
        return text.split('.', 1)[0]
    return text


def dedupe_columns(columns) -> list[str]:
    seen: dict[str, int] = {}
    output: list[str] = []
    for raw in columns:
        base = clean_text(raw) or '_blank'
        count = seen.get(base, 0)
        if count == 0:
            output.append(base)
        else:
            output.append(f'{base}_{count}')
        seen[base] = count + 1
    return output


def to_number(value):
    text = clean_text(value)
    if text == '':
        return pd.NA
    text = text.replace(',', '')
    if text.startswith('(') and text.endswith(')'):
        text = '-' + text[1:-1]
    try:
        return float(text)
    except ValueError:
        return pd.NA


def ensure_parent_dir(path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def first_matching_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for candidate in candidates:
        hit = lower_map.get(candidate.strip().lower())
        if hit is not None:
            return hit
    if required:
        raise KeyError(f'None of the candidate columns were found: {candidates}')
    return None
