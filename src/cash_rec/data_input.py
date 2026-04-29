from __future__ import annotations

from pathlib import Path
from pypdf import PdfReader
import pandas as pd


def read_tabular_file(path: str | Path, dtype=str, **kwargs) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == '.csv':
        return pd.read_csv(path, dtype=dtype, **kwargs)
    return pd.read_excel(path, dtype=dtype, **kwargs)
def read_pdf_text(pdf_file: str | Path) -> str:
    pdf_file = Path(pdf_file)
    reader = PdfReader(str(pdf_file))

    text_parts = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            text_parts.append(text)

    return "\n".join(text_parts)