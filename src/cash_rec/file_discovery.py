from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from cash_rec.exceptions import CashRecError


def latest_matching_file(folder: str | Path, pattern: str) -> Path:
    folder = Path(folder)
    matches = [p for p in folder.glob(pattern) if p.is_file()]
    if not matches:
        raise CashRecError(f'No files found in {folder} matching pattern {pattern}')
    return max(matches, key=lambda p: p.stat().st_mtime)


def default_output_path(output_dir: str | Path) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    return output_dir / f'cash_rec_{ts}.xlsx'


def resolve_source_dir(config: dict[str, Any], source_name: str) -> Path:
    """Return the directory for a named source.

    Checks sources.<name>.path first (absolute override for FTP folders),
    then falls back to input_root / sources.<name>.subdir.
    """
    source_cfg = config['sources'][source_name]
    explicit = source_cfg.get('path')
    if explicit:
        return Path(explicit)
    return Path(config['paths']['input_root']) / source_cfg['subdir']


def resolve_input_paths(config: dict, overrides: dict[str, str | None]) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for source_name, source_cfg in config['sources'].items():
        if not source_cfg.get('enabled', True):
            continue
        override = overrides.get(source_name)
        if override:
            paths[source_name] = Path(override)
            continue
        source_dir = resolve_source_dir(config, source_name)
        paths[source_name] = latest_matching_file(source_dir, source_cfg['filename_pattern'])
    return paths


def validate_path_isolation(input_paths: dict[str, Path], output_path: Path) -> None:
    """Raise CashRecError if the output directory overlaps with any source directory.

    Prevents accidental writes into FTP/source folders via misconfiguration.
    """
    out_dir = Path(output_path).resolve().parent
    for name, src_path in input_paths.items():
        src_dir = Path(src_path).resolve().parent
        try:
            out_dir.relative_to(src_dir)
            raise CashRecError(
                f"Output directory '{out_dir}' is inside the source folder for '{name}' ({src_dir}). "
                "Update output_root in your config to a separate location."
            )
        except ValueError:
            pass
        try:
            src_dir.relative_to(out_dir)
            raise CashRecError(
                f"Source folder for '{name}' ({src_dir}) is inside the output directory '{out_dir}'. "
                "This risks overwriting source files. Update output_root to a separate location."
            )
        except ValueError:
            pass


def check_source_writability(input_paths: dict[str, Path]) -> list[str]:
    """Return advisory warnings for source folders that are writable at OS level.

    The pipeline never writes to source paths, but writable FTP folders can be
    accidentally modified by other processes. Set folders to read-only at the OS
    level for the strongest guarantee.
    """
    warnings: list[str] = []
    seen_dirs: set[Path] = set()
    for name, src_path in input_paths.items():
        src_dir = Path(src_path).resolve().parent
        if src_dir in seen_dirs:
            continue
        seen_dirs.add(src_dir)
        if src_dir.exists() and os.access(src_dir, os.W_OK):
            warnings.append(
                f"Source folder for '{name}' is writable: {src_dir}  "
                f"(consider setting it to read-only at the OS level)"
            )
    return warnings
