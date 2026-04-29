"""Sync input files from FTP/network receipt folders to local data/input.

MODES
-----
daily (default)
    Copies only files that do not yet exist locally. Run each morning after
    overnight FTP delivery. Completes in seconds.

full  (--full)
    Copies all files within the lookback window. Use for the initial setup or
    after an extended gap. May take a few minutes over a network share.

USAGE
-----
    python sync_inputs.py               # daily incremental
    python sync_inputs.py --full        # full sync (60-day default window)
    python sync_inputs.py --full --lookback 90
"""
from __future__ import annotations

import argparse
import fnmatch
import os
import re
import shutil
import sys
from pathlib import Path

import pandas as pd

# Sources that require ALL files within the lookback window (one file = one
# settlement date). Everything else just needs the single latest file.
MULTI_FILE_SOURCES = {"citi_hi"}


def _extract_date(path: Path) -> pd.Timestamp | None:
    """Return the date embedded in a filename (YYYYMMDD or YYMMDD), or None."""
    stem = path.stem
    m8 = re.search(r'(\d{8})', stem)
    if m8:
        try:
            return pd.Timestamp(m8.group(1), format="%Y%m%d").normalize()
        except Exception:
            pass
    m6 = re.search(r'(\d{6})', stem)
    if m6:
        s = m6.group(1)
        try:
            return pd.Timestamp(f"20{s[:2]}-{s[2:4]}-{s[4:6]}").normalize()
        except Exception:
            pass
    return None


def _copy_if_needed(src: Path, dest: Path, dry_run: bool = False) -> bool:
    """Copy src → dest if dest doesn't exist or differs in size. Returns True if copied."""
    if dest.exists() and dest.stat().st_size == src.stat().st_size:
        return False
    if not dry_run:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    return True


def _today_start_ts() -> float:
    return pd.Timestamp.today().normalize().timestamp()


def _file_date_key(name: str) -> pd.Timestamp:
    d = _extract_date(Path(name))
    return d if d is not None else pd.Timestamp.min


def _scandir_matching(directory: Path, pattern: str):
    """Yield os.DirEntry objects matching pattern. scandir reuses dir-listing metadata."""
    with os.scandir(directory) as it:
        for entry in it:
            if entry.is_file(follow_symlinks=False) and fnmatch.fnmatch(entry.name, pattern):
                yield entry


def _local_has_today(dest_dir: Path, pattern: str) -> str | None:
    """Return filename if a matching local file was copied today (mtime >= midnight)."""
    ts = _today_start_ts()
    try:
        for entry in _scandir_matching(dest_dir, pattern):
            if entry.stat().st_mtime >= ts:
                return entry.name
    except FileNotFoundError:
        pass
    return None


def sync_single(
    src_dir: Path,
    dest_dir: Path,
    pattern: str,
    dry_run: bool = False,
) -> tuple[int, list[str]]:
    """Copy only the latest file matching pattern. Returns (files_copied, log_messages)."""
    # Fast path: a matching local file was already copied today (mtime >= midnight).
    local_today = _local_has_today(dest_dir, pattern)
    if local_today:
        return 0, [f"  Current (synced today): {local_today}"]

    # Network scan via scandir — on SMB/Windows shares DirEntry caches the
    # directory-listing metadata, so entry.stat() avoids extra round-trips.
    entries = list(_scandir_matching(src_dir, pattern))
    if not entries:
        return 0, [f"  [WARN] No files matching '{pattern}' in {src_dir}"]

    # Prefer filename-date sort (zero extra stat calls).
    # Fall back to mtime only when no files carry a date in their name.
    dated = [e for e in entries if _extract_date(Path(e.name)) is not None]
    if dated:
        latest = max(dated, key=lambda e: _file_date_key(e.name))
    else:
        latest = max(entries, key=lambda e: e.stat().st_mtime)

    dest = dest_dir / latest.name
    copied = _copy_if_needed(Path(latest.path), dest, dry_run)
    msg = f"  {'Copied ' if copied else 'Current'}: {latest.name}"
    return int(copied), [msg]


def sync_multi(
    src_dir: Path,
    dest_dir: Path,
    pattern: str,
    cutoff: pd.Timestamp,
    dry_run: bool = False,
) -> tuple[int, list[str]]:
    """Copy all files within the lookback window. Returns (files_copied, log_messages)."""
    # Fast path: a matching local file was already copied today.
    local_today = _local_has_today(dest_dir, pattern)
    if local_today:
        return 0, [f"  Current (synced today): {local_today} (and others)"]

    # Network scan — filter by date in filename (no extra stat calls needed).
    entries = [
        e for e in _scandir_matching(src_dir, pattern)
        if (d := _extract_date(Path(e.name))) is not None and d >= cutoff
    ]
    if not entries:
        return 0, [f"  [WARN] No files within window in {src_dir}"]
    msgs: list[str] = []
    copied = 0
    for entry in sorted(entries, key=lambda e: e.name):
        dest = dest_dir / entry.name
        if _copy_if_needed(Path(entry.path), dest, dry_run):
            msgs.append(f"  Copied : {entry.name}")
            copied += 1
        else:
            msgs.append(f"  Current: {entry.name}")
    return copied, msgs


def sync_all(
    config: dict,
    full: bool = False,
    lookback_days: int | None = None,
    dry_run: bool = False,
) -> tuple[int, list[str]]:
    """Run sync for all configured FTP sources.

    Returns (total_files_copied, log_messages).
    Suitable for calling from the Streamlit app or other Python code.
    """
    ftp_cfg = config.get("ftp", {})
    if not ftp_cfg:
        return 0, ["ERROR: No 'ftp' section in config. Add FTP source paths to config.local.yaml."]

    input_root = Path(config["paths"]["input_root"])
    _lookback = lookback_days or int(config["reconciliation"]["default_lookback_days"])
    run_date = pd.Timestamp.today().normalize()
    cutoff = pd.bdate_range(end=run_date, periods=_lookback + 1)[0].normalize()

    mode = "full" if full else "daily"
    msgs: list[str] = [
        f"Sync mode : {mode}",
        f"Lookback  : {_lookback} business days (from {cutoff.date()} to {run_date.date()})",
        f"Dest root : {input_root.resolve()}",
    ]
    if dry_run:
        msgs.append("DRY RUN   : no files will be written")

    sources = config.get("sources", {})
    total_copied = 0

    for source_name, ftp_path_str in ftp_cfg.items():
        src_dir = Path(ftp_path_str)
        source_cfg = sources.get(source_name, {})
        pattern = source_cfg.get("filename_pattern", "*")
        subdir = source_cfg.get("subdir", source_name)
        dest_dir = input_root / subdir

        msgs.append(f"[{source_name}]  {src_dir}")

        if not src_dir.is_dir():
            msgs.append("  SKIP: source folder not found")
            continue

        if source_name in MULTI_FILE_SOURCES:
            n, source_msgs = sync_multi(src_dir, dest_dir, pattern, cutoff, dry_run=dry_run)
        else:
            n, source_msgs = sync_single(src_dir, dest_dir, pattern, dry_run=dry_run)

        msgs.extend(source_msgs)
        total_copied += n

    msgs.append(f"Done. {total_copied} file(s) copied.")
    return total_copied, msgs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--full", action="store_true", help="Full sync: copy all files within the lookback window")
    parser.add_argument("--lookback", type=int, default=None, help="Lookback window in calendar days (default: from config)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be copied without copying")
    args = parser.parse_args()

    sys.path.insert(0, str(Path(__file__).parent / "src"))
    from cash_rec.config import load_config

    config = load_config()
    total_copied, msgs = sync_all(
        config,
        full=args.full,
        lookback_days=args.lookback,
        dry_run=args.dry_run,
    )
    print()
    for m in msgs:
        print(m)
    print()


if __name__ == "__main__":
    main()
