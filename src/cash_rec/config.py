from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open('r', encoding='utf-8') as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f'Config file must contain a YAML mapping: {path}')
    return data


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    config_dir = Path(__file__).with_name('config')
    config = _load_yaml(config_dir / 'defaults.yaml')

    local_path = config_dir / 'config.local.yaml'
    if local_path.exists():
        config = _deep_merge(config, _load_yaml(local_path))

    if config_path is not None:
        config = _deep_merge(config, _load_yaml(Path(config_path)))

    return config
