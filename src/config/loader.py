"""Low-level, safe YAML loading used by the application's configuration boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_yaml_config(config_path: Path) -> dict[str, Any]:
    """Load a YAML mapping from ``config_path`` without applying domain rules.

    This function deliberately only parses YAML. Application-specific validation
    belongs in ``main.py``, where the parsed mapping is validated as a
    ``PipelineConfig`` before any pipeline work begins.
    """
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open(mode="r", encoding="utf-8") as file:
        parsed = yaml.safe_load(file)

    if parsed is None:
        return {}
    if not isinstance(parsed, dict):
        raise TypeError("YAML configuration root must be a mapping")
    return parsed
