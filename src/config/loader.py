"""Low-level, safe YAML loading used by the application's configuration boundary."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_yaml_config(config_path: Path) -> tuple[str, dict[str, Any]]:
    """Load a YAML mapping and its original text from ``config_path``.

    Application-specific validation belongs in ``main.py``, where the parsed
    mapping is validated as a ``PipelineConfig`` before any pipeline work
    begins. Returning the source text lets the entry point hash the exact YAML
    content that was parsed and validated.
    """
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw_text = config_path.read_text(encoding="utf-8")
    parsed = yaml.safe_load(raw_text)

    if parsed is None:
        return raw_text, {}
    if not isinstance(parsed, dict):
        raise TypeError("YAML configuration root must be a mapping")
    return raw_text, parsed
