"""
PharmaSafe Configuration Loader
Loads and validates YAML configuration files for data sources, analysis parameters,
and reference benchmark signals.
"""

from pathlib import Path
from typing import Any, Dict
import yaml


def get_project_root() -> Path:
    """Returns the absolute root directory of the PharmaSafe project."""
    return Path(__file__).resolve().parent.parent.parent


def load_yaml(path_or_relative: str | Path) -> Dict[str, Any]:
    """Loads a YAML configuration file from absolute or project-root relative path."""
    path = Path(path_or_relative)
    if not path.is_absolute():
        path = get_project_root() / path

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def load_data_sources_config() -> Dict[str, Any]:
    """Loads configs/data_sources.yaml."""
    return load_yaml("configs/data_sources.yaml")


def load_analysis_config() -> Dict[str, Any]:
    """Loads configs/analysis.yaml."""
    return load_yaml("configs/analysis.yaml")


def load_reference_signals() -> Dict[str, Any]:
    """Loads configs/reference_signals.yaml."""
    return load_yaml("configs/reference_signals.yaml")
