"""
PharmaSafe Configuration Loader
Loads and validates YAML configuration files for data sources, analysis parameters,
and reference benchmark signals.
"""

import json
from pathlib import Path, PureWindowsPath
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


def resolve_path(path_like: str | Path) -> Path:
    """Resolves a configured path: absolute paths are kept, relative ones are anchored at the project root."""
    path = Path(path_like)
    return path if path.is_absolute() else get_project_root() / path


def to_portable_path(path_like: str) -> Path:
    """
    Converts a manifest path recorded on any OS into a local Path.
    The manifest was first generated on Windows ('data\\\\raw\\\\...'), which POSIX Path does not split.
    """
    return Path(*PureWindowsPath(path_like).parts) if "\\" in path_like else Path(path_like)


def load_raw_manifest() -> Dict[str, Any]:
    """Loads data/raw/manifest.json with portable archive paths resolved under the project root."""
    manifest = load_yaml_or_json(load_data_sources_config()["storage"]["manifest_path"])
    for meta in manifest.get("archives", {}).values():
        if "relative_path" in meta:
            meta["resolved_path"] = get_project_root() / to_portable_path(meta["relative_path"])
    return manifest


def load_yaml_or_json(path_or_relative: str | Path) -> Dict[str, Any]:
    """Loads a JSON or YAML file (JSON is a YAML subset, but json is faster and stricter)."""
    path = resolve_path(path_or_relative)
    if path.suffix == ".json":
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return load_yaml(path)
