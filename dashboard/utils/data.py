"""Data access for the dashboard: one cached read-only GoldStore plus pandas conversion helpers."""

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.serving.gold_store import GoldStore

POLICY_LABELS = {
    "primary": "Primary suspect (PS)",
    "all_suspect": "All suspect (PS + SS)",
    "all_reported": "All reported drugs",
}


@st.cache_resource(show_spinner="Opening Gold datasets...")
def _open_store(gold: str, silver: str, snapshot: str) -> GoldStore:
    return GoldStore(Path(gold), snapshot, reports_dir=PROJECT_ROOT / "reports", silver_dir=Path(silver))


def get_store() -> GoldStore:
    gold = os.environ.get("PHARMASAFE_GOLD_DIR", str(PROJECT_ROOT / "data/gold"))
    silver = os.environ.get("PHARMASAFE_SILVER_DIR", str(PROJECT_ROOT / "data/silver"))
    snapshot = os.environ.get("PHARMASAFE_SNAPSHOT", "latest")
    try:
        return _open_store(gold, silver, snapshot)
    except FileNotFoundError:
        st.error("No Gold data found. Build it first: `python scripts/run_full_pipeline.py`")
        st.stop()


def role_policy() -> str:
    return st.session_state.get("role_policy", "primary")


def df(records) -> pd.DataFrame:
    return pd.DataFrame(records)


@st.cache_data(show_spinner=False)
def cached_signals(_store: GoldStore, key: str, **kwargs) -> dict:
    """key makes the cache entry specific to the store/snapshot; kwargs are the query."""
    return _store.signals(**kwargs)
