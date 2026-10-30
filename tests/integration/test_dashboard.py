"""Every dashboard page renders against the synthetic Gold layer without raising."""

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

PAGES = Path(__file__).resolve().parent.parent.parent / "dashboard/pages"


@pytest.fixture(autouse=True)
def gold_env(synthetic_gold, monkeypatch):
    monkeypatch.setenv("PHARMASAFE_GOLD_DIR", str(synthetic_gold / "gold"))
    monkeypatch.setenv("PHARMASAFE_SILVER_DIR", str(synthetic_gold / "silver"))


def run(page: str, **inputs) -> AppTest:
    at = AppTest.from_file(str(PAGES / page), default_timeout=90)
    for key, value in inputs.items():
        at.session_state[key] = value
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    return at


@pytest.mark.parametrize("policy", ["primary", "all_suspect", "all_reported"])
def test_overview_renders_for_each_policy(policy):
    at = run("1_Overview.py", role_policy=policy)
    assert at.title[0].value.startswith("PharmaSafe")
    labels = [m.label for m in at.metric]
    assert "Unique cases" in labels and "Potential signals" in labels


def test_drug_explorer_search_flow():
    at = run("2_Drug_Explorer.py", drug_query="alpha")
    assert at.subheader[0].value == "SYNTH_DRUG_ALPHA"


def test_signal_explorer_explains_a_signal():
    at = run("3_Signal_Explorer.py")
    texts = " ".join(s.value for s in at.success)
    assert "Potential safety signal" in texts
    assert any("cannot establish" in m.value.lower() for m in at.markdown)


def test_temporal_and_combination_and_quality_pages_render():
    run("4_Temporal_Analytics.py", t_drug="alpha")
    run("5_Drug_Combinations.py")
    at = run("6_Data_Quality_and_Methodology.py")
    assert any("Dataset provenance" in m.value for m in at.markdown)
