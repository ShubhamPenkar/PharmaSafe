"""Drug combination explorer: co-reported drug pairs and reaction disproportionality."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.utils import ui
from dashboard.utils.data import POLICY_LABELS, df, get_store, role_policy

store, policy = get_store(), role_policy()
st.title("Drug combination explorer")
st.warning("Two drugs appearing in the same case does not show that they interacted or that the combination caused the reaction. "
           "These are co-reporting patterns for further investigation.")

pairs = df(store.top_combinations(100))
if pairs.empty:
    st.info("No combination analytics found. Run `python scripts/run_full_pipeline.py`.")
    st.stop()

st.markdown("#### Most frequently co-reported drug pairs")
st.dataframe(pairs[["drug_1", "drug_2", "pair_cases", "expected_pair_cases", "lift"]].head(50), hide_index=True, width="stretch", column_config={
    "pair_cases": "Cases with both", "expected_pair_cases": st.column_config.NumberColumn("Expected if independent", format="%.1f"),
    "lift": st.column_config.NumberColumn("Lift", format="%.2f", help="Observed / expected co-reporting")})

st.markdown("#### Reactions disproportionately reported with a combination")
drug = st.text_input("Contains drug (exact normalized name, optional)", key="c_drug")
only = st.toggle("Potential signals only", value=True)
sig = df(store.combinations(drug or None, "POTENTIAL_SIGNAL" if only else None, 300))
if sig.empty:
    st.info("No combination-reaction pairs match.")
else:
    show = sig[["drug_name", "reaction_term", "observed_cases", "expected_cases", "ROR", "ROR_CI_lower", "ROR_CI_upper", "PRR", "screening_status"] + [c for c in ("ror_drug_1", "ror_drug_2", "combo_exceeds_single_drug_ror") if c in sig.columns]]
    st.dataframe(show, hide_index=True, width="stretch", column_config={
        "drug_name": "Drug pair", "reaction_term": "Reaction", "observed_cases": "Cases with both and reaction",
        "ror_drug_1": st.column_config.NumberColumn("ROR drug 1 alone", format="%.2f"), "ror_drug_2": st.column_config.NumberColumn("ROR drug 2 alone", format="%.2f"),
        "combo_exceeds_single_drug_ror": st.column_config.CheckboxColumn("Exceeds both single-drug RORs", help="A screening aid only; not evidence of interaction")})
    st.download_button("Download CSV", sig.drop(columns=["screening_reasons"], errors="ignore").to_csv(index=False), file_name="combination_signals.csv", key="dl_combo")
