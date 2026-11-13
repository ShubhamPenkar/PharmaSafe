"""Temporal analytics: how reporting patterns and candidate signals change by source quarter."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.utils import ui
from dashboard.utils.data import POLICY_LABELS, df, get_store, role_policy

store, policy = get_store(), role_policy()
st.title("Temporal analytics")
st.info("Quarter = FDA source quarter of each case's latest report version. Changes in counts describe reporting patterns, "
        "not changes in the actual occurrence of adverse events. Per-quarter statistics use only that quarter's cases.")

ui.lines(df(store.quarterly_trends()), "report_quarter", {"cases": "All cases", "serious_cases": "Cases with a serious outcome"}, "Cases by report quarter")

tab_drug, tab_reaction, tab_persist = st.tabs(["Drug and reaction trends", "Reaction trend", "Signal persistence"])
with tab_drug:
    drug = st.text_input("Drug (exact normalized name, or search below)", key="t_drug")
    if drug and len(drug) >= 2:
        hits = store.search_drugs(drug, 10)
        pick = st.selectbox("Drug", [h["drug_key"] for h in hits]) if hits else None
        if pick:
            d = store.drug_detail(pick, policy)
            ui.lines(df(d["quarterly_cases"]), "report_quarter", {"cases": "Cases"}, f"{pick}: cases by quarter ({POLICY_LABELS[policy]})")
            ts = df(store.temporal_signals(pick))
            if ts.empty:
                st.info("No drug-reaction pair reached the per-quarter minimum case count.")
            else:
                top = ts.groupby("reaction_term")["observed_cases"].sum().sort_values(ascending=False).head(10).index.tolist()
                sel = st.selectbox("Reaction", top)
                one = ts[ts["reaction_term"] == sel]
                ui.lines(one, "analysis_period", {"ROR": "ROR"}, f"{pick} / {sel}: ROR by quarter with 95% interval", error={"ROR": ("ROR_CI_lower", "ROR_CI_upper")})
                ui.lines(one, "analysis_period", {"PRR": "PRR"}, "PRR by quarter")
                st.dataframe(one[["analysis_period", "observed_cases", "ROR", "ROR_CI_lower", "ROR_CI_upper", "PRR", "screening_status"]], hide_index=True, width="stretch")
with tab_reaction:
    reaction = st.text_input("Reaction term (MedDRA preferred term)", key="t_reaction")
    if reaction:
        rq = df(store.quarterly_reaction_counts(reaction))
        ui.lines(rq, "report_quarter", {"cases": "Cases"}, f"{reaction.upper()}: cases by quarter") if not rq.empty else st.warning("Reaction term not found; use the exact preferred term.")
with tab_persist:
    st.caption("Per drug-event pair: in how many quarters the screening criteria were met, using that quarter's cases only.")
    pdrug = st.text_input("Filter by drug (optional, exact name)", key="p_drug")
    minq = st.slider("Minimum quarters flagged", 1, 8, 2)
    pers = df(store.persistence(pdrug or None, minq, 300))
    if pers.empty:
        st.info("No pairs match.")
    else:
        st.dataframe(pers, hide_index=True, width="stretch")
        st.download_button("Download CSV", pers.to_csv(index=False), file_name="signal_persistence.csv", key="dl_persist")
