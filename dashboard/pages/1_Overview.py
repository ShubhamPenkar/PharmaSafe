"""Overview: dataset-level counts and distributions."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.utils import ui
from dashboard.utils.data import POLICY_LABELS, df, get_store, role_policy

store, policy = get_store(), role_policy()
o = store.overview(policy)

st.title("PharmaSafe: pharmaceutical safety overview")
st.caption(f"Snapshot `{o['snapshot']}` · exposure policy: {POLICY_LABELS[policy]} · quarters {o['quarters'][0]} to {o['quarters'][-1]}")
ui.caveat()

ui.metric_row([
    ("Report records processed", o["total_report_records_processed"], "DEMO rows across quarters, before version handling"),
    ("Unique cases", o["unique_cases"], "One latest report version per case"),
    ("Drug records", o["drug_records"], "DRUG rows linked to a retained report version"),
    ("Drug entities", o["distinct_drug_entities"], "Distinct normalized active ingredients (or reported names when none)"),
])
ui.metric_row([
    ("Reaction terms", o["distinct_reaction_terms"], "Distinct MedDRA preferred terms"),
    ("Potential signals", o["potential_signals"], "Drug-event pairs meeting the configured screening criteria"),
    ("Pairs screened", o["drug_event_pairs_screened"], "Pairs with at least the minimum stored cases"),
    ("Reporting quarters", o["reporting_quarters"], None),
])
st.caption("Report records, unique cases, drug records and reaction entries are different units and are never interchangeable.")

left, right = st.columns(2)
with left:
    ui.hbar(df(store.top_reactions(15)), "pt", "cases", "Most frequently reported reactions (cases)")
with right:
    ui.hbar(df(store.top_drugs(15, policy)).rename(columns={"drug_key": "drug"}), "drug", "cases", "Most frequently reported drugs (cases)")

left, right = st.columns(2)
with left:
    pairs = df(store.top_pairs(12, policy))
    if not pairs.empty:
        pairs["pair"] = pairs["drug_name"] + " / " + pairs["reaction_term"]
    ui.hbar(pairs, "pair", "observed_cases", "Most frequently reported drug-event pairs (cases)")
with right:
    out = df(store.outcomes())
    if not out.empty:
        out["outcome"] = out["outc_cod"].map(ui.OUTCOME_LABELS).fillna(out["outc_cod"])
    ui.hbar(out, "outcome", "cases", "Reported outcomes (cases, a case can have several)", height=330)

left, right = st.columns(2)
with left:
    status = df(store.signal_status_counts(policy))
    if not status.empty:
        status["screening"] = status["screening_status"].map(ui.STATUS_LABEL)
    ui.bars(status, "screening", "pairs", "Screened drug-event pairs by screening result")
with right:
    trend = df(store.quarterly_trends())
    ui.lines(trend, "report_quarter", {"cases": "Cases"}, "Cases by report quarter (reporting volume, not incidence)")

with st.expander("Screening criteria in force"):
    st.json(o["criteria"])
