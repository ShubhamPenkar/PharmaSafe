"""Safety Signal Explorer: search, rank and explain candidate drug-event signals."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.utils import ui
from dashboard.utils.data import POLICY_LABELS, df, get_store, role_policy

store, policy = get_store(), role_policy()
st.title("Safety signal explorer")
ui.caveat()

c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
drug = c1.text_input("Drug contains", key="sig_drug")
reaction = c2.text_input("Reaction contains", key="sig_reaction")
min_cases = c3.number_input("Min cases", 0, 100000, 3, key="sig_min")
status = c4.selectbox("Screening", ["POTENTIAL_SIGNAL", "NOT_FLAGGED", "All"], format_func=lambda s: ui.STATUS_LABEL.get(s, s), key="sig_status")
sort_by = st.selectbox("Rank by", ["ROR_CI_lower", "ROR", "PRR", "observed_cases", "chi2_yates"], format_func={
    "ROR_CI_lower": "ROR lower confidence bound (conservative)", "ROR": "ROR", "PRR": "PRR", "observed_cases": "Observed cases", "chi2_yates": "Chi-square (Yates)"}.get)

res = store.signals(policy, drug or None, reaction or None, None if status == "All" else status, int(min_cases), sort_by, True, 200, 0)
st.caption(f"{res['total']:,} matching pairs; showing the top {len(res['items'])}. ROR, PRR and intervals describe reporting disproportion only.")
frame = df(res["items"])
ui.signals_table(frame, "signals")

if not frame.empty:
    st.markdown("#### Explain a signal")
    options = {f"{r.drug_name} / {r.reaction_term}": (r.drug_name, r.reaction_term) for r in frame.head(100).itertuples()}
    choice = st.selectbox("Drug-event pair", list(options))
    found = store.signal(*options[choice], role_policy=policy)
    s, e = found["signal"], found["explanation"]
    (st.success if s["screening_status"] == "POTENTIAL_SIGNAL" else st.info)(e["headline"])
    left, right = st.columns([1, 1])
    with left:
        st.markdown("**2x2 table of unique eligible cases**")
        st.dataframe(ui.contingency_table(s), width="stretch")
        ui.metric_row([("Observed", int(s["observed_cases"]), None), ("Expected if independent", f"{s['expected_cases']:.1f}", None),
                       ("ROR", f"{s['ROR']:.2f}", f"95% CI {s['ROR_CI_lower']:.2f} to {s['ROR_CI_upper']:.2f}"), ("PRR", f"{s['PRR']:.2f}", f"95% CI {s['PRR_CI_lower']:.2f} to {s['PRR_CI_upper']:.2f}")])
    with right:
        for title, items in (("What the numbers mean", e["what_the_numbers_mean"]), ("Counting rules", e["counting_rules"]), ("What this cannot establish", e["cannot_establish"])):
            st.markdown(f"**{title}**")
            st.markdown("\n".join(f"- {x}" for x in items))
        if s["screening_reasons"]:
            st.markdown("**Criteria not met:** " + "; ".join(s["screening_reasons"]))
    st.download_button("Download explanation (Markdown)", ui.explanation_markdown(s, e), file_name=f"{s['drug_name']}_{s['reaction_term']}.md", key="dl_explain")
