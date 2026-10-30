"""Drug Explorer: one drug reporting profile."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.utils import ui
from dashboard.utils.data import POLICY_LABELS, df, get_store, role_policy

store, policy = get_store(), role_policy()
st.title("Drug explorer")
ui.caveat()

query = st.text_input("Search by active ingredient or reported product name", placeholder="e.g. a brand or generic name", key="drug_query")
matches = store.search_drugs(query, 25) if len(query.strip()) >= 2 else []
if query and not matches:
    st.warning("No drug matches. Try a shorter fragment of the name.")
if not matches:
    st.stop()

labels = {m["drug_key"]: f"{m['drug_key']} ({m['cases_any_role']:,} cases)" + (f" · matched '{m['matched_reported_name']}'" if m["matched_reported_name"] and m["matched_reported_name"] != m["drug_key"] else "") for m in matches}
drug_key = st.selectbox("Matching drugs", list(labels), format_func=labels.get)
d = store.drug_detail(drug_key, policy)

st.subheader(d["drug_key"])
ui.metric_row([
    ("Cases (all roles)", int(d["summary"]["cases_any_role"]), None),
    ("Cases under selected policy", sum(r["cases"] for r in d["quarterly_cases"]), POLICY_LABELS[policy]),
    ("Drug records", int(d["summary"]["drug_records"]), "Rows in DRUG; a case can have several"),
    ("Reporting period", " to ".join(d["reporting_period"]) if d["reporting_period"] else "n/a", "Source quarters"),
])
st.caption("Reported product names: " + (", ".join(d["reported_names"][:12]) or "n/a"))

st.markdown("#### Reported adverse reactions and signal measures")
only_flagged = st.toggle("Show only potential signals", value=False)
res = store.signals(policy, drug=drug_key, exact=True, status="POTENTIAL_SIGNAL" if only_flagged else None, sort_by="observed_cases", limit=200)
st.caption(f"{res['total']:,} stored drug-reaction pairs (at least the minimum stored cases); showing the 200 most frequently reported.")
ui.signals_table(df(res["items"]), f"{drug_key}_reactions")

left, right = st.columns(2)
with left:
    out = df(d["outcomes"])
    if not out.empty:
        out["outcome"] = out["outc_cod"].map(ui.OUTCOME_LABELS).fillna(out["outc_cod"])
    ui.hbar(out, "outcome", "cases", "Reported outcomes (cases)", height=300)
with right:
    roles = df([{"role": k.upper(), "cases": v} for k, v in d["role_distribution"].items()]).replace({"PS": "Primary suspect", "SS": "Secondary suspect", "C": "Concomitant", "I": "Interacting"})
    ui.hbar(roles, "role", "cases", "Drug-role distribution (cases)", height=300)
left, right = st.columns(2)
with left:
    ui.bars(df(d["sex"]).rename(columns={"sex_norm": "sex"}).fillna("Unknown"), "sex", "cases", "Cases by reported sex")
with right:
    ages = df(d["age_bands"])
    if not ages.empty:
        ages["age_band"] = ages["age_band"].astype("category").cat.set_categories(["0-17", "18-44", "45-64", "65-74", "75+", "Unknown"], ordered=True)
        ages = ages.sort_values("age_band")
    ui.bars(ages, "age_band", "cases", "Cases by age band")
ui.lines(df(d["quarterly_cases"]), "report_quarter", {"cases": "Cases"}, "Cases by report quarter (reporting volume, not incidence)")
