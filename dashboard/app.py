"""PharmaSafe dashboard shell: navigation and the global role-policy selector.  streamlit run dashboard/app.py"""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.utils.data import POLICY_LABELS

st.set_page_config(page_title="PharmaSafe", page_icon=":pill:", layout="wide")

pages = [
    st.Page("pages/1_Overview.py", title="Overview", default=True),
    st.Page("pages/2_Drug_Explorer.py", title="Drug Explorer"),
    st.Page("pages/3_Signal_Explorer.py", title="Safety Signals"),
    st.Page("pages/4_Temporal_Analytics.py", title="Temporal Analytics"),
    st.Page("pages/5_Drug_Combinations.py", title="Drug Combinations"),
    st.Page("pages/6_Data_Quality_and_Methodology.py", title="Data Quality & Methodology"),
]
nav = st.navigation(pages)

with st.sidebar:
    st.selectbox("Drug role policy", list(POLICY_LABELS), format_func=POLICY_LABELS.get, key="role_policy",
                 help="Which reported drug roles count as exposure. Primary suspect is the default analysis.")
    st.caption("Source: FDA FAERS / AEMS quarterly extracts. Potential signals are for further investigation only.")
nav.run()
