"""Data quality, provenance and methodology."""

import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.utils import ui
from dashboard.utils.data import POLICY_LABELS, df, get_store, role_policy

store, policy = get_store(), role_policy()
st.title("Data quality and methodology")
dq = store.data_quality()

st.markdown("#### Dataset provenance")
arch = dq.get("manifest_archives", {})
if arch:
    st.dataframe(df([{"Quarter": q, "File": a.get("filename"), "Size (MB)": a.get("file_size_mb"), "SHA-256": a.get("sha256_checksum"), "Status": a.get("verification_status")} for q, a in arch.items()]), hide_index=True, width="stretch")
else:
    st.info("Provenance manifest not available in this environment.")

st.markdown("#### Processing and quality checks")
checks = df(dq.get("checks", []))
if checks.empty:
    st.info("No audit found. Run the full pipeline to generate one.")
else:
    st.caption(f"{int(checks['passed'].sum())} of {len(checks)} checks passed.")
    checks["result"] = checks["passed"].map({True: "pass", False: "FAIL"})
    st.dataframe(checks[["check", "result", "detail"]], hide_index=True, width="stretch")

silver = (dq.get("silver") or store.silver_metrics()).get("tables", {})
if silver:
    demo = silver["DEMO"]
    st.markdown("#### Duplicate and superseded report handling")
    ui.metric_row([("Raw DEMO rows", demo["raw_rows"], None), ("Superseded versions", demo["superseded_versions"], "Older versions of a case"),
                   ("Excluded duplicate rows", demo["excluded_rows"], None), ("Canonical cases", demo["canonical_cases_latest"], None)])
    st.json(demo["excluded_by_reason"])
    st.markdown("#### Child-table relational integrity")
    st.dataframe(df([{"Table": t, "Raw rows": m["raw_rows"], "Retained": m["retained_rows"], "Excluded": m["excluded_rows"], "Retention %": m["retention_pct"], "Reasons": m["excluded_by_reason"]}
                     for t, m in silver.items() if t != "DEMO" and "raw_rows" in m]), hide_index=True, width="stretch")
    st.markdown("#### Missing values in key fields (% of retained rows)")
    miss = [{"Table": t, "Field": c, "Missing %": v} for t, m in silver.items() for c, v in (m.get("missing_pct") or {}).items()
            if c in {"age", "sex", "event_dt", "fda_dt", "occr_country", "caseversion", "drugname", "prod_ai", "role_cod", "pt", "outc_cod"}]
    st.dataframe(df(miss), hide_index=True, width="stretch")

st.markdown("#### Statistical methodology and screening thresholds")
o = store.overview(policy)
st.markdown(f"""
- **Unit of analysis:** unique eligible cases (latest report version per case). Eligible = at least one qualifying drug and one reaction. N = {o['eligible_cases_N']:,} under this policy.
- **ROR** = (a/b)/(c/d) with a Wald 95% interval on ln(ROR). **PRR** = [a/(a+b)]/[c/(c+d)]. Zero cells: 0.5 added to every cell.
- **Screening criteria (project choices, not clinical standards):** {o['criteria']}.
""")
st.caption("Counting rules and limits are documented in docs/methodology_and_counting_rules.md and docs/limitations.md.")

report = store.reports_dir / "validation_report.md" if store.reports_dir else None
if report and report.exists():
    with st.expander("Validation report (reference signals, robustness, temporal validation)"):
        st.markdown(report.read_text(encoding="utf-8"))
limits = (store.reports_dir.parent / "docs/limitations.md") if store.reports_dir else None
if limits and limits.exists():
    with st.expander("Known limitations", expanded=True):
        st.markdown(limits.read_text(encoding="utf-8"))
