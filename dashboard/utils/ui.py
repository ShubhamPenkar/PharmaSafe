"""Shared dashboard styling: validated categorical palette, plotly helpers, standing caveats."""

from typing import Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Reference categorical palette (slots 1-3 validate for all-pairs use); status is also carried by text/labels
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
NEUTRAL = "#8a8985"
SERIES = [BLUE, ORANGE, AQUA]
STATUS_COLOR = {"POTENTIAL_SIGNAL": BLUE, "NOT_FLAGGED": NEUTRAL}
STATUS_LABEL = {"POTENTIAL_SIGNAL": "Potential signal", "NOT_FLAGGED": "Not flagged"}

OUTCOME_LABELS = {
    "DE": "Death", "LT": "Life-threatening", "HO": "Hospitalization", "DS": "Disability",
    "CA": "Congenital anomaly", "RI": "Required intervention", "OT": "Other serious",
}

CAVEAT = ("Disproportionate reporting is a screening result, not evidence that a drug caused a reaction. "
          "Spontaneous reports have no exposure denominator, so no incidence or patient risk can be derived.")


def caveat() -> None:
    st.caption(CAVEAT)


def _style(fig: go.Figure, height: int, legend: bool) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=36, b=8), template="plotly",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=legend,
        legend=dict(orientation="h", y=-0.2), hoverlabel=dict(namelength=-1),
        font=dict(size=13),
    )
    fig.update_xaxes(showgrid=False, linecolor="rgba(128,128,128,.4)")
    fig.update_yaxes(gridcolor="rgba(128,128,128,.18)", zeroline=False)
    return fig


def hbar(frame: pd.DataFrame, label: str, value: str, title: str, color: str = BLUE, height: int = 380) -> None:
    """Single-series horizontal bar, largest on top, value shown in the hover tooltip and as a label."""
    if frame.empty:
        st.info(f"No data for {title.lower()}.")
        return
    d = frame.iloc[::-1]
    fig = go.Figure(go.Bar(x=d[value], y=d[label], orientation="h", marker_color=color,
                           hovertemplate="%{y}<br>%{x:,}<extra></extra>", text=d[value], texttemplate="%{x:,}", textposition="outside", cliponaxis=False))
    fig.update_layout(title=dict(text=title, font=dict(size=15)))
    fig.update_yaxes(automargin=True)
    st.plotly_chart(_style(fig, height, False), width="stretch")


def lines(frame: pd.DataFrame, x: str, series: Dict[str, str], title: str, height: int = 340, error: Optional[Dict[str, tuple]] = None) -> None:
    """Line chart with one axis. series maps column -> display name (max 3, fixed palette order)."""
    if frame.empty:
        st.info(f"No data for {title.lower()}.")
        return
    fig = go.Figure()
    for i, (colname, name) in enumerate(series.items()):
        err = None
        if error and colname in error:
            lo, hi = error[colname]
            err = dict(type="data", symmetric=False, array=(frame[hi] - frame[colname]).clip(lower=0), arrayminus=(frame[colname] - frame[lo]).clip(lower=0))
        fig.add_trace(go.Scatter(x=frame[x], y=frame[colname], name=name, mode="lines+markers", error_y=err,
                                 line=dict(color=SERIES[i % 3], width=2), marker=dict(size=8, line=dict(color="white", width=1.5)),
                                 hovertemplate=f"{name}: %{{y:,.2f}}<extra></extra>"))
    fig.update_layout(title=dict(text=title, font=dict(size=15)), hovermode="x unified")
    st.plotly_chart(_style(fig, height, len(series) > 1), width="stretch")


def bars(frame: pd.DataFrame, x: str, y: str, title: str, height: int = 300, color: str = BLUE) -> None:
    if frame.empty:
        st.info(f"No data for {title.lower()}.")
        return
    fig = go.Figure(go.Bar(x=frame[x], y=frame[y], marker_color=color, hovertemplate="%{x}<br>%{y:,}<extra></extra>"))
    fig.update_layout(title=dict(text=title, font=dict(size=15)))
    st.plotly_chart(_style(fig, height, False), width="stretch")


def metric_row(items: List[tuple]) -> None:
    cols = st.columns(len(items))
    for c, (label, value, help_) in zip(cols, items):
        c.metric(label, "n/a" if value is None else (f"{value:,}" if isinstance(value, int) else str(value)), help=help_)


def signals_table(frame: pd.DataFrame, key: str) -> None:
    """Formatted signal table with a status label and CSV export."""
    if frame.empty:
        st.info("No drug-event pairs match these filters.")
        return
    show = frame.copy()
    show["status"] = show["screening_status"].map(STATUS_LABEL)
    cols = ["drug_name", "reaction_term", "observed_cases", "expected_cases", "ROR", "ROR_CI_lower", "ROR_CI_upper", "PRR", "chi2_yates", "status"]
    show = show[[c for c in cols if c in show.columns]]
    st.dataframe(show, hide_index=True, width="stretch", column_config={
        "drug_name": "Drug", "reaction_term": "Reaction", "observed_cases": st.column_config.NumberColumn("Cases", format="%d"),
        "expected_cases": st.column_config.NumberColumn("Expected", format="%.1f", help="Cases expected if the drug and reaction were independent"),
        "ROR": st.column_config.NumberColumn("ROR", format="%.2f"), "ROR_CI_lower": st.column_config.NumberColumn("ROR 95% low", format="%.2f"),
        "ROR_CI_upper": st.column_config.NumberColumn("ROR 95% high", format="%.2f"), "PRR": st.column_config.NumberColumn("PRR", format="%.2f"),
        "chi2_yates": st.column_config.NumberColumn("Chi² (Yates)", format="%.1f"), "status": "Screening",
    })
    st.download_button("Download CSV", frame.drop(columns=["screening_reasons"], errors="ignore").to_csv(index=False), file_name=f"{key}.csv", mime="text/csv", key=f"dl_{key}")


def contingency_table(s: dict) -> pd.DataFrame:
    return pd.DataFrame(
        {"Reaction reported": [s["a"], s["c"], s["a"] + s["c"]], "Reaction not reported": [s["b"], s["d"], s["b"] + s["d"]],
         "Total cases": [s["a"] + s["b"], s["c"] + s["d"], s["a"] + s["b"] + s["c"] + s["d"]]},
        index=["Drug reported", "Drug not reported", "Total"],
    )


def explanation_markdown(s: dict, e: dict) -> str:
    return "\n".join([
        f"# {s['drug_name']} / {s['reaction_term']}", "", f"**{e['headline']}**", "",
        "## What the numbers mean", *[f"- {x}" for x in e["what_the_numbers_mean"]], "",
        "## Counting rules", *[f"- {x}" for x in e["counting_rules"]], "",
        "## What this cannot establish", *[f"- {x}" for x in e["cannot_establish"]], "",
    ])
